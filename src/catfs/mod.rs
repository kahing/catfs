extern crate fuser;
extern crate libc;
extern crate threadpool;

use self::fuser::{
    ReplyEntry,
    ReplyAttr,
    ReplyOpen,
    ReplyEmpty,
    ReplyDirectory,
    ReplyData,ReplyWrite,
    ReplyCreate,
    ReplyStatfs,
    TimeOrNow
};

use std::cmp;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use self::threadpool::ThreadPool;

pub mod error;
pub mod file;
pub mod flags;
pub mod rlibc;
pub mod tests;

mod dir;
mod inode;
mod substr;

use self::inode::Inode;
use self::flags::DiskSpace;
use super::evicter::Evicter;

#[derive(Default)]
struct InodeStore {
    inodes: HashMap<u64, Arc<RwLock<Inode>>>,
    inodes_cache: HashMap<PathBuf, u64>,
}

impl InodeStore {
    fn get(&self, ino: u64) -> Arc<RwLock<Inode>> {
        return self.inodes.get(&ino).unwrap().clone();
    }

    fn get_mut_by_path(&mut self, path: &Path) -> Option<Arc<RwLock<Inode>>> {
        let ino: u64;

        if let Some(ino_ref) = self.inodes_cache.get(path) {
            ino = *ino_ref;
        } else {
            return None;
        }

        return Some(self.get(ino));
    }

    fn remove_ino(&mut self, ino: u64) {
        let inode = self.inodes.remove(&ino).unwrap();
        let inode = inode.read().unwrap();
        self.inodes_cache.remove(inode.get_path());
    }
}

struct HandleStore<T> {
    handles: HashMap<u64, T>,
    next_id: u64,
}

impl<T> Default for HandleStore<T> {
    fn default() -> HandleStore<T> {
        return HandleStore {
            handles: Default::default(),
            next_id: 1,
        };
    }
}

pub struct CatFS {
    from: PathBuf,
    cache: PathBuf,
    src_dir: RawFd,
    cache_dir: RawFd,

    ttl: Duration,
    store: Mutex<InodeStore>,
    dh_store: Mutex<HandleStore<dir::Handle>>,
    fh_store: Mutex<HandleStore<Arc<Mutex<file::Handle>>>>,
    tp: Mutex<ThreadPool>,
}

impl Drop for CatFS {
    fn drop(&mut self) {
        self.tp.lock().unwrap().join();
        if let Err(e) = rlibc::close(self.src_dir) {
            error!("!close({}) = {}", self.src_dir, error::RError::from(e));
        }
        if let Err(e) = rlibc::close(self.cache_dir) {
            error!("!close({}) = {}", self.cache_dir, error::RError::from(e));
        }
    }
}

// only safe to use when we know the return value will never be used
// before the fs instance is dropped, for example if we are spawning
// new threads, since drop() waits for the threads to finish first
pub fn make_self<T>(s: &mut T) -> &'static T {
    return unsafe { ::std::mem::transmute(s) };
}

impl CatFS {
    pub fn new(from: &dyn AsRef<Path>, to: &dyn AsRef<Path>) -> error::Result<CatFS> {
        let src_dir = rlibc::open(from, rlibc::O_RDONLY, 0)?;
        let cache_dir = rlibc::open(to, rlibc::O_RDONLY, 0)?;

        let mut catfs = CatFS {
            from: from.as_ref().to_path_buf(),
            cache: to.as_ref().to_path_buf(),
            src_dir: src_dir,
            cache_dir: cache_dir,
            ttl: Duration::ZERO,
            store: Mutex::new(Default::default()),
            dh_store: Mutex::new(Default::default()),
            fh_store: Mutex::new(Default::default()),
            tp: Mutex::new(ThreadPool::new(5)),
        };

        catfs.make_root()?;
        debug!("catfs {:?} {:?}", catfs.from, catfs.cache);

        return Ok(catfs);
    }

    pub fn get_cache_dir(&self) -> error::Result<RawFd> {
        return Ok(rlibc::openat(self.cache_dir, &".", rlibc::O_RDONLY, 0)?);
    }

    fn make_root(&mut self) -> error::Result<()> {
        let root_attr = Inode::lookup_path(self.src_dir, &self.from)?;

        let mut inode = Inode::new(
            self.src_dir,
            self.cache_dir,
            OsString::new(),
            PathBuf::new(),
            root_attr,
        );
        inode.use_ino(fuser::FUSE_ROOT_ID);

        self.insert_inode(inode);

        return Ok(());
    }

    fn insert_inode(&mut self, inode: Inode) {
        let mut store = self.store.lock().unwrap();
        let ino: u64;
        {
            let attr = inode.get_attr();
            ino = attr.ino;
            store.inodes_cache.insert(
                inode.get_path().to_path_buf(),
                attr.ino,
            );
        }
        store.inodes.insert(ino, Arc::new(RwLock::new(inode)));
    }

    fn get_inode(&self, ino: u64) -> Arc<RwLock<Inode>> {
        let store = self.store.lock().unwrap();
        return store.get(ino);
    }

    fn replace_path(&mut self, path: &Path, new_path: PathBuf) {
        let mut store = self.store.lock().unwrap();
        if let Some(ino) = store.inodes_cache.remove(path) {
            store.inodes_cache.insert(new_path, ino);
        }
    }

    fn remove_path(&mut self, path: &Path) {
        let mut store = self.store.lock().unwrap();
        store.inodes_cache.remove(path);
    }

    fn ttl_now(&self) -> Duration {
        return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap() + self.ttl;
    }

    pub fn statfs(&mut self, _ino: u64, reply: ReplyStatfs) {
        match rlibc::fstatvfs(self.cache_dir) {
            Ok(st) => {
                reply.statfs(
                    st.f_blocks as u64,
                    st.f_bfree as u64,
                    st.f_bavail as u64,
                    st.f_files as u64,
                    st.f_ffree as u64,
                    st.f_bsize as u32,
                    st.f_namemax as u32,
                    st.f_frsize as u32,
                )
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
        }
    }

    pub fn lookup(&mut self, parent: u64, name: OsString, reply: ReplyEntry) {
        let parent_inode: Arc<RwLock<Inode>>;
        let mut old_inode: Option<Arc<RwLock<Inode>>> = None;
        let path: PathBuf;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        {
            let parent_inode = parent_inode.read().unwrap();
            path = parent_inode.get_child_name(&name);
        }

        {
            let mut i: Option<Arc<RwLock<Inode>>>;

            {
                let mut store = self.store.lock().unwrap();
                i = store.get_mut_by_path(&path);
            }

            if let Some(ref mut i) = i {
                old_inode = Some(i.clone());
                let mut inode = i.write().unwrap();
                let refcnt = inode.inc_ref();

                if inode.not_expired(&self.ttl) {
                    reply.entry(&self.ttl_now(), inode.get_attr(), 0);
                    debug!(
                        "<-- lookup {:?} = 0x{:016x}, {:?} refcnt {}",
                        inode.get_path(),
                        inode.get_ino(),
                        inode.get_kind(),
                        refcnt
                    );
                    return;
                } else {
                    debug!(
                        "<-- lookup {:?} = 0x{:016x}, {:?} refcnt {} expired",
                        inode.get_path(),
                        inode.get_ino(),
                        inode.get_kind(),
                        refcnt
                    );
                }
            }
        }

        let parent_inode = parent_inode.read().unwrap();
        match parent_inode.lookup(&name) {
            Ok(new_inode) => {
                if let Some(inode) = old_inode {
                    let mut inode = inode.write().unwrap();
                    inode.take(new_inode);
                    reply.entry(&self.ttl_now(), &inode.get_attr(), 0);
                    debug!(
                        "<-- lookup {:?} = 0x{:016x}, {:?} refcnt {}",
                        inode.get_path(),
                        inode.get_ino(),
                        inode.get_kind(),
                        inode.get_refcnt(),
                    );
                } else {
                    debug!(
                        "<-- lookup {:?} = 0x{:016x}, {:?} refcnt *1",
                        new_inode.get_path(),
                        new_inode.get_ino(),
                        new_inode.get_kind()
                    );
                    let attr = *new_inode.get_attr();
                    self.insert_inode(new_inode);

                    reply.entry(&self.ttl_now(), &attr, 0);
                }
            }
            Err(e) => {
                if let Some(inode) = old_inode {
                    let mut inode = inode.write().unwrap();
                    let stale = inode.deref(1);
                    if stale {
                        let mut store = self.store.lock().unwrap();
                        store.remove_ino(inode.get_attr().ino);
                        debug!("<-- expired 0x{:016x}", inode.get_attr().ino);
                    }
                }
                debug!("<-- !lookup {:?} = {}", path, e);
                reply.error(error::errno(&e));
            }
        }
    }

    pub fn getattr(&mut self, ino: u64, reply: ReplyAttr) {
        let inode: Arc<RwLock<Inode>>;

        {
            let store = self.store.lock().unwrap();
            inode = store.get(ino);
        }

        {
            let inode = inode.read().unwrap();
            if !inode.was_flush_failed() {
                reply.attr(&self.ttl_now(), inode.get_attr());
                debug!(
                    "<-- getattr {} {:?} {} bytes",
                    ino,
                    inode.get_path(),
                    inode.get_attr().size
                );
                return;
            }
        }

        let mut inode = inode.write().unwrap();
        if let Err(e) = inode.refresh() {
            debug!("<-- !getattr {:?} = {}", inode.get_path(), e);
            reply.error(error::errno(&e));
            return;
        }
        reply.attr(&self.ttl_now(), inode.get_attr());
        debug!(
            "<-- getattr {} {:?} {} bytes",
            ino,
            inode.get_path(),
            inode.get_attr().size
        );
        return;
    }

    pub fn setattr(
        &mut self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if uid.is_some() || gid.is_some() {
            // need to think about how to support this as metadata is
            // only coming from src and catfs may not be running as root
            reply.error(libc::ENOTSUP);
            return;
        }

        if crtime.is_some() || chgtime.is_some() || bkuptime.is_some() {
            // don't know how to change these
            reply.error(libc::ENOTSUP);
            return;
        }

        let inode_ref: Arc<RwLock<Inode>>;
        let mut inode: RwLockWriteGuard<Inode>;
        let was_valid: error::Result<bool>;

        let file_ref: Arc<Mutex<file::Handle>>;
        let mut file: Option<MutexGuard<file::Handle>>;
        if let Some(fh) = fh {
            let fh_store = self.fh_store.lock().unwrap();
            file_ref = fh_store.handles.get(&fh).unwrap().clone();
            file = Some(file_ref.lock().unwrap());
            // if we had the file open, then we know that it's valid
            was_valid = Ok(true);
            inode_ref = self.get_inode(ino);
            inode = inode_ref.write().unwrap();
        } else {
            file = None;
            inode_ref = self.get_inode(ino);
            inode = inode_ref.write().unwrap();
            // if we change the size or mtime then we need to restore the
            // checksum xattr. XXX make this thing atomic
            was_valid = file::Handle::validate_cache(
                self.src_dir,
                self.cache_dir,
                &inode.get_path(),
                file.is_some(),
                true,
            );

            if let Err(e) = was_valid {
                error!("<-- !setattr {:16x} = {}", ino, e);
                reply.error(e.raw_os_error().unwrap());
                return;
            }
        }

        if let Some(mode) = mode {
            if let Some(ref file) = file {
                if let Err(e) = file.chmod(mode as libc::mode_t) {
                    error!("<-- !setattr {:16x} = {}", ino, e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            } else {
                if let Err(e) = inode.chmod(mode as libc::mode_t, flags.unwrap_or(0)) {
                    error!("<-- !setattr {:?} = {}", inode.get_path(), e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            }
        }

        if let Some(size) = size {
            if let Some(ref mut file) = file {
                if let Err(e) = file.truncate(size) {
                    error!("<-- !setattr {:16x} = {}", ino, e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            } else {
                if let Err(e) = inode.truncate(size) {
                    error!("<-- !setattr {:?} = {}", inode.get_path(), e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            }
        }

        if mtime.is_some() || atime.is_some() {
            let old_attr = inode.get_attr();

            let mtime = match mtime {
                Some(time_or_now) => match time_or_now {
                    TimeOrNow::SpecificTime(time) => Some(time),
                    TimeOrNow::Now => Some(SystemTime::now()),
                },
                None => None,
            };

            let atime = match atime {
                Some(time_or_now) => match time_or_now {
                    TimeOrNow::SpecificTime(time) => Some(time),
                    TimeOrNow::Now => Some(SystemTime::now()),
                },
                None => None,
            };

            if let Err(e) = inode.utimes(
                &atime.unwrap_or(old_attr.atime),
                &mtime.unwrap_or(old_attr.mtime),
                flags.unwrap_or(0),
            )
            {
                error!("<-- !setattr {:?} = {}", inode.get_path(), e);
                reply.error(e.raw_os_error().unwrap());
                return;
            }
        }

        // still need to restore the checksum even if a file handle is
        // supplied, because we may never flush that file handle
        if was_valid.unwrap() {
            if let Some(ref file) = file {
                if let Err(e) = file.set_pristine(true) {
                    error!("<-- !setattr {:?} = {}", inode.get_path(), e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            } else {
                if let Err(e) = file::Handle::make_pristine(
                    self.src_dir,
                    self.cache_dir,
                    &inode.get_path(),
                )
                {
                    error!("<-- !setattr {:?} = {}", inode.get_path(), e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            }
        }

        if let Err(e) = inode.refresh() {
            error!("<-- !setattr {:?} = {}", inode.get_path(), e);
            reply.error(e.raw_os_error().unwrap());
        } else {
            debug!(
                "<-- setattr {:?} 0x{:016x} 0x{:?}",
                inode.get_path(),
                ino,
                fh
            );
            reply.attr(&self.ttl_now(), inode.get_attr());
        }
    }

    pub fn forget(&mut self, ino: u64, nlookup: u64) {
        let inode: Arc<RwLock<Inode>>;
        let stale: bool;
        {
            let store = self.store.lock().unwrap();
            inode = store.get(ino);
        }

        {
            let mut inode = inode.write().unwrap();
            stale = inode.deref(nlookup);
        }

        if stale {
            debug!("<-- forgot 0x{:016x}", ino);
            let mut store = self.store.lock().unwrap();
            store.remove_ino(ino);
        }
    }

    pub fn opendir(&mut self, ino: u64, flags: i32, reply: ReplyOpen) {
        let inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            inode = store.get(ino);
        }

        let inode = inode.read().unwrap();
        match inode.opendir() {
            Ok(dir) => {
                let mut dh_store = self.dh_store.lock().unwrap();
                let dh = dh_store.next_id;
                dh_store.next_id += 1;
                dh_store.handles.insert(dh, dir);
                reply.opened(dh, flags as u32);
                debug!("<-- opendir {:?} = {}", inode.get_path(), dh);
            }
            Err(e) => {
                error!("<-- !opendir {:?} = {}", inode.get_path(), e);
                reply.error(error::errno(&e));
            }
        }
    }

    pub fn readdir(&mut self, _ino: u64, dh: u64, offset: i64, mut reply: ReplyDirectory) {
        let mut dh_store = self.dh_store.lock().unwrap();
        let dir = dh_store.handles.get_mut(&dh).unwrap();
        dir.seekdir(offset);
        loop {
            match dir.readdir() {
                Ok(res) => {
                    match res {
                        Some(entry) => {
                            if reply.add(entry.ino(), entry.off(), entry.kind(), entry.name()) {
                                dir.push(entry);
                                break;
                            } else {
                                dir.consumed(&entry);
                            }
                            debug!("<-- readdir {} = {:?} {}", dh, entry.name(), entry.off());
                        }
                        None => {
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("<-- !readdir {} = {}", dh, e);
                    reply.error(e.raw_os_error().unwrap());
                    return;
                }
            }
        }

        reply.ok();
    }

    pub fn releasedir(&mut self, _ino: u64, dh: u64, _flags: i32, reply: ReplyEmpty) {
        let mut dh_store = self.dh_store.lock().unwrap();
        // the handle will be destroyed and closed
        dh_store.handles.remove(&dh);
        reply.ok();
    }

    pub fn open(&mut self, ino: u64, flags: i32, reply: ReplyOpen) {
        let inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            inode = store.get(ino);
        }

        let mut inode = inode.write().unwrap();
        match inode.open(flags as u32, &self.tp) {
            Ok(file) => {
                let mut fh_store = self.fh_store.lock().unwrap();
                let fh = fh_store.next_id;
                fh_store.next_id += 1;
                fh_store.handles.insert(fh, Arc::new(Mutex::new(file)));
                reply.opened(fh, flags as u32);
                debug!("<-- open {:?} = {}", inode.get_path(), fh);
            }
            Err(e) => {
                reply.error(error::errno(&e));
                error!("<-- !open {:?} = {}", inode.get_path(), e);
            }
        }
    }

    pub fn read(
        &mut self,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData
    ) {
        let file: Arc<Mutex<file::Handle>>;
        {
            let fh_store = self.fh_store.lock().unwrap();
            file = fh_store.handles.get(&fh).unwrap().clone();
        }
        // TODO spawn a thread
        let mut buf: Vec<u8> = vec![0; size as usize];
        let mut file = file.lock().unwrap();
        match file.read(offset, &mut buf) {
            Ok(nread) => {
                reply.data(&buf[..nread]);
            }
            Err(e) => {
                debug!("<-- !read {} = {}", fh, e);
                reply.error(e.raw_os_error().unwrap());
            }
        }
    }

    pub fn create(
        &mut self,
        parent: u64,
        name: OsString,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.read().unwrap();
        match parent_inode.create(&name, mode as libc::mode_t) {
            Ok((inode, file)) => {
                let fh: u64;
                {
                    let mut fh_store = self.fh_store.lock().unwrap();
                    fh = fh_store.next_id;
                    fh_store.next_id += 1;
                    fh_store.handles.insert(fh, Arc::new(Mutex::new(file)));
                }

                let attr = *inode.get_attr();
                debug!("<-- create {:?} = {}", inode.get_path(), fh);
                self.insert_inode(inode);
                reply.created(&self.ttl_now(), &attr, 0, fh, flags as u32);
            }
            Err(e) => {
                error!(
                    "<-- !create {:?} = {}",
                    parent_inode.get_child_name(&name),
                    e
                );
                reply.error(e.raw_os_error().unwrap());
            }
        }
    }

    pub fn write(
        &mut self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        _flags: i32,
        reply: ReplyWrite,
    ) {
        let nwritten: usize;
        {
            let fh_store = self.fh_store.lock().unwrap();
            let file = fh_store.handles.get(&fh).unwrap();
            let mut file = file.lock().unwrap();
            // TODO spawn a thread
            loop {
                match file.write(offset, &data) {
                    Ok(nbytes) => {
                        nwritten = nbytes;
                        break;
                    }
                    Err(e) => {
                        if e.errno() == libc::ENOTSUP {
                            debug!("write rejected, reopening for sequential write");
                            // the src filesystem rejected our write,
                            // maybe because this is random
                            // write. reopen the src and try again
                            let inode: Arc<RwLock<Inode>>;

                            {
                                let store = self.store.lock().unwrap();
                                inode = store.get(ino);
                            }
                            let inode = inode.read().unwrap();

                            if let Err(e2) = inode.reopen_src(&mut file) {
                                reply.error(e2.raw_os_error().unwrap());
                                return;
                            }
                        } else if e.errno() == libc::ENOSPC {
                            debug!(
                                "write(0x{:016x}, 0x{:016x}, {}) = ENOSPC",
                                ino,
                                fh,
                                data.len()
                            );
                            let _ = Evicter::new(self.cache_dir, &DiskSpace::Percent(1.0))
                                .loop_once();
                        } else {
                            error!(
                                "<-- !write 0x{:016x} {:?} @ {} = {}",
                                fh,
                                OsStr::from_bytes(&data[..cmp::min(32, data.len())]),
                                offset,
                                e
                            );
                            reply.error(e.raw_os_error().unwrap());
                            return;
                        }
                    }
                }
            }
        }

        let inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            inode = store.get(ino);
        }
        let mut inode = inode.write().unwrap();
        inode.extend((offset as u64) + (nwritten as u64));
        reply.written(nwritten as u32);
    }

    pub fn flush(&mut self, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let s = make_self(self);
        self.tp.lock().unwrap().execute(move || {
            let flushed_to_src: bool;
            let inode: Arc<RwLock<Inode>>;
            {
                // first flush locally
                let file: Arc<Mutex<file::Handle>>;
                {
                    let fh_store = s.fh_store.lock().unwrap();
                    file = fh_store.handles.get(&fh).unwrap().clone();
                    let store = s.store.lock().unwrap();
                    inode = store.get(ino);
                }

                let mut file = file.lock().unwrap();
                match file.flush() {
                    Ok(b) => flushed_to_src = b,
                    Err(e) => {
                        let mut inode = inode.write().unwrap();
                        inode.flush_failed();

                        error!("<-- !flush {:016x} = {}", fh, e);
                        reply.error(error::errno(&e));
                        return;
                    }
                }
            }

            if flushed_to_src {
                let mut inode = inode.write().unwrap();
                inode.flushed();

                // refresh attr with the original file so it will be consistent with lookup
                if let Err(e) = inode.refresh() {
                    error!("<-- !flush {:?} = {}", inode.get_path(), e);
                    reply.error(error::errno(&e));
                    return;
                }
                debug!("<-- flush {:?}", inode.get_path());
            } else {
                let mut inode = inode.write().unwrap();
                inode.flushed();
                debug!("<-- flush ino: {:016x} fh: {}", ino, fh);
            }

            reply.ok();
        });
    }

    pub fn release(
        &mut self,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut fh_store = self.fh_store.lock().unwrap();
        // the handle will be destroyed and closed
        fh_store.handles.remove(&fh);
        reply.ok();
    }

    pub fn unlink(&mut self, parent: u64, name: OsString, reply: ReplyEmpty) {
        let parent_inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.read().unwrap();
        let path = parent_inode.get_child_name(&name);
        if let Err(e) = parent_inode.unlink(&name) {
            debug!("<-- !unlink {:?} = {}", path, e);
            reply.error(e.raw_os_error().unwrap());
        } else {
            self.remove_path(&path);
            debug!("<-- unlink {:?}", path);
            reply.ok();
        }
    }

    pub fn rmdir(&mut self, parent: u64, name: OsString, reply: ReplyEmpty) {
        let parent_inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.read().unwrap();
        if let Err(e) = parent_inode.rmdir(&name) {
            debug!(
                "<-- !rmdir {:?}/{:?} = {}",
                parent_inode.get_path(),
                name,
                e
            );
            reply.error(e.raw_os_error().unwrap());
        } else {
            debug!("<-- rmdir {:?}/{:?}", parent_inode.get_path(), name);
            self.remove_path(&parent_inode.get_path().join(name));
            reply.ok();
        }
    }

    pub fn mkdir(&mut self, parent: u64, name: OsString, mode: u32, _umask: u32, reply: ReplyEntry) {
        let parent_inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.read().unwrap();
        match parent_inode.mkdir(&name, mode as libc::mode_t) {
            Ok(inode) => {
                debug!("<-- mkdir {:?}/{:?}", parent_inode.get_path(), name);
                let attr = *inode.get_attr();
                self.insert_inode(inode);
                reply.entry(&self.ttl_now(), &attr, 0);
            }
            Err(e) => {
                debug!(
                    "<-- !mkdir {:?}/{:?} = {}",
                    parent_inode.get_path(),
                    name,
                    e
                );
                reply.error(e.raw_os_error().unwrap());
            }
        }
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: OsString,
        newparent: u64,
        newname: OsString,
        reply: ReplyEmpty,
    ) {
        let inode: Arc<RwLock<Inode>>;
        let path: PathBuf;
        let new_path: PathBuf;
        let parent_inode: Arc<RwLock<Inode>>;
        let new_parent_inode: Arc<RwLock<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
            new_parent_inode = store.get(newparent);
        }

        {
            let parent_inode = parent_inode.read().unwrap();
            let new_parent_inode = new_parent_inode.read().unwrap();

            path = parent_inode.get_child_name(&name);
            new_path = new_parent_inode.get_child_name(&newname);
        }

        {
            let mut store = self.store.lock().unwrap();
            match store.get_mut_by_path(&path) {
                Some(i) => inode = i,
                None => panic!("rename source not in inode cache: {:?}", path),
            }
        }

        let mut inode = inode.write().unwrap();
        if let Err(e) = inode.rename(&newname, &new_path) {
            debug!("<-- !rename {:?} -> {:?} = {}", path, new_path, e);
            reply.error(e.raw_os_error().unwrap());
        } else {
            debug!("<-- rename {:?} -> {:?}", path, new_path);
            self.replace_path(&path, new_path);
            reply.ok();
        }
    }
}
