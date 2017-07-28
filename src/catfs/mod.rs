extern crate fuse;
extern crate libc;
extern crate threadpool;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr, ReplyOpen, ReplyEmpty,
                 ReplyDirectory, ReplyData, ReplyWrite, ReplyCreate};

use self::time::Duration;

use std::cmp;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};

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

#[derive(Default)]
struct InodeStore {
    inodes: HashMap<u64, Arc<Mutex<Inode>>>,
    inodes_cache: HashMap<PathBuf, u64>,
}

impl InodeStore {
    fn get(&self, ino: u64) -> Arc<Mutex<Inode>> {
        return self.inodes.get(&ino).unwrap().clone();
    }

    fn get_mut_by_path(&mut self, path: &Path) -> Option<Arc<Mutex<Inode>>> {
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
        let inode = inode.lock().unwrap();
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
    pub fn new(from: &AsRef<Path>, to: &AsRef<Path>) -> error::Result<CatFS> {
        let src_dir = rlibc::open(from, rlibc::O_RDONLY, 0)?;
        let cache_dir = rlibc::open(to, rlibc::O_RDONLY, 0)?;

        let mut catfs = CatFS {
            from: from.as_ref().to_path_buf(),
            cache: to.as_ref().to_path_buf(),
            src_dir: src_dir,
            cache_dir: cache_dir,
            ttl: Duration::zero(),
            store: Mutex::new(Default::default()),
            dh_store: Mutex::new(Default::default()),
            fh_store: Mutex::new(Default::default()),
            tp: Mutex::new(ThreadPool::new(100)),
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
        inode.use_ino(fuse::FUSE_ROOT_ID);

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
        store.inodes.insert(ino, Arc::new(Mutex::new(inode)));
    }

    fn remove_path(&mut self, path: &Path) {
        let mut store = self.store.lock().unwrap();
        store.inodes_cache.remove(path);
    }

    fn ttl_now(&self) -> time::Timespec {
        return time::get_time() + self.ttl;
    }
}

impl Filesystem for CatFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode: Arc<Mutex<Inode>>;
        let mut old_inode: Option<Arc<Mutex<Inode>>> = None;
        let path: PathBuf;
        {
            let mut store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
            let parent_inode = parent_inode.lock().unwrap();
            path = parent_inode.get_child_name(name);
            if let Some(ref mut i) = store.get_mut_by_path(&path) {
                old_inode = Some(i.clone());
                let mut inode = i.lock().unwrap();
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

        // TODO spawn a thread to do lookup
        let parent_inode = parent_inode.lock().unwrap();
        match parent_inode.lookup(name) {
            Ok(new_inode) => {
                if let Some(inode) = old_inode {
                    let mut inode = inode.lock().unwrap();
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
                    reply.entry(&self.ttl_now(), &new_inode.get_attr(), 0);
                    debug!(
                        "<-- lookup {:?} = 0x{:016x}, {:?} refcnt *1",
                        new_inode.get_path(),
                        new_inode.get_ino(),
                        new_inode.get_kind()
                    );
                    self.insert_inode(new_inode);
                }
            }
            Err(e) => {
                if let Some(inode) = old_inode {
                    let mut inode = inode.lock().unwrap();
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

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);
        let inode = inode.lock().unwrap();
        reply.attr(&self.ttl_now(), inode.get_attr());
        debug!(
            "<-- getattr {} {:?} {} bytes",
            ino,
            inode.get_path(),
            inode.get_attr().size
        );
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        let mut store = self.store.lock().unwrap();
        let stale: bool;
        {
            let inode = store.get(ino);
            let mut inode = inode.lock().unwrap();
            stale = inode.deref(nlookup);
        }
        if stale {
            debug!("<-- forgot 0x{:016x}", ino);
            store.remove_ino(ino);
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);
        let inode = inode.lock().unwrap();

        match inode.opendir() {
            Ok(dir) => {
                let mut dh_store = self.dh_store.lock().unwrap();
                let dh = dh_store.next_id;
                dh_store.next_id += 1;
                dh_store.handles.insert(dh, dir);
                reply.opened(dh, flags);
                debug!("<-- opendir {:?} = {}", inode.get_path(), dh);
            }
            Err(e) => {
                error!("<-- !opendir {:?} = {}", inode.get_path(), e);
                reply.error(error::errno(&e));
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        dh: u64,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let mut dh_store = self.dh_store.lock().unwrap();
        let mut dir = dh_store.handles.get_mut(&dh).unwrap();
        // TODO spawn a thread
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

    fn releasedir(&mut self, _req: &Request, _ino: u64, dh: u64, _flags: u32, reply: ReplyEmpty) {
        let mut dh_store = self.dh_store.lock().unwrap();
        // the handle will be destroyed and closed
        dh_store.handles.remove(&dh);
        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let s = make_self(self);
        self.tp.lock().unwrap().execute(move || {
            let inode: Arc<Mutex<Inode>>;
            {
                let store = s.store.lock().unwrap();
                inode = store.get(ino);
            }

            let mut inode = inode.lock().unwrap();
            match inode.open(flags) {
                Ok(file) => {
                    let mut fh_store = s.fh_store.lock().unwrap();
                    let fh = fh_store.next_id;
                    fh_store.next_id += 1;
                    fh_store.handles.insert(fh, Arc::new(Mutex::new(file)));
                    reply.opened(fh, flags);
                    debug!("<-- open {:?} = {}", inode.get_path(), fh);
                }
                Err(e) => {
                    reply.error(error::errno(&e));
                    error!("<-- !open {:?} = {}", inode.get_path(), e);
                }
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        reply: ReplyData,
    ) {
        let fh_store = self.fh_store.lock().unwrap();
        let file = fh_store.handles.get(&fh).unwrap();
        // TODO spawn a thread
        let mut buf: Vec<u8> = Vec::with_capacity(size as usize);
        buf.resize(size as usize, 0u8);
        let file = file.lock().unwrap();
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

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        let parent_inode: Arc<Mutex<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.lock().unwrap();
        match parent_inode.create(name, mode) {
            Ok((inode, file)) => {
                let fh: u64;
                {
                    let mut fh_store = self.fh_store.lock().unwrap();
                    fh = fh_store.next_id;
                    fh_store.next_id += 1;
                    fh_store.handles.insert(fh, Arc::new(Mutex::new(file)));
                }

                reply.created(&self.ttl_now(), &inode.get_attr(), 0, fh, flags);
                debug!("<-- create {:?} = {}", inode.get_path(), fh);
                self.insert_inode(inode);
            }
            Err(e) => {
                error!(
                    "<-- !create {:?} = {}",
                    parent_inode.get_child_name(name),
                    e
                );
                reply.error(e.raw_os_error().unwrap());
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let nwritten: usize;
        {
            let fh_store = self.fh_store.lock().unwrap();
            let file = fh_store.handles.get(&fh).unwrap();
            let mut file = file.lock().unwrap();
            // TODO spawn a thread
            match file.write(offset, data) {
                Ok(nbytes) => nwritten = nbytes,
                Err(e) => {
                    debug!(
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

        reply.written(nwritten as u32);
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);
        let mut inode = inode.lock().unwrap();
        inode.extend(offset + (nwritten as u64));
    }

    fn flush(&mut self, _req: &Request, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let s = make_self(self);
        self.tp.lock().unwrap().execute(move || {
            let flushed_to_src: bool;
            {
                // first flush locally
                let file: Arc<Mutex<file::Handle>>;
                {
                    let fh_store = s.fh_store.lock().unwrap();
                    file = fh_store.handles.get(&fh).unwrap().clone();
                }

                let mut file = file.lock().unwrap();
                match file.flush() {
                    Ok(b) => flushed_to_src = b,
                    Err(e) => {
                        error!("<-- !flush {:?} = {}", fh, e);
                        reply.error(error::errno(&e));
                        return;
                    }
                }
            }

            if flushed_to_src {
                let store = s.store.lock().unwrap();
                let inode = store.get(ino);
                let mut inode = inode.lock().unwrap();

                // refresh attr with the original file so it will be consistent with lookup
                if let Err(e) = inode.refresh() {
                    error!("<-- !flush {:?} = {}", inode.get_path(), e);
                    reply.error(error::errno(&e));
                    return;
                }
                debug!("<-- flush {:?}", inode.get_path());
            } else {
                debug!("<-- flush ino: {} fh: {}", ino, fh);
            }

            reply.ok();
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut fh_store = self.fh_store.lock().unwrap();
        // the handle will be destroyed and closed
        fh_store.handles.remove(&fh);
        reply.ok();
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_inode: Arc<Mutex<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.lock().unwrap();
        let path = parent_inode.get_child_name(name);
        if let Err(e) = parent_inode.unlink(name) {
            debug!("<-- !unlink {:?} = {}", path, e);
            reply.error(e.raw_os_error().unwrap());
        } else {
            self.remove_path(&path);
            debug!("<-- unlink {:?}", path);
            reply.ok();
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_inode: Arc<Mutex<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.lock().unwrap();
        if let Err(e) = parent_inode.rmdir(name) {
            debug!(
                "<-- !rmdir {:?}/{:?} = {}",
                parent_inode.get_path(),
                name,
                e
            );
            reply.error(e.raw_os_error().unwrap());
        } else {
            debug!("<-- rmdir {:?}/{:?}", parent_inode.get_path(), name);
            reply.ok();
            self.remove_path(&parent_inode.get_path().join(name));
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let parent_inode: Arc<Mutex<Inode>>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent);
        }

        let parent_inode = parent_inode.lock().unwrap();
        match parent_inode.mkdir(name, mode) {
            Ok(inode) => {
                reply.entry(&self.ttl_now(), inode.get_attr(), 0);
                debug!("<-- mkdir {:?}/{:?}", parent_inode.get_path(), name);
                self.insert_inode(inode);
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
}
