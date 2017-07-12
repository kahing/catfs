extern crate fuse;
extern crate libc;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr, ReplyOpen, ReplyEmpty,
                 ReplyDirectory, ReplyData, ReplyWrite, ReplyCreate};

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use self::time::Timespec;

mod dir;
mod file;
mod inode;
mod rlibc;

use self::inode::Inode;

#[derive(Default)]
struct InodeStore {
    inodes: HashMap<u64, Arc<Inode>>,
    inodes_cache: HashMap<OsString, u64>,
}

impl InodeStore {
    fn get(&self, ino: u64) -> &Arc<Inode> {
        return self.inodes.get(&ino).unwrap();
    }

    fn get_mut(&mut self, ino: u64) -> &mut Arc<Inode> {
        return self.inodes.get_mut(&ino).unwrap();
    }

    fn get_mut_by_path(&mut self, path: &OsStr) -> Option<&mut Arc<Inode>> {
        let ino: u64;

        if let Some(ino_ref) = self.inodes_cache.get(path) {
            ino = *ino_ref;
        } else {
            return None;
        }

        return Some(self.get_mut(ino));
    }

    fn remove_ino(&mut self, ino: u64) {
        let inode = self.inodes.remove(&ino).unwrap();
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
    from: OsString,
    cache: OsString,

    ttl: Timespec,
    store: Mutex<InodeStore>,
    dh_store: Mutex<HandleStore<dir::Handle>>,
    fh_store: Mutex<HandleStore<file::Handle>>,
}

fn to_abs(from: &OsStr) -> io::Result<OsString> {
    return Ok(fs::canonicalize(from)?.into_os_string());
}

impl CatFS {
    pub fn new(from: &OsStr, to: &OsStr) -> io::Result<CatFS> {
        let mut catfs = CatFS {
            from: to_abs(from)?,
            cache: to_abs(to)?,
            ttl: Timespec { sec: 0, nsec: 0 },
            store: Mutex::new(Default::default()),
            dh_store: Mutex::new(Default::default()),
            fh_store: Mutex::new(Default::default()),
        };

        let root_attr = Inode::lookup_path(from)?;
        let mut inode = Inode::new(OsString::new(), OsString::new(), root_attr);
        inode.use_ino(fuse::FUSE_ROOT_ID);

        catfs.insert_inode(Arc::new(inode));

        debug!("catfs {:?} {:?}", catfs.from, catfs.cache);

        return Ok(catfs);
    }
}

impl CatFS {
    fn insert_inode(&mut self, inode: Arc<Inode>) {
        let mut store = self.store.lock().unwrap();
        let attr = inode.get_attr();
        store.inodes.insert(attr.ino, inode.clone());
        store.inodes_cache.insert(
            inode.get_path().to_os_string(),
            attr.ino,
        );
    }
}

impl Filesystem for CatFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode: Arc<Inode>;
        {
            let mut store = self.store.lock().unwrap();
            parent_inode = store.get(parent).clone();
            let path = parent_inode.get_child_name(name);

            if let Some(ref mut inode) = store.get_mut_by_path(&path) {
                reply.entry(&self.ttl, inode.get_attr(), 0);
                Arc::get_mut(inode).unwrap().inc_ref();
                debug!("<-- lookup {:?} = {:?}", inode.get_path(), inode.get_kind());
                return;
            }
        }

        // TODO spawn a thread to do lookup
        match parent_inode.lookup(name, &self.from) {
            Ok(inode) => {
                let inode = Arc::new(inode);
                reply.entry(&self.ttl, &inode.get_attr(), 0);
                debug!("<-- lookup {:?} = {:?}", inode.get_path(), inode.get_kind());
                self.insert_inode(inode);
            }
            Err(e) => {
                debug!(
                    "<-- !lookup {:?} = {:?}",
                    parent_inode.get_child_name(name),
                    e
                );
                reply.error(e.raw_os_error().unwrap());
            }
        }

    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);
        reply.attr(&self.ttl, inode.get_attr());
        debug!("<-- getattr {} {:?}", ino, inode.get_path());
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        let mut store = self.store.lock().unwrap();
        let stale: bool;
        {
            let mut inode = store.get_mut(ino);
            stale = Arc::get_mut(inode).unwrap().deref(nlookup);
        }
        if stale {
            store.remove_ino(ino);
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);

        match inode.opendir(&self.from) {
            Ok(dir) => {
                let mut dh_store = self.dh_store.lock().unwrap();
                let dh = dh_store.next_id;
                dh_store.next_id += 1;
                dh_store.handles.insert(dh, dir);
                reply.opened(dh, flags);
                debug!("<-- opendir {:?} = {}", inode.get_path(), dh);
            }
            Err(e) => {
                debug!("<-- !opendir {:?} = {}", inode.get_path(), e);
                reply.error(e.raw_os_error().unwrap());
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
                            debug!("<-- readdir {} {:?}", dh, entry.name());
                            if reply.add(entry.ino(), entry.off(), entry.kind(), entry.name()) {
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                Err(e) => {
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
        let store = self.store.lock().unwrap();
        let inode = store.get(ino);

        if !file::is_truncate(flags) {
            // start paging the file in
            // TODO do this in background and ensure only one copy is done
            if let Err(e) = inode.cache(&self.from, &self.cache) {
                debug!("<-- !open {:?} = {}", inode.get_path(), e);
                reply.error(e.raw_os_error().unwrap());
                return;
            }
        }

        match inode.open(&self.from, flags) {
            Ok(file) => {
                let mut fh_store = self.fh_store.lock().unwrap();
                let fh = fh_store.next_id;
                fh_store.next_id += 1;
                fh_store.handles.insert(fh, file);
                reply.opened(fh, flags);
                debug!("<-- open {:?} = {}", inode.get_path(), fh);
            }
            Err(e) => {
                reply.error(e.raw_os_error().unwrap());
                debug!("<-- !open {:?} = {}", inode.get_path(), e);
            }
        }
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
        let mut fh_store = self.fh_store.lock().unwrap();
        let mut file = fh_store.handles.get_mut(&fh).unwrap();
        // TODO spawn a thread
        let mut buf: Vec<u8> = Vec::with_capacity(size as usize);
        buf.resize(size as usize, 0u8);
        match file.read(offset, &mut buf) {
            Ok(nread) => {
                reply.data(&buf[..nread]);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
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
        let parent_inode: Arc<Inode>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.get(parent).clone();
        }

        match parent_inode.create(name, &self.cache, mode) {
            Ok((inode, file)) => {
                let fh: u64;
                {
                    let mut fh_store = self.fh_store.lock().unwrap();
                    fh = fh_store.next_id;
                    fh_store.next_id += 1;
                    fh_store.handles.insert(fh, file);
                }

                let inode = Arc::new(inode);
                reply.created(&self.ttl, &inode.get_attr(), 0, fh, flags);
                debug!("<-- create {:?} = {}", inode.get_path(), fh);
                self.insert_inode(inode);
            }
            Err(e) => {
                debug!(
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
        _ino: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let mut fh_store = self.fh_store.lock().unwrap();
        let mut file = fh_store.handles.get_mut(&fh).unwrap();
        // TODO spawn a thread
        match file.write(offset, data) {
            Ok(nwritten) => {
                reply.written(nwritten as u32);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
        }
    }

    fn flush(&mut self, _req: &Request, _ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let mut fh_store = self.fh_store.lock().unwrap();
        let mut file = fh_store.handles.get_mut(&fh).unwrap();
        // TODO spawn a thread
        match file.flush() {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
        }
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
}
