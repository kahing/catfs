extern crate fuse;
extern crate libc;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr, ReplyOpen, ReplyEmpty,
                 ReplyDirectory, ReplyData};

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
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

struct DirHandleStore {
    handles: HashMap<u64, dir::DirHandle>,
    next_id: u64,
}

impl Default for DirHandleStore {
    fn default() -> DirHandleStore {
        return DirHandleStore {
            handles: Default::default(),
            next_id: 1,
        };
    }
}

struct FileHandleStore {
    handles: HashMap<u64, file::FileHandle>,
    next_id: u64,
}

impl Default for FileHandleStore {
    fn default() -> FileHandleStore {
        return FileHandleStore {
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
    dh_store: Mutex<DirHandleStore>,
    fh_store: Mutex<FileHandleStore>,
}

impl CatFS {
    pub fn new(from: &OsStr, to: &OsStr) -> io::Result<CatFS> {
        let mut catfs = CatFS {
            from: from.to_os_string(),
            cache: to.to_os_string(),
            ttl: Timespec { sec: 0, nsec: 0 },
            store: Mutex::new(Default::default()),
            dh_store: Mutex::new(Default::default()),
            fh_store: Mutex::new(Default::default()),
        };

        let root_attr = Inode::lookup_path(from)?;
        let mut inode = Inode::new(OsString::new(), to.to_os_string(), root_attr);
        inode.use_ino(fuse::FUSE_ROOT_ID);

        catfs.insert_inode(inode.get_path().to_os_string(), Arc::new(inode));

        return Ok(catfs);
    }
}

impl CatFS {
    fn insert_inode(&mut self, path: OsString, inode: Arc<Inode>) {
        let mut store = self.store.lock().unwrap();
        let attr = inode.get_attr();
        store.inodes.insert(attr.ino, inode.clone());
        store.inodes_cache.insert(path, attr.ino);
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
                debug!("<-- lookup {} {:?}", parent, name);
                return;
            }
        }

        // TODO spawn a thread to do lookup
        match parent_inode.lookup(name) {
            Ok(inode) => {
                let inode = Arc::new(inode);
                reply.entry(&self.ttl, &inode.get_attr(), 0);
                self.insert_inode(inode.get_path().to_os_string(), inode);
                debug!("<-- lookup {} {:?}", parent, name);
            }
            Err(e) => {
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
        match dir::DirHandle::open(inode.get_path()) {
            Ok(dir) => {
                let mut dh_store = self.dh_store.lock().unwrap();
                let dh = dh_store.next_id;
                dh_store.next_id += 1;
                dh_store.handles.insert(dh, dir);
                reply.opened(dh, flags);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
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
                                reply.ok();
                                break;
                            }
                        }
                        None => {
                            reply.ok();
                            break;
                        }
                    }
                }
                Err(e) => {
                    reply.error(e.raw_os_error().unwrap());
                    break;
                }
            }
        }
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
        }

        match file::FileHandle::open(inode.get_path(), flags) {
            Ok(file) => {
                let mut fh_store = self.fh_store.lock().unwrap();
                let fh = fh_store.next_id;
                fh_store.next_id += 1;
                fh_store.handles.insert(fh, file);
                reply.opened(fh, flags);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap()),
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
