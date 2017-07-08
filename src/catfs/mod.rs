extern crate fuse;
extern crate libc;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr};

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use self::time::Timespec;

mod inode;
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

pub struct CatFS {
    from: OsString,
    cache: OsString,

    ttl: Timespec,
    store: Mutex<InodeStore>,
}

impl CatFS {
    pub fn new(from: &OsStr, to: &OsStr) -> io::Result<CatFS> {
        let mut catfs = CatFS {
            from: from.to_os_string(),
            cache: to.to_os_string(),
            ttl: Timespec { sec: 0, nsec: 0 },
            store: Mutex::new(Default::default()),
        };

        let root_attr = Inode::lookup_path(from)?;
        let mut inode = Inode::new(OsString::new(), to.to_os_string(), root_attr);
        inode.use_ino(fuse::FUSE_ROOT_ID);

        catfs.insert_inode(inode.get_path().clone(), Arc::new(inode));

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

        match parent_inode.lookup(name) {
            Ok(inode) => {
                let inode = Arc::new(inode);
                reply.entry(&self.ttl, &inode.get_attr(), 0);
                self.insert_inode(inode.get_path().clone(), inode);
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
}
