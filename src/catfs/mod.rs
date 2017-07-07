extern crate fuse;
extern crate libc;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr};

use std::collections::HashMap;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use self::time::Timespec;

mod inode;
use self::inode::Inode;

#[derive(Default)]
struct InodeStore {
    inodes: HashMap<u64, Arc<Inode>>,
    inodes_cache: HashMap<OsString, Arc<Inode>>,
    next_inode_id: u64,
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
        store.inodes_cache.insert(path, inode.clone());
        let attr = inode.get_attr();
        store.inodes.insert(attr.ino, inode.clone());
    }
}

impl Filesystem for CatFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode: Arc<Inode>;
        {
            let store = self.store.lock().unwrap();
            parent_inode = store.inodes.get(&parent).unwrap().clone();
            let path = parent_inode.get_child_name(name);

            if let Some(inode) = store.inodes_cache.get(&path) {
                reply.entry(&self.ttl, inode.get_attr(), 0);
                return;
            }
        }

        match parent_inode.lookup(name) {
            Ok(inode) => {
                let inode = Arc::new(inode);
                self.insert_inode(inode.get_path().clone(), inode.clone());
                reply.entry(&self.ttl, &inode.get_attr(), 0);
            }
            Err(e) => {
                reply.error(e.raw_os_error().unwrap());
            }
        }

    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let store = self.store.lock().unwrap();
        let inode = store.inodes.get(&ino).unwrap();
        reply.attr(&self.ttl, inode.get_attr());
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {}
}
