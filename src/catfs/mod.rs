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
    pub fn new(from: &OsStr, to: &OsStr) -> CatFS {
        return CatFS {
            from: from.to_os_string(),
            cache: to.to_os_string(),
            ttl: Timespec { sec: 0, nsec: 0 },
            store: Mutex::new(Default::default()),
        };
    }
}

impl Filesystem for CatFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_inode: Arc<Inode>;
        {
            let store = self.store.lock().unwrap();
            // clone to hack around the borrow checker, I want to
            // unlock the store while keeping the parent_inode so I
            // can do lookup without holding the lock. This is safe
            // because the kernel shouldn't forget the parent inode
            // while it's looking up a child
            parent_inode = store.inodes.get(&parent).unwrap().clone();
            let path = parent_inode.get_child_name(name);

            if let Some(inode) = store.inodes_cache.get(&path) {
                reply.entry(&self.ttl, inode.get_attr(), 0);
                return;
            }
        }

        match parent_inode.lookup(name) {
            Ok(inode) => {
                // cache the inode
                let path = inode.get_path().clone();
                let mut store = self.store.lock().unwrap();
                let rc_inode = Arc::new(inode);
                store.inodes_cache.insert(path, rc_inode.clone());
                let attr = rc_inode.get_attr();
                store.inodes.insert(attr.ino, rc_inode.clone());
                reply.entry(&self.ttl, &attr, 0);
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
