extern crate fuse;

use std::ffi::OsStr;
use std::ffi::OsString;
use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr};

pub struct CatFS {
    from: OsString,
    cache: OsString,
}

impl CatFS {
    pub fn new(from: &OsStr, to: &OsStr) -> CatFS {
        return CatFS {
            from: from.to_os_string(),
            cache: to.to_os_string(),
        };
    }
}

impl Filesystem for CatFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {}

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {}

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {}
}
