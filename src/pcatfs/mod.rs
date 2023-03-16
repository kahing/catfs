extern crate fuse;
extern crate threadpool;
extern crate time;

use self::fuse::{Filesystem, Request, ReplyEntry, ReplyAttr, ReplyOpen, ReplyEmpty,
                 ReplyDirectory, ReplyData, ReplyWrite, ReplyCreate, ReplyStatfs};
use self::threadpool::ThreadPool;
use self::time::Timespec;

use std::ffi::OsStr;
use std::ops::Deref;

use catfs::CatFS;

pub struct PCatFS {
    tp: ThreadPool,
    fs: CatFS,
}

impl Drop for PCatFS {
    fn drop(&mut self) {
        self.tp.join();
    }
}

pub fn make_self<T>(s: &mut T) -> &'static mut T {
    return unsafe { ::std::mem::transmute(s) };
}

impl PCatFS {
    pub fn new(fs: CatFS, n_threads : usize) -> PCatFS {
        PCatFS {
            tp: ThreadPool::new(n_threads),
            fs: fs,
        }
    }
}

impl Deref for PCatFS {
    type Target = CatFS;

    fn deref(&self) -> &CatFS {
        &self.fs
    }
}

macro_rules! run_in_threadpool {
    ($( fn $name:ident(&mut self, _req: &Request, parent: u64, name: &OsStr, $($arg:ident : $argtype:ty),* $(,)*) $body:block )*) => (
        $(
            fn $name(&mut self, _req: &Request, parent: u64, name: &OsStr, $($arg : $argtype),*) {
                let s = make_self(self);
                let name = name.to_os_string();
                self.tp.execute(
                    move || {
                        s.fs.$name(parent, name, $($arg),*);
                        debug!("queue size is {}", s.tp.queued_count());
                    }
                );
            }
        )*
    );
    ($( fn $name:ident(&mut self, _req: &Request, $($arg:ident : $argtype:ty),* $(,)*) $body:block )*) => (
        $(
            fn $name(&mut self, _req: &Request, $($arg : $argtype),*) {
                let s = make_self(self);
                self.tp.execute(
                    move || {
                        s.fs.$name($($arg),*);
                        debug!("queue size is {}", s.tp.queued_count());
                    }
                );
            }
        )*
    );
}

impl Filesystem for PCatFS {
    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let s = make_self(self);
        let data = data.to_vec();
        self.tp.execute(move || {
            s.fs.write(ino, fh, offset, data, _flags, reply);
        });
    }


    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let s = make_self(self);
        let name = name.to_os_string();
        let newname = newname.to_os_string();
        self.tp.execute(move || {
            s.fs.rename(parent, name, newparent, newname, reply);
        });
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        self.fs.forget(ino, nlookup);
    }

    run_in_threadpool!{
        fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        }

        fn setattr(
            &mut self,
            _req: &Request,
            ino: u64,
            mode: Option<u32>,
            uid: Option<u32>,
            gid: Option<u32>,
            size: Option<u64>,
            atime: Option<Timespec>,
            mtime: Option<Timespec>,
            fh: Option<u64>,
            crtime: Option<Timespec>,
            chgtime: Option<Timespec>,
            bkuptime: Option<Timespec>,
            flags: Option<u32>,
            reply: ReplyAttr,
        ) {
        }

        fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        }

        fn readdir(
            &mut self,
            _req: &Request,
            _ino: u64,
            dh: u64,
            offset: i64,
            reply: ReplyDirectory,
        ) {
        }

        fn releasedir(&mut self, _req: &Request, _ino: u64, dh: u64, _flags: u32, reply: ReplyEmpty) {
        }

        fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        }

        fn read(
            &mut self,
            _req: &Request,
            _ino: u64,
            fh: u64,
            offset: i64,
            size: u32,
            reply: ReplyData,
        ) {
        }

        fn flush(&mut self, _req: &Request, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
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
        }

        fn statfs(&mut self, _req: &Request, ino: u64, reply: ReplyStatfs) {
        }
    }

    run_in_threadpool!{
        fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
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
        }

        fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        }


        fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        }

        fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        }
    }
}
