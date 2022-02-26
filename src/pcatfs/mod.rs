extern crate fuser;
extern crate threadpool;

use self::fuser::{
    Filesystem,
    Request,
    ReplyEntry,
    ReplyAttr,
    ReplyOpen,
    ReplyEmpty,ReplyDirectory,
    ReplyData,
    ReplyWrite,
    ReplyCreate,
    ReplyStatfs,
    TimeOrNow
};

//use self::fuser::ll::TimeOrNow;

use self::threadpool::ThreadPool;

use std::ffi::OsStr;
use std::ops::Deref;
use std::time::SystemTime;


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
    pub fn new(fs: CatFS) -> PCatFS {
        PCatFS {
            tp: ThreadPool::new(100),
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
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
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
        _flags: u32,
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
    }

    run_in_threadpool!{
        fn setattr(
            &mut self,
            _req: &Request,
            ino: u64,
            mode: Option<u32>,
            uid: Option<u32>,
            gid: Option<u32>,
            size: Option<u64>,
            atime: Option<TimeOrNow>,
            mtime: Option<TimeOrNow>,
            ctime: Option<SystemTime>,
            fh: Option<u64>,
            crtime: Option<SystemTime>,
            chgtime: Option<SystemTime>,
            bkuptime: Option<SystemTime>,
            flags: Option<u32>,
            reply: ReplyAttr,
        ) {
        }
    }

    run_in_threadpool!{
        fn opendir(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        }
    }

    run_in_threadpool!{
        fn readdir(
            &mut self,
            _req: &Request,
            _ino: u64,
            dh: u64,
            offset: i64,
            reply: ReplyDirectory,
        ) {
        }
    }

    run_in_threadpool!{
        fn releasedir(&mut self, _req: &Request, _ino: u64, dh: u64, _flags: i32, reply: ReplyEmpty) {
        }
    }

    run_in_threadpool!{
        fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        }
    }

    run_in_threadpool!{
        fn read(
            &mut self,
            _req: &Request,
            _ino: u64,
            fh: u64,
            offset: i64,
            size: u32,
            flags: i32,
            lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
        }
    }

    run_in_threadpool!{
        fn flush(&mut self, _req: &Request, ino: u64, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        }
    }

    run_in_threadpool!{
        fn release(
            &mut self,
            _req: &Request,
            _ino: u64,
            fh: u64,
            _flags: i32,
            _lock_owner: Option<u64>,
            _flush: bool,
            reply: ReplyEmpty,
        ) {
        }
    }

    run_in_threadpool!{
        fn statfs(&mut self, _req: &Request, ino: u64, reply: ReplyStatfs) {
        }
    }

    run_in_threadpool!{
        fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        }
    }

    run_in_threadpool!{
        fn create(
            &mut self,
            _req: &Request,
            parent: u64,
            name: &OsStr,
            mode: u32,
            umask: u32,
            flags: i32,
            reply: ReplyCreate,
        ) {
        }
    }

    run_in_threadpool!{
        fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        }
    }

    run_in_threadpool!{
        fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        }
    }

    run_in_threadpool!{
        fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        }
    }
}
