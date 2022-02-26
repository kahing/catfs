extern crate fuser;
extern crate libc;
extern crate threadpool;

use self::threadpool::ThreadPool;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::io;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use catfs::dir;
use catfs::error;
use catfs::file;
use catfs::rlibc;
use catfs::rlibc::File;

#[derive(Clone)]
pub struct Inode {
    src_dir: RawFd,
    cache_dir: RawFd,

    name: OsString,
    path: PathBuf,

    attr: fuser::FileAttr,
    time: SystemTime,
    cache_valid_if_present: bool,
    flush_failed: bool,

    refcnt: u64,
}

fn to_filetype(t: libc::mode_t) -> fuser::FileType {
    match t & libc::S_IFMT {
        libc::S_IFLNK => fuser::FileType::Symlink,
        libc::S_IFREG => fuser::FileType::RegularFile,
        libc::S_IFBLK => fuser::FileType::BlockDevice,
        libc::S_IFDIR => fuser::FileType::Directory,
        libc::S_IFCHR => fuser::FileType::CharDevice,
        libc::S_IFIFO => fuser::FileType::NamedPipe,
        v => panic!("unknown type: {}", v),
    }
}


impl Inode {
    pub fn new(
        src_dir: RawFd,
        cache_dir: RawFd,
        name: OsString,
        path: PathBuf,
        attr: fuser::FileAttr,
    ) -> Inode {
        return Inode {
            src_dir: src_dir,
            cache_dir: cache_dir,
            name: name,
            path: path,
            attr: attr,
            time: SystemTime::now(),
            cache_valid_if_present: false,
            flush_failed: false,
            refcnt: 1,
        };
    }

    pub fn take(&mut self, other: Inode) {
        self.attr = other.attr;
        self.time = other.time;
    }

    pub fn not_expired(&self, ttl: &Duration) -> bool {
        SystemTime::now() > self.time + *ttl
    }

    pub fn get_child_name(&self, name: &OsStr) -> PathBuf {
        let mut path = self.path.clone();
        path.push(name);
        return path;
    }

    pub fn get_path(&self) -> &Path {
        return &self.path;
    }

    pub fn get_attr(&self) -> &fuser::FileAttr {
        return &self.attr;
    }

    pub fn get_kind(&self) -> fuser::FileType {
        return self.attr.kind;
    }

    pub fn get_ino(&self) -> u64 {
        return self.attr.ino;
    }

    pub fn extend(&mut self, offset: u64) {
        if self.attr.size < offset {
            self.attr.size = offset;
        }
    }

    pub fn lookup_path(dir: RawFd, path: &dyn AsRef<Path>) -> io::Result<fuser::FileAttr> {
        let st = rlibc::fstatat(dir, path)?;
        let attr = fuser::FileAttr {
            ino: st.st_ino,
            size: st.st_size as u64,
            blocks: st.st_blocks as u64,
            atime: SystemTime::UNIX_EPOCH + Duration::from_secs(st.st_atime as u64),
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(st.st_mtime as u64),
            ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(st.st_ctime as u64),
            crtime: SystemTime::UNIX_EPOCH + Duration::from_secs(st.st_ctime as u64),
            kind: to_filetype(st.st_mode),
            perm: (st.st_mode & !libc::S_IFMT) as u16,
            nlink: st.st_nlink as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            rdev: st.st_rdev as u32,
            blksize: st.st_blksize as u32,
            flags: 0,
            padding: 0
        };
        return Ok(attr);
    }

    pub fn flushed(&mut self) {
        // we know that this file really exist now, demand more from the pristineness
        self.cache_valid_if_present = false;
        self.flush_failed = false;
    }

    pub fn refresh(&mut self) -> error::Result<()> {
        match Inode::lookup_path(self.src_dir, &self.path) {
            Ok(attr) => self.attr = attr,
            Err(e) => {
                if error::is_enoent(&e) {
                    return Err(error::RError::propagate(e));
                } else {
                    return Err(error::RError::from(e));
                }
            }
        }

        return Ok(());
    }

    pub fn flush_failed(&mut self) {
        // we know that flush failed, demand more from the pristineness
        self.cache_valid_if_present = false;
        self.flush_failed = true;
    }

    pub fn was_flush_failed(&self) -> bool {
        self.flush_failed
    }

    pub fn lookup(&self, name: &OsStr) -> error::Result<Inode> {
        let path = self.get_child_name(name);
        match Inode::lookup_path(self.src_dir, &path) {
            Ok(attr) => {
                return Ok(Inode::new(
                    self.src_dir,
                    self.cache_dir,
                    name.to_os_string(),
                    path,
                    attr,
                ))
            }
            Err(e) => return error::propagate(e),
        }
    }

    pub fn create(&self, name: &OsStr, mode: libc::mode_t) -> error::Result<(Inode, file::Handle)> {
        let path = self.get_child_name(name);

        let flags = rlibc::O_WRONLY | rlibc::O_CREAT | rlibc::O_EXCL;

        let wh = file::Handle::create(self.src_dir, self.cache_dir, &path, flags, mode)?;

        let attr = Inode::lookup_path(self.src_dir, &path)?;
        let mut inode = Inode::new(
            self.src_dir,
            self.cache_dir,
            name.to_os_string(),
            path,
            attr,
        );
        // we just created this file, it's gotta be valid
        inode.cache_valid_if_present = true;

        return Ok((inode, wh));
    }

    pub fn open(&mut self, flags: u32, tp: &Mutex<ThreadPool>) -> error::Result<file::Handle> {
        let f = file::Handle::open(
            self.src_dir,
            self.cache_dir,
            &self.path,
            flags,
            self.cache_valid_if_present,
            self.flush_failed,
            tp,
        )?;
        // Handle::open deletes the cache file if it was invalid, so
        // at this point it must be valid, even after we start writing to it
        self.cache_valid_if_present = true;
        return Ok(f);
    }

    pub fn reopen_src(&self, file: &mut file::Handle) -> error::Result<()> {
        file.reopen_src(self.src_dir, &self.path, self.cache_valid_if_present)
    }

    pub fn unlink(&self, name: &OsStr) -> io::Result<()> {
        return file::Handle::unlink(self.src_dir, self.cache_dir, &self.get_child_name(name));
    }

    pub fn rename(&mut self, new_name: &OsStr, new_path: &dyn AsRef<Path>) -> error::Result<()> {
        // XXX emulate some sort of atomicity

        // rename src first because if it's a directory, underlining
        // filesystem may reject if it's non-empty, where as if it's
        // the cache it may not contain anything or may even not exist
        rlibc::renameat(self.src_dir, &self.path, new_path)?;
        // source is renamed and now rename what's in the
        // cache. If things fail here we are inconsistent. XXX
        // delete cache path (could be a dir) if we failed to
        // rename it
        if rlibc::existat(self.cache_dir, &self.path)? {
            if let Some(parent) = new_path.as_ref().parent() {
                file::mkdirat_all(self.cache_dir, &parent, 0o777)?;
            }
            rlibc::renameat(self.cache_dir, &self.path, new_path)?;
        }

        self.name = new_name.to_os_string();
        self.path = new_path.as_ref().to_path_buf();
        return Ok(());
    }

    pub fn truncate(&mut self, size: u64) -> error::Result<()> {
        let mut f = File::openat(self.src_dir, &self.path, rlibc::O_WRONLY, 0)?;
        f.set_size(size)?;
        f.close()?;

        match File::openat(self.cache_dir, &self.path, rlibc::O_WRONLY, 0) {
            Ok(mut f) => {
                f.set_size(size)?;
                f.close()?;
            }
            Err(e) => {
                error::try_enoent(e)?;
            }
        }

        return Ok(());
    }

    pub fn utimes(&self, atime: &SystemTime, mtime: &SystemTime, flags: u32) -> io::Result<()> {
        rlibc::utimensat(self.src_dir, &self.path, atime, mtime, flags)
    }

    pub fn chmod(&self, mode: libc::mode_t, flags: u32) -> io::Result<()> {
        rlibc::fchmodat(self.src_dir, &self.path, mode, flags)?;
        return Ok(());
    }

    pub fn mkdir(&self, name: &OsStr, mode: libc::mode_t) -> error::Result<Inode> {
        let path = self.get_child_name(name);

        rlibc::mkdirat(self.src_dir, &path, mode)?;

        let attr = Inode::lookup_path(self.src_dir, &path)?;
        let inode = Inode::new(
            self.src_dir,
            self.cache_dir,
            name.to_os_string(),
            path,
            attr,
        );

        return Ok(inode);
    }

    pub fn rmdir(&self, name: &OsStr) -> io::Result<()> {
        return dir::Handle::rmdirat(self.src_dir, self.cache_dir, &self.get_child_name(name));
    }

    pub fn opendir(&self) -> error::Result<dir::Handle> {
        return dir::Handle::openat(self.src_dir, &self.path);
    }

    pub fn use_ino(&mut self, ino: u64) {
        self.attr.ino = ino;
    }

    pub fn inc_ref(&mut self) -> u64 {
        self.refcnt += 1;
        return self.refcnt;
    }

    pub fn get_refcnt(&self) -> u64 {
        return self.refcnt;
    }

    // return stale
    pub fn deref(&mut self, n: u64) -> bool {
        if self.refcnt < n {
            panic!(
                "ino 0x{:016x} refcnt {} deref {}",
                self.attr.ino,
                self.refcnt,
                n
            );
        }
        self.refcnt -= n;
        return self.refcnt == 0;
    }
}
