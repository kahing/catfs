extern crate fuser;
extern crate libc;
extern crate xattr;

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fmt;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::io;
use std::mem::MaybeUninit;
use std::path::Path;
use std::ptr;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::os::unix::fs::FileExt;
use std::time::SystemTime;

use self::fuser::FileType;
use self::xattr::FileExt as XattrFileExt;

#[cfg(not(any(target_os = "macos", target_pointer_width = "32")))]
use self::libc::{fstat64, fstatvfs64, ftruncate64, open64, openat64, pread64, pwrite64, stat64, statvfs64};
#[cfg(any(target_os = "macos", target_pointer_width = "32"))]
use self::libc::{fstat as fstat64, fstatvfs as fstatvfs64, ftruncate as ftruncate64, open as open64, openat as openat64, pread as pread64, pwrite as pwrite64, stat as stat64, statvfs as statvfs64};

use catfs::error;
use catfs::error::RError;

// libc defines these as i32 which means they can't naturally be OR'ed
// with u32
pub static O_ACCMODE: u32 = libc::O_ACCMODE as u32;
pub static O_RDONLY: u32 = libc::O_RDONLY as u32;
pub static O_WRONLY: u32 = libc::O_WRONLY as u32;
pub static O_RDWR: u32 = libc::O_RDWR as u32;

pub static O_CLOEXEC: u32 = libc::O_CLOEXEC as u32;
pub static O_CREAT: u32 = libc::O_CREAT as u32;
pub static O_EXCL: u32 = libc::O_EXCL as u32;
// XXX for some reason this is not found
//pub static O_PATH: u32 = libc::O_PATH as u32;
#[allow(dead_code)]
pub static O_PATH: u32 = 2097152;
pub static O_TRUNC: u32 = libc::O_TRUNC as u32;

pub fn to_cstring(path: &dyn AsRef<Path>) -> CString {
    let bytes = path.as_ref().as_os_str().to_os_string().into_vec();
    return CString::new(bytes).unwrap();
}

macro_rules! libc_wrap {
    ($( pub fn $name:ident($($arg:ident : $argtype:ty),*) $body:block )*) => (
        $(
            pub fn $name($($arg : $argtype),*) -> io::Result<()> {
                let err: libc::c_int;
                unsafe { err = libc::$name($($arg),*) }
                match err {
                    0 => return Ok(()),
                    _ => return Err(io::Error::last_os_error()),
                }
            }
        )*
    );
}

libc_wrap!{
    pub fn setuid(uid: libc::uid_t) {}
    pub fn setgid(gid: libc::gid_t) {}
}

pub fn opendir(path: &dyn AsRef<Path>) -> io::Result<*mut libc::DIR> {
    let s = to_cstring(path);
    let dh = unsafe { libc::opendir(s.as_ptr()) };
    if dh.is_null() {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(dh);
    }
}

pub fn fdopendir(fd: RawFd) -> io::Result<*mut libc::DIR> {
    let dh = unsafe { libc::fdopendir(fd) };
    if dh.is_null() {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(dh);
    }
}

pub fn closedir(dir: *mut libc::DIR) -> io::Result<()> {
    let err: libc::c_int;
    unsafe { err = libc::closedir(dir) }
    match err {
        0 => return Ok(()),
        _ => return Err(io::Error::last_os_error()),
    }
}

pub fn seekdir(dir: *mut libc::DIR, loc: i64) {
    unsafe {
        libc::seekdir(dir, loc as libc::c_long);
    }
}

#[derive(Clone)]
pub struct Dirent {
    pub en: libc::dirent,
}

impl Default for Dirent {
    #[cfg(not(target_os = "macos"))]
    fn default() -> Dirent {
        return Dirent {
            en: libc::dirent {
                d_ino: 0,
                d_off: 0,
                d_reclen: 0,
                d_type: libc::DT_REG,
                d_name: [0i8 as libc::c_char; 256], // FIXME: don't hardcode 256
            },
        };
    }
    #[cfg(target_os = "macos")]
    fn default() -> Dirent {
        return Dirent {
            en: libc::dirent {
                d_ino: 0,
                d_seekoff: 0,
                d_reclen: 0,
                d_type: libc::DT_REG,
                d_name: [0i8; 1024], // FIXME: don't hardcode 1024
                d_namlen: 0,
            },
        };
    }
}

impl fmt::Debug for Dirent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ino: {} type: {:?} name: {:?}",
            self.ino(),
            self.kind(),
            self.name()
        )
    }
}

fn array_to_osstring(cslice: &[libc::c_char]) -> OsString {
    let s = unsafe { CStr::from_ptr(cslice.as_ptr()) };
    return OsStr::from_bytes(s.to_bytes()).to_os_string();
}

impl Dirent {
    pub fn ino(&self) -> u64 {
        return self.en.d_ino as u64;
    }
    pub fn off(&self) -> i64 {
        #[cfg(not(target_os = "macos"))]
        return self.en.d_off as i64;
        #[cfg(target_os = "macos")]
        return self.en.d_seekoff as i64;
    }
    pub fn kind(&self) -> fuser::FileType {
        match self.en.d_type {
            libc::DT_BLK => return FileType::BlockDevice,
            libc::DT_CHR => return FileType::CharDevice,
            libc::DT_DIR => return FileType::Directory,
            libc::DT_FIFO => return FileType::NamedPipe,
            libc::DT_LNK => return FileType::Symlink,
            _ => return FileType::RegularFile,
        }
    }
    pub fn name(&self) -> OsString {
        return array_to_osstring(&self.en.d_name);
    }
}

pub fn readdir(dir: *mut libc::DIR) -> io::Result<Option<Dirent>> {
    let mut entry_p = MaybeUninit::<libc::dirent>::uninit();
    let mut entry_pp = ptr::null_mut();

    let err = unsafe { libc::readdir_r(dir, entry_p.as_mut_ptr(), &mut entry_pp) };
    if err == 0 {
        if entry_pp == ptr::null_mut() {
            return Ok(None);
        } else {
            return Ok(Some(Dirent { en: unsafe { entry_p.assume_init() } }));
        }
    } else {
        return Err(io::Error::last_os_error());
    }
}

pub fn mkdir(path: &dyn AsRef<Path>, mode: libc::mode_t) -> io::Result<()> {
    let s = to_cstring(path);
    let res = unsafe { libc::mkdir(s.as_ptr(), mode) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(());
    }
}

pub fn mkdirat(dir: RawFd, path: &dyn AsRef<Path>, mode: libc::mode_t) -> io::Result<()> {
    let s = to_cstring(path);
    let res = unsafe { libc::mkdirat(dir, s.as_ptr(), mode) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(());
    }
}

#[cfg(not(target_os = "macos"))]
pub fn pipe() -> io::Result<(libc::c_int, libc::c_int)> {
    let mut p = [0; 2];
    let res = unsafe { libc::pipe2(p.as_mut_ptr(), libc::O_CLOEXEC) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok((p[0], p[1]));
    }
}

#[cfg(not(target_os = "macos"))]
pub fn splice(
    fd: libc::c_int,
    off_self: i64,
    other: libc::c_int,
    off_other: i64,
    len: usize,
) -> io::Result<usize> {
    let mut off_from = off_self;
    let mut off_to = off_other;

    let off_from_ptr = if off_from == -1 {
        ptr::null()
    } else {
        &mut off_from
    } as *mut i64;
    let off_to_ptr = if off_to == -1 {
        ptr::null()
    } else {
        &mut off_to
    } as *mut i64;

    let res = unsafe { libc::splice(fd, off_from_ptr, other, off_to_ptr, len, 0) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(res as usize);
    }
}

pub fn close(fd: libc::c_int) -> io::Result<()> {
    let res = unsafe { libc::close(fd) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(());
    }
}

pub fn unlinkat(dir: RawFd, path: &dyn AsRef<Path>, flags: u32) -> io::Result<()> {
    let s = to_cstring(path);
    let res = unsafe { libc::unlinkat(dir, s.as_ptr(), flags as i32) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(());
    }
}

pub fn existat(dir: RawFd, path: &dyn AsRef<Path>) -> error::Result<bool> {
    if let Err(e) = fstatat(dir, path) {
        if error::try_enoent(e)? {
            return Ok(false);
        }
    }

    return Ok(true);
}

pub fn renameat(dir: RawFd, path: &dyn AsRef<Path>, newpath: &dyn AsRef<Path>) -> error::Result<()> {
    let s = to_cstring(path);
    let new_s = to_cstring(newpath);

    let res = unsafe { libc::renameat(dir, s.as_ptr(), dir, new_s.as_ptr()) };
    if res < 0 {
        // rename(2): "On NFS filesystems, you can not assume that
        // if the operation failed, the file was not renamed"
        if existat(dir, path)? {
            // rename actually worked
            return Ok(());
        } else {
            return Err(RError::from(io::Error::last_os_error()));
        }
    } else {
        return Ok(());
    }
}

pub fn fstat(fd: libc::c_int) -> io::Result<stat64> {
    let mut st = MaybeUninit::<stat64>::uninit();

    let res = unsafe { fstat64(fd, st.as_mut_ptr()) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(unsafe { st.assume_init() });
    }
}

pub fn fstatat(dir: RawFd, path: &dyn AsRef<Path>) -> io::Result<stat64> {
    let mut st = MaybeUninit::<stat64>::uninit();
    let s = to_cstring(path);

    #[cfg(not(target_os = "macos"))]
    let res = unsafe { libc::fstatat64(dir, s.as_ptr(), st.as_mut_ptr(), libc::AT_EMPTY_PATH) };
    #[cfg(target_os = "macos")]
    let res = unsafe { libc::fstatat(dir, s.as_ptr(), st.as_mut_ptr(), 0) };

    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(unsafe { st.assume_init() });
    }
}

pub fn fstatvfs(fd: RawFd) -> io::Result<statvfs64> {
    let mut st = MaybeUninit::<statvfs64>::uninit();
    let res = unsafe { fstatvfs64(fd, st.as_mut_ptr()) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(unsafe { st.assume_init() });
    }
}

pub fn openat(dir: RawFd, path: &dyn AsRef<Path>, flags: u32, mode: libc::mode_t) -> io::Result<RawFd> {
    let s = to_cstring(path);
    let fd = unsafe { openat64(dir, s.as_ptr(), (flags | O_CLOEXEC) as i32, mode as libc::c_uint) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(fd);
    }
}

#[allow(dead_code)]
pub fn utimes(path: &dyn AsRef<Path>, atime: libc::time_t, mtime: libc::time_t) -> io::Result<()> {
    let s = to_cstring(path);
    let atv = libc::timeval { tv_sec: atime, tv_usec: 0 };
    let mtv = libc::timeval { tv_sec: mtime, tv_usec: 0 };
    let res = unsafe { libc::utimes(s.as_ptr(), [atv, mtv].as_ptr()) };
    if res == 0 {
        return Ok(());
    } else {
        return Err(io::Error::last_os_error());
    }
}

pub fn utimensat(
    dir: RawFd,
    path: &dyn AsRef<Path>,
    atime: &SystemTime,
    mtime: &SystemTime,
    flags: u32,
) -> io::Result<()> {
    let s = to_cstring(path);
    let mut times = [
        libc::timespec {
            tv_sec: atime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as libc::time_t,
            tv_nsec: atime.duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos() as libc::c_long,
        },
        libc::timespec {
            tv_sec: mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as libc::time_t,
            tv_nsec: mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap().subsec_nanos() as libc::c_long,
        },
    ];

    let res = unsafe { libc::utimensat(dir, s.as_ptr(), times.as_mut_ptr(), flags as i32) };
    if res == 0 {
        return Ok(());
    } else {
        return Err(io::Error::last_os_error());
    }
}

pub fn fchmodat(dir: RawFd, path: &dyn AsRef<Path>, mode: libc::mode_t, flags: u32) -> io::Result<()> {
    let s = to_cstring(path);
    let res = unsafe { libc::fchmodat(dir, s.as_ptr(), mode, flags as i32) };
    if res == 0 {
        return Ok(());
    } else {
        return Err(io::Error::last_os_error());
    }
}

pub struct File {
    fd: libc::c_int,
}

fn as_void_ptr<T>(s: &[T]) -> *const libc::c_void {
    return s.as_ptr() as *const libc::c_void;
}

fn as_mut_void_ptr<T>(s: &mut [T]) -> *mut libc::c_void {
    return s.as_mut_ptr() as *mut libc::c_void;
}

pub fn open(path: &dyn AsRef<Path>, flags: u32, mode: u32) -> io::Result<RawFd> {
    let s = to_cstring(path);
    let fd = unsafe { open64(s.as_ptr(), (flags | O_CLOEXEC) as i32, mode as libc::c_uint) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    } else {
        return Ok(fd);
    }
}

impl File {
    pub fn openat(dir: RawFd, path: &dyn AsRef<Path>, flags: u32, mode: libc::mode_t) -> io::Result<File> {
        let fd = openat(dir, path, flags, mode)?;
        debug!(
            "<-- openat {:?} {:b} {:#o} = {}",
            path.as_ref(),
            flags,
            mode,
            fd
        );
        return Ok(File { fd: fd });
    }

    #[allow(dead_code)]
    pub fn open(path: &dyn AsRef<Path>, flags: u32, mode: u32) -> io::Result<File> {
        let fd = open(path, flags, mode)?;
        debug!(
            "<-- open {:?} {:b} {:#o} = {}",
            path.as_ref(),
            flags,
            mode,
            fd
        );
        return Ok(File { fd: fd });
    }

    pub fn with_fd(fd: libc::c_int) -> File {
        return File { fd: fd };
    }

    pub fn valid(&self) -> bool {
        return self.fd != -1;
    }

    pub fn filesize(&self) -> io::Result<u64> {
        let st = fstat(self.fd)?;
        return Ok(st.st_size as u64);
    }

    pub fn stat(&self) -> io::Result<stat64> {
        fstat(self.fd)
    }

    pub fn truncate(&self, size: u64) -> io::Result<()> {
        let res = unsafe { ftruncate64(self.fd, size as i64) };
        if res < 0 {
            return Err(io::Error::last_os_error());
        } else {
            return Ok(());
        }
    }

    #[cfg(not(target_os = "macos"))]
    pub fn allocate(&self, offset: u64, len: u64) -> io::Result<()> {
        let res = unsafe { libc::posix_fallocate64(self.fd, offset as i64, len as i64) };
        if res == 0 {
            return Ok(());
        } else {
            return Err(io::Error::from_raw_os_error(res));
        }
    }

    #[cfg(target_os = "macos")]
    pub fn allocate(&self, offset: u64, len: u64) -> io::Result<()> {
        self.truncate(offset + len)
    }

    #[allow(dead_code)]
    pub fn set_size(&self, size: u64) -> error::Result<()> {
        let old_size = self.filesize()?;

        if let Err(e) = self.truncate(size) {
            if size > old_size && e.raw_os_error().unwrap() == libc::EPERM {
                self.allocate(old_size as u64, size - old_size)?;
            } else {
                return Err(RError::from(e));
            }
        }

        return Ok(());
    }

    pub fn chmod(&self, mode: libc::mode_t) -> io::Result<()> {
        let res = unsafe { libc::fchmod(self.fd, mode) };
        if res == 0 {
            return Ok(());
        } else {
            return Err(io::Error::from_raw_os_error(res));
        }
    }

    pub fn read_at(&self, buf: &mut [u8], offset: i64) -> io::Result<usize> {
        let nbytes =
            unsafe { pread64(self.fd, as_mut_void_ptr(buf), buf.len(), offset) };
        if nbytes < 0 {
            return Err(io::Error::last_os_error());
        } else {
            return Ok(nbytes as usize);
        }
    }

    pub fn write_at(&self, buf: &[u8], offset: i64) -> io::Result<usize> {
        let nbytes = unsafe { pwrite64(self.fd, as_void_ptr(buf), buf.len(), offset) };
        if nbytes < 0 {
            return Err(io::Error::last_os_error());
        } else {
            return Ok(nbytes as usize);
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        debug!("flush {}", self.fd);
        // trigger a flush for the underly fd, this could be called
        // multiple times, for ex:
        //
        // int fd2 = dup(fd); close(fd2); close(fd)
        //
        // so the fd needs to stay valid. Note that this means when an
        // application sends close(), kernel will send us
        // flush()/release(), and we will send close()/close(), which
        // will be translated to flush()/flush()/release() to the
        // underlining filesystem
        let fd = unsafe { libc::dup(self.fd) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        } else {
            let res = unsafe { libc::close(fd) };
            if res < 0 {
                return Err(io::Error::last_os_error());
            } else {
                return Ok(());
            }
        }
    }

    pub fn close(&mut self) -> io::Result<()> {
        let res = unsafe { libc::close(self.fd) };
        self.fd = -1;
        if res < 0 {
            return Err(io::Error::last_os_error());
        } else {
            return Ok(());
        }
    }

    pub fn as_raw_fd(&self) -> RawFd {
        if !self.valid() {
            error!("as_raw_fd called on invalid fd");
        }

        return self.fd;
    }

    pub fn into_raw(&mut self) -> RawFd {
        let fd = self.fd;
        self.fd = -1;
        fd
    }
}

impl Default for File {
    fn default() -> File {
        File { fd: -1 }
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if self.fd != -1 {
            error!(
                "{} dropped but not closed: {}",
                self.fd,
                RError::from(io::Error::from_raw_os_error(libc::EIO))
            );
            if let Err(e) = self.close() {
                error!("!close({}) = {}", self.fd, RError::from(e));
            }
        }
    }
}

impl FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        File::read_at(self, buf, offset as i64)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        File::write_at(self, buf, offset as i64)
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        File::as_raw_fd(self)
    }
}

impl XattrFileExt for File {}
