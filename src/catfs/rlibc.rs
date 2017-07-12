extern crate fuse;
extern crate libc;

use std::ffi::{CStr, CString, OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::io;
use self::fuse::FileType;
use std::path::Path;

pub fn to_cstring(path: &AsRef<Path>) -> CString {
    let bytes = path.as_ref().as_os_str().to_os_string().into_vec();
    return CString::new(bytes).unwrap();
}

pub fn opendir(path: &AsRef<Path>) -> io::Result<*mut libc::DIR> {
    let s = to_cstring(path);
    let dh = unsafe { libc::opendir(s.as_ptr()) };
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

pub fn seekdir(dir: *mut libc::DIR, loc: u64) {
    unsafe {
        libc::seekdir(dir, loc as i64);
    }
}

pub struct Dirent {
    en: libc::dirent,
}

fn array_to_osstring(cslice: &[libc::c_char]) -> OsString {
    let s = unsafe { CStr::from_ptr(cslice.as_ptr()) };
    return OsStr::from_bytes(s.to_bytes()).to_os_string();
}


impl Dirent {
    pub fn ino(&self) -> u64 {
        return self.en.d_ino;
    }
    pub fn off(&self) -> u64 {
        return self.en.d_off as u64;
    }
    pub fn kind(&self) -> fuse::FileType {
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
    let mut entry: libc::dirent = libc::dirent {
        d_ino: 0,
        d_off: 0,
        d_reclen: 0,
        d_type: libc::DT_REG,
        d_name: [0i8; 256], // FIXME: don't hardcode 256
    };
    let mut entry_p: *mut libc::dirent = &mut entry;
    let entry_pp: *mut *mut libc::dirent = &mut entry_p;

    unsafe {
        let err = libc::readdir_r(dir, entry_p, entry_pp);
        if err == 0 {
            if (*entry_pp).is_null() {
                return Ok(None);
            } else {
                return Ok(Some(Dirent { en: entry }));
            }
        } else {
            return Err(io::Error::last_os_error());
        }
    }
}
