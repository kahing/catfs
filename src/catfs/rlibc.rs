extern crate fuse;
extern crate libc;

use std::slice;
use std::ffi::CString;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::io;
use fuse::FileType;

pub fn opendir(path: &OsStr) -> io::Result<*mut libc::DIR> {
    unsafe {
        let mut dh = libc::opendir(path.as_bytes().as_ptr() as *const libc::c_char);
        if dh.is_null() {
            return Err(io::Error::last_os_error());
        } else {
            return Ok(dh);
        }
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

pub struct dirent {
    en: libc::dirent,
}

fn array_to_osstring(cslice: &[libc::c_char]) -> OsString {
    let cdata: *const u8 = cslice.as_ptr() as *const u8;
    let slice = unsafe { slice::from_raw_parts(cdata, cslice.len()) };
    return OsStr::from_bytes(slice).to_os_string();
}


impl dirent {
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

pub fn readdir(dir: *mut libc::DIR) -> io::Result<Option<dirent>> {
    let mut entry: libc::dirent = libc::dirent {
        d_ino: 0,
        d_off: 0,
        d_reclen: 0,
        d_type: libc::DT_REG,
        d_name: [0i8; 256], // FIXME: don't hardcode 256
    };
    let mut entry_p: *mut libc::dirent = &mut entry;
    let mut entry_pp: *mut *mut libc::dirent = &mut entry_p;

    unsafe {
        let err = libc::readdir_r(dir, entry_p, entry_pp);
        if err == 0 {
            if (*entry_pp).is_null() {
                return Ok(None);
            } else {
                return Ok(Some(dirent { en: entry }));
            }
        } else {
            return Err(io::Error::last_os_error());
        }
    }
}
