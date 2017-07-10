extern crate libc;

use std::ffi::OsStr;
use std::io;

use catfs::rlibc;

pub struct DirHandle {
    dh: *mut libc::DIR,
    offset: u64,
}

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for DirHandle {}

impl Drop for DirHandle {
    fn drop(&mut self) {
        rlibc::closedir(self.dh);
    }
}

impl DirHandle {
    pub fn open(path: &OsStr) -> io::Result<DirHandle> {
        let dh = rlibc::opendir(path)?;
        return Ok(DirHandle { dh: dh, offset: 0 });
    }

    pub fn seekdir(&mut self, offset: u64) {
        if offset != self.offset {
            rlibc::seekdir(self.dh, offset);
            self.offset = offset;
        }
    }

    pub fn readdir(&mut self) -> io::Result<Option<rlibc::Dirent>> {
        match rlibc::readdir(self.dh)? {
            Some(entry) => {
                self.offset = entry.off();
                return Ok(Some(entry));
            }
            None => return Ok(None),
        }
    }
}
