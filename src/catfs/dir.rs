extern crate libc;

use std::path::Path;

use catfs::error;
use catfs::rlibc;

pub struct Handle {
    dh: *mut libc::DIR,
    offset: u64,
}

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Err(e) = rlibc::closedir(self.dh) {
            error!("!closedir {:?} = {}", self.dh, e);
        }
    }
}

impl Handle {
    pub fn open(path: &AsRef<Path>) -> error::Result<Handle> {
        let dh = rlibc::opendir(path)?;
        return Ok(Handle { dh: dh, offset: 0 });
    }

    pub fn seekdir(&mut self, offset: u64) {
        if offset != self.offset {
            rlibc::seekdir(self.dh, offset);
            self.offset = offset;
        }
    }

    pub fn readdir(&mut self) -> error::Result<Option<rlibc::Dirent>> {
        match rlibc::readdir(self.dh)? {
            Some(entry) => {
                self.offset = entry.off();
                return Ok(Some(entry));
            }
            None => return Ok(None),
        }
    }
}
