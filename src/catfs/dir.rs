extern crate libc;

use std::path::Path;

use catfs::error;
use catfs::rlibc;

pub struct Handle {
    dh: *mut libc::DIR,
    offset: u64,
    entry: rlibc::Dirent,
    entry_valid: bool,
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
        return Ok(Handle {
            dh: dh,
            offset: 0,
            entry: Default::default(),
            entry_valid: false,
        });
    }

    pub fn seekdir(&mut self, offset: u64) {
        if offset != self.offset {
            debug!(
                "seeking {} to {}",
                unsafe { libc::telldir(self.dh) },
                offset
            );
            rlibc::seekdir(self.dh, offset);
            self.offset = offset;
            self.entry_valid = false;
        }
    }

    pub fn push(&mut self, en: rlibc::Dirent) {
        self.entry = en;
        self.entry_valid = true;
    }

    pub fn consumed(&mut self, en: &rlibc::Dirent) {
        self.offset = en.off();
        self.entry_valid = false;
    }

    pub fn readdir(&mut self) -> error::Result<Option<rlibc::Dirent>> {
        if self.entry_valid {
            return Ok(Some(self.entry.clone()));
        } else {
            match rlibc::readdir(self.dh)? {
                Some(entry) => {
                    return Ok(Some(entry));
                }
                None => return Ok(None),
            }
        }
    }
}
