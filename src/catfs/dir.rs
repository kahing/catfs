extern crate libc;

use std::fs;
use std::io;
use std::os::unix::io::RawFd;
use std::path::Path;

use catfs::error;
use catfs::rlibc;

pub struct Handle {
    dh: *mut libc::DIR,
    offset: i64,
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

#[allow(dead_code)]
pub fn openpath(path: &dyn AsRef<Path>) -> io::Result<RawFd> {
    rlibc::open(&path, rlibc::O_PATH, 0)
}

impl Handle {
    pub fn openat(dir: RawFd, path: &dyn AsRef<Path>) -> error::Result<Handle> {
        let fd = if path.as_ref() == Path::new("") {
            rlibc::openat(dir, &".", rlibc::O_RDONLY, 0)?
        } else {
            rlibc::openat(dir, &path, rlibc::O_RDONLY, 0)?
        };
        return Ok(Handle {
            dh: rlibc::fdopendir(fd)?,
            offset: 0,
            entry: Default::default(),
            entry_valid: false,
        });
    }

    #[allow(dead_code)]
    pub fn open(path: &dyn AsRef<Path>) -> error::Result<Handle> {
        let dh = rlibc::opendir(&path)?;
        return Ok(Handle {
            dh: dh,
            offset: 0,
            entry: Default::default(),
            entry_valid: false,
        });
    }

    pub fn seekdir(&mut self, offset: i64) {
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

    #[allow(dead_code)]
    pub fn mkdir(path: &dyn AsRef<Path>, mode: libc::mode_t) -> io::Result<()> {
        rlibc::mkdir(path, mode)
    }

    pub fn rmdirat(src_dir: RawFd, cache_dir: RawFd, path: &dyn AsRef<Path>) -> io::Result<()> {
        if let Err(e) = rlibc::unlinkat(cache_dir, path, libc::AT_REMOVEDIR as u32) {
            if !error::is_enoent(&e) {
                return Err(e);
            }
        }

        return rlibc::unlinkat(src_dir, path, libc::AT_REMOVEDIR as u32);
    }

    #[allow(dead_code)]
    pub fn rmdir(src_path: &dyn AsRef<Path>, cache_path: &dyn AsRef<Path>) -> io::Result<()> {
        if let Err(e) = fs::remove_dir(cache_path) {
            if !error::is_enoent(&e) {
                return Err(e);
            }
        }
        return fs::remove_dir(src_path);
    }
}
