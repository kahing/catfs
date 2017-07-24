extern crate libc;
extern crate xattr;

use std::fs;
use std::io;
use std::path::Path;

use self::xattr::FileExt;

use catfs::error;
use catfs::error::RError;
use catfs::rlibc;
use catfs::rlibc::File;

pub struct Handle {
    src_file: File,
    cache_file: File,
    dirty: bool,
}

pub static PRISTINE: [u8; 1] = ['1' as u8];
pub static DIRTY: [u8; 1] = ['0' as u8];

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for Handle {}

fn make_rdwr(f: &mut u32) {
    *f = (*f & !rlibc::O_ACCMODE) | rlibc::O_RDWR;
}

fn maybe_unlink(path: &AsRef<Path>) -> io::Result<()> {
    if let Err(e) = fs::remove_file(path) {
        if !error::is_enoent(&e) {
            return Err(e);
        }
    }
    return Ok(());
}

impl Handle {
    pub fn create(
        src_path: &AsRef<Path>,
        cache_path: &AsRef<Path>,
        flags: u32,
        mode: u32,
    ) -> error::Result<Handle> {
        // need to read the cache file for writeback
        let mut cache_flags = flags.clone();
        if (cache_flags & rlibc::O_ACCMODE) == rlibc::O_WRONLY {
            make_rdwr(&mut cache_flags);
        }
        //debug!("create {:b} {:b} {:#o}", flags, cache_flags, mode);

        if let Some(parent) = cache_path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let src_file = File::open(src_path, flags, mode)?;
        // we are able to create the src file, then the cache file
        // shouldn't be here, but it could be because of bug/crash,
        // so unlink it first
        maybe_unlink(cache_path)?;

        return Ok(Handle {
            src_file: src_file,
            cache_file: File::open(cache_path, cache_flags, mode)?,
            dirty: true,
        });
    }

    pub fn open(
        src_path: &AsRef<Path>,
        cache_path: &AsRef<Path>,
        flags: u32,
        cache_valid_if_present: bool,
    ) -> error::Result<Handle> {
        // even if file is open for write only, I still need to be
        // able to read the src for read-modify-write
        let mut flags = flags;
        if (flags & rlibc::O_ACCMODE) == rlibc::O_WRONLY {
            make_rdwr(&mut flags);
        }

        let valid = Handle::validate_cache(src_path, cache_path, cache_valid_if_present)?;
        debug!(
            "{:?} {} a valid cache file for {:?}",
            cache_path.as_ref(),
            if valid { "is" } else { "is not" },
            src_path.as_ref()
        );
        let mut cache_flags = flags.clone();

        if !valid {
            // mkdir the parents
            if let Some(parent) = cache_path.as_ref().parent() {
                fs::create_dir_all(parent)?;
            }
            // need to cache this file so need to open it for write
            cache_flags |= rlibc::O_CREAT;
            if (cache_flags & rlibc::O_ACCMODE) == rlibc::O_RDONLY {
                make_rdwr(&mut cache_flags);
            }
        }

        let src_file: File;
        if valid && (flags & rlibc::O_ACCMODE) == rlibc::O_RDONLY {
            src_file = Default::default();
        } else {
            src_file = File::open(src_path, flags, 0o666)?;
        }

        let handle = Handle {
            src_file: src_file,
            cache_file: File::open(cache_path, cache_flags, 0o666)?,
            dirty: false,
        };

        if !valid && (flags & rlibc::O_TRUNC) == 0 {
            debug!("read ahead {:?}", src_path.as_ref());
            handle.copy(true)?;
        }

        return Ok(handle);
    }

    pub fn unlink(src_path: &AsRef<Path>, cache_path: &AsRef<Path>) -> io::Result<()> {
        maybe_unlink(cache_path)?;
        return fs::remove_file(src_path);
    }

    fn is_pristine(cache_path: &AsRef<Path>) -> error::Result<bool> {
        if let Some(v) = xattr::get(cache_path, "user.catfs.pristine")? {
            return Ok(v == PRISTINE);
        }
        return Ok(false);
    }

    fn validate_cache(
        src_path: &AsRef<Path>,
        cache_path: &AsRef<Path>,
        cache_valid_if_present: bool,
    ) -> error::Result<bool> {
        match fs::symlink_metadata(src_path) {
            Ok(_) => {
                match fs::symlink_metadata(cache_path) {
                    Ok(_) => {
                        if cache_valid_if_present || Handle::is_pristine(cache_path)? {
                            return Ok(true);
                        } else {
                            fs::remove_file(cache_path)?;
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        if error::try_enoent(e)? {
                            return Ok(false);
                        }
                    }
                }
            }
            Err(e) => {
                if error::try_enoent(e)? {
                    // the source file doesn't exist, the cache file shouldn't either
                    maybe_unlink(cache_path)?;
                }
            }
        }

        return Ok(false);
    }

    pub fn read(&self, offset: u64, buf: &mut [u8]) -> error::Result<usize> {
        let nwant = buf.len();
        let mut bytes_read: usize = 0;

        while bytes_read < nwant {
            match self.cache_file.read_at(
                &mut buf[bytes_read..],
                offset + (bytes_read as u64),
            ) {
                Ok(nread) => {
                    if nread == 0 {
                        return Ok(bytes_read);
                    }
                    bytes_read += nread;
                }
                Err(e) => {
                    if bytes_read > 0 {
                        return Ok(bytes_read);
                    } else {
                        return Err(RError::from(e));
                    }
                }
            }
        }

        return Ok(bytes_read);
    }

    pub fn write(&mut self, offset: u64, buf: &[u8]) -> error::Result<usize> {
        let nwant = buf.len();
        let mut bytes_written: usize = 0;

        if !self.dirty {
            // assumes that the metadata will hit the disk before the
            // incoming data will, and not flushing
            self.cache_file.set_xattr("user.catfs.pristine", &DIRTY)?;
        }

        while bytes_written < nwant {
            match self.cache_file.write_at(
                &buf[bytes_written..],
                offset + (bytes_written as u64),
            ) {
                Ok(nwritten) => {
                    if nwritten == 0 {
                        return Ok(bytes_written);
                    }
                    bytes_written += nwritten;
                }
                Err(e) => {
                    if bytes_written > 0 {
                        return Ok(bytes_written);
                    } else {
                        return Err(RError::from(e));
                    }
                }
            }
        }

        if bytes_written != 0 {
            self.dirty = true;
        }

        return Ok(bytes_written);
    }

    pub fn flush(&mut self) -> error::Result<()> {
        if self.dirty {
            self.copy(false)?;
            self.dirty = false;
        }
        self.cache_file.flush()?;
        // if file was opened for read only and cache is valid we
        // might not have opened src_file
        if self.src_file.valid() {
            self.src_file.flush()?;
        }
        return Ok(());
    }

    fn copy_user(rh: &File, wh: &File) -> error::Result<()> {
        let mut buf = [0u8; 32 * 1024];
        let mut offset = 0;
        loop {
            let nread = rh.read_at(&mut buf, offset)?;
            if nread == 0 {
                break;
            }
            wh.write_at(&buf[..nread], offset)?;
            offset += nread as u64;
        }
        return Ok(());
    }

    fn copy_splice(rh: &File, wh: &File) -> error::Result<()> {
        let (pin, pout) = rlibc::pipe()?;

        let mut offset = 0;
        loop {
            let nread = rlibc::splice(rh.as_raw_fd(), offset, pout, -1, 128 * 1024)?;
            if nread == 0 {
                break;
            }

            let mut written = 0;
            while written < nread {
                let nxfer = rlibc::splice(pin, -1, wh.as_raw_fd(), offset, 128 * 1024)?;

                written += nxfer;
                offset += nxfer as i64;
            }
        }

        if let Err(e) = rlibc::close(pin) {
            rlibc::close(pout)?;
            return Err(RError::from(e));
        } else {
            rlibc::close(pout)?;
        }

        return Ok(());
    }

    fn copy(&self, to_cache: bool) -> error::Result<()> {
        let rh: &File;
        let wh: &File;
        if to_cache {
            rh = &self.src_file;
            wh = &self.cache_file;
        } else {
            rh = &self.cache_file;
            wh = &self.src_file;
        }

        wh.set_size(rh.filesize()?)?;

        if let Err(e) = Handle::copy_splice(rh, wh) {
            if e.raw_os_error().unwrap() == libc::EINVAL {
                Handle::copy_user(rh, wh)?;
            }
        }

        self.cache_file.set_xattr("user.catfs.pristine", &PRISTINE)?;
        return Ok(());
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Err(e) = self.cache_file.close() {
            error!("!close(cache) = {}", RError::from(e));
        }
        if self.src_file.valid() {
            if let Err(e) = self.src_file.close() {
                error!("!close(src) = {}", RError::from(e));
            }
        }
    }
}
