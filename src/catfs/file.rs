extern crate libc;

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use catfs::error;
use catfs::error::RError;

pub struct Handle {
    src_file: File,
    cache_file: File,
    dirty: bool,
}

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for Handle {}

pub fn is_truncate(flags: u32) -> bool {
    return (flags & (libc::O_TRUNC as u32)) != 0;
}

pub fn flags_to_open_options(flags: i32) -> OpenOptions {
    let mut opt = OpenOptions::new();
    let access_mode = flags & libc::O_ACCMODE;

    if access_mode == libc::O_RDONLY {
        opt.read(true);
    } else if access_mode == libc::O_WRONLY {
        opt.write(true);
    } else if access_mode == libc::O_RDWR {
        opt.read(true).write(true);
    }

    opt.custom_flags(flags);

    return opt;
}


impl Handle {
    pub fn create(
        src_path: &AsRef<Path>,
        cache_path: &AsRef<Path>,
        opt: &OpenOptions,
    ) -> error::Result<Handle> {
        let mut cache_opt = opt.clone();
        cache_opt.read(true);

        if let Some(parent) = cache_path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        return Ok(Handle {
            src_file: opt.open(src_path)?,
            cache_file: cache_opt.open(cache_path)?,
            dirty: true,
        });
    }

    pub fn open(
        src_path: &AsRef<Path>,
        cache_path: &AsRef<Path>,
        flags: u32,
    ) -> error::Result<Handle> {
        let mut opt = flags_to_open_options(flags as i32);

        // even if file is open for write only, I still need to be
        // able to read the src for read-modify-write
        opt.read(true);

        let valid = Handle::validate_cache(src_path, cache_path)?;
        let mut cache_opt = opt.clone();

        if !valid {
            // mkdir the parents
            if let Some(parent) = cache_path.as_ref().parent() {
                fs::create_dir_all(parent)?;
            }
            cache_opt.create(true).write(true);
        }

        let handle = Handle {
            src_file: opt.open(src_path)?,
            cache_file: cache_opt.open(cache_path)?,
            dirty: false,
        };

        if !valid && !is_truncate(flags) {
            handle.copy(true)?;
        }

        return Ok(handle);
    }

    fn validate_cache(src_path: &AsRef<Path>, cache_path: &AsRef<Path>) -> error::Result<bool> {
        match fs::symlink_metadata(src_path) {
            Ok(m) => {
                match fs::symlink_metadata(cache_path) {
                    Ok(m2) => {
                        if m.mtime() < m2.mtime() {
                            return Ok(true);
                        } else {
                            fs::remove_file(cache_path)?;
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        if error::is_enoent(e)? {
                            return Ok(false);
                        }
                    }
                }
            }
            Err(e) => {
                if error::is_enoent(e)? {
                    fs::remove_file(cache_path)?;
                    return Ok(true);
                }
            }
        }

        return Ok(false);
    }

    pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> error::Result<usize> {
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
}
