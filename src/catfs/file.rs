extern crate generic_array;
extern crate libc;
extern crate sha2;
extern crate xattr;

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::{Component, Path, PathBuf};

use self::generic_array::GenericArray;
use self::generic_array::typenum::U64;
use self::sha2::{Sha512, Digest};
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

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for Handle {}

fn make_rdwr(f: &mut u32) {
    *f = (*f & !rlibc::O_ACCMODE) | rlibc::O_RDWR;
}

fn maybe_unlinkat(dir: RawFd, path: &AsRef<Path>) -> io::Result<()> {
    if let Err(e) = rlibc::unlinkat(dir, path, 0) {
        if !error::is_enoent(&e) {
            return Err(e);
        }
    }
    return Ok(());
}

pub fn mkdirat_all(dir: RawFd, path: &AsRef<Path>, mode: u32) -> io::Result<()> {
    let mut p = PathBuf::new();

    for c in path.as_ref().components() {
        if let Component::Normal(n) = c {
            p.push(n);

            if let Err(e) = rlibc::mkdirat(dir, &p, mode) {
                if e.raw_os_error().unwrap() != libc::EEXIST {
                    return Err(e);
                }
            }
        }
    }

    return Ok(());
}

impl Handle {
    pub fn create(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
        flags: u32,
        mode: u32,
    ) -> error::Result<Handle> {
        // need to read the cache file for writeback
        let mut cache_flags = flags.clone();
        if (cache_flags & rlibc::O_ACCMODE) == rlibc::O_WRONLY {
            make_rdwr(&mut cache_flags);
        }
        //debug!("create {:b} {:b} {:#o}", flags, cache_flags, mode);

        if let Some(parent) = path.as_ref().parent() {
            mkdirat_all(cache_dir, &parent, 0o777)?;
        }

        let src_file = File::openat(src_dir, path, flags, mode)?;
        // we are able to create the src file, then the cache file
        // shouldn't be here, but it could be because of bug/crash,
        // so unlink it first
        maybe_unlinkat(cache_dir, path)?;

        return Ok(Handle {
            src_file: src_file,
            cache_file: File::openat(cache_dir, path, cache_flags, mode)?,
            dirty: true,
        });
    }

    pub fn open(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
        flags: u32,
        cache_valid_if_present: bool,
    ) -> error::Result<Handle> {
        // even if file is open for write only, I still need to be
        // able to read the src for read-modify-write
        let mut flags = flags;
        if (flags & rlibc::O_ACCMODE) == rlibc::O_WRONLY {
            make_rdwr(&mut flags);
        }

        let valid = Handle::validate_cache(src_dir, cache_dir, &path, cache_valid_if_present)?;
        debug!(
            "{:?} {} a valid cache file",
            path.as_ref(),
            if valid { "is" } else { "is not" },
        );
        let mut cache_flags = flags.clone();

        if !valid {
            // mkdir the parents
            if let Some(parent) = path.as_ref().parent() {
                mkdirat_all(cache_dir, &parent, 0o777)?;
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
            src_file = File::openat(src_dir, path, flags, 0o666)?;
        }

        let handle = Handle {
            src_file: src_file,
            cache_file: File::openat(cache_dir, path, cache_flags, 0o666)?,
            dirty: false,
        };

        if !valid && (flags & rlibc::O_TRUNC) == 0 {
            debug!("read ahead {:?}", path.as_ref());
            handle.copy(true)?;
        }

        return Ok(handle);
    }

    // the equivalent of:
    // getfattr -e hex --match=.* -d $f 2>/dev/null | grep =;
    // /usr/bin/stat -t --printf "%Y\n%s\n" $f
    // note that mtime is printed first and then size
    pub fn src_str_to_checksum(f: &File) -> error::Result<OsString> {
        let mut s = OsString::new();
        match f.list_xattr() {
            Ok(attrs) => {
                for x in attrs {
                    if let Some(v) = f.get_xattr(&x)? {
                        s.push(x);
                        s.push(OsStr::new("="));
                        s.push("0x");
                        for b in v {
                            s.push(format!("{:x}", b));
                        }
                        s.push("\n");
                    }
                }
            }
            Err(e) => {
                if e.raw_os_error().unwrap() != libc::ENOTSUP {
                    return Err(RError::from(e));
                }
            }
        }

        let st = f.stat()?;
        s.push(format!("{}\n", st.st_mtime));
        s.push(format!("{}\n", st.st_size));
        return Ok(s);
    }

    fn src_chksum(f: &File) -> error::Result<GenericArray<u8, U64>> {
        let s = Handle::src_str_to_checksum(f)?;
        debug!("checksum is {:?}", s);
        let mut h = Sha512::default();
        h.input(s.as_bytes());
        return Ok(h.result());
    }

    fn set_pristine(&self, pristine: bool) -> error::Result<()> {
        if pristine {
            self.cache_file.set_xattr(
                "user.catfs.src_chksum",
                Handle::src_chksum(&self.src_file)?
                    .as_slice(),
            )?;
        } else {
            self.cache_file.remove_xattr("user.catfs.src_chksum")?;
        }
        return Ok(());
    }

    fn is_pristine(src_file: &File, cache_file: &File) -> error::Result<bool> {
        if let Some(v) = cache_file.get_xattr("user.catfs.src_chksum")? {
            let expected = Handle::src_chksum(src_file)?;
            if v == expected.as_slice() {
                return Ok(true);
            } else {
                debug!("{:?} != {:?}, {} {}", v, expected, v.len(), expected.len());
                return Ok(false);
            }
        }
        debug!("user.catfs.src_chksum missing for cache_file");

        return Ok(false);
    }

    pub fn unlink(src_dir: RawFd, cache_dir: RawFd, path: &AsRef<Path>) -> io::Result<()> {
        maybe_unlinkat(cache_dir, path)?;
        return rlibc::unlinkat(src_dir, path, 0);
    }

    fn validate_cache(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
        cache_valid_if_present: bool,
    ) -> error::Result<bool> {
        match File::openat(src_dir, path, rlibc::O_RDONLY, 0) {
            Ok(mut src_file) => {
                match File::openat(cache_dir, path, rlibc::O_RDONLY, 0) {
                    Ok(mut cache_file) => {
                        let valid: bool;
                        if cache_valid_if_present || Handle::is_pristine(&src_file, &cache_file)? {
                            valid = true;
                        } else {
                            valid = false;
                            rlibc::unlinkat(cache_dir, path, 0)?;
                        }
                        src_file.close()?;
                        cache_file.close()?;
                        return Ok(valid);
                    }
                    Err(e) => {
                        src_file.close()?;
                        if error::try_enoent(e)? {
                            return Ok(false);
                        }
                    }
                }
            }
            Err(e) => {
                if error::try_enoent(e)? {
                    // the source file doesn't exist, the cache file shouldn't either
                    maybe_unlinkat(cache_dir, path)?;
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
            self.set_pristine(false)?;
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

    pub fn flush(&mut self) -> error::Result<(bool)> {
        let mut flushed_to_src = false;
        if self.dirty {
            self.copy(false)?;
            self.dirty = false;
            self.cache_file.flush()?;
            // if file was opened for read only and cache is valid we
            // might not have opened src_file
            if self.src_file.valid() {
                self.src_file.flush()?;
                flushed_to_src = true;
            }
        }
        return Ok(flushed_to_src);
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

        let size = rh.filesize()?;
        if size < wh.filesize()? {
            wh.truncate(size)?;
        }

        if let Err(e) = Handle::copy_splice(rh, wh) {
            if e.raw_os_error().unwrap() == libc::EINVAL {
                Handle::copy_user(rh, wh)?;
            }
        }

        self.set_pristine(true)?;
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
