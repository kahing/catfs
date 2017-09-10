extern crate generic_array;
extern crate libc;
extern crate sha2;
extern crate threadpool;
extern crate xattr;

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::RawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use self::generic_array::GenericArray;
use self::generic_array::typenum::U64;
use self::sha2::{Sha512, Digest};
use self::threadpool::ThreadPool;
use self::xattr::FileExt;

use catfs::error;
use catfs::error::RError;
use catfs::rlibc;
use catfs::rlibc::File;

type CvData<T> = Arc<(Mutex<T>, Condvar)>;

#[derive(Default)]
struct PageInInfo {
    offset: u64,
    dirty: bool,
    eof: bool,
    err: Option<RError<io::Error>>,
}

pub struct Handle {
    src_file: File,
    cache_file: File,
    dirty: bool,
    write_through_failed: bool,
    has_page_in_thread: bool,
    page_in_res: CvData<PageInInfo>,
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
            write_through_failed: false,
            has_page_in_thread: false,
            page_in_res: Arc::new((Default::default(), Condvar::new())),
        });
    }

    pub fn open(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
        flags: u32,
        cache_valid_if_present: bool,
        disable_splice: bool,
        tp: &Mutex<ThreadPool>,
    ) -> error::Result<Handle> {
        // even if file is open for write only, I still need to be
        // able to read the src for read-modify-write
        let mut flags = flags;
        if (flags & rlibc::O_ACCMODE) == rlibc::O_WRONLY {
            make_rdwr(&mut flags);
        }

        let valid =
            Handle::validate_cache(src_dir, cache_dir, &path, cache_valid_if_present, false)?;
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

        let mut handle = Handle {
            src_file: src_file,
            cache_file: File::openat(cache_dir, path, cache_flags, 0o666)?,
            dirty: false,
            write_through_failed: false,
            has_page_in_thread: false,
            page_in_res: Arc::new((Default::default(), Condvar::new())),
        };

        if !valid && (flags & rlibc::O_TRUNC) == 0 {
            debug!("read ahead {:?}", path.as_ref());
            handle.has_page_in_thread = true;
            let mut h = handle.clone();
            let path = path.as_ref().to_path_buf();
            tp.lock().unwrap().execute(move || {
                if let Err(e) = h.copy(true, disable_splice) {
                    let mut is_cancel = false;

                    {
                        let page_in_res = h.page_in_res.0.lock().unwrap();
                        if let Some(ref e2) = page_in_res.err {
                            if e2.raw_os_error().unwrap() == libc::ECANCELED {
                                is_cancel = true;
                            }
                        }
                    }

                    if !is_cancel {
                        error!("read ahead {:?} failed: {}", path, e);
                        h.notify_offset(Err(e), false).unwrap();
                    } else {
                        debug!("read ahead {:?} canceled", path);
                    }
                }
                // the files are always closed in the main IO path, consume
                // the fds to prevent closing
                h.src_file.into_raw();
                h.cache_file.into_raw();
            });
        }

        return Ok(handle);
    }

    // see validate_cache.sh on how to replicate this
    pub fn src_str_to_checksum(f: &File) -> error::Result<OsString> {
        let mut s = OsString::new();
        for x in ["s3.etag"].iter() {
            match f.get_xattr(&x) {
                Ok(v) => {
                    if let Some(v) = v {
                        s.push(x);
                        s.push(OsStr::new("="));
                        s.push("0x");
                        for b in v {
                            s.push(format!("{:x}", b));
                        }
                        s.push("\n");
                    }
                }
                Err(e) => {
                    let errno = e.raw_os_error().unwrap();
                    if errno != libc::ENOENT && errno != libc::ENOTSUP {
                        return Err(RError::from(e));
                    }
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
        //debug!("checksum is {:?}", s);
        let mut h = Sha512::default();
        h.input(s.as_bytes());
        return Ok(h.result());
    }

    pub fn make_pristine(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
    ) -> error::Result<()> {
        match File::openat(cache_dir, path, rlibc::O_WRONLY, 0) {
            Err(e) => {
                return Err(RError::from(e));
            }
            Ok(mut cache) => {
                let mut src = File::openat(src_dir, path, rlibc::O_RDONLY, 0)?;
                cache.set_xattr(
                    "user.catfs.src_chksum",
                    Handle::src_chksum(&src)?.as_slice(),
                )?;
                src.close()?;
                cache.close()?;
            }
        }

        return Ok(());
    }

    pub fn set_pristine(&self, pristine: bool) -> error::Result<()> {
        if pristine {
            self.cache_file.set_xattr(
                "user.catfs.src_chksum",
                Handle::src_chksum(&self.src_file)?
                    .as_slice(),
            )?;
        } else {
            if let Err(e) = self.cache_file.remove_xattr("user.catfs.src_chksum") {
                if e.raw_os_error().unwrap() != libc::ENODATA {
                    return Err(RError::from(e));
                }
            }
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

    pub fn validate_cache(
        src_dir: RawFd,
        cache_dir: RawFd,
        path: &AsRef<Path>,
        cache_valid_if_present: bool,
        check_only: bool,
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
                            if !check_only {
                                error!("{:?} is not a valid cache file, deleting", path.as_ref());
                                rlibc::unlinkat(cache_dir, path, 0)?;
                            }
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
                    if !check_only {
                        maybe_unlinkat(cache_dir, path)?;
                    }
                }
            }
        }

        return Ok(false);
    }

    pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> error::Result<usize> {
        let nwant = buf.len();
        let mut bytes_read: usize = 0;

        if self.has_page_in_thread {
            self.wait_for_offset(offset + buf.len() as u64, false)?;
        }

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

    pub fn truncate(&mut self, size: usize) -> error::Result<()> {
        // pristiness comes from size as well so this automatically
        // invalidates the cache file if it's used again
        self.src_file.set_size(size)?;

        // wait for the background thread to finish so we won't have
        // more bytes being concurrently written to cache_file
        if self.has_page_in_thread {
            self.wait_for_eof()?;
        }

        self.cache_file.set_size(size)?;
        // caller is responsible for setting this to pristine if necessary
        return Ok(());
    }

    pub fn chmod(&self, mode: u32) -> io::Result<()> {
        self.src_file.chmod(mode)?;
        return Ok(());
    }

    pub fn write(&mut self, offset: u64, buf: &[u8]) -> error::Result<usize> {
        let nwant = buf.len();
        let mut bytes_written: usize = 0;

        if !self.dirty {
            // assumes that the metadata will hit the disk before the
            // incoming data will, and not flushing
            self.set_pristine(false)?;
        }

        if self.has_page_in_thread {
            self.wait_for_offset(offset + buf.len() as u64, true)?;
        }

        while bytes_written < nwant {
            if !self.write_through_failed {
                if let Err(e) = self.src_file.write_at(
                    &buf[bytes_written..],
                    offset + (bytes_written as u64),
                )
                {
                    if e.raw_os_error().unwrap() == libc::ENOTSUP {
                        self.write_through_failed = true;
                        return Err(RError::propagate(e));
                    } else {
                        if bytes_written != 0 {
                            self.dirty = true;
                        }
                        return Err(RError::from(e));
                    }
                }

            }

            match self.cache_file.write_at(
                &buf[bytes_written..],
                offset + (bytes_written as u64),
            ) {
                Ok(nwritten) => {
                    bytes_written += nwritten;
                }
                Err(e) => {
                    if bytes_written > 0 {
                        break;
                    } else {
                        if bytes_written != 0 {
                            self.dirty = true;
                        }
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

    pub fn flush(&mut self) -> error::Result<bool> {
        let mut flushed_to_src = false;
        if self.dirty {
            if self.write_through_failed {
                if self.has_page_in_thread {
                    self.wait_for_eof()?;
                }

                self.copy(false, false)?;
            } else {
                self.set_pristine(true)?;
            }
            self.cache_file.flush()?;
            if let Err(e) = self.src_file.flush() {
                error!("!flush(src) = {}", e);
                // flush failed, now the fd is invalid, get rid of it
                self.src_file.into_raw();

                // we couldn't flush the src_file, because of some
                // linux vfs oddity the file would appear to be
                // "normal" until we try to read it (the inode is
                // cached), so we need to invalidate our cache (which
                // would validate and thus we would never read from
                // src).

                // we only have the fd and there's no funlink, so we
                // will just unset the xattr
                self.set_pristine(false)?;

                return Err(RError::propagate(e));
            }
            self.dirty = false;
            flushed_to_src = true;
        } else {
            if self.has_page_in_thread {
                // tell it to cancel
                let mut page_in_res = self.page_in_res.0.lock().unwrap();
                page_in_res.err = Some(RError::propagate(
                    io::Error::from_raw_os_error(libc::ECANCELED),
                ));
            }
        }
        return Ok(flushed_to_src);
    }

    fn wait_for_eof(&mut self) -> error::Result<()> {
        let mut page_in_res = self.page_in_res.0.lock().unwrap();
        loop {
            if page_in_res.eof {
                self.has_page_in_thread = false;
                return Ok(());
            } else {
                page_in_res = self.page_in_res.1.wait(page_in_res).unwrap();
            }
        }
    }

    fn wait_for_offset(&mut self, offset: u64, set_dirty: bool) -> error::Result<()> {
        let &(ref lock, ref cvar) = &*self.page_in_res;

        let mut page_in_res = lock.lock().unwrap();
        if set_dirty {
            // setting this to dirty prevents us from marking this as pristine
            page_in_res.dirty = true;
        }
        loop {
            if page_in_res.eof {
                self.has_page_in_thread = false;
                return Ok(());
            }

            if page_in_res.offset >= offset {
                return Ok(());
            } else if let Some(e) = page_in_res.err.clone() {
                return Err(e.clone());
            } else {
                page_in_res = cvar.wait(page_in_res).unwrap();
            }
        }
    }

    fn notify_offset(&self, res: error::Result<u64>, eof: bool) -> error::Result<()> {
        let &(ref lock, ref cvar) = &*self.page_in_res;

        let mut page_in_res = lock.lock().unwrap();
        if !eof && page_in_res.err.is_some() {
            // main IO thread sets this to cancel paging, but if eof
            // is reached then we might as well finish it
            return Err(page_in_res.err.clone().unwrap());
        }

        match res {
            Ok(offset) => page_in_res.offset = offset,
            Err(e) => page_in_res.err = Some(e),
        }
        page_in_res.eof = eof;
        if eof && !page_in_res.dirty {
            self.set_pristine(true)?;
        }
        cvar.notify_all();
        return Ok(());
    }

    pub fn reopen_src(
        &mut self,
        dir: RawFd,
        path: &AsRef<Path>,
        create: bool,
    ) -> error::Result<()> {
        let _ = self.page_in_res.0.lock().unwrap();

        let mut buf = [0u8; 0];
        let mut flags = rlibc::O_RDWR;
        if let Err(e) = self.src_file.read_at(&mut buf, 0) {
            if e.raw_os_error().unwrap() == libc::EBADF {
                // this was not open for read
                flags = rlibc::O_WRONLY;
            } else {
                return Err(RError::from(e));
            }
        }

        let mut mode = 0;
        if create {
            let st = self.src_file.stat()?;
            mode = st.st_mode & !libc::S_IFMT;
            flags = flags | rlibc::O_CREAT;
        }

        if let Err(e) = self.src_file.close() {
            // normal for this close to fail
            if e.raw_os_error().unwrap() != libc::ENOTSUP {
                return Err(RError::from(e));
            }
        }

        self.src_file = File::openat(dir, path, flags, mode)?;
        return Ok(());
    }

    fn copy_user(&self, rh: &File, wh: &File) -> error::Result<u64> {
        let mut buf = [0u8; 32 * 1024];
        let mut offset = 0;
        loop {
            let nread = rh.read_at(&mut buf, offset as u64)?;
            if nread == 0 {
                break;
            }
            wh.write_at(&buf[..nread], offset as u64)?;
            offset += nread as u64;

            self.notify_offset(Ok(offset), false)?;
        }

        return Ok(offset);
    }

    fn copy_splice(&self, rh: &File, wh: &File) -> error::Result<u64> {
        let (pin, pout) = rlibc::pipe()?;

        let mut offset = 0;
        loop {
            let nread = rlibc::splice(rh.as_raw_fd(), offset as i64, pout, -1, 128 * 1024)?;
            if nread == 0 {
                break;
            }

            let mut written = 0;
            while written < nread {
                let nxfer = rlibc::splice(pin, -1, wh.as_raw_fd(), offset as i64, 128 * 1024)?;

                written += nxfer;
                offset += nxfer as u64;

                self.notify_offset(Ok(offset), false)?;
            }
        }

        if let Err(e) = rlibc::close(pin) {
            rlibc::close(pout)?;
            return Err(RError::from(e));
        } else {
            rlibc::close(pout)?;
        }

        return Ok(offset);
    }

    fn copy(&self, to_cache: bool, disable_splice: bool) -> error::Result<()> {
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

        let offset: u64;

        if disable_splice {
            offset = self.copy_user(rh, wh)?;
        } else {
            match self.copy_splice(rh, wh) {
                Err(e) => {
                    if e.raw_os_error().unwrap() == libc::EINVAL {
                        offset = self.copy_user(rh, wh)?;
                    } else {
                        return Err(e);
                    }
                }
                Ok(off) => offset = off,
            }
        }

        self.notify_offset(Ok(offset), true)?;
        return Ok(());
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if self.cache_file.valid() {
            if let Err(e) = self.cache_file.close() {
                error!("!close(cache) = {}", RError::from(e));
            }
        }

        if self.src_file.valid() {
            if let Err(e) = self.src_file.close() {
                error!("!close(src) = {}", RError::from(e));
            }
        }
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        return Handle {
            src_file: File::with_fd(self.src_file.as_raw_fd()),
            cache_file: File::with_fd(self.cache_file.as_raw_fd()),
            dirty: self.dirty,
            write_through_failed: self.write_through_failed,
            has_page_in_thread: false,
            page_in_res: self.page_in_res.clone(),
        };
    }
}
