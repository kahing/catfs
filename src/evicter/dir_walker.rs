extern crate libc;

use std::path::{Path, PathBuf};
use std::ptr;
use std::os::unix::io::RawFd;

use catfs::error;
use catfs::rlibc;

pub struct DirWalker {
    dir: RawFd,
    cur: *mut libc::DIR,
    cur_path: PathBuf,
    stack: Vec<PathBuf>,
}

impl DirWalker {
    pub fn new(dir: RawFd) -> error::Result<DirWalker> {
        let fd = rlibc::openat(dir, &".", rlibc::O_RDONLY, 0)?;
        Ok(DirWalker {
            dir: dir,
            cur: rlibc::fdopendir(fd)?,
            cur_path: Default::default(),
            stack: Default::default(),
        })
    }

    fn next_internal(&mut self) -> error::Result<Option<PathBuf>> {
        loop {
            match rlibc::readdir(self.cur)? {
                Some(entry) => {
                    if entry.en.d_type == libc::DT_DIR {
                        let name = entry.name();
                        if name != Path::new(".") && name != Path::new("..") {
                            self.stack.push(self.cur_path.join(entry.name()));
                        }
                    } else {
                        return Ok(Some(self.cur_path.join(entry.name())));
                    }
                }
                None => {
                    rlibc::closedir(self.cur)?;
                    self.cur = ptr::null_mut();

                    if let Some(next) = self.stack.pop() {
                        let fd = rlibc::openat(self.dir, &next, rlibc::O_RDONLY, 0)?;
                        self.cur = rlibc::fdopendir(fd)?;
                        self.cur_path = next;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl Drop for DirWalker {
    fn drop(&mut self) {
        if !self.cur.is_null() {
            if let Err(e) = rlibc::closedir(self.cur) {
                error!("!closedir {:?} = {}", self.cur, e);
            }
        }
    }
}

impl Iterator for DirWalker {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_internal() {
            Ok(item) => item,
            Err(e) => {
                error!("!DirWalker::next {:?} = {}", self.cur, e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    use std::env;
    use std::path::{Path, PathBuf};
    use catfs::rlibc;
    use super::*;

    #[test]
    fn iterator_test() {
        let _ = env_logger::init();

        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        let resources = PathBuf::from(manifest).join("tests/resources");
        let fd = rlibc::open(&resources, rlibc::O_RDONLY, 0).unwrap();
        let mut files: Vec<PathBuf> = DirWalker::new(fd).unwrap().collect();
        files.sort();

        assert_eq!(files.len(), 5);

        let mut iter = files.into_iter();
        assert_eq!(iter.next().unwrap(), Path::new("dir1/file1"));
        assert_eq!(iter.next().unwrap(), Path::new("dir1/file2"));
        assert_eq!(iter.next().unwrap(), Path::new("file1"));
        assert_eq!(iter.next().unwrap(), Path::new("file2"));
        assert_eq!(iter.next().unwrap(), Path::new("file3"));
        assert_eq!(iter.next(), None);
    }
}
