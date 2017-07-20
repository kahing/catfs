extern crate fuse;
extern crate time;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::fs::OpenOptions;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use catfs::dir;
use catfs::error;
use catfs::file;
use catfs::substr::Substr;
use self::time::Timespec;

#[derive(Clone)]
pub struct Inode<'a> {
    src_dir: &'a Path,
    cache_dir: &'a Path,

    name: OsString,
    path: PathBuf,

    attr: fuse::FileAttr,

    refcnt: u64,
}

fn to_filetype(t: fs::FileType) -> fuse::FileType {
    if t.is_dir() {
        return fuse::FileType::Directory;
    } else if t.is_symlink() {
        return fuse::FileType::Symlink;
    } else {
        return fuse::FileType::RegularFile;
    }
}


#[allow(dead_code)]
fn now() -> Timespec {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    return Timespec {
        sec: d.as_secs() as i64,
        nsec: d.subsec_nanos() as i32,
    };
}

impl<'a> Inode<'a> {
    pub fn new<P: AsRef<Path> + ?Sized>(
        src_dir: &'a P,
        cache_dir: &'a P,
        name: OsString,
        path: PathBuf,
        attr: fuse::FileAttr,
    ) -> Inode<'a> {
        return Inode {
            src_dir: src_dir.as_ref(),
            cache_dir: cache_dir.as_ref(),
            name: name,
            path: path,
            attr: attr,
            refcnt: 1,
        };
    }

    pub fn get_child_name(&self, name: &OsStr) -> PathBuf {
        let mut path = self.path.clone();
        path.push(name);
        return path;
    }

    pub fn get_path(&self) -> &Path {
        return &self.path;
    }

    pub fn to_src_path(&self) -> PathBuf {
        return self.src_dir.join(&self.path);
    }

    pub fn to_cache_path(&self) -> PathBuf {
        return self.cache_dir.join(&self.path);
    }

    pub fn get_attr(&self) -> &fuse::FileAttr {
        return &self.attr;
    }

    pub fn get_kind(&self) -> fuse::FileType {
        return self.attr.kind;
    }

    pub fn get_ino(&self) -> u64 {
        return self.attr.ino;
    }

    pub fn extend(&mut self, offset: u64) {
        if self.attr.size < offset {
            self.attr.size = offset;
        }
    }

    pub fn lookup_path(path: &AsRef<Path>) -> io::Result<fuse::FileAttr> {
        // misnomer as symlink_metadata is the one that does NOT follow symlinks
        let m = fs::symlink_metadata(path)?;
        let attr = fuse::FileAttr {
            ino: m.ino(),
            size: m.len(),
            blocks: m.blocks(),
            atime: Timespec {
                sec: m.atime(),
                nsec: m.atime_nsec() as i32,
            },
            mtime: Timespec {
                sec: m.mtime(),
                nsec: m.mtime_nsec() as i32,
            },
            ctime: Timespec {
                sec: m.ctime(),
                nsec: m.ctime_nsec() as i32,
            },
            crtime: Timespec {
                sec: m.ctime(),
                nsec: m.ctime_nsec() as i32,
            },
            kind: to_filetype(m.file_type()),
            perm: m.mode() as u16,
            nlink: m.nlink() as u32,
            uid: m.uid(),
            gid: m.gid(),
            rdev: m.rdev() as u32,
            flags: 0,
        };
        return Ok(attr);
    }

    pub fn refresh(&mut self) -> error::Result<()> {
        self.attr = Inode::lookup_path(&self.to_src_path())?;
        return Ok(());
    }

    pub fn lookup(&self, name: &OsStr) -> error::Result<Inode<'a>> {
        let path = self.get_child_name(name);
        match Inode::lookup_path(&self.to_src_path().join(name)) {
            Ok(attr) => {
                return Ok(Inode::new(
                    self.src_dir,
                    self.cache_dir,
                    name.to_os_string(),
                    path,
                    attr,
                ))
            }
            Err(e) => return error::propagate(e),
        }
    }

    pub fn create(&self, name: &OsStr, mode: u32) -> error::Result<(Inode<'a>, file::Handle)> {
        let path = self.get_child_name(name);

        let mut opt = OpenOptions::new();
        opt.write(true).create_new(true).mode(mode as u32);

        let wh = file::Handle::create(
            &self.to_src_path().join(name),
            &self.to_cache_path().join(name),
            &opt,
        )?;

        let attr = Inode::lookup_path(&self.to_src_path().join(name))?;
        let inode = Inode::new(
            self.src_dir,
            self.cache_dir,
            name.to_os_string(),
            path,
            attr,
        );

        return Ok((inode, wh));
    }

    pub fn open(&self, flags: u32) -> error::Result<file::Handle> {
        return file::Handle::open(&self.to_src_path(), &self.to_cache_path(), flags);
    }

    pub fn unlink(&self, name: &OsStr) -> io::Result<()> {
        return file::Handle::unlink(
            &self.to_src_path().join(name),
            &self.to_cache_path().join(name),
        );
    }

    pub fn rmdir(&self, name: &OsStr) -> io::Result<()> {
        return file::Handle::rmdir(
            &self.to_src_path().join(name),
            &self.to_cache_path().join(name),
        );
    }

    pub fn opendir(&self) -> error::Result<dir::Handle> {
        return dir::Handle::open(&self.to_src_path());
    }

    pub fn use_ino(&mut self, ino: u64) {
        self.attr.ino = ino;
    }

    pub fn inc_ref(&mut self) {
        self.refcnt += 1;
    }

    // return stale
    pub fn deref(&mut self, n: u64) -> bool {
        self.refcnt -= n;
        return self.refcnt == 0;
    }
}
