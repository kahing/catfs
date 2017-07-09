extern crate fuse;
extern crate time;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;

use self::time::Timespec;

#[derive(Clone)]
pub struct Inode {
    name: OsString,
    path: OsString,

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


impl Inode {
    pub fn new(name: OsString, path: OsString, attr: fuse::FileAttr) -> Inode {
        return Inode {
            name: name,
            path: path,
            attr: attr,
            refcnt: 1,
        };
    }

    pub fn get_child_name(&self, name: &OsStr) -> OsString {
        if self.attr.ino == fuse::FUSE_ROOT_ID {
            return name.to_os_string();
        } else {
            let mut s = self.path.clone();
            s.push("/");
            s.push(name);
            return s;
        }
    }

    pub fn get_path(&self) -> &OsStr {
        return &self.path;
    }

    pub fn get_attr(&self) -> &fuse::FileAttr {
        return &self.attr;
    }

    pub fn get_ino(&self) -> u64 {
        return self.attr.ino;
    }

    pub fn lookup_path(path: &OsStr) -> io::Result<fuse::FileAttr> {
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

    pub fn lookup(&self, name: &OsStr) -> io::Result<Inode> {
        let path = self.get_child_name(name);
        // misnomer as symlink_metadata is the one that does NOT follow symlinks
        let attr = Inode::lookup_path(&path)?;
        return Ok(Inode::new(name.to_os_string(), path, attr));
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
