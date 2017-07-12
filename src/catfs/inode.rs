extern crate fuse;
extern crate time;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use catfs::dir;
use catfs::file;
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


#[allow(dead_code)]
fn now() -> Timespec {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    return Timespec {
        sec: d.as_secs() as i64,
        nsec: d.subsec_nanos() as i32,
    };
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

    pub fn get_kind(&self) -> fuse::FileType {
        return self.attr.kind;
    }

    pub fn lookup_path(path: &OsStr) -> io::Result<fuse::FileAttr> {
        debug!("lookup_path {:?}", path);
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

    pub fn to_absolute(&self, relative_to: &OsStr) -> OsString {
        let mut path = relative_to.to_os_string();
        path.push("/");
        path.push(&self.path);
        return path;
    }

    pub fn lookup(&self, name: &OsStr, relative_to: &OsStr) -> io::Result<Inode> {
        let path = self.get_child_name(name);
        let mut abs_path = relative_to.to_os_string();
        abs_path.push("/");
        abs_path.push(&path);
        let attr = Inode::lookup_path(&abs_path)?;
        return Ok(Inode::new(name.to_os_string(), path, attr));
    }

    pub fn create(
        &self,
        name: &OsStr,
        relative_to: &OsStr,
        mode: u32,
    ) -> io::Result<(Inode, file::Handle)> {
        let path = self.get_child_name(name);

        let mut cache_path = relative_to.to_os_string();
        cache_path.push("/");
        cache_path.push(&path);

        let mut opt = OpenOptions::new();
        opt.write(true).create_new(true).mode(mode as u32);
        let wh = file::Handle::open(&cache_path, &opt)?;

        let attr = Inode::lookup_path(&cache_path)?;
        let inode = Inode::new(name.to_os_string(), path, attr);

        return Ok((inode, wh));
    }

    pub fn open(&self, relative_to: &OsStr, flags: u32) -> io::Result<file::Handle> {
        return file::Handle::open_as(&self.to_absolute(relative_to), flags);
    }

    pub fn opendir(&self, relative_to: &OsStr) -> io::Result<dir::Handle> {
        return dir::Handle::open(&self.to_absolute(relative_to));
    }

    pub fn cache(&self, from: &OsStr, to: &OsStr) -> io::Result<()> {
        let mut rh = file::Handle::open_rdonly(&self.to_absolute(from))?;
        let cache_path = self.to_absolute(to);

        // don't check for error, if this fails then create_new will fail too
        if let Err(e) = fs::remove_file(&cache_path) {
            debug!("!remove_file {:?} = {}", cache_path, e);
        }

        // mkdir the parents
        if let Some(parent) = Path::new(&cache_path).parent() {
            fs::create_dir_all(parent)?;
        }

        let mut opt = OpenOptions::new();
        opt.write(true).create_new(true).mode(self.attr.perm as u32);
        let mut wh = file::Handle::open(&cache_path, &opt)?;
        let mut buf = [0u8; 32 * 1024];
        let mut offset = 0;
        loop {
            let nread = rh.read(offset, &mut buf)?;
            if nread == 0 {
                break;
            }
            offset += nread as u64;
            wh.write(offset, &mut buf)?;
        }

        return Ok(());
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
