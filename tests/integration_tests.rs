#[allow(unused_imports)]
#[macro_use]
extern crate log;
extern crate libc;
extern crate env_logger;
extern crate catfs;
extern crate rand;
extern crate fuse;
extern crate xattr;

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::process::Command;
use std::path::{Path, PathBuf};
use std::os::unix::fs::FileExt;

use rand::{thread_rng, Rng};

use catfs::CatFS;
use catfs::catfs::error;

#[macro_use]
mod test_suite;


trait Fixture {
    fn setup() -> error::Result<Self>
    where
        Self: std::marker::Sized;
    fn teardown(self) -> error::Result<()>;
}

struct CatFSTests<'a> {
    prefix: PathBuf,
    mnt: PathBuf,
    src: PathBuf,
    cache: PathBuf,
    session: Option<fuse::BackgroundSession<'a>>,
    nested: Option<Box<CatFSTests<'a>>>,
}

fn copy_all(dir1: &AsRef<Path>, dir2: &AsRef<Path>) -> error::Result<()> {
    fs::create_dir(dir2)?;

    for entry in fs::read_dir(dir1)? {
        let entry = entry?;
        let to = dir2.as_ref().join(entry.file_name());

        if entry.file_type()?.is_dir() {
            copy_all(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), to)?;
        }
    }

    return Ok(());
}

impl<'a> CatFSTests<'a> {
    fn get_orig_dir() -> PathBuf {
        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        return PathBuf::from(manifest).join("tests/resources");
    }

    fn get_from(&self) -> PathBuf {
        return self.src.clone();
    }

    fn get_cache(&self) -> PathBuf {
        return self.cache.clone();
    }

    fn mount(&self) -> error::Result<fuse::BackgroundSession<'a>> {
        let fs = CatFS::new(&self.src, &self.cache)?;

        return Ok(unsafe { fuse::spawn_mount(fs, &self.mnt, &[])? });
    }
}

impl<'a> Fixture for CatFSTests<'a> {
    fn setup() -> error::Result<CatFSTests<'a>> {
        #[allow(unused_must_use)] env_logger::init();

        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        let prefix = PathBuf::from(manifest).join("target/test").join(
            thread_rng().gen_ascii_chars().take(10).collect::<String>(),
        );
        let mnt = prefix.join("mnt");
        let resources = prefix.join("resources");
        let cache = prefix.join("cache");

        fs::create_dir_all(&mnt)?;
        fs::create_dir_all(&cache)?;

        fs::create_dir(CatFSTests::get_orig_dir().join("dir2"));
        copy_all(&CatFSTests::get_orig_dir(), &resources)?;

        let mut t = CatFSTests {
            prefix: prefix,
            mnt: mnt,
            src: resources,
            cache: cache,
            session: Default::default(),
            nested: Default::default(),
        };

        t.session = Some(t.mount()?);

        if let Some(v) = env::var_os("CATFS_SELF_HOST") {
            if v == OsStr::new("1") {
                let mnt = t.mnt.clone();
                let mut mnt2 = mnt.as_os_str().to_os_string();
                mnt2.push("2");
                let cache = t.cache.to_path_buf();
                let mut cache2 = cache.as_os_str().to_os_string();
                cache2.push("2");

                let mnt2 = PathBuf::from(mnt2);
                let cache2 = PathBuf::from(cache2);

                fs::create_dir_all(&mnt2)?;
                fs::create_dir_all(&cache2)?;
                
                let mut t2 = CatFSTests {
                    prefix: t.prefix.clone(),
                    mnt: mnt2,
                    src: mnt,
                    cache: cache2,
                    session: Default::default(),
                    nested: Some(Box::new(t)),
                };

                t2.session = Some(t2.mount()?);

                return Ok(t2);
            }
        }

        return Ok(t);
    }

    fn teardown(self) -> error::Result<()> {
        {
            // move out the session to let us umount first
            std::mem::drop(self.session);
        }

        if let Some(t) = self.nested {
            // unmount the inner session
            std::mem::drop(t.session);
        }

        fs::remove_dir_all(&self.prefix)?;
        // TODO do I need to free self.src/cache?
        return Ok(());
    }
}

fn diff(dir1: &AsRef<Path>, dir2: &AsRef<Path>) {
    debug!("diff {:?} {:?}", dir1.as_ref(), dir2.as_ref());
    let status = Command::new("diff")
        .arg("-ru")
        .arg(dir1.as_ref().as_os_str())
        .arg(dir2.as_ref().as_os_str())
        .status()
        .expect("failed to execute `diff'");
    assert!(status.success());
}

unit_tests!{
    fn read_one(f: &CatFSTests) {
        let mut s = String::new();
        File::open(f.mnt.join("dir1/file1")).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "dir1/file1\n");
    }

    fn read_all(f: &CatFSTests) {
        diff(&CatFSTests::get_orig_dir(), &f.mnt);
    }

    fn create(f: &CatFSTests) {
        {
            let mut fh = OpenOptions::new().write(true).create(true)
                .open(Path::new(&f.mnt).join("foo")).unwrap();
            fh.write_all(b"hello world").unwrap();
        }

        fs::symlink_metadata(&f.get_from().join("foo")).unwrap();
        diff(&f.get_from(), &f.mnt);
    }

    fn large_write(f: &CatFSTests) {
        let mut of=OsString::from("of=");
        of.push(f.mnt.join("foo"));
        let status = Command::new("dd")
            .arg("if=/dev/zero").arg(of)
            .arg("bs=1M").arg("count=10")
            .status().expect("failed to execute `dd'");
        assert!(status.success());
        
        let foo = fs::symlink_metadata(&f.get_from().join("foo")).unwrap();
        assert_eq!(foo.len(), 10 * 1024 * 1024);
        diff(&f.get_from(), &f.mnt);
    }

    fn large_dir(f: &CatFSTests) {
        let dir2 = Path::new(&f.mnt).join("dir2");
        for i in 1..1001 {
            let _fh = OpenOptions::new().write(true).create(true)
                .open(dir2.join(format!("{}", i))).unwrap();
        }

        let mut i = 0;
        let mut total = 0;
        for entry in fs::read_dir(dir2).unwrap() {
            i += 1;
            total += entry.unwrap().file_name().to_str().unwrap().parse().unwrap();
        }
        assert_eq!(i, 1000);
        assert_eq!(total, (1000 * (1000 + 1)) / 2);
        diff(&f.get_from(), &f.mnt);
    }

    fn read_modify_write(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        {
            let fh = OpenOptions::new().write(true).open(&file1).unwrap();
            let nbytes = fh.write_at("*".as_bytes(), 9).unwrap();
            assert_eq!(nbytes, 1);
        }

        let mut s = String::new();
        File::open(&file1).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "dir1/file*\n");
        diff(&f.get_from(), &f.mnt);
    }

    fn write_twice(f: &CatFSTests) {
        for _ in 0..2 {
            let mut fh = OpenOptions::new().write(true).create(true)
                .open(Path::new(&f.mnt).join("foo")).unwrap();
            fh.write_all(b"hello world").unwrap();
        }

        fs::symlink_metadata(&f.get_from().join("foo")).unwrap();
        diff(&f.get_from(), &f.mnt);
    }

    fn unlink_one(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        fs::remove_file(&file1).unwrap();
        if let Err(e) = fs::symlink_metadata(&file1) {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("{:?} still exists", file1);
        }
        diff(&f.get_from(), &f.mnt);
    }

    fn read_unlink(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        {
            let mut s = String::new();
            File::open(&file1).unwrap().read_to_string(&mut s).unwrap();
            assert_eq!(s, "dir1/file1\n");
        }
        fs::remove_file(&file1).unwrap();
        if let Err(e) = fs::symlink_metadata(&file1) {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("{:?} still exists", file1);
        }
        diff(&f.get_from(), &f.mnt);
    }

    fn mkdir(f: &CatFSTests) {
        let foo = f.mnt.join("foo");
        fs::create_dir(&foo).unwrap();
    }

    fn rmdir(f: &CatFSTests) {
        let dir2 = f.mnt.join("dir2");
        fs::remove_dir(&dir2).unwrap();
        if let Err(e) = fs::symlink_metadata(&dir2) {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("{:?} still exists", dir2);
        }
        diff(&f.get_from(), &f.mnt);
    }

    fn rmdir_not_empty(f: &CatFSTests) {
        let dir1 = f.mnt.join("dir1");
        if let Err(e) = fs::remove_dir(&dir1) {
            assert_eq!(e.raw_os_error().unwrap(), libc::ENOTEMPTY);
        } else {
            panic!("{:?} deleted", dir1);
        }
    }

    fn check_dirty(f: &CatFSTests) {
        let foo = f.mnt.join("foo");
        let foo_cache = f.get_cache().join("foo");
        {
            let mut fh = OpenOptions::new().write(true).create(true)
                .open(&foo).unwrap();
            fh.write_all(b"hello world").unwrap();

            // at this point the file is NOT flushed yet, so pristine
            // should not be set
            assert!(xattr::get(&foo_cache, "user.catfs.pristine").unwrap().is_none());
        }

        let v = xattr::get(&foo_cache, "user.catfs.pristine").unwrap().unwrap();
        assert_eq!(v, catfs::catfs::file::PRISTINE);

        {
            let mut fh = OpenOptions::new().write(true)
                .open(&foo).unwrap();
            fh.write_all(b"hello world").unwrap();

            // at this point the file is NOT flushed yet, so pristine
            // should be dirty
            let v = xattr::get(&foo_cache, "user.catfs.pristine").unwrap().unwrap();
            assert_eq!(v, catfs::catfs::file::DIRTY);
        }

        let v = xattr::get(&foo_cache, "user.catfs.pristine").unwrap().unwrap();
        assert_eq!(v, catfs::catfs::file::PRISTINE);
    }

    fn create_pristine(f: &CatFSTests) {
        let foo = Path::new(&f.mnt).join("foo");
        {
            let mut wh = OpenOptions::new().write(true).create(true)
                .open(&foo).unwrap();
            wh.write_all(b"hello world").unwrap();

            // we haven't closed the file yet, but we should still be
            // reading from it
            let mut contents = String::new();
            let mut rh = OpenOptions::new().read(true).open(&foo).unwrap();
            rh.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "hello world");
        }
    }
}
