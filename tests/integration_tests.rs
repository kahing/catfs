#[allow(unused_imports)]
#[macro_use]
extern crate log;
extern crate libc;
extern crate env_logger;
extern crate fuser;
extern crate xattr;
extern crate chrono;

use std::env;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, Write};
use std::process::{Command, Stdio};
use std::path::{Path, PathBuf};
use std::os::unix::fs::FileExt;
use std::time;

use env_logger::LogBuilder;
use log::LogRecord;
use chrono::offset::Local;
use chrono::DateTime;

extern crate catfs;

use catfs::CatFS;
use catfs::catfs::error;
use catfs::catfs::flags::DiskSpace;
use catfs::catfs::file;
use catfs::catfs::rlibc;
use catfs::evicter::Evicter;
use catfs::pcatfs::PCatFS;

#[macro_use]
mod test_suite;


trait Fixture {
    fn setup() -> error::Result<Self>
    where
        Self: std::marker::Sized;
    fn init(&mut self) -> error::Result<()>;
    fn teardown(self) -> error::Result<()>;
}

struct CatFSTests {
    prefix: PathBuf,
    mnt: PathBuf,
    src: PathBuf,
    cache: PathBuf,
    session: Option<fuser::BackgroundSession>,
    evicter: Option<Evicter>,
    nested: Option<Box<CatFSTests>>,
}

impl CatFSTests {
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

    fn mount(&self) -> error::Result<(fuser::BackgroundSession, Evicter)> {
        let fs = CatFS::new(&self.src, &self.cache)?;
        let fs = PCatFS::new(fs);

        let cache_dir = fs.get_cache_dir()?;
        // essentially no-op, but ensures that it starts and terminates
        let ev = Evicter::new(cache_dir, &DiskSpace::Bytes(1));
        return Ok((unsafe { fuser::spawn_mount(fs, &self.mnt, &[])? }, ev));
    }

    fn assert_cache_valid(&self, path: &dyn AsRef<Path>) {
        let src_dir = rlibc::open(&self.src, rlibc::O_RDONLY, 0).unwrap();
        let cache_dir = rlibc::open(&self.cache, rlibc::O_RDONLY, 0).unwrap();

        assert!(file::Handle::validate_cache(src_dir, cache_dir, path, false, true).unwrap());
        rlibc::close(src_dir).unwrap();
        rlibc::close(cache_dir).unwrap();
    }
}

impl Fixture for CatFSTests {
    fn setup() -> error::Result<CatFSTests> {
        let format = |record: &LogRecord| {
            let t = time::SystemTime::now();
            let t = DateTime::<Local>::from(t);
            format!(
                "{} {:5} - {}",
                t.format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        };

        let mut builder = LogBuilder::new();
        builder.format(format);

        if env::var("RUST_LOG").is_ok() {
            builder.parse(&env::var("RUST_LOG").unwrap());
        }

        let _ = builder.init();

        let _ = fs::create_dir(CatFSTests::get_orig_dir().join("dir2"));

        let prefix = catfs::catfs::tests::copy_resources();
        let mnt = prefix.join("mnt");
        let resources = prefix.join("resources");
        let cache = prefix.join("cache");

        fs::create_dir_all(&mnt)?;
        fs::create_dir_all(&cache)?;

        let t = CatFSTests {
            prefix: prefix,
            mnt: mnt,
            src: resources,
            cache: cache,
            session: Default::default(),
            evicter: Default::default(),
            nested: Default::default(),
        };

        if let Some(v) = env::var_os("CATFS_SELF_HOST") {
            if v == OsStr::new("1") || v == OsStr::new("true") {
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

                let t2 = CatFSTests {
                    prefix: t.prefix.clone(),
                    mnt: mnt2,
                    src: mnt,
                    cache: cache2,
                    session: Default::default(),
                    evicter: Default::default(),
                    nested: Some(Box::new(t)),
                };

                return Ok(t2);
            }
        }

        return Ok(t);
    }

    fn init(&mut self) -> error::Result<()> {
        if let Some(ref mut t) = self.nested {
            t.init()?;
        }

        let (session, ev) = self.mount()?;
        self.session = Some(session);
        self.evicter = Some(ev);
        if let Some(ref mut ev) = self.evicter {
            ev.run();
        }
        return Ok(());
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

fn diff(dir1: &dyn AsRef<Path>, dir2: &dyn AsRef<Path>) {
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
            .arg("bs=1048576").arg("count=10")
            .stderr(Stdio::null())
            .status().expect("failed to execute `dd'");
        assert!(status.success());

        let foo = fs::symlink_metadata(&f.get_from().join("foo")).unwrap();
        assert_eq!(foo.len(), 10 * 1024 * 1024);
        diff(&f.get_from(), &f.mnt);
    }

    fn large_seek(f: &CatFSTests) {
        let mut file = OpenOptions::new().write(true).create(true).open(f.mnt.join("foo")).unwrap();
        let offset = 2 * 1024 * 1024 * 1024;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all(b"x").unwrap();

        let foo = fs::symlink_metadata(&f.get_from().join("foo")).unwrap();
        assert_eq!(foo.len(), offset + 1);
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
            let num: u32 = entry.unwrap().file_name().to_str().unwrap().parse().unwrap();
            total += num;
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

    fn read_unlink_while_open(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        let mut s = String::new();
        {
            let mut f = File::open(&file1).unwrap();
            f.read_to_string(&mut s).unwrap();
            assert_eq!(s, "dir1/file1\n");

            fs::remove_file(&file1).unwrap();
        }
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

    fn checksum_str(f: &CatFSTests) {
        if let Some(v) = env::var_os("CATFS_SELF_HOST") {
            if v == OsStr::new("1") || v == OsStr::new("true") {
                // skip this test since we don't support xattr for now
                return;
            }
        }

        let foo = f.src.join("file1");
        xattr::set(&foo, "user.catfs.random", b"hello").unwrap();
        rlibc::utimes(&foo, 0, 100000000).unwrap();
        let mut fh = rlibc::File::open(&foo, rlibc::O_RDONLY, 0).unwrap();
        let s = file::Handle::src_str_to_checksum(&fh).unwrap();
        assert_eq!(s, OsStr::new("100000000\n6\n"));
        fh.close().unwrap();
    }

    fn check_dirty(f: &CatFSTests) {
        let foo = f.mnt.join("foo");
        let foo_cache = f.get_cache().join("foo");
        {
            let mut fh = OpenOptions::new().write(true).create(true)
                .open(&foo).unwrap();
            fh.write_all(b"hello").unwrap();

            // at this point the file is NOT flushed yet, so pristine
            // should not be set
            assert!(!xattr::get(&foo_cache, "user.catfs.src_chksum").unwrap().is_some());
        }

        f.assert_cache_valid(&Path::new("foo"));
        let mut contents = String::new();
        let mut rh = OpenOptions::new().read(true).open(&foo).unwrap();
        rh.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "hello");

        {
            let mut fh = OpenOptions::new().write(true)
                .open(&foo).unwrap();
            fh.write_all(b"world").unwrap();

            // at this point the file is NOT flushed yet, so pristine
            // should be dirty
            assert!(!xattr::get(&foo_cache, "user.catfs.src_chksum").unwrap().is_some());
        }

        f.assert_cache_valid(&Path::new("foo"));
        let mut contents = String::new();
        let mut rh = OpenOptions::new().read(true).open(&foo).unwrap();
        rh.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "world");
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

    fn rename_one(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        let file_rename = f.mnt.join("dir1/file1_rename");
        fs::rename(&file1, &file_rename).unwrap();
        if let Err(e) = fs::symlink_metadata(&file1) {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("{:?} still exists", file1);
        }
        fs::symlink_metadata(&file_rename).unwrap();
        diff(&f.get_from(), &f.mnt);
    }

    fn read_rename(f: &CatFSTests) {
        let file1 = f.mnt.join("dir1/file1");
        {
            let mut s = String::new();
            File::open(&file1).unwrap().read_to_string(&mut s).unwrap();
            assert_eq!(s, "dir1/file1\n");
        }
        let file_rename = f.mnt.join("dir1/file1_rename");
        fs::rename(&file1, &file_rename).unwrap();
        if let Err(e) = fs::symlink_metadata(&file1) {
            assert_eq!(e.kind(), io::ErrorKind::NotFound);
        } else {
            panic!("{:?} still exists", file1);
        }
        fs::symlink_metadata(&file_rename).unwrap();
        diff(&f.get_from(), &f.mnt);
    }

    fn chmod(f: &CatFSTests) {
        let file1 = f.mnt.join("file1");
        let mut perm = fs::symlink_metadata(&file1).unwrap().permissions();
        assert!(!perm.readonly());
        perm.set_readonly(true);
        fs::set_permissions(&file1, perm).unwrap();
        perm = fs::symlink_metadata(&file1).unwrap().permissions();
        assert!(perm.readonly());
    }

    fn read_chmod(f: &CatFSTests) {
        let file1 = f.mnt.join("file1");
        {
            let mut s = String::new();
            File::open(&file1).unwrap().read_to_string(&mut s).unwrap();
            assert_eq!(s, "file1\n");
        }

        let mut perm = fs::symlink_metadata(&file1).unwrap().permissions();
        assert!(!perm.readonly());
        perm.set_readonly(true);
        fs::set_permissions(&file1, perm).unwrap();
        perm = fs::symlink_metadata(&file1).unwrap().permissions();
        assert!(perm.readonly());
        f.assert_cache_valid(&Path::new("file1"));
    }

    fn prefetch_canceled(f: &CatFSTests) {
        let file1 = f.mnt.join("file1");
        {
            let _ = File::open(&file1).unwrap();
        }

        {
            let file1 = Path::new(&f.cache).join("file1");
            let mut fh = OpenOptions::new().write(true).truncate(true).create(true)
                .open(&file1).unwrap();
            fh.write_all(b"f").unwrap();
            let _ = xattr::remove(&file1, "user.catfs.src_chksum");
        }

        {
            let mut s = String::new();
            let mut f = File::open(&file1).unwrap();
            f.read_to_string(&mut s).unwrap();
            assert_eq!(s, "file1\n");
        }
    }
}
