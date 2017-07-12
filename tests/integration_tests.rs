#[allow(unused_imports)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate catfs;
extern crate rand;
extern crate fuse;

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Result;
use std::path::Path;
use std::process::Command;
use rand::{thread_rng, Rng};

use catfs::CatFS;

#[macro_use]
mod test_suite;


trait Fixture {
    fn setup() -> Result<Self>
    where
        Self: std::marker::Sized;
    fn teardown(self) -> Result<()>;
}

struct CatFSTests<'a> {
    prefix: OsString,
    mnt: OsString,
    session: fuse::BackgroundSession<'a>,
}

fn copy_all(dir1: &Path, dir2: &Path) -> io::Result<()> {
    fs::create_dir(dir2)?;

    for entry in fs::read_dir(dir1)? {
        let entry = entry?;
        let mut to = dir2.to_path_buf();
        to.push(entry.file_name());

        if entry.file_type()?.is_dir() {
            copy_all(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), to)?;
        }
    }

    return Ok(());
}

impl<'a> CatFSTests<'a> {
    fn get_orig_dir() -> OsString {
        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        let mut test_resource_dir = manifest.clone();
        test_resource_dir.push("/tests/resources");
        return test_resource_dir;
    }

    fn get_from(&self) -> OsString {
        let mut from = self.prefix.clone();
        from.push("/resources");
        return from;
    }
}

impl<'a> Fixture for CatFSTests<'a> {
    fn setup() -> Result<CatFSTests<'a>> {
        env_logger::init();

        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        let mut prefix = manifest.clone();
        prefix.push("/target/test/");
        prefix.push(thread_rng().gen_ascii_chars().take(10).collect::<String>());
        let mut mnt = prefix.clone();
        mnt.push("/mnt");
        let mut resources = prefix.clone();
        resources.push("/resources");
        let mut cache = prefix.clone();
        cache.push("/cache");

        fs::create_dir_all(&mnt)?;
        fs::create_dir_all(&cache)?;

        copy_all(CatFSTests::get_orig_dir().as_ref(), resources.as_ref())?;

        let fs = CatFS::new(&resources, &cache)?;

        let session = unsafe { fuse::spawn_mount(fs, &mnt, &[])? };

        let t = CatFSTests {
            prefix: prefix,
            mnt: mnt,
            session: session,
        };

        return Ok(t);
    }

    fn teardown(self) -> Result<()> {
        {
            // move out the session to let us umount first
            #[allow(unused_variables)]
            let session = self.session;
        }
        fs::remove_dir_all(&self.prefix)?;
        return Ok(());
    }
}

fn diff(dir1: &OsStr, dir2: &OsStr) {
    let status = Command::new("diff")
        .arg("-r")
        .arg(dir1)
        .arg(dir2)
        .status()
        .expect("failed to execute `diff'");
    assert!(status.success());
}

unit_tests!{
    fn read_all(f: &CatFSTests) {
        diff(&CatFSTests::get_orig_dir(), &f.mnt);
    }

    fn create(f: &CatFSTests) {
        let mut fh = OpenOptions::new().write(true).create(true)
            .open(Path::new(&f.mnt).join("foo")).unwrap();
        //fs::symlink_metadata(&f.get_from()).unwrap();
    }
}
