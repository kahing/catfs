#[allow(unused_imports)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate catfs;
extern crate rand;
extern crate fuse;

use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Result;
use std::process::Command;
use rand::{thread_rng, Rng};
use std::path::{Path, PathBuf};

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
    prefix: PathBuf,
    mnt: PathBuf,
    session: fuse::BackgroundSession<'a>,
}

fn copy_all(dir1: &AsRef<Path>, dir2: &AsRef<Path>) -> io::Result<()> {
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
        return self.prefix.join("resources");
    }
}

impl<'a> Fixture for CatFSTests<'a> {
    fn setup() -> Result<CatFSTests<'a>> {
        env_logger::init();

        let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
        let prefix = PathBuf::from(manifest).join("target/test").join(
            thread_rng().gen_ascii_chars().take(10).collect::<String>(),
        );
        let mnt = prefix.join("mnt");
        let resources = prefix.join("resources");
        let cache = prefix.join("cache");

        fs::create_dir_all(&mnt)?;
        fs::create_dir_all(&cache)?;

        copy_all(&CatFSTests::get_orig_dir(), &resources)?;

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

fn diff(dir1: &AsRef<Path>, dir2: &AsRef<Path>) {
    let status = Command::new("diff")
        .arg("-r")
        .arg(dir1.as_ref().as_os_str())
        .arg(dir2.as_ref().as_os_str())
        .status()
        .expect("failed to execute `diff'");
    assert!(status.success());
}

unit_tests!{
    fn read_all(f: &CatFSTests) {
        diff(&CatFSTests::get_orig_dir(), &f.mnt);
    }

    fn create(f: &CatFSTests) {
        {
            let mut fh = OpenOptions::new().write(true).create(true)
                .open(Path::new(&f.mnt).join("foo")).unwrap();
        }
        fs::symlink_metadata(f.get_from().join("foo")).unwrap();
    }
}
