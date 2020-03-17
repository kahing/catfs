extern crate rand;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use self::rand::{thread_rng, Rng};
use catfs::error;

#[allow(dead_code)]
fn copy_all(dir1: &dyn AsRef<Path>, dir2: &dyn AsRef<Path>) -> error::Result<()> {
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


#[allow(dead_code)]
pub fn copy_resources() -> PathBuf {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let resources = manifest.join("tests/resources");

    let prefix = manifest.join("target/test").join(
        thread_rng()
            .gen_ascii_chars()
            .take(10)
            .collect::<String>(),
    );

    fs::create_dir_all(&prefix).unwrap();

    copy_all(&resources, &prefix.join("resources")).unwrap();
    return prefix;
}
