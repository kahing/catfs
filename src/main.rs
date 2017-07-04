extern crate clap;
extern crate env_logger;
extern crate fuse;

use std::env;
use fuse::Filesystem;

struct CatFS;

impl Filesystem for CatFS {}

fn main() {
    env_logger::init().unwrap();



    let mountpoint = env::args_os().nth(1).unwrap();
    fuse::mount(CatFS, &mountpoint, &[]).unwrap();
}
