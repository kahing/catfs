#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate fuse;

use std::env;
use std::collections::HashMap;

use clap::{App, Arg};
use fuse::Filesystem;

struct CatFS;

impl Filesystem for CatFS {}

mod flags;

struct FlagStorage {
    cat_from: Option<String>,
    cat_to: Option<String>,
    mount_point: Option<String>,
    mount_options: HashMap<String, String>,
    foreground: bool,
}

impl Default for FlagStorage {
    fn default() -> FlagStorage {
        return FlagStorage {
            cat_from: None,
            cat_to: None,
            mount_point: None,
            mount_options: HashMap::new(),
            foreground: false,
        };
    }
}

fn main() {
    env_logger::init().unwrap();

    let mut flags = FlagStorage { ..Default::default() };

    let mut app = App::new("catfs")
        .about("Cache Anything FileSystem")
        .version(crate_version!());

    {
        let mut args = [
            flags::Flag {
                arg: Arg::with_name("foreground").short("f").help(
                    "Run catfs in foreground",
                ),
                value: &mut flags.foreground,
            },
            flags::Flag {
                arg: Arg::with_name("option").short("o").takes_value(true).help(
                    "Additional system-specific mount options. Be careful!",
                ),
                value: &mut flags.mount_options,
            },
        ];


        app = flags::add_options(app, &args);

        flags::parse_options(app, &mut args);
    }

    if flags.foreground {
        println!("foreground on");
    }

    //let mountpoint = env::args_os().nth(1).unwrap();
    //fuse::mount(CatFS, &mountpoint, &[]).unwrap();
}
