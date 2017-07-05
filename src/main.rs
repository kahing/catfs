#[macro_use]
extern crate clap;
extern crate env_logger;

use std::collections::HashMap;

use clap::{App, Arg};

mod flags;
mod catfs;

struct FlagStorage {
    cat_from: String,
    cat_to: String,
    mount_point: String,
    mount_options: HashMap<String, String>,
    foreground: bool,
}

impl Default for FlagStorage {
    fn default() -> FlagStorage {
        return FlagStorage {
            cat_from: String::from(""),
            cat_to: String::from(""),
            mount_point: String::from(""),
            mount_options: HashMap::new(),
            foreground: false,
        };
    }
}

fn main() {
    env_logger::init().unwrap();

    let mut flags = FlagStorage { ..Default::default() };

    let app = App::new("catfs")
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
            flags::Flag {
                arg: Arg::with_name("from").index(1).required(true).help(
                    "Cache files from this directory",
                ),
                value: &mut flags.cat_from,
            },
            flags::Flag {
                arg: Arg::with_name("to").index(2).required(true).help(
                    "Cache files to this directory",
                ),
                value: &mut flags.cat_to,
            },
            flags::Flag {
                arg: Arg::with_name("mountpoint").index(3).required(true).help(
                    "Expose the mount point at this directory",
                ),
                value: &mut flags.mount_point,
            },
        ];


        flags::parse_options(app, &mut args);
    }

    if flags.foreground {
        println!("foreground on");
    }

    //let mountpoint = env::args_os().nth(1).unwrap();
    //fuse::mount(CatFS, &mountpoint, &[]).unwrap();
}
