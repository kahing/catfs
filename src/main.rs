#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate libc;
#[macro_use]
extern crate log;
extern crate chan_signal;

use std::error::Error;
use std::ffi::OsStr;
use std::path::Path;
use std::str::FromStr;
use std::thread;

use chan_signal::Signal;
use clap::{App, Arg};

mod catfs;
mod flags;
mod evicter;

use catfs::error;
use catfs::flags::{DiskSpace, FlagStorage};
use catfs::rlibc;

fn main() {
    if let Err(e) = main_internal() {
        error!("Cannot mount: {}", e);
        std::process::exit(1);
    }
}

fn main_internal() -> error::Result<()> {
    env_logger::init().unwrap();

    let mut flags: FlagStorage = Default::default();
    let mut test = false;

    let app = App::new("catfs")
        .about("Cache Anything FileSystem")
        .version(crate_version!());

    {
        fn diskspace_validator(s: String) -> Result<(), String> {
            DiskSpace::from_str(&s).map(|_| ()).map_err(
                |e| e.to_str().to_owned(),
            )
        }

        fn path_validator(s: String) -> Result<(), String> {
            Path::new(&s)
                .canonicalize()
                .map_err(|e| e.description().to_owned())
                .and_then(|p| if p.is_dir() {
                    Ok(())
                } else {
                    Err("is not a directory".to_owned())
                })
        }

        let mut args = [
            flags::Flag {
                arg: Arg::with_name("space")
                    .long("free")
                    .takes_value(true)
                    .help(
                        "Ensure filesystem has at least this much free space. (ex: 9.5%, 10G)",
                    )
                    .validator(diskspace_validator),
                value: &mut flags.free_space,
            },
            flags::Flag {
                arg: Arg::with_name("foreground").short("f").help(
                    "Run catfs in foreground.",
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
                arg: Arg::with_name("test").long("test").help(
                    "Exit after parsing arguments",
                ),
                value: &mut test,
            },
            flags::Flag {
                arg: Arg::with_name("from")
                    .index(1)
                    .required(true)
                    .help("Cache files from this directory.")
                    .validator(path_validator),
                value: &mut flags.cat_from,
            },
            flags::Flag {
                arg: Arg::with_name("to")
                    .index(2)
                    .required(true)
                    .help("Cache files to this directory.")
                    .validator(path_validator),
                value: &mut flags.cat_to,
            },
            flags::Flag {
                arg: Arg::with_name("mountpoint")
                    .index(3)
                    .required(true)
                    .help("Expose the mount point at this directory.")
                    .validator(path_validator),
                value: &mut flags.mount_point,
            },
        ];


        flags::parse_options(app, &mut args);
    }

    if test {
        return Ok(());
    }

    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    let path_from = Path::new(&flags.cat_from).canonicalize()?;
    let path_to = Path::new(&flags.cat_to).canonicalize()?;
    let fs = catfs::CatFS::new(&path_from, &path_to)?;
    let cache_dir = fs.get_cache_dir()?;
    let mut options: Vec<&OsStr> = Vec::new();
    for i in 0..flags.mount_options.len() {
        options.push(&flags.mount_options[i]);
    }

    {
        let mut session = fuse::Session::new(fs, Path::new(&flags.mount_point), &options)?;
        unsafe {
            thread::spawn(move || {
                if let Err(e) = session.run() {
                    error!("session.run() = {}", e);
                }
                libc::kill(0, libc::SIGTERM);
            });
        }
        let mut ev = evicter::Evicter::new(cache_dir, &flags.free_space);
        ev.run();
        // unmount after we get signaled becausep session will go out of scope
        signal.recv().unwrap();
    }
    rlibc::close(cache_dir)?;
    return Ok(());
}
