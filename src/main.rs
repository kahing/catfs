#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate fuse;
#[macro_use]
extern crate log;
extern crate chan_signal;

use std::path::Path;

use chan_signal::Signal;
use clap::{App, Arg};

mod catfs;
mod flags;
mod evicter;

use catfs::error;
use catfs::flags::FlagStorage;
use catfs::rlibc;

fn main() {
    if let Err(e) = main_internal() {
        error!("Cannot mount: {}", e);
    }
}

fn main_internal() -> error::Result<()> {
    env_logger::init().unwrap();

    let mut flags: FlagStorage = Default::default();

    let app = App::new("catfs")
        .about("Cache Anything FileSystem")
        .version(crate_version!());

    {
        let mut args = [
            flags::Flag {
                arg: Arg::with_name("space").long("free").takes_value(true).help(
                    "Ensure filesystem has at least this much free space. (ex: 9.5%, 10G)",
                ),
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
                arg: Arg::with_name("from").index(1).required(true).help(
                    "Cache files from this directory.",
                ),
                value: &mut flags.cat_from,
            },
            flags::Flag {
                arg: Arg::with_name("to").index(2).required(true).help(
                    "Cache files to this directory.",
                ),
                value: &mut flags.cat_to,
            },
            flags::Flag {
                arg: Arg::with_name("mountpoint").index(3).required(true).help(
                    "Expose the mount point at this directory.",
                ),
                value: &mut flags.mount_point,
            },
        ];


        flags::parse_options(app, &mut args);
    }

    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    let path_from = Path::new(&flags.cat_from).canonicalize()?;
    let path_to = Path::new(&flags.cat_to).canonicalize()?;
    let fs = catfs::CatFS::new(&path_from, &path_to)?;
    let cache_dir = fs.get_cache_dir()?;
    {
        let _session: fuse::BackgroundSession;
        unsafe {
            _session = fuse::spawn_mount(fs, &flags.mount_point, &[])?;
        }
        let _ev = evicter::Evicter::new(cache_dir, &flags.free_space);
        // unmount after we get signaled becausep session will go out of scope
        signal.recv().unwrap();
    }
    rlibc::close(cache_dir)?;
    return Ok(());
}
