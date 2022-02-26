extern crate chan_signal;
#[macro_use]
extern crate clap;
extern crate daemonize;
extern crate env_logger;
extern crate fuser;
extern crate libc;
#[macro_use]
extern crate log;
extern crate syslog;
extern crate chrono;

use std::env;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::io;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

use chan_signal::Signal;
use clap::{App, Arg};
use daemonize::{Daemonize};
use env_logger::LogBuilder;
use log::LogRecord;
use syslog::{Facility,Severity};
use chrono::offset::Local;
use chrono::DateTime;

mod pcatfs;
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

static mut SYSLOG: bool = false;
static mut SYSLOGGER: Option<Box<syslog::Logger>> = None;

fn main_internal() -> error::Result<()> {
    let format = |record: &LogRecord| {
        let t = time::SystemTime::now();
        let t = DateTime::<Local>::from(t);
        let syslog: bool;
        unsafe {
            syslog = SYSLOG;
        }
        if !syslog {
            format!(
                "{} {:5} - {}",
                t.format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        } else {
            unsafe {
                if let Some(ref logger) = SYSLOGGER {
                    let level = match record.level() {
                        log::LogLevel::Trace => Severity::LOG_DEBUG,
                        log::LogLevel::Debug => Severity::LOG_DEBUG,
                        log::LogLevel::Info => Severity::LOG_INFO,
                        log::LogLevel::Warn => Severity::LOG_WARNING,
                        log::LogLevel::Error => Severity::LOG_ERR,
                    };
                    let msg = format!("{}", record.args());
                    for line in msg.split('\n') {
                        // ignore error if we can't log, not much we can do anyway
                        let _ = logger.send_3164(level, line);
                    }
                }
            }
            format!("\u{08}")
        }
    };

    let mut builder = LogBuilder::new();
    builder.format(format);

    if env::var("RUST_LOG").is_ok() {
        builder.parse(&env::var("RUST_LOG").unwrap());
    } else {
        // default to info
        builder.parse("info");
    }

    builder.init().unwrap();

    let mut flags: FlagStorage = Default::default();
    let mut test = false;

    flags.mount_options.push(OsString::from("-o"));
    flags.mount_options.push(OsString::from("atomic_o_trunc"));
    flags.mount_options.push(OsString::from("-o"));
    flags.mount_options.push(
        OsString::from("default_permissions"),
    );

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
                .map_err(|e| e.to_string().to_owned())
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
            flags::Flag{
                arg: Arg::with_name("uid")
                    .long("uid")
                    .takes_value(true)
                    .help("Run as this uid"),
                value: &mut flags.uid,
            },
            flags::Flag{
                arg: Arg::with_name("gid")
                    .long("gid")
                    .takes_value(true)
                    .help("Run as this gid"),
                value: &mut flags.gid,
            },
            flags::Flag {
                arg: Arg::with_name("option")
                    .short("o")
                    .takes_value(true)
                    .multiple(true)
                    .help("Additional system-specific mount options. Be careful!"),
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

    if flags.gid != 0 {
        rlibc::setgid(flags.gid)?;
    }
    if flags.uid != 0 {
        rlibc::setuid(flags.uid)?;
    }

    if !flags.foreground {
        let daemonize = Daemonize::new()
            .working_directory(env::current_dir()?.as_path())
            ;

        match daemonize.start() {
            Ok(_) => {
                if let Ok(mut logger) = syslog::unix(Facility::LOG_USER) {
                    unsafe {
                        logger.set_process_name("catfs".to_string());
                        logger.set_process_id(libc::getpid());
                        SYSLOGGER = Some(logger);
                        SYSLOG = true;
                    }
                }
            },
            Err(e) => error!("unable to daemonize: {}", e),
        }
    }

    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    let path_from = Path::new(&flags.cat_from).canonicalize()?;
    let path_to = Path::new(&flags.cat_to).canonicalize()?;
    let fs = catfs::CatFS::new(&path_from, &path_to)?;
    let fs = pcatfs::PCatFS::new(fs);
    let cache_dir = fs.get_cache_dir()?;
    let mut options: Vec<&OsStr> = Vec::new();
    for i in 0..flags.mount_options.len() {
        options.push(&flags.mount_options[i]);
    }

    debug!("options are {:?}", flags.mount_options);

    {
        let mut session = fuser::Session::new(fs, Path::new(&flags.mount_point), &options)?;
        let need_unmount = Arc::new(Mutex::new(true));
        let need_unmount2 = need_unmount.clone();
        thread::spawn(move || {
            if let Err(e) = session.run() {
                error!("session.run() = {}", e);
            }
            info!("{:?} unmounted", session.mountpoint());
            let mut need_unmount = need_unmount2.lock().unwrap();
            *need_unmount = false;
            unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
        });

        let mut ev = evicter::Evicter::new(cache_dir, &flags.free_space);
        ev.run();
        // unmount after we get signaled becausep session will go out of scope
        let s = signal.recv().unwrap();
        info!(
            "Received {:?}, attempting to unmount {:?}",
            s,
            flags.mount_point
        );
        let need_unmount = need_unmount.lock().unwrap();
        if *need_unmount {
            unmount(Path::new(&flags.mount_point))?;
        }
    }
    rlibc::close(cache_dir)?;
    return Ok(());
}

use libc::{c_char, c_int};
use std::ffi::{CString, CStr};
/// Unmount an arbitrary mount point
pub fn unmount(mountpoint: &Path) -> io::Result<()> {
    // fuse_unmount_compat22 unfortunately doesn't return a status. Additionally,
    // it attempts to call realpath, which in turn calls into the filesystem. So
    // if the filesystem returns an error, the unmount does not take place, with
    // no indication of the error available to the caller. So we call unmount
    // directly, which is what osxfuse does anyway, since we already converted
    // to the real path when we first mounted.

    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly",
                target_os = "openbsd", target_os = "bitrig", target_os = "netbsd"))]
    #[inline]
    fn libc_umount(mnt: &CStr) -> c_int {
        unsafe { libc::unmount(mnt.as_ptr(), 0) }
    }

    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly",
                      target_os = "openbsd", target_os = "bitrig", target_os = "netbsd")))]
    #[inline]
    fn libc_umount(mnt: &CStr) -> c_int {
        use std::io::ErrorKind::PermissionDenied;

        let rc = unsafe { libc::umount(mnt.as_ptr()) };
        if rc < 0 && io::Error::last_os_error().kind() == PermissionDenied {
            // Linux always returns EPERM for non-root users.  We have to let the
            // library go through the setuid-root "fusermount -u" to unmount.
            unsafe {
                fuse_unmount_compat22(mnt.as_ptr());
            }
            0
        } else {
            rc
        }
    }

    let mnt = CString::new(mountpoint.as_os_str().as_bytes())?;
    let rc = libc_umount(&mnt);
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

extern "system" {
    pub fn fuse_unmount_compat22(mountpoint: *const c_char);
}
