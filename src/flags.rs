extern crate clap;
extern crate libc;

use std::any::Any;
use std::env;
use std::ffi::OsString;

use catfs::flags::DiskSpace;

pub struct Flag<'a, 'b> {
    pub arg: clap::Arg<'a, 'a>,
    pub value: &'b mut dyn Any,
}

pub fn parse_options<'a, 'b>(mut app: clap::App<'a, 'a>, flags: &'b mut [Flag<'a, 'b>]) {
    for f in flags.iter() {
        app = app.arg(f.arg.clone());
    }
    let mut argv = env::args_os().collect::<Vec<OsString>>();
    if argv.len() == 5 && argv[3] == OsString::from("-o") {
	// looks like it's coming from fstab!
        // [0]: catfs, [1] = src_dir#cache_dir, [2] = mnt
        // [3]: -o, [4] = opt1,opt2
        // XXX str has split but OsString doesn't

        // XXX2 this is more convoluted than necessary because paths
        // immutably borrows from argv and we can't modify elements of
        // argv again because that requires another mutable borrow

        // XXX3 need to initialize src/cache because the compiler is
        // not smart enough
        let mut src: OsString = Default::default();
        let mut cache: OsString = Default::default();
        let mut is_fstab = false;

        {
            let paths = argv[1].to_str().unwrap().splitn(2, '#').collect::<Vec<&str>>();
            if paths.len() == 2 {
                src = OsString::from(paths[0]);
                cache = OsString::from(paths[1]);
                is_fstab = true;
            }
        }

        if is_fstab {
            argv[1] = src;
            argv.insert(2, cache);
            let mut options = String::new();
            let mut arguments: Vec<OsString> = Default::default();

            // options are now pushed down
            for opt in argv[5].to_str().unwrap().split(',') {
                if opt.starts_with("-") {
                    arguments.push(OsString::from(opt));
                } else {
                    options += opt;
                    options += ",";
                }
            }

            if options.len() != 0 {
                options.pop();
                argv[5] = OsString::from(options);
            } else {
                // no options, pop -o and empty string
                argv.pop();
                argv.pop();
            }
            for arg in arguments {
                argv.insert(3, arg);
            }
        }
    }

    let matches = app.get_matches_from(argv);

    for f in flags.iter_mut() {
        let name = f.arg.b.name;

        if matches.is_present(name) {
            // cannot use else if here or rust would claim double
            // mutable borrow because apparently a borrow sends with
            // the last else if
            if let Some(v) = f.value.downcast_mut::<String>() {
                let s = matches.value_of(name).unwrap();
                *v = String::from(s);
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<OsString>() {
                let s = matches.value_of_os(name).unwrap();
                *v = s.to_os_string();
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<bool>() {
                *v = true;
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<Vec<OsString>>() {
                let options = matches.values_of(name).unwrap();
                for s in options {
                    for s in s.split(',') {
                        v.push(OsString::from("-o"));
                        v.push(OsString::from(s));
                    }
                }
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<DiskSpace>() {
                let s = matches.value_of(name).unwrap();
                *v = s.parse().unwrap();
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<libc::uid_t>() {
                let s = matches.value_of(name).unwrap();
                *v = s.parse().unwrap();
                continue;
            }
            if let Some(v) = f.value.downcast_mut::<libc::gid_t>() {
                let s = matches.value_of(name).unwrap();
                *v = s.parse().unwrap();
                continue;
            }

            panic!("unknown type for {}", name);
        }
    }
}
