//extern crate mopa;
extern crate clap;

use std::any::Any;
use std::collections::HashMap;
use std::ffi::OsString;

use catfs::flags::DiskSpace;

pub struct Flag<'a, 'b> {
    pub arg: clap::Arg<'a, 'a>,
    pub value: &'b mut Any,
}

pub fn parse_options<'a, 'b>(mut app: clap::App<'a, 'a>, flags: &'b mut [Flag<'a, 'b>]) {
    for f in flags.iter() {
        app = app.arg(f.arg.clone());
    }
    let matches = app.get_matches();

    for f in flags.iter_mut() {
        let name = f.arg.b.name;

        if matches.is_present(name) {
            // cannot use else if here or rust would claim double mutable borrow
            if let Some(v) = f.value.downcast_mut::<String>() {
                let s = matches.value_of(name).unwrap();
                *v = String::from(s);
            }
            if let Some(v) = f.value.downcast_mut::<OsString>() {
                let s = matches.value_of_os(name).unwrap();
                *v = s.to_os_string();
            }
            if let Some(v) = f.value.downcast_mut::<bool>() {
                *v = true;
            }
            if let Some(_v) = f.value.downcast_mut::<HashMap<String, String>>() {
                // parse key=value
            }
            if let Some(v) = f.value.downcast_mut::<DiskSpace>() {
                let s = matches.value_of(name).unwrap();
                *v = s.parse().unwrap();
            }
        }
    }
}
