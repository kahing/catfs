extern crate clap;

use std::collections::HashMap;

use clap::{App, Arg, SubCommand};


struct FlagStorage {
    cat_from: String,
    cat_to: String,
    mount_point: String,
    mount_options: HashMap<String, String>,
    foreground: bool,
}

pub fn add_options<'a, 'b>(app: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
    return app.arg(Arg::from_usage(
        "-o 'Additional system-specific mount options. Be careful!'",
    ));
}
//pub fn add_options(app: u32) {}
