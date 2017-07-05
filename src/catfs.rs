extern crate fuse;

use self::fuse::Filesystem;

struct CatFS;

impl Filesystem for CatFS {}
