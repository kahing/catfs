use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

pub trait Substr<T: ?Sized> {
    fn substr(&self, begin_inclusive: usize, end_exclusive: usize) -> &T;
}

impl Substr<OsStr> for OsStr {
    fn substr(&self, begin_inclusive: usize, end_exclusive: usize) -> &OsStr {
        OsStr::from_bytes(&self.as_bytes()[begin_inclusive..end_exclusive])
    }
}

impl Substr<Path> for Path {
    fn substr(&self, begin_inclusive: usize, end_exclusive: usize) -> &Path {
        Path::new(self.as_os_str().substr(begin_inclusive, end_exclusive))
    }
}
