extern crate backtrace;
extern crate libc;

use std::fmt;
use std::ops::Deref;
use std::io;
use std::string::FromUtf8Error;

use self::backtrace::Backtrace;
use self::backtrace::BacktraceFrame;

#[derive(Debug)]
pub struct RError<E> {
    e: E,
    bt: Option<Backtrace>,
}

pub fn is_enoent(e: &io::Error) -> bool {
    return e.kind() == io::ErrorKind::NotFound;
}

pub fn try_enoent(e: io::Error) -> Result<bool> {
    if is_enoent(&e) {
        return Ok(true);
    } else {
        return Err(RError::from(e));
    }
}

pub fn propagate<T>(e: io::Error) -> Result<T> {
    return Err(RError::propagate(e));
}

pub fn errno(e: &RError<io::Error>) -> libc::c_int {
    if RError::expected(e) {
        return e.e.raw_os_error().unwrap();
    } else {
        return libc::EIO;
    }
}


impl<E> RError<E> {
    pub fn propagate(e: E) -> RError<E> {
        RError {
            e: e,
            bt: Default::default(),
        }
    }

    pub fn from(e: E) -> RError<E> {
        let mut bt = Backtrace::new();
        let mut i: usize = 0;
        let mut chop: usize = 0;
        for f in bt.frames() {
            if let Some(p) = f.symbols()[0].filename() {
                if p.file_name().unwrap() == "error.rs" {
                    chop = i;
                    break;
                }
            }
            i += 1;
        }

        if chop != 0 {
            let mut frames: Vec<BacktraceFrame> = bt.into();
            let _: Vec<_> = frames.drain(0..i).collect();
            bt = Backtrace::from(frames);
        }

        RError { e: e, bt: Some(bt) }
    }

    fn expected(&self) -> bool {
        return self.bt.is_none();
    }
}

impl RError<io::Error> {
    pub fn errno(&self) -> i32 {
        return self.e.raw_os_error().unwrap();
    }
}

impl fmt::Display for RError<io::Error> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.bt {
            Some(ref bt) => write!(f, "{} {:?}", self.e, bt),
            None => write!(f, "{}", self.e),
        }

    }
}

impl<E> Deref for RError<E> {
    type Target = E;

    fn deref(&self) -> &E {
        &self.e
    }
}

// XXX not really a clone
impl Clone for RError<io::Error> {
    fn clone(&self) -> Self {
        RError {
            e: io::Error::from_raw_os_error(self.e.raw_os_error().unwrap()),
            bt: Default::default(),
        }
    }
}

impl From<io::Error> for RError<io::Error> {
    fn from(e: io::Error) -> RError<io::Error> {
        RError::from(e)
    }
}

impl From<FromUtf8Error> for RError<FromUtf8Error> {
    fn from(e: FromUtf8Error) -> RError<FromUtf8Error> {
        RError::from(e)
    }
}

pub type Result<T> = ::std::result::Result<T, RError<io::Error>>;
