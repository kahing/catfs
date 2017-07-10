extern crate libc;

use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;

pub struct Handle {
    file: File,
    offset: u64,
}

// no-op to workaround the fact that we send the entire CatFS at start
// time, but we never send anything. Could have used Unique but that
// bounds us to rust nightly
unsafe impl Send for Handle {}

pub fn is_truncate(flags: u32) -> bool {
    return (flags & (libc::O_TRUNC as u32)) != 0;
}

impl Handle {
    fn flags_to_open_options(flags: i32) -> OpenOptions {
        let mut opt = OpenOptions::new();
        let access_mode = flags & libc::O_ACCMODE;

        if access_mode == libc::O_RDONLY {
            opt.read(true);
        } else if access_mode == libc::O_WRONLY {
            opt.write(true);
        } else if access_mode == libc::O_RDWR {
            opt.read(true).write(true);
        }

        opt.custom_flags(flags);

        return opt;
    }

    pub fn open(path: &OsStr, flags: u32) -> io::Result<Handle> {
        let opt = Handle::flags_to_open_options(flags as i32);
        let fh = opt.open(path)?;
        return Ok(Handle {
            file: fh,
            offset: 0,
        });
    }

    pub fn read(&mut self, offset: u64, buf: &mut Vec<u8>) -> io::Result<usize> {
        if self.offset != offset {
            self.offset = self.file.seek(SeekFrom::Start(offset))?;
        }

        let nwant = buf.len();
        let mut bytes_read: usize = 0;

        while bytes_read < nwant {
            match self.file.read(&mut buf[bytes_read..]) {
                Ok(nread) => {
                    if nread == 0 {
                        return Ok(bytes_read);
                    }
                    bytes_read += nread;
                }
                Err(e) => {
                    if bytes_read > 0 {
                        return Ok(bytes_read);
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        return Ok(bytes_read);
    }
}
