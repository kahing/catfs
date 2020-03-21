extern crate libc;

use std::error::Error;
use std::ffi::OsString;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

#[derive(PartialEq)]
#[derive(Clone)]
#[derive(Debug)]
pub enum DiskSpace {
    Percent(f64),
    Bytes(u64),
}

impl Default for DiskSpace {
    fn default() -> DiskSpace {
        DiskSpace::Bytes(0)
    }
}

#[derive(Debug)]
pub struct DiskSpaceParseError(String);

impl DiskSpaceParseError {
    pub fn to_str(&self) -> &str {
        &self.0
    }
}

impl From<ParseIntError> for DiskSpaceParseError {
    fn from(e: ParseIntError) -> DiskSpaceParseError {
        return DiskSpaceParseError(e.description().to_string());
    }
}

impl From<ParseFloatError> for DiskSpaceParseError {
    fn from(e: ParseFloatError) -> DiskSpaceParseError {
        return DiskSpaceParseError(e.description().to_string());
    }
}

impl FromStr for DiskSpace {
    type Err = DiskSpaceParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with('%') {
            return Ok(DiskSpace::Percent(s[0..s.len() - 1].parse()?));
        } else {
            // interpret it as a byte size
            let unit = match s.chars().last().unwrap() {
                'T' => 1024 * 1024 * 1024 * 1024,
                'G' => 1024 * 1024 * 1024,
                'M' => 1024 * 1024,
                'K' => 1024,
                '0'..='9' => 1,
                _ => return Err(DiskSpaceParseError("unrecognize unit in ".to_owned() + s)),
            };
            if unit > 1 {
                return Ok(DiskSpace::Bytes(s[0..s.len() - 1].parse::<u64>()? * unit));
            } else {
                return Ok(DiskSpace::Bytes(s.parse()?));
            }
        }
    }
}

#[derive(Default)]
pub struct FlagStorage {
    pub cat_from: OsString,
    pub cat_to: OsString,
    pub mount_point: OsString,
    pub mount_options: Vec<OsString>,
    pub foreground: bool,
    pub free_space: DiskSpace,
    pub uid: libc::uid_t,
    pub gid: libc::gid_t,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(
            DiskSpace::from_str("25G").unwrap(),
            DiskSpace::Bytes(25 * 1024 * 1024 * 1024)
        );
        assert_eq!(DiskSpace::from_str("25").unwrap(), DiskSpace::Bytes(25));
        assert_eq!(
            DiskSpace::from_str("25%").unwrap(),
            DiskSpace::Percent(25.0)
        );
    }

    #[test]
    #[should_panic]
    fn parse_negative() {
        DiskSpace::from_str("-25").unwrap();
    }

    #[test]
    #[should_panic]
    fn parse_unknown_unit() {
        DiskSpace::from_str("25W").unwrap();
    }

    #[test]
    #[should_panic]
    #[allow(non_snake_case)]
    fn parse_NaN() {
        DiskSpace::from_str("CAT").unwrap();
    }
}
