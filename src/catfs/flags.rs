use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsString;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

pub enum DiskSpace {
    Percent(f32),
    Bytes(u64),
}

impl Default for DiskSpace {
    fn default() -> DiskSpace {
        DiskSpace::Bytes(0)
    }
}

#[derive(Debug)]
pub struct DiskSpaceParseError(String);

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
        if s.ends_with("%") {
            return Ok(DiskSpace::Percent(s[0..s.len() - 1].parse()?));
        } else {
            // interpret it as a byte size
            let unit = match s.chars().last().unwrap() {
                'T' => 1 * 1024 * 1024 * 1024 * 1024,
                'G' => 1 * 1024 * 1024 * 1024,
                'M' => 1 * 1024 * 1024,
                'K' => 1 * 1024,
                _ => 1,
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
    pub mount_options: HashMap<String, String>,
    pub foreground: bool,
    pub free_space: DiskSpace,
}
