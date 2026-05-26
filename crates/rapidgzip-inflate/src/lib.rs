// Private constants that are pub(crate) in upstream zlib-rs and not
// accessible via __internal-api.
#[allow(unused)]
pub(crate) const ENOUGH: usize = ENOUGH_LENS + ENOUGH_DISTS;
pub(crate) const ENOUGH_LENS: usize = 1332;
pub(crate) const ENOUGH_DISTS: usize = 592;
pub(crate) const ADLER32_INITIAL_VALUE: usize = 1;
pub(crate) const CRC32_INITIAL_VALUE: u32 = 0;
pub(crate) const DEF_WBITS: i32 = MAX_WBITS;

// Code is pub(crate) in upstream; we need it for the Huffman tables.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Code {
    pub op: u8,
    pub bits: u8,
    pub val: u16,
}

// ReturnCode cannot be re-exported from upstream because stable.rs implements
// From<InflateError> for it (orphan rule). We copy the definition here and
// keep it crate-private; callers see only the stable Status / InflateError API.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum ReturnCode {
    Ok = 0,
    StreamEnd = 1,
    NeedDict = 2,
    ErrNo = -1,
    StreamError = -2,
    DataError = -3,
    MemError = -4,
    BufError = -5,
    VersionError = -6,
}

impl ReturnCode {
    pub(crate) fn error_message_str(self) -> &'static str {
        match self {
            ReturnCode::Ok => "",
            ReturnCode::StreamEnd => "stream end",
            ReturnCode::NeedDict => "need dictionary",
            ReturnCode::ErrNo => "file error",
            ReturnCode::StreamError => "stream error",
            ReturnCode::DataError => "data error",
            ReturnCode::MemError => "insufficient memory",
            ReturnCode::BufError => "buffer error",
            ReturnCode::VersionError => "incompatible version",
        }
    }

    pub(crate) const fn error_message(self) -> *const core::ffi::c_char {
        let msg = match self {
            ReturnCode::Ok => "\0",
            ReturnCode::StreamEnd => "stream end\0",
            ReturnCode::NeedDict => "need dictionary\0",
            ReturnCode::ErrNo => "file error\0",
            ReturnCode::StreamError => "stream error\0",
            ReturnCode::DataError => "data error\0",
            ReturnCode::MemError => "insufficient memory\0",
            ReturnCode::BufError => "buffer error\0",
            ReturnCode::VersionError => "incompatible version\0",
        };
        msg.as_ptr().cast::<core::ffi::c_char>()
    }

    pub(crate) const fn try_from_c_int(err: core::ffi::c_int) -> Option<Self> {
        match err {
            0 => Some(Self::Ok),
            1 => Some(Self::StreamEnd),
            2 => Some(Self::NeedDict),
            -1 => Some(Self::ErrNo),
            -2 => Some(Self::StreamError),
            -3 => Some(Self::DataError),
            -4 => Some(Self::MemError),
            -5 => Some(Self::BufError),
            -6 => Some(Self::VersionError),
            _ => None,
        }
    }
}

impl From<i32> for ReturnCode {
    fn from(value: i32) -> Self {
        match Self::try_from_c_int(value) {
            Some(v) => v,
            None => panic!("invalid return code {value}"),
        }
    }
}

// InflateFlush is public in upstream and has no orphan issues.
pub use zlib_rs::InflateFlush;
// MAX_WBITS / MIN_WBITS are exposed by __internal-api.
pub use zlib_rs::{MAX_WBITS, MIN_WBITS};

// Shim modules that mirror zlib-rs's internal module layout so that the
// moved inflate.rs and its sub-modules can use unchanged `crate::` paths.
pub(crate) mod adler32;
pub(crate) mod allocate;
pub(crate) mod c_api;
pub(crate) mod cpu_features;
pub(crate) mod crc32;
pub(crate) mod weak_slice;

// Moved from third_party/zlib-rs.
mod inflate;
mod stable;
pub mod speculative;

pub use inflate::InflateConfig;
pub use stable::{Inflate, InflateError, Status};

