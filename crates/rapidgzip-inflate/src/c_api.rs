#![allow(non_camel_case_types)]

pub use zlib_rs::c_api::*;

// These are pub(crate) in upstream zlib-rs, not accessible via __internal-api.
pub(crate) type z_size = core::ffi::c_ulong;
pub(crate) type z_checksum = core::ffi::c_ulong;
