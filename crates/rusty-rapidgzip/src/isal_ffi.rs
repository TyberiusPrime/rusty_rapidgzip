//! Experimental ISA-L (igzip) backed per-member decode (feature `isal`).
//!
//! A sibling of [`crate::libdeflate_ffi`] with identical semantics, swapping the
//! decode engine for Intel's ISA-L `isal_inflate` (hand-written SIMD assembly).
//! The point is to quantify whether ISA-L's inflate beats libdeflate / our own
//! kernel on the same workloads — see the `isal` feature in Cargo.toml.
//!
//! Like libdeflate, ISA-L decodes a whole DEFLATE stream to its BFINAL block and
//! cannot stop at an arbitrary mid-member block boundary, so [`decode_member`]
//! returns [`IsalOutcome::Straddle`] when it would overshoot the chunk boundary
//! and the caller falls back to our own `decode_member_u8`. The real struct lives
//! behind a tiny C shim (isal_shim.c); we hold it as an opaque pointer.

#![allow(unsafe_code)]

use std::os::raw::{c_int, c_void};

use crate::deflate::DeflateError;

extern "C" {
    fn rrg_isal_alloc() -> *mut c_void;
    fn rrg_isal_free(p: *mut c_void);
    fn rrg_isal_inflate_raw(
        st: *mut c_void,
        in_: *const u8,
        in_len: usize,
        out: *mut u8,
        out_cap: usize,
        in_used: *mut usize,
        out_made: *mut usize,
    ) -> c_int;
    fn rrg_isal_inflate_gzip(
        st: *mut c_void,
        in_: *const u8,
        in_len: usize,
        out: *mut u8,
        out_cap: usize,
        out_made: *mut usize,
    ) -> c_int;
}

// Shim return codes.
const ISAL_SHIM_OK: c_int = 0;
const ISAL_SHIM_OVERFLOW: c_int = 2;

/// Owns one ISA-L inflate state. ISA-L's state is single-threaded; each worker
/// owns its own — never shared (mirrors [`crate::libdeflate_ffi::Decompressor`]).
pub(crate) struct Decompressor(*mut c_void);

// SAFETY: the handle is only ever touched by the single worker thread that owns
// its `WorkerKernel`; moved between threads at most when the kernel is, never
// aliased.
unsafe impl Send for Decompressor {}

impl Default for Decompressor {
    fn default() -> Self {
        let p = unsafe { rrg_isal_alloc() };
        assert!(!p.is_null(), "rrg_isal_alloc returned null");
        Decompressor(p)
    }
}

impl Drop for Decompressor {
    fn drop(&mut self) {
        unsafe { rrg_isal_free(self.0) }
    }
}

/// Result of attempting an ISA-L member decode (mirrors `LdOutcome`).
pub(crate) enum IsalOutcome {
    /// Decoded a whole member. `(end_bit, hit_bfinal=true)` — `end_bit` is the
    /// byte-aligned start of the gzip trailer.
    Done(u64, bool),
    /// The member would extend past `end_bit_hint`; nothing committed to `out`,
    /// caller must use the fallback kernel.
    Straddle,
}

/// Decode the single gzip member's DEFLATE stream beginning (byte-aligned) at
/// `start_bit`, appending its bytes to `out`. See [`crate::libdeflate_ffi::decode_member`].
pub(crate) fn decode_member(
    d: &Decompressor,
    body: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<IsalOutcome, DeflateError> {
    debug_assert_eq!(start_bit % 8, 0, "member start must be byte-aligned");
    let byte_pos = (start_bit / 8) as usize;
    if byte_pos >= body.len() {
        return Err(DeflateError::UnexpectedEof);
    }
    let in_ptr = unsafe { body.as_ptr().add(byte_pos) };
    let in_avail = body.len() - byte_pos;

    // `out` length stays at `out_start` until we commit on success, so a straddle
    // or error leaves `out` untouched (the bytes we wrote sit in spare capacity).
    let out_start = out.len();
    let mut want: usize = 16 * 1024 * 1024;
    out.reserve(want);

    loop {
        let out_ptr = unsafe { out.as_mut_ptr().add(out_start) };
        let out_avail = out.capacity() - out_start;
        let mut in_used: usize = 0;
        let mut out_made: usize = 0;
        let r = unsafe {
            rrg_isal_inflate_raw(
                d.0,
                in_ptr,
                in_avail,
                out_ptr,
                out_avail,
                &mut in_used,
                &mut out_made,
            )
        };
        match r {
            ISAL_SHIM_OK => {
                let new_end_bit = (byte_pos + in_used) as u64 * 8;
                if new_end_bit > end_bit_hint {
                    return Ok(IsalOutcome::Straddle);
                }
                // SAFETY: the shim wrote `out_made` initialised bytes at `out_ptr`.
                unsafe { out.set_len(out_start + out_made) };
                return Ok(IsalOutcome::Done(new_end_bit, true));
            }
            ISAL_SHIM_OVERFLOW => {
                want = (out.capacity() - out_start).saturating_mul(2).max(want * 2);
                out.reserve(want);
                continue;
            }
            _ => return Err(DeflateError::Invalid("ISA-L decode failed")),
        }
    }
}

/// Decode one *complete* gzip member (header + DEFLATE + trailer), appending its
/// bytes to `out`. ISA-L parses the gzip header and verifies CRC32 + ISIZE.
/// This is the BGZF path; see [`crate::libdeflate_ffi::decode_gzip_member`].
/// Returns the number of bytes appended on success.
pub(crate) fn decode_gzip_member(
    d: &Decompressor,
    member: &[u8],
    out: &mut Vec<u8>,
) -> Result<usize, DeflateError> {
    if member.len() < 18 {
        return Err(DeflateError::UnexpectedEof);
    }
    // ISIZE (last 4 bytes) is the exact uncompressed size for BGZF (≤64 KiB) blocks.
    let isize = u32::from_le_bytes(
        member[member.len() - 4..]
            .try_into()
            .expect("4-byte slice (len >= 18 checked above)"),
    ) as usize;

    let out_start = out.len();
    out.reserve(isize);
    let out_ptr = unsafe { out.as_mut_ptr().add(out_start) };
    let mut out_made: usize = 0;
    let r = unsafe {
        rrg_isal_inflate_gzip(
            d.0,
            member.as_ptr(),
            member.len(),
            out_ptr,
            isize,
            &mut out_made,
        )
    };
    match r {
        ISAL_SHIM_OK => {
            // SAFETY: the shim wrote `out_made <= isize <= reserved capacity` bytes.
            unsafe { out.set_len(out_start + out_made) };
            Ok(out_made)
        }
        _ => Err(DeflateError::Invalid("ISA-L gzip decode failed")),
    }
}
