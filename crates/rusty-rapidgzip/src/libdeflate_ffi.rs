//! Experimental libdeflate-backed per-member decode (feature `libdeflate`).
//!
//! libdeflate decodes a whole DEFLATE stream to its BFINAL block; it cannot stop
//! at an arbitrary mid-member block boundary. Our parallel pipeline, however,
//! chunks at *block* boundaries, so the chunk-final member often straddles
//! `end_bit_hint`. [`decode_member`] therefore returns [`LdOutcome::Straddle`]
//! when libdeflate would overshoot the chunk boundary, and the caller falls back
//! to our own `decode_member_u8` (which stops exactly at the boundary). All other
//! members — fully contained in the chunk — decode through libdeflate.

#![allow(unsafe_code)]

use std::os::raw::c_int;

use crate::deflate::DeflateError;

#[repr(C)]
struct LibdeflateDecompressor {
    _private: [u8; 0],
}

// libdeflate_result discriminants (enum libdeflate_result).
const LIBDEFLATE_SUCCESS: c_int = 0;
const LIBDEFLATE_INSUFFICIENT_SPACE: c_int = 3;

extern "C" {
    fn libdeflate_alloc_decompressor() -> *mut LibdeflateDecompressor;
    fn libdeflate_free_decompressor(d: *mut LibdeflateDecompressor);
    fn libdeflate_deflate_decompress_ex(
        d: *mut LibdeflateDecompressor,
        in_: *const u8,
        in_nbytes: usize,
        out: *mut u8,
        out_nbytes_avail: usize,
        actual_in_nbytes_ret: *mut usize,
        actual_out_nbytes_ret: *mut usize,
    ) -> c_int;
}

/// Owns one libdeflate decompressor. A libdeflate decompressor must not be used
/// concurrently, so each worker thread owns its own — never shared.
pub(crate) struct Decompressor(*mut LibdeflateDecompressor);

// SAFETY: the handle is only ever touched by the single worker thread that owns
// its `WorkerKernel`; it is moved between threads at most when the kernel is, and
// never aliased.
unsafe impl Send for Decompressor {}

impl Default for Decompressor {
    fn default() -> Self {
        let p = unsafe { libdeflate_alloc_decompressor() };
        assert!(!p.is_null(), "libdeflate_alloc_decompressor returned null");
        Decompressor(p)
    }
}

impl Drop for Decompressor {
    fn drop(&mut self) {
        unsafe { libdeflate_free_decompressor(self.0) }
    }
}

/// Result of attempting a libdeflate member decode.
pub(crate) enum LdOutcome {
    /// Decoded a whole member. `(end_bit, hit_bfinal=true)` — `end_bit` is
    /// byte-aligned at the start of the gzip trailer, matching the contract of
    /// `decode_member_u8` after its post-decode `byte_align`.
    Done(u64, bool),
    /// The member would extend past `end_bit_hint` (it belongs partly to the next
    /// chunk). Nothing was committed to `out`; caller must use the fallback kernel.
    Straddle,
}

/// Decode the single gzip member's DEFLATE stream beginning (byte-aligned) at
/// `start_bit`, appending its bytes to `out`.
pub(crate) fn decode_member(
    d: &Decompressor,
    body: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<LdOutcome, DeflateError> {
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
    // FASTQ members decode to ~5 MiB; 16 MiB covers them without a retry, and the
    // INSUFFICIENT_SPACE loop handles anything larger.
    let mut want: usize = 16 * 1024 * 1024;
    out.reserve(want);

    loop {
        let out_ptr = unsafe { out.as_mut_ptr().add(out_start) };
        let out_avail = out.capacity() - out_start;
        let mut in_used: usize = 0;
        let mut out_made: usize = 0;
        let r = unsafe {
            libdeflate_deflate_decompress_ex(
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
            LIBDEFLATE_SUCCESS => {
                // libdeflate consumed whole input bytes up to the byte-aligned end
                // of the DEFLATE stream — i.e. the gzip trailer start.
                let new_end_bit = (byte_pos + in_used) as u64 * 8;
                if new_end_bit > end_bit_hint {
                    return Ok(LdOutcome::Straddle);
                }
                // SAFETY: libdeflate wrote `out_made` initialised bytes at `out_ptr`.
                unsafe { out.set_len(out_start + out_made) };
                return Ok(LdOutcome::Done(new_end_bit, true));
            }
            LIBDEFLATE_INSUFFICIENT_SPACE => {
                want = (out.capacity() - out_start).saturating_mul(2).max(want * 2);
                out.reserve(want);
                continue;
            }
            _ => return Err(DeflateError::Invalid("libdeflate decode failed")),
        }
    }
}
