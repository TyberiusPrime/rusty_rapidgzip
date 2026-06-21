//! Experimental zlib-rs backed per-member decode (feature `zlib-rs`).
//!
//! A pure-Rust sibling of [`crate::libdeflate_ffi`] / [`crate::isal_ffi`], using
//! the `libz-rs-sys` standard-zlib C ABI (implemented in safe Rust by zlib-rs)
//! as the decode engine, to A/B/C it against libdeflate and ISA-L. No C compiler
//! or external linking — it's a normal Rust dependency.
//!
//! Semantics match the other backends exactly: members fully inside the chunk
//! decode here; the chunk-final straddling member returns [`ZlibOutcome::Straddle`]
//! and the caller falls back to our `decode_member_u8`.
//!
//! Byte-aligned end recovery: zlib's raw inflate stops at the final block's last
//! bit but may have pulled whole bytes past it into its bit buffer. zlib reports
//! the number of unused buffered bits in the low 6 bits of `data_type`, so the
//! byte-aligned gzip-trailer start is `total_in - unused_bits / 8`.

#![allow(unsafe_code)]

use std::os::raw::c_int;

use libz_rs_sys::{
    inflate, inflateEnd, inflateInit2_, inflateReset2, z_stream, zlibVersion, Z_BUF_ERROR, Z_FINISH,
    Z_OK, Z_STREAM_END,
};

use crate::deflate::DeflateError;

/// windowBits selectors: negative ⇒ raw DEFLATE (no wrapper); 16+15 ⇒ gzip
/// (parse header, verify CRC32/ISIZE).
const WBITS_RAW: c_int = -15;
const WBITS_GZIP: c_int = 16 + 15;

/// Owns one zlib-rs inflate stream. Single-threaded like the other backends;
/// each worker owns its own. Boxed so the `z_stream` address is stable.
pub(crate) struct Decompressor(Box<z_stream>);

// SAFETY: only ever touched by the single worker thread that owns its
// `WorkerKernel`; moved between threads at most when the kernel is, never aliased.
unsafe impl Send for Decompressor {}

impl Default for Decompressor {
    fn default() -> Self {
        // `z_stream::default()` wires up zlib-rs's Rust allocator (feature
        // `rust-allocator`, on by default). Init raw; each decode resets to the
        // window it needs.
        let mut strm = Box::new(z_stream::default());
        let ret = unsafe {
            inflateInit2_(
                &mut *strm,
                WBITS_RAW,
                zlibVersion(),
                std::mem::size_of::<z_stream>() as c_int,
            )
        };
        assert_eq!(ret, Z_OK, "inflateInit2_ failed: {ret}");
        Decompressor(strm)
    }
}

impl Drop for Decompressor {
    fn drop(&mut self) {
        unsafe { inflateEnd(&mut *self.0) };
    }
}

/// Result of attempting a zlib-rs member decode (mirrors `LdOutcome`).
pub(crate) enum ZlibOutcome {
    /// Decoded a whole member. `(end_bit, hit_bfinal=true)` — `end_bit` is the
    /// byte-aligned start of the gzip trailer.
    Done(u64, bool),
    /// The member would extend past `end_bit_hint`; nothing committed to `out`.
    Straddle,
}

/// Decode the single gzip member's DEFLATE stream beginning (byte-aligned) at
/// `start_bit`, appending its bytes to `out`. See [`crate::libdeflate_ffi::decode_member`].
pub(crate) fn decode_member(
    d: &mut Decompressor,
    body: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<ZlibOutcome, DeflateError> {
    debug_assert_eq!(start_bit % 8, 0, "member start must be byte-aligned");
    let byte_pos = (start_bit / 8) as usize;
    if byte_pos >= body.len() {
        return Err(DeflateError::UnexpectedEof);
    }
    let in_ptr = unsafe { body.as_ptr().add(byte_pos) };
    // `avail_in` is a u32 in zlib's C ABI, but `body` is the whole mmap'd file,
    // which is routinely >4 GiB. Clamp (don't truncate `as u32`) so a member that
    // sits past the 4 GiB mark still gets enough input to decode — a member is at
    // most a few MiB, so u32::MAX of input always covers it.
    let in_avail = (body.len() - byte_pos).min(u32::MAX as usize) as u32;

    // Like the libdeflate/ISA-L backends this is one-shot: on output overflow we
    // grow and re-decode from a clean reset (the 16 MiB start covers FASTQ-sized
    // members without ever retrying). `out` stays at `out_start` until success,
    // so a straddle/error leaves it untouched.
    let out_start = out.len();
    let mut want: usize = 16 * 1024 * 1024;
    out.reserve(want);

    let strm = &mut *d.0;
    loop {
        let rc = unsafe { inflateReset2(strm, WBITS_RAW) };
        if rc != Z_OK {
            return Err(DeflateError::Invalid("zlib-rs inflateReset2 failed"));
        }
        let out_avail = out.capacity() - out_start;
        strm.next_in = in_ptr;
        strm.avail_in = in_avail;
        strm.next_out = unsafe { out.as_mut_ptr().add(out_start) };
        strm.avail_out = out_avail.min(u32::MAX as usize) as u32;

        let ret = unsafe { inflate(strm, Z_FINISH) };
        match ret {
            Z_STREAM_END => {
                let total_in = strm.total_in as usize;
                // Low 6 bits of data_type = bits still buffered past the deflate
                // stream; whole over-read bytes are `unused_bits / 8`.
                let unused_bits = (strm.data_type & 0x3f) as usize;
                let trailer_start = byte_pos + total_in - unused_bits / 8;
                let new_end_bit = trailer_start as u64 * 8;
                if new_end_bit > end_bit_hint {
                    return Ok(ZlibOutcome::Straddle);
                }
                // SAFETY: zlib wrote `total_out` initialised bytes at out_start.
                unsafe { out.set_len(out_start + strm.total_out as usize) };
                return Ok(ZlibOutcome::Done(new_end_bit, true));
            }
            Z_OK | Z_BUF_ERROR if strm.avail_out == 0 => {
                want = (out.capacity() - out_start).saturating_mul(2).max(want * 2);
                out.reserve(want);
                continue;
            }
            _ => return Err(DeflateError::Invalid("zlib-rs decode failed")),
        }
    }
}

/// Decode one *complete* gzip member (header + DEFLATE + trailer), appending its
/// bytes to `out`. zlib parses the gzip header and verifies CRC32 + ISIZE.
/// This is the BGZF path; see [`crate::libdeflate_ffi::decode_gzip_member`].
/// Returns the number of bytes appended on success.
pub(crate) fn decode_gzip_member(
    d: &mut Decompressor,
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

    let strm = &mut *d.0;
    let rc = unsafe { inflateReset2(strm, WBITS_GZIP) };
    if rc != Z_OK {
        return Err(DeflateError::Invalid("zlib-rs inflateReset2 failed"));
    }
    strm.next_in = member.as_ptr();
    strm.avail_in = member.len() as u32;
    strm.next_out = unsafe { out.as_mut_ptr().add(out_start) };
    strm.avail_out = isize as u32;

    let ret = unsafe { inflate(strm, Z_FINISH) };
    if ret != Z_STREAM_END {
        return Err(DeflateError::Invalid("zlib-rs gzip decode failed"));
    }
    let made = strm.total_out as usize;
    // SAFETY: zlib wrote `made <= isize <= reserved capacity` bytes at out_start.
    unsafe { out.set_len(out_start + made) };
    Ok(made)
}
