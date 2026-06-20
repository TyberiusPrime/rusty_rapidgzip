//! Experimental zune-inflate backed per-member decode (feature `zune`).
//!
//! zune-inflate is a pure-Rust port of libdeflate's algorithm — the point is to
//! see whether it reaches libdeflate-class speed with no C dependency. Unlike the
//! other backends it has no reusable decompressor state (a `DeflateDecoder`
//! borrows the input and is created per call) and it always returns a freshly
//! allocated `Vec<u8>` that we then copy into `out` — an inherent disadvantage
//! versus the in-place backends, noted when interpreting the numbers.
//!
//! The byte-aligned member end comes from the vendored `input_position()` patch
//! (see vendor/zune-inflate/LOCAL_PATCH.md): after a bare `decode_deflate`, it is
//! the offset of the byte-aligned trailer that follows the DEFLATE stream.

use zune_inflate::{DeflateDecoder, DeflateOptions};

use crate::deflate::DeflateError;

/// Initial output reservation for a raw member decode. zune zero-fills this, so
/// it is a real cost; FASTQ members decode to ~5 MiB, and the decoder grows past
/// it if needed.
const RAW_SIZE_HINT: usize = 8 * 1024 * 1024;

/// Result of attempting a zune member decode (mirrors `LdOutcome`).
pub(crate) enum ZuneOutcome {
    /// Decoded a whole member. `(end_bit, hit_bfinal=true)` — `end_bit` is the
    /// byte-aligned start of the gzip trailer.
    Done(u64, bool),
    /// The member would extend past `end_bit_hint`; nothing committed to `out`.
    Straddle,
}

/// Decode the single gzip member's DEFLATE stream beginning (byte-aligned) at
/// `start_bit`, appending its bytes to `out`. See [`crate::libdeflate_ffi::decode_member`].
pub(crate) fn decode_member(
    body: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<ZuneOutcome, DeflateError> {
    debug_assert_eq!(start_bit % 8, 0, "member start must be byte-aligned");
    let byte_pos = (start_bit / 8) as usize;
    if byte_pos >= body.len() {
        return Err(DeflateError::UnexpectedEof);
    }
    let input = &body[byte_pos..];
    // Bare DEFLATE has no checksum, so confirm_checksum is irrelevant here.
    let opts = DeflateOptions::default().set_size_hint(RAW_SIZE_HINT);
    let mut dec = DeflateDecoder::new_with_options(input, opts);
    match dec.decode_deflate() {
        Ok(decoded) => {
            // `input_position` (local patch) = byte-aligned offset, within `input`,
            // where the DEFLATE stream ended (the gzip trailer start).
            let new_end_bit = (byte_pos + dec.input_position()) as u64 * 8;
            if new_end_bit > end_bit_hint {
                return Ok(ZuneOutcome::Straddle);
            }
            out.extend_from_slice(&decoded);
            Ok(ZuneOutcome::Done(new_end_bit, true))
        }
        Err(_) => Err(DeflateError::Invalid("zune-inflate decode failed")),
    }
}

/// Decode one *complete* gzip member (header + DEFLATE + trailer), appending its
/// bytes to `out`. zune parses the gzip header and verifies the CRC32 trailer.
/// This is the BGZF path; see [`crate::libdeflate_ffi::decode_gzip_member`].
/// Returns the number of bytes appended on success.
pub(crate) fn decode_gzip_member(member: &[u8], out: &mut Vec<u8>) -> Result<usize, DeflateError> {
    let mut dec = DeflateDecoder::new(member);
    match dec.decode_gzip() {
        Ok(decoded) => {
            let n = decoded.len();
            out.extend_from_slice(&decoded);
            Ok(n)
        }
        Err(_) => Err(DeflateError::Invalid("zune-inflate gzip decode failed")),
    }
}
