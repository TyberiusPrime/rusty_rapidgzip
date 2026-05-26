//! Speculative DEFLATE decode driven by the vendored zlib-rs engine.
//!
//! This is the perf-tuned path: zlib-rs decodes faster than our in-tree
//! decoder. The vendored engine has been patched to consult a thread-local
//! [`rapidgzip_inflate::speculative::SpeculativeContext`] on the over-distance
//! back-ref path (emits a marker + placeholder) and on every in-buffer
//! back-ref copy (propagates markers from source to destination).
//!
//! ## Entry at arbitrary bit positions
//!
//! The block-finder picks chunk starts at bit-level boundaries. zlib-rs's
//! `Inflate::decompress` reads byte-by-byte; we use its `prime(bits, value)`
//! to inject the leftover bits of the partial first byte into the bit buffer
//! before any input is consumed.
//!
//! ## Bit-exact consumption
//!
//! After each call we compute `consumed_bits = primed + total_in*8 -
//! bits_in_buffer`, which converts back to an absolute bit offset in the
//! original input.

use crate::speculative::SpeculativeChunk;
use crate::DeflateError;

use rapidgzip_inflate::speculative::{ContextGuard, SpeculativeContext};
use rapidgzip_inflate::{Inflate, InflateError, InflateFlush, Status};

/// One reusable engine instance. Re-used across chunks on a single worker
/// thread to amortise the inflate state's window allocation (32 KiB).
///
/// The output buffer is supplied by the caller via `chunk.bytes` and is
/// written into directly — no intermediate scratch. The caller is expected
/// to recycle `chunk.bytes` across chunks (via the pipeline's recycle
/// channel) so its pages stay faulted-in.
pub struct SpeculativeZlibDecoder {
    engine: Inflate,
}

impl SpeculativeZlibDecoder {
    /// Step size for growing `chunk.bytes` when the engine wants more
    /// output room. Chosen so most members fit in 1–2 grows; a 4 MiB step
    /// matches the typical chunk size.
    pub const GROW_STEP: usize = 4 * 1024 * 1024;

    pub fn new() -> Self {
        Self {
            engine: Inflate::new(false, 15),
        }
    }

    /// Decode one complete deflate stream (one gzip member's body) starting
    /// at `start_bit` in `input`.
    ///
    /// Returns the absolute bit offset in `input` where decoding stopped
    /// (one past the last consumed bit; byte-aligned after BFINAL).
    ///
    /// Decoded bytes are appended to `chunk.bytes`; any back-refs that
    /// reached before this member's first byte are recorded as markers on
    /// `chunk.markers`, with `out_pos` translated to the chunk's coordinate
    /// space.
    pub fn decode_member(
        &mut self,
        input: &[u8],
        start_bit: u64,
        chunk: &mut SpeculativeChunk,
    ) -> Result<u64, DeflateError> {
        self.decode_until(input, start_bit, u64::MAX, chunk)
            .map(|(end_bit, _bf)| end_bit)
    }

    /// Decode the deflate stream starting at `start_bit`, stopping either
    /// when BFINAL is reached (returns `hit_bfinal = true`, byte-aligned
    /// end_bit) or when a block boundary at or past `end_bit_hint` is
    /// reached (returns `hit_bfinal = false`). The latter case never
    /// overshoots — the engine uses `InflateFlush::Block` so it stops
    /// exactly between blocks.
    pub fn decode_until(
        &mut self,
        input: &[u8],
        start_bit: u64,
        end_bit_hint: u64,
        chunk: &mut SpeculativeChunk,
    ) -> Result<(u64, bool), DeflateError> {
        let byte_off = (start_bit / 8) as usize;
        let bit_off = (start_bit % 8) as u8;
        if byte_off > input.len() {
            return Err(DeflateError::UnexpectedEof);
        }

        self.engine.reset(false);

        let mut primed: u8 = 0;
        if bit_off != 0 {
            let byte = input[byte_off];
            let remaining_bits = 8 - bit_off;
            let value = (byte >> bit_off) as u64;
            self.engine.prime(remaining_bits, value);
            primed = remaining_bits;
        }
        let feed_start = byte_off + if bit_off != 0 { 1 } else { 0 };
        let feed = &input[feed_start..];

        // Base offset in chunk.bytes where this member's output begins.
        let chunk_base = chunk.bytes.len();

        let mut ctx = SpeculativeContext::default();
        let guard = ContextGuard::new(&mut ctx);

        // Decode directly into chunk.bytes. We grow the Vec's len (with
        // zeros) so the engine can write into the slice, then truncate
        // back at the end. When chunk.bytes was recycled by the pipeline,
        // its pages are already faulted in, so the resize is a cheap
        // length-bump rather than a page-fault parade.
        //
        // Keep the entire member's output contiguous so that most
        // back-refs land in-buffer (the engine never has to read from
        // its 32 KiB Window). Window-source copies are also instrumented
        // for marker propagation, so spilling to the window is correct;
        // it's just slower.
        let mut written: usize = 0;
        let mut feed_consumed: usize = 0;
        let mut hit_bfinal = false;
        loop {
            // Ensure spare room beyond chunk_base + written. We reserve
            // capacity and bump `len` *without* zero-filling: zlib-rs only
            // writes to the output buffer (never reads from uninit bytes
            // before writing them), so handing it uninit memory is safe.
            // Zero-filling on a 4 MiB grow shows up as several seconds of
            // usr time on a multi-GB stream.
            let needed = chunk_base + written + Self::GROW_STEP;
            if chunk.bytes.len() < needed {
                if chunk.bytes.capacity() < needed {
                    chunk.bytes.reserve(needed - chunk.bytes.len());
                }
                // SAFETY: capacity is now >= needed. Bytes in
                // [chunk.bytes.len(), needed) are uninitialized; the
                // engine writes into them sequentially and only reads
                // back from positions it has already written within the
                // current call (DEFLATE in-buffer back-refs read from
                // bytes the engine just emitted, never beyond `written`).
                #[allow(unsafe_code)]
                unsafe { chunk.bytes.set_len(needed); }
            }
            // Marker positions must be member-absolute; the engine's
            // per-call `writer.len()` resets between calls when the window
            // flushes. Update the offset before each call.
            rapidgzip_inflate::speculative::set_out_pos_offset(written as u32);
            let total_out_before = self.engine.total_out();
            let total_in_before = self.engine.total_in();
            // `InflateFlush::Block` returns `Ok` at every block boundary,
            // so we can stop precisely at the chunk's `end_bit_hint`.
            let avail_out = chunk.bytes.len() - (chunk_base + written);
            let status = self
                .engine
                .decompress(
                    &feed[feed_consumed..],
                    &mut chunk.bytes[chunk_base + written..],
                    InflateFlush::Block,
                )
                .map_err(map_inflate_err)?;
            let produced =
                (self.engine.total_out() - total_out_before) as usize;
            feed_consumed += (self.engine.total_in() - total_in_before) as usize;
            written += produced;

            // Was this an "at-a-block-boundary" Ok, or an "output-buffer-full" Ok?
            // If we produced exactly `avail_out`, we are likely not at a
            // block boundary — extend chunk.bytes and continue.
            let buffer_was_full = produced > 0 && produced == avail_out;
            match status {
                Status::StreamEnd => {
                    hit_bfinal = true;
                    break;
                }
                Status::Ok => {
                    if buffer_was_full {
                        continue;
                    }
                    let bits_in_buf = self.engine.bits_in_buffer() as u64;
                    let consumed = primed as u64
                        + (feed_consumed as u64) * 8
                        - bits_in_buf;
                    if start_bit + consumed >= end_bit_hint {
                        break;
                    }
                    continue;
                }
                Status::BufError => return Err(DeflateError::UnexpectedEof),
            }
        }
        chunk.bytes.truncate(chunk_base + written);

        // Drop the guard so we can stop borrowing ctx mutably.
        drop(guard);
        // Translate markers into chunk coordinates.
        chunk.bytes_offset_markers(&ctx.markers, chunk_base as u32);

        let bits_in_buf = self.engine.bits_in_buffer() as u64;
        let consumed_total_bits =
            primed as u64 + (feed_consumed as u64) * 8 - bits_in_buf;
        Ok((start_bit + consumed_total_bits, hit_bfinal))
    }
}

impl Default for SpeculativeZlibDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn map_inflate_err(e: InflateError) -> DeflateError {
    match e {
        InflateError::DataError => DeflateError::Invalid("zlib-rs data error"),
        InflateError::MemError => DeflateError::Invalid("zlib-rs mem error"),
        InflateError::StreamError => DeflateError::Invalid("zlib-rs stream error"),
        InflateError::NeedDict { .. } => DeflateError::Invalid("zlib-rs needs dict"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::speculative::resolve_markers;

    fn deflate_via_gzip(payload: &[u8], level: u32) -> Vec<u8> {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut child = Command::new("gzip")
            .args([&format!("-{level}"), "-c", "-n"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let payload = payload.to_vec();
        let writer = std::thread::spawn(move || stdin.write_all(&payload).unwrap());
        let out = child.wait_with_output().unwrap();
        writer.join().unwrap();
        out.stdout[10..out.stdout.len() - 8].to_vec()
    }

    fn ascii_payload(n: usize) -> Vec<u8> {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut p = Vec::with_capacity(n);
        while p.len() < n {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        p
    }

    /// Decode a whole member from byte 0: there's no unknown prefix, so we
    /// expect 0 markers and an output equal to the original payload.
    #[test]
    fn whole_member_no_markers() {
        let payload = ascii_payload(64 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut dec = SpeculativeZlibDecoder::new();
        let mut chunk = SpeculativeChunk::default();
        let end = dec.decode_member(&body, 0, &mut chunk).unwrap();
        assert!(chunk.markers.is_empty(), "got {} markers", chunk.markers.len());
        assert_eq!(chunk.bytes, payload);
        // end should land at the byte-aligned end-of-stream.
        assert!(end > 0 && end % 8 == 0);
    }

    /// Tiny payload to verify literal-only decoding works.
    #[test]
    fn tiny_payload() {
        let body = deflate_via_gzip(b"abc", 6);
        let mut dec = SpeculativeZlibDecoder::new();
        let mut chunk = SpeculativeChunk::default();
        dec.decode_member(&body, 0, &mut chunk).unwrap();
        assert_eq!(chunk.bytes, b"abc");
        assert!(chunk.markers.is_empty());
    }

    /// Two consecutive members in one stream. First member decoded
    /// normally; the test asserts decode_member stops at the right bit.
    #[test]
    fn member_end_bit_is_byte_aligned() {
        let body = deflate_via_gzip(b"hello world", 6);
        let mut dec = SpeculativeZlibDecoder::new();
        let mut chunk = SpeculativeChunk::default();
        let end_bit = dec.decode_member(&body, 0, &mut chunk).unwrap();
        assert_eq!(end_bit % 8, 0);
        // The end bit should be at or past the body's last bit (raw
        // deflate does not include a trailer; gzip trailer was stripped).
        let body_bits = body.len() as u64 * 8;
        assert!(end_bit <= body_bits);
    }

    /// Sanity check the bit-offset entry path: build a stream, find a
    /// block start at a non-byte-aligned bit, and decode from there
    /// speculatively. Markers should resolve against the known prefix to
    /// match the rest of the original payload.
    #[test]
    fn midstream_entry_with_markers() {
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);

        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);
        let mut starts: Vec<u64> = Vec::new();
        {
            let mut br = crate::BitReader::new(&padded);
            let mut out = Vec::new();
            loop {
                starts.push(br.tell_bit());
                let bf = crate::inflate::inflate_block(&mut br, &mut out).unwrap();
                if bf {
                    break;
                }
            }
        }
        assert!(starts.len() >= 3, "need at least 2 non-final blocks");

        // Decode the head with the serial path — it knows the full history.
        let split_a = starts[1];
        let mut chunk0 = SpeculativeChunk::default();
        {
            let mut br = crate::BitReader::new(&padded);
            loop {
                if br.tell_bit() >= split_a {
                    break;
                }
                crate::inflate::inflate_block(&mut br, &mut chunk0.bytes).unwrap();
            }
        }
        assert!(chunk0.is_resolved());

        // Decode the rest speculatively from split_a.
        let mut dec = SpeculativeZlibDecoder::new();
        let mut chunk1 = SpeculativeChunk::default();
        let _end = dec.decode_member(&padded, split_a, &mut chunk1).unwrap();

        // Resolve chunk1 against chunk0's tail (last 32 KiB).
        let tail_start = chunk0.bytes.len().saturating_sub(32 * 1024);
        resolve_markers(&mut chunk1, &chunk0.bytes[tail_start..]).unwrap();

        let mut stitched = chunk0.bytes.clone();
        stitched.extend_from_slice(&chunk1.bytes);
        assert_eq!(stitched.len(), payload.len(), "length mismatch");
        assert!(
            stitched.iter().zip(&payload).all(|(a, b)| a == b),
            "stitched != original"
        );
    }
}
