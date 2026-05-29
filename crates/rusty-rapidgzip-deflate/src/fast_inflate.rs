//! Fast DEFLATE inflate with speculative (mid-stream / no-window) decode support.
//!
//! This kernel serves two entry points:
//!
//! - **`inflate_fast`**: drop-in replacement for the serial `inflate::inflate` path
//!   (window-known, no markers needed).
//! - **`decode_until`** / **`decode_member`**: the parallel pipeline's speculative
//!   path. Starts decoding at an arbitrary bit offset with no prior history.
//!   Back-references whose source falls before the chunk's first byte are recorded
//!   as [`Marker`]s and resolved downstream once the preceding chunk's window is
//!   known.
//!
//! ## Speculative marker tracking
//!
//! For in-buffer back-refs (distance ≤ bytes emitted in this member):
//! - Copy bytes from the output buffer as normal.
//! - Call [`propagate_match_cached`] to propagate any source-byte markers to the
//!   newly written destination bytes. On the serial path (`ctx_ptr == null`) this
//!   is a single null-check and returns immediately — essentially free.
//!
//! For over-distance back-refs (distance > bytes emitted):
//! - Serial path: `Err(DeflateError::Invalid)`.
//! - Speculative path: push placeholder `0u8` bytes and call
//!   [`record_match_prefix`] to register the markers. Then copy any in-buffer
//!   tail normally.
//!
//! ## Relationship to existing code
//!
//! - `inflate.rs` remains the serial path until Phase 5 routes traffic here.
//! - `speculative_zlib.rs` remains the parallel path until Phase 5.
//! - `decode_until` is a drop-in for `SpeculativeZlibDecoder::decode_until`.

use crate::huffman::{HUFFDEC_EXCEPTIONAL, HUFFDEC_LITERAL, LUT_BITS};
use crate::inflate::read_dynamic_header;
use crate::speculative::SpeculativeChunk;
use crate::tables::{
    fixed_distance_lengths, fixed_literal_lengths, DISTANCE_BASE, DISTANCE_EXTRA,
};
use crate::{BitReader, DeflateError, HuffmanDecoder};

use rusty_rapidgzip_inflate::speculative::{
    cache_active_ptr, propagate_match_cached, record_match_prefix, set_out_pos_offset,
    ContextGuard, SpeculativeContext,
};

// ── Public entry points ──────────────────────────────────────────────────────

/// Decode a complete deflate member from `start_bit` in `input`.
///
/// Appends decompressed bytes to `chunk.bytes`. Over-distance back-refs are
/// recorded as markers in `chunk.markers`. Returns the bit offset in `input`
/// immediately after the BFINAL block ends (byte-aligned).
pub fn decode_member(
    input: &[u8],
    start_bit: u64,
    chunk: &mut SpeculativeChunk,
) -> Result<u64, DeflateError> {
    decode_until(input, start_bit, u64::MAX, chunk).map(|(end_bit, _)| end_bit)
}

/// Decode deflate blocks starting at `start_bit`.
///
/// Stops either when BFINAL is encountered (returns `hit_bfinal = true`,
/// end bit is byte-aligned) or when a block boundary at or after
/// `end_bit_hint` is crossed (returns `hit_bfinal = false`). Never
/// overshoots a block boundary.
///
/// Returns `(end_bit, hit_bfinal)` where `end_bit` is the bit position in
/// `input` after the last decoded block.
pub fn decode_until(
    input: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    chunk: &mut SpeculativeChunk,
) -> Result<(u64, bool), DeflateError> {
    let mut br = BitReader::new(input);
    br.seek_to_bit(start_bit)?;

    let chunk_base = chunk.bytes.len();

    let mut ctx = SpeculativeContext::default();
    let end_bit;
    let hit_bfinal;
    {
        let _guard = ContextGuard::new(&mut ctx);
        // out_pos_offset = 0: this is a single-call decode; member-relative
        // positions start at 0.
        set_out_pos_offset(0);
        let ctx_ptr = cache_active_ptr();

        let mut final_block = false;
        loop {
            let bfinal = br.read(1)? != 0;
            let btype = br.read(2)?;
            decode_block::<true>(&mut br, &mut chunk.bytes, btype, chunk_base, ctx_ptr)?;
            if bfinal {
                final_block = true;
                break;
            }
            if br.tell_bit() >= end_bit_hint {
                break;
            }
        }
        end_bit = br.tell_bit();
        hit_bfinal = final_block;
    }

    chunk.bytes_offset_markers(&ctx.markers, chunk_base as u32);
    Ok((end_bit, hit_bfinal))
}

/// Serial inflate — drop-in for `inflate::inflate`.
///
/// Decodes the complete deflate stream from `br` into `out`. No speculative
/// markers; all back-references must resolve within `out`.
///
/// `chunk_base = 0` here: the full `out` history is the DEFLATE window.
pub fn inflate_fast(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    loop {
        let bfinal = br.read(1)? != 0;
        let btype = br.read(2)?;
        decode_block::<false>(br, out, btype, 0, core::ptr::null_mut())?;
        if bfinal {
            return Ok(());
        }
    }
}

// ── Block dispatcher ─────────────────────────────────────────────────────────

fn decode_block<const IS_SPECULATIVE: bool>(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    btype: u32,
    chunk_base: usize,
    ctx_ptr: *mut SpeculativeContext,
) -> Result<(), DeflateError> {
    match btype {
        0 => decode_stored(br, out),
        1 => {
            let lit = HuffmanDecoder::from_lengths_litlen(&fixed_literal_lengths())?;
            let dist = HuffmanDecoder::from_lengths(&fixed_distance_lengths())?;
            decode_compressed::<IS_SPECULATIVE>(br, out, &lit, &dist, chunk_base, ctx_ptr)
        }
        2 => {
            let (lit, dist) = read_dynamic_header(br)?;
            decode_compressed::<IS_SPECULATIVE>(br, out, &lit, &dist, chunk_base, ctx_ptr)
        }
        _ => Err(DeflateError::Invalid("reserved block type")),
    }
}

// ── Stored block ─────────────────────────────────────────────────────────────

fn decode_stored(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    br.byte_align();
    let len = br.read(16)? as u16;
    let nlen = br.read(16)? as u16;
    if !len ^ nlen != 0 {
        return Err(DeflateError::Invalid("stored block: LEN/NLEN mismatch"));
    }
    out.reserve(len as usize);
    for _ in 0..len {
        out.push(br.read(8)? as u8);
    }
    Ok(())
}

// ── Compressed block hot path ─────────────────────────────────────────────────

/// Chunk size for non-overlapping back-reference copies.
/// With compile-time AVX2: 32 bytes (one YMM load+store per chunk).
/// Otherwise: 16 bytes (one XMM load+store per chunk).
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
const COPY_N: usize = 32;
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
const COPY_N: usize = 16;

/// Over-write headroom required by `copy_back_raw` (one extra COPY_N-byte
/// store may land past `dst + length`).
const COPY_HEADROOM: usize = COPY_N;

/// Copy `length` bytes in `N`-byte chunks.  May over-write up to `N-1` bytes
/// beyond `dst + length`; caller ensures `dst + length + N` is valid.
///
/// # Safety
///
/// - `src` readable for `length + N - 1` bytes.
/// - `dst` writable for `length + N - 1` bytes.
/// - `src..src+length` and `dst..dst+length` must not overlap.
/// - `length > 0`.
#[inline(always)]
#[allow(unsafe_code)]
unsafe fn copy_chunked_unchecked<const N: usize>(src: *const u8, dst: *mut u8, length: usize) {
    let end = src.add(length);
    let chunk: [u8; N] = std::ptr::read_unaligned(src.cast::<[u8; N]>());
    std::ptr::write_unaligned(dst.cast::<[u8; N]>(), chunk);
    let mut s = src.add(N);
    let mut d = dst.add(N);
    while s < end {
        let chunk: [u8; N] = std::ptr::read_unaligned(s.cast::<[u8; N]>());
        std::ptr::write_unaligned(d.cast::<[u8; N]>(), chunk);
        s = s.add(N);
        d = d.add(N);
    }
}


/// Inline back-reference copy.  Works entirely with raw pointers to avoid
/// the `Vec::set_len` / `Vec::len` round-trip on every back-ref.
/// `N` is the SIMD chunk width (16 for SSE2/NEON, 32 for AVX2).
///
/// # Safety
///
/// - `out_ptr[cur - distance .. cur + length + N]` is valid memory.
/// - `distance > 0`.
#[inline(always)]
#[allow(unsafe_code)]
unsafe fn copy_back_raw(out_ptr: *mut u8, cur: usize, distance: usize, length: usize) {
    let src = out_ptr.add(cur - distance) as *const u8;
    let dst = out_ptr.add(cur);
    if distance >= length {
        // Non-overlapping: COPY_N-byte chunked copy.
        copy_chunked_unchecked::<COPY_N>(src, dst, length);
    } else if distance == 1 {
        // RLE: broadcast single byte with 8-byte stores.
        let b = *src;
        let bcast = (b as u64).wrapping_mul(0x0101_0101_0101_0101u64);
        let mut n = length;
        let mut d = dst;
        while n >= 8 {
            std::ptr::write_unaligned(d as *mut u64, bcast);
            d = d.add(8);
            n -= 8;
        }
        while n > 0 {
            *d = b;
            d = d.add(1);
            n -= 1;
        }
    } else {
        // Overlapping: 2 ≤ distance < length (RLE with period > 1).
        for i in 0..length {
            *dst.add(i) = *src.add(i);
        }
    }
}

/// Decode one compressed deflate block (fixed or dynamic Huffman).
///
/// `IS_SPECULATIVE = false`: serial path — all speculative marker code dead.
/// Copy width is `COPY_N` (32 with AVX2 target feature, 16 otherwise).
#[allow(unsafe_code)]
fn decode_compressed<const IS_SPECULATIVE: bool>(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
    chunk_base: usize,
    ctx_ptr: *mut SpeculativeContext,
) -> Result<(), DeflateError> {
    // Worst-case bits needed for a full L/D pair: 15+5+15+13 = 48.
    // We only need this for the near-EOF slow path. The fast path uses the
    // unconditional-refill trick (bits |= 56) which guarantees ≥ 56 bits after
    // every refill, making NEEDED checks unnecessary in the hot loop body.
    const NEEDED: u32 = 48;
    const HEADROOM: usize = 4096;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;

    if out.capacity() - out.len() < HEADROOM {
        out.reserve(HEADROOM);
    }
    let mut cap = out.capacity();
    let mut out_ptr = out.as_mut_ptr();
    let mut cur = out.len();

    // Shadow bit-reader state as locals so the hot loop is register-resident.
    let mut buf = br.buf;
    let mut bits = br.bits;
    let mut byte_pos = br.byte_pos;
    let input_ptr = br.input.as_ptr();
    let input_len = br.input.len();
    let lit_lut = lit.lut_ptr();
    let dist_lut = dist.lut_ptr();

    macro_rules! sync_br {
        () => {
            br.buf = buf;
            br.bits = bits;
            br.byte_pos = byte_pos;
        };
    }
    macro_rules! reload_from_br {
        () => {
            buf = br.buf;
            bits = br.bits;
            byte_pos = br.byte_pos;
        };
    }

    let result: Result<(), DeflateError> = 'outer: loop {
        // ── Refill: unconditional fast path, byte-by-byte slow path ─────────
        //
        // When ≥ 8 bytes remain in input, we refill unconditionally every
        // iteration — no conditional branch and no mispredictions.  The
        // `bits |= 56` trick (from zlib-rs) ensures we always have ≥ 56 bits
        // after the fast refill, which is enough for the worst-case
        // L/D pair (15+5+15+13 = 48 bits) without needing a mid-pair refill.
        //
        // When advance = (63 ^ bits) >> 3 = 0 (i.e., bits ≥ 57), the load is
        // effectively a no-op: we OR in bytes we've already passed, advance 0
        // bytes, and bits stays unchanged.  The load itself still hits L1 since
        // we're reading sequentially, and LLVM can hoist it out of the
        // branch-free path.
        //
        // SAFETY: `byte_pos + 8 <= input_len` is checked before the load.
        if byte_pos + 8 <= input_len {
            let chunk =
                unsafe { std::ptr::read_unaligned(input_ptr.add(byte_pos) as *const u64) };
            buf |= u64::from_le(chunk) << bits;
            let advance = (63u32 ^ bits) >> 3;
            byte_pos += advance as usize;
            bits |= 56;
        } else if bits < NEEDED {
            // Near-EOF: slow byte-by-byte fill until we have enough for the
            // current symbol.
            while bits < NEEDED {
                if byte_pos >= input_len {
                    unsafe { out.set_len(cur) };
                    sync_br!();
                    br.exhausted = true;
                    break 'outer Err(DeflateError::UnexpectedEof);
                }
                buf |= unsafe { (*input_ptr.add(byte_pos)) as u64 } << bits;
                byte_pos += 1;
                bits += 8;
            }
        }

        // ── Decode literal/length symbol ─────────────────────────────────────
        let entry = {
            let idx = (buf & LUT_MASK) as usize;
            let e = unsafe { *lit_lut.add(idx) };
            let len = e & 0xff;
            if len == 0 {
                // Long-code fallback (cold).
                unsafe { out.set_len(cur) };
                sync_br!();
                match lit.lookup_long(br) {
                    Ok(e) => {
                        reload_from_br!();
                        e
                    }
                    Err(e) => break 'outer Err(e),
                }
            } else {
                buf >>= len;
                bits -= len;
                e
            }
        };

        // ── Literal ──────────────────────────────────────────────────────────
        if entry & HUFFDEC_LITERAL != 0 {
            if cur == cap {
                unsafe { out.set_len(cur) };
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe { *out_ptr.add(cur) = (entry >> 16) as u8 };
            cur += 1;
            continue 'outer;
        }

        // ── EOB / reserved ───────────────────────────────────────────────────
        if entry & HUFFDEC_EXCEPTIONAL != 0 {
            unsafe { out.set_len(cur) };
            sync_br!();
            if entry >> 16 == 0 {
                break 'outer Ok(()); // EOB
            }
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        }

        // ── Length code ──────────────────────────────────────────────────────
        let length_base = ((entry >> 16) & 0x1ff) as usize;
        let len_extra = ((entry >> 8) & 0x1f) as u32;
        let mut length = length_base;
        if len_extra > 0 {
            length += (buf & ((1u64 << len_extra) - 1)) as usize;
            buf >>= len_extra;
            bits -= len_extra;
        }

        // ── Distance symbol ──────────────────────────────────────────────────
        let dsym = {
            let idx = (buf & LUT_MASK) as usize;
            let e = unsafe { *dist_lut.add(idx) };
            let len = e & 0xff;
            if len == 0 {
                unsafe { out.set_len(cur) };
                sync_br!();
                match dist.lookup_long(br) {
                    Ok(e) => {
                        reload_from_br!();
                        (e >> 16) as u16
                    }
                    Err(e) => break 'outer Err(e),
                }
            } else {
                buf >>= len;
                bits -= len;
                (e >> 16) as u16
            }
        };
        if dsym >= 30 {
            unsafe { out.set_len(cur) };
            sync_br!();
            break 'outer Err(DeflateError::Invalid("distance symbol out of range"));
        }
        let di = dsym as usize;
        let mut distance = DISTANCE_BASE[di] as usize;
        let dextra = DISTANCE_EXTRA[di] as u32;
        if dextra > 0 {
            distance += (buf & ((1u64 << dextra) - 1)) as usize;
            buf >>= dextra;
            bits -= dextra;
        }

        // ── Back-reference resolve ────────────────────────────────────────────
        if IS_SPECULATIVE {
            // Speculative path: back-refs may reach into the unknown prefix.
            let emitted = cur - chunk_base;
            if distance > emitted {
                let prefix_count = (distance - emitted).min(length);

                unsafe { out.set_len(cur) };
                if out.capacity() - cur < length {
                    out.reserve(length);
                    cap = out.capacity();
                    out_ptr = out.as_mut_ptr();
                }
                unsafe {
                    std::ptr::write_bytes(out_ptr.add(cur), 0, prefix_count);
                }
                record_match_prefix(emitted, distance, prefix_count);
                cur += prefix_count;

                let in_buffer_count = length - prefix_count;
                if in_buffer_count > 0 {
                    let in_dist = distance - prefix_count;
                    if cap - cur < in_buffer_count + COPY_HEADROOM {
                        unsafe { out.set_len(cur) };
                        out.reserve(in_buffer_count + HEADROOM);
                        cap = out.capacity();
                        out_ptr = out.as_mut_ptr();
                    }
                    unsafe { copy_back_raw(out_ptr, cur, in_dist, in_buffer_count) };
                    propagate_match_cached(ctx_ptr, emitted + prefix_count, distance, in_buffer_count);
                    cur += in_buffer_count;
                    if cap - cur < HEADROOM {
                        unsafe { out.set_len(cur) };
                        out.reserve(HEADROOM);
                        cap = out.capacity();
                        out_ptr = out.as_mut_ptr();
                    }
                }
                continue 'outer;
            }
            // distance <= emitted: falls through to in-buffer path below.
            // Propagate markers from source bytes to newly written bytes.
            if distance == 0 {
                unsafe { out.set_len(cur) };
                sync_br!();
                break 'outer Err(DeflateError::Invalid("zero back-reference distance"));
            }
            if cap - cur < length + COPY_HEADROOM {
                unsafe { out.set_len(cur) };
                out.reserve(length + HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe { copy_back_raw(out_ptr, cur, distance, length) };
            propagate_match_cached(ctx_ptr, emitted, distance, length);
            cur += length;
            if cap - cur < HEADROOM {
                unsafe { out.set_len(cur) };
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
        } else {
            // Serial path: all back-refs must be within the output so far.
            // distance > cur is always false in valid streams (after the first
            // 32 KB), so all speculative code is dead-eliminated.
            if distance == 0 || distance > cur {
                unsafe { out.set_len(cur) };
                sync_br!();
                break 'outer if distance == 0 {
                    Err(DeflateError::Invalid("zero back-reference distance"))
                } else {
                    Err(DeflateError::Invalid("back-reference distance out of bounds"))
                };
            }
            if cap - cur < length + COPY_HEADROOM {
                unsafe { out.set_len(cur) };
                out.reserve(length + HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe { copy_back_raw(out_ptr, cur, distance, length) };
            cur += length;
            if cap - cur < HEADROOM {
                unsafe { out.set_len(cur) };
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
        }
    };

    result
}


// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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

    fn check_inflate_fast(payload: &[u8], level: u32) {
        let deflate = deflate_via_gzip(payload, level);
        let mut padded = deflate.clone();
        padded.extend_from_slice(&[0u8; 16]);
        let mut br = BitReader::new(&padded);
        let mut out = Vec::new();
        inflate_fast(&mut br, &mut out).expect("inflate_fast failed");
        assert_eq!(out, payload, "inflate_fast mismatch (len={} level={})", payload.len(), level);
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

    #[test]
    fn inflate_fast_empty() {
        check_inflate_fast(b"", 6);
    }

    #[test]
    fn inflate_fast_tiny() {
        check_inflate_fast(b"hello, world\n", 6);
    }

    #[test]
    fn inflate_fast_repeating() {
        let mut p = Vec::new();
        for _ in 0..1000 { p.extend_from_slice(b"abcdefghij"); }
        check_inflate_fast(&p, 6);
    }

    #[test]
    fn inflate_fast_rle() {
        check_inflate_fast(&vec![b'x'; 10000], 6);
    }

    #[test]
    fn inflate_fast_ascii_1k() {
        check_inflate_fast(&ascii_payload(1024), 6);
    }

    #[test]
    fn inflate_fast_fixed_huffman() {
        check_inflate_fast(b"aaaaaaaaaa", 9);
    }

    #[test]
    fn inflate_fast_stored() {
        let mut s: u64 = 0xA1B2C3D4E5F60718;
        let mut p = Vec::with_capacity(8192);
        while p.len() < 8192 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.extend_from_slice(&s.to_le_bytes());
        }
        check_inflate_fast(&p, 1);
    }

    #[test]
    fn inflate_fast_level9_dynamic() {
        let mut p = Vec::new();
        for i in 0..50000u32 {
            p.extend_from_slice(format!("line {i}: hello world\n").as_bytes());
        }
        check_inflate_fast(&p, 9);
    }

    /// decode_member with no prior window should produce marker-free output
    /// equal to the original payload.
    #[test]
    fn decode_member_whole_no_markers() {
        let payload = ascii_payload(64 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);
        let mut chunk = SpeculativeChunk::default();
        let end_bit = decode_member(&padded, 0, &mut chunk).unwrap();
        assert!(chunk.markers.is_empty(), "{} unexpected markers", chunk.markers.len());
        assert_eq!(chunk.bytes, payload, "decode_member output mismatch");
        assert!(end_bit > 0 && end_bit <= (padded.len() as u64) * 8);
    }

    /// decode_until stops at a block boundary before hitting bfinal.
    #[test]
    fn decode_until_stops_at_boundary() {
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);

        // Find the second block boundary using the serial path.
        let mut br = crate::BitReader::new(&padded);
        let mut dummy: Vec<u8> = Vec::new();
        let _ = crate::inflate::inflate_block(&mut br, &mut dummy).unwrap(); // block 0
        let boundary_after_block0 = br.tell_bit();

        let mut chunk = SpeculativeChunk::default();
        let (end_bit, hit_bfinal) =
            decode_until(&padded, 0, boundary_after_block0, &mut chunk).unwrap();

        // Stopped at or just after the hint, at a block boundary.
        assert!(!hit_bfinal, "should not have hit bfinal");
        assert!(end_bit >= boundary_after_block0);
    }

    /// Midstream speculative decode: decode the first chunk serially, then
    /// decode the rest from a mid-stream bit offset speculatively, resolve
    /// markers, and assert the stitched output matches the original.
    #[test]
    fn midstream_speculative_roundtrip() {
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);

        // Collect block start positions.
        let mut block_starts: Vec<u64> = Vec::new();
        {
            let mut br = crate::BitReader::new(&padded);
            let mut dummy = Vec::new();
            loop {
                block_starts.push(br.tell_bit());
                let bfinal = crate::inflate::inflate_block(&mut br, &mut dummy).unwrap();
                if bfinal { break; }
            }
        }
        assert!(block_starts.len() >= 3, "need ≥ 3 blocks for this test");
        let split_bit = block_starts[1];

        // Decode the head serially.
        let mut chunk0 = SpeculativeChunk::default();
        {
            let mut br = crate::BitReader::new(&padded);
            loop {
                if br.tell_bit() >= split_bit { break; }
                crate::inflate::inflate_block(&mut br, &mut chunk0.bytes).unwrap();
            }
        }
        assert!(chunk0.is_resolved());

        // Decode the rest speculatively.
        let mut chunk1 = SpeculativeChunk::default();
        decode_member(&padded, split_bit, &mut chunk1).unwrap();

        // Resolve markers.
        let tail_start = chunk0.bytes.len().saturating_sub(32 * 1024);
        crate::speculative::resolve_markers(&mut chunk1, &chunk0.bytes[tail_start..]).unwrap();

        let mut stitched = chunk0.bytes.clone();
        stitched.extend_from_slice(&chunk1.bytes);
        assert_eq!(stitched.len(), payload.len(), "stitched length mismatch");
        assert!(
            stitched == payload,
            "stitched output does not match original"
        );
    }
}
