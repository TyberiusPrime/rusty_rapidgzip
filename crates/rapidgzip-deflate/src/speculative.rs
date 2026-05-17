//! Speculative ("no-window") DEFLATE decode.
//!
//! When a worker thread starts decoding mid-stream — at a chunk boundary
//! that the block-finder picked — it does not yet know the 32 KiB of
//! decompressed data preceding its starting position. Literals decode fine.
//! Back-references whose source falls inside the chunk's already-emitted
//! bytes also decode fine. Back-references reaching back into the unknown
//! prefix can't be resolved yet — we record a [`Marker`] for each such
//! byte and continue.
//!
//! Once the previous chunk's tail window is known (resolved by a serial
//! pass downstream), [`resolve_markers`] walks every marker and substitutes
//! the real byte. After that the chunk is identical to what a serial
//! decoder would have produced.
//!
//! ## Marker representation
//!
//! Markers are kept *sparse*: just `markers: Vec<Marker>` recording
//! `(out_pos, prefix_offset)`, plus a placeholder `0` byte at each marker
//! position in `bytes`. We deliberately do *not* maintain a dense parallel
//! `marker_at: Vec<u16>` — the inner literal loop runs millions of times per
//! chunk and a parallel vec doubles its per-byte cost.
//!
//! For marker *propagation* (an in-chunk back-reference whose source byte is
//! itself a marker), we short-circuit with `max_marker_pos`: if the source
//! offset is strictly greater than the highest marker position we've ever
//! seen, no propagation is possible and we bulk-copy. Otherwise we fall back
//! to a binary search over `markers` (already sorted by position).
//!
//! `prefix_offset` is the distance into the unknown prefix counted backwards
//! from the byte just before the chunk's first byte: 0 = the immediately
//! preceding byte, …, 32767 = the oldest byte still in the 32 KiB window.
//!
//! In real-world streams the marker count is small: roughly
//! `O(32 KiB / mean_match_distance)` per chunk. After ~32 KiB of decoded
//! output, every back-reference lands in-chunk and stops producing markers.

use crate::huffman::{HUFFDEC_EXCEPTIONAL, HUFFDEC_LITERAL};
use crate::tables::{DISTANCE_BASE, DISTANCE_EXTRA};
use std::sync::LazyLock;

use crate::{tables, BitReader, DeflateError, HuffmanDecoder};

/// Fixed Huffman trees (RFC 1951 §3.2.6) never change. Build them once.
static FIXED_LIT: LazyLock<HuffmanDecoder> = LazyLock::new(|| {
    HuffmanDecoder::from_lengths_litlen(&tables::fixed_literal_lengths())
        .expect("fixed literal tree is well-formed")
});
static FIXED_DIST: LazyLock<HuffmanDecoder> = LazyLock::new(|| {
    HuffmanDecoder::from_lengths(&tables::fixed_distance_lengths())
        .expect("fixed distance tree is well-formed")
});

/// One unresolved back-reference byte. See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    /// Position within `SpeculativeChunk::bytes`.
    pub out_pos: u32,
    /// Distance back into the unknown prefix, 0-based:
    /// `prefix_offset = 0` means "the byte immediately before the chunk".
    pub prefix_offset: u16,
}

/// One chunk's worth of speculatively-decoded output.
///
/// - `bytes` holds output bytes, with a placeholder (any value) at marker
///   positions.
/// - `markers` is a sparse list of `(out_pos, prefix_offset)` kept sorted
///   by `out_pos` (push order is increasing position).
/// - `max_marker_pos` is the largest `out_pos` ever inserted into `markers`.
///   Used as a fast non-marker test in [`emit_back_ref`]: if a source byte
///   is strictly past it, there's no marker there to propagate.
#[derive(Debug, Default)]
pub struct SpeculativeChunk {
    pub bytes: Vec<u8>,
    pub markers: Vec<Marker>,
    /// Highest output position that is (or was) a marker. `None` when no
    /// marker has ever been pushed.
    max_marker_pos: Option<u32>,
    /// True if the last block consumed had BFINAL set.
    pub final_block_seen: bool,
    /// Reusable decoder buffers for dynamic blocks. Built lazily by
    /// `inflate_dynamic`. Allocating a 1024-entry LUT per block per tree
    /// shows up at the top of the profile, so we pool them across blocks
    /// within a chunk.
    decoder_pool: Option<Box<DecoderPool>>,
}

#[derive(Debug)]
struct DecoderPool {
    cl: HuffmanDecoder,
    lit: HuffmanDecoder,
    dist: HuffmanDecoder,
    lengths: Vec<u8>,
}

impl DecoderPool {
    fn new() -> Self {
        Self {
            cl: HuffmanDecoder::new_empty(),
            lit: HuffmanDecoder::new_empty(),
            dist: HuffmanDecoder::new_empty(),
            lengths: Vec::new(),
        }
    }
}

impl SpeculativeChunk {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_resolved(&self) -> bool {
        self.markers.is_empty()
    }

    /// Pre-allocate `bytes` to avoid reallocation while decoding a chunk
    /// whose expected output size is known approximately. Used by the
    /// parallel pipeline to size each worker's chunk up front.
    pub fn reserve_bytes(&mut self, n: usize) {
        self.bytes.reserve(n);
    }

    #[inline]
    fn push_literal(&mut self, b: u8) {
        self.bytes.push(b);
    }

    #[inline]
    fn push_marker(&mut self, prefix_offset: u16) {
        let pos = self.bytes.len() as u32;
        self.bytes.push(0);
        self.markers.push(Marker { out_pos: pos, prefix_offset });
        self.max_marker_pos = Some(pos);
    }

    /// True if `src` is known *not* to be a marker without searching
    /// `markers`. Cheap fast path for the common case.
    #[inline]
    fn cannot_be_marker(&self, src: u32) -> bool {
        match self.max_marker_pos {
            None => true,
            Some(max) => src > max,
        }
    }

    /// `src` is in-range; check whether it is a marker. Caller has verified
    /// `cannot_be_marker(src) == false`. Returns `Some(prefix_offset)` if
    /// it is.
    #[inline]
    fn marker_at(&self, src: u32) -> Option<u16> {
        // `markers` is sorted ascending by `out_pos` (push order).
        self.markers
            .binary_search_by_key(&src, |m| m.out_pos)
            .ok()
            .map(|i| self.markers[i].prefix_offset)
    }
}

/// Decode one DEFLATE block speculatively.
///
/// Appends decoded bytes to `chunk.bytes` and any unresolvable back-refs to
/// `chunk.markers`. Returns `true` iff this block had BFINAL set.
pub fn inflate_block_speculative(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
) -> Result<bool, DeflateError> {
    let bfinal = br.read(1)? != 0;
    let btype = br.read(2)?;
    match btype {
        0 => inflate_stored(br, chunk)?,
        1 => {
            decode_block(br, chunk, &FIXED_LIT, &FIXED_DIST)?;
        }
        2 => inflate_dynamic(br, chunk)?,
        _ => return Err(DeflateError::Invalid("reserved block type")),
    }
    if bfinal {
        chunk.final_block_seen = true;
    }
    Ok(bfinal)
}

/// Decode DEFLATE blocks speculatively until BFINAL is seen.
pub fn inflate_speculative(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
) -> Result<(), DeflateError> {
    loop {
        if inflate_block_speculative(br, chunk)? {
            return Ok(());
        }
    }
}

fn inflate_stored(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
) -> Result<(), DeflateError> {
    br.byte_align();
    let len = br.read(16)? as u16;
    let nlen = br.read(16)? as u16;
    if !len ^ nlen != 0 {
        return Err(DeflateError::Invalid("stored block: LEN/NLEN mismatch"));
    }
    chunk.bytes.reserve(len as usize);
    for _ in 0..len {
        chunk.push_literal(br.read(8)? as u8);
    }
    Ok(())
}

fn inflate_dynamic(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
) -> Result<(), DeflateError> {
    let hlit = br.read(5)? as usize + 257;
    let hdist = br.read(5)? as usize + 1;
    let hclen = br.read(4)? as usize + 4;
    if hlit > 286 || hdist > 30 {
        return Err(DeflateError::Invalid("dynamic block: HLIT/HDIST out of range"));
    }
    let mut cl_lengths = [0u8; 19];
    for i in 0..hclen {
        cl_lengths[tables::CODE_LENGTH_ORDER[i]] = br.read(3)? as u8;
    }

    let mut pool = chunk.decoder_pool.take().unwrap_or_else(|| Box::new(DecoderPool::new()));
    pool.cl.rebuild_from_lengths(&cl_lengths, false)?;

    let total = hlit + hdist;
    pool.lengths.clear();
    pool.lengths.resize(total, 0);
    let mut i = 0;
    while i < total {
        let sym = pool.cl.decode(br)?;
        let lengths = &mut pool.lengths;
        match sym {
            0..=15 => {
                lengths[i] = sym as u8;
                i += 1;
            }
            16 => {
                if i == 0 {
                    return Err(DeflateError::Invalid("code 16 with no previous"));
                }
                let n = (br.read(2)? as usize) + 3;
                let prev = lengths[i - 1];
                if i + n > total {
                    return Err(DeflateError::Invalid("code 16 overruns lengths"));
                }
                for j in 0..n {
                    lengths[i + j] = prev;
                }
                i += n;
            }
            17 => {
                let n = (br.read(3)? as usize) + 3;
                if i + n > total {
                    return Err(DeflateError::Invalid("code 17 overruns lengths"));
                }
                i += n;
            }
            18 => {
                let n = (br.read(7)? as usize) + 11;
                if i + n > total {
                    return Err(DeflateError::Invalid("code 18 overruns lengths"));
                }
                i += n;
            }
            _ => return Err(DeflateError::Invalid("bad code-length symbol")),
        }
    }
    pool.lit.rebuild_from_lengths_litlen(&pool.lengths[..hlit])?;
    pool.dist.rebuild_from_lengths(&pool.lengths[hlit..], false)?;
    let result = decode_block(br, chunk, &pool.lit, &pool.dist);
    chunk.decoder_pool = Some(pool);
    result
}

#[allow(unsafe_code)]
fn decode_block(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
) -> Result<(), DeflateError> {
    // One refill per iteration covers the worst-case 48-bit symbol cost.
    // See decode_block in inflate.rs for the bit budget.
    const NEEDED: u32 = 48;
    // Headroom we keep available in the output buffer so the literal hot
    // path can write through a raw pointer without per-byte capacity checks.
    // When `cur` reaches `cap`, we sync, reserve another `HEADROOM` bytes,
    // and re-acquire the pointer. A 4 KiB headroom amortizes the capacity
    // check to ~once per 4 KiB of decoded literals.
    const HEADROOM: usize = 4096;
    if chunk.bytes.capacity() - chunk.bytes.len() < HEADROOM {
        chunk.bytes.reserve(HEADROOM);
    }
    let mut cap = chunk.bytes.capacity();
    let mut ptr = chunk.bytes.as_mut_ptr();
    let mut cur = chunk.bytes.len();

    let result: Result<(), DeflateError> = 'outer: loop {
        if let Err(e) = br.ensure_bits(NEEDED) {
            break 'outer Err(e);
        }
        let entry = match lit.lookup_filled(br) {
            Ok(e) => e,
            Err(e) => break 'outer Err(e),
        };
        if entry & HUFFDEC_LITERAL != 0 {
            // Literal hot path: raw pointer write, no Vec::push round-trip.
            if cur == cap {
                unsafe { chunk.bytes.set_len(cur); }
                chunk.bytes.reserve(HEADROOM);
                cap = chunk.bytes.capacity();
                ptr = chunk.bytes.as_mut_ptr();
            }
            unsafe { *ptr.add(cur) = (entry >> 16) as u8; }
            cur += 1;
        } else if entry & HUFFDEC_EXCEPTIONAL != 0 {
            if entry >> 16 == 0 {
                break 'outer Ok(()); // EOB (sym 256)
            }
            unsafe { chunk.bytes.set_len(cur); }
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        } else {
            let length_base = ((entry >> 16) & 0x1ff) as usize;
            let extra = ((entry >> 8) & 0x1f) as u32;
            let mut length = length_base;
            if extra > 0 {
                length += br.peek_bits_unchecked(extra) as usize;
                br.consume(extra);
            }
            let dsym = match dist.decode_filled(br) {
                Ok(s) => s,
                Err(e) => {
                    unsafe { chunk.bytes.set_len(cur); }
                    break 'outer Err(e);
                }
            };
            if dsym >= 30 {
                unsafe { chunk.bytes.set_len(cur); }
                break 'outer Err(DeflateError::Invalid("distance symbol out of range"));
            }
            let di = dsym as usize;
            let mut distance = DISTANCE_BASE[di] as usize;
            let dextra = DISTANCE_EXTRA[di];
            if dextra > 0 {
                distance += br.peek_bits_unchecked(dextra as u32) as usize;
                br.consume(dextra as u32);
            }
            if distance == 0 || distance > 32 * 1024 {
                unsafe { chunk.bytes.set_len(cur); }
                break 'outer Err(DeflateError::Invalid("distance out of range"));
            }
            // Sync our local `cur` into chunk.bytes so emit_back_ref sees
            // the correct length. emit_back_ref may also reserve, so we
            // refresh the pointer / capacity afterward.
            unsafe { chunk.bytes.set_len(cur); }
            if let Err(e) = emit_back_ref(chunk, distance, length) {
                cur = chunk.bytes.len();
                break 'outer Err(e);
            }
            cur = chunk.bytes.len();
            // Ensure we still have HEADROOM of slack for the next literal run.
            if cap - cur < HEADROOM {
                chunk.bytes.reserve(HEADROOM);
                cap = chunk.bytes.capacity();
            }
            ptr = chunk.bytes.as_mut_ptr();
        }
    };

    unsafe { chunk.bytes.set_len(cur); }
    result
}

/// Emit `length` bytes of a back-reference at `distance` from current
/// position.
///
/// Three cases per byte:
/// 1. Source is in the unknown prefix         → emit a fresh marker.
/// 2. Source is in-chunk and is itself a marker → propagate the marker
///    (the target position becomes a marker with the same prefix_offset).
/// 3. Source is in-chunk and is a real literal → copy the byte.
///
/// Case 2 is the load-bearing one: without it, an in-chunk back-reference
/// that copies from a placeholder would silently produce a zero byte, and
/// the eventual marker resolution would never visit that target.
#[inline]
#[allow(unsafe_code)]
fn emit_back_ref(
    chunk: &mut SpeculativeChunk,
    distance: usize,
    length: usize,
) -> Result<(), DeflateError> {
    chunk.bytes.reserve(length);
    let cur = chunk.bytes.len();

    if cur < distance {
        // Source falls (at least partly) in the unknown prefix: every byte
        // of the copy that lands there becomes a fresh marker. The rest, if
        // any, falls in-chunk at increasing positions starting from 0.
        for _ in 0..length {
            let cur_now = chunk.bytes.len();
            if cur_now < distance {
                let prefix_offset = distance - cur_now - 1;
                debug_assert!(prefix_offset <= 32 * 1024);
                chunk.push_marker(prefix_offset as u16);
            } else {
                // In-chunk part — propagate via the slow path. Source bytes
                // here may themselves be markers we just emitted.
                let src = cur_now - distance;
                if let Some(po) = chunk.marker_at(src as u32) {
                    chunk.push_marker(po);
                } else {
                    let b = chunk.bytes[src];
                    chunk.push_literal(b);
                }
            }
        }
        return Ok(());
    }

    // Pure in-chunk back-reference (cur >= distance).
    let src = cur - distance;

    // Hot fast path: no overlap (src+length <= cur), and source range is
    // guaranteed marker-free. Markers live at positions <= max_marker_pos,
    // so if the FIRST source byte (the lowest position) is past it, none of
    // the source bytes are markers.
    //
    // Inlined 8-byte unrolled copy. `chunk.bytes.reserve(length)` above
    // guarantees `cap >= cur + length`. `distance >= length` ⇒ src+length ≤
    // cur, so loads from src..src+length never touch [cur, cur+length).
    if distance >= length && chunk.cannot_be_marker(src as u32) {
        unsafe {
            let buf = chunk.bytes.as_mut_ptr();
            if length >= 8 {
                let mut n = length;
                let mut sp = buf.add(src);
                let mut dp = buf.add(cur);
                while n >= 8 {
                    let v = std::ptr::read_unaligned(sp as *const u64);
                    std::ptr::write_unaligned(dp as *mut u64, v);
                    sp = sp.add(8);
                    dp = dp.add(8);
                    n -= 8;
                }
                if n > 0 {
                    let v = std::ptr::read_unaligned(
                        buf.add(src + length - 8) as *const u64,
                    );
                    std::ptr::write_unaligned(
                        buf.add(cur + length - 8) as *mut u64,
                        v,
                    );
                }
            } else {
                let mut i = 0;
                while i < length {
                    *buf.add(cur + i) = *buf.add(src + i);
                    i += 1;
                }
            }
            chunk.bytes.set_len(cur + length);
        }
        return Ok(());
    }

    // Slow path: either source range overlaps the destination, or it might
    // contain a marker we need to propagate.
    for _ in 0..length {
        let cur_now = chunk.bytes.len();
        let s = cur_now - distance;
        if chunk.cannot_be_marker(s as u32) {
            let b = chunk.bytes[s];
            chunk.push_literal(b);
        } else if let Some(po) = chunk.marker_at(s as u32) {
            chunk.push_marker(po);
        } else {
            let b = chunk.bytes[s];
            chunk.push_literal(b);
        }
    }
    Ok(())
}

/// Substitute marker placeholders using the previous chunk's tail window.
///
/// `prev_tail` must contain at least one byte. The byte at index
/// `prev_tail.len() - 1` is the byte immediately preceding the chunk's
/// first output byte (i.e., `prefix_offset == 0`). Older bytes have larger
/// indices into `prev_tail` from the right.
///
/// Returns an error if any marker references a `prefix_offset` deeper than
/// `prev_tail.len()`. That would mean the chunk's distance exceeded the
/// available window, which shouldn't happen for valid DEFLATE streams.
pub fn resolve_markers(
    chunk: &mut SpeculativeChunk,
    prev_tail: &[u8],
) -> Result<(), DeflateError> {
    for m in &chunk.markers {
        let off = m.prefix_offset as usize;
        if off >= prev_tail.len() {
            return Err(DeflateError::Invalid(
                "marker references bytes outside the available prefix window",
            ));
        }
        // prefix_offset 0 = last byte of prev_tail.
        let idx = prev_tail.len() - 1 - off;
        let b = prev_tail[idx];
        let pos = m.out_pos as usize;
        chunk.bytes[pos] = b;
    }
    chunk.markers.clear();
    chunk.max_marker_pos = None;
    Ok(())
}

/// Compute the resolved tail window of `chunk` (last `min(32 KiB, len)`
/// bytes of `chunk.bytes`). Caller must ensure `chunk` is resolved.
pub fn tail_window(chunk: &SpeculativeChunk) -> &[u8] {
    debug_assert!(chunk.is_resolved(), "tail_window requires resolved chunk");
    let n = chunk.bytes.len();
    let start = n.saturating_sub(32 * 1024);
    &chunk.bytes[start..]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inflate;

    /// Build a deflate stream by piping through gzip and stripping the
    /// fixed 10-byte header and 8-byte trailer. (Same trick as in the
    /// `inflate` module's tests.)
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
        let body = &out.stdout[10..out.stdout.len() - 8];
        body.to_vec()
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

    /// Sanity: speculative decode from byte 0 (no unknown prefix) must
    /// produce the same bytes as the serial decoder. Markers may be present
    /// (early back-refs that look "unresolved" because the chunk's first
    /// bytes are nominally "after" the prefix), but with an empty prefix
    /// this is impossible — `cur >= distance` always holds at distance 0,
    /// but distance >= 1, so for the first ~few bytes some back-refs WILL
    /// produce markers. We need an empty-prefix-aware resolution: pass
    /// `prev_tail = &[]` and verify there are zero markers.
    #[test]
    fn speculative_from_byte_zero_no_markers_when_no_backrefs() {
        // Tiny payload — no back-references possible.
        let body = deflate_via_gzip(b"abc", 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);
        let mut br = BitReader::new(&padded);
        let mut chunk = SpeculativeChunk::default();
        inflate_speculative(&mut br, &mut chunk).unwrap();
        assert_eq!(chunk.bytes, b"abc");
        assert!(
            chunk.markers.is_empty(),
            "tiny payload shouldn't produce markers, got {} markers",
            chunk.markers.len()
        );
    }

    /// Speculative decode of a longer payload from byte 0 may produce
    /// markers (back-refs to "before the chunk start" — but the chunk
    /// started at the beginning of the stream, so any back-ref into the
    /// prefix is an encoder bug we treat as out-of-window).
    /// In that case the resolver with `prev_tail=&[]` must error.
    #[test]
    fn speculative_from_byte_zero_against_full_stream() {
        // Repeating data → back-references early in the stream.
        let payload = ascii_payload(8192);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);

        // Speculative decode the whole stream as one chunk.
        let mut br = BitReader::new(&padded);
        let mut chunk = SpeculativeChunk::default();
        inflate_speculative(&mut br, &mut chunk).unwrap();

        // Even from byte 0, gzip *can* emit a back-ref before any literal
        // has been produced if the encoder uses cross-block back-refs.
        // It can't — the very first block's first symbol is always a
        // literal in practice. So markers should be empty here.
        assert!(chunk.markers.is_empty());
        assert_eq!(chunk.bytes, payload);
    }

    /// The big one: chop a DEFLATE stream at a real block boundary,
    /// speculatively decode each piece, resolve, stitch — must equal the
    /// serial decoder's output.
    #[test]
    fn chunked_speculative_then_resolve() {
        // Need a payload that gzip splits into ≥3 blocks. Highly compressible
        // input gives few large blocks; pseudo-random ASCII keeps blocks small
        // and produces many of them. 4 MB of pseudo-random ASCII at level 6
        // reliably gives dozens of blocks.
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);

        // Find block boundaries via an instrumented serial decode.
        let boundaries = record_block_starts(&padded).expect("record");
        assert!(
            boundaries.len() >= 3,
            "need at least 2 blocks beyond the first to test chunking"
        );

        // Pick boundaries[1] and boundaries[2] as chunk splits — we'll have
        // chunk0: [0 .. boundaries[1])
        // chunk1: [boundaries[1] .. boundaries[2])
        // chunk2: [boundaries[2] .. end]   (contains BFINAL)
        let split_a_bit = boundaries[1];
        let split_b_bit = boundaries[2];

        // Decode chunk 0 the easy way (serial), it knows everything.
        let mut chunk0 = SpeculativeChunk::default();
        let mut br0 = BitReader::new(&padded);
        decode_until_bit(&mut br0, &mut chunk0, split_a_bit).unwrap();
        assert!(chunk0.is_resolved(), "chunk0 from byte 0 has no prefix");

        // Decode chunks 1 and 2 speculatively starting at their split bit.
        let chunk1 = decode_chunk(&padded, split_a_bit, Some(split_b_bit));
        let chunk2 = decode_chunk(&padded, split_b_bit, None);

        // Resolve in order.
        let mut chunk1 = chunk1;
        let mut chunk2 = chunk2;
        resolve_markers(&mut chunk1, tail_window(&chunk0)).unwrap();
        resolve_markers(&mut chunk2, tail_window(&chunk1)).unwrap();

        let mut stitched = Vec::new();
        stitched.extend_from_slice(&chunk0.bytes);
        stitched.extend_from_slice(&chunk1.bytes);
        stitched.extend_from_slice(&chunk2.bytes);

        assert_eq!(stitched.len(), payload.len(), "length mismatch");
        assert_eq!(stitched, payload, "stitched output != original payload");
    }

    /// Helper: serial decode until we hit a block whose start ≥ `until_bit`.
    /// Stops without consuming the matching block.
    fn decode_until_bit(
        br: &mut BitReader<'_>,
        chunk: &mut SpeculativeChunk,
        until_bit: u64,
    ) -> Result<(), DeflateError> {
        // We use the regular serial inflate but block-by-block, peeking at
        // the position before each block.
        loop {
            if br.tell_bit() >= until_bit {
                return Ok(());
            }
            let bfinal = inflate::inflate_block(br, &mut chunk.bytes)?;
            if bfinal {
                return Ok(());
            }
        }
    }

    /// Helper: speculatively decode blocks starting at `start_bit` and
    /// stopping when (a) we'd start a block at or past `stop_bit_exclusive`,
    /// or (b) BFINAL is reached.
    fn decode_chunk(
        input: &[u8],
        start_bit: u64,
        stop_bit_exclusive: Option<u64>,
    ) -> SpeculativeChunk {
        let mut br = BitReader::new(input);
        // Skip ahead to start_bit by reading bits.
        let mut to_skip = start_bit;
        while to_skip > 0 {
            let n = to_skip.min(32) as u32;
            br.read(n).unwrap();
            to_skip -= n as u64;
        }
        let mut chunk = SpeculativeChunk::default();
        loop {
            if let Some(stop) = stop_bit_exclusive {
                if br.tell_bit() >= stop {
                    return chunk;
                }
            }
            let bfinal = inflate_block_speculative(&mut br, &mut chunk).unwrap();
            if bfinal {
                return chunk;
            }
        }
    }

    /// Run a serial decode, recording the bit position at the start of each
    /// block. (block 0 always starts at bit 0.) Returns positions in order.
    fn record_block_starts(input: &[u8]) -> Result<Vec<u64>, DeflateError> {
        let mut br = BitReader::new(input);
        let mut out = Vec::new();
        let mut starts = Vec::new();
        loop {
            starts.push(br.tell_bit());
            let bfinal = inflate::inflate_block(&mut br, &mut out)?;
            if bfinal {
                return Ok(starts);
            }
        }
    }
}
