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
//! We keep two parallel buffers:
//!   - `bytes: Vec<u8>` — the chunk output, with a placeholder `0` byte
//!     wherever a marker lives.
//!   - `markers: Vec<Marker>` — sparse list of `(out_pos, prefix_offset)`.
//!
//! `prefix_offset` is the distance into the unknown prefix counted backwards
//! from the byte just before the chunk's first byte: 0 = the immediately
//! preceding byte, 1 = the one before that, …, 32767 = the oldest byte
//! still in the 32 KiB window.
//!
//! In real-world streams the marker count is small: roughly
//! `O(32 KiB / mean_match_distance)` per chunk. After ~32 KiB of decoded
//! output, every back-reference lands in-chunk and stops producing markers.

use crate::tables::{
    DISTANCE_BASE, DISTANCE_EXTRA, LENGTH_BASE, LENGTH_EXTRA,
};
use crate::{tables, BitReader, DeflateError, HuffmanDecoder};

/// One unresolved back-reference byte. See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    /// Position within `SpeculativeChunk::bytes`.
    pub out_pos: u32,
    /// Distance back into the unknown prefix, 0-based:
    /// `prefix_offset = 0` means "the byte immediately before the chunk".
    pub prefix_offset: u16,
}

/// Sentinel for "this output byte is a real literal, not a marker".
/// DEFLATE's max distance is 32768, so values 0..32768 are valid offsets
/// and `u16::MAX` is unambiguous.
const NO_MARKER: u16 = u16::MAX;

/// One chunk's worth of speculatively-decoded output.
///
/// Uses two parallel buffers:
///   - `bytes` holds output bytes, with `0` at marker positions as a
///     placeholder.
///   - `marker_at` is the same length as `bytes`; `NO_MARKER` means
///     "literal byte", any other value is a prefix offset.
///
/// `markers` is a sparse list of marker positions used for the resolution
/// loop. It's kept in sync with `marker_at` and is the canonical way to
/// iterate just the marker bytes.
///
/// The `marker_at` lookup is what makes back-reference propagation correct:
/// when an in-chunk back-reference's *source* byte is itself a marker, we
/// must emit a new marker at the target position rather than copying the
/// placeholder zero.
#[derive(Debug, Default)]
pub struct SpeculativeChunk {
    pub bytes: Vec<u8>,
    marker_at: Vec<u16>,
    pub markers: Vec<Marker>,
    /// True if the last block consumed had BFINAL set.
    pub final_block_seen: bool,
}

impl SpeculativeChunk {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_resolved(&self) -> bool {
        self.markers.is_empty()
    }

    #[inline]
    fn push_literal(&mut self, b: u8) {
        self.bytes.push(b);
        self.marker_at.push(NO_MARKER);
    }

    #[inline]
    fn push_marker(&mut self, prefix_offset: u16) {
        debug_assert!(prefix_offset != NO_MARKER);
        let pos = self.bytes.len() as u32;
        self.bytes.push(0);
        self.marker_at.push(prefix_offset);
        self.markers.push(Marker { out_pos: pos, prefix_offset });
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
            let lit = HuffmanDecoder::from_lengths(&tables::fixed_literal_lengths())?;
            let dist = HuffmanDecoder::from_lengths(&tables::fixed_distance_lengths())?;
            decode_block(br, chunk, &lit, &dist)?;
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
    chunk.marker_at.reserve(len as usize);
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
    let cl_decoder = HuffmanDecoder::from_lengths(&cl_lengths)?;

    let total = hlit + hdist;
    let mut lengths = vec![0u8; total];
    let mut i = 0;
    while i < total {
        let sym = cl_decoder.decode(br)?;
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
    let lit = HuffmanDecoder::from_lengths(&lengths[..hlit])?;
    let dist = HuffmanDecoder::from_lengths(&lengths[hlit..])?;
    decode_block(br, chunk, &lit, &dist)
}

fn decode_block(
    br: &mut BitReader<'_>,
    chunk: &mut SpeculativeChunk,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
) -> Result<(), DeflateError> {
    loop {
        let sym = lit.decode(br)?;
        match sym {
            0..=255 => chunk.push_literal(sym as u8),
            256 => return Ok(()),
            257..=285 => {
                let li = (sym - 257) as usize;
                let mut length = LENGTH_BASE[li] as usize;
                let extra = LENGTH_EXTRA[li];
                if extra > 0 {
                    length += br.read(extra as u32)? as usize;
                }
                let dsym = dist.decode(br)?;
                if dsym >= 30 {
                    return Err(DeflateError::Invalid("distance symbol out of range"));
                }
                let di = dsym as usize;
                let mut distance = DISTANCE_BASE[di] as usize;
                let dextra = DISTANCE_EXTRA[di];
                if dextra > 0 {
                    distance += br.read(dextra as u32)? as usize;
                }
                if distance == 0 || distance > 32 * 1024 {
                    return Err(DeflateError::Invalid("distance out of range"));
                }
                emit_back_ref(chunk, distance, length)?;
            }
            _ => return Err(DeflateError::Invalid("literal/length symbol out of range")),
        }
    }
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
fn emit_back_ref(
    chunk: &mut SpeculativeChunk,
    distance: usize,
    length: usize,
) -> Result<(), DeflateError> {
    chunk.bytes.reserve(length);
    chunk.marker_at.reserve(length);
    for _ in 0..length {
        let cur = chunk.bytes.len();
        if cur >= distance {
            let src = cur - distance;
            let src_marker = chunk.marker_at[src];
            if src_marker == NO_MARKER {
                let b = chunk.bytes[src];
                chunk.push_literal(b);
            } else {
                chunk.push_marker(src_marker);
            }
        } else {
            let prefix_offset = distance - cur - 1;
            debug_assert!(prefix_offset < NO_MARKER as usize); // DEFLATE caps at 32 KiB
            chunk.push_marker(prefix_offset as u16);
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
        chunk.marker_at[pos] = NO_MARKER;
    }
    chunk.markers.clear();
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
