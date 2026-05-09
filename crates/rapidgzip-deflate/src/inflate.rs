//! Serial DEFLATE inflate (RFC 1951).
//!
//! Window-known mode only — i.e., the decoder is initialized with the prior
//! 32 KiB (or empty if at start of stream) and decodes block-by-block,
//! resolving back-references against the running output. Phase 2 will add
//! a separate "no-window / speculative" path that emits markers.
//!
//! Output is appended to a caller-owned `Vec<u8>`. Back-references look
//! into that same buffer — so the caller controls memory: trim it
//! periodically if streaming long files.

use crate::tables::*;
use crate::{BitReader, DeflateError, HuffmanDecoder};

/// Maximum back-reference distance permitted by DEFLATE.
pub const MAX_DISTANCE: usize = 32 * 1024;

/// Decode one DEFLATE block. Returns `true` if this was the final block
/// (BFINAL set). Appends decompressed bytes to `out`.
pub fn inflate_block(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
) -> Result<bool, DeflateError> {
    let bfinal = br.read(1)? != 0;
    let btype = br.read(2)?;
    match btype {
        0 => inflate_stored(br, out)?,
        1 => inflate_fixed(br, out)?,
        2 => inflate_dynamic(br, out)?,
        _ => return Err(DeflateError::Invalid("reserved block type")),
    }
    Ok(bfinal)
}

/// Decode an entire DEFLATE stream until BFINAL is reached.
pub fn inflate(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    loop {
        if inflate_block(br, out)? {
            return Ok(());
        }
    }
}

fn inflate_stored(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    br.byte_align();
    let len = br.read(16)? as u16;
    let nlen = br.read(16)? as u16;
    if !len ^ nlen != 0 {
        // RFC 1951 §3.2.4: NLEN is the one's complement of LEN.
        return Err(DeflateError::Invalid("stored block: LEN/NLEN mismatch"));
    }
    out.reserve(len as usize);
    for _ in 0..len {
        out.push(br.read(8)? as u8);
    }
    Ok(())
}

fn inflate_fixed(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    let lit = HuffmanDecoder::from_lengths(&fixed_literal_lengths())?;
    let dist = HuffmanDecoder::from_lengths(&fixed_distance_lengths())?;
    decode_block(br, out, &lit, &dist)
}

fn inflate_dynamic(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    let (lit, dist) = read_dynamic_header(br)?;
    decode_block(br, out, &lit, &dist)
}

/// Parse a dynamic-Huffman block header. Caller must have already consumed
/// the BFINAL bit and the 2 BTYPE bits. On success, returns the literal/length
/// and distance Huffman decoders, and the bitreader is positioned at the
/// start of the compressed body. On any structural failure returns
/// `DeflateError`. Used by the block finder to verify candidate offsets.
pub fn read_dynamic_header(
    br: &mut BitReader<'_>,
) -> Result<(HuffmanDecoder, HuffmanDecoder), DeflateError> {
    let hlit = br.read(5)? as usize + 257;
    let hdist = br.read(5)? as usize + 1;
    let hclen = br.read(4)? as usize + 4;

    if hlit > 286 || hdist > 30 {
        return Err(DeflateError::Invalid("dynamic block: HLIT/HDIST out of range"));
    }

    let mut cl_lengths = [0u8; 19];
    for i in 0..hclen {
        cl_lengths[CODE_LENGTH_ORDER[i]] = br.read(3)? as u8;
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

    // The end-of-block symbol must be representable, otherwise the block
    // can never terminate. This is the cheapest "is this really a block?"
    // signal beyond the canonical-Huffman check.
    if lengths[256] == 0 {
        return Err(DeflateError::Invalid("EOB symbol has zero length"));
    }

    let lit = HuffmanDecoder::from_lengths(&lengths[..hlit])?;
    let dist = HuffmanDecoder::from_lengths(&lengths[hlit..])?;
    Ok((lit, dist))
}

fn decode_block(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
) -> Result<(), DeflateError> {
    loop {
        let sym = lit.decode(br)?;
        match sym {
            0..=255 => out.push(sym as u8),
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
                if distance == 0 || distance > out.len() {
                    return Err(DeflateError::Invalid(
                        "back-reference distance out of bounds",
                    ));
                }
                if distance > MAX_DISTANCE {
                    return Err(DeflateError::Invalid("distance > 32 KiB"));
                }
                copy_back(out, distance, length);
            }
            _ => return Err(DeflateError::Invalid("literal/length symbol out of range")),
        }
    }
}

/// Copy `length` bytes from position `out.len() - distance` to the end of
/// `out`. Overlapping copies are well-defined here: each byte is read after
/// it's written for the run-length-encoding case (`distance == 1`).
#[inline]
fn copy_back(out: &mut Vec<u8>, distance: usize, length: usize) {
    out.reserve(length);
    let start = out.len() - distance;
    if distance >= length {
        // Non-overlapping: do it as a single extend.
        let src_end = start + length;
        out.extend_from_within(start..src_end);
    } else {
        // Overlap: byte-by-byte. distance < length means we read bytes that
        // were just written. distance == 1 is the all-same-byte run.
        for i in 0..length {
            let b = out[start + i];
            out.push(b);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deflate stream by piping through the system `gzip` and
    /// stripping the gzip framing — keeps tests anchored to a real encoder.
    fn deflate_via_gzip(payload: &[u8], level: u32) -> Vec<u8> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let mut child = Command::new("gzip")
            .args([&format!("-{level}"), "-c", "-n"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn gzip");
        let mut stdin = child.stdin.take().unwrap();
        let payload = payload.to_vec();
        let writer = std::thread::spawn(move || {
            stdin.write_all(&payload).unwrap();
        });
        let out = child.wait_with_output().expect("wait gzip");
        writer.join().unwrap();
        assert!(out.status.success());
        // Strip gzip framing: 10-byte header (no flags assumed here since `-n`),
        // 8-byte trailer (CRC32 + ISIZE).
        let body = &out.stdout[10..out.stdout.len() - 8];
        body.to_vec()
    }

    fn check_roundtrip(payload: &[u8], level: u32) {
        let deflate_bytes = deflate_via_gzip(payload, level);
        // Pad the input so peek(LUT_BITS) past end-of-stream stays in bounds.
        let mut padded = deflate_bytes.clone();
        padded.extend_from_slice(&[0u8; 16]);
        let mut br = BitReader::new(&padded);
        let mut out = Vec::new();
        inflate(&mut br, &mut out).expect("inflate failed");
        assert_eq!(
            out, payload,
            "roundtrip mismatch (payload {} bytes, level {level})",
            payload.len()
        );
    }

    #[test]
    fn empty_payload() { check_roundtrip(b"", 6); }

    #[test]
    fn tiny_payload() { check_roundtrip(b"hello, world\n", 6); }

    #[test]
    fn repeating_payload() {
        // Heavy back-references — exercises copy_back.
        let mut p = Vec::new();
        for _ in 0..1000 { p.extend_from_slice(b"abcdefghij"); }
        check_roundtrip(&p, 6);
    }

    #[test]
    fn run_length_payload() {
        // distance == 1 corner case.
        let p = vec![b'x'; 10000];
        check_roundtrip(&p, 6);
    }

    #[test]
    fn ascii_1k() {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut p = Vec::with_capacity(1024);
        while p.len() < 1024 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        check_roundtrip(&p, 6);
    }

    #[test]
    fn fixed_huffman_path() {
        // Very short input → gzip picks fixed Huffman.
        check_roundtrip(b"aaaaaaaaaa", 9);
    }

    #[test]
    fn stored_block_path() {
        // Random bytes at low level → stored blocks.
        let mut s: u64 = 0xA1B2C3D4E5F60718;
        let mut p = Vec::with_capacity(8192);
        while p.len() < 8192 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.extend_from_slice(&s.to_le_bytes());
        }
        check_roundtrip(&p, 1);
    }

    #[test]
    fn level_9_dynamic() {
        let mut p = Vec::new();
        for i in 0..50000u32 {
            p.extend_from_slice(format!("line {i}: hello world\n").as_bytes());
        }
        check_roundtrip(&p, 9);
    }
}
