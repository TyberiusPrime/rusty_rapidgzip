//! Serial DEFLATE inflate (RFC 1951).
//!
//! Window-known mode: decodes block-by-block, resolving back-references
//! against the running output. The speculative (no-window) path that handles
//! mid-stream worker entry is in `speculative_zlib`.
//!
//! Output is appended to a caller-owned `Vec<u8>`. Back-references look
//! into that same buffer — so the caller controls memory: trim it
//! periodically if streaming long files.

use crate::huffman::{HUFFDEC_EXCEPTIONAL, HUFFDEC_LITERAL, LUT_BITS};
use crate::tables::*;
use crate::{BitReader, DeflateError, HuffmanDecoder};

/// Maximum back-reference distance permitted by DEFLATE.
pub const MAX_DISTANCE: usize = 32 * 1024;

/// Decode one DEFLATE block. Returns `true` if this was the final block
/// (BFINAL set). Appends decompressed bytes to `out`.
pub fn inflate_block(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<bool, DeflateError> {
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
    let lit = HuffmanDecoder::from_lengths_litlen(&fixed_literal_lengths())?;
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
        return Err(DeflateError::Invalid(
            "dynamic block: HLIT/HDIST out of range",
        ));
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

    let lit = HuffmanDecoder::from_lengths_litlen(&lengths[..hlit])?;
    let dist = HuffmanDecoder::from_lengths(&lengths[hlit..])?;
    Ok((lit, dist))
}

#[allow(unsafe_code)]
fn decode_block(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
) -> Result<(), DeflateError> {
    // Worst-case 48-bit symbol: 15 (lit) + 5 (length-extra) + 15 (dist) + 13.
    const NEEDED: u32 = 48;
    const HEADROOM: usize = 4096;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;

    if out.capacity() - out.len() < HEADROOM {
        out.reserve(HEADROOM);
    }
    let mut cap = out.capacity();
    let mut out_ptr = out.as_mut_ptr();
    let mut cur = out.len();

    // Pull bit-reader state into locals — the whole point of this function.
    // We sync back on exit and around any helper that touches `br`.
    let mut buf = br.buf;
    let mut bits = br.bits;
    let mut byte_pos = br.byte_pos;
    // Copy the slice handle (ptr+len) into a local — releases the borrow on
    // `br` so we can still sync `br.*` inside the loop, while letting the
    // refill use bounds-checked indexing that the explicit guards elide.
    let input: &[u8] = br.input;
    let lit_lut = lit.lut();
    let dist_lut = dist.lut();

    // Publish the locally-tracked output length back to `out`.
    //
    // SAFETY: every byte in `out[..cur]` has been initialised — each literal
    // and back-reference writes through `out_ptr` *before* advancing `cur`, and
    // `cur` only ever grows past bytes we just wrote. So `out`'s first `cur`
    // bytes are always valid, and publishing that length is sound at any point.
    macro_rules! publish_len {
        () => {
            unsafe { out.set_len(cur) }
        };
    }

    let result: Result<(), DeflateError> = 'outer: loop {
        // ---- Local refill to >= NEEDED bits. ----
        if bits < NEEDED {
            if byte_pos + 8 <= input.len() {
                // Fast path: unaligned 8-byte LE load. `bits <= 48 < 56` so
                // the shift is always defined. The guard makes the slice index
                // and `try_into` fold to a bare unaligned load.
                let chunk = u64::from_le_bytes(
                    input[byte_pos..byte_pos + 8]
                        .try_into()
                        .expect("8-byte slice (byte_pos + 8 <= input.len() guarded above)"),
                );
                buf |= chunk << bits;
                let added = 64 - bits;
                byte_pos += (added / 8) as usize;
                bits += (added / 8) * 8;
            } else {
                // Slow byte-by-byte until either enough bits or EOF.
                while bits < NEEDED {
                    if byte_pos >= input.len() {
                        // Sync, mark exhausted, return.
                        br.buf = buf;
                        br.bits = bits;
                        br.byte_pos = byte_pos;
                        br.exhausted = true;
                        publish_len!();
                        break 'outer Err(DeflateError::UnexpectedEof);
                    }
                    buf |= (input[byte_pos] as u64) << bits;
                    byte_pos += 1;
                    bits += 8;
                }
            }
        }

        // ---- Decode lit/length symbol via direct LUT lookup. ----
        let entry = {
            let idx = (buf & LUT_MASK) as usize;
            let e = lit_lut[idx];
            let len = e & 0xff;
            if len == 0 {
                // Cold: long-code chain. Sync state, call, reload.
                br.buf = buf;
                br.bits = bits;
                br.byte_pos = byte_pos;
                match lit.lookup_long(br) {
                    Ok(e) => {
                        buf = br.buf;
                        bits = br.bits;
                        byte_pos = br.byte_pos;
                        e
                    }
                    Err(e) => {
                        // `br` already holds the latest state — we synced
                        // before the call. Don't reload locals; we're done.
                        publish_len!();
                        break 'outer Err(e);
                    }
                }
            } else {
                buf >>= len;
                bits -= len;
                e
            }
        };

        if entry & HUFFDEC_LITERAL != 0 {
            if cur == cap {
                publish_len!();
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe {
                *out_ptr.add(cur) = (entry >> 16) as u8;
            }
            cur += 1;
            continue 'outer;
        }
        if entry & HUFFDEC_EXCEPTIONAL != 0 {
            if entry >> 16 == 0 {
                br.buf = buf;
                br.bits = bits;
                br.byte_pos = byte_pos;
                publish_len!();
                break 'outer Ok(()); // EOB
            }
            br.buf = buf;
            br.bits = bits;
            br.byte_pos = byte_pos;
            publish_len!();
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        }

        // Length code: base + extra (extra ≤ 5 bits, already buffered).
        let length_base = ((entry >> 16) & 0x1ff) as usize;
        let len_extra = (entry >> 8) & 0x1f;
        let mut length = length_base;
        if len_extra > 0 {
            length += (buf & ((1u64 << len_extra) - 1)) as usize;
            buf >>= len_extra;
            bits -= len_extra;
        }

        // Distance symbol — same LUT pattern.
        let dsym = {
            let idx = (buf & LUT_MASK) as usize;
            let e = dist_lut[idx];
            let len = e & 0xff;
            if len == 0 {
                br.buf = buf;
                br.bits = bits;
                br.byte_pos = byte_pos;
                match dist.lookup_long(br) {
                    Ok(e) => {
                        buf = br.buf;
                        bits = br.bits;
                        byte_pos = br.byte_pos;
                        (e >> 16) as u16
                    }
                    Err(e) => {
                        // `br` already holds the latest state — we synced
                        // before the call. Don't reload locals; we're done.
                        publish_len!();
                        break 'outer Err(e);
                    }
                }
            } else {
                buf >>= len;
                bits -= len;
                (e >> 16) as u16
            }
        };

        if dsym >= 30 {
            br.buf = buf;
            br.bits = bits;
            br.byte_pos = byte_pos;
            publish_len!();
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
        if distance == 0 || distance > cur {
            br.buf = buf;
            br.bits = bits;
            br.byte_pos = byte_pos;
            publish_len!();
            break 'outer Err(DeflateError::Invalid(
                "back-reference distance out of bounds",
            ));
        }
        if distance > MAX_DISTANCE {
            br.buf = buf;
            br.bits = bits;
            br.byte_pos = byte_pos;
            publish_len!();
            break 'outer Err(DeflateError::Invalid("distance > 32 KiB"));
        }
        publish_len!();
        copy_back(out, distance, length);
        cur = out.len();
        if cap - cur < HEADROOM {
            out.reserve(HEADROOM);
            cap = out.capacity();
        }
        out_ptr = out.as_mut_ptr();
    };

    result
}

/// Copy `length` bytes from position `out.len() - distance` to the end of
/// `out`. Overlapping copies are well-defined here: each byte is read after
/// it's written for the run-length-encoding case (`distance == 1`).
#[inline]
#[allow(unsafe_code)]
fn copy_back(out: &mut Vec<u8>, distance: usize, length: usize) {
    out.reserve(length);
    let cur = out.len();
    let start = cur - distance;
    // Hot path: non-overlapping copy. Inlined 8-byte unrolled load/store
    // avoids the libc memmove call that dominates the profile on text-heavy
    // inputs (matches are typically 5–30 bytes, where the call overhead
    // alone is comparable to the copy itself).
    if distance >= length {
        unsafe {
            let buf = out.as_mut_ptr();
            if length >= 8 {
                let mut n = length;
                let mut sp = buf.add(start);
                let mut dp = buf.add(cur);
                while n >= 8 {
                    let v = std::ptr::read_unaligned(sp as *const u64);
                    std::ptr::write_unaligned(dp as *mut u64, v);
                    sp = sp.add(8);
                    dp = dp.add(8);
                    n -= 8;
                }
                if n > 0 {
                    // Overlapping tail store: re-copy the last 8 bytes from
                    // the end. Safe because length >= 8 and src is fully
                    // disjoint from dst (distance >= length).
                    let v = std::ptr::read_unaligned(buf.add(start + length - 8) as *const u64);
                    std::ptr::write_unaligned(buf.add(cur + length - 8) as *mut u64, v);
                }
            } else {
                let mut i = 0;
                while i < length {
                    *buf.add(cur + i) = *buf.add(start + i);
                    i += 1;
                }
            }
            out.set_len(cur + length);
        }
    } else if distance == 1 {
        // RLE: broadcast the single source byte. Common in fastq quality runs.
        unsafe {
            let b = *out.as_ptr().add(start);
            let bcast = (b as u64).wrapping_mul(0x0101_0101_0101_0101);
            let buf = out.as_mut_ptr();
            let mut n = length;
            let mut dp = buf.add(cur);
            while n >= 8 {
                std::ptr::write_unaligned(dp as *mut u64, bcast);
                dp = dp.add(8);
                n -= 8;
            }
            while n > 0 {
                *dp = b;
                dp = dp.add(1);
                n -= 1;
            }
            out.set_len(cur + length);
        }
    } else {
        // 2 <= distance < length: short RLE pattern. Byte-by-byte is fine
        // here — these are uncommon and short.
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
            out,
            payload,
            "roundtrip mismatch (payload {} bytes, level {level})",
            payload.len()
        );
    }

    #[test]
    fn empty_payload() {
        check_roundtrip(b"", 6);
    }

    #[test]
    fn tiny_payload() {
        check_roundtrip(b"hello, world\n", 6);
    }

    #[test]
    fn repeating_payload() {
        // Heavy back-references — exercises copy_back.
        let mut p = Vec::new();
        for _ in 0..1000 {
            p.extend_from_slice(b"abcdefghij");
        }
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
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
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
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
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
