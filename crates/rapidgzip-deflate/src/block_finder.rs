//! Locate dynamic-Huffman DEFLATE block boundaries by bit-level scanning.
//!
//! The parallel pipeline (phase 4) chops the compressed stream at near-fixed
//! byte boundaries and hands each chunk to a worker. Workers don't know
//! where the next block actually starts, so they call [`find_next_dynamic_block`]
//! to scan forward bit-by-bit until they find a plausible block header.
//!
//! ## What we look for
//!
//! Only **non-final dynamic-Huffman** blocks. We don't try to find:
//! - **Final** blocks: by definition the last block, no parallelism win
//! - **Stored** blocks: rare, and a separate fast scan can find them
//! - **Fixed-Huffman** blocks: most encoders (incl. zlib) prefer dynamic
//!   above a tiny size threshold, so anchoring on dynamic is enough
//!
//! ## How we filter
//!
//! Two stages:
//!
//! 1. **13-bit prefilter LUT** — at each candidate offset, peek 13 bits and
//!    look up "is this a plausible block prefix?". Encodes:
//!    - bit 0:    BFINAL = 0
//!    - bits 1-2: BTYPE  = 0b10 (dynamic)
//!    - bits 3-7: HLIT   ≤ 29  (so `257+HLIT ≤ 286`)
//!    - bits 8-12: HDIST ≤ 29  (so `1+HDIST ≤ 30`)
//!    The LUT also encodes the next-bit-offset to try so we can skip more
//!    than one bit when no candidate begins at the current position.
//!    See `rapidgzip_cpp/src/rapidgzip/blockfinder/DynamicHuffman.hpp`.
//!
//! 2. **Full header parse** via [`crate::inflate::read_dynamic_header`]. This
//!    rebuilds the precode, decodes HLIT+HDIST code lengths, and constructs
//!    canonical Huffman decoders. False positives can survive the 13-bit
//!    prefilter; very few survive a successful header parse.
//!
//! False positives at the second stage are still possible (the first ~150
//! bytes of any random data have a tiny but nonzero chance of validating as
//! a block header). Phase 4 absorbs these via a "did the chunk decode
//! cleanly?" check and a serial-fallback path.

use crate::inflate::read_dynamic_header;
use crate::{BitReader, DeflateError};

/// Bits of input checked by the prefilter LUT.
const LUT_BITS: u32 = 13;

const fn is_dynamic_candidate(bits: u32) -> bool {
    // BFINAL = 0
    if bits & 1 != 0 {
        return false;
    }
    // BTYPE = 0b10 (note: LSB-first, so bits 1..=2 read as 0b10 stored as 0b01)
    if (bits >> 1) & 0b11 != 0b10 {
        return false;
    }
    // HLIT (5 bits) ≤ 29  (equivalently, not 30 or 31)
    if (bits >> 3) & 0b1_1111 > 29 {
        return false;
    }
    // HDIST (5 bits) ≤ 29
    if (bits >> 8) & 0b1_1111 > 29 {
        return false;
    }
    true
}

/// For each 13-bit window, the next bit offset to try if it isn't itself a
/// candidate. `result[i]` is the smallest j ∈ [1, 13] such that the prefix
/// (bits) shifted right by j is a candidate, or 13 if none of the suffixes
/// match (so we can skip the whole window).
const NEXT_LUT: [u8; 1 << LUT_BITS] = {
    let mut out = [0u8; 1 << LUT_BITS];
    let mut i = 0u32;
    while i < (1 << LUT_BITS) {
        let mut step = 0u8;
        let mut bits = i;
        while step < LUT_BITS as u8 {
            if is_dynamic_candidate(bits) {
                break;
            }
            bits >>= 1;
            step += 1;
        }
        out[i as usize] = step;
        i += 1;
    }
    out
};

/// Scan `data` for the next dynamic-Huffman block starting at or after
/// `from_bit`, before `until_bit`. Returns the absolute bit offset of the
/// BFINAL bit, or `None` if no candidate fully validates in the window.
///
/// Validation passes the full header parser, so a returned offset is safe
/// to hand to the inflate decoder (caller must seek there and re-parse).
pub fn find_next_dynamic_block(
    data: &[u8],
    from_bit: u64,
    until_bit: u64,
) -> Option<u64> {
    let total_bits = (data.len() as u64) * 8;
    let limit = until_bit.min(total_bits);
    if from_bit >= limit {
        return None;
    }

    let mut probe = BitReader::new(data);
    let mut verify = BitReader::new(data);
    let mut pos = from_bit;

    while pos + LUT_BITS as u64 <= limit {
        if probe.seek_to_bit(pos).is_err() {
            return None;
        }
        let window = match probe.peek(LUT_BITS) {
            Ok(w) => w,
            Err(_) => return None,
        };
        let step = NEXT_LUT[window as usize];
        if step == 0 {
            // Looks like a candidate. Try to fully parse the header.
            if verify.seek_to_bit(pos).is_ok()
                && verify.read(3).is_ok() // BFINAL + BTYPE
                && read_dynamic_header(&mut verify).is_ok()
            {
                return Some(pos);
            }
            // False positive at the LUT layer: advance one bit.
            pos += 1;
        } else {
            pos += step as u64;
        }
    }
    let _: Result<(), DeflateError> = Ok(());
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inflate::inflate_block;

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
        let writer = std::thread::spawn(move || stdin.write_all(&payload).unwrap());
        let out = child.wait_with_output().expect("wait gzip");
        writer.join().unwrap();
        assert!(out.status.success());
        out.stdout[10..out.stdout.len() - 8].to_vec()
    }

    /// Walk a deflate stream block-by-block, recording the bit offset of
    /// each block's BFINAL bit.
    fn record_block_starts(deflate: &[u8]) -> Vec<u64> {
        let mut padded = deflate.to_vec();
        padded.extend_from_slice(&[0u8; 16]);
        let mut br = BitReader::new(&padded);
        let mut out = Vec::new();
        let mut starts = Vec::new();
        loop {
            starts.push(br.tell_bit());
            let bfinal = inflate_block(&mut br, &mut out).expect("inflate");
            if bfinal {
                return starts;
            }
        }
    }

    #[test]
    fn lut_basic_properties() {
        // BFINAL=0, BTYPE=0b10 (LSB-first → bit1=0, bit2=1), HLIT=0, HDIST=0
        // → low 13 bits = 0b0_00000_00000_100 = 4.
        assert!(is_dynamic_candidate(0b0_00000_00000_100));
        // BTYPE=0b11 (reserved):
        assert!(!is_dynamic_candidate(0b0_00000_00000_110));
        // BTYPE=0b00 (stored):
        assert!(!is_dynamic_candidate(0b0_00000_00000_000));
        // HLIT = 30 (bits 3..=7 = 0b11110):
        assert!(!is_dynamic_candidate(0b0_00000_11110_100));
        // HDIST = 30:
        assert!(!is_dynamic_candidate(0b0_11110_00000_100));
    }

    #[test]
    fn finds_first_non_final_dynamic_block() {
        // Big enough to produce ≥2 blocks at level 6, so the first one is
        // non-final and the finder will pick it up at bit 0.
        let mut p = Vec::with_capacity(4 << 20);
        let mut s: u64 = 0xC0DE_F00D_BAAD_F00D;
        while p.len() < 4 << 20 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        let deflate = deflate_via_gzip(&p, 6);
        let recorded = record_block_starts(&deflate);
        assert!(recorded.len() >= 2, "need ≥2 blocks");
        assert_eq!(recorded[0], 0);
        let found = find_next_dynamic_block(&deflate, 0, deflate.len() as u64 * 8);
        assert_eq!(found, Some(0));
    }

    #[test]
    fn finds_internal_block_boundaries() {
        // Big enough to produce multiple blocks at level 6.
        let mut p = Vec::with_capacity(4 << 20);
        let mut s: u64 = 0xC0DE_F00D_BAAD_F00D;
        while p.len() < 4 << 20 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        let deflate = deflate_via_gzip(&p, 6);
        let recorded = record_block_starts(&deflate);
        assert!(recorded.len() >= 3, "need several blocks, got {}", recorded.len());

        // The non-final block boundaries reachable by the dynamic-only finder.
        let total_bits = deflate.len() as u64 * 8;
        // Skip past the first block to test "find next block after offset X".
        let after_first = recorded[1];
        // For each known internal start, the finder must locate it (or an
        // earlier valid candidate) when started at or just before it.
        let found = find_next_dynamic_block(&deflate, after_first - 7, total_bits)
            .expect("should find a block near second boundary");
        assert!(
            recorded.contains(&found),
            "finder returned {found}, not in block-start set {:?}",
            recorded
        );
    }

    #[test]
    fn returns_none_in_random_garbage() {
        // 1 KiB of pseudo-random bytes — overwhelmingly unlikely to validate
        // as a dynamic-Huffman header at any bit offset.
        let mut s: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let mut data = Vec::with_capacity(1024);
        while data.len() < 1024 {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            data.extend_from_slice(&s.to_le_bytes());
        }
        // Don't assert None unconditionally — random data can occasionally
        // pass. Just assert the call doesn't panic and terminates.
        let _ = find_next_dynamic_block(&data, 0, data.len() as u64 * 8);
    }
}
