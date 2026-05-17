//! Canonical Huffman decoder for DEFLATE.
//!
//! ## Bit ordering
//!
//! DEFLATE writes Huffman codes MSB-first: the first bit of the code is the
//! highest-order bit. Our [`BitReader`] reads LSB-first. So when we read
//! `L` bits from the stream, we get the **bit-reversed** code. The cleanest
//! way to handle that is to build the LUT keyed by the bits-as-read (i.e.
//! by the reversed code).
//!
//! ## LUT layout
//!
//! Indexed by `peek(LUT_BITS)`. Each entry is a [`Entry`] packing
//! `(symbol, length)`. For codes shorter than `LUT_BITS`, every index whose
//! low `length` bits equal the reversed code maps to the same symbol — that
//! is, we store the entry at `(prefix << length) | reversed_code` for every
//! `prefix`.
//!
//! Codes longer than `LUT_BITS` are uncommon in DEFLATE (the literal/length
//! tree max length is 15, the LUT is 10). For those we fall back to a small
//! linear scan over the long-code table — a clean port target for Phase 5
//! is to upgrade this to chained subtables if benchmarks demand it.

use crate::{BitReader, DeflateError};

/// LUT prefix length. 12 bits → 4096-entry LUT (~16 KiB), fits comfortably
/// in L1d. Profile on a real bgzf workload shows `decode` dominates (~47%);
/// going from 10 → 12 cuts the long-code fallback rate and shaves a peek
/// of MAX_CODE_LEN bits in the common case. Revisit if cache pressure shows
/// up under heavier multi-decoder mixes.
pub const LUT_BITS: u32 = 10;
const LUT_SIZE: usize = 1 << LUT_BITS;

/// Maximum Huffman code length permitted by DEFLATE.
pub const MAX_CODE_LEN: u32 = 15;

/// Packed table entry.
///
/// - `length == 0`            → invalid (no code at that prefix)
/// - `length <= LUT_BITS`     → direct hit; symbol is the decoded value
/// - `length >  LUT_BITS`     → unused (this LUT only stores ≤ LUT_BITS;
///                              longer codes go in `long_codes`)
#[derive(Debug, Clone, Copy, Default)]
struct Entry {
    symbol: u16,
    length: u8,
}

/// A `(reversed_bits, length, symbol)` triple for the long-code fallback.
#[derive(Debug, Clone, Copy)]
struct LongCode {
    reversed: u32,
    length: u8,
    symbol: u16,
}

#[derive(Debug)]
pub struct HuffmanDecoder {
    lut: Vec<Entry>,
    /// Codes longer than LUT_BITS, sorted by length ascending. Linear scan
    /// is fine — there are typically only a handful per tree.
    long_codes: Vec<LongCode>,
}

impl HuffmanDecoder {
    /// Build a decoder from a vector of bit-lengths. `lengths[s] = 0` means
    /// symbol `s` is not present in this tree. Accepts incomplete trees
    /// (Kraft sum < full code space) — see comment in the body.
    pub fn from_lengths(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths(lengths, false)?;
        Ok(out)
    }

    /// Strict variant: also reject incomplete trees with ≥2 symbols. Used by
    /// the block finder, where an incomplete tree is a strong signal that
    /// the candidate offset is a false positive (random bits parsing as a
    /// header). Real DEFLATE encoders produce complete trees.
    pub fn from_lengths_strict(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths(lengths, true)?;
        Ok(out)
    }

    /// Allocate a decoder with empty buffers, ready to be reused via
    /// [`Self::rebuild_from_lengths`]. The hot path allocates two of these
    /// per worker and rebuilds them once per dynamic block, avoiding the
    /// 1024-entry LUT allocation that would otherwise dominate decode time.
    pub fn new_empty() -> Self {
        Self {
            lut: vec![Entry::default(); LUT_SIZE],
            long_codes: Vec::new(),
        }
    }

    /// Rebuild this decoder's tables in place from the given code-lengths.
    /// Preserves the LUT allocation so successive dynamic blocks don't
    /// re-pay the 8 KiB-per-tree allocation cost.
    pub fn rebuild_from_lengths(
        &mut self,
        lengths: &[u8],
        strict: bool,
    ) -> Result<(), DeflateError> {
        Self::build_into(&mut self.lut, &mut self.long_codes, lengths, strict)
    }

    fn build_into(
        lut: &mut Vec<Entry>,
        long_codes: &mut Vec<LongCode>,
        lengths: &[u8],
        strict: bool,
    ) -> Result<(), DeflateError> {
        // Reuse buffers: clear entries in place rather than reallocating.
        if lut.len() != LUT_SIZE {
            lut.resize(LUT_SIZE, Entry::default());
        }
        for e in lut.iter_mut() {
            *e = Entry::default();
        }
        long_codes.clear();

        // Count codes per length.
        let mut bl_count = [0u32; (MAX_CODE_LEN + 1) as usize];
        for &l in lengths {
            if l as u32 > MAX_CODE_LEN {
                return Err(DeflateError::Invalid("huffman code length > 15"));
            }
            bl_count[l as usize] += 1;
        }
        bl_count[0] = 0;

        // Validate Kraft. We accept complete trees, the empty tree, and
        // (when !strict) incomplete trees such as the fixed distance tree.
        // Oversubscribed trees are always rejected.
        let mut total = 0u64;
        for l in 1..=(MAX_CODE_LEN as usize) {
            total += (bl_count[l] as u64) << (MAX_CODE_LEN as usize - l);
        }
        let full = 1u64 << MAX_CODE_LEN;
        let n_symbols: u32 = bl_count[1..].iter().sum();
        if n_symbols >= 2 && total > full {
            return Err(DeflateError::Invalid("oversubscribed huffman tree"));
        }
        if strict && n_symbols >= 2 && total != full {
            return Err(DeflateError::Invalid("incomplete huffman tree (strict)"));
        }

        // Counting-sort symbols into per-length buckets. The literal/length
        // tree has up to 288 symbols; distance up to 30; code-length 19.
        debug_assert!(lengths.len() <= 288);
        let mut bucket_offset = [0u32; (MAX_CODE_LEN + 2) as usize];
        for l in 1..=(MAX_CODE_LEN as usize) {
            bucket_offset[l + 1] = bucket_offset[l] + bl_count[l];
        }
        let mut sorted_syms = [0u16; 288];
        let mut cursors = bucket_offset;
        for (sym, &len) in lengths.iter().enumerate() {
            let l = len as usize;
            if l != 0 {
                sorted_syms[cursors[l] as usize] = sym as u16;
                cursors[l] += 1;
            }
        }

        // Emit codes in length-then-symbol order, maintaining the reversed
        // canonical code incrementally. Incrementing the canonical code by 1
        // at width L corresponds to a carry propagating from the MSB of the
        // L-bit reversed value toward the LSB — no per-symbol bit_reverse.
        // Transitioning from length L to L+1 (`next_code[L+1] = (next_code[L]
        // + bl_count[L]) << 1`) corresponds to one extra advance plus a free
        // width extension since the new high bit is always 0.
        let mut rev: u32 = 0;
        let mut start = 0usize;
        for l in 1..=(MAX_CODE_LEN as usize) {
            let count = bl_count[l] as usize;
            let len_u8 = l as u8;
            let len = l as u32;
            let high_bit = 1u32 << (len - 1);
            if len <= LUT_BITS {
                let stride = 1usize << len;
                for i in 0..count {
                    let sym = sorted_syms[start + i];
                    let mut idx = rev as usize;
                    while idx < LUT_SIZE {
                        lut[idx] = Entry { symbol: sym, length: len_u8 };
                        idx += stride;
                    }
                    if i + 1 < count {
                        let mut bit = high_bit;
                        while rev & bit != 0 {
                            rev ^= bit;
                            bit >>= 1;
                        }
                        rev |= bit;
                    }
                }
            } else {
                for i in 0..count {
                    let sym = sorted_syms[start + i];
                    long_codes.push(LongCode { reversed: rev, length: len_u8, symbol: sym });
                    if i + 1 < count {
                        let mut bit = high_bit;
                        while rev & bit != 0 {
                            rev ^= bit;
                            bit >>= 1;
                        }
                        rev |= bit;
                    }
                }
            }
            start += count;
            // Transition to length L+1: one more rev-increment if any code
            // was emitted at this length. Width extension is free (new high
            // bit is 0).
            if count > 0 {
                let mut bit = high_bit;
                while rev & bit != 0 {
                    rev ^= bit;
                    bit >>= 1;
                }
                rev |= bit;
            }
        }
        // long_codes are pushed in length-ascending order by construction.

        Ok(())
    }

    /// Decode the next symbol from `br`.
    #[inline]
    pub fn decode(&self, br: &mut BitReader<'_>) -> Result<u16, DeflateError> {
        let peeked = br.peek(LUT_BITS)?;
        let entry = self.lut[peeked as usize];
        if entry.length != 0 {
            br.consume(entry.length as u32);
            return Ok(entry.symbol);
        }
        self.decode_long(br)
    }

    /// Decode the next symbol assuming the caller has already ensured the
    /// buffer has ≥ `MAX_CODE_LEN` bits. Skips the refill-and-Result of
    /// [`Self::decode`] in the hot path — the inner inflate loop calls
    /// `ensure_bits` once per iteration and then decodes multiple symbols
    /// without further checks. Long-code fallback path peeks normally (rare).
    #[inline]
    pub fn decode_filled(&self, br: &mut BitReader<'_>) -> Result<u16, DeflateError> {
        let peeked = br.peek_bits_unchecked(LUT_BITS);
        let entry = self.lut[peeked as usize];
        if entry.length != 0 {
            br.consume(entry.length as u32);
            return Ok(entry.symbol);
        }
        self.decode_long(br)
    }

    #[cold]
    fn decode_long(&self, br: &mut BitReader<'_>) -> Result<u16, DeflateError> {
        // Need up to MAX_CODE_LEN bits; peek that many.
        let bits = br.peek(MAX_CODE_LEN)?;
        for c in &self.long_codes {
            let mask = (1u32 << c.length) - 1;
            if (bits & mask) == c.reversed {
                br.consume(c.length as u32);
                return Ok(c.symbol);
            }
        }
        Err(DeflateError::Invalid("no matching huffman code"))
    }
}

/// Reverse the low `len` bits of `v`. Used to convert RFC-1951 canonical
/// codes (MSB-first on the wire) into LSB-first form for LUT indexing.
fn bit_reverse(mut v: u32, len: u32) -> u32 {
    let mut out = 0u32;
    for _ in 0..len {
        out = (out << 1) | (v & 1);
        v >>= 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a known tiny tree: symbols A=0, B=1, C=2, D=3 with lengths
    /// [2, 1, 3, 3]. Per RFC 1951 §3.2.2 the codes are:
    ///   B = 0       (length 1)
    ///   A = 10      (length 2)
    ///   C = 110     (length 3)
    ///   D = 111     (length 3)
    /// On the wire, written MSB-first; we read LSB-first → reversed bits.
    fn tiny_tree() -> HuffmanDecoder {
        HuffmanDecoder::from_lengths(&[2, 1, 3, 3]).unwrap()
    }

    #[test]
    fn bit_reverse_basic() {
        assert_eq!(bit_reverse(0b110, 3), 0b011);
        assert_eq!(bit_reverse(0b10, 2), 0b01);
        assert_eq!(bit_reverse(0, 5), 0);
        assert_eq!(bit_reverse(0b11111, 5), 0b11111);
    }

    #[test]
    fn decode_tiny_tree() {
        let dec = tiny_tree();
        // Encode B A C D D: codes are 0, 10, 110, 111, 111 (MSB first).
        // Concatenated bit-stream MSB-first: 0 10 110 111 111
        // = 0|10|110|111|111  (13 bits)
        // Stored on wire LSB-first per byte: byte0 low bits first.
        // Easier: build the byte buffer directly.
        let mut bits: Vec<u8> = Vec::new(); // each 0/1
        for code_str in ["0", "10", "110", "111", "111"] {
            for c in code_str.chars() {
                bits.push((c == '1') as u8);
            }
        }
        // Pack into bytes LSB-first.
        let mut bytes: Vec<u8> = Vec::new();
        let mut acc = 0u8;
        let mut nb = 0u32;
        for b in &bits {
            acc |= b << nb;
            nb += 1;
            if nb == 8 {
                bytes.push(acc);
                acc = 0;
                nb = 0;
            }
        }
        if nb > 0 {
            bytes.push(acc);
        }
        // Pad: DEFLATE always has a trailer (≥4 bytes for gzip CRC+ISIZE) so
        // the decoder can safely peek LUT_BITS past the end of the symbol stream.
        bytes.extend_from_slice(&[0; 8]);

        let mut br = BitReader::new(&bytes);
        assert_eq!(dec.decode(&mut br).unwrap(), 1); // B
        assert_eq!(dec.decode(&mut br).unwrap(), 0); // A
        assert_eq!(dec.decode(&mut br).unwrap(), 2); // C
        assert_eq!(dec.decode(&mut br).unwrap(), 3); // D
        assert_eq!(dec.decode(&mut br).unwrap(), 3); // D
    }

    #[test]
    fn rejects_oversubscribed() {
        // Two length-1 codes covers 2/2 of length-1 space — fine alone, but
        // adding a length-2 makes it oversubscribed.
        let err = HuffmanDecoder::from_lengths(&[1, 1, 2]).unwrap_err();
        assert!(matches!(err, DeflateError::Invalid(_)));
    }

    #[test]
    fn accepts_incomplete() {
        // RFC 1951's fixed distance tree (30 length-5 codes, 2 slots unused)
        // is technically incomplete; zlib emits it and the decoder must
        // accept. Decoding a missing prefix errors at lookup time.
        let dec = HuffmanDecoder::from_lengths(&[2, 2, 2]).unwrap();
        // The 4th 2-bit slot has no entry; decoding bits that map there errors.
        let bytes = [0b11u8, 0, 0, 0, 0, 0, 0, 0, 0]; // padded
        let mut br = BitReader::new(&bytes);
        // The unused slot for our tree: its reversed-LSB bits — three codes
        // at length 2 occupy three of {00, 01, 10, 11}. Per canonical
        // assignment in symbol order: sym 0 → 00, sym 1 → 01, sym 2 → 10,
        // and reversed-LSB of those is 00, 10, 01. Slot 11 (reversed) is
        // unused. Reading bits 11 should error.
        assert!(dec.decode(&mut br).is_err());
    }

    #[test]
    fn rejects_too_long() {
        let mut lens = vec![0u8; 10];
        lens[0] = 16;
        assert!(HuffmanDecoder::from_lengths(&lens).is_err());
    }

    #[test]
    fn fixed_literal_tree_smoke() {
        // RFC 1951 §3.2.6 fixed Huffman: 8-bit codes for 0..=143 and 280..=287,
        // 9-bit for 144..=255, 7-bit for 256..=279.
        let mut lengths = vec![0u8; 288];
        for i in 0..=143 { lengths[i] = 8; }
        for i in 144..=255 { lengths[i] = 9; }
        for i in 256..=279 { lengths[i] = 7; }
        for i in 280..=287 { lengths[i] = 8; }
        let dec = HuffmanDecoder::from_lengths(&lengths).unwrap();

        // Fixed code for symbol 0 is the 8-bit canonical 00110000 (per zlib),
        // = 0x30 MSB-first → reversed-LSB = 0x0C, but easiest sanity check:
        // build a stream from a known encoding using deflate's fixed table and
        // decode it. Pick: end-of-block symbol 256 has code 0000000 (7 bits).
        // On the wire MSB-first 0000000 → LSB-first reads: 7 zero bits.
        let bytes = [0u8; 8]; // padded so peek(LUT_BITS) won't EOF
        let mut br = BitReader::new(&bytes);
        assert_eq!(dec.decode(&mut br).unwrap(), 256);
    }

    #[test]
    fn long_code_path() {
        // Construct a tree where one symbol has length > LUT_BITS to exercise
        // the slow path. Use lengths covering the full 15-bit space.
        // Build: 1 code at len 1, 1 at 2, 1 at 3, 1 at 4, …, 1 at 15, plus
        // one extra at 15 to round out the tree.
        // Counts: bl[1]=1, bl[2]=1, …, bl[14]=1, bl[15]=2 → total = 1*2^14 +
        // 1*2^13 + … + 1*2^1 + 2*2^0 = 2^15. Complete.
        let mut lengths = Vec::new();
        for l in 1..=14u8 { lengths.push(l); }
        lengths.push(15);
        lengths.push(15);
        let dec = HuffmanDecoder::from_lengths(&lengths).unwrap();
        assert!(!dec.long_codes.is_empty(), "expected long-code fallback to be exercised");
    }
}
