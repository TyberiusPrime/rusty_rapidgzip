//! Canonical Huffman decoder for DEFLATE, libdeflate-style packed entries.
//!
//! ## Bit ordering
//!
//! DEFLATE writes Huffman codes MSB-first; our [`BitReader`] reads LSB-first.
//! So when we read `L` bits from the stream we get the **bit-reversed** code,
//! and we key the LUT by the reversed code.
//!
//! ## Packed entry layout (`u32`)
//!
//! Each LUT slot is a single `u32`. The **low byte** is the codeword length
//! (i.e. how many bits the decoder must consume). That makes the post-decode
//! step a single masked `br.consume(entry & 0xff)`.
//!
//! Two encodings share the same physical layout; the caller picks which to
//! build and which extractor to use:
//!
//! **`Simple`** (used for the precode and distance trees, and by the block
//! finder's strict-verify path):
//!
//! ```text
//!   bits 31..16: symbol value
//!   bits 7..0:   codeword length
//! ```
//!
//! Extract via [`HuffmanDecoder::decode`] / [`HuffmanDecoder::decode_filled`]
//! — both return the `u16` symbol.
//!
//! **`LitLen`** (used for the literal/length tree, the hot path):
//!
//! ```text
//!   Literal (sym 0..=255):
//!     bit 31:      HUFFDEC_LITERAL (1)
//!     bits 23..16: literal byte
//!     bits 7..0:   codeword length
//!
//!   End-of-block (sym == 256):
//!     bit 15:     HUFFDEC_EXCEPTIONAL (1)
//!     bits 7..0:  codeword length
//!
//!   Length (sym 257..=285):
//!     bits 24..16: length base value (3..=258, fits in 9 bits)
//!     bits 12..8:  length-extra bit count (0..=5)
//!     bits 7..0:   codeword length
//! ```
//!
//! Extract via [`HuffmanDecoder::lookup_filled`], which returns the raw
//! packed entry. The caller dispatches on the flag bits:
//! `entry & HUFFDEC_LITERAL`, `entry & HUFFDEC_EXCEPTIONAL`, otherwise it is
//! a length code with base/extra pre-baked into the entry.
//!
//! **`Dist`** (used for the distance tree on the hot inflate paths):
//!
//! ```text
//!   Distance (sym 0..=29):
//!     bits 31..16: distance base value (1..=24577, fits in 15 bits)
//!     bits 12..8:  distance-extra bit count (0..=13)
//!     bits 7..0:   codeword length
//!
//!   Reserved (sym 30/31, never valid):
//!     bit 15:     HUFFDEC_EXCEPTIONAL (1)
//!     bits 7..0:  codeword length
//! ```
//!
//! Like `LitLen`, the base distance and extra-bit count are pre-baked into the
//! entry, so the hot match path resolves a distance from the single LUT load —
//! no dependent `DISTANCE_BASE[sym]` / `DISTANCE_EXTRA[sym]` lookups on the
//! critical path. The `Simple` encoding (which returns the bare symbol) is kept
//! for the precode and the block finder's strict-verify path.
//!
//! ## Long codes
//!
//! Codes longer than `LUT_BITS` go into a `long_codes` table with a small
//! linear scan. libdeflate uses a chained subtable here — a follow-on phase
//! can swap that in if profiles ever show it mattering (currently ~0.1%).

use super::tables::{DISTANCE_BASE, DISTANCE_EXTRA, LENGTH_BASE, LENGTH_EXTRA};
use super::{BitReader, DeflateError};

/// LUT prefix length. 10 bits → 1024-entry × 4 B = 4 KiB LUT. Comfortable in
/// L1d and the rebuild cost stays modest. 11 was tried and regressed because
/// the doubled rebuild cost outweighed the long-code-fallback savings.
pub const LUT_BITS: u32 = 10;
const LUT_SIZE: usize = 1 << LUT_BITS;

/// Maximum Huffman code length permitted by DEFLATE.
pub const MAX_CODE_LEN: u32 = 15;

// Packed-entry flag bits.
pub const HUFFDEC_LITERAL: u32 = 1 << 31;
pub const HUFFDEC_EXCEPTIONAL: u32 = 1 << 15;
/// Low-byte mask: codeword length to consume.
const LENGTH_MASK: u32 = 0xff;

/// 64-byte-aligned fixed-size backing for the LUT. The hot decode loop
/// reads one `u32` per iteration from a near-random index; pinning the
/// base to a cacheline keeps every entry on a single line and avoids
/// page-spanning loads when the LUT crosses a page boundary. rapidgzip
/// uses `alignas(64)` for the same reason (see
/// `HuffmanCodingShortBitsMultiCached.hpp` line 263).
#[repr(C, align(64))]
#[derive(Debug)]
struct LutStorage([u32; LUT_SIZE]);

impl LutStorage {
    fn new_zeroed() -> Box<Self> {
        // `Box::new([0u32; LUT_SIZE])` allocates on the stack first, then
        // moves; for 1024 × 4 B = 4 KiB that's fine and the optimiser
        // typically elides the move. Worth re-checking if LUT_SIZE grows.
        Box::new(Self([0u32; LUT_SIZE]))
    }
}

/// Which encoding the entries use. Affects only `build_into`; the decoder
/// methods are encoding-agnostic except for which extractor the caller picks.
#[derive(Debug, Clone, Copy)]
pub enum Encoding {
    Simple,
    LitLen,
    Dist,
}

#[derive(Debug, Clone, Copy)]
struct LongCode {
    reversed: u32,
    length: u8,
    /// Packed entry to return on hit. Same layout as the LUT entries.
    entry: u32,
}

#[derive(Debug)]
pub struct HuffmanDecoder {
    lut: Box<LutStorage>,
    long_codes: Vec<LongCode>,
}

impl HuffmanDecoder {
    /// Build a `Simple`-encoded decoder. Accepts incomplete trees.
    pub fn from_lengths(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths(lengths, false)?;
        Ok(out)
    }

    /// Strict `Simple` variant: rejects incomplete trees with ≥2 symbols.
    /// Used by the block finder to reject false-positive candidate offsets.
    pub fn from_lengths_strict(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths(lengths, true)?;
        Ok(out)
    }

    /// Build a `LitLen`-encoded decoder for the literal/length tree.
    pub fn from_lengths_litlen(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths_litlen(lengths)?;
        Ok(out)
    }

    /// Build a `Dist`-encoded decoder for the distance tree (hot inflate path).
    pub fn from_lengths_dist(lengths: &[u8]) -> Result<Self, DeflateError> {
        let mut out = Self::new_empty();
        out.rebuild_from_lengths_dist(lengths)?;
        Ok(out)
    }

    /// Empty decoder, ready to be filled in place. Each worker keeps one of
    /// these per role (literal / distance / precode) and rebuilds them once
    /// per dynamic block — see the `DecoderPool` in `speculative.rs`.
    pub fn new_empty() -> Self {
        Self {
            lut: LutStorage::new_zeroed(),
            long_codes: Vec::new(),
        }
    }

    pub fn rebuild_from_lengths(
        &mut self,
        lengths: &[u8],
        strict: bool,
    ) -> Result<(), DeflateError> {
        Self::build_into(
            &mut self.lut.0,
            &mut self.long_codes,
            lengths,
            strict,
            Encoding::Simple,
        )
    }

    pub fn rebuild_from_lengths_litlen(&mut self, lengths: &[u8]) -> Result<(), DeflateError> {
        Self::build_into(
            &mut self.lut.0,
            &mut self.long_codes,
            lengths,
            false,
            Encoding::LitLen,
        )
    }

    pub fn rebuild_from_lengths_dist(&mut self, lengths: &[u8]) -> Result<(), DeflateError> {
        Self::build_into(
            &mut self.lut.0,
            &mut self.long_codes,
            lengths,
            false,
            Encoding::Dist,
        )
    }

    #[expect(
        clippy::needless_range_loop,
        reason = "bit-length-indexed loops mirror the canonical Huffman construction (RFC 1951 §3.2.2) and the C++ reference; index access is clearer here than zipping parallel slices"
    )]
    fn build_into(
        lut: &mut [u32; LUT_SIZE],
        long_codes: &mut Vec<LongCode>,
        lengths: &[u8],
        strict: bool,
        encoding: Encoding,
    ) -> Result<(), DeflateError> {
        for e in lut.iter_mut() {
            *e = 0;
        }
        long_codes.clear();

        let mut bl_count = [0u32; (MAX_CODE_LEN + 1) as usize];
        for &l in lengths {
            if l as u32 > MAX_CODE_LEN {
                return Err(DeflateError::Invalid("huffman code length > 15"));
            }
            bl_count[l as usize] += 1;
        }
        bl_count[0] = 0;

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

        // Counting-sort symbols by length. Max alphabet is 288 (literal/length).
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

        // Emit codes in length-then-symbol order; maintain the reversed
        // canonical code incrementally (see prior comment in git history for
        // the carry-from-MSB derivation).
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
                    let entry = encode_entry(sym, len_u8, encoding);
                    let mut idx = rev as usize;
                    while idx < LUT_SIZE {
                        lut[idx] = entry;
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
                    let entry = encode_entry(sym, len_u8, encoding);
                    long_codes.push(LongCode {
                        reversed: rev,
                        length: len_u8,
                        entry,
                    });
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
            if count > 0 {
                let mut bit = high_bit;
                while rev & bit != 0 {
                    rev ^= bit;
                    bit >>= 1;
                }
                rev |= bit;
            }
        }

        Ok(())
    }

    /// Simple-encoded decode. Returns the symbol value. Refills as needed.
    #[inline]
    pub fn decode(&self, br: &mut BitReader<'_>) -> Result<u16, DeflateError> {
        let entry = self.lookup(br)?;
        Ok((entry >> 16) as u16)
    }

    /// Simple-encoded decode assuming the buffer already holds enough bits
    /// (caller did `ensure_bits` once outside the inner loop).
    #[inline]
    pub fn decode_filled(&self, br: &mut BitReader<'_>) -> Result<u16, DeflateError> {
        let entry = self.lookup_filled(br)?;
        Ok((entry >> 16) as u16)
    }

    /// Return the raw packed entry. For the litlen hot path the caller
    /// dispatches on the flag bits directly without re-extracting the symbol.
    /// Caller must have ensured the buffer holds ≥ `MAX_CODE_LEN` bits.
    #[inline(always)]
    pub fn lookup_filled(&self, br: &mut BitReader<'_>) -> Result<u32, DeflateError> {
        let peeked = br.peek_bits_unchecked(LUT_BITS);
        let entry = self.lut.0[peeked as usize];
        let length = entry & LENGTH_MASK;
        if length == 0 {
            return self.lookup_long(br);
        }
        br.consume(length);
        Ok(entry)
    }

    #[inline(always)]
    fn lookup(&self, br: &mut BitReader<'_>) -> Result<u32, DeflateError> {
        let peeked = br.peek(LUT_BITS)?;
        let entry = self.lut.0[peeked as usize];
        let length = entry & LENGTH_MASK;
        if length == 0 {
            return self.lookup_long(br);
        }
        br.consume(length);
        Ok(entry)
    }

    /// LUT as a fixed-size array reference. Hot-path callers index this with a
    /// value masked to `LUT_BITS` bits; because the length is a compile-time
    /// constant, the bounds check folds away — a bare load, no `unsafe` at the
    /// call site.
    #[inline]
    pub(crate) fn lut(&self) -> &[u32; LUT_SIZE] {
        &self.lut.0
    }

    #[cold]
    pub(crate) fn lookup_long(&self, br: &mut BitReader<'_>) -> Result<u32, DeflateError> {
        let bits = br.peek(MAX_CODE_LEN)?;
        for c in &self.long_codes {
            let mask = (1u32 << c.length) - 1;
            if (bits & mask) == c.reversed {
                br.consume(c.length as u32);
                return Ok(c.entry);
            }
        }
        Err(DeflateError::Invalid("no matching huffman code"))
    }
}

/// Pack one (symbol, code-length) into the appropriate `u32` entry.
fn encode_entry(sym: u16, length: u8, encoding: Encoding) -> u32 {
    let len = length as u32;
    match encoding {
        Encoding::Simple => ((sym as u32) << 16) | len,
        Encoding::LitLen => {
            if sym < 256 {
                HUFFDEC_LITERAL | ((sym as u32) << 16) | len
            } else if sym == 256 {
                HUFFDEC_EXCEPTIONAL | len
            } else if sym <= 285 {
                let li = (sym - 257) as usize;
                let base = LENGTH_BASE[li] as u32;
                let extra = LENGTH_EXTRA[li] as u32;
                (base << 16) | (extra << 8) | len
            } else {
                // sym 286/287 are reserved by RFC 1951. Encode as exceptional
                // with the symbol value retained in bits 31..16 so the hot
                // path can tell them apart from EOB (sym 256), which has
                // zeros there.
                HUFFDEC_EXCEPTIONAL | ((sym as u32) << 16) | len
            }
        }
        Encoding::Dist => {
            if sym <= 29 {
                let base = DISTANCE_BASE[sym as usize] as u32;
                let extra = DISTANCE_EXTRA[sym as usize] as u32;
                (base << 16) | (extra << 8) | len
            } else {
                // sym 30/31 are reserved by RFC 1951 and never appear in valid
                // data. Flag exceptional so the hot path rejects them after the
                // single LUT load (mirrors the old `dsym >= 30` guard).
                HUFFDEC_EXCEPTIONAL | len
            }
        }
    }
}

/// Reverse the low `len` bits of `v`. Used in tests; production builder uses
/// incremental reversed-code maintenance.
#[cfg(test)]
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
    #![expect(
        clippy::needless_range_loop,
        reason = "tests fill expected-code arrays by literal-value index (0..=143 etc.); index access reads as the spec table"
    )]
    use super::*;

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
        let mut bits: Vec<u8> = Vec::new();
        for code_str in ["0", "10", "110", "111", "111"] {
            for c in code_str.chars() {
                bits.push((c == '1') as u8);
            }
        }
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
        bytes.extend_from_slice(&[0; 8]);

        let mut br = BitReader::new(&bytes);
        assert_eq!(dec.decode(&mut br).unwrap(), 1);
        assert_eq!(dec.decode(&mut br).unwrap(), 0);
        assert_eq!(dec.decode(&mut br).unwrap(), 2);
        assert_eq!(dec.decode(&mut br).unwrap(), 3);
        assert_eq!(dec.decode(&mut br).unwrap(), 3);
    }

    #[test]
    fn rejects_oversubscribed() {
        let err = HuffmanDecoder::from_lengths(&[1, 1, 2]).unwrap_err();
        assert!(matches!(err, DeflateError::Invalid(_)));
    }

    #[test]
    fn accepts_incomplete() {
        let dec = HuffmanDecoder::from_lengths(&[2, 2, 2]).unwrap();
        let bytes = [0b11u8, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut br = BitReader::new(&bytes);
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
        let mut lengths = vec![0u8; 288];
        for i in 0..=143 {
            lengths[i] = 8;
        }
        for i in 144..=255 {
            lengths[i] = 9;
        }
        for i in 256..=279 {
            lengths[i] = 7;
        }
        for i in 280..=287 {
            lengths[i] = 8;
        }
        let dec = HuffmanDecoder::from_lengths(&lengths).unwrap();
        let bytes = [0u8; 8];
        let mut br = BitReader::new(&bytes);
        assert_eq!(dec.decode(&mut br).unwrap(), 256);
    }

    #[test]
    fn long_code_path() {
        let mut lengths = Vec::new();
        for l in 1..=14u8 {
            lengths.push(l);
        }
        lengths.push(15);
        lengths.push(15);
        let dec = HuffmanDecoder::from_lengths(&lengths).unwrap();
        assert!(!dec.long_codes.is_empty());
    }

    #[test]
    fn litlen_encoding_literal_byte() {
        // Fixed-Huffman litlen tree: symbol 65 ('A') has 8-bit code.
        let mut lengths = vec![0u8; 288];
        for i in 0..=143 {
            lengths[i] = 8;
        }
        for i in 144..=255 {
            lengths[i] = 9;
        }
        for i in 256..=279 {
            lengths[i] = 7;
        }
        for i in 280..=287 {
            lengths[i] = 8;
        }
        let dec = HuffmanDecoder::from_lengths_litlen(&lengths).unwrap();
        // Build a stream that decodes to literal 'A' (sym 65).
        // Fixed code for 65 (canonical MSB-first): 65+48 = 113 = 0b01110001.
        // Reversed for LSB read: 0b10001110 = 0x8E.
        let bytes = [0x8Eu8, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut br = BitReader::new(&bytes);
        br.ensure_bits(48).unwrap();
        let entry = dec.lookup_filled(&mut br).unwrap();
        assert_ne!(entry & HUFFDEC_LITERAL, 0, "expected literal flag set");
        assert_eq!((entry >> 16) as u8, 65, "literal byte at bits 23..16");
    }

    #[test]
    fn litlen_encoding_eob() {
        let mut lengths = vec![0u8; 288];
        for i in 0..=143 {
            lengths[i] = 8;
        }
        for i in 144..=255 {
            lengths[i] = 9;
        }
        for i in 256..=279 {
            lengths[i] = 7;
        }
        for i in 280..=287 {
            lengths[i] = 8;
        }
        let dec = HuffmanDecoder::from_lengths_litlen(&lengths).unwrap();
        // EOB (sym 256) has 7-bit code 0b0000000 → seven zero bits.
        let bytes = [0u8; 8];
        let mut br = BitReader::new(&bytes);
        br.ensure_bits(48).unwrap();
        let entry = dec.lookup_filled(&mut br).unwrap();
        assert_eq!(entry & HUFFDEC_LITERAL, 0);
        assert_ne!(entry & HUFFDEC_EXCEPTIONAL, 0);
    }
}
