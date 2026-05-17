//! LSB-first bit reader over `&[u8]`, tuned for DEFLATE.
//!
//! DEFLATE packs data elements LSB-first within each byte; multi-byte values
//! are little-endian. Huffman codes are technically packed MSB-first as a
//! bit sequence, but the convention is to read them LSB-first and have the
//! decoder consume bits in code order — so a single LSB-first reader is all
//! we need.
//!
//! The hot path keeps an internal 64-bit buffer with `bits_in_buffer` valid
//! low-order bits. Refills happen 8 bytes at a time from the underlying
//! slice with an unaligned little-endian load.
//!
//! ## Why slice-only (no `Read`)
//!
//! For Phase 1 the gzip-framing layer buffers / memory-maps the input and
//! hands us a slice. That keeps the bit-level hot path branch-free. The
//! parallel pipeline (phase 4) will hand each worker its own slice anyway.

use crate::DeflateError;

/// Maximum bits any single `read` / `peek` call may request.
pub const MAX_READ_BITS: u32 = 32;

/// The "fast" refill path needs at least this many readable bytes ahead so
/// it can do an unaligned 8-byte load.
const REFILL_FAST_AHEAD: usize = 8;

#[derive(Debug)]
pub struct BitReader<'a> {
    /// Backing input. Never reborrowed.
    input: &'a [u8],
    /// Byte offset of the *next* unbuffered byte in `input`.
    byte_pos: usize,
    /// Up to 64 buffered bits, low-order bits valid.
    buf: u64,
    /// Number of valid bits in `buf` (0..=64).
    bits: u32,
    /// Sticky EOF latch: once we've returned `UnexpectedEof`, stay there.
    exhausted: bool,
}

impl<'a> BitReader<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            byte_pos: 0,
            buf: 0,
            bits: 0,
            exhausted: false,
        }
    }

    /// Current absolute bit position (bits consumed since construction).
    #[inline]
    pub fn tell_bit(&self) -> u64 {
        (self.byte_pos as u64) * 8 - self.bits as u64
    }

    /// Bits still available in input + buffer.
    #[inline]
    pub fn bits_remaining(&self) -> u64 {
        ((self.input.len() - self.byte_pos) as u64) * 8 + self.bits as u64
    }

    /// Discard buffered bits up to the next byte boundary. Used for stored
    /// blocks: after the 3-bit block header, DEFLATE byte-aligns then reads
    /// 4 bytes of LEN/NLEN.
    #[inline]
    pub fn byte_align(&mut self) {
        let drop = self.bits & 7;
        self.buf >>= drop;
        self.bits -= drop;
    }

    /// Ensure at least `n` bits are buffered. Public for hot-path callers
    /// that want to amortize one refill across many small peeks/consumes.
    /// After a successful call, [`Self::peek_bits_unchecked`] /
    /// [`Self::consume`] are safe for any `m ≤ n`.
    #[inline]
    pub fn ensure_bits(&mut self, n: u32) -> Result<(), DeflateError> {
        self.refill(n)
    }

    /// Peek the low `n` bits of the buffer without checking it has enough.
    /// **Caller must have ensured the buffer holds ≥ `n` bits.** Used by the
    /// inflate inner loop after a single `ensure_bits` per iteration.
    #[inline]
    pub fn peek_bits_unchecked(&self, n: u32) -> u32 {
        debug_assert!(self.bits >= n, "peek_bits_unchecked: buffer underfilled");
        let mask: u64 = (1u64 << n) - 1;
        (self.buf & mask) as u32
    }

    /// Ensure `bits_in_buffer >= n`. `n <= 56` is required — the fast-path
    /// load needs ≤8 bits of headroom in the 64-bit buffer to avoid an
    /// undefined shift when topping up. Callers that want to consume more
    /// than `MAX_READ_BITS` in one go must do so via `peek_bits_unchecked` +
    /// `consume`, not `read`/`peek`.
    #[inline]
    fn refill(&mut self, n: u32) -> Result<(), DeflateError> {
        debug_assert!(n <= 56);
        if self.bits >= n {
            return Ok(());
        }
        // Fast path: ≥8 bytes ahead → one unaligned LE load fills 64 bits.
        if self.byte_pos + REFILL_FAST_AHEAD <= self.input.len() && self.bits <= 56 {
            let chunk = u64::from_le_bytes(
                // SAFETY-equivalent: bounds checked above. Use a copy.
                self.input[self.byte_pos..self.byte_pos + 8]
                    .try_into()
                    .unwrap(),
            );
            self.buf |= chunk << self.bits;
            let added = 64 - self.bits;
            self.byte_pos += (added / 8) as usize;
            self.bits += (added / 8) * 8;
            return Ok(());
        }
        // Slow path: byte-by-byte until we either have enough or hit EOF.
        while self.bits < n {
            if self.byte_pos >= self.input.len() {
                self.exhausted = true;
                return Err(DeflateError::UnexpectedEof);
            }
            self.buf |= (self.input[self.byte_pos] as u64) << self.bits;
            self.byte_pos += 1;
            self.bits += 8;
        }
        Ok(())
    }

    /// Read `n` bits (0 ≤ n ≤ 32) LSB-first as the low bits of the return.
    #[inline]
    pub fn read(&mut self, n: u32) -> Result<u32, DeflateError> {
        if n == 0 {
            return Ok(0);
        }
        self.refill(n)?;
        let mask: u64 = (1u64 << n) - 1;
        let v = (self.buf & mask) as u32;
        self.buf >>= n;
        self.bits -= n;
        Ok(v)
    }

    /// Peek the next `n` bits without consuming. Caller must `consume(n)`
    /// once the actual length is known (Huffman decoder pattern).
    /// Returns `None` if input is exhausted before we can guarantee `n` bits.
    #[inline]
    pub fn peek(&mut self, n: u32) -> Result<u32, DeflateError> {
        debug_assert!(n <= MAX_READ_BITS);
        self.refill(n)?;
        let mask: u64 = (1u64 << n) - 1;
        Ok((self.buf & mask) as u32)
    }

    /// Consume `n` previously-peeked bits. `n` must be ≤ what was peeked.
    #[inline]
    pub fn consume(&mut self, n: u32) {
        debug_assert!(self.bits >= n);
        self.buf >>= n;
        self.bits -= n;
    }

    /// True if no more bits are readable.
    #[inline]
    pub fn at_eof(&self) -> bool {
        self.exhausted || (self.bits == 0 && self.byte_pos >= self.input.len())
    }

    /// Reposition to absolute bit offset `pos`. Drops the buffer.
    /// Required for the block-finder, which probes many candidate offsets.
    pub fn seek_to_bit(&mut self, pos: u64) -> Result<(), DeflateError> {
        let total_bits = (self.input.len() as u64) * 8;
        if pos > total_bits {
            self.exhausted = true;
            return Err(DeflateError::UnexpectedEof);
        }
        let byte = (pos / 8) as usize;
        let sub = (pos & 7) as u32;
        self.byte_pos = byte;
        self.buf = 0;
        self.bits = 0;
        self.exhausted = false;
        if sub != 0 {
            // Discard `sub` bits at the new byte boundary.
            self.refill(sub)?;
            self.buf >>= sub;
            self.bits -= sub;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let mut br = BitReader::new(&[]);
        assert!(br.at_eof());
        assert_eq!(br.tell_bit(), 0);
        assert!(matches!(br.read(1), Err(DeflateError::UnexpectedEof)));
    }

    #[test]
    fn lsb_order_single_byte() {
        // 0b10110100 → LSB first reads: 0, 0, 1, 0, 1, 1, 0, 1
        let mut br = BitReader::new(&[0b1011_0100]);
        for &expected in &[0, 0, 1, 0, 1, 1, 0, 1] {
            assert_eq!(br.read(1).unwrap(), expected);
        }
        assert!(br.read(1).is_err());
    }

    #[test]
    fn read_zero() {
        let mut br = BitReader::new(&[0xFF]);
        assert_eq!(br.read(0).unwrap(), 0);
        assert_eq!(br.tell_bit(), 0);
    }

    #[test]
    fn multibyte_le() {
        // 0xCAFE little-endian = bytes [0xFE, 0xCA]. Read as 16 bits → 0xCAFE.
        let mut br = BitReader::new(&[0xFE, 0xCA]);
        assert_eq!(br.read(16).unwrap(), 0xCAFE);
        assert!(br.at_eof());
    }

    #[test]
    fn read_32_bits() {
        let mut br = BitReader::new(&[0x78, 0x56, 0x34, 0x12]);
        assert_eq!(br.read(32).unwrap(), 0x1234_5678);
    }

    #[test]
    fn split_reads() {
        // Read 3 + 5 + 8 + 8 from [0b10110100, 0xFE, 0xCA] = 0xCAFE + 0xB4
        let mut br = BitReader::new(&[0b1011_0100, 0xFE, 0xCA]);
        assert_eq!(br.read(3).unwrap(), 0b100);
        assert_eq!(br.read(5).unwrap(), 0b10110);
        assert_eq!(br.read(8).unwrap(), 0xFE);
        assert_eq!(br.read(8).unwrap(), 0xCA);
    }

    #[test]
    fn peek_then_consume() {
        let mut br = BitReader::new(&[0xAB, 0xCD]);
        assert_eq!(br.peek(8).unwrap(), 0xAB);
        // peeking again returns the same.
        assert_eq!(br.peek(8).unwrap(), 0xAB);
        br.consume(4);
        // After dropping the low 4 bits of 0xCDAB we have 0xCDA; low 8 = 0xDA.
        assert_eq!(br.peek(8).unwrap(), 0xDA);
        br.consume(8);
        assert_eq!(br.peek(4).unwrap(), 0xC);
    }

    #[test]
    fn byte_align() {
        let mut br = BitReader::new(&[0xFF, 0xAA]);
        assert_eq!(br.read(3).unwrap(), 0b111);
        br.byte_align(); // drop the remaining 5 bits of the first byte
        assert_eq!(br.tell_bit(), 8);
        assert_eq!(br.read(8).unwrap(), 0xAA);
    }

    #[test]
    fn byte_align_already_aligned() {
        let mut br = BitReader::new(&[0xAA, 0xBB]);
        br.byte_align(); // no-op
        assert_eq!(br.read(8).unwrap(), 0xAA);
        br.byte_align();
        assert_eq!(br.read(8).unwrap(), 0xBB);
    }

    #[test]
    fn tell_bit_tracks_progress() {
        let mut br = BitReader::new(&[0xFF; 4]);
        assert_eq!(br.tell_bit(), 0);
        br.read(5).unwrap();
        assert_eq!(br.tell_bit(), 5);
        br.read(11).unwrap();
        assert_eq!(br.tell_bit(), 16);
        br.read(16).unwrap();
        assert_eq!(br.tell_bit(), 32);
    }

    #[test]
    fn fast_refill_path() {
        // Force the fast-load path: ≥8 bytes available, low buffer.
        let bytes: Vec<u8> = (0..32).collect();
        let mut br = BitReader::new(&bytes);
        for i in 0..32 {
            assert_eq!(br.read(8).unwrap(), i as u32);
        }
        assert!(br.at_eof());
    }

    #[test]
    fn slow_refill_path() {
        // Fewer than 8 bytes left forces the byte-by-byte refill.
        let bytes = [0x12, 0x34, 0x56];
        let mut br = BitReader::new(&bytes);
        // Read 24 bits in one go → must use slow refill at end.
        assert_eq!(br.read(24).unwrap(), 0x563412);
        assert!(br.at_eof());
    }

    #[test]
    fn eof_in_middle_of_read() {
        let mut br = BitReader::new(&[0xFF]);
        assert_eq!(br.read(8).unwrap(), 0xFF);
        assert!(matches!(br.read(1), Err(DeflateError::UnexpectedEof)));
    }
}
