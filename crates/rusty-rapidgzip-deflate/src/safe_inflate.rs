//! Safe, allocation-light DEFLATE inflater — alternative to [`crate::inflate`].
//!
//! Algorithm follows Mark Adler's reference puff.c (public domain, distributed
//! with zlib); the Rust shape draws from ieviev/mini-gzip's compact rendering.
//! Re-implemented here so that:
//!   - panics on bad input become [`DeflateError`]
//!   - it integrates with our framing layer (output appended to a caller-owned
//!     `Vec<u8>`, byte-consumption reported on success)
//!   - no `unsafe` is used, so this serves as a clean baseline we can profile
//!     and optimize against the existing zlib-rs-derived hot path.
//!
//! This is most useful in comparing with fast_inflate's output in our fuzzing.
//!
//! Use [`inflate`] for "give me the bytes" decoding, or [`inflate_into`] when
//! the caller (e.g. gzip framing) needs to know how many input bytes were
//! consumed.

use crate::DeflateError;

const MAXBITS: usize = 15;
const MAXLCODES: usize = 286;
const MAXDCODES: usize = 30;
const FIXLCODES: usize = 288;
const MAXDIST: usize = 32 * 1024;

const LENS: [u16; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LEXT: [u16; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DISTS: [u16; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
const DEXT: [u16; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

struct State<'a> {
    /// LSB-first bit buffer. Holds up to 63 bits at any time.
    bit_buffer: u64,
    bit_count: u32,
    input: &'a [u8],
    pos: usize,
}

/// Bit-width of the direct-lookup Huffman table. 9 covers the vast majority
/// of literal/length codes (typical 4–9 bits) and every distance code on
/// well-compressed inputs. Increasing this past 10 starts to hurt build
/// cost more than it saves on decode.
const LUT_BITS: u32 = 9;
const LUT_SIZE: usize = 1 << LUT_BITS;

/// Packed Huffman LUT entry. Low 8 bits = code length (0 = "long, fall back
/// to canonical walk"); next 16 bits = symbol value.
type LutEntry = u32;

#[inline(always)]
fn lut_pack(sym: u16, len: u8) -> LutEntry {
    ((sym as u32) << 8) | (len as u32)
}

#[inline(always)]
fn lut_len(e: LutEntry) -> u32 {
    e & 0xff
}

#[inline(always)]
fn lut_sym(e: LutEntry) -> u32 {
    e >> 8
}

struct Huffman<'a> {
    count: &'a mut [i16],
    symbol: &'a mut [i16],
    lut: &'a mut [LutEntry; LUT_SIZE],
}

/// Reverse the low `bits` bits of `code`. Used to translate canonical
/// (MSB-first) Huffman codes into the LSB-first index used by the LUT.
#[inline]
fn reverse_bits(code: u32, bits: u32) -> u32 {
    let mut x = code;
    // Standard bit-reverse for up to 16 bits via byte/nibble swaps.
    x = ((x & 0x5555) << 1) | ((x & 0xAAAA) >> 1);
    x = ((x & 0x3333) << 2) | ((x & 0xCCCC) >> 2);
    x = ((x & 0x0F0F) << 4) | ((x & 0xF0F0) >> 4);
    x = (x << 8) | (x >> 8);
    x >> (16 - bits)
}

impl<'a> State<'a> {
    /// Ensure the buffer holds at least `need` bits (`need` ≤ 56). Errors
    /// if the input runs dry before reaching `need`. Refills 8 bytes at a
    /// time when there's room; falls back to byte-at-a-time near EOF.
    #[inline(always)]
    fn refill(&mut self, need: u32) -> Result<(), DeflateError> {
        if self.bit_count >= need {
            return Ok(());
        }
        if self.pos + 8 <= self.input.len() {
            let chunk = u64::from_le_bytes(self.input[self.pos..self.pos + 8].try_into().unwrap());
            self.bit_buffer |= chunk << self.bit_count;
            let added = 64 - self.bit_count;
            self.pos += (added / 8) as usize;
            self.bit_count += (added / 8) * 8;
            return Ok(());
        }
        while self.bit_count < need {
            if self.pos >= self.input.len() {
                return Err(DeflateError::UnexpectedEof);
            }
            self.bit_buffer |= (self.input[self.pos] as u64) << self.bit_count;
            self.pos += 1;
            self.bit_count += 8;
        }
        Ok(())
    }

    /// Refill as many bytes as fit (up to 56 bits), but never error on EOF.
    /// Use this in tight loops where the decoder must walk bit-by-bit and
    /// the actual minimum-bits requirement isn't known up front.
    #[inline(always)]
    fn refill_best_effort(&mut self) {
        if self.bit_count >= 56 {
            return;
        }
        if self.pos + 8 <= self.input.len() {
            let chunk = u64::from_le_bytes(self.input[self.pos..self.pos + 8].try_into().unwrap());
            self.bit_buffer |= chunk << self.bit_count;
            let added = 64 - self.bit_count;
            self.pos += (added / 8) as usize;
            self.bit_count += (added / 8) * 8;
            return;
        }
        while self.bit_count + 8 <= 64 && self.pos < self.input.len() {
            self.bit_buffer |= (self.input[self.pos] as u64) << self.bit_count;
            self.pos += 1;
            self.bit_count += 8;
        }
    }

    /// Pull `need` bits LSB-first (DEFLATE order). `need` must be ≤ 56.
    #[inline(always)]
    fn bits(&mut self, need: u32) -> Result<u32, DeflateError> {
        self.refill(need)?;
        let val = (self.bit_buffer as u32) & ((1u32 << need) - 1);
        self.bit_buffer >>= need;
        self.bit_count -= need;
        Ok(val)
    }

    /// Drop any buffered bits — used at stored-block boundaries which are
    /// byte-aligned. Also rewinds `pos` so the stored-block byte reader sees
    /// the bytes that were sitting in the bit buffer.
    #[inline]
    fn drop_to_byte_boundary(&mut self) {
        let buffered_bytes = (self.bit_count / 8) as usize;
        self.pos -= buffered_bytes;
        self.bit_buffer = 0;
        self.bit_count = 0;
    }
}

/// Copy `length` bytes from `out[out.len() - distance ..]` to the end of
/// `out`, allowing overlap (RLE-style). Pure-safe — uses slice methods only.
#[inline]
fn copy_back(out: &mut Vec<u8>, distance: usize, length: usize) {
    out.reserve(length);
    let cur = out.len();
    let start = cur - distance;
    if distance >= length {
        // Non-overlapping: copy_within is a memcpy under the hood once the
        // safety bounds are checked at slice level.
        out.extend_from_within(start..start + length);
    } else if distance == 1 {
        // RLE on a single byte.
        let b = out[start];
        out.resize(cur + length, b);
    } else {
        // Short pattern (distance < length). Byte-wise is uncommon and tiny.
        for i in 0..length {
            let b = out[start + i];
            out.push(b);
        }
    }
}

/// Canonical-Huffman decode. Walks one bit at a time against the
/// counts-per-length table; on match returns the symbol index.
///
/// Refills the bit buffer to ≥15 bits up front so the inner loop is
/// branchless on bit availability.
/// LUT-first Huffman decode. The hot path takes just a load + shift; the
/// `codes` loop is responsible for keeping the bit buffer topped up so the
/// LUT hit doesn't have to refill.
#[inline(always)]
fn decode(s: &mut State, h: &Huffman) -> Result<i32, DeflateError> {
    if s.bit_count >= LUT_BITS {
        let idx = (s.bit_buffer as u32) & ((1u32 << LUT_BITS) - 1);
        let entry = h.lut[idx as usize];
        let len = lut_len(entry);
        if len != 0 {
            s.bit_buffer >>= len;
            s.bit_count -= len;
            return Ok(lut_sym(entry) as i32);
        }
        // Long code (>LUT_BITS). May need more bits than the buffer holds.
        if s.bit_count < MAXBITS as u32 {
            s.refill_best_effort();
        }
        return decode_short(s, h);
    }
    // Cold: refill (may EOF), then fall back to canonical walk.
    s.refill_best_effort();
    decode_short(s, h)
}

/// Walks the canonical-Huffman tables bit-by-bit. Used for codes longer
/// than the LUT covers and for end-of-stream where the buffer can't be
/// fully refilled.
#[inline]
#[expect(
    clippy::explicit_counter_loop,
    reason = "next_idx walks the canonical-code index table in lockstep with the bit-length loop (RFC 1951 §3.2.2 / C++ reference); a manual counter is clearer than enumerate here"
)]
fn decode_short(s: &mut State, h: &Huffman) -> Result<i32, DeflateError> {
    let mut bitbuf = s.bit_buffer;
    let mut available = s.bit_count;
    let mut code: i32 = 0;
    let mut first: i32 = 0;
    let mut index: i32 = 0;
    let mut next_idx = 1usize;
    for len in 1..=MAXBITS as u32 {
        if available == 0 {
            return Err(DeflateError::UnexpectedEof);
        }
        code |= (bitbuf & 1) as i32;
        bitbuf >>= 1;
        available -= 1;
        let count = h.count[next_idx] as i32;
        next_idx += 1;
        if code < first + count {
            s.bit_buffer = bitbuf;
            s.bit_count -= len;
            let sym_idx = (index + code - first) as usize;
            return Ok(h.symbol[sym_idx] as i32);
        }
        index += count;
        first = (first + count) << 1;
        code <<= 1;
    }
    Err(DeflateError::Invalid("invalid huffman code"))
}

/// Build a canonical-Huffman table from per-symbol code lengths.
/// Returns `Ok(())` on a complete or empty table. An incomplete table is OK
/// only for the distance table with a single code (RFC 1951 §3.2.7).
#[expect(
    clippy::needless_range_loop,
    reason = "canonical Huffman construction (RFC 1951 §3.2.2); the symbol/bit-length indices mirror the spec and the C++ reference"
)]
fn construct(h: &mut Huffman, length: &[u16], n: usize) -> Result<i32, DeflateError> {
    h.count[..=MAXBITS].fill(0);
    h.lut.fill(0);
    for i in 0..n {
        let l = length[i] as usize;
        if l > MAXBITS {
            return Err(DeflateError::Invalid("code length exceeds 15"));
        }
        h.count[l] += 1;
    }
    if h.count[0] as usize == n {
        return Ok(0);
    }
    let mut left: i32 = 1;
    for len in 1..=MAXBITS {
        left = (left << 1) - h.count[len] as i32;
        if left < 0 {
            return Err(DeflateError::Invalid("over-subscribed huffman code"));
        }
    }
    let mut offs = [0i16; MAXBITS + 1];
    for len in 1..MAXBITS {
        offs[len + 1] = offs[len] + h.count[len];
    }
    for sym in 0..n {
        if length[sym] != 0 {
            let l = length[sym] as usize;
            h.symbol[offs[l] as usize] = sym as i16;
            offs[l] += 1;
        }
    }

    // Build the LUT. Walk symbols in canonical-code order (length, then
    // index-within-length); the canonical code increments by 1 per symbol
    // and shifts left when length increases.
    let mut next_code = [0u32; MAXBITS + 2];
    let mut code: u32 = 0;
    for bits in 1..=MAXBITS {
        code = (code + h.count[bits - 1] as u32) << 1;
        next_code[bits] = code;
    }
    for sym in 0..n {
        let len = length[sym] as u32;
        if len == 0 || len > LUT_BITS {
            continue;
        }
        let c = next_code[len as usize];
        next_code[len as usize] += 1;
        // Bit-reverse to get the LSB-first index. Fill every extension
        // of the low `len` bits across the full LUT_BITS-wide index space.
        let base = reverse_bits(c, len);
        let entry = lut_pack(sym as u16, len as u8);
        let step = 1u32 << len;
        let mut idx = base;
        while (idx as usize) < LUT_SIZE {
            h.lut[idx as usize] = entry;
            idx += step;
        }
    }
    Ok(left)
}

fn codes(s: &mut State, out: &mut Vec<u8>, lc: &Huffman, dc: &Huffman) -> Result<(), DeflateError> {
    // Worst-case symbol consumption: 15 (lit/len Huffman) + 5 (length extra)
    // + 15 (distance Huffman) + 13 (distance extra) = 48 bits. One up-front
    // refill per iteration covers the entire symbol.
    const NEEDED: u32 = 48;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;

    // Hoist hot state into locals so the compiler keeps them in registers
    // across iterations. We write back to `*s` only at exits (return / error
    // / cold helper call).
    let mut buf = s.bit_buffer;
    let mut bits = s.bit_count;
    let mut pos = s.pos;
    let input = s.input;
    let lc_lut: &[LutEntry; LUT_SIZE] = lc.lut;
    let dc_lut: &[LutEntry; LUT_SIZE] = dc.lut;

    macro_rules! sync_back {
        () => {
            s.bit_buffer = buf;
            s.bit_count = bits;
            s.pos = pos;
        };
    }

    let result = loop {
        // Inline refill — fast path is an 8-byte LE load.
        if bits < NEEDED {
            if pos + 8 <= input.len() {
                let chunk = u64::from_le_bytes(input[pos..pos + 8].try_into().unwrap());
                buf |= chunk << bits;
                let added = 64 - bits;
                pos += (added / 8) as usize;
                bits += (added / 8) * 8;
            } else {
                while bits + 8 <= 64 && pos < input.len() {
                    buf |= (input[pos] as u64) << bits;
                    pos += 1;
                    bits += 8;
                }
            }
        }

        // ---- literal/length decode ----
        let sym = if bits >= LUT_BITS {
            let entry = lc_lut[(buf & LUT_MASK) as usize];
            let l = lut_len(entry);
            if l != 0 {
                buf >>= l;
                bits -= l;
                lut_sym(entry) as i32
            } else {
                // Long code: write back state, hand off to canonical walk.
                s.bit_buffer = buf;
                s.bit_count = bits;
                s.pos = pos;
                let r = decode_short(s, lc);
                buf = s.bit_buffer;
                bits = s.bit_count;
                pos = s.pos;
                match r {
                    Ok(v) => v,
                    Err(e) => break Err(e),
                }
            }
        } else {
            s.bit_buffer = buf;
            s.bit_count = bits;
            s.pos = pos;
            let r = decode_short(s, lc);
            buf = s.bit_buffer;
            bits = s.bit_count;
            pos = s.pos;
            match r {
                Ok(v) => v,
                Err(e) => break Err(e),
            }
        };

        if sym < 256 {
            out.push(sym as u8);
            continue;
        }
        if sym == 256 {
            break Ok(());
        }
        let idx = (sym - 257) as usize;
        if idx >= LENS.len() {
            break Err(DeflateError::Invalid("length symbol out of range"));
        }
        let len_extra = LEXT[idx] as u32;
        // A truncated stream can leave fewer buffered bits than the symbol's
        // extra-bit width; refill is best-effort, so guard before consuming.
        if bits < len_extra {
            break Err(DeflateError::UnexpectedEof);
        }
        let length = LENS[idx] as usize + (buf as usize & ((1usize << len_extra) - 1));
        buf >>= len_extra;
        bits -= len_extra;

        // ---- distance decode ----
        let dsym = if bits >= LUT_BITS {
            let entry = dc_lut[(buf & LUT_MASK) as usize];
            let l = lut_len(entry);
            if l != 0 {
                buf >>= l;
                bits -= l;
                lut_sym(entry) as i32
            } else {
                s.bit_buffer = buf;
                s.bit_count = bits;
                s.pos = pos;
                let r = decode_short(s, dc);
                buf = s.bit_buffer;
                bits = s.bit_count;
                pos = s.pos;
                match r {
                    Ok(v) => v,
                    Err(e) => break Err(e),
                }
            }
        } else {
            s.bit_buffer = buf;
            s.bit_count = bits;
            s.pos = pos;
            let r = decode_short(s, dc);
            buf = s.bit_buffer;
            bits = s.bit_count;
            pos = s.pos;
            match r {
                Ok(v) => v,
                Err(e) => break Err(e),
            }
        };

        let dsym = dsym as usize;
        if dsym >= DISTS.len() {
            break Err(DeflateError::Invalid("distance symbol out of range"));
        }
        let dist_extra = DEXT[dsym] as u32;
        if bits < dist_extra {
            break Err(DeflateError::UnexpectedEof);
        }
        let dist = DISTS[dsym] as usize + (buf as usize & ((1usize << dist_extra) - 1));
        buf >>= dist_extra;
        bits -= dist_extra;
        if dist == 0 || dist > MAXDIST || dist > out.len() {
            break Err(DeflateError::Invalid("distance out of bounds"));
        }
        copy_back(out, dist, length);
    };

    sync_back!();
    result
}

fn fixed(s: &mut State, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    let mut lc_cnt = [0i16; MAXBITS + 1];
    let mut lc_sym = [0i16; FIXLCODES];
    let mut lc_lut: Box<[LutEntry; LUT_SIZE]> = Box::new([0; LUT_SIZE]);
    let mut dc_cnt = [0i16; MAXBITS + 1];
    let mut dc_sym = [0i16; MAXDCODES];
    let mut dc_lut: Box<[LutEntry; LUT_SIZE]> = Box::new([0; LUT_SIZE]);
    let mut lc = Huffman {
        count: &mut lc_cnt,
        symbol: &mut lc_sym,
        lut: &mut lc_lut,
    };
    let mut dc = Huffman {
        count: &mut dc_cnt,
        symbol: &mut dc_sym,
        lut: &mut dc_lut,
    };
    let mut lengths = [8u16; FIXLCODES];
    lengths[144..256].fill(9);
    lengths[256..280].fill(7);
    construct(&mut lc, &lengths, FIXLCODES)?;
    let mut dlen = [5u16; MAXDCODES];
    dlen.fill(5);
    construct(&mut dc, &dlen, MAXDCODES)?;
    codes(s, out, &lc, &dc)
}

fn dynamic(s: &mut State, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    let nlen = s.bits(5)? as usize + 257;
    let ndist = s.bits(5)? as usize + 1;
    let ncode = s.bits(4)? as usize + 4;
    if nlen > MAXLCODES || ndist > MAXDCODES {
        return Err(DeflateError::Invalid("HLIT/HDIST out of range"));
    }
    let mut lengths = [0u16; MAXLCODES + MAXDCODES];
    for i in 0..ncode {
        lengths[ORDER[i]] = s.bits(3)? as u16;
    }
    let mut lc_cnt = [0i16; MAXBITS + 1];
    let mut lc_sym = [0i16; MAXLCODES];
    let mut lc_lut: Box<[LutEntry; LUT_SIZE]> = Box::new([0; LUT_SIZE]);
    let mut lc = Huffman {
        count: &mut lc_cnt,
        symbol: &mut lc_sym,
        lut: &mut lc_lut,
    };
    construct(&mut lc, &lengths, 19)?;

    let mut idx = 0usize;
    let total = nlen + ndist;
    while idx < total {
        let sym = decode(s, &lc)?;
        if !(0..=18).contains(&sym) {
            return Err(DeflateError::Invalid("bad code-length symbol"));
        }
        if sym < 16 {
            lengths[idx] = sym as u16;
            idx += 1;
        } else {
            let (len, rep) = match sym {
                16 => {
                    if idx == 0 {
                        return Err(DeflateError::Invalid("code 16 with no previous"));
                    }
                    (lengths[idx - 1], 3 + s.bits(2)? as usize)
                }
                17 => (0, 3 + s.bits(3)? as usize),
                _ => (0, 11 + s.bits(7)? as usize),
            };
            if idx + rep > total {
                return Err(DeflateError::Invalid("repeat overruns code lengths"));
            }
            for _ in 0..rep {
                lengths[idx] = len;
                idx += 1;
            }
        }
    }
    if lengths[256] == 0 {
        return Err(DeflateError::Invalid("EOB symbol has zero length"));
    }
    construct(&mut lc, &lengths, nlen)?;
    let mut dc_cnt = [0i16; MAXBITS + 1];
    let mut dc_sym = [0i16; MAXDCODES];
    let mut dc_lut: Box<[LutEntry; LUT_SIZE]> = Box::new([0; LUT_SIZE]);
    let mut dc = Huffman {
        count: &mut dc_cnt,
        symbol: &mut dc_sym,
        lut: &mut dc_lut,
    };
    construct(&mut dc, &lengths[nlen..], ndist)?;
    codes(s, out, &lc, &dc)
}

fn stored(s: &mut State, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    s.drop_to_byte_boundary();
    if s.pos + 4 > s.input.len() {
        return Err(DeflateError::UnexpectedEof);
    }
    let len = u16::from_le_bytes([s.input[s.pos], s.input[s.pos + 1]]);
    let nlen = u16::from_le_bytes([s.input[s.pos + 2], s.input[s.pos + 3]]);
    if len ^ nlen != 0xFFFF {
        return Err(DeflateError::Invalid("stored block: LEN/NLEN mismatch"));
    }
    s.pos += 4;
    let len = len as usize;
    if s.pos + len > s.input.len() {
        return Err(DeflateError::UnexpectedEof);
    }
    out.extend_from_slice(&s.input[s.pos..s.pos + len]);
    s.pos += len;
    Ok(())
}

/// Decode the entire DEFLATE stream in `input`, appending bytes to `out`.
/// Returns the number of input bytes consumed (rounded up to the next byte
/// after the final block).
pub fn inflate_into(input: &[u8], out: &mut Vec<u8>) -> Result<usize, DeflateError> {
    let mut s = State {
        bit_buffer: 0,
        bit_count: 0,
        input,
        pos: 0,
    };
    loop {
        let last = s.bits(1)?;
        let btype = s.bits(2)?;
        match btype {
            0 => stored(&mut s, out)?,
            1 => fixed(&mut s, out)?,
            2 => dynamic(&mut s, out)?,
            _ => return Err(DeflateError::Invalid("reserved block type")),
        }
        if last != 0 {
            break;
        }
    }
    // `pos` is the read cursor: refill pulls whole bytes into the 64-bit buffer
    // eagerly, so at end-of-stream up to 7 bytes may still sit buffered and
    // unconsumed. The deflate stream actually ends mid-`bit_buffer`; the number
    // of *consumed* input bytes (rounded up to the next byte after the final
    // block) is `pos` minus the whole bytes still buffered. `bit_count <= 8*pos`
    // always holds, so this never underflows.
    Ok(s.pos - (s.bit_count / 8) as usize)
}

/// Convenience wrapper: returns the decompressed bytes as a fresh `Vec<u8>`.
pub fn inflate(input: &[u8]) -> Result<Vec<u8>, DeflateError> {
    let mut out = Vec::new();
    inflate_into(input, &mut out)?;
    Ok(out)
}

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
            .expect("spawn gzip");
        let mut stdin = child.stdin.take().unwrap();
        let payload = payload.to_vec();
        let writer = std::thread::spawn(move || {
            stdin.write_all(&payload).unwrap();
        });
        let out = child.wait_with_output().expect("wait gzip");
        writer.join().unwrap();
        assert!(out.status.success());
        // Strip gzip header (10 bytes, -n leaves no FNAME) and 8-byte trailer.
        let body = &out.stdout[10..out.stdout.len() - 8];
        body.to_vec()
    }

    fn check_roundtrip(payload: &[u8], level: u32) {
        let body = deflate_via_gzip(payload, level);
        let got = inflate(&body).expect("inflate failed");
        assert_eq!(
            got,
            payload,
            "roundtrip mismatch ({} bytes, level {level})",
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
        let mut p = Vec::new();
        for _ in 0..1000 {
            p.extend_from_slice(b"abcdefghij");
        }
        check_roundtrip(&p, 6);
    }

    #[test]
    fn run_length_payload() {
        let p = vec![b'x'; 10000];
        check_roundtrip(&p, 6);
    }

    #[test]
    fn fixed_huffman_path() {
        check_roundtrip(b"aaaaaaaaaa", 9);
    }

    #[test]
    fn stored_block_path() {
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

    #[test]
    fn truncated_input_errors() {
        let body = deflate_via_gzip(b"hello, world\n", 6);
        let err = inflate(&body[..body.len() / 2]).unwrap_err();
        assert!(matches!(
            err,
            DeflateError::UnexpectedEof | DeflateError::Invalid(_)
        ));
    }
}
