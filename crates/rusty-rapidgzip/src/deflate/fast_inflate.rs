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

use super::huffman::{HUFFDEC_EXCEPTIONAL, HUFFDEC_LITERAL, LUT_BITS};
use super::inflate::read_dynamic_header;
use super::speculative::SpeculativeChunk;
use super::tables::{fixed_distance_lengths, fixed_literal_lengths};
use super::{BitReader, DeflateError, HuffmanDecoder};

use super::inflate::speculative::{
    propagate_match_cached, record_match_prefix, SpeculativeContext,
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

    // out_pos_offset = 0: this is a single-call decode; member-relative
    // positions start at 0 (already the default; set explicitly for clarity).
    let mut ctx = SpeculativeContext {
        out_pos_offset: 0,
        ..SpeculativeContext::default()
    };
    let end_bit;
    let hit_bfinal;
    {
        let mut final_block = false;
        loop {
            let bfinal = br.read(1)? != 0;
            let btype = br.read(2)?;
            decode_block::<true>(&mut br, &mut chunk.bytes, btype, chunk_base, Some(&mut ctx))?;
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

/// Decode one complete gzip member (or up to `end_bit_hint`) with the
/// non-speculative `u8` kernel, appending to `out`.
///
/// Every gzip member after the first in a chunk begins with a fresh, empty
/// DEFLATE window — back-references cannot cross a member boundary — so no
/// speculative markers can ever arise. Such members skip [`decode_until_u16`]'s
/// phase-1 entirely and decode at full `u8` bandwidth. Semantics match that
/// function's phase-2 loop (distances resolve within `out`, which holds the
/// member's own already-decoded history). Returns `(end_bit, hit_bfinal)`.
pub fn decode_member_u8(
    input: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<(u64, bool), DeflateError> {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Instant;
    let t = Instant::now();
    let bytes_in = out.len();
    let mut br = BitReader::new(input);
    br.seek_to_bit(start_bit)?;
    let mut final_block = false;
    loop {
        if decode_one_block(&mut br, out)? {
            final_block = true;
            break;
        }
        if br.tell_bit() >= end_bit_hint {
            break;
        }
    }
    PHASE2_NS.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
    PHASE2_BYTES.fetch_add((out.len() - bytes_in) as u64, Relaxed);
    Ok((br.tell_bit(), final_block))
}

/// Experimental libdeflate-style **preload** kernel — same contract as
/// [`decode_member_u8`], non-speculative, all back-refs resolve within `out`.
///
/// The shipped `decode_compressed` loop is latency-bound on the
/// `buf → LUT-load → shift` recurrence: the table load sits at the *top* of the
/// loop, serially after the previous iteration's shift, and the literal store /
/// branch work does not overlap it. This kernel restructures the loop the way
/// libdeflate / ISA-L do — it **preloads** the next table entry immediately
/// after consuming the current symbol, so the store + pointer-bump + branch run
/// in the shadow of the load latency, raising IPC. Literals are processed in a
/// tight preload sub-loop (the common, latency-sensitive case); matches reload
/// at the top after a refill.
pub fn decode_member_u8_preload(
    input: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    out: &mut Vec<u8>,
) -> Result<(u64, bool), DeflateError> {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Instant;
    let t = Instant::now();
    let bytes_in = out.len();
    let mut br = BitReader::new(input);
    br.seek_to_bit(start_bit)?;
    let mut final_block = false;
    loop {
        if decode_one_block_preload(&mut br, out)? {
            final_block = true;
            break;
        }
        if br.tell_bit() >= end_bit_hint {
            break;
        }
    }
    PHASE2_NS.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
    PHASE2_BYTES.fetch_add((out.len() - bytes_in) as u64, Relaxed);
    Ok((br.tell_bit(), final_block))
}

/// One non-speculative deflate block via the preload kernel. Returns `true` on
/// the final block. Drop-in for [`decode_one_block`] with the same window
/// contract (back-refs resolve within `out`).
pub fn decode_one_block_preload(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
) -> Result<bool, DeflateError> {
    let bfinal = br.read(1)? != 0;
    let btype = br.read(2)?;
    match btype {
        0 => decode_stored(br, out)?,
        1 => {
            let lit = HuffmanDecoder::from_lengths_litlen(&fixed_literal_lengths())?;
            let dist = HuffmanDecoder::from_lengths_dist(&fixed_distance_lengths())?;
            decode_compressed_preload(br, out, &lit, &dist)?;
        }
        2 => {
            let (lit, dist) = read_dynamic_header(br)?;
            decode_compressed_preload(br, out, &lit, &dist)?;
        }
        _ => return Err(DeflateError::Invalid("reserved block type")),
    }
    Ok(bfinal)
}

/// Preload-structured compressed-block decoder (non-speculative). See
/// [`decode_member_u8_preload`].
#[allow(unsafe_code)]
fn decode_compressed_preload(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
) -> Result<(), DeflateError> {
    const NEEDED: u32 = 48;
    const HEADROOM: usize = 4096;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;
    // Minimum valid bits to keep the literal run going without an inline refill:
    // enough to index the LUT (≥ LUT_BITS) and, should the next symbol be a
    // length code, consume its code (≤ LUT_BITS) + length-extra (≤5) before the
    // distance path's own refill. Below this the run tops up inline.
    const LIT_GUARD: u32 = 15;

    if out.capacity() - out.len() < HEADROOM {
        out.reserve(HEADROOM);
    }
    let mut cap = out.capacity();
    let mut out_ptr = out.as_mut_ptr();
    let mut cur = out.len();

    let mut buf = br.buf;
    let mut bits = br.bits;
    let mut byte_pos = br.byte_pos;
    let input_ptr = br.input.as_ptr();
    let input_len = br.input.len();
    let lit_lut = lit.lut();
    let dist_lut = dist.lut();

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
    macro_rules! publish_len {
        () => {
            unsafe { out.set_len(cur) }
        };
    }
    // Refill `buf` to ≥ NEEDED bits (≥56 on the fast path). EOF-safe.
    macro_rules! refill {
        () => {
            if byte_pos + 8 <= input_len {
                buf |= unsafe { load_le_u64(input_ptr, byte_pos) } << bits;
                let advance = (63u32 ^ bits) >> 3;
                byte_pos += advance as usize;
                bits |= 56;
            } else if bits < NEEDED {
                while bits < NEEDED {
                    if byte_pos >= input_len {
                        publish_len!();
                        sync_br!();
                        br.exhausted = true;
                        return Err(DeflateError::UnexpectedEof);
                    }
                    buf |= unsafe { (*input_ptr.add(byte_pos)) as u64 } << bits;
                    byte_pos += 1;
                    bits += 8;
                }
            }
        };
    }

    let result: Result<(), DeflateError> = 'outer: loop {
        // Ensure room for everything this iteration can emit: a short literal
        // run plus one max-length match plus copy overshoot.
        if cap - cur < 288 + COPY_HEADROOM {
            publish_len!();
            out.reserve(HEADROOM);
            cap = out.capacity();
            out_ptr = out.as_mut_ptr();
        }

        refill!();
        // Preloaded current entry (load, not yet consumed); reloaded here at the
        // top of every iteration.
        let mut entry = lit_lut[(buf & LUT_MASK) as usize];

        // ── Literal preload sub-loop ─────────────────────────────────────────
        // Consume + store the literal, then immediately preload the next entry
        // so the store / pointer-bump / flag-test overlap the load latency.
        // Short codes in the LUT are ≤ LUT_BITS bits, so a literal consumes
        // ≤ LUT_BITS and the next index only needs ≥ LUT_BITS valid bits — the
        // run therefore self-refills inline (and re-reserves output) only when
        // the budget runs low, instead of bailing to the top every couple of
        // literals. Exits with `entry` holding a non-literal symbol (loaded,
        // not consumed, ≥ LIT_GUARD bits valid).
        while entry & HUFFDEC_LITERAL != 0 {
            let len = entry & 0xff;
            buf >>= len;
            bits -= len;
            unsafe { *out_ptr.add(cur) = (entry >> 16) as u8 };
            cur += 1;
            if bits < LIT_GUARD {
                // Cold-ish: top up bits (and output room) without leaving the run.
                refill!();
                if cap - cur < 288 + COPY_HEADROOM {
                    publish_len!();
                    out.reserve(HEADROOM);
                    cap = out.capacity();
                    out_ptr = out.as_mut_ptr();
                }
            }
            entry = lit_lut[(buf & LUT_MASK) as usize];
        }

        // ── Long-code / EOB / reserved ───────────────────────────────────────
        let code_len = entry & 0xff;
        if code_len == 0 {
            // Cold: code longer than LUT_BITS. Resolve the whole symbol the
            // slow way, act on it, and loop.
            publish_len!();
            sync_br!();
            let e = match lit.lookup_long(br) {
                Ok(e) => e,
                Err(e) => break 'outer Err(e),
            };
            reload_from_br!();
            if e & HUFFDEC_LITERAL != 0 {
                if cur == cap {
                    out.reserve(HEADROOM);
                    cap = out.capacity();
                    out_ptr = out.as_mut_ptr();
                }
                unsafe { *out_ptr.add(cur) = (e >> 16) as u8 };
                cur += 1;
                continue 'outer;
            }
            if e & HUFFDEC_EXCEPTIONAL != 0 {
                if e >> 16 == 0 {
                    break 'outer Ok(());
                }
                break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
            }
            // Long length code: fold into the shared match path via `entry`.
            // `lookup_long` already consumed the code bits, so re-add them
            // conceptually by treating the loaded `entry`'s extra/base directly.
            let length_base = ((e >> 16) & 0x1ff) as usize;
            let len_extra = (e >> 8) & 0x1f;
            let mut length = length_base;
            if len_extra > 0 {
                refill!();
                length += (buf & ((1u64 << len_extra) - 1)) as usize;
                buf >>= len_extra;
                bits -= len_extra;
            }
            match decode_distance_and_copy(
                &mut buf,
                &mut bits,
                &mut byte_pos,
                input_ptr,
                input_len,
                dist_lut,
                dist,
                br,
                out,
                &mut out_ptr,
                &mut cap,
                &mut cur,
                length,
            ) {
                Ok(()) => continue 'outer,
                Err(e) => break 'outer Err(e),
            }
        }
        if entry & HUFFDEC_EXCEPTIONAL != 0 {
            let len = entry & 0xff;
            buf >>= len;
            bits -= len;
            publish_len!();
            sync_br!();
            if entry >> 16 == 0 {
                break 'outer Ok(()); // EOB
            }
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        }

        // ── Length code ──────────────────────────────────────────────────────
        buf >>= code_len;
        bits -= code_len;
        let length_base = ((entry >> 16) & 0x1ff) as usize;
        let len_extra = (entry >> 8) & 0x1f;
        let mut length = length_base;
        if len_extra > 0 {
            length += (buf & ((1u64 << len_extra) - 1)) as usize;
            buf >>= len_extra;
            bits -= len_extra;
        }

        // ── Distance + copy ──────────────────────────────────────────────────
        // Refill so the distance code (≤15) + extra (≤13) is always covered.
        if bits < 28 {
            refill!();
        }
        let dentry = {
            let e = dist_lut[(buf & LUT_MASK) as usize];
            let len = e & 0xff;
            if len == 0 {
                publish_len!();
                sync_br!();
                match dist.lookup_long(br) {
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
        if dentry & HUFFDEC_EXCEPTIONAL != 0 {
            publish_len!();
            sync_br!();
            break 'outer Err(DeflateError::Invalid("distance symbol out of range"));
        }
        let mut distance = (dentry >> 16) as usize;
        let dextra = (dentry >> 8) & 0x1f;
        if dextra > 0 {
            distance += (buf & ((1u64 << dextra) - 1)) as usize;
            buf >>= dextra;
            bits -= dextra;
        }
        if distance == 0 || distance > cur {
            publish_len!();
            sync_br!();
            break 'outer if distance == 0 {
                Err(DeflateError::Invalid("zero back-reference distance"))
            } else {
                Err(DeflateError::Invalid(
                    "back-reference distance out of bounds",
                ))
            };
        }
        unsafe { copy_back_raw(out_ptr, cur, distance, length) };
        cur += length;
    };

    result
}

/// Cold-path helper: decode a distance symbol + extra bits and perform the
/// back-reference copy for a `length` already decoded. Only used by the
/// long-length-code branch of [`decode_compressed_preload`]; the hot match path
/// is inlined there. Refills internally.
#[allow(clippy::too_many_arguments, unsafe_code)]
#[inline]
fn decode_distance_and_copy(
    buf: &mut u64,
    bits: &mut u32,
    byte_pos: &mut usize,
    input_ptr: *const u8,
    input_len: usize,
    dist_lut: &[u32; 1 << LUT_BITS],
    dist: &HuffmanDecoder,
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    out_ptr: &mut *mut u8,
    cap: &mut usize,
    cur: &mut usize,
    length: usize,
) -> Result<(), DeflateError> {
    const NEEDED: u32 = 48;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;
    // Refill.
    if *byte_pos + 8 <= input_len {
        *buf |= unsafe { load_le_u64(input_ptr, *byte_pos) } << *bits;
        let advance = (63u32 ^ *bits) >> 3;
        *byte_pos += advance as usize;
        *bits |= 56;
    } else {
        while *bits < NEEDED {
            if *byte_pos >= input_len {
                unsafe { out.set_len(*cur) };
                br.buf = *buf;
                br.bits = *bits;
                br.byte_pos = *byte_pos;
                br.exhausted = true;
                return Err(DeflateError::UnexpectedEof);
            }
            *buf |= unsafe { (*input_ptr.add(*byte_pos)) as u64 } << *bits;
            *byte_pos += 1;
            *bits += 8;
        }
    }
    let dentry = {
        let e = dist_lut[(*buf & LUT_MASK) as usize];
        let len = e & 0xff;
        if len == 0 {
            unsafe { out.set_len(*cur) };
            br.buf = *buf;
            br.bits = *bits;
            br.byte_pos = *byte_pos;
            let e = dist.lookup_long(br)?;
            *buf = br.buf;
            *bits = br.bits;
            *byte_pos = br.byte_pos;
            e
        } else {
            *buf >>= len;
            *bits -= len;
            e
        }
    };
    if dentry & HUFFDEC_EXCEPTIONAL != 0 {
        return Err(DeflateError::Invalid("distance symbol out of range"));
    }
    let mut distance = (dentry >> 16) as usize;
    let dextra = (dentry >> 8) & 0x1f;
    if dextra > 0 {
        distance += (*buf & ((1u64 << dextra) - 1)) as usize;
        *buf >>= dextra;
        *bits -= dextra;
    }
    if distance == 0 || distance > *cur {
        return Err(DeflateError::Invalid(
            "back-reference distance out of bounds",
        ));
    }
    if *cap - *cur < length + COPY_HEADROOM {
        unsafe { out.set_len(*cur) };
        out.reserve(length + 4096);
        *cap = out.capacity();
        *out_ptr = out.as_mut_ptr();
    }
    unsafe { copy_back_raw(*out_ptr, *cur, distance, length) };
    *cur += length;
    Ok(())
}

/// Serial inflate — drop-in for `inflate::inflate`.
///
/// Decodes the complete deflate stream from `br` into `out`. No speculative
/// markers; all back-references must resolve within `out`.
///
/// `chunk_base = 0` here: the full `out` history is the DEFLATE window.
pub fn inflate_fast(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<(), DeflateError> {
    loop {
        if decode_one_block(br, out)? {
            return Ok(());
        }
    }
}

/// Decode a single deflate block (non-speculative) appending its output to
/// `out`. Returns `true` if this was the final block (BFINAL set).
///
/// Back-references resolve within `out` via relative distances, so the caller
/// must guarantee that the last 32 KiB of valid window precede the write head
/// (`out` holds at least `min(out.len(), 32768)` bytes of real history). This
/// is the streaming counterpart to [`inflate_fast`]: a serial driver can call
/// it block-by-block and periodically drain emitted bytes off the front of
/// `out` while retaining a 32 KiB window, decoding an arbitrarily long member
/// in bounded memory with no marker machinery.
pub fn decode_one_block(br: &mut BitReader<'_>, out: &mut Vec<u8>) -> Result<bool, DeflateError> {
    let bfinal = br.read(1)? != 0;
    let btype = br.read(2)?;
    decode_block::<false>(br, out, btype, 0, None)?;
    Ok(bfinal)
}

// ── Block dispatcher ─────────────────────────────────────────────────────────

fn decode_block<const IS_SPECULATIVE: bool>(
    br: &mut BitReader<'_>,
    out: &mut Vec<u8>,
    btype: u32,
    chunk_base: usize,
    ctx: Option<&mut SpeculativeContext>,
) -> Result<(), DeflateError> {
    match btype {
        0 => decode_stored(br, out),
        1 => {
            let lit = HuffmanDecoder::from_lengths_litlen(&fixed_literal_lengths())?;
            let dist = HuffmanDecoder::from_lengths_dist(&fixed_distance_lengths())?;
            decode_compressed::<IS_SPECULATIVE>(br, out, &lit, &dist, chunk_base, ctx)
        }
        2 => {
            let (lit, dist) = read_dynamic_header(br)?;
            decode_compressed::<IS_SPECULATIVE>(br, out, &lit, &dist, chunk_base, ctx)
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

/// Copy one `N`-byte chunk `src → dst`, reading the whole chunk into a SIMD
/// register before writing (so it tolerates the over-read when `distance < N`).
///
/// We go through explicit `__m256i` / `__m128i` intrinsics rather than
/// `read_unaligned::<[u8; N]>()` because LLVM materialises the `[u8; N]` array
/// on the stack — the round-trip showed up as a 32-byte `vmovdqu %ymm,(%rsp)`
/// burning ~16% of decode cycles. The intrinsic keeps the chunk register-resident.
///
/// # Safety
///
/// - `src` readable for `N` bytes, `dst` writable for `N` bytes.
/// - `N` is one of `{16, 32}` (the only widths with a SIMD path; other widths
///   fall back to a plain unaligned `[u8; N]` copy, which has the same contract).
#[inline(always)]
#[allow(unsafe_code)]
unsafe fn copy_one_chunk<const N: usize>(src: *const u8, dst: *mut u8) {
    // SIMD intrinsics are skipped under Miri (incomplete intrinsic coverage
    // would abort the interpreter); the scalar `read_unaligned` fallback below
    // exercises the exact same pointer-bounds contract, which is what we want
    // Miri to validate.
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(miri)))]
    if N == 32 {
        use core::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256};
        let v = unsafe { _mm256_loadu_si256(src as *const __m256i) };
        unsafe { _mm256_storeu_si256(dst as *mut __m256i, v) };
        return;
    }
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    if N == 16 {
        use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_storeu_si128};
        let v = unsafe { _mm_loadu_si128(src as *const __m128i) };
        unsafe { _mm_storeu_si128(dst as *mut __m128i, v) };
        return;
    }
    let chunk: [u8; N] = unsafe { std::ptr::read_unaligned(src.cast::<[u8; N]>()) };
    unsafe { std::ptr::write_unaligned(dst.cast::<[u8; N]>(), chunk) };
}

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
    copy_one_chunk::<N>(src, dst);
    let mut s = src.add(N);
    let mut d = dst.add(N);
    while s < end {
        copy_one_chunk::<N>(s, d);
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

/// Unaligned little-endian 8-byte load from `ptr[pos..pos+8]`. This is the hot
/// refill primitive; we keep the raw load (rather than `from_le_bytes` over a
/// slice) because the bounds-provable slice form leaves a never-taken `pos + 8`
/// overflow branch in the loop. Endianness is normalised so callers get the
/// bytes in stream order on any target.
///
/// # Safety
///
/// `ptr` must be valid for reads of `pos + 8` bytes (callers guard with
/// `pos + 8 <= len`).
#[inline(always)]
#[allow(unsafe_code)]
unsafe fn load_le_u64(ptr: *const u8, pos: usize) -> u64 {
    u64::from_le(std::ptr::read_unaligned(ptr.add(pos) as *const u64))
}

// ── EXPERIMENT: dual-stream interleave (single-core MLP test) ──────────────────
// Hypothesis: the decode loop is latency-bound on a per-symbol L1 LUT load, so a
// single core has spare execution capacity. Decoding TWO independent streams with
// interleaved per-symbol steps presents the core two independent load chains that
// should overlap, ~1.5-1.8x single-core throughput if the hypothesis holds. The
// baseline and interleaved paths share the identical `step1!` body, so the only
// variable is interleaving — a clean ratio test. (`#[cfg(test)]`, bench-only.)

/// Decode exactly one litlen symbol (literal, EOB, or a full length+distance
/// match) for a stream whose bit state lives in the named locals. Refills first.
/// Cold paths (long codes, EOF) sync to `$br` and reload. Sets `$inb=false` (and
/// `$done` on a final-block EOB) when the block ends.
#[cfg(test)]
macro_rules! step1 {
    ($buf:ident,$bits:ident,$bp:ident,$cur:ident,$outp:ident,
     $litlut:expr,$distlut:expr,$lit:expr,$dist:expr,
     $inb:ident,$done:ident,$bfinal:ident,$br:expr,$inptr:ident,$inlen:ident) => {{
        const MASK: u64 = (1u64 << LUT_BITS) - 1;
        if $bp + 8 <= $inlen {
            $buf |= unsafe { load_le_u64($inptr, $bp) } << $bits;
            $bp += ((63u32 ^ $bits) >> 3) as usize;
            $bits |= 56;
        } else {
            while $bits < 48 {
                if $bp >= $inlen {
                    $done = true;
                    $inb = false;
                    break;
                }
                $buf |= unsafe { (*$inptr.add($bp)) as u64 } << $bits;
                $bp += 1;
                $bits += 8;
            }
        }
        if $inb {
            let mut entry = $litlut[($buf & MASK) as usize];
            let clen = entry & 0xff;
            if clen == 0 {
                $br.buf = $buf;
                $br.bits = $bits;
                $br.byte_pos = $bp;
                entry = $lit.lookup_long($br)?;
                $buf = $br.buf;
                $bits = $br.bits;
                $bp = $br.byte_pos;
            } else {
                $buf >>= clen;
                $bits -= clen;
            }
            if entry & HUFFDEC_LITERAL != 0 {
                unsafe { *$outp.add($cur) = (entry >> 16) as u8 };
                $cur += 1;
            } else if entry & HUFFDEC_EXCEPTIONAL != 0 {
                if entry >> 16 == 0 {
                    $inb = false;
                    if $bfinal {
                        $done = true;
                    }
                } else {
                    return Err(DeflateError::Invalid("litlen symbol out of range"));
                }
            } else {
                let mut length = ((entry >> 16) & 0x1ff) as usize;
                let lextra = (entry >> 8) & 0x1f;
                if lextra > 0 {
                    length += ($buf & ((1u64 << lextra) - 1)) as usize;
                    $buf >>= lextra;
                    $bits -= lextra;
                }
                if $bits < 33 {
                    if $bp + 8 <= $inlen {
                        $buf |= unsafe { load_le_u64($inptr, $bp) } << $bits;
                        $bp += ((63u32 ^ $bits) >> 3) as usize;
                        $bits |= 56;
                    } else {
                        while $bits < 33 && $bp < $inlen {
                            $buf |= unsafe { (*$inptr.add($bp)) as u64 } << $bits;
                            $bp += 1;
                            $bits += 8;
                        }
                    }
                }
                let mut dentry = $distlut[($buf & MASK) as usize];
                let dlen = dentry & 0xff;
                if dlen == 0 {
                    $br.buf = $buf;
                    $br.bits = $bits;
                    $br.byte_pos = $bp;
                    dentry = $dist.lookup_long($br)?;
                    $buf = $br.buf;
                    $bits = $br.bits;
                    $bp = $br.byte_pos;
                } else {
                    $buf >>= dlen;
                    $bits -= dlen;
                }
                let mut distance = (dentry >> 16) as usize;
                let dextra = (dentry >> 8) & 0x1f;
                if dextra > 0 {
                    distance += ($buf & ((1u64 << dextra) - 1)) as usize;
                    $buf >>= dextra;
                    $bits -= dextra;
                }
                unsafe { copy_back_raw($outp, $cur, distance, length) };
                $cur += length;
            }
        }
    }};
}

/// Cold per-block header parse, shared by the single and dual-stream drivers.
/// On a stored block it copies the payload directly into `out_ptr[*cur..]`.
/// Returns `(in_block, bfinal, done)`.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
#[allow(unsafe_code)]
fn step_start_block(
    br: &mut BitReader<'_>,
    buf: &mut u64,
    bits: &mut u32,
    bp: &mut usize,
    out_ptr: *mut u8,
    cur: &mut usize,
    lit: &mut HuffmanDecoder,
    dist: &mut HuffmanDecoder,
) -> Result<(bool, bool, bool), DeflateError> {
    br.buf = *buf;
    br.bits = *bits;
    br.byte_pos = *bp;
    let bfinal = br.read(1)? != 0;
    let btype = br.read(2)?;
    let mut in_block = true;
    let mut done = false;
    match btype {
        0 => {
            br.byte_align();
            let len = br.read(16)? as usize;
            let _nlen = br.read(16)?;
            for _ in 0..len {
                let b = br.read(8)? as u8;
                unsafe { *out_ptr.add(*cur) = b };
                *cur += 1;
            }
            in_block = false;
            done = bfinal;
        }
        1 => {
            lit.rebuild_from_lengths_litlen(&fixed_literal_lengths())?;
            dist.rebuild_from_lengths_dist(&fixed_distance_lengths())?;
        }
        2 => {
            let (l, d) = read_dynamic_header(br)?;
            *lit = l;
            *dist = d;
        }
        _ => return Err(DeflateError::Invalid("reserved block type")),
    }
    *buf = br.buf;
    *bits = br.bits;
    *bp = br.byte_pos;
    Ok((in_block, bfinal, done))
}

/// Baseline: decode one whole member, one symbol per loop step (no literal
/// sub-loop, refill every symbol) — the per-symbol shape the dual-stream test
/// interleaves, so the two are directly comparable.
#[cfg(test)]
#[allow(unsafe_code)]
pub fn decode_member_stepwise(input: &[u8], out: &mut Vec<u8>) -> Result<(), DeflateError> {
    let mut br = BitReader::new(input);
    let out_ptr = out.as_mut_ptr();
    let mut cur = 0usize;
    let mut buf = 0u64;
    let mut bits = 0u32;
    let mut bp = 0usize;
    let input_ptr = input.as_ptr();
    let input_len = input.len();
    let mut in_block = false;
    let mut done = false;
    let mut bfinal = false;
    let mut lit = HuffmanDecoder::new_empty();
    let mut dist = HuffmanDecoder::new_empty();
    while !done {
        if !in_block {
            let (ib, bf, dn) = step_start_block(
                &mut br, &mut buf, &mut bits, &mut bp, out_ptr, &mut cur, &mut lit, &mut dist,
            )?;
            in_block = ib;
            bfinal = bf;
            done = dn;
            continue;
        }
        let lit_lut = lit.lut();
        let dist_lut = dist.lut();
        while in_block {
            step1!(
                buf, bits, bp, cur, out_ptr, lit_lut, dist_lut, &lit, &dist, in_block, done,
                bfinal, &mut br, input_ptr, input_len
            );
        }
    }
    unsafe { out.set_len(cur) };
    Ok(())
}

/// Interleaved: decode two members at once, one symbol from each per fused step,
/// so the two independent LUT-load chains overlap on one core.
#[cfg(test)]
#[allow(unsafe_code)]
pub fn decode_two_interleaved(
    in_a: &[u8],
    out_a: &mut Vec<u8>,
    in_b: &[u8],
    out_b: &mut Vec<u8>,
) -> Result<(), DeflateError> {
    let mut bra = BitReader::new(in_a);
    let mut brb = BitReader::new(in_b);
    let pa = out_a.as_mut_ptr();
    let pb = out_b.as_mut_ptr();
    let (mut ca, mut cb) = (0usize, 0usize);
    let (mut bufa, mut bufb) = (0u64, 0u64);
    let (mut bitsa, mut bitsb) = (0u32, 0u32);
    let (mut bpa, mut bpb) = (0usize, 0usize);
    let ipa = in_a.as_ptr();
    let ila = in_a.len();
    let ipb = in_b.as_ptr();
    let ilb = in_b.len();
    let (mut iba, mut ibb) = (false, false);
    let (mut da, mut db) = (false, false);
    let (mut bfa, mut bfb) = (false, false);
    let mut lita = HuffmanDecoder::new_empty();
    let mut dista = HuffmanDecoder::new_empty();
    let mut litb = HuffmanDecoder::new_empty();
    let mut distb = HuffmanDecoder::new_empty();
    while !(da && db) {
        if !iba && !da {
            let (ib, bf, dn) = step_start_block(
                &mut bra, &mut bufa, &mut bitsa, &mut bpa, pa, &mut ca, &mut lita, &mut dista,
            )?;
            iba = ib;
            bfa = bf;
            da = dn;
        }
        if !ibb && !db {
            let (ib, bf, dn) = step_start_block(
                &mut brb, &mut bufb, &mut bitsb, &mut bpb, pb, &mut cb, &mut litb, &mut distb,
            )?;
            ibb = ib;
            bfb = bf;
            db = dn;
        }
        if iba && ibb {
            let lut_a = lita.lut();
            let dlut_a = dista.lut();
            let lut_b = litb.lut();
            let dlut_b = distb.lut();
            while iba && ibb {
                step1!(
                    bufa, bitsa, bpa, ca, pa, lut_a, dlut_a, &lita, &dista, iba, da, bfa, &mut bra,
                    ipa, ila
                );
                step1!(
                    bufb, bitsb, bpb, cb, pb, lut_b, dlut_b, &litb, &distb, ibb, db, bfb, &mut brb,
                    ipb, ilb
                );
            }
        } else if iba {
            let lut_a = lita.lut();
            let dlut_a = dista.lut();
            while iba {
                step1!(
                    bufa, bitsa, bpa, ca, pa, lut_a, dlut_a, &lita, &dista, iba, da, bfa, &mut bra,
                    ipa, ila
                );
            }
        } else if ibb {
            let lut_b = litb.lut();
            let dlut_b = distb.lut();
            while ibb {
                step1!(
                    bufb, bitsb, bpb, cb, pb, lut_b, dlut_b, &litb, &distb, ibb, db, bfb, &mut brb,
                    ipb, ilb
                );
            }
        }
    }
    unsafe { out_a.set_len(ca) };
    unsafe { out_b.set_len(cb) };
    Ok(())
}

/// 3-way interleave over THREE DISTINCT inputs (the likely production N).
#[cfg(test)]
#[allow(unsafe_code)]
#[allow(clippy::too_many_arguments)]
pub fn decode_three_interleaved(
    in_a: &[u8],
    out_a: &mut Vec<u8>,
    in_b: &[u8],
    out_b: &mut Vec<u8>,
    in_c: &[u8],
    out_c: &mut Vec<u8>,
) -> Result<(), DeflateError> {
    let (mut bra, mut brb, mut brc) = (
        BitReader::new(in_a),
        BitReader::new(in_b),
        BitReader::new(in_c),
    );
    let (pa, pb, pc) = (out_a.as_mut_ptr(), out_b.as_mut_ptr(), out_c.as_mut_ptr());
    let (ipa, ila) = (in_a.as_ptr(), in_a.len());
    let (ipb, ilb) = (in_b.as_ptr(), in_b.len());
    let (ipc, ilc) = (in_c.as_ptr(), in_c.len());
    let (mut ca, mut cb, mut cc) = (0usize, 0usize, 0usize);
    let (mut bufa, mut bufb, mut bufc) = (0u64, 0u64, 0u64);
    let (mut bitsa, mut bitsb, mut bitsc) = (0u32, 0u32, 0u32);
    let (mut bpa, mut bpb, mut bpc) = (0usize, 0usize, 0usize);
    let (mut iba, mut ibb, mut ibc) = (false, false, false);
    let (mut da, mut db, mut dc) = (false, false, false);
    let (mut bfa, mut bfb, mut bfc) = (false, false, false);
    let mut la = HuffmanDecoder::new_empty();
    let mut dista = HuffmanDecoder::new_empty();
    let mut lb = HuffmanDecoder::new_empty();
    let mut distb = HuffmanDecoder::new_empty();
    let mut lc = HuffmanDecoder::new_empty();
    let mut distc = HuffmanDecoder::new_empty();
    while !(da && db && dc) {
        if !iba && !da {
            let r = step_start_block(
                &mut bra, &mut bufa, &mut bitsa, &mut bpa, pa, &mut ca, &mut la, &mut dista,
            )?;
            (iba, bfa, da) = r;
        }
        if !ibb && !db {
            let r = step_start_block(
                &mut brb, &mut bufb, &mut bitsb, &mut bpb, pb, &mut cb, &mut lb, &mut distb,
            )?;
            (ibb, bfb, db) = r;
        }
        if !ibc && !dc {
            let r = step_start_block(
                &mut brc, &mut bufc, &mut bitsc, &mut bpc, pc, &mut cc, &mut lc, &mut distc,
            )?;
            (ibc, bfc, dc) = r;
        }
        if iba && ibb && ibc {
            let (lla, dla) = (la.lut(), dista.lut());
            let (llb, dlb) = (lb.lut(), distb.lut());
            let (llc, dlc) = (lc.lut(), distc.lut());
            while iba && ibb && ibc {
                step1!(
                    bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra, ipa,
                    ila
                );
                step1!(
                    bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb, ipb,
                    ilb
                );
                step1!(
                    bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc, ipc,
                    ilc
                );
            }
        } else {
            if iba {
                let (lla, dla) = (la.lut(), dista.lut());
                while iba {
                    step1!(
                        bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra,
                        ipa, ila
                    );
                }
            }
            if ibb {
                let (llb, dlb) = (lb.lut(), distb.lut());
                while ibb {
                    step1!(
                        bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb,
                        ipb, ilb
                    );
                }
            }
            if ibc {
                let (llc, dlc) = (lc.lut(), distc.lut());
                while ibc {
                    step1!(
                        bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc,
                        ipc, ilc
                    );
                }
            }
        }
    }
    unsafe { out_a.set_len(ca) };
    unsafe { out_b.set_len(cb) };
    unsafe { out_c.set_len(cc) };
    Ok(())
}

/// 4-way interleave over FOUR DISTINCT inputs (truly distinct working sets, the
/// realistic ship-or-not validation — vs `decode_four_interleaved` which reuses
/// one input four times and so shares the compressed footprint in L1).
#[cfg(test)]
#[allow(unsafe_code)]
pub fn decode_four_distinct(ins: [&[u8]; 4], outs: &mut [Vec<u8>; 4]) -> Result<(), DeflateError> {
    let [oa, ob, oc, od] = outs;
    let [ina, inb, inc, ind] = ins;
    let (mut bra, mut brb, mut brc, mut brd) = (
        BitReader::new(ina),
        BitReader::new(inb),
        BitReader::new(inc),
        BitReader::new(ind),
    );
    let (pa, pb, pc, pd) = (
        oa.as_mut_ptr(),
        ob.as_mut_ptr(),
        oc.as_mut_ptr(),
        od.as_mut_ptr(),
    );
    let (ipa, ila) = (ina.as_ptr(), ina.len());
    let (ipb, ilb) = (inb.as_ptr(), inb.len());
    let (ipc, ilc) = (inc.as_ptr(), inc.len());
    let (ipd, ild) = (ind.as_ptr(), ind.len());
    let (mut ca, mut cb, mut cc, mut cd) = (0usize, 0usize, 0usize, 0usize);
    let (mut bufa, mut bufb, mut bufc, mut bufd) = (0u64, 0u64, 0u64, 0u64);
    let (mut bitsa, mut bitsb, mut bitsc, mut bitsd) = (0u32, 0u32, 0u32, 0u32);
    let (mut bpa, mut bpb, mut bpc, mut bpd) = (0usize, 0usize, 0usize, 0usize);
    let (mut iba, mut ibb, mut ibc, mut ibd) = (false, false, false, false);
    let (mut da, mut db, mut dc, mut dd) = (false, false, false, false);
    let (mut bfa, mut bfb, mut bfc, mut bfd) = (false, false, false, false);
    let mut la = HuffmanDecoder::new_empty();
    let mut dista = HuffmanDecoder::new_empty();
    let mut lb = HuffmanDecoder::new_empty();
    let mut distb = HuffmanDecoder::new_empty();
    let mut lc = HuffmanDecoder::new_empty();
    let mut distc = HuffmanDecoder::new_empty();
    let mut ld = HuffmanDecoder::new_empty();
    let mut distd = HuffmanDecoder::new_empty();
    while !(da && db && dc && dd) {
        if !iba && !da {
            let r = step_start_block(
                &mut bra, &mut bufa, &mut bitsa, &mut bpa, pa, &mut ca, &mut la, &mut dista,
            )?;
            (iba, bfa, da) = r;
        }
        if !ibb && !db {
            let r = step_start_block(
                &mut brb, &mut bufb, &mut bitsb, &mut bpb, pb, &mut cb, &mut lb, &mut distb,
            )?;
            (ibb, bfb, db) = r;
        }
        if !ibc && !dc {
            let r = step_start_block(
                &mut brc, &mut bufc, &mut bitsc, &mut bpc, pc, &mut cc, &mut lc, &mut distc,
            )?;
            (ibc, bfc, dc) = r;
        }
        if !ibd && !dd {
            let r = step_start_block(
                &mut brd, &mut bufd, &mut bitsd, &mut bpd, pd, &mut cd, &mut ld, &mut distd,
            )?;
            (ibd, bfd, dd) = r;
        }
        if iba && ibb && ibc && ibd {
            let (lla, dla) = (la.lut(), dista.lut());
            let (llb, dlb) = (lb.lut(), distb.lut());
            let (llc, dlc) = (lc.lut(), distc.lut());
            let (lld, dld) = (ld.lut(), distd.lut());
            while iba && ibb && ibc && ibd {
                step1!(
                    bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra, ipa,
                    ila
                );
                step1!(
                    bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb, ipb,
                    ilb
                );
                step1!(
                    bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc, ipc,
                    ilc
                );
                step1!(
                    bufd, bitsd, bpd, cd, pd, lld, dld, &ld, &distd, ibd, dd, bfd, &mut brd, ipd,
                    ild
                );
            }
        } else {
            if iba {
                let (lla, dla) = (la.lut(), dista.lut());
                while iba {
                    step1!(
                        bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra,
                        ipa, ila
                    );
                }
            }
            if ibb {
                let (llb, dlb) = (lb.lut(), distb.lut());
                while ibb {
                    step1!(
                        bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb,
                        ipb, ilb
                    );
                }
            }
            if ibc {
                let (llc, dlc) = (lc.lut(), distc.lut());
                while ibc {
                    step1!(
                        bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc,
                        ipc, ilc
                    );
                }
            }
            if ibd {
                let (lld, dld) = (ld.lut(), distd.lut());
                while ibd {
                    step1!(
                        bufd, bitsd, bpd, cd, pd, lld, dld, &ld, &distd, ibd, dd, bfd, &mut brd,
                        ipd, ild
                    );
                }
            }
        }
    }
    unsafe { oa.set_len(ca) };
    unsafe { ob.set_len(cb) };
    unsafe { oc.set_len(cc) };
    unsafe { od.set_len(cd) };
    Ok(())
}

/// 4-way interleave, pure-locals (register-resident) to find the MLP sweet spot.
/// Same structure as the 2-way, extended to four lockstep streams. If 4-way
/// stops beating 2-way that's the register/load-port ceiling — i.e. the answer
/// to "how many chunks per worker".
#[cfg(test)]
#[allow(unsafe_code)]
pub fn decode_four_interleaved(input: &[u8], outs: &mut [Vec<u8>; 4]) -> Result<(), DeflateError> {
    let [oa, ob, oc, od] = outs;
    let (mut bra, mut brb, mut brc, mut brd) = (
        BitReader::new(input),
        BitReader::new(input),
        BitReader::new(input),
        BitReader::new(input),
    );
    let (pa, pb, pc, pd) = (
        oa.as_mut_ptr(),
        ob.as_mut_ptr(),
        oc.as_mut_ptr(),
        od.as_mut_ptr(),
    );
    let ip = input.as_ptr();
    let il = input.len();
    let (mut ca, mut cb, mut cc, mut cd) = (0usize, 0usize, 0usize, 0usize);
    let (mut bufa, mut bufb, mut bufc, mut bufd) = (0u64, 0u64, 0u64, 0u64);
    let (mut bitsa, mut bitsb, mut bitsc, mut bitsd) = (0u32, 0u32, 0u32, 0u32);
    let (mut bpa, mut bpb, mut bpc, mut bpd) = (0usize, 0usize, 0usize, 0usize);
    let (mut iba, mut ibb, mut ibc, mut ibd) = (false, false, false, false);
    let (mut da, mut db, mut dc, mut dd) = (false, false, false, false);
    let (mut bfa, mut bfb, mut bfc, mut bfd) = (false, false, false, false);
    let mut la = HuffmanDecoder::new_empty();
    let mut dista = HuffmanDecoder::new_empty();
    let mut lb = HuffmanDecoder::new_empty();
    let mut distb = HuffmanDecoder::new_empty();
    let mut lc = HuffmanDecoder::new_empty();
    let mut distc = HuffmanDecoder::new_empty();
    let mut ld = HuffmanDecoder::new_empty();
    let mut distd = HuffmanDecoder::new_empty();

    while !(da && db && dc && dd) {
        if !iba && !da {
            let r = step_start_block(
                &mut bra, &mut bufa, &mut bitsa, &mut bpa, pa, &mut ca, &mut la, &mut dista,
            )?;
            (iba, bfa, da) = r;
        }
        if !ibb && !db {
            let r = step_start_block(
                &mut brb, &mut bufb, &mut bitsb, &mut bpb, pb, &mut cb, &mut lb, &mut distb,
            )?;
            (ibb, bfb, db) = r;
        }
        if !ibc && !dc {
            let r = step_start_block(
                &mut brc, &mut bufc, &mut bitsc, &mut bpc, pc, &mut cc, &mut lc, &mut distc,
            )?;
            (ibc, bfc, dc) = r;
        }
        if !ibd && !dd {
            let r = step_start_block(
                &mut brd, &mut bufd, &mut bitsd, &mut bpd, pd, &mut cd, &mut ld, &mut distd,
            )?;
            (ibd, bfd, dd) = r;
        }
        if iba && ibb && ibc && ibd {
            let (lla, dla) = (la.lut(), dista.lut());
            let (llb, dlb) = (lb.lut(), distb.lut());
            let (llc, dlc) = (lc.lut(), distc.lut());
            let (lld, dld) = (ld.lut(), distd.lut());
            while iba && ibb && ibc && ibd {
                step1!(
                    bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra, ip, il
                );
                step1!(
                    bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb, ip, il
                );
                step1!(
                    bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc, ip, il
                );
                step1!(
                    bufd, bitsd, bpd, cd, pd, lld, dld, &ld, &distd, ibd, dd, bfd, &mut brd, ip, il
                );
            }
        } else {
            // Ragged tail: drain whichever are mid-block one at a time.
            if iba {
                let (lla, dla) = (la.lut(), dista.lut());
                while iba {
                    step1!(
                        bufa, bitsa, bpa, ca, pa, lla, dla, &la, &dista, iba, da, bfa, &mut bra,
                        ip, il
                    );
                }
            }
            if ibb {
                let (llb, dlb) = (lb.lut(), distb.lut());
                while ibb {
                    step1!(
                        bufb, bitsb, bpb, cb, pb, llb, dlb, &lb, &distb, ibb, db, bfb, &mut brb,
                        ip, il
                    );
                }
            }
            if ibc {
                let (llc, dlc) = (lc.lut(), distc.lut());
                while ibc {
                    step1!(
                        bufc, bitsc, bpc, cc, pc, llc, dlc, &lc, &distc, ibc, dc, bfc, &mut brc,
                        ip, il
                    );
                }
            }
            if ibd {
                let (lld, dld) = (ld.lut(), distd.lut());
                while ibd {
                    step1!(
                        bufd, bitsd, bpd, cd, pd, lld, dld, &ld, &distd, ibd, dd, bfd, &mut brd,
                        ip, il
                    );
                }
            }
        }
    }
    unsafe { oa.set_len(ca) };
    unsafe { ob.set_len(cb) };
    unsafe { oc.set_len(cc) };
    unsafe { od.set_len(cd) };
    Ok(())
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
    mut ctx: Option<&mut SpeculativeContext>,
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
    let lit_lut = lit.lut();
    let dist_lut = dist.lut();

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
            buf |= unsafe { load_le_u64(input_ptr, byte_pos) } << bits;
            let advance = (63u32 ^ bits) >> 3;
            byte_pos += advance as usize;
            bits |= 56;
        } else if bits < NEEDED {
            // Near-EOF: slow byte-by-byte fill until we have enough for the
            // current symbol.
            while bits < NEEDED {
                if byte_pos >= input_len {
                    publish_len!();
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
            let e = lit_lut[idx];
            let len = e & 0xff;
            if len == 0 {
                // Long-code fallback (cold).
                publish_len!();
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

        // ── Literal (with a speculative second literal) ──────────────────────
        if entry & HUFFDEC_LITERAL != 0 {
            // After the unconditional `bits |= 56` refill we hold ≥ 56 bits, and
            // the first literal consumed ≤ LUT_BITS (10), so ≥ 46 valid bits
            // remain — enough for a second in-range LUT lookup, and two ≤ 15-bit
            // codes fit in 56 bits with no mid-pair refill. Committing two
            // literals per refill on literal runs is a small consistent win; only
            // a short-code literal is taken speculatively, anything else leaves
            // `buf` untouched and is decoded normally on the next iteration.
            let e2 = lit_lut[(buf & LUT_MASK) as usize];
            let len2 = e2 & 0xff;
            if len2 != 0 && e2 & HUFFDEC_LITERAL != 0 {
                buf >>= len2;
                bits -= len2;
                if cur + 2 > cap {
                    publish_len!();
                    out.reserve(HEADROOM);
                    cap = out.capacity();
                    out_ptr = out.as_mut_ptr();
                }
                unsafe {
                    *out_ptr.add(cur) = (entry >> 16) as u8;
                    *out_ptr.add(cur + 1) = (e2 >> 16) as u8;
                }
                cur += 2;
                continue 'outer;
            }
            if cur == cap {
                publish_len!();
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
            publish_len!();
            sync_br!();
            if entry >> 16 == 0 {
                break 'outer Ok(()); // EOB
            }
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        }

        // ── Length code ──────────────────────────────────────────────────────
        let length_base = ((entry >> 16) & 0x1ff) as usize;
        let len_extra = (entry >> 8) & 0x1f;
        let mut length = length_base;
        if len_extra > 0 {
            length += (buf & ((1u64 << len_extra) - 1)) as usize;
            buf >>= len_extra;
            bits -= len_extra;
        }

        // ── Distance symbol ──────────────────────────────────────────────────
        // Dist-encoded entry: base + extra-count are pre-baked, so the match
        // path needs no dependent DISTANCE_BASE / DISTANCE_EXTRA loads (the
        // latter sat on the `buf` critical chain via `buf >>= dextra`).
        let dentry = {
            let idx = (buf & LUT_MASK) as usize;
            let e = dist_lut[idx];
            let len = e & 0xff;
            if len == 0 {
                publish_len!();
                sync_br!();
                match dist.lookup_long(br) {
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
        if dentry & HUFFDEC_EXCEPTIONAL != 0 {
            publish_len!();
            sync_br!();
            break 'outer Err(DeflateError::Invalid("distance symbol out of range"));
        }
        let mut distance = (dentry >> 16) as usize;
        let dextra = (dentry >> 8) & 0x1f;
        if dextra > 0 {
            distance += (buf & ((1u64 << dextra) - 1)) as usize;
            buf >>= dextra;
            bits -= dextra;
        }

        // ── Back-reference resolve ────────────────────────────────────────────
        if IS_SPECULATIVE {
            let ctx = ctx
                .as_mut()
                .expect("IS_SPECULATIVE == true guarantees ctx is Some (caller invariant)");
            // Speculative path: back-refs may reach into the unknown prefix.
            let emitted = cur - chunk_base;
            if distance > emitted {
                let prefix_count = (distance - emitted).min(length);

                publish_len!();
                if out.capacity() - cur < length {
                    out.reserve(length);
                    cap = out.capacity();
                    out_ptr = out.as_mut_ptr();
                }
                unsafe {
                    std::ptr::write_bytes(out_ptr.add(cur), 0, prefix_count);
                }
                record_match_prefix(ctx, emitted, distance, prefix_count);
                cur += prefix_count;

                let in_buffer_count = length - prefix_count;
                if in_buffer_count > 0 {
                    // The in-buffer portion copies from absolute position
                    // `chunk_base + 0` (member-local position 0): this is
                    // where the original match source reaches after the
                    // prefix_count unknown-prefix bytes are exhausted.
                    // `cur` is now `chunk_base + emitted + prefix_count =
                    // chunk_base + distance`, so the copy distance is
                    // exactly `distance` (not `distance - prefix_count`).
                    if cap - cur < in_buffer_count + COPY_HEADROOM {
                        publish_len!();
                        out.reserve(in_buffer_count + HEADROOM);
                        cap = out.capacity();
                        out_ptr = out.as_mut_ptr();
                    }
                    unsafe { copy_back_raw(out_ptr, cur, distance, in_buffer_count) };
                    propagate_match_cached(ctx, emitted + prefix_count, distance, in_buffer_count);
                    cur += in_buffer_count;
                    if cap - cur < HEADROOM {
                        publish_len!();
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
                publish_len!();
                sync_br!();
                break 'outer Err(DeflateError::Invalid("zero back-reference distance"));
            }
            if cap - cur < length + COPY_HEADROOM {
                publish_len!();
                out.reserve(length + HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe { copy_back_raw(out_ptr, cur, distance, length) };
            propagate_match_cached(ctx, emitted, distance, length);
            cur += length;
            if cap - cur < HEADROOM {
                publish_len!();
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
        } else {
            // Serial path: all back-refs must be within the output so far.
            // distance > cur is always false in valid streams (after the first
            // 32 KB), so all speculative code is dead-eliminated.
            if distance == 0 || distance > cur {
                publish_len!();
                sync_br!();
                break 'outer if distance == 0 {
                    Err(DeflateError::Invalid("zero back-reference distance"))
                } else {
                    Err(DeflateError::Invalid(
                        "back-reference distance out of bounds",
                    ))
                };
            }
            if cap - cur < length + COPY_HEADROOM {
                publish_len!();
                out.reserve(length + HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
            unsafe { copy_back_raw(out_ptr, cur, distance, length) };
            cur += length;
            if cap - cur < HEADROOM {
                publish_len!();
                out.reserve(HEADROOM);
                cap = out.capacity();
                out_ptr = out.as_mut_ptr();
            }
        }
    };

    result
}

// ── Dense u16 marker-window speculative path ─────────────────────────────────
//
// The side-table speculative path above (`decode_compressed::<true>`) calls
// into `propagate_match_cached` on every in-buffer back-reference to copy
// marker-ness forward. On FASTQ, marker-ness saturates the output, so that
// turned into a binary search per back-reference and dominated runtime.
//
// This path mirrors rapidgzip's `m_window16`: cells carry their resolution
// state inline. A cell with the high bit (`SpeculativeChunk::MARKER16`) set is
// an unresolved prefix-referencing byte whose low 15 bits hold its
// `prefix_offset`; otherwise it is a resolved literal in the low 8 bits.
// Back-reference copies move whole `u16` cells, so marker-ness propagates for
// *free* as part of the copy — no per-back-ref bookkeeping.
//
// Crucially the window is **bounded**: a fixed `U16_BUFCAP`-cell buffer that
// retains the last `U16_WINDOW` (= max DEFLATE distance) cells of history and
// flushes everything older into the `(bytes, markers)` contract as it fills.
// That keeps the per-worker footprint ~512 KiB (L2-resident) instead of a
// full-chunk buffer that, at 32 SMT threads, faulted ~900 MiB concurrently and
// blew up sys time. The flush is amortized: emit `U16_FLUSH_AT - U16_WINDOW`
// cells, then slide the retained window down with one `copy_within`.

/// High bit marks a prefix-referencing cell; low 15 bits are the prefix offset.
const MARKER16: u16 = SpeculativeChunk::MARKER16;
/// Cells of slop `copy_back_u16` may overwrite past `dst + length`.
const COPY_HEADROOM_U16: usize = 16;
/// History retained for back-references (max DEFLATE distance, in cells).
const U16_WINDOW: usize = 32 * 1024;
/// Flush once the write cursor reaches here; emits `_AT - _WINDOW` cells.
const U16_FLUSH_AT: usize = 256 * 1024;
/// Hand off the speculative `u16` window to the byte-wide kernel once this many
/// cells have been produced (at a block boundary). A prefix marker sits at
/// output position `< U16_WINDOW`, and a back-reference (distance ≤ `U16_WINDOW`)
/// can only *read* one when its destination is `< 2·U16_WINDOW`. So beyond this
/// point every byte is provably marker-free and decodes in `u8` — confining the
/// 2×-bandwidth `u16` path to ≤ 64 KiB of each multi-MiB chunk.
const U16_HANDOFF_AT: usize = 2 * U16_WINDOW;
/// Fixed buffer capacity. Headroom past `_FLUSH_AT` covers one max-length
/// (258-cell) copy plus `copy_back_u16`'s chunked overshoot.
const U16_BUFCAP: usize = U16_FLUSH_AT + 258 + COPY_HEADROOM_U16 + 8;

/// Speculative decode, lowering emitted cells into `chunk` incrementally.
///
/// Drop-in for [`decode_until`] on the parallel fast path. Two phases:
///
/// 1. **Speculative `u16` window** — decode through the bounded `u16` window
///    (worker-owned `scratch`, `U16_BUFCAP` cells) for as long as a
///    back-reference might still reach the unknown prefix, emitting markers for
///    those that do. By the DEFLATE 32 KiB-distance bound this lasts at most
///    [`U16_HANDOFF_AT`] cells.
/// 2. **Byte-wide kernel** — once the output is provably marker-free (`window_base`
///    advanced, or `cur ≥ U16_HANDOFF_AT`, always at a block boundary), lower the
///    live window into `chunk.bytes` and finish with the non-speculative `u8`
///    [`decode_one_block`] writing straight into `chunk.bytes`. This is where the
///    bulk of a multi-MiB chunk decodes, at byte (not `u16`) store bandwidth.
pub fn decode_until_u16(
    input: &[u8],
    start_bit: u64,
    end_bit_hint: u64,
    chunk: &mut SpeculativeChunk,
    scratch: &mut Vec<u16>,
) -> Result<(u64, bool), DeflateError> {
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Instant;
    let t_start = Instant::now();
    let bytes_in = chunk.bytes.len();
    macro_rules! acct_phase1 {
        () => {{
            PHASE1_NS.fetch_add(t_start.elapsed().as_nanos() as u64, Relaxed);
            PHASE1_BYTES.fetch_add((chunk.bytes.len() - bytes_in) as u64, Relaxed);
        }};
    }

    let mut br = BitReader::new(input);
    br.seek_to_bit(start_bit)?;
    if scratch.len() != U16_BUFCAP {
        scratch.clear();
        scratch.resize(U16_BUFCAP, 0);
    }

    // `cur` is the window-local write cursor; `window_base` is the global
    // member-relative position of `scratch[0]` (only ever 0 before the first
    // flush — overshoot markers can only appear in that regime). Both persist
    // across blocks of the member, since back-references cross block boundaries.
    let mut cur: usize = 0;
    let mut window_base: u64 = 0;

    // ── Phase 1 — speculative u16 window ────────────────────────────────────
    loop {
        let bfinal = br.read(1)? != 0;
        let btype = br.read(2)?;
        decode_block_u16(&mut br, scratch, chunk, btype, &mut cur, &mut window_base)?;
        if bfinal {
            // Whole member finished within phase 1: emit all of it and return.
            chunk.extract_from_u16(&scratch[0..cur]);
            acct_phase1!();
            return Ok((br.tell_bit(), true));
        }
        if br.tell_bit() >= end_bit_hint {
            // Chunk boundary reached on a block edge, still speculative.
            chunk.extract_from_u16(&scratch[0..cur]);
            acct_phase1!();
            return Ok((br.tell_bit(), false));
        }
        // Hand off to the byte kernel once the retained 32 KiB window — the only
        // history a phase-2 back-reference can read — is provably marker-free.
        // Markers start in the first 32 KiB but `copy_back_u16` can propagate
        // them forward through back-reference chains, so a position bound alone
        // is unsound; we must confirm the live window carries none. Checked only
        // at block boundaries, once enough history exists to retain a full window
        // beyond the original marker region. If markers persist (still
        // propagating), we stay on the u16 path — correct, just not accelerated.
        if cur >= U16_HANDOFF_AT
            && scratch[cur - U16_WINDOW..cur]
                .iter()
                .all(|&c| c & MARKER16 == 0)
        {
            break;
        }
    }

    // ── Hand-off ────────────────────────────────────────────────────────────
    // Lower the entire live window into the chunk. Cells at position ≥ U16_WINDOW
    // are real bytes; any markers (only possible below that) are recorded by
    // `extract_from_u16`. Afterwards `chunk.bytes` holds the full prefix and is
    // the DEFLATE window for the byte kernel below.
    chunk.extract_from_u16(&scratch[0..cur]);
    acct_phase1!();

    // ── Phase 2 — non-speculative byte kernel into `chunk.bytes` ─────────────
    // Every remaining back-reference (distance ≤ 32 KiB) now resolves within the
    // real history we just materialised, so no markers arise and we reuse the
    // optimised `u8` `decode_one_block`. Marker placeholders sit at positions
    // < U16_WINDOW and are never read here (phase-2 sources are ≥ U16_WINDOW).
    let t_p2 = Instant::now();
    let bytes_mid = chunk.bytes.len();
    let mut final_block = false;
    loop {
        if decode_one_block_preload(&mut br, &mut chunk.bytes)? {
            final_block = true;
            break;
        }
        if br.tell_bit() >= end_bit_hint {
            break;
        }
    }
    PHASE2_NS.fetch_add(t_p2.elapsed().as_nanos() as u64, Relaxed);
    PHASE2_BYTES.fetch_add((chunk.bytes.len() - bytes_mid) as u64, Relaxed);
    Ok((br.tell_bit(), final_block))
}

/// Temporary phase-split diagnostics (printed under `-v`): wall-ns and output
/// bytes attributed to the speculative u16 phase vs the byte-kernel phase,
/// summed across all decode workers.
pub static PHASE1_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PHASE2_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PHASE1_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PHASE2_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Emit everything older than the retained window, then slide the window down.
/// Caller guarantees `*cur > U16_WINDOW`.
#[inline]
fn flush_u16(
    scratch: &mut [u16],
    chunk: &mut SpeculativeChunk,
    cur: &mut usize,
    window_base: &mut u64,
) {
    let flush_count = *cur - U16_WINDOW;
    chunk.extract_from_u16(&scratch[0..flush_count]);
    scratch.copy_within((*cur - U16_WINDOW)..*cur, 0);
    *window_base += flush_count as u64;
    *cur = U16_WINDOW;
}

fn decode_block_u16(
    br: &mut BitReader<'_>,
    scratch: &mut Vec<u16>,
    chunk: &mut SpeculativeChunk,
    btype: u32,
    cur: &mut usize,
    window_base: &mut u64,
) -> Result<(), DeflateError> {
    match btype {
        0 => decode_stored_u16(br, scratch, chunk, cur, window_base),
        1 => {
            let lit = HuffmanDecoder::from_lengths_litlen(&fixed_literal_lengths())?;
            let dist = HuffmanDecoder::from_lengths_dist(&fixed_distance_lengths())?;
            decode_compressed_u16(br, scratch, chunk, &lit, &dist, cur, window_base)
        }
        2 => {
            let (lit, dist) = read_dynamic_header(br)?;
            decode_compressed_u16(br, scratch, chunk, &lit, &dist, cur, window_base)
        }
        _ => Err(DeflateError::Invalid("reserved block type")),
    }
}

fn decode_stored_u16(
    br: &mut BitReader<'_>,
    scratch: &mut [u16],
    chunk: &mut SpeculativeChunk,
    cur: &mut usize,
    window_base: &mut u64,
) -> Result<(), DeflateError> {
    br.byte_align();
    let len = br.read(16)? as u16;
    let nlen = br.read(16)? as u16;
    if !len ^ nlen != 0 {
        return Err(DeflateError::Invalid("stored block: LEN/NLEN mismatch"));
    }
    let mut remaining = len as usize;
    while remaining > 0 {
        if *cur >= U16_FLUSH_AT {
            flush_u16(scratch, chunk, cur, window_base);
        }
        let take = remaining.min(U16_FLUSH_AT - *cur);
        for _ in 0..take {
            scratch[*cur] = br.read(8)? as u16;
            *cur += 1;
        }
        remaining -= take;
    }
    Ok(())
}

/// Copy `length` `u16` cells from `cur - distance` to `cur`. May overwrite up to
/// `COPY_HEADROOM_U16 - 1` cells past `cur + length`; caller guarantees capacity.
///
/// # Safety
/// - `out_ptr[cur - distance .. cur + length + COPY_HEADROOM_U16]` is valid.
/// - `distance > 0` and `length > 0`.
#[inline(always)]
#[allow(unsafe_code)]
unsafe fn copy_back_u16(out_ptr: *mut u16, cur: usize, distance: usize, length: usize) {
    let src = out_ptr.add(cur - distance);
    let dst = out_ptr.add(cur);
    if distance >= length {
        // Non-overlapping: 8-cell (16-byte) chunked copy, may overshoot.
        let end = src.add(length);
        let mut s = src as *const u16;
        let mut d = dst;
        loop {
            let chunk: [u16; 8] = std::ptr::read_unaligned(s.cast::<[u16; 8]>());
            std::ptr::write_unaligned(d.cast::<[u16; 8]>(), chunk);
            s = s.add(8);
            d = d.add(8);
            if s >= end {
                break;
            }
        }
    } else if distance == 1 {
        // RLE: broadcast one cell (marker-ness included) the whole run.
        let v = *src;
        let mut d = dst;
        for _ in 0..length {
            *d = v;
            d = d.add(1);
        }
    } else {
        // Overlapping period > 1: cell-by-cell.
        for i in 0..length {
            *dst.add(i) = *src.add(i);
        }
    }
}

/// Decode one compressed block (fixed or dynamic) through the sliding window.
#[allow(unsafe_code)]
fn decode_compressed_u16(
    br: &mut BitReader<'_>,
    scratch: &mut Vec<u16>,
    chunk: &mut SpeculativeChunk,
    lit: &HuffmanDecoder,
    dist: &HuffmanDecoder,
    cur_ref: &mut usize,
    window_base_ref: &mut u64,
) -> Result<(), DeflateError> {
    const NEEDED: u32 = 48;
    const LUT_MASK: u64 = (1u64 << LUT_BITS) - 1;

    let mut out_ptr = scratch.as_mut_ptr();
    let mut cur = *cur_ref;
    let mut window_base = *window_base_ref;

    let mut buf = br.buf;
    let mut bits = br.bits;
    let mut byte_pos = br.byte_pos;
    let input_ptr = br.input.as_ptr();
    let input_len = br.input.len();
    let lit_lut = lit.lut();
    let dist_lut = dist.lut();

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
    // Refill to ≥ NEEDED bits. Takes the loop label so its EOF bail can break
    // out of the caller's `'outer` loop (label hygiene blocks referring to it
    // from the macro definition otherwise).
    macro_rules! refill {
        ($lbl:lifetime) => {
            if byte_pos + 8 <= input_len {
                buf |= unsafe { load_le_u64(input_ptr, byte_pos) } << bits;
                let advance = (63u32 ^ bits) >> 3;
                byte_pos += advance as usize;
                bits |= 56;
            } else if bits < NEEDED {
                while bits < NEEDED {
                    if byte_pos >= input_len {
                        sync_br!();
                        br.exhausted = true;
                        break $lbl Err(DeflateError::UnexpectedEof);
                    }
                    buf |= unsafe { (*input_ptr.add(byte_pos)) as u64 } << bits;
                    byte_pos += 1;
                    bits += 8;
                }
            }
        };
    }

    let result: Result<(), DeflateError> = 'outer: loop {
        if byte_pos + 8 <= input_len {
            buf |= unsafe { load_le_u64(input_ptr, byte_pos) } << bits;
            let advance = (63u32 ^ bits) >> 3;
            byte_pos += advance as usize;
            bits |= 56;
        } else if bits < NEEDED {
            while bits < NEEDED {
                if byte_pos >= input_len {
                    sync_br!();
                    br.exhausted = true;
                    break 'outer Err(DeflateError::UnexpectedEof);
                }
                buf |= unsafe { (*input_ptr.add(byte_pos)) as u64 } << bits;
                byte_pos += 1;
                bits += 8;
            }
        }

        // ── Flush the window if it has filled (cold: ~every 224 Ki cells) ─────
        if cur >= U16_FLUSH_AT {
            let flush_count = cur - U16_WINDOW;
            chunk.extract_from_u16(&scratch[0..flush_count]);
            scratch.copy_within((cur - U16_WINDOW)..cur, 0);
            window_base += flush_count as u64;
            cur = U16_WINDOW;
            out_ptr = scratch.as_mut_ptr();
        }

        // ── Literal/length symbol ────────────────────────────────────────────
        let mut entry = {
            let idx = (buf & LUT_MASK) as usize;
            let e = lit_lut[idx];
            let len = e & 0xff;
            if len == 0 {
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

        // ── Literal preload sub-loop ──────────────────────────────────────────
        // Same latency-hiding restructure as the u8 kernel: store the literal,
        // then immediately preload the next entry so the (2-byte) cell store and
        // flag test overlap the table-load latency. Bail to the top — which owns
        // the unconditional refill and the window-flush — once the bit budget
        // runs low or the window fills, so both invariants stay where they are.
        // Mask to the literal byte: `entry >> 16` would otherwise carry
        // HUFFDEC_LITERAL (bit 31) into bit 15, colliding with MARKER16.
        while entry & HUFFDEC_LITERAL != 0 {
            unsafe { *out_ptr.add(cur) = ((entry >> 16) & 0xff) as u16 };
            cur += 1;
            if bits < 15 || cur >= U16_FLUSH_AT {
                continue 'outer;
            }
            let e = lit_lut[(buf & LUT_MASK) as usize];
            let len = e & 0xff;
            if len == 0 {
                continue 'outer; // long code: re-resolve at the top
            }
            buf >>= len;
            bits -= len;
            entry = e;
        }

        // ── EOB / reserved ───────────────────────────────────────────────────
        if entry & HUFFDEC_EXCEPTIONAL != 0 {
            sync_br!();
            if entry >> 16 == 0 {
                break 'outer Ok(());
            }
            break 'outer Err(DeflateError::Invalid("literal/length symbol out of range"));
        }

        // The literal run may have left few bits; ensure the whole match
        // (length-extra ≤5 + distance code ≤15 + distance-extra ≤13) is covered.
        // When `entry` came straight from the top refill this is a no-op.
        if bits < 33 {
            refill!('outer);
        }

        // ── Length code ──────────────────────────────────────────────────────
        let length_base = ((entry >> 16) & 0x1ff) as usize;
        let len_extra = (entry >> 8) & 0x1f;
        let mut length = length_base;
        if len_extra > 0 {
            length += (buf & ((1u64 << len_extra) - 1)) as usize;
            buf >>= len_extra;
            bits -= len_extra;
        }

        // ── Distance symbol ──────────────────────────────────────────────────
        // Dist-encoded entry carries base + extra-count (see the u8 path).
        let dentry = {
            let idx = (buf & LUT_MASK) as usize;
            let e = dist_lut[idx];
            let len = e & 0xff;
            if len == 0 {
                sync_br!();
                match dist.lookup_long(br) {
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
        if dentry & HUFFDEC_EXCEPTIONAL != 0 {
            sync_br!();
            break 'outer Err(DeflateError::Invalid("distance symbol out of range"));
        }
        let mut distance = (dentry >> 16) as usize;
        let dextra = (dentry >> 8) & 0x1f;
        if dextra > 0 {
            distance += (buf & ((1u64 << dextra) - 1)) as usize;
            buf >>= dextra;
            bits -= dextra;
        }

        // ── Back-reference ────────────────────────────────────────────────────
        // Overshoot into the unknown prefix can only happen before the first
        // flush (`window_base == 0`): afterwards `cur >= U16_WINDOW >= distance`,
        // so the source is always in-window. That lets the hot test stay
        // `distance > cur` and the prefix_offset formula stay window-base-free.
        if window_base == 0 && distance > cur {
            let prefix_count = (distance - cur).min(length);
            for k in 0..prefix_count {
                let prefix_offset = (distance - cur - k - 1) as u16;
                unsafe { *out_ptr.add(cur + k) = MARKER16 | prefix_offset };
            }
            cur += prefix_count;
            let in_buffer = length - prefix_count;
            if in_buffer > 0 {
                unsafe { copy_back_u16(out_ptr, cur, distance, in_buffer) };
                cur += in_buffer;
            }
            continue 'outer;
        }
        if distance == 0 {
            sync_br!();
            break 'outer Err(DeflateError::Invalid("zero back-reference distance"));
        }
        unsafe { copy_back_u16(out_ptr, cur, distance, length) };
        cur += length;
    };

    *cur_ref = cur;
    *window_base_ref = window_base;
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
        assert_eq!(
            out,
            payload,
            "inflate_fast mismatch (len={} level={})",
            payload.len(),
            level
        );
    }

    fn ascii_payload(n: usize) -> Vec<u8> {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut p = Vec::with_capacity(n);
        while p.len() < n {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        p
    }

    /// The preload kernel must produce byte-identical output to the shipped
    /// `decode_member_u8` kernel for every payload/level we test.
    fn check_preload_matches(payload: &[u8], level: u32) {
        let deflate = deflate_via_gzip(payload, level);
        let mut padded = deflate.clone();
        padded.extend_from_slice(&[0u8; 64]);

        let mut a = Vec::new();
        decode_member_u8(&padded, 0, u64::MAX, &mut a).unwrap();
        let mut b = Vec::new();
        decode_member_u8_preload(&padded, 0, u64::MAX, &mut b).unwrap();

        assert_eq!(
            a,
            payload,
            "u8 kernel mismatch (len={} level={level})",
            payload.len()
        );
        assert_eq!(
            b,
            payload,
            "preload kernel mismatch (len={} level={level})",
            payload.len()
        );
    }

    #[test]
    fn preload_matches_u8() {
        check_preload_matches(b"", 6);
        check_preload_matches(b"hello, world\n", 6);
        check_preload_matches(b"aaaaaaaaaa", 9);
        check_preload_matches(&vec![b'x'; 10000], 6);
        check_preload_matches(&ascii_payload(1024), 6);
        check_preload_matches(&ascii_payload(256 * 1024), 6);
        let mut p = Vec::new();
        for _ in 0..1000 {
            p.extend_from_slice(b"abcdefghij");
        }
        check_preload_matches(&p, 6);
        let mut q = Vec::new();
        for i in 0..50000u32 {
            q.extend_from_slice(format!("line {i}: hello world\n").as_bytes());
        }
        check_preload_matches(&q, 9);
        // Stored-block coverage.
        let mut s: u64 = 0xA1B2C3D4E5F60718;
        let mut r = Vec::with_capacity(8192);
        while r.len() < 8192 {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            r.extend_from_slice(&s.to_le_bytes());
        }
        check_preload_matches(&r, 1);
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
        for _ in 0..1000 {
            p.extend_from_slice(b"abcdefghij");
        }
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
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
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
        assert!(
            chunk.markers.is_empty(),
            "{} unexpected markers",
            chunk.markers.len()
        );
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
        let _ = super::super::inflate::inflate_block(&mut br, &mut dummy).unwrap(); // block 0
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
                let bfinal = super::super::inflate::inflate_block(&mut br, &mut dummy).unwrap();
                if bfinal {
                    break;
                }
            }
        }
        assert!(block_starts.len() >= 3, "need ≥ 3 blocks for this test");
        let split_bit = block_starts[1];

        // Decode the head serially.
        let mut chunk0 = SpeculativeChunk::default();
        {
            let mut br = crate::BitReader::new(&padded);
            loop {
                if br.tell_bit() >= split_bit {
                    break;
                }
                super::super::inflate::inflate_block(&mut br, &mut chunk0.bytes).unwrap();
            }
        }
        assert!(chunk0.is_resolved());

        // Decode the rest speculatively.
        let mut chunk1 = SpeculativeChunk::default();
        decode_member(&padded, split_bit, &mut chunk1).unwrap();

        // Resolve markers.
        let tail_start = chunk0.bytes.len().saturating_sub(32 * 1024);
        super::super::speculative::resolve_markers(&mut chunk1, &chunk0.bytes[tail_start..])
            .unwrap();

        let mut stitched = chunk0.bytes.clone();
        stitched.extend_from_slice(&chunk1.bytes);
        assert_eq!(stitched.len(), payload.len(), "stitched length mismatch");
        assert!(
            stitched == payload,
            "stitched output does not match original"
        );
    }

    /// The u16 dense-window path must produce byte-and-marker-identical results
    /// to the side-table `decode_member` for the same midstream chunk.
    #[test]
    fn u16_matches_sidetable_midstream() {
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 16]);

        let mut block_starts: Vec<u64> = Vec::new();
        {
            let mut br = crate::BitReader::new(&padded);
            let mut dummy = Vec::new();
            loop {
                block_starts.push(br.tell_bit());
                let bfinal = super::super::inflate::inflate_block(&mut br, &mut dummy).unwrap();
                if bfinal {
                    break;
                }
            }
        }
        assert!(block_starts.len() >= 3);
        let split_bit = block_starts[1];

        let mut a = SpeculativeChunk::default();
        decode_member(&padded, split_bit, &mut a).unwrap();

        let mut b = SpeculativeChunk::default();
        let mut scratch = Vec::new();
        decode_until_u16(&padded, split_bit, u64::MAX, &mut b, &mut scratch).unwrap();

        assert_eq!(a.bytes.len(), b.bytes.len(), "byte length differs");
        assert_eq!(a.markers.len(), b.markers.len(), "marker count differs");
        // Compare marker (out_pos, prefix_offset) sets.
        for (i, (ma, mb)) in a.markers.iter().zip(b.markers.iter()).enumerate() {
            assert_eq!(
                (ma.out_pos, ma.prefix_offset),
                (mb.out_pos, mb.prefix_offset),
                "marker {i} differs: sidetable={ma:?} u16={mb:?}"
            );
        }
        // Compare non-marker bytes (marker placeholders may differ: 0 vs 0).
        assert!(a.bytes == b.bytes, "decoded bytes differ");
    }

    /// Simulate pipeline chunked speculative decode: split at every N-th block
    /// boundary, decode each chunk speculatively, resolve markers, stitch, compare.
    #[test]
    fn multichunk_speculative_roundtrip() {
        use super::super::find_next_dynamic_block;
        let payload = ascii_payload(4 * 1024 * 1024);
        let body = deflate_via_gzip(&payload, 6);
        let mut padded = body.clone();
        padded.extend_from_slice(&[0u8; 32]);

        let total_bits = body.len() as u64 * 8;

        // Find block boundaries at ~64KB compressed intervals (simulating pipeline).
        let chunk_bits = 64u64 * 1024 * 8;
        let mut boundaries: Vec<u64> = vec![0];
        let mut c = chunk_bits;
        while c < total_bits {
            if let Some(b) = find_next_dynamic_block(&padded, c, total_bits) {
                if b > *boundaries.last().unwrap() {
                    boundaries.push(b);
                }
            }
            c += chunk_bits;
        }

        // Decode each chunk speculatively, resolve, stitch.
        let mut resolved: Vec<u8> = Vec::new();
        let mut prev_tail: Vec<u8> = Vec::new();

        for i in 0..boundaries.len() {
            let start_bit = boundaries[i];
            let end_bit_hint = boundaries.get(i + 1).copied().unwrap_or(u64::MAX);

            let mut chunk = SpeculativeChunk::default();
            let (_, hit_bfinal) =
                decode_until(&padded, start_bit, end_bit_hint, &mut chunk).unwrap();

            super::super::speculative::resolve_markers(&mut chunk, &prev_tail).unwrap_or_else(
                |e| {
                    panic!("resolve_markers failed on chunk {i}: {e}");
                },
            );

            // Update prev_tail (last 32KB).
            const WINDOW: usize = 32 * 1024;
            prev_tail.extend_from_slice(&chunk.bytes);
            if prev_tail.len() > WINDOW {
                let drop = prev_tail.len() - WINDOW;
                prev_tail.drain(..drop);
            }

            resolved.extend_from_slice(&chunk.bytes);

            if hit_bfinal {
                break;
            }
        }

        assert_eq!(resolved.len(), payload.len(), "length mismatch");
        // Find first divergence for a useful error message.
        for (i, (a, b)) in resolved.iter().zip(payload.iter()).enumerate() {
            if a != b {
                panic!("first mismatch at byte {i}: got {a:#04x}, expected {b:#04x}");
            }
        }
    }
}
