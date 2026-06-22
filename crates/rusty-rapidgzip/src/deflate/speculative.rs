//! Speculative ("no-window") DEFLATE decode.
//!
//! When a worker starts mid-stream without the preceding 32 KiB window, any
//! back-reference into that unknown region can't be resolved yet.  We emit a
//! placeholder byte and record a [`Marker`] — once the previous chunk's tail is
//! known, [`resolve_markers`] fills every placeholder in a single pass.

use crate::DeflateError;

/// One unresolved back-reference byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    pub out_pos: u32,
    /// 0-based distance back into the unknown prefix: 0 = the byte immediately
    /// before the chunk's first byte.
    pub prefix_offset: u16,
}

/// One chunk's worth of speculatively-decoded output.
#[derive(Debug, Default)]
pub struct SpeculativeChunk {
    pub bytes: Vec<u8>,
    pub markers: Vec<Marker>,
    max_marker_pos: Option<u32>,
}

impl SpeculativeChunk {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_resolved(&self) -> bool {
        self.markers.is_empty()
    }
    pub fn reserve_bytes(&mut self, n: usize) {
        self.bytes.reserve(n);
    }

    /// High bit of a dense-window cell marks an unresolved (prefix-referencing)
    /// byte; the low 15 bits hold its `prefix_offset`. See `fast_inflate`'s
    /// u16 speculative path.
    pub const MARKER16: u16 = 0x8000;

    /// Append a dense u16 marker-window into this chunk's `(bytes, markers)`.
    ///
    /// Each cell is either a resolved literal (high bit clear, value in the low
    /// 8 bits) or a marker (high bit set, `prefix_offset` in the low 15 bits).
    /// Markers are emitted in increasing `out_pos` order (scratch scanned
    /// left-to-right), preserving the sorted invariant the serializer relies on.
    ///
    /// Two passes instead of one branchy interleaved loop: on marker-saturated
    /// FASTQ this function is ~40% of the speculative path's cost, and the old
    /// per-cell `if marker { push marker; push 0 } else { push byte }` branch
    /// mispredicts heavily (~16% marker rate) *and* blocks the u16→u8 narrowing
    /// from vectorizing.
    ///
    /// - **Pass 1** narrows every cell to its low byte, branch-free, straight
    ///   into reserved spare capacity. This autovectorizes to a `packuswb`-style
    ///   u16→u8 pack. Marker cells get a junk low byte here.
    /// - **Pass 2** records the markers and rewrites their placeholder byte to
    ///   `0`. The placeholder value is irrelevant downstream (every marker
    ///   position is overwritten by [`resolve_markers`] before it is read, and
    ///   in-chunk back-references propagate marker-ness through *cells*, not
    ///   these bytes), but we keep the `0` to stay byte-identical to the
    ///   side-table path the tests pin against.
    #[allow(unsafe_code)]
    pub fn extract_from_u16(&mut self, scratch: &[u16]) {
        let n = scratch.len();
        let base = self.bytes.len();

        // Pass 1: branch-free narrow of all cells (vectorizes).
        self.bytes.reserve(n);
        let spare = self.bytes.spare_capacity_mut();
        for (d, &c) in spare[..n].iter_mut().zip(scratch) {
            d.write(c as u8);
        }
        // SAFETY: the loop initialised exactly `n` bytes of reserved spare
        // capacity starting at the old length.
        unsafe { self.bytes.set_len(base + n) }

        // Pass 2: record markers, zeroing their placeholder byte. Reserve for a
        // generous marker rate; Vec growth absorbs the rest if it saturates.
        self.markers.reserve(n / 4);
        let mut last = self.max_marker_pos.unwrap_or(0);
        let mut any = false;

        // Record one marker cell. `idx` is in 0..n; `cell` has MARKER16 set.
        // Markers are visited in ascending `idx`, preserving the sorted-by-
        // out_pos invariant the serializer relies on.
        macro_rules! record {
            ($idx:expr, $cell:expr) => {{
                let out_pos = (base + $idx) as u32;
                self.markers.push(Marker {
                    out_pos,
                    prefix_offset: $cell & 0x7fff,
                });
                self.bytes[base + $idx] = 0;
                last = out_pos;
                any = true;
            }};
        }

        // SSE2 marker scan: instead of a per-cell branch (which mispredicts at
        // FASTQ's ~16% marker rate), pull the high bit of 8 cells at once with
        // `movemask` and iterate only the set bits. SSE2 is baseline on
        // x86_64, so no runtime feature check.
        #[cfg(all(target_arch = "x86_64", not(miri)))]
        let tail_start = {
            use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_movemask_epi8};
            let mut i = 0;
            while i + 8 <= n {
                // SAFETY: `i + 8 <= n`, so 8 cells (16 bytes) are in bounds.
                let v = unsafe { _mm_loadu_si128(scratch.as_ptr().add(i).cast::<__m128i>()) };
                // Each u16 lane's high byte (MARKER16 = bit 15) lands on an odd
                // movemask bit (2*lane + 1); 0xAAAA keeps just those.
                let mut m = (unsafe { _mm_movemask_epi8(v) } as u32) & 0xAAAA;
                while m != 0 {
                    let lane = (m.trailing_zeros() >> 1) as usize;
                    let idx = i + lane;
                    record!(idx, scratch[idx]);
                    m &= m - 1;
                }
                i += 8;
            }
            i
        };
        #[cfg(not(all(target_arch = "x86_64", not(miri))))]
        let tail_start = 0;

        // `idx` is the marker position we record (via `record!`), not merely a
        // loop counter, so the index form is the natural one here.
        #[allow(clippy::needless_range_loop)]
        for idx in tail_start..n {
            let cell = scratch[idx];
            if cell & Self::MARKER16 != 0 {
                record!(idx, cell);
            }
        }

        if any {
            self.max_marker_pos = Some(last);
        }
    }

    pub fn bytes_offset_markers(
        &mut self,
        src: &[super::inflate::speculative::MarkerRec],
        base: u32,
    ) {
        if src.is_empty() {
            return;
        }
        self.markers.reserve(src.len());
        let mut last = self.max_marker_pos.unwrap_or(0);
        for m in src {
            let pos = m.out_pos + base;
            self.markers.push(Marker {
                out_pos: pos,
                prefix_offset: m.prefix_offset,
            });
            if pos > last {
                last = pos;
            }
        }
        self.max_marker_pos = Some(last);
    }
}

/// Substitute all marker placeholders using the previous chunk's tail window.
///
/// Two code paths:
/// - **Fast** (`prev_tail.len() >= 32768`): all 15-bit `prefix_offset` values
///   are in-range, so per-marker bounds checks are skipped.
/// - **Safe** (shorter tail, first chunk only): full bounds-checked loop.
#[allow(unsafe_code)]
pub fn resolve_markers(chunk: &mut SpeculativeChunk, prev_tail: &[u8]) -> Result<(), DeflateError> {
    // uncomment this to verify Miri triggers.
    // let mut v = vec![1, 2, 3];
    // let p = v.as_ptr();
    //
    // drop(v);
    //
    // unsafe {
    //     let val = *p;
    // }

    let ptail_len = prev_tail.len();
    // if false && ptail_len >= 32768 {
    //     // Fast path: all valid prefix_offsets (≤ 0x7FFF = 32767) are < ptail_len.
    //     unsaf {
    //         let ptail  = prev_tail.as_ptr();
    //         let pbytes = chunk.bytes.as_mut_ptr();
    //         for m in &chunk.markers {
    //             *pbytes.add(m.out_pos as usize) =
    //                 *ptail.add(ptail_len - 1 - m.prefix_offset as usize);
    //         }
    //     }
    // } else {
    // cargo bench -p rusty-rapidgzip-deflate --bench inflate_kernels -- --baseline before
    // says this within sthe noise threshold.
    for m in &chunk.markers {
        let off = m.prefix_offset as usize;
        if off >= ptail_len {
            return Err(DeflateError::Invalid(
                "marker references bytes outside the available prefix window",
            ));
        }
        chunk.bytes[m.out_pos as usize] = prev_tail[ptail_len - 1 - off];
    }
    // }
    chunk.markers.clear();
    chunk.max_marker_pos = None;
    Ok(())
}
