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
    pub fn new() -> Self { Self::default() }
    pub fn is_resolved(&self) -> bool { self.markers.is_empty() }
    pub fn reserve_bytes(&mut self, n: usize) { self.bytes.reserve(n); }

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
    pub fn extract_from_u16(&mut self, scratch: &[u16]) {
        self.bytes.reserve(scratch.len());
        self.markers.reserve(scratch.len() / 8);
        let base = self.bytes.len() as u32;
        let mut last = self.max_marker_pos.unwrap_or(0);
        let mut any = false;
        for (i, &cell) in scratch.iter().enumerate() {
            if cell & Self::MARKER16 != 0 {
                let out_pos = base + i as u32;
                self.markers.push(Marker { out_pos, prefix_offset: cell & 0x7fff });
                self.bytes.push(0);
                last = out_pos;
                any = true;
            } else {
                self.bytes.push(cell as u8);
            }
        }
        if any {
            self.max_marker_pos = Some(last);
        }
    }

    pub fn bytes_offset_markers(
        &mut self,
        src: &[rusty_rapidgzip_inflate::speculative::MarkerRec],
        base: u32,
    ) {
        if src.is_empty() { return; }
        self.markers.reserve(src.len());
        let mut last = self.max_marker_pos.unwrap_or(0);
        for m in src {
            let pos = m.out_pos + base;
            self.markers.push(Marker { out_pos: pos, prefix_offset: m.prefix_offset });
            if pos > last { last = pos; }
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
pub fn resolve_markers(
    chunk: &mut SpeculativeChunk,
    prev_tail: &[u8],
) -> Result<(), DeflateError> {
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
