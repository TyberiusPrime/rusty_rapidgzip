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
    if ptail_len >= 32768 {
        // Fast path: all valid prefix_offsets (≤ 0x7FFF = 32767) are < ptail_len.
        unsafe {
            let ptail  = prev_tail.as_ptr();
            let pbytes = chunk.bytes.as_mut_ptr();
            for m in &chunk.markers {
                *pbytes.add(m.out_pos as usize) =
                    *ptail.add(ptail_len - 1 - m.prefix_offset as usize);
            }
        }
    } else {
        for m in &chunk.markers {
            let off = m.prefix_offset as usize;
            if off >= ptail_len {
                return Err(DeflateError::Invalid(
                    "marker references bytes outside the available prefix window",
                ));
            }
            chunk.bytes[m.out_pos as usize] = prev_tail[ptail_len - 1 - off];
        }
    }
    chunk.markers.clear();
    chunk.max_marker_pos = None;
    Ok(())
}
