//! Speculative ("no-window") DEFLATE decode.
//!
//! When a worker thread starts decoding mid-stream — at a chunk boundary
//! that the block-finder picked — it does not yet know the 32 KiB of
//! decompressed data preceding its starting position. Literals decode fine.
//! Back-references whose source falls inside the chunk's already-emitted
//! bytes also decode fine. Back-references reaching back into the unknown
//! prefix can't be resolved yet — we record a [`Marker`] for each such
//! byte and continue.
//!
//! Once the previous chunk's tail window is known (resolved by a serial
//! pass downstream), [`resolve_markers`] walks every marker and substitutes
//! the real byte. After that the chunk is identical to what a serial
//! decoder would have produced.
//!
//! ## Marker representation
//!
//! Markers are kept *sparse*: just `markers: Vec<Marker>` recording
//! `(out_pos, prefix_offset)`, plus a placeholder `0` byte at each marker
//! position in `bytes`. We deliberately do *not* maintain a dense parallel
//! `marker_at: Vec<u16>` — the inner literal loop runs millions of times per
//! chunk and a parallel vec doubles its per-byte cost.
//!
//! `prefix_offset` is the distance into the unknown prefix counted backwards
//! from the byte just before the chunk's first byte: 0 = the immediately
//! preceding byte, …, 32767 = the oldest byte still in the 32 KiB window.

use crate::DeflateError;

/// One unresolved back-reference byte. See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    /// Position within `SpeculativeChunk::bytes`.
    pub out_pos: u32,
    /// Distance back into the unknown prefix, 0-based:
    /// `prefix_offset = 0` means "the byte immediately before the chunk".
    pub prefix_offset: u16,
}

/// One chunk's worth of speculatively-decoded output.
///
/// - `bytes` holds output bytes, with a placeholder (any value) at marker
///   positions.
/// - `markers` is a sparse list of `(out_pos, prefix_offset)` kept sorted
///   by `out_pos` (push order is increasing position).
/// - `max_marker_pos` is the largest `out_pos` ever inserted into `markers`.
///   Used as a fast non-marker test: if a source byte is strictly past it,
///   there's no marker there to propagate.
#[derive(Debug, Default)]
pub struct SpeculativeChunk {
    pub bytes: Vec<u8>,
    pub markers: Vec<Marker>,
    /// Highest output position that is (or was) a marker. `None` when no
    /// marker has ever been pushed.
    max_marker_pos: Option<u32>,
}

impl SpeculativeChunk {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_resolved(&self) -> bool {
        self.markers.is_empty()
    }

    /// Pre-allocate `bytes` to avoid reallocation while decoding a chunk
    /// whose expected output size is known approximately.
    pub fn reserve_bytes(&mut self, n: usize) {
        self.bytes.reserve(n);
    }

    /// Append a batch of vendored-engine markers, translating their
    /// `out_pos` from member-local (0-based at the engine's first output
    /// byte) into chunk-local (0-based at `chunk.bytes[0]`).
    pub fn bytes_offset_markers(
        &mut self,
        src: &[rusty_rapidgzip_inflate::speculative::MarkerRec],
        base: u32,
    ) {
        if src.is_empty() {
            return;
        }
        self.markers.reserve(src.len());
        let mut last = self.max_marker_pos.unwrap_or(0);
        for m in src {
            let pos = m.out_pos + base;
            self.markers.push(Marker { out_pos: pos, prefix_offset: m.prefix_offset });
            if pos > last {
                last = pos;
            }
        }
        self.max_marker_pos = Some(last);
    }
}

/// Substitute marker placeholders using the previous chunk's tail window.
///
/// `prev_tail` must contain at least one byte. The byte at index
/// `prev_tail.len() - 1` is the byte immediately preceding the chunk's
/// first output byte (i.e., `prefix_offset == 0`). Older bytes have larger
/// indices into `prev_tail` from the right.
///
/// Returns an error if any marker references a `prefix_offset` deeper than
/// `prev_tail.len()`. That would mean the chunk's distance exceeded the
/// available window, which shouldn't happen for valid DEFLATE streams.
pub fn resolve_markers(
    chunk: &mut SpeculativeChunk,
    prev_tail: &[u8],
) -> Result<(), DeflateError> {
    for m in &chunk.markers {
        let off = m.prefix_offset as usize;
        if off >= prev_tail.len() {
            return Err(DeflateError::Invalid(
                "marker references bytes outside the available prefix window",
            ));
        }
        // prefix_offset 0 = last byte of prev_tail.
        let idx = prev_tail.len() - 1 - off;
        let b = prev_tail[idx];
        let pos = m.out_pos as usize;
        chunk.bytes[pos] = b;
    }
    chunk.markers.clear();
    chunk.max_marker_pos = None;
    Ok(())
}
