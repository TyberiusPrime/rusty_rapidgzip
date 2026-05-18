//! Speculative (mid-stream / no-window) decode support.
//!
//! When a worker starts mid-stream it has no real 32 KiB history. The
//! standard inflate engine would error with "invalid distance too far back"
//! on any back-reference whose distance exceeds the bytes emitted so far.
//!
//! Instead, the engine consults a thread-local [`SpeculativeContext`] when
//! it would otherwise error. The hook emits a placeholder byte and records
//! a `(out_pos, prefix_offset)` marker. Downstream code resolves the marker
//! once the preceding chunk's tail window is known.
//!
//! ## Invariants while a context is active
//! - The vendored engine's `Window` is empty (`have() == 0`). The engine
//!   never tries to read history; either the distance is in-buffer
//!   (handled normally) or it overflows (handled by marker emission).
//! - The hook is consulted *only* on the over-distance path, so the
//!   common in-buffer back-ref hot loop is untouched.

use core::cell::Cell;

/// One unresolved back-reference byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerRec {
    /// Output offset (in the speculative chunk's bytes) where the
    /// placeholder was written.
    pub out_pos: u32,
    /// Distance back into the unknown prefix, 0-based:
    /// 0 = the byte immediately before the chunk's first byte.
    pub prefix_offset: u16,
}

/// Storage exposed to the engine while a speculative decode is active.
#[derive(Debug, Default)]
pub struct SpeculativeContext {
    /// Markers recorded so far, in push (and therefore output-position) order.
    pub markers: Vec<MarkerRec>,
    /// Highest output position that has ever been recorded as a marker.
    /// Used as a fast early-out in [`propagate_match`]: if a back-ref's
    /// source range is strictly above this, no propagation is needed.
    pub max_marker_pos: Option<u32>,
    /// Cumulative bytes already produced by previous `decompress` calls in
    /// the same member. The engine's `writer.len()` is per-call (it resets
    /// between calls when the engine flushes to its `Window`), so the
    /// caller must update this offset before each call to keep marker
    /// `out_pos` values in member-absolute coordinates.
    pub out_pos_offset: u32,
}

thread_local! {
    /// Raw pointer to the active context. Null when none is active.
    static ACTIVE: Cell<*mut SpeculativeContext> = const { Cell::new(core::ptr::null_mut()) };
}

/// RAII guard that installs `ctx` as the active context for the current
/// thread for the duration of its lifetime. Use this around a call into
/// the engine when you want mid-stream decoding.
pub struct ContextGuard<'a> {
    _ctx: &'a mut SpeculativeContext,
    prev: *mut SpeculativeContext,
}

impl<'a> ContextGuard<'a> {
    pub fn new(ctx: &'a mut SpeculativeContext) -> Self {
        let raw = ctx as *mut SpeculativeContext;
        let prev = ACTIVE.with(|c| {
            let p = c.get();
            c.set(raw);
            p
        });
        Self { _ctx: ctx, prev }
    }
}

impl Drop for ContextGuard<'_> {
    fn drop(&mut self) {
        ACTIVE.with(|c| c.set(self.prev));
    }
}

pub fn current_out_pos_offset() -> u32 {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() { return 0; }
    let ctx = unsafe { &*ptr };
    ctx.out_pos_offset
}

/// `true` if a context is installed on the current thread.
#[inline(always)]
pub fn is_active() -> bool {
    ACTIVE.with(|c| !c.get().is_null())
}

/// Snapshot the active context pointer once. Hot inflate paths call this at
/// the top of the function and then pass the snapshot to
/// [`propagate_match_cached`] for every back-ref, avoiding a TLS load per
/// match. The pointer remains valid for the duration of the active
/// `ContextGuard` — i.e., for the whole `decompress` call.
#[inline(always)]
pub fn cache_active_ptr() -> *mut SpeculativeContext {
    ACTIVE.with(|c| c.get())
}

/// Set the active context's `out_pos_offset`. The caller must update this
/// before each `decompress` call so the engine's per-call `writer.len()`
/// values translate into member-absolute marker positions. No-op if no
/// context is installed.
pub fn set_out_pos_offset(offset: u32) {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() {
        return;
    }
    // SAFETY: caller of ContextGuard::new ensures the pointer is valid
    // for the duration of the guard, and we are inside that guard.
    let ctx = unsafe { &mut *ptr };
    ctx.out_pos_offset = offset;
}

/// Push `count` markers for a single match. The match starts at output
/// position `written_at_match_start` and would read from `dist` bytes
/// back. Only the first `count` bytes of the match are recorded here —
/// the caller advances the writer past these placeholders and copies any
/// in-buffer tail separately.
///
/// Returns `true` if a context was active and markers were pushed,
/// `false` if no context is active (caller should fall back to original
/// behaviour — typically an error).
#[inline]
pub fn record_match_prefix(
    written_at_match_start: usize,
    dist: usize,
    count: usize,
) -> bool {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() {
        return false;
    }
    // SAFETY: caller of ContextGuard::new ensures the pointer is valid
    // for the duration of the guard, and we are inside that guard.
    let ctx = unsafe { &mut *ptr };
    ctx.markers.reserve(count);
    let offset = ctx.out_pos_offset as usize;
    for k in 0..count {
        let out_pos = offset + written_at_match_start + k;
        let prefix_offset = dist - written_at_match_start - k - 1;
        ctx.markers.push(MarkerRec {
            out_pos: out_pos as u32,
            prefix_offset: prefix_offset as u16,
        });
        ctx.max_marker_pos = Some(out_pos as u32);
    }
    true
}

/// After zlib-rs has run an in-buffer `copy_match(dist, len)` writing to
/// output position `[written_at_match_start, written_at_match_start + len)`,
/// propagate any source-byte markers to the destination.
///
/// Called *unconditionally* by the engine on the in-buffer back-ref paths.
/// Fast early-out: if a context is not active, or the source range is
/// strictly above `max_marker_pos`, returns immediately with no work.
#[inline(always)]
pub fn propagate_match(written_at_match_start: usize, dist: usize, len: usize) {
    propagate_match_cached(cache_active_ptr(), written_at_match_start, dist, len);
}

/// Same as [`propagate_match`] but uses a pre-snapshotted context pointer
/// (see [`cache_active_ptr`]). The hot loops in `inflate_fast_help_impl`
/// and `dispatch` call this once per back-ref so the TLS load happens
/// once per `decompress` call instead of once per match.
#[inline(always)]
pub fn propagate_match_cached(
    ctx_ptr: *mut SpeculativeContext,
    written_at_match_start: usize,
    dist: usize,
    len: usize,
) {
    if ctx_ptr.is_null() {
        return;
    }
    // SAFETY: see record_match_prefix.
    let ctx = unsafe { &mut *ctx_ptr };
    let max = match ctx.max_marker_pos {
        None => return,
        Some(m) => m as usize,
    };
    // Source positions for k in 0..len are at (dst_start + k) - dist, in
    // member-absolute coordinates. This works for both in-buffer copies
    // (dist <= writer.len()) AND extend-from-window copies (dist > writer.len()
    // but the source is still within member-output via the engine's window).
    let dst_start = ctx.out_pos_offset as usize + written_at_match_start;
    if dst_start < dist {
        // Defensive: source would be before member start — over-distance hook
        // should have handled this. Don't propagate.
        return;
    }
    let src_lo = dst_start - dist;
    if src_lo > max {
        return;
    }

    // Walk left-to-right. For overlap copies (dist < len), source bytes at k>=dist
    // are themselves freshly-written destination bytes (which may have just
    // become markers in this very loop). The position formula handles both
    // by using absolute output positions: src_pos(k) = dst_start - dist + k.
    let mut new_max = max;
    for k in 0..len {
        let src_pos = (dst_start + k).wrapping_sub(dist);
        if src_pos > new_max {
            break;
        }
        let lookup =
            ctx.markers.binary_search_by_key(&(src_pos as u32), |m| m.out_pos);
        if let Ok(idx) = lookup {
            let po = ctx.markers[idx].prefix_offset;
            let new_pos = (dst_start + k) as u32;
            ctx.markers.push(MarkerRec { out_pos: new_pos, prefix_offset: po });
            new_max = new_pos as usize;
        }
    }
    ctx.max_marker_pos = Some(new_max as u32);
}
