//! Speculative (mid-stream / no-window) decode support.
//!
//! When a worker starts mid-stream it has no real 32 KiB history.  Any
//! back-reference whose distance exceeds the bytes emitted so far would
//! otherwise error; instead the engine emits a placeholder byte and records a
//! `(out_pos, prefix_offset)` marker.  Downstream code resolves the marker once
//! the preceding chunk's tail window is known.

use core::cell::Cell;

pub const RUN_FLAG: u16 = 0x8000; // reserved; unused by this crate

/// One unresolved back-reference byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerRec {
    pub out_pos: u32,
    /// Distance back into the unknown prefix, 0-based: 0 = the byte immediately
    /// before the chunk's first byte.
    pub prefix_offset: u16,
}

/// Storage exposed to the engine while a speculative decode is active.
#[derive(Debug, Default)]
pub struct SpeculativeContext {
    pub markers: Vec<MarkerRec>,
    pub max_marker_pos: Option<u32>,
    pub out_pos_offset: u32,
    /// Index into `markers` of the first marker still reachable as a copy
    /// *source*. Markers below this have `out_pos < write_head - MAX_DISTANCE`
    /// and can never be matched again, so the per-back-ref `partition_point`
    /// only ever scans `markers[live_start..]` — a window-bounded, cache-hot
    /// tail rather than the whole (potentially multi-million-entry) Vec.
    /// Markers are pushed in strictly increasing `out_pos` order, so the Vec
    /// stays sorted and this cursor only advances. Retired markers remain in
    /// the Vec for final resolution; this is purely a search optimization.
    pub live_start: usize,
}

/// Maximum DEFLATE back-reference distance. A marker at output position `p`
/// can only be a copy source while the write head is in `(p, p + MAX_DISTANCE]`.
const MAX_DISTANCE: usize = 32768;

thread_local! {
    static ACTIVE: Cell<*mut SpeculativeContext> = const { Cell::new(core::ptr::null_mut()) };
}

pub struct ContextGuard<'a> {
    _ctx: &'a mut SpeculativeContext,
    prev: *mut SpeculativeContext,
}

impl<'a> ContextGuard<'a> {
    pub fn new(ctx: &'a mut SpeculativeContext) -> Self {
        let raw = ctx as *mut SpeculativeContext;
        let prev = ACTIVE.with(|c| { let p = c.get(); c.set(raw); p });
        Self { _ctx: ctx, prev }
    }
}

impl Drop for ContextGuard<'_> {
    fn drop(&mut self) { ACTIVE.with(|c| c.set(self.prev)); }
}

pub fn current_out_pos_offset() -> u32 {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() { return 0; }
    unsafe { (*ptr).out_pos_offset }
}

#[inline(always)]
pub fn is_active() -> bool { ACTIVE.with(|c| !c.get().is_null()) }

#[inline(always)]
pub fn cache_active_ptr() -> *mut SpeculativeContext { ACTIVE.with(|c| c.get()) }

pub fn set_out_pos_offset(offset: u32) {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() { return; }
    unsafe { (*ptr).out_pos_offset = offset; }
}

/// Push one per-byte marker for each byte of the prefix-overshoot match.
#[inline]
pub fn record_match_prefix(written_at_match_start: usize, dist: usize, count: usize) -> bool {
    let ptr = ACTIVE.with(|c| c.get());
    if ptr.is_null() { return false; }
    let ctx = unsafe { &mut *ptr };
    ctx.markers.reserve(count);
    let offset = ctx.out_pos_offset as usize;
    for k in 0..count {
        let out_pos = (offset + written_at_match_start + k) as u32;
        let prefix_offset = (dist - written_at_match_start - k - 1) as u16;
        ctx.markers.push(MarkerRec { out_pos, prefix_offset });
        ctx.max_marker_pos = Some(out_pos);
    }
    true
}

#[inline(always)]
pub fn propagate_match(written_at_match_start: usize, dist: usize, len: usize) {
    propagate_match_cached(cache_active_ptr(), written_at_match_start, dist, len);
}

/// Propagate markers from a just-executed in-buffer copy.
///
/// One `partition_point` to find the first relevant source marker, then a
/// single linear walk.  Monotone cursor handles both non-overlap and overlap
/// (RLE-tiling) copies.
#[inline(always)]
pub fn propagate_match_cached(
    ctx_ptr: *mut SpeculativeContext,
    written_at_match_start: usize,
    dist: usize,
    len: usize,
) {
    if ctx_ptr.is_null() { return; }
    let ctx = unsafe { &mut *ctx_ptr };
    let max = match ctx.max_marker_pos { None => return, Some(m) => m as usize };
    let dst_start = ctx.out_pos_offset as usize + written_at_match_start;
    if dst_start < dist { return; }
    let src_lo = dst_start - dist;
    if src_lo > max { return; }

    // Retire markers that can no longer be a copy source: any marker with
    // `out_pos < dst_start - MAX_DISTANCE` is unreachable (no future back-ref
    // can reach that far back). The source range starts at `src_lo >= dst_start
    // - MAX_DISTANCE`, so retired markers are always below the search window.
    // This keeps the searched slice bounded to one window (≤ 32768 entries),
    // which stays cache-resident instead of binary-searching the full Vec.
    let live_floor = dst_start.saturating_sub(MAX_DISTANCE);
    {
        let markers = &ctx.markers;
        let mut ls = ctx.live_start;
        while ls < markers.len() && (markers[ls].out_pos as usize) < live_floor {
            ls += 1;
        }
        ctx.live_start = ls;
    }
    let live_start = ctx.live_start;

    let mut cursor = live_start
        + ctx.markers[live_start..].partition_point(|m| (m.out_pos as usize) < src_lo);
    let mut new_max = max;

    for k in 0..len {
        let src_pos = src_lo + k;
        if src_pos > new_max { break; }
        let markers_len = ctx.markers.len();
        while cursor < markers_len && (ctx.markers[cursor].out_pos as usize) < src_pos {
            cursor += 1;
        }
        if cursor >= markers_len {
            if k + 1 < len && src_lo + k + 1 <= new_max { continue; }
            break;
        }
        if (ctx.markers[cursor].out_pos as usize) == src_pos {
            let po = ctx.markers[cursor].prefix_offset;
            let new_pos = (dst_start + k) as u32;
            ctx.markers.push(MarkerRec { out_pos: new_pos, prefix_offset: po });
            new_max = new_pos as usize;
            cursor += 1;
        }
    }

    ctx.max_marker_pos = Some(new_max as u32);
}
