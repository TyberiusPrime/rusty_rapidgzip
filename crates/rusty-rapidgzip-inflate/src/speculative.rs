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
}

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

    let mut cursor = ctx.markers.partition_point(|m| (m.out_pos as usize) < src_lo);
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
