//! A process-global allocator that routes *large* allocations into a shared-memory
//! pool, so the decode pipeline's output `Vec<u8>`s land directly in shared memory
//! and the consumer process can read them by offset with no copy.
//!
//! Why a global allocator at all: the library hands output back as plain
//! `Vec<u8>` (global allocator). We cannot safely point a `Vec` at foreign memory
//! (it would free/realloc through the global allocator and corrupt the pool). So
//! instead we *make* the global allocator hand the pool's memory to exactly the
//! allocations we care about, and let `Vec` grow/free against it correctly.
//!
//! Routing:
//! - **alloc**: if the pool is initialised (child only) and `size >= THRESHOLD`,
//!   serve from the pool; otherwise fall through to the System allocator. Output
//!   chunks are megabytes (well over the threshold); the pipeline's hot small
//!   allocations stay on the fast System allocator, so the pool lock is cold.
//! - **dealloc / realloc**: route purely by *address* — a pointer inside the pool
//!   range goes back to the pool, anything else to System. This is what makes the
//!   parent process (pool never initialised) and pre-init allocations safe, and
//!   makes `Vec` growth across the threshold correct.
//!
//! The pool is a trivial segregated free-list (power-of-two size classes, intrusive
//! free lists stored in the freed blocks themselves, bump-allocated on miss) under
//! a spinlock. It never allocates, so it is safe to call from within the allocator.
//! Allocations are rare (output buffers are recycled), so the spinlock is cold.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

/// Only allocations this size or larger are eligible for the pool. Output chunks
/// are MiB-scale; everything smaller (huffman scratch, channel nodes, ...) stays
/// on System so the pool lock stays cold and uncontended.
pub const THRESHOLD: usize = 64 * 1024;

/// Size classes 2^3 .. 2^(3+NUM_CLASSES-1). 40 ⇒ up to 2^42 = 4 TiB, ample.
const NUM_CLASSES: usize = 40;
const MIN_CLASS_SHIFT: u32 = 3; // 8 bytes, room for the intrusive next-pointer

static POOL_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
static POOL_SIZE: AtomicUsize = AtomicUsize::new(0);

/// Spinlock-guarded pool bookkeeping. `free` holds intrusive free-list heads per
/// size class; `cursor` is the bump offset for never-before-allocated classes.
struct PoolState {
    free: UnsafeCell<[*mut u8; NUM_CLASSES]>,
    cursor: UnsafeCell<usize>,
}
// SAFETY: all access is serialised by `LOCK`.
unsafe impl Sync for PoolState {}

static STATE: PoolState = PoolState {
    free: UnsafeCell::new([std::ptr::null_mut(); NUM_CLASSES]),
    cursor: UnsafeCell::new(0),
};
static LOCK: AtomicBool = AtomicBool::new(false);

#[inline]
fn lock() {
    while LOCK.swap(true, Ordering::Acquire) {
        std::hint::spin_loop();
    }
}
#[inline]
fn unlock() {
    LOCK.store(false, Ordering::Release);
}

/// Initialise the pool over `[base, base+size)`. Call once, in the child, before
/// any worker threads (or any large allocation) start. Idempotent-unsafe: don't
/// call twice.
pub fn init(base: *mut u8, size: usize) {
    POOL_SIZE.store(size, Ordering::Relaxed);
    // Release so a subsequent allocator `alloc` (Acquire load) sees size too.
    POOL_BASE.store(base, Ordering::Release);
}

#[inline]
fn class_index(layout: Layout) -> Option<(usize, usize)> {
    let need = layout.size().max(layout.align());
    let class = need.next_power_of_two();
    let shift = class.trailing_zeros().max(MIN_CLASS_SHIFT);
    let idx = (shift - MIN_CLASS_SHIFT) as usize;
    if idx >= NUM_CLASSES {
        None
    } else {
        Some((idx, 1usize << shift))
    }
}

#[inline]
fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}

pub struct DualAlloc;

// SAFETY: alloc serves either System (sound) or pool memory carved from a live
// mapping; dealloc/realloc route by address so a pointer always returns to the
// allocator that produced it. Pool internals are spinlock-serialised.
unsafe impl GlobalAlloc for DualAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let base = POOL_BASE.load(Ordering::Acquire);
        if base.is_null() || layout.size() < THRESHOLD {
            return System.alloc(layout);
        }
        let (idx, class) = match class_index(layout) {
            Some(v) => v,
            None => return System.alloc(layout),
        };
        let size = POOL_SIZE.load(Ordering::Relaxed);
        lock();
        // Reuse a freed block of this class if any.
        let heads = &mut *STATE.free.get();
        let head = heads[idx];
        if !head.is_null() {
            // The freed block's first word holds the next free pointer.
            heads[idx] = *(head as *const *mut u8);
            unlock();
            return head;
        }
        // Otherwise bump-allocate a fresh `class`-sized, `class`-aligned block.
        let cursor = &mut *STATE.cursor.get();
        let off = align_up(*cursor, class);
        if off + class > size {
            unlock();
            return System.alloc(layout); // pool exhausted → safe fallback
        }
        *cursor = off + class;
        unlock();
        base.add(off)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !in_pool(ptr) {
            System.dealloc(ptr, layout);
            return;
        }
        // Layout matches the original alloc, so the class matches too.
        let (idx, _) = class_index(layout).expect("pool ptr ⇒ valid class");
        lock();
        let heads = &mut *STATE.free.get();
        *(ptr as *mut *mut u8) = heads[idx];
        heads[idx] = ptr;
        unlock();
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Cross-allocator-safe by construction: alloc routes the new block, our
        // dealloc routes the old one by address. (Same as the default impl, spelled
        // out so the routing intent is explicit.)
        let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
        let new_ptr = self.alloc(new_layout);
        if !new_ptr.is_null() {
            std::ptr::copy_nonoverlapping(ptr, new_ptr, layout.size().min(new_size));
            self.dealloc(ptr, layout);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout};

    /// Concurrently alloc/fill/verify/free pool blocks. If the allocator ever
    /// hands out overlapping live blocks (or corrupts its free list), a thread's
    /// pattern gets clobbered and the verify fails (or it segfaults).
    #[test]
    fn stress_no_overlap() {
        // Page-align the base like a real shm mapping (the production invariant
        // the allocator relies on: pool base aligned ≥ any requested alignment).
        let region = vec![0u8; 256 * 1024 * 1024 + 4096].into_boxed_slice();
        let base = align_up(region.as_ptr() as usize, 4096) as *mut u8;
        init(base, 256 * 1024 * 1024);

        std::thread::scope(|s| {
            for t in 0..8u64 {
                s.spawn(move || {
                    let a = DualAlloc;
                    let mut rng = t.wrapping_mul(0x9E3779B97F4A7C15) + 1;
                    let mut live: Vec<(*mut u8, usize, u8)> = Vec::new();
                    for _ in 0..4000 {
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                        // Keep ~6 blocks live so overlaps have a window to clobber.
                        if live.len() >= 6 || (rng & 1 == 0 && !live.is_empty()) {
                            let (p, sz, pat) = live.swap_remove((rng as usize) % live.len());
                            for i in (0..sz).step_by(512) {
                                assert_eq!(unsafe { *p.add(i) }, pat, "clobbered before free");
                            }
                            unsafe { a.dealloc(p, Layout::from_size_align(sz, 1).unwrap()) };
                        } else {
                            let sz = THRESHOLD + (rng as usize % (1 << 20));
                            let align = 1 << ((rng >> 20) % 7); // 1..64
                            let layout = Layout::from_size_align(sz, align).unwrap();
                            let mut p = unsafe { a.alloc(layout) };
                            assert!(!p.is_null(), "pool exhausted unexpectedly");
                            let mut cur = sz;
                            let pat = (rng as u8) | 1;
                            unsafe { std::ptr::write_bytes(p, pat, cur) };
                            assert_eq!(p as usize % align, 0, "misaligned");
                            // Half the time, grow it via realloc (Vec's hot path).
                            if rng & 2 == 0 {
                                let newsz = cur + (rng as usize % (1 << 20));
                                p = unsafe { a.realloc(p, layout, newsz) };
                                assert!(!p.is_null());
                                // old bytes must survive the realloc copy
                                for i in (0..cur).step_by(512) {
                                    assert_eq!(unsafe { *p.add(i) }, pat, "realloc lost data");
                                }
                                unsafe { std::ptr::write_bytes(p, pat, newsz) };
                                cur = newsz;
                            }
                            live.push((p, cur, pat));
                        }
                        // Re-verify all live blocks each iteration.
                        for &(p, sz, pat) in &live {
                            assert_eq!(unsafe { *p.add(sz - 1) }, pat, "live block clobbered");
                        }
                    }
                    for (p, sz, _) in live {
                        unsafe { a.dealloc(p, Layout::from_size_align(sz, 1).unwrap()) };
                    }
                });
            }
        });
    }
}

/// Whether `ptr` lies inside the shared pool range.
#[inline]
pub fn in_pool(ptr: *const u8) -> bool {
    let base = POOL_BASE.load(Ordering::Acquire) as usize;
    if base == 0 {
        return false;
    }
    let p = ptr as usize;
    p >= base && p < base + POOL_SIZE.load(Ordering::Relaxed)
}
