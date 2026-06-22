//! Descriptor protocol for the zero-copy "direct" mode: the decoder's output
//! `Vec<u8>`s already live in the shared pool (see [`crate::shm_alloc`]), so the
//! child only needs to tell the parent *where* each chunk is, and the parent
//! reads it in place. No bytes are copied across the process boundary; the only
//! per-byte work left is the consumer's own `write_all` (a copy the in-process
//! build also pays) and the unavoidable cross-core cache traffic.
//!
//! Two SPSC rings live in the shared region's header:
//! - **forward** (child → parent): a `Desc { off, len }` per decoded chunk.
//! - **return** (parent → child): the `off` of each chunk the parent has finished
//!   reading, so the child can recycle/free that pool buffer.
//!
//! Both are bounded (depth [`RING`]); flow control is the caller's (spin/backoff).

use std::sync::atomic::{
    AtomicU32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};

/// Depth of each descriptor ring (entries, power of two). This is the crucial
/// backpressure knob: the child can publish at most `RING` chunks ahead of the
/// consumer, so it bounds the number of pool buffers held in flight (and hence
/// the pool size needed). Too deep and a slow consumer lets the child pin the
/// whole file's worth of buffers and exhaust the pool; this is comfortably more
/// than the pipeline's own in-flight cap (max 2·threads) so it never throttles
/// decode for reasonable thread counts.
pub const RING: u64 = 32;
const MASK: u64 = RING - 1;

/// Where a chunk lives in the shared pool.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Desc {
    pub off: u64,
    pub len: u64,
    /// Debug-only checksum of the chunk bytes at publish time (0 otherwise).
    pub sum: u64,
}

/// Cheap order-sensitive checksum for the in-flight-mutation diagnostic.
pub fn debug_sum(b: &[u8]) -> u64 {
    let mut h: u64 = 1469598103934665603;
    for &x in b.iter().step_by(257) {
        h = (h ^ x as u64).wrapping_mul(1099511628211);
    }
    h ^ (b.len() as u64)
}

/// Control block: four cursors (each alone on a cache line) plus end-of-stream
/// flags.
#[repr(C, align(64))]
struct Ctl {
    fwd_head: AtomicU64, // child publishes
    _p0: [u8; 56],
    fwd_tail: AtomicU64, // parent consumes
    _p1: [u8; 56],
    ret_head: AtomicU64, // parent publishes
    _p2: [u8; 56],
    ret_tail: AtomicU64, // child consumes
    _p3: [u8; 56],
    done: AtomicU32,
    error: AtomicU32,
    _p4: [u8; 56],
}

/// Handle onto the shared region. `Copy` so the parent can share one with its
/// watchdog thread; the owning `Shmem` keeps the mapping alive.
#[derive(Clone, Copy)]
pub struct Direct {
    ctl: *const Ctl,
    fwd: *mut Desc,
    ret: *mut u64,
    pool: *mut u8,
    pool_size: usize,
}

// SAFETY: see `crate::ring::Ring` — plain values into a shared mapping that
// outlives every handle; producer/consumer touch disjoint fields.
unsafe impl Send for Direct {}

impl Direct {
    const CTL: usize = std::mem::size_of::<Ctl>();
    const FWD: usize = Self::CTL;
    const RET: usize = Self::FWD + (RING as usize) * std::mem::size_of::<Desc>();
    const POOL: usize = {
        // 4 KiB-align the pool start (page boundary).
        let end = Self::RET + (RING as usize) * std::mem::size_of::<u64>();
        (end + 4095) & !4095
    };

    /// Total shared-region bytes for a pool of `pool_size` bytes.
    pub fn shared_size(pool_size: usize) -> usize {
        Self::POOL + pool_size
    }

    /// Consumer side: zero the header and return a handle.
    ///
    /// # Safety
    /// `base` must map at least `shared_size(pool_size)` writable bytes.
    pub unsafe fn init(base: *mut u8, pool_size: usize) -> Direct {
        std::ptr::write_bytes(base, 0, Self::CTL);
        Self::attach(base, pool_size)
    }

    /// Producer side: attach to a region the consumer already initialised.
    ///
    /// # Safety
    /// Same mapping contract as [`Direct::init`].
    pub unsafe fn attach(base: *mut u8, pool_size: usize) -> Direct {
        Direct {
            ctl: base as *const Ctl,
            fwd: base.add(Self::FWD) as *mut Desc,
            ret: base.add(Self::RET) as *mut u64,
            pool: base.add(Self::POOL),
            pool_size,
        }
    }

    #[inline]
    fn ctl(&self) -> &Ctl {
        // SAFETY: `ctl` points at a live, initialised Ctl for the handle's life.
        unsafe { &*self.ctl }
    }

    /// Pool base pointer (what `shm_alloc::init` is given) and size.
    pub fn pool(&self) -> *mut u8 {
        self.pool
    }
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    // ---- child (producer) ----

    /// Try to publish a chunk descriptor. Returns false if the forward ring is
    /// full (caller should drain returns and retry).
    pub fn try_push_desc(&self, d: Desc) -> bool {
        let c = self.ctl();
        let head = c.fwd_head.load(Relaxed); // sole producer
        let tail = c.fwd_tail.load(Acquire);
        if head.wrapping_sub(tail) >= RING {
            return false;
        }
        // SAFETY: slot is free (not yet consumed) and in-bounds.
        unsafe { *self.fwd.add((head & MASK) as usize) = d };
        c.fwd_head.store(head.wrapping_add(1), Release);
        true
    }

    /// Pop one finished-chunk offset the parent returned, if any.
    pub fn pop_return(&self) -> Option<u64> {
        let c = self.ctl();
        let tail = c.ret_tail.load(Relaxed); // sole consumer
        let head = c.ret_head.load(Acquire);
        if tail == head {
            return None;
        }
        let off = unsafe { *self.ret.add((tail & MASK) as usize) };
        c.ret_tail.store(tail.wrapping_add(1), Release);
        Some(off)
    }

    pub fn finish(&self) {
        self.ctl().done.store(1, Release);
    }
    pub fn fail(&self) {
        self.ctl().error.store(1, Relaxed);
        self.ctl().done.store(1, Release);
    }

    // ---- parent (consumer) ----

    /// Pop the next chunk descriptor, freeing its forward slot. The pool bytes it
    /// points at stay valid until the parent returns the offset (the child won't
    /// reuse the buffer before then).
    pub fn pop_desc(&self) -> Option<Desc> {
        let c = self.ctl();
        let tail = c.fwd_tail.load(Relaxed); // sole consumer
        let head = c.fwd_head.load(Acquire);
        if tail == head {
            return None;
        }
        let d = unsafe { *self.fwd.add((tail & MASK) as usize) };
        c.fwd_tail.store(tail.wrapping_add(1), Release);
        Some(d)
    }

    /// Borrow a chunk's bytes in the pool.
    ///
    /// # Safety
    /// `d` must be a descriptor just returned by [`Direct::pop_desc`]; the borrow
    /// is valid until the offset is returned via [`Direct::try_push_return`].
    pub unsafe fn slice(&self, d: Desc) -> &[u8] {
        std::slice::from_raw_parts(self.pool.add(d.off as usize), d.len as usize)
    }

    /// Return a finished offset to the child. False if the return ring is full
    /// (caller should retry; the child always eventually drains it).
    pub fn try_push_return(&self, off: u64) -> bool {
        let c = self.ctl();
        let head = c.ret_head.load(Relaxed); // sole producer
        let tail = c.ret_tail.load(Acquire);
        if head.wrapping_sub(tail) >= RING {
            return false;
        }
        unsafe { *self.ret.add((head & MASK) as usize) = off };
        c.ret_head.store(head.wrapping_add(1), Release);
        true
    }

    pub fn is_done(&self) -> bool {
        self.ctl().done.load(Acquire) != 0
    }
    pub fn errored(&self) -> bool {
        self.ctl().error.load(Acquire) != 0
    }
}

/// Spin → yield backoff shared by the wait loops.
pub struct Backoff(u32);
impl Backoff {
    pub fn new() -> Self {
        Backoff(0)
    }
    pub fn reset(&mut self) {
        self.0 = 0;
    }
    pub fn snooze(&mut self) {
        if self.0 < 10 {
            for _ in 0..(1u32 << self.0) {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        self.0 = (self.0 + 1).min(20);
    }
}
