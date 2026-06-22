//! A single-producer / single-consumer byte ring living in a shared-memory
//! region, so the producer (decoder process) and consumer (this binary) can hand
//! decoded bytes across a process boundary without a kernel copy or a pipe.
//!
//! It is a textbook Lamport SPSC ring: two monotonically-increasing `u64`
//! cursors (`write_pos`, `read_pos`) on their own cache lines, `Acquire`/`Release`
//! ordered. `used = write_pos - read_pos`, `free = cap - used`; the ring index is
//! `pos & (cap - 1)` (capacity is a power of two). The whole control block lives
//! at the front of the shared region; the ring data follows it.
//!
//! Correctness across processes relies only on the region being the *same*
//! physical memory mapped into both address spaces — which is exactly what
//! `shared_memory` gives us — so the atomics synchronise as usual.

use std::sync::atomic::{
    AtomicU32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};

/// Control block at offset 0 of the shared region. Each hot cursor sits alone on
/// a 64-byte line to keep the producer's `write_pos` store from invalidating the
/// consumer's `read_pos` line (false sharing would erase the point of the ring).
#[repr(C, align(64))]
struct Header {
    write_pos: AtomicU64,
    _pad0: [u8; 56],
    read_pos: AtomicU64,
    _pad1: [u8; 56],
    /// 1 once the producer has published its final `write_pos` and will write no
    /// more (set on both clean finish and error).
    done: AtomicU32,
    /// 1 if the producer ended abnormally (decode error or child crash via the
    /// parent watchdog). Output already drained may be partial.
    error: AtomicU32,
    _pad2: [u8; 56],
}

/// Handle onto a ring inside a shared region. `Copy` so the parent can hand a
/// second handle to its child-watchdog thread; both point at the same region,
/// which the owning `Shmem` keeps mapped for the lifetime of the handles.
#[derive(Clone, Copy)]
pub struct Ring {
    hdr: *const Header,
    data: *mut u8,
    /// Power-of-two ring capacity in bytes.
    cap: usize,
}

// SAFETY: a `Ring` is just three plain values pointing into a shared mapping that
// outlives every handle. The producer touches only `data`/`write_pos`/`done`/
// `error`, the consumer only `data`/`read_pos`; the one cross-process pair of
// handles (parent consumer + watchdog) touch disjoint fields. No aliased &mut.
unsafe impl Send for Ring {}

impl Ring {
    /// Bytes the header occupies before the ring data. `size_of::<Header>()` is a
    /// multiple of 64, so the data start stays 64-byte aligned.
    const DATA_OFFSET: usize = std::mem::size_of::<Header>();

    /// Total shared-region size needed for a ring of `cap` data bytes.
    pub fn shared_size(cap: usize) -> usize {
        Self::DATA_OFFSET + cap
    }

    /// Consumer side: zero the header (all cursors/flags to 0) and return a
    /// handle. Call exactly once, before the producer attaches.
    ///
    /// # Safety
    /// `base` must point at a writable mapping of at least `shared_size(cap)`
    /// bytes, and `cap` must be a power of two.
    pub unsafe fn init(base: *mut u8, cap: usize) -> Ring {
        debug_assert!(cap.is_power_of_two());
        std::ptr::write_bytes(base, 0, Self::DATA_OFFSET);
        Ring {
            hdr: base as *const Header,
            data: base.add(Self::DATA_OFFSET),
            cap,
        }
    }

    /// Producer side: attach to a region the consumer already initialised.
    ///
    /// # Safety
    /// Same contract as [`Ring::init`]; the header must already be initialised by
    /// the consumer.
    pub unsafe fn attach(base: *mut u8, cap: usize) -> Ring {
        debug_assert!(cap.is_power_of_two());
        Ring {
            hdr: base as *const Header,
            data: base.add(Self::DATA_OFFSET),
            cap,
        }
    }

    #[inline]
    fn hdr(&self) -> &Header {
        // SAFETY: `hdr` points at a live, initialised Header for the handle's life.
        unsafe { &*self.hdr }
    }

    /// Producer: write every byte of `buf`, blocking (with backoff) whenever the
    /// ring is full. Handles `buf` larger than the ring by looping.
    pub fn produce(&self, mut buf: &[u8]) {
        let h = self.hdr();
        let mask = self.cap - 1;
        let mut backoff = Backoff::new();
        while !buf.is_empty() {
            let r = h.read_pos.load(Acquire);
            // We are the only writer, so a relaxed load of our own cursor is fine.
            let w = h.write_pos.load(Relaxed);
            let free = self.cap - (w.wrapping_sub(r) as usize);
            if free == 0 {
                backoff.snooze();
                continue;
            }
            backoff.reset();
            let off = (w as usize) & mask;
            let contig = self.cap - off; // bytes until the wrap point
            let n = buf.len().min(free).min(contig);
            // SAFETY: [off, off+n) is in-bounds (n <= contig) and currently free
            // (n <= free), so the consumer is not reading it.
            unsafe {
                std::ptr::copy_nonoverlapping(buf.as_ptr(), self.data.add(off), n);
            }
            // Release so the consumer's Acquire load of write_pos sees the bytes.
            h.write_pos.store(w.wrapping_add(n as u64), Release);
            buf = &buf[n..];
        }
    }

    /// Producer: publish a clean end-of-stream. No `produce` may follow.
    pub fn finish(&self) {
        self.hdr().done.store(1, Release);
    }

    /// Producer (or parent watchdog): publish an abnormal end-of-stream.
    pub fn fail(&self) {
        self.hdr().error.store(1, Relaxed);
        self.hdr().done.store(1, Release);
    }

    /// Consumer: borrow the next contiguous run of readable bytes, blocking (with
    /// backoff) for the producer. Returns `None` once the stream is fully drained
    /// and the producer is done. The borrow is valid until [`Ring::consume`].
    pub fn read_slice(&self) -> Option<&[u8]> {
        let h = self.hdr();
        let mask = self.cap - 1;
        // We are the only reader, so a relaxed load of our own cursor is fine.
        let r = h.read_pos.load(Relaxed);
        let mut backoff = Backoff::new();
        loop {
            let w = h.write_pos.load(Acquire);
            let avail = w.wrapping_sub(r) as usize;
            if avail > 0 {
                let off = (r as usize) & mask;
                let n = avail.min(self.cap - off); // stop at the wrap point
                // SAFETY: [off, off+n) was published by the producer (Acquire
                // pairs with its Release) and is ours until we advance read_pos.
                return Some(unsafe { std::slice::from_raw_parts(self.data.add(off), n) });
            }
            // No data. If the producer is done, re-check write_pos under the same
            // Acquire that observed `done` — once done is visible, so is the final
            // write_pos — and only then conclude the stream is truly empty.
            if h.done.load(Acquire) != 0 {
                if h.write_pos.load(Acquire).wrapping_sub(r) == 0 {
                    return None;
                }
                continue;
            }
            backoff.snooze();
        }
    }

    /// Consumer: release `n` bytes returned by the preceding [`Ring::read_slice`].
    pub fn consume(&self, n: usize) {
        let h = self.hdr();
        let r = h.read_pos.load(Relaxed);
        // Release so the producer's Acquire load of read_pos sees the free space.
        h.read_pos.store(r.wrapping_add(n as u64), Release);
    }

    /// Whether the producer ended abnormally.
    pub fn errored(&self) -> bool {
        self.hdr().error.load(Acquire) != 0
    }
}

/// Spin → yield backoff for the SPSC wait loops. Spins (doubling) for the first
/// few rounds to win the low-latency case, then yields the core so a full ring /
/// empty ring doesn't peg a CPU.
struct Backoff(u32);

impl Backoff {
    fn new() -> Self {
        Backoff(0)
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn snooze(&mut self) {
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
