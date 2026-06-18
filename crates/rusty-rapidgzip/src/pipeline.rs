//! Parallel speculative decode pipeline.
//!
//! Three roles, all running concurrently:
//!
//! 1. **Boundary finder** (main thread, before workers start): scans the
//!    compressed body for dynamic-Huffman block boundaries at roughly
//!    `chunk_size_bytes` intervals. Produces a list of bit offsets.
//! 2. **Workers**: each takes one `(chunk_id, start_bit, end_bit_hint)` and
//!    decodes blocks until `end_bit_hint`
//!    is reached or BFINAL is seen. Emits a `SpeculativeChunk`.
//! 3. **Serializer**: receives chunks out of order, buffers them in a
//!    `BTreeMap`, and processes them in `chunk_id` order — resolves markers
//!    against the running 32 KiB tail, then streams resolved bytes to the
//!    caller's sink. Also validates CRC32 + ISIZE in the trailer when the
//!    final block is reached.
//!
//! ## Bounds & assumptions
//!
//! - Handles multi-member gzip directly: at each BFINAL the worker consumes
//!   the 8-byte trailer + the next gzip header and continues decoding into
//!   the next member, recording a [`MemberBoundary`] at each transition. The
//!   serializer keeps a running CRC32 + uncompressed counter per member and
//!   validates the trailer at every boundary.
//! - If the block finder fails to locate enough internal boundaries, we
//!   silently degrade to fewer (or one) chunks. A degenerate decode is
//!   serial-equivalent.
//! - Correctness does **not** rest on the block finder never producing a false
//!   boundary. The serializer (stage A) runs an *anchored, self-verifying
//!   chain*: chunk 0 starts at the true first block, and every later chunk's
//!   speculative `start_bit` is checked against the previous chunk's
//!   `actual_end_bit`. A mismatch — or a speculative decode that errored —
//!   means the guessed start was false, so that chunk is re-decoded from the
//!   known-correct offset (counted as a `speculation_failure`). The block
//!   finder's full-header verification keeps such re-decodes rare, so this is a
//!   performance cost, not a correctness risk. (This mirrors rapidgzip-cpp's
//!   `ChunkData::matchesEncodedOffset` + re-decode fallback.) The per-member
//!   CRC32 remains as an independent integrity check on top.

use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};

/// Compressed input backing store. Either an mmap'd file (production) or a
/// heap buffer (tests / small inputs). Both deref to `&[u8]`.
#[derive(Debug)]
pub enum InputBytes {
    Owned(Vec<u8>),
    Mapped(memmap2::Mmap),
}

impl InputBytes {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            InputBytes::Owned(v) => v,
            InputBytes::Mapped(m) => m,
        }
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
}

impl std::ops::Deref for InputBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

use crate::deflate::{
    fast_inflate, find_next_dynamic_block, resolve_markers, BitReader, SpeculativeChunk,
};

use crate::{Config, Error};

/// Per-worker kernel state: a reusable `u16` dense-window scratch buffer for
/// `fast_inflate`, reused across chunks so its pages stay faulted.
#[derive(Default)]
struct WorkerKernel {
    scratch: Vec<u16>,
}

impl WorkerKernel {
    fn new() -> Self {
        Self::default()
    }

    fn decode_until(
        &mut self,
        input: &[u8],
        start_bit: u64,
        end_bit_hint: u64,
        chunk: &mut SpeculativeChunk,
    ) -> Result<(u64, bool), crate::deflate::DeflateError> {
        fast_inflate::decode_until_u16(input, start_bit, end_bit_hint, chunk, &mut self.scratch)
    }
}

/// Dispatch-side back-pressure on the number of *decoded chunks in flight*.
///
/// A decoded chunk carries its full `bytes` (≈3–4× the compressed chunk) plus a
/// `markers` vector; with a slow consumer the pipeline otherwise runs as far
/// ahead as the summed channel capacities allow (~`8 × num_threads` chunks
/// spread across the result / resolve / done / crc stages), and that headroom —
/// not the active worker count — is what fixes peak RSS. This counting
/// semaphore caps outstanding chunks: dispatch [`acquire`]s a slot before
/// sending each work item, and the output stage [`release`]s it once the chunk
/// has been streamed to the sink.
///
/// On by default at [`INFLIGHT_DEFAULT_FACTOR`] chunks per worker (floored by
/// [`INFLIGHT_FLOOR`]); `RAPIDGZIP_INFLIGHT=<factor>` overrides the factor and
/// `RAPIDGZIP_INFLIGHT=0` disables the cap entirely (the `Option` becomes
/// `None`, so there is zero added lock traffic).
///
/// [`acquire`]: InflightCap::acquire
/// [`release`]: InflightCap::release
struct InflightCap {
    inner: Mutex<InflightState>,
    cv: Condvar,
}

struct InflightState {
    in_flight: usize,
    max: usize,
    shutdown: bool,
}

impl InflightCap {
    fn new(max: usize) -> Self {
        Self {
            inner: Mutex::new(InflightState {
                in_flight: 0,
                max: max.max(1),
                shutdown: false,
            }),
            cv: Condvar::new(),
        }
    }

    /// Block until a slot is free, then claim it. Returns `false` if the cap was
    /// shut down while waiting (teardown) — the caller should stop dispatching.
    fn acquire(&self) -> bool {
        let mut g = self.inner.lock().expect("inflight-cap mutex poisoned");
        loop {
            if g.shutdown {
                return false;
            }
            if g.in_flight < g.max {
                g.in_flight += 1;
                return true;
            }
            g = self.cv.wait(g).expect("inflight-cap mutex poisoned");
        }
    }

    /// Return one slot and wake a waiting dispatcher.
    fn release(&self) {
        let mut g = self.inner.lock().expect("inflight-cap mutex poisoned");
        g.in_flight = g.in_flight.saturating_sub(1);
        drop(g);
        self.cv.notify_one();
    }

    /// Release a dispatcher blocked in [`acquire`] during teardown.
    fn shutdown(&self) {
        let mut g = self.inner.lock().expect("inflight-cap mutex poisoned");
        g.shutdown = true;
        drop(g);
        self.cv.notify_all();
    }
}

/// Absolute floor on the in-flight cap. The serial stage-A tail-chain + resolve
/// pool need a minimum lookahead to stay fed; below ~this the pipeline starves
/// at low thread counts (measured: `2×P` throttled P=2 ~6%). The floor only ever
/// *raises* the cap, so it costs nothing at high N — where `N×factor` dominates
/// and the runaway (~8N buffered chunks) actually lives.
const INFLIGHT_FLOOR: usize = 16;

/// Default in-flight chunks per worker. 2 is the measured sweet spot: ~−50% peak
/// RSS under a slow consumer with the throughput path unaffected at every P.
const INFLIGHT_DEFAULT_FACTOR: usize = 2;

/// Configured in-flight factor: `Some(f)` ⇒ cap outstanding chunks at
/// `max(N·f, INFLIGHT_FLOOR)`; `None` ⇒ cap disabled. On by default; override
/// with `RAPIDGZIP_INFLIGHT` (`0` disables, any other value sets the factor,
/// garbage falls back to the default).
fn inflight_factor() -> Option<usize> {
    match std::env::var("RAPIDGZIP_INFLIGHT") {
        Ok(s) => match s.parse::<usize>() {
            Ok(0) => None,
            Ok(f) => Some(f),
            Err(_) => Some(INFLIGHT_DEFAULT_FACTOR),
        },
        Err(_) => Some(INFLIGHT_DEFAULT_FACTOR),
    }
}

/// Build the cap for `num_threads` workers from a configured `factor`.
fn make_inflight_cap(num_threads: usize, factor: Option<usize>) -> Option<Arc<InflightCap>> {
    factor.map(|f| Arc::new(InflightCap::new((num_threads * f).max(INFLIGHT_FLOOR))))
}

/// One unit of speculative decode work.
struct WorkItem {
    id: u64,
    start_bit: u64,
    /// Exclusive upper bound on `tell_bit()` after each block. The worker
    /// stops when `tell_bit() >= end_bit_hint` after completing a block, or
    /// when BFINAL is reached. `u64::MAX` for the last chunk.
    end_bit_hint: u64,
}

struct WorkResult {
    id: u64,
    chunk: SpeculativeChunk,
    /// Set when the worker reached real EOF after the last member's trailer.
    final_block: bool,
    /// Member transitions encountered while decoding this chunk. Each entry
    /// records where (in `chunk.bytes`) one member ends and the next begins,
    /// plus the trailer (CRC32 / ISIZE) for the just-ended member.
    member_boundaries: Vec<MemberBoundary>,
    /// Absolute bit offset in `body` where decoding stopped — i.e. the start of
    /// the next real deflate block (or EOF for the final chunk). Used by the
    /// serializer to verify that the *next* chunk's speculative `start_bit`
    /// really is a block boundary (see [`parallel_decode_member`]).
    actual_end_bit: u64,
}

/// A worker's output for one [`WorkItem`], carrying the item's offsets even on
/// the error path. The serializer consumes these in `id` order and verifies
/// each chunk's speculative `start_bit` against the previous chunk's
/// `actual_end_bit`. A `start_bit` that doesn't line up — or a speculative
/// decode that *errored* — means the block finder produced a false boundary;
/// the serializer re-decodes that chunk from the known-correct offset. This is
/// what makes correctness independent of the block finder never false-positiving
/// (the CRC32 in each trailer is then only a redundant integrity check).
struct WorkOutcome {
    id: u64,
    start_bit: u64,
    end_bit_hint: u64,
    result: Result<WorkResult, Error>,
}

#[derive(Debug, Clone)]
struct MemberBoundary {
    /// Byte offset in `chunk.bytes` where the just-ended member's decoded
    /// output ends. Stable across marker resolution.
    byte_offset_in_chunk: usize,
    crc_expected: u32,
    isize_expected: u32,
    /// Byte offset of the trailer in `body`. For error messages.
    trailer_input_byte: u64,
}

/// A chunk handed to the resolve pool: its still-unresolved markers plus the
/// (already fully-resolved) previous-chunk tail they reference.
struct ResolveJob {
    id: u64,
    chunk: SpeculativeChunk,
    prev_tail: Arc<Vec<u8>>,
    final_block: bool,
    member_boundaries: Vec<MemberBoundary>,
}

/// A chunk whose markers have been resolved in place, ready to stream in order.
struct ResolveDone {
    id: u64,
    chunk: SpeculativeChunk,
    final_block: bool,
    member_boundaries: Vec<MemberBoundary>,
}

/// Decode the deflate body of one or more concatenated gzip members in
/// parallel.
///
/// `body` is the slice from end-of-first-gzip-header to end-of-file. The
/// caller has parsed and stripped only the *first* member's header; this
/// function handles every member's trailer, every subsequent member's
/// header, and validates CRC32 + ISIZE for each member as it streams.
///
/// On success, returns `(total_uncompressed_bytes, body_bytes_consumed,
/// chunks_sent, speculation_failures)` where `chunks_sent` is the number of
/// `Vec<u8>` chunks pushed onto `sink` and `speculation_failures` counts chunks
/// the serializer had to re-decode because the block finder's guessed start was
/// a false boundary (normally 0).
pub fn parallel_decode_member(
    input: Arc<InputBytes>,
    body_offset: usize,
    sink: &Sender<Arc<Vec<u8>>>,
    config: &Config,
) -> Result<(u64, usize, u64, u64), Error> {
    // Inside this function `body` is the deflate body — the slice of `input`
    // starting at `body_offset`. Workers receive the full Arc + the offset
    // so they don't need to know about the gzip header layer.
    let body: &[u8] = &input.as_slice()[body_offset..];
    let num_threads = effective_threads(config);
    let verbose = config.verbose.is_on();

    // Single worker: there is no parallelism to gain from speculation, so skip
    // the boundary scan / marker machinery entirely and decode serially with a
    // real 32 KiB window (plain u8 output, no marker-window overhead).
    if num_threads == 1 {
        // Serial path has no speculation, hence no false-boundary recovery.
        let (uc, bc, ch) = serial_decode_member(input, body_offset, sink, config)?;
        return Ok((uc, bc, ch, 0));
    }

    let recycle_rx = config.recycle_rx.clone();
    let recycle_tx_for_crc = config.recycle_tx.clone();
    let total_bits = (body.len() as u64) * 8;
    let chunk_bits = (config.chunk_size_bytes as u64).max(64 * 1024) * 8;

    // Find block boundaries in parallel. Pick fixed target offsets every
    // `chunk_bits` and search each in parallel; each search is local and
    // independent — the only post-step is dedup + sort.
    let t_scan = std::time::Instant::now();
    let mut boundaries: Vec<u64> = vec![0];
    {
        let mut targets: Vec<u64> = Vec::new();
        let mut c = chunk_bits;
        while c < total_bits {
            targets.push(c);
            c += chunk_bits;
        }
        if !targets.is_empty() {
            let body_ref = body;
            let found: Vec<Option<u64>> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(targets.len().min(num_threads));
                // Split targets across `num_threads` workers in round-robin.
                let n = num_threads.min(targets.len()).max(1);
                let chunks: Vec<Vec<usize>> = (0..n)
                    .map(|i| (i..targets.len()).step_by(n).collect())
                    .collect();
                for indices in chunks {
                    let targets = &targets;
                    handles.push(s.spawn(move || {
                        indices
                            .into_iter()
                            .map(|i| {
                                let t = targets[i];
                                (i, find_next_dynamic_block(body_ref, t, total_bits))
                            })
                            .collect::<Vec<_>>()
                    }));
                }
                let mut out: Vec<Option<u64>> = vec![None; targets.len()];
                for h in handles {
                    for (i, b) in h.join().expect("block-finder worker thread panicked") {
                        out[i] = b;
                    }
                }
                out
            });
            for b in found.into_iter().flatten() {
                if b > *boundaries
                    .last()
                    .expect("boundaries seeded with [0], never empty")
                {
                    boundaries.push(b);
                }
            }
        }
    }
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] pipeline: scanned in {:.3}s",
            crate::elapsed_since_start(),
            t_scan.elapsed().as_secs_f64(),
        );
    }

    // Build work items. The last item has no end_bit_hint (decode until BFINAL).
    let mut work_items: Vec<WorkItem> = Vec::with_capacity(boundaries.len());
    for i in 0..boundaries.len() {
        let start_bit = boundaries[i];
        let end_bit_hint = boundaries.get(i + 1).copied().unwrap_or(u64::MAX);
        work_items.push(WorkItem {
            id: i as u64,
            start_bit,
            end_bit_hint,
        });
    }
    let num_chunks = work_items.len();
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] pipeline: {} boundaries found → {num_chunks} chunk(s), {num_threads} worker(s)",
            crate::elapsed_since_start(),
            boundaries.len(),
        );
        if num_chunks <= 1 {
            eprintln!(
                "[rapidgzip +{:.2}s] pipeline: only one chunk — parallel path degrades to serial-equivalent decode",
                crate::elapsed_since_start(),
            );
        }
    }

    // Channels.
    let (work_tx, work_rx) = bounded::<WorkItem>(num_threads * 2);
    let (result_tx, result_rx) = bounded::<WorkOutcome>(num_threads * 2);

    // Spawn the decode pool. All `num_threads` workers are spawned up front and
    // stay alive for the whole decode, pulling from the work channel until it
    // drains. Because the pool is fully spawned here, there are no channel clones
    // to retain for spawning later, so the eager drops below stay.
    let mut worker_handles = Vec::with_capacity(num_threads);
    for _index in 0..num_threads {
        let input = Arc::clone(&input);
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let recycle_rx = recycle_rx.clone();
        let handle = thread::spawn(move || {
            let body = &input.as_slice()[body_offset..];
            let mut wk = WorkerKernel::new();
            loop {
                let Ok(item) = work_rx.recv() else {
                    // Work channel drained + dispatch done → decode is finished.
                    return;
                };
                // Try to pull a recycled output buffer; pages on it are
                // warm so subsequent writes don't take page faults.
                let recycled = recycle_rx.as_ref().and_then(|r| r.try_recv().ok());
                let result = decode_one_chunk(body, &item, &mut wk, recycled);
                let outcome = WorkOutcome {
                    id: item.id,
                    start_bit: item.start_bit,
                    end_bit_hint: item.end_bit_hint,
                    result,
                };
                if result_tx.send(outcome).is_err() {
                    return;
                }
            }
        });
        worker_handles.push(handle);
    }
    drop(work_rx);
    drop(result_tx);

    // ── In-flight chunk cap (default-on) ─────────────────────────────────────
    //
    // Bounds the number of decoded chunks outstanding between dispatch and the
    // sink (see [`InflightCap`]). On by default; `RAPIDGZIP_INFLIGHT=0` disables
    // it. The factor is "chunks per worker".
    let inflight = make_inflight_cap(num_threads, inflight_factor());

    // Dispatch work — send all items. The bounded channel applies backpressure;
    // the optional in-flight cap additionally throttles how many decoded chunks
    // may be outstanding at once (acquire here, released at the sink).
    let dispatch = {
        let inflight = inflight.clone();
        thread::spawn(move || {
            for item in work_items {
                if let Some(cap) = &inflight {
                    if !cap.acquire() {
                        return; // cap shut down during teardown
                    }
                }
                if work_tx.send(item).is_err() {
                    return;
                }
            }
        })
    };

    // ── CRC validator thread ─────────────────────────────────────────────────
    //
    // Receives fully-resolved chunks in order from the serializer, validates
    // CRC32 + ISIZE off the hot path, and recycles the output buffer.
    let (crc_tx, crc_rx) = bounded::<(Arc<Vec<u8>>, Vec<MemberBoundary>)>(num_threads * 2);
    let crc_handle: thread::JoinHandle<Result<(), Error>> = thread::spawn(move || {
        let mut cur_crc = crc32fast::Hasher::new();
        let mut cur_uncompressed: u64 = 0;
        let mut member_idx: u32 = 0;
        for (bytes_arc, member_boundaries) in crc_rx {
            let bytes: &[u8] = &bytes_arc;
            let mut cursor = 0usize;
            for mb in &member_boundaries {
                let piece = &bytes[cursor..mb.byte_offset_in_chunk];
                cur_crc.update(piece);
                cur_uncompressed += piece.len() as u64;
                let crc_got = std::mem::replace(&mut cur_crc, crc32fast::Hasher::new()).finalize();
                if crc_got != mb.crc_expected {
                    return Err(Error::Gzip(crate::GzipError::CrcMismatch {
                        expected: mb.crc_expected,
                        got: crc_got,
                        member: member_idx,
                        uncompressed: cur_uncompressed,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                }
                if (cur_uncompressed & 0xFFFF_FFFF) as u32 != mb.isize_expected {
                    return Err(Error::Gzip(crate::GzipError::IsizeMismatch {
                        expected: mb.isize_expected,
                        got: cur_uncompressed,
                        member: member_idx,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                }
                member_idx += 1;
                cur_uncompressed = 0;
                cursor = mb.byte_offset_in_chunk;
            }
            let tail = &bytes[cursor..];
            cur_crc.update(tail);
            cur_uncompressed += tail.len() as u64;
            drop(member_boundaries);
            if let (Some(tx), Ok(mut v)) = (recycle_tx_for_crc.as_ref(), Arc::try_unwrap(bytes_arc))
            {
                v.clear();
                let _ = tx.try_send(v);
            }
        }
        Ok(())
    });

    // ── Resolve fan-out ──────────────────────────────────────────────────────
    //
    // Marker resolution used to run inline in the (serial) serializer and, on
    // FASTQ at high thread counts, became the Amdahl bottleneck (~0.5s of serial
    // work patching ~800M markers). Only the 32 KiB tail of each chunk is a true
    // serial dependency: `build_prev_tail_fast` must resolve chunk N-1's tail
    // before chunk N's markers (which reference it) can resolve. So we split:
    //
    //   • Stage A (spawned, serial, cheap): reorder decode results by id, run
    //     the `build_prev_tail_fast` chain, hand each chunk + the prev_tail it
    //     needs to the resolve pool.
    //   • Resolve pool (parallel): `resolve_markers` patches the chunk's bulk
    //     markers in place — fully independent across chunks.
    //   • Output (this thread): reorder resolved chunks by id, stream to CRC +
    //     sink in order.
    let body_len = body.len();
    // Marker resolution is memory-bound (scattered writes), so a small pool
    // saturates bandwidth; more just oversubscribes the decode workers. 4 is
    // the measured sweet spot on a 16-core/32-thread box (P32 matches/beats
    // C++). Override with RAPIDGZIP_RESOLVE_THREADS for other machines.
    let resolve_threads = std::env::var("RAPIDGZIP_RESOLVE_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| num_threads.min(4))
        .max(1);
    let (resolve_tx, resolve_rx) = bounded::<ResolveJob>(num_threads * 2);
    let (done_tx, done_rx) = bounded::<Result<ResolveDone, Error>>(num_threads * 2);

    let n_markers = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let resolve_ns = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Resolve pool — each chunk's markers resolve against its own prev_tail, so
    // jobs are independent and need no ordering here.
    let mut resolve_handles = Vec::with_capacity(resolve_threads);
    for _ in 0..resolve_threads {
        let resolve_rx = resolve_rx.clone();
        let done_tx = done_tx.clone();
        let n_markers = Arc::clone(&n_markers);
        let resolve_ns = Arc::clone(&resolve_ns);
        resolve_handles.push(thread::spawn(move || {
            use std::sync::atomic::Ordering::Relaxed;
            while let Ok(mut job) = resolve_rx.recv() {
                n_markers.fetch_add(job.chunk.markers.len() as u64, Relaxed);
                let t0 = std::time::Instant::now();
                let r = resolve_markers(&mut job.chunk, &job.prev_tail);
                resolve_ns.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
                let msg = r.map(|()| ResolveDone {
                    id: job.id,
                    chunk: job.chunk,
                    final_block: job.final_block,
                    member_boundaries: job.member_boundaries,
                });
                if done_tx.send(msg.map_err(Error::Deflate)).is_err() {
                    return;
                }
            }
        }));
    }
    drop(resolve_rx);

    // Stage A — serial tail-chain + dispatch. Owns `result_rx`. `done_tx_err`
    // forwards decode/channel errors straight to the output stage.
    let done_tx_err = done_tx.clone();
    drop(done_tx);
    let stage_a = {
        let num_chunks = num_chunks as u64;
        let input_a = Arc::clone(&input);
        // Returns the number of chunks that had to be re-decoded because the
        // block finder's guessed start was a false boundary.
        thread::spawn(move || -> u64 {
            let body = &input_a.as_slice()[body_offset..];
            let mut redecode_kernel = WorkerKernel::new();
            let mut reorder: std::collections::BTreeMap<u64, WorkOutcome> =
                std::collections::BTreeMap::new();
            let mut next_id: u64 = 0;
            let mut prev_tail: Vec<u8> = Vec::new();
            // Anchored, self-verifying chain: chunk 0 always starts at the true
            // first block (bit 0, immediately after the stripped gzip header).
            // Every later chunk's speculative `start_bit` must equal the previous
            // chunk's `actual_end_bit`; otherwise the guessed boundary was false
            // and we re-decode from the real offset. This removes the dependence
            // on the finder never false-positiving — see [`block_finder`] and the
            // gzip-of-gzip test in `tests/nested_gzip.rs`.
            let mut expected_start: u64 = 0;
            let mut spec_failures: u64 = 0;
            while next_id < num_chunks {
                while let Some(outcome) = reorder.remove(&next_id) {
                    let WorkOutcome {
                        id,
                        start_bit,
                        end_bit_hint,
                        result,
                    } = outcome;
                    debug_assert_eq!(id, next_id);
                    // Accept the speculative result only if it decoded cleanly
                    // *and* started exactly where the chain expects. Otherwise
                    // re-decode from the known-correct offset (`expected_start`,
                    // a true boundary). A genuine error at the correct offset
                    // reproduces here and aborts the decode.
                    let res = match result {
                        Ok(r) if start_bit == expected_start => r,
                        wrong => {
                            // `wrong` is either a clean decode from a false start
                            // (discard its bytes) or a decode error; both recover
                            // the same way.
                            drop(wrong);
                            spec_failures += 1;
                            let corrected = WorkItem {
                                id,
                                start_bit: expected_start,
                                end_bit_hint,
                            };
                            match decode_one_chunk(body, &corrected, &mut redecode_kernel, None) {
                                Ok(r) => r,
                                Err(e) => {
                                    let _ = done_tx_err.send(Err(e));
                                    return spec_failures;
                                }
                            }
                        }
                    };
                    expected_start = res.actual_end_bit;
                    // Resolve only this chunk's tail to feed the next chunk; the
                    // bulk resolve happens in the pool against `prev_arc`.
                    let new_tail = build_prev_tail_fast(&res.chunk, &prev_tail);
                    let prev_arc = Arc::new(std::mem::replace(&mut prev_tail, new_tail));
                    let saw_final = res.final_block;
                    let job = ResolveJob {
                        id: res.id,
                        chunk: res.chunk,
                        prev_tail: prev_arc,
                        final_block: res.final_block,
                        member_boundaries: res.member_boundaries,
                    };
                    if resolve_tx.send(job).is_err() {
                        return spec_failures;
                    }
                    next_id += 1;
                    if saw_final {
                        return spec_failures;
                    }
                }
                match result_rx.recv() {
                    Ok(outcome) => {
                        reorder.insert(outcome.id, outcome);
                    }
                    Err(_) => {
                        let _ = done_tx_err.send(Err(Error::Io(std::io::Error::other(
                            "worker channel closed",
                        ))));
                        return spec_failures;
                    }
                }
            }
            spec_failures
        })
    };

    // ── Output stage (in order) ──────────────────────────────────────────────
    let mut reorder: std::collections::BTreeMap<u64, ResolveDone> =
        std::collections::BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut total_uc: u64 = 0;
    let mut sent: u64 = 0;
    let mut final_byte: Option<usize> = None;
    let mut last_err: Option<Error> = None;
    let mut sink_closed = false;

    'outer: while next_id < num_chunks as u64 {
        while let Some(done) = reorder.remove(&next_id) {
            total_uc += done.chunk.bytes.len() as u64;
            let saw_final = done.final_block;
            if saw_final {
                if let Some(mb) = done.member_boundaries.last() {
                    final_byte = Some(mb.trailer_input_byte as usize + 8);
                }
            }
            let bytes_arc = Arc::new(done.chunk.bytes);
            let _ = crc_tx.send((Arc::clone(&bytes_arc), done.member_boundaries));
            let send_ok = sink.send(bytes_arc).is_ok();
            // Chunk has left the decode pipeline — free its in-flight slot so
            // dispatch may admit the next one. (Release whether or not the sink
            // accepted it; on close we tear down and abort dispatch anyway.)
            if let Some(cap) = &inflight {
                cap.release();
            }
            if !send_ok {
                sink_closed = true;
                break 'outer;
            }
            sent += 1;
            next_id += 1;
            if saw_final {
                break 'outer;
            }
        }
        if next_id >= num_chunks as u64 {
            break;
        }
        match done_rx.recv() {
            Ok(Ok(d)) => {
                reorder.insert(d.id, d);
            }
            Ok(Err(e)) => {
                last_err = Some(e);
                break 'outer;
            }
            Err(_) => {
                last_err = Some(Error::Io(std::io::Error::other("resolve channel closed")));
                break 'outer;
            }
        }
    }
    if verbose {
        use std::sync::atomic::Ordering::Relaxed;
        eprintln!(
            "[rapidgzip +{:.2}s] resolve fan-out: {sent} chunks | {} markers | resolve_cpu={:.3}s across {resolve_threads} threads",
            crate::elapsed_since_start(),
            n_markers.load(Relaxed),
            resolve_ns.load(Relaxed) as f64 / 1e9,
        );
    }

    // Tear down. Dropping `done_rx` unblocks the pool if we exited early; the
    // channel disconnects then cascade upstream (pool → stage A → workers →
    // dispatch), so the joins below complete.
    drop(crc_tx);
    drop(done_rx);
    // Wake `dispatch` if it's parked in `InflightCap::acquire` (e.g. we broke at
    // the final block before processing every dispatched item) so it can exit.
    if let Some(cap) = &inflight {
        cap.shutdown();
    }
    let spec_failures = stage_a.join().unwrap_or(0);
    for h in resolve_handles {
        let _ = h.join();
    }
    let _ = dispatch.join();
    for h in worker_handles {
        let _ = h.join();
    }
    if verbose && spec_failures > 0 {
        eprintln!(
            "[rapidgzip +{:.2}s] block-finder false boundaries: {spec_failures} chunk(s) re-decoded from the correct offset",
            crate::elapsed_since_start(),
        );
    }

    if sink_closed {
        return Ok((total_uc, body_len, sent, spec_failures));
    }
    if let Some(e) = last_err {
        return Err(e);
    }

    crc_handle
        .join()
        .unwrap_or_else(|_| Err(Error::Io(std::io::Error::other("crc thread panicked"))))?;

    let bc = final_byte.ok_or_else(|| {
        Error::Io(std::io::Error::other(
            "parallel decode produced no final block",
        ))
    })?;
    Ok((total_uc, bc, sent, spec_failures))
}

/// Build the next chunk's prev_tail by applying only the markers that land in
/// the last 32 KiB of this chunk's output.
pub(crate) fn build_prev_tail_fast(chunk: &SpeculativeChunk, prev_tail: &[u8]) -> Vec<u8> {
    const WINDOW: usize = 32 * 1024;
    let len = chunk.bytes.len();
    let tail_start = len.saturating_sub(WINDOW);
    let first = chunk
        .markers
        .partition_point(|m| (m.out_pos as usize) < tail_start);
    let mut new_tail = chunk.bytes[tail_start..].to_vec();
    for m in &chunk.markers[first..] {
        let pos = m.out_pos as usize;
        if pos >= tail_start + new_tail.len() {
            break;
        }
        let dst = pos - tail_start;
        let off = m.prefix_offset as usize;
        if off < prev_tail.len() {
            new_tail[dst] = prev_tail[prev_tail.len() - 1 - off];
        }
    }
    // A chunk shorter than the window (e.g. an empty chunk produced when a
    // false-boundary re-decode is absorbed by the previous chunk, or a tiny
    // final chunk) does not by itself hold a full 32 KiB window. Carry the
    // missing prefix from the previous tail so the next chunk's back-references
    // still resolve. For the common case (len >= WINDOW) this is a no-op.
    if len < WINDOW && !prev_tail.is_empty() {
        let need = WINDOW - len;
        let carry = &prev_tail[prev_tail.len().saturating_sub(need)..];
        let mut combined = Vec::with_capacity(carry.len() + new_tail.len());
        combined.extend_from_slice(carry);
        combined.extend_from_slice(&new_tail);
        return combined;
    }
    new_tail
}

fn decode_one_chunk(
    body: &[u8],
    item: &WorkItem,
    wk: &mut WorkerKernel,
    recycled_bytes: Option<Vec<u8>>,
) -> Result<WorkResult, Error> {
    let mut br = BitReader::new(body);
    br.seek_to_bit(item.start_bit).map_err(Error::Deflate)?;
    let mut chunk = SpeculativeChunk::default();
    if let Some(mut v) = recycled_bytes {
        v.clear();
        chunk.bytes = v;
    }
    // Optimistic pre-allocate: assume ~3x compression ratio. Worst case
    // (incompressible / stored blocks) we over-reserve harmlessly; common
    // case avoids the cascade of doublings.
    let span_bits = item.end_bit_hint.saturating_sub(item.start_bit);
    if span_bits != u64::MAX && span_bits > 0 {
        let est_output = ((span_bits / 8) as usize).saturating_mul(3);
        chunk.reserve_bytes(est_output.min(64 * 1024 * 1024));
    }
    let mut final_block = false;
    let mut member_boundaries: Vec<MemberBoundary> = Vec::new();

    loop {
        let pos = br.tell_bit();
        if pos >= item.end_bit_hint {
            break;
        }

        let (new_end, hit_bfinal) = wk
            .decode_until(body, pos, item.end_bit_hint, &mut chunk)
            .map_err(Error::Deflate)?;
        br.seek_to_bit(new_end).map_err(Error::Deflate)?;

        if !hit_bfinal {
            // Stopped at a block boundary at/past end_bit_hint — done.
            break;
        }

        // Final block of a member — byte-align and read the 8-byte trailer.
        br.byte_align();
        let after_bit = br.tell_bit();
        debug_assert_eq!(after_bit % 8, 0);
        let trailer_byte = (after_bit / 8) as usize;
        if trailer_byte + 8 > body.len() {
            return Err(Error::Gzip(crate::GzipError::Truncated));
        }
        let crc_expected = u32::from_le_bytes(
            body[trailer_byte..trailer_byte + 4]
                .try_into()
                .expect("4-byte slice (trailer_byte + 8 bounds-checked above)"),
        );
        let isize_expected = u32::from_le_bytes(
            body[trailer_byte + 4..trailer_byte + 8]
                .try_into()
                .expect("4-byte slice (trailer_byte + 8 bounds-checked above)"),
        );
        member_boundaries.push(MemberBoundary {
            byte_offset_in_chunk: chunk.bytes.len(),
            crc_expected,
            isize_expected,
            trailer_input_byte: trailer_byte as u64,
        });

        let after_trailer = trailer_byte + 8;
        if after_trailer >= body.len() {
            // Real EOF — the very last member has been consumed.
            final_block = true;
            br.seek_to_bit((after_trailer as u64) * 8)
                .map_err(Error::Deflate)?;
            break;
        }

        // Another member follows. Parse its gzip header and continue.
        let header_len = crate::gzip::parse_header(&body[after_trailer..]).map_err(Error::Gzip)?;
        let next_block_byte = after_trailer + header_len;
        br.seek_to_bit((next_block_byte as u64) * 8)
            .map_err(Error::Deflate)?;
        // Loop: next iteration will inflate the first block of the new member.
    }

    // `br` is now parked at the start of the next real block (the non-final
    // break / top-of-loop break left it on a block boundary) or at EOF (the
    // final-block break sought past the last trailer). Either way this is the
    // exact offset the *next* chunk's speculative start must match.
    let actual_end_bit = br.tell_bit();

    Ok(WorkResult {
        id: item.id,
        chunk,
        final_block,
        member_boundaries,
        actual_end_bit,
    })
}

fn effective_threads(config: &Config) -> usize {
    let n = if config.num_threads == 0 {
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        config.num_threads
    };
    n.max(1)
}

/// Fully serial, marker-less streaming decode for the single-worker case.
///
/// With one thread there is nothing to parallelise, so speculation is pure
/// overhead. Instead we decode the deflate body block-by-block carrying a real
/// 32 KiB window: `fast_inflate::decode_one_block` resolves every back-ref in
/// place and emits plain `u8` output, avoiding the `u16` marker-window cost
/// (double the output bandwidth + the `extract_from_u16` lowering pass + the
/// marker-resolution sweep) the parallel path pays. Output is streamed to
/// `sink` in ~`chunk_size_bytes` pieces while the trailing 32 KiB is retained
/// for subsequent copies, so memory stays bounded regardless of member size.
/// CRC32 + ISIZE are validated inline per member.
///
/// Returns `(total_uncompressed, body_consumed, chunks_sent)`.
fn serial_decode_member(
    input: Arc<InputBytes>,
    body_offset: usize,
    sink: &Sender<Arc<Vec<u8>>>,
    config: &Config,
) -> Result<(u64, usize, u64), Error> {
    // Retained window == max DEFLATE back-ref distance. Keeping exactly the
    // last 32 KiB is enough for every copy source to stay in `out`.
    const SERIAL_WINDOW: usize = 32 * 1024;

    let body: &[u8] = &input.as_slice()[body_offset..];
    let chunk_target = config.chunk_size_bytes.max(64 * 1024);
    let recycle_rx = config.recycle_rx.clone();

    // Pull a recycled buffer (warm pages) or allocate a fresh one, sized for a
    // chunk's worth of decode plus the window carried into it.
    let take_buf = |recycle_rx: &Option<Receiver<Vec<u8>>>| -> Vec<u8> {
        let mut b = recycle_rx
            .as_ref()
            .and_then(|r| r.try_recv().ok())
            .unwrap_or_default();
        b.clear();
        b.reserve(chunk_target + SERIAL_WINDOW + 64 * 1024);
        b
    };

    let mut br = BitReader::new(body);
    // `out` holds `[not-yet-emitted output]`. We decode into it, then hand the
    // whole Vec off to `sink` (no output copy) and carry only the trailing
    // 32 KiB window into a fresh buffer so back-refs in the next chunk resolve.
    let mut out: Vec<u8> = take_buf(&recycle_rx);

    let mut total_uncompressed: u64 = 0;
    let mut chunks_sent: u64 = 0;
    let mut member_idx: u32 = 0;

    // Per-member running CRC + uncompressed byte count. Each output byte is
    // folded into the CRC exactly once, at the moment it is emitted.
    let mut member_crc = crc32fast::Hasher::new();
    let mut member_uncompressed: u64 = 0;

    loop {
        let bfinal = fast_inflate::decode_one_block(&mut br, &mut out).map_err(Error::Deflate)?;

        if !bfinal {
            // Mid-member: emit whole chunks while keeping the 32 KiB window.
            // Hand `out` off by value; seed the next buffer with the window.
            while out.len() >= chunk_target + SERIAL_WINDOW {
                let emit_len = out.len() - SERIAL_WINDOW;
                let mut next = take_buf(&recycle_rx);
                next.extend_from_slice(&out[emit_len..]); // carry 32 KiB window
                out.truncate(emit_len);
                member_crc.update(&out);
                let n = out.len() as u64;
                let chunk_vec = std::mem::replace(&mut out, next);
                if sink.send(Arc::new(chunk_vec)).is_err() {
                    return Ok((total_uncompressed, 0, chunks_sent));
                }
                total_uncompressed += n;
                member_uncompressed += n;
                chunks_sent += 1;
            }
            continue;
        }

        // Final block of this member — flush everything it produced. (Member
        // boundaries reset the window, so nothing here carries into the next
        // member; each emitted chunk belongs to exactly one member.)
        member_crc.update(&out);
        if !out.is_empty() {
            let n = out.len() as u64;
            let next = take_buf(&recycle_rx);
            let chunk_vec = std::mem::replace(&mut out, next);
            if sink.send(Arc::new(chunk_vec)).is_err() {
                return Ok((total_uncompressed, 0, chunks_sent));
            }
            total_uncompressed += n;
            member_uncompressed += n;
            chunks_sent += 1;
        }

        // Byte-align and read the 8-byte trailer.
        br.byte_align();
        let after_bit = br.tell_bit();
        debug_assert_eq!(after_bit % 8, 0);
        let trailer_byte = (after_bit / 8) as usize;
        if trailer_byte + 8 > body.len() {
            return Err(Error::Gzip(crate::GzipError::Truncated));
        }
        let crc_expected = u32::from_le_bytes(
            body[trailer_byte..trailer_byte + 4]
                .try_into()
                .expect("4-byte slice (trailer_byte + 8 bounds-checked above)"),
        );
        let isize_expected = u32::from_le_bytes(
            body[trailer_byte + 4..trailer_byte + 8]
                .try_into()
                .expect("4-byte slice (trailer_byte + 8 bounds-checked above)"),
        );

        let crc_got = std::mem::replace(&mut member_crc, crc32fast::Hasher::new()).finalize();
        if crc_got != crc_expected {
            return Err(Error::Gzip(crate::GzipError::CrcMismatch {
                expected: crc_expected,
                got: crc_got,
                member: member_idx,
                uncompressed: member_uncompressed,
                trailer_byte: (body_offset + trailer_byte) as u64,
            }));
        }
        if (member_uncompressed & 0xFFFF_FFFF) as u32 != isize_expected {
            return Err(Error::Gzip(crate::GzipError::IsizeMismatch {
                expected: isize_expected,
                got: member_uncompressed,
                member: member_idx,
                trailer_byte: (body_offset + trailer_byte) as u64,
            }));
        }

        let after_trailer = trailer_byte + 8;
        member_idx += 1;
        if after_trailer >= body.len() {
            // Real EOF — last member consumed.
            return Ok((total_uncompressed, after_trailer, chunks_sent));
        }

        // Another member follows: parse its header and reset per-member state.
        let header_len = crate::gzip::parse_header(&body[after_trailer..]).map_err(Error::Gzip)?;
        let next_block_byte = after_trailer + header_len;
        br.seek_to_bit((next_block_byte as u64) * 8)
            .map_err(Error::Deflate)?;
        member_uncompressed = 0;
        // `out` is already empty; the new member starts with a fresh window.
    }
}

/// BGZF fast-path pipeline.
///
/// BGZF (bgzip / samtools spec) is gzip with a mandatory `BC` FEXTRA subfield
/// giving each member's size. Every member is an independent deflate stream
/// with no back-refs across boundaries, so we can split the file at known
/// byte offsets and decode each member with the plain serial inflater — no
/// speculation, no marker resolution, no boundary scanning.
///
/// `file` is the entire compressed input (every member has its own header).
/// Returns `(total_uncompressed, chunks_sent)`.
pub fn parallel_decode_bgzf(
    file: Arc<InputBytes>,
    sink: &Sender<Arc<Vec<u8>>>,
    config: &Config,
) -> Result<(u64, u64), Error> {
    use std::collections::BTreeMap;

    let num_threads = effective_threads(config);
    let verbose = config.verbose.is_on();

    // Walk member boundaries up front. The walk stops at the first member that
    // is not a BGZF block; everything before `pos` is decoded here in parallel.
    // A non-BGZF member is not necessarily an error: a file may be BGZF blocks
    // with a plain gzip member concatenated on the end (e.g. `cat a.bgz b.gz`).
    // In that case `pos` marks where the regular member path takes over below.
    let mut members: Vec<(usize, usize)> = Vec::new();
    let mut pos = 0usize;
    while pos < file.len() {
        let Some(size) = crate::gzip::parse_bgzf_block_size(&file[pos..]) else {
            break;
        };
        let size = size as usize;
        if pos + size > file.len() {
            return Err(Error::Gzip(crate::gzip::GzipError::Truncated));
        }
        members.push((pos, pos + size));
        pos += size;
    }
    // Byte offset where the non-BGZF remainder (if any) begins.
    let remainder_offset = pos;

    // Batch members so each work item covers ~chunk_size_bytes of compressed
    // input. With BGZF blocks typically ~16 KiB compressed, a 4 MiB chunk
    // groups ~256 members — enough amortization to keep workers busy. But
    // for small files we'd end up with one giant batch; cap by the total
    // size divided by (num_threads × 4) so every worker gets several batches.
    let total_compressed: usize = members.iter().map(|(s, e)| e - s).sum();
    let per_worker_target = total_compressed / (num_threads * 4).max(1);
    let target = config
        .chunk_size_bytes
        .min(per_worker_target.max(64 * 1024))
        .max(64 * 1024);
    let mut batches: Vec<(usize, usize)> = Vec::new(); // (start_member, end_member) exclusive
    let mut i = 0;
    while i < members.len() {
        let start = i;
        let mut bytes = 0usize;
        while i < members.len() && (bytes == 0 || bytes < target) {
            bytes += members[i].1 - members[i].0;
            i += 1;
        }
        batches.push((start, i));
    }

    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] bgzf: {} members → {} batch(es), {} worker(s)",
            crate::elapsed_since_start(),
            members.len(),
            batches.len(),
            num_threads,
        );
    }

    // Work channel: (batch_id, member_start, member_end_exclusive).
    let (work_tx, work_rx) = bounded::<(u64, usize, usize)>(num_threads * 2);
    let (result_tx, result_rx) = bounded::<Result<(u64, Vec<u8>), Error>>(num_threads * 2);

    let mut workers = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let file = Arc::clone(&file);
        let members_ref: Vec<(usize, usize)> = members.clone();
        workers.push(thread::spawn(move || {
            while let Ok((id, m_start, m_end)) = work_rx.recv() {
                // BGZF blocks are ≤64 KiB uncompressed; preallocate to avoid
                // Vec growth reallocations inside the inflate hot loop.
                let mut out: Vec<u8> = Vec::with_capacity((m_end - m_start) * 65_536);
                let mut err: Option<Error> = None;
                #[expect(
                    clippy::needless_range_loop,
                    reason = "mi is the absolute member index — used both to index members_ref and as the member id passed to decode_one_indexed_fast"
                )]
                for mi in m_start..m_end {
                    let (s, e) = members_ref[mi];
                    let res =
                        crate::gzip::decode_one_indexed_fast(&file[s..e], &mut out, mi as u32);
                    match res {
                        Ok(_) => {}
                        Err(ge) => {
                            err = Some(Error::Gzip(ge));
                            break;
                        }
                    }
                }
                let msg = match err {
                    Some(e) => Err(e),
                    None => Ok((id, out)),
                };
                if result_tx.send(msg).is_err() {
                    return;
                }
            }
        }));
    }
    drop(result_tx);

    // In-flight cap: bound decoded batches outstanding between dispatch and the
    // sink (same default-on policy as the speculative path; see [`InflightCap`]).
    let inflight = make_inflight_cap(num_threads, inflight_factor());

    // Dispatch all work items, then close the work channel. The in-flight cap
    // throttles how many decoded batches may be outstanding (acquire here,
    // released at the sink).
    let dispatch = {
        let inflight = inflight.clone();
        thread::spawn(move || {
            for (id, &(s, e)) in batches.iter().enumerate() {
                if let Some(cap) = &inflight {
                    if !cap.acquire() {
                        return; // cap shut down during teardown
                    }
                }
                if work_tx.send((id as u64, s, e)).is_err() {
                    return;
                }
            }
            drop(work_tx);
        })
    };

    // Serializer: reorder, stream in order.
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut total_uncompressed: u64 = 0;
    let mut chunks_sent: u64 = 0;
    let mut last_err: Option<Error> = None;
    for msg in result_rx.iter() {
        match msg {
            Err(e) => {
                last_err = Some(e);
                break;
            }
            Ok((id, bytes)) => {
                pending.insert(id, bytes);
                while let Some(bytes) = pending.remove(&next_id) {
                    total_uncompressed += bytes.len() as u64;
                    let send_ok = sink.send(Arc::new(bytes)).is_ok();
                    // Batch has left the pipeline — free its in-flight slot.
                    if let Some(cap) = &inflight {
                        cap.release();
                    }
                    if !send_ok {
                        last_err = Some(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "sink closed",
                        )));
                        break;
                    }
                    chunks_sent += 1;
                    next_id += 1;
                }
                if last_err.is_some() {
                    break;
                }
            }
        }
    }

    // Wake `dispatch` if it's parked in `acquire` (we broke out of the
    // serializer early, e.g. on error, before releasing every batch).
    if let Some(cap) = &inflight {
        cap.shutdown();
    }
    let _ = dispatch.join();
    for w in workers {
        let _ = w.join();
    }

    if let Some(e) = last_err {
        return Err(e);
    }

    // The BGZF prefix has been fully streamed in order. If a non-BGZF gzip
    // remainder was concatenated after it, decode it through the regular member
    // path, which transparently handles any further concatenated members. Its
    // output is appended after the BGZF output, preserving stream order.
    if remainder_offset < file.len() {
        let header_len =
            crate::gzip::parse_header(&file[remainder_offset..]).map_err(Error::Gzip)?;
        let (uc, _body_consumed, ch, _spec_failures) = parallel_decode_member(
            Arc::clone(&file),
            remainder_offset + header_len,
            sink,
            config,
        )?;
        total_uncompressed += uc;
        chunks_sent += ch;
    }

    Ok((total_uncompressed, chunks_sent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read_gz, Config};
    use crossbeam_channel::bounded;
    use sha2::{Digest, Sha256};
    use std::io::Write;
    use std::process::{Command, Stdio};

    /// Build a single gz member via the system `gzip`. Drains stdout on a
    /// worker thread to avoid pipe-buffer deadlock for big payloads.
    fn gz_encode(payload: &[u8], level: u32) -> Vec<u8> {
        let mut child = Command::new("gzip")
            .args([&format!("-{level}"), "-c", "-n"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn gzip");
        let mut stdin = child.stdin.take().unwrap();
        let payload = payload.to_vec();
        let writer = std::thread::spawn(move || stdin.write_all(&payload).unwrap());
        let out = child.wait_with_output().expect("wait gzip");
        writer.join().unwrap();
        out.stdout
    }

    fn decode_via_read_gz(path: &std::path::Path, cfg: Config) -> Vec<u8> {
        let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
        let path = path.to_owned();
        let producer = std::thread::spawn(move || read_gz(&path, tx, cfg));
        let mut out = Vec::new();
        for chunk in rx {
            out.extend_from_slice(&chunk);
        }
        producer.join().expect("producer").expect("read_gz");
        out
    }

    fn write_tmp(name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("rapidgzip_rs_test_{name}"));
        std::fs::write(&path, data).unwrap();
        path
    }

    fn ascii_payload(n: usize) -> Vec<u8> {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut p = Vec::with_capacity(n);
        while p.len() < n {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        p
    }

    fn sha(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        hex::encode(h.finalize())
    }

    /// Many concatenated members, with chunk size tuned so member boundaries
    /// land in the *middle* of speculative chunks (mirrors the bug observed in
    /// the wild: CRC mismatch on member ~22 of a real `.fastq.gz` file with
    /// ~1MB-uncompressed members).
    #[test]
    fn multistream_member_crosses_chunk_boundary() {
        // Make enough members to span several chunks, with size chosen so
        // chunk boundaries don't align with member ends.
        // Mirrors the real-world failing case: ~1MB uncompressed per
        // member, chunk_size 4MB compressed, many members so several chunk
        // boundaries land mid-member.
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..30u32 {
            let mut p = ascii_payload(1_048_576);
            let suffix = format!("==MEMBER-{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 6));
        }
        let path = write_tmp("zlib_rs_multistream.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 4 * 1024 * 1024,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&expected), "threads={threads}");
        }
    }

    /// Repro of real-world CRC failure: many ~1MB members at level 9
    /// (compressed members are smaller, packing more per 4MB chunk so
    /// boundaries land at varied positions).
    #[test]
    fn multistream_level9() {
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..30u32 {
            let mut p = ascii_payload(1_048_576);
            let suffix = format!("==MEMBER-{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 9));
        }
        let path = write_tmp("multistream_l9.gz", &gz);
        for threads in [1usize, 4] {
            for chunk_sz in [256usize * 1024, 1 << 20, 4 << 20] {
                let out = decode_via_read_gz(
                    &path,
                    Config {
                        num_threads: threads,
                        chunk_size_bytes: chunk_sz,
                        ..Config::default()
                    },
                );
                assert_eq!(sha(&out), sha(&expected), "t={threads} cs={chunk_sz}");
            }
        }
    }

    /// FastInflate kernel: correctness across various payloads and chunk sizes.
    #[test]
    fn fast_inflate_kernel_correctness() {
        // Highly repetitive payload (RLE-heavy) to stress the prefix-overhang
        // propagation path (distance > emitted, with in-buffer extension).
        let mut rle = Vec::new();
        for _ in 0..1000 {
            rle.extend_from_slice(b"ACGTACGTACGTACGT");
        }

        // Random ASCII to exercise literals and varied back-refs.
        let ascii = ascii_payload(4 * 1024 * 1024);

        for (name, payload, level) in [
            ("rle", rle.as_slice(), 6u32),
            ("ascii", ascii.as_slice(), 6),
            ("ascii9", ascii.as_slice(), 9),
        ] {
            let gz = gz_encode(payload, level);
            let path = write_tmp(&format!("fast_{name}.gz"), &gz);
            for threads in [1usize, 4] {
                for chunk_sz in [64 * 1024usize, 1 << 20] {
                    let out = decode_via_read_gz(
                        &path,
                        Config {
                            num_threads: threads,
                            chunk_size_bytes: chunk_sz,
                            ..Config::default()
                        },
                    );
                    assert_eq!(
                        sha(&out),
                        sha(payload),
                        "fast_inflate mismatch: payload={name} threads={threads} chunk={chunk_sz}"
                    );
                }
            }
        }
    }

    /// FastInflate kernel: multi-stream (concatenated members) correctness.
    #[test]
    fn fast_inflate_kernel_multistream() {
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..10u32 {
            let mut p = ascii_payload(512 * 1024);
            let suffix = format!("==M{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 6));
        }
        let path = write_tmp("fast_multistream.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 4 * 1024 * 1024,
                    ..Config::default()
                },
            );
            assert_eq!(
                sha(&out),
                sha(&expected),
                "fast_inflate multistream threads={threads}"
            );
        }
    }

    /// Parallel decode produces correct output across a large payload spanning
    /// many chunk boundaries.
    #[test]
    fn parallel_large_payload() {
        let payload = ascii_payload(8 * 1024 * 1024);
        let gz = gz_encode(&payload, 6);
        let path = write_tmp("parallel_large.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 1 << 20,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&payload), "threads={threads}");
        }
    }

    /// Same payload, parallel and serial paths must produce identical output.
    #[test]
    fn parallel_matches_serial_large() {
        let payload = ascii_payload(8 * 1024 * 1024);
        let gz = gz_encode(&payload, 6);
        let path = write_tmp("par_vs_serial.gz", &gz);
        for threads in [1usize, 2, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 1 << 20,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&payload), "threads={threads}");
        }
    }

    /// Multi-stream: 3 gzip members concatenated. Each member uses a
    /// different payload so we'd notice if the boundary between members
    /// were dropped or duplicated.
    #[test]
    fn multistream_three_members() {
        let a = ascii_payload(100_000);
        let b = ascii_payload(200_017);
        let c = ascii_payload(50_003);
        let mut gz = gz_encode(&a, 6);
        gz.extend_from_slice(&gz_encode(&b, 6));
        gz.extend_from_slice(&gz_encode(&c, 6));
        let mut expected = Vec::new();
        expected.extend_from_slice(&a);
        expected.extend_from_slice(&b);
        expected.extend_from_slice(&c);
        let path = write_tmp("multi3.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config {
                num_threads: 4,
                chunk_size_bytes: 64 * 1024,
                ..Config::default()
            },
        );
        assert_eq!(sha(&out), sha(&expected));
    }

    /// Large multi-stream: first member is big enough to trigger
    /// multiple-chunk parallel decode, second member exercises the
    /// "stop at BFINAL, hand off to serial multi-stream loop" path.
    #[test]
    fn multistream_large_then_small() {
        let big = ascii_payload(4 * 1024 * 1024);
        let small = ascii_payload(1000);
        let mut gz = gz_encode(&big, 6);
        gz.extend_from_slice(&gz_encode(&small, 6));
        let mut expected = big.clone();
        expected.extend_from_slice(&small);
        let path = write_tmp("multi_big_small.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config {
                num_threads: 4,
                chunk_size_bytes: 1 << 20,
                ..Config::default()
            },
        );
        assert_eq!(sha(&out), sha(&expected));
    }

    /// Tiny file: smaller than chunk_size. Parallel pipeline must degrade
    /// gracefully to a single chunk.
    #[test]
    fn tiny_file() {
        let payload = b"hello, world\n";
        let gz = gz_encode(payload, 6);
        let path = write_tmp("tiny.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config {
                num_threads: 4,
                chunk_size_bytes: 1 << 20,
                ..Config::default()
            },
        );
        assert_eq!(out, payload);
    }

    /// Cross-product of {payload sizes} × {chunk sizes} × {thread counts}.
    /// Catches off-by-one issues where chunk boundary lands at member
    /// boundary, single-chunk degenerate cases, and high-contention paths.
    #[test]
    fn matrix_sizes_chunks_threads() {
        for &size in &[1024usize, 65_536, 250_000, 1_000_000, 5_000_000] {
            let payload = ascii_payload(size);
            let gz = gz_encode(&payload, 6);
            let path = write_tmp(&format!("matrix_{size}.gz"), &gz);
            for &cs in &[64 * 1024usize, 1 << 20, 4 << 20] {
                for &nt in &[1usize, 4] {
                    let out = decode_via_read_gz(
                        &path,
                        Config {
                            num_threads: nt,
                            chunk_size_bytes: cs,
                            ..Config::default()
                        },
                    );
                    assert_eq!(
                        sha(&out),
                        sha(&payload),
                        "size={size} chunk={cs} threads={nt}"
                    );
                }
            }
        }
    }

    /// Stored blocks (level 1 on incompressible data) and dynamic blocks
    /// interleaved: the boundary finder skips over stored blocks, so a
    /// worker may decode several block types in a single chunk.
    #[test]
    fn mixed_block_types() {
        let mut payload = ascii_payload(500_000);
        let mut s: u64 = 0xABCDEF0123456789;
        for _ in 0..(500_000 / 8) {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            payload.extend_from_slice(&s.to_le_bytes());
        }
        payload.extend_from_slice(&ascii_payload(500_000));
        let gz = gz_encode(&payload, 1);
        let path = write_tmp("mixed.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config {
                num_threads: 4,
                chunk_size_bytes: 256 * 1024,
                ..Config::default()
            },
        );
        assert_eq!(sha(&out), sha(&payload));
    }

    /// InflightCap: acquire blocks at the ceiling, release admits the next,
    /// and `shutdown` frees a blocked waiter.
    #[test]
    fn inflight_cap_blocks_releases_shuts_down() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        // Ceiling 2: first two acquires pass immediately.
        let cap = Arc::new(InflightCap::new(2));
        assert!(cap.acquire());
        assert!(cap.acquire());

        // A third acquire must block until a slot frees.
        let admitted = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&cap);
        let a = Arc::clone(&admitted);
        let h = std::thread::spawn(move || {
            assert!(c.acquire());
            a.fetch_add(1, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            admitted.load(Ordering::SeqCst),
            0,
            "acquired over the ceiling"
        );
        cap.release(); // now 1 in flight → waiter proceeds
        h.join().unwrap();
        assert_eq!(admitted.load(Ordering::SeqCst), 1);

        // We're at 2 in flight again (waiter took the freed slot + one held), so
        // we're back at the ceiling; a further acquire blocks until shutdown
        // releases it with `false`.
        let c = Arc::clone(&cap);
        let shut = std::thread::spawn(move || c.acquire());
        std::thread::sleep(Duration::from_millis(50));
        cap.shutdown();
        assert!(
            !shut.join().unwrap(),
            "shutdown should abort a blocked acquire"
        );
    }

    /// Gzip levels 1..9 should all decode correctly. Different levels
    /// produce different block shapes and Huffman trees.
    #[test]
    fn all_gzip_levels() {
        let payload = ascii_payload(2 * 1024 * 1024);
        for level in 1..=9u32 {
            let gz = gz_encode(&payload, level);
            let path = write_tmp(&format!("level_{level}.gz"), &gz);
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: 4,
                    chunk_size_bytes: 512 * 1024,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&payload), "level={level}");
        }
    }

    /// A zero-byte file is not valid gzip; read_gz must return an error rather
    /// than panicking (mmap of an empty file is undefined/platform-specific).
    #[test]
    fn zero_byte_file_returns_error() {
        let path = write_tmp("zero_byte.gz", b"");
        let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
        let producer = std::thread::spawn(move || read_gz(&path, tx, Config::default()));
        drop(rx);
        let result = producer.join().expect("producer thread panicked");
        assert!(
            result.is_err(),
            "zero-byte file should return an error, not succeed"
        );
    }

    /// A named pipe (FIFO) cannot be mmap'd; read_gz must fall back to
    /// buffered reading and decode correctly.
    #[test]
    #[cfg(unix)]
    fn named_pipe_decodes_correctly() {
        use std::io::Write;

        let pipe_path = std::env::temp_dir().join("rapidgzip_rs_pipe_test.fifo");
        let _ = std::fs::remove_file(&pipe_path);
        let status = Command::new("mkfifo")
            .arg(&pipe_path)
            .status()
            .expect("mkfifo");
        assert!(status.success(), "mkfifo failed");

        let payload = ascii_payload(256 * 1024);
        let gz = gz_encode(&payload, 1);

        // The writer must run on a separate thread: opening a FIFO for writing
        // blocks until a reader opens the read end, and vice versa.
        let writer_path = pipe_path.clone();
        let writer = std::thread::spawn(move || {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&writer_path)
                .expect("open pipe for writing");
            f.write_all(&gz).expect("write gz to pipe");
        });

        let out = decode_via_read_gz(&pipe_path, Config::default());
        writer.join().expect("writer thread panicked");

        let _ = std::fs::remove_file(&pipe_path);
        assert_eq!(
            sha(&out),
            sha(&payload),
            "pipe-decoded output does not match"
        );
    }
}
