//! Streaming-parallel decode for non-seekable inputs (named pipes, stdin,
//! sockets) — bounded total memory, full multi-core throughput.
//!
//! ## Why this exists
//!
//! The mmap pipeline ([`crate::pipeline::parallel_decode_member`]) needs the
//! whole compressed body addressable up front: its boundary scan walks the
//! entire length and every worker indexes the full slice. A pipe can't be
//! mmap'd, and the obvious fallbacks are both bad — read the whole thing into
//! RAM (unbounded memory, no read/decode overlap) or decode serially on one
//! core (≈9× slower).
//!
//! ## The insight
//!
//! Parallelism never needed a *seekable file* — it needed the compressed bytes
//! resident as a contiguous slice. And within a single gzip member, compressed
//! bytes behind the decode frontier are never re-read: DEFLATE back-references
//! point into *decompressed* output, never into earlier compressed input. So we
//! can read the pipe forward, hand each speculative chunk its own owned
//! compressed slice, and drop the front of the read buffer the moment a chunk
//! has been cut out of it.
//!
//! ## Architecture
//!
//! A **dispatcher** reads the pipe into a small sliding buffer, finds the next
//! dynamic-block boundary roughly every `chunk_size_bytes`, copies the bytes
//! between boundaries into an owned per-chunk slice, drains the consumed prefix,
//! and sends the chunk to a bounded work channel (the bound is what caps
//! read-ahead, hence memory). The rest mirrors the mmap pipeline exactly:
//!
//! - **decode pool** — speculative `decode_until_u16` per chunk, emitting
//!   placeholder markers for back-refs into the (not-yet-known) preceding 32 KiB.
//! - **stage A** (serial, cheap) — reorder by id, run the `build_prev_tail_fast`
//!   tail-chain, hand each chunk + the tail it needs to the resolve pool.
//! - **resolve pool** — `resolve_markers` patches each chunk's bulk markers
//!   against its prev_tail, independently across chunks.
//! - **output** (serial, in order) — reorder, fold CRC32 + ISIZE per member,
//!   stream resolved bytes to the sink.
//!
//! Peak resident compressed memory ≈ one sliding buffer (~`chunk_size_bytes` +
//! lookahead) + the in-flight work channel (`num_threads` owned slices), which
//! is independent of the total stream size.
//!
//! The single-worker case stays on the marker-less serial path
//! ([`crate::read_gz_streaming`]); speculation is pure overhead there.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};

use rusty_rapidgzip_deflate::{
    fast_inflate, find_next_dynamic_block, resolve_markers, SpeculativeChunk,
};

use crate::pipeline::build_prev_tail_fast;
use crate::{Error, GzipError};

/// Window of decompressed history a back-ref can reach — the serial dependency
/// the tail-chain threads across chunks.
const WINDOW: usize = 32 * 1024;
/// Bytes appended past a non-final chunk's last block so the bit reader's
/// look-ahead `peek` never runs off the owned slice (the mmap path gets this
/// for free from the following chunk's bytes).
const PAD: usize = 8;
/// How far past a boundary target we want resident before trusting a search
/// result — enough for `find_next_dynamic_block` to validate a full dynamic
/// header.
const SCAN_LOOKAHEAD: usize = 256 * 1024;
/// Pipe read granularity.
const READ_CHUNK: usize = 1 << 20;

/// A member transition seen while decoding one chunk: where (in the chunk's
/// output) the ending member stops, plus its trailer for validation.
#[derive(Clone)]
struct MemberBoundary {
    byte_offset_in_chunk: usize,
    crc_expected: u32,
    isize_expected: u32,
    trailer_input_byte: u64,
}

/// One unit of work: an owned compressed slice plus where to start/stop.
struct Item {
    id: u64,
    bytes: Vec<u8>,
    /// Start bit, relative to `bytes[0]` (0..8).
    start_bit: u64,
    /// Stop once a block boundary at/after this bit (relative to `bytes[0]`) is
    /// crossed. `u64::MAX` for the final region (decode to BFINAL).
    end_bit_hint: u64,
    /// This slice runs to the true end of the stream, so a BFINAL whose trailer
    /// ends the slice is real EOF.
    is_final_region: bool,
    /// Absolute body-byte offset of `bytes[0]`, for trailer error reporting.
    base_byte: u64,
}

struct DecodeResult {
    id: u64,
    chunk: SpeculativeChunk,
    final_block: bool,
    member_boundaries: Vec<MemberBoundary>,
}

struct ResolveJob {
    id: u64,
    chunk: SpeculativeChunk,
    prev_tail: Arc<Vec<u8>>,
    final_block: bool,
    member_boundaries: Vec<MemberBoundary>,
}

/// Forward-only compressed source with a sliding, front-droppable buffer.
/// `base` is the absolute body-byte offset of `buf[0]`.
struct Source<R> {
    reader: R,
    buf: Vec<u8>,
    base: u64,
    src_eof: bool,
    total_read: u64,
}

impl<R: Read> Source<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::with_capacity(READ_CHUNK * 2),
            base: 0,
            src_eof: false,
            total_read: 0,
        }
    }

    /// Absolute body-byte offset one past the last resident byte.
    fn abs_end(&self) -> u64 {
        self.base + self.buf.len() as u64
    }

    /// Read one more `READ_CHUNK`; returns bytes read (0 ⇒ EOF).
    fn fill_more(&mut self) -> Result<usize, Error> {
        let old = self.buf.len();
        self.buf.resize(old + READ_CHUNK, 0);
        let got = self.reader.read(&mut self.buf[old..]).map_err(Error::Io)?;
        self.buf.truncate(old + got);
        if got == 0 {
            self.src_eof = true;
        }
        self.total_read += got as u64;
        Ok(got)
    }

    /// Ensure the buffer is resident up to absolute byte `want`, or EOF.
    fn fill_to(&mut self, want: u64) -> Result<(), Error> {
        while self.abs_end() < want && !self.src_eof {
            self.fill_more()?;
        }
        Ok(())
    }

    /// Drop everything before absolute byte `abs`, advancing `base`.
    fn drain_to(&mut self, abs: u64) {
        debug_assert!(abs >= self.base);
        let drop = (abs - self.base) as usize;
        if drop > 0 {
            self.buf.drain(..drop);
            self.base = abs;
        }
    }

    /// Read and strip the first gzip header, leaving `buf[0]` at the first
    /// deflate body byte and `base == 0`.
    fn strip_first_header(&mut self) -> Result<(), Error> {
        loop {
            match crate::gzip::parse_header(&self.buf) {
                Ok(n) => {
                    self.buf.drain(..n);
                    // base stays 0: body coordinates start at the first block.
                    return Ok(());
                }
                Err(GzipError::Truncated) => {
                    if self.src_eof {
                        return Err(Error::Gzip(GzipError::Truncated));
                    }
                    self.fill_more()?;
                }
                Err(e) => return Err(Error::Gzip(e)),
            }
        }
    }
}

/// Decode one chunk speculatively. Mirrors `pipeline::decode_one_chunk`, but
/// EOF is signalled by `is_final_region` rather than `body.len()` because the
/// owned slice is a window, not the whole stream.
fn decode_item(item: &Item, scratch: &mut Vec<u16>) -> Result<DecodeResult, Error> {
    let body = &item.bytes[..];
    let mut chunk = SpeculativeChunk::default();
    if item.end_bit_hint != u64::MAX && item.end_bit_hint > item.start_bit {
        let span = ((item.end_bit_hint - item.start_bit) / 8) as usize;
        chunk.reserve_bytes(span.saturating_mul(3).min(64 * 1024 * 1024));
    } else {
        chunk.reserve_bytes(WINDOW * 4);
    }

    let mut pos = item.start_bit;
    let mut final_block = false;
    let mut member_boundaries: Vec<MemberBoundary> = Vec::new();

    loop {
        // Guard before decoding: decode_until always consumes at least one
        // block, so without this it could overshoot end_bit_hint into the next
        // chunk's territory.
        if pos >= item.end_bit_hint {
            break;
        }

        let (end_bit, hit_bfinal) =
            fast_inflate::decode_until_u16(body, pos, item.end_bit_hint, &mut chunk, scratch)
                .map_err(Error::Deflate)?;
        pos = end_bit;

        if !hit_bfinal {
            // Stopped at a block boundary at/after end_bit_hint — chunk done.
            break;
        }

        // Final block of a member: byte-align, then read the 8-byte trailer.
        let trailer_byte = ((pos + 7) / 8) as usize;
        if trailer_byte + 8 > body.len() {
            return Err(Error::Gzip(GzipError::Truncated));
        }
        let crc_expected =
            u32::from_le_bytes(body[trailer_byte..trailer_byte + 4].try_into().unwrap());
        let isize_expected =
            u32::from_le_bytes(body[trailer_byte + 4..trailer_byte + 8].try_into().unwrap());
        member_boundaries.push(MemberBoundary {
            byte_offset_in_chunk: chunk.bytes.len(),
            crc_expected,
            isize_expected,
            trailer_input_byte: item.base_byte + trailer_byte as u64,
        });

        let after_trailer = trailer_byte + 8;
        if item.is_final_region && after_trailer >= body.len() {
            final_block = true;
            break;
        }
        if after_trailer >= body.len() {
            // Member ended at this (non-final) chunk's edge; the next chunk
            // resumes with the following member (which it decodes fresh — a new
            // member resets the window, so no cross-member markers arise).
            break;
        }

        // Another member follows within this slice: parse its header and resume.
        let header_len = crate::gzip::parse_header(&body[after_trailer..]).map_err(Error::Gzip)?;
        pos = ((after_trailer + header_len) as u64) * 8;
    }

    Ok(DecodeResult {
        id: item.id,
        chunk,
        final_block,
        member_boundaries,
    })
}

/// Dispatcher: read the pipe, cut owned per-chunk slices at dynamic-block
/// boundaries, and feed them to the work channel until EOF.
fn dispatch<R: Read>(
    mut src: Source<R>,
    work_tx: Sender<Item>,
    token_rx: Receiver<()>,
    chunk_bytes: usize,
    abort: &AtomicBool,
) -> Result<u64, Error> {
    src.fill_more()?;
    src.strip_first_header()?;

    let chunk_bits = (chunk_bytes.max(64 * 1024) as u64) * 8;
    let mut item_start_bit: u64 = 0;
    let mut id: u64 = 0;

    loop {
        if abort.load(Ordering::Relaxed) {
            return Ok(src.total_read);
        }

        let target_bit = item_start_bit + chunk_bits;
        let target_byte = target_bit / 8;
        // Make sure we have room to *find and validate* a boundary near target.
        src.fill_to(target_byte + SCAN_LOOKAHEAD as u64)?;

        let resident_bits = src.buf.len() as u64 * 8;
        let from_rel = target_bit.saturating_sub(src.base * 8);

        let boundary = if from_rel >= resident_bits {
            None
        } else {
            find_next_dynamic_block(&src.buf, from_rel, resident_bits).map(|rel| src.base * 8 + rel)
        };

        match boundary {
            Some(end_bit) => {
                // Acquire an in-flight slot (blocks if `inflight_cap` chunks are
                // already outstanding dispatch→sink). A closed token channel
                // means the output stage tore down — stop dispatching.
                if token_rx.recv().is_err() {
                    return Ok(src.total_read);
                }
                emit(&mut src, &work_tx, id, item_start_bit, end_bit, false)?;
                id += 1;
                item_start_bit = end_bit;
                src.drain_to(end_bit / 8);
            }
            None => {
                if !src.src_eof {
                    // No boundary in the resident window yet — read further.
                    if src.fill_more()? == 0 {
                        // hit EOF on this read; fall through to final region.
                    } else {
                        continue;
                    }
                }
                // EOF: the remainder is the final region (one chunk to BFINAL).
                while !src.src_eof {
                    src.fill_more()?;
                }
                if token_rx.recv().is_err() {
                    return Ok(src.total_read);
                }
                emit(&mut src, &work_tx, id, item_start_bit, u64::MAX, true)?;
                return Ok(src.total_read);
            }
        }
    }
}

/// Copy `[item_start_bit, end_bit)` (plus padding) out of the resident buffer
/// into an owned slice and send it as a work item. `end_bit == u64::MAX` ⇒
/// final region (take everything resident from the start byte to EOF).
fn emit<R>(
    src: &mut Source<R>,
    work_tx: &Sender<Item>,
    id: u64,
    item_start_bit: u64,
    end_bit: u64,
    is_final_region: bool,
) -> Result<(), Error> {
    let start_byte = item_start_bit / 8;
    let start_rel = (start_byte - src.base) as usize;
    let start_bit_in_slice = item_start_bit - start_byte * 8;

    let (slice, end_bit_hint) = if is_final_region {
        let s = src.buf[start_rel..].to_vec();
        (s, u64::MAX)
    } else {
        let end_byte = (end_bit + 7) / 8; // ceil
        let take_to = ((end_byte - src.base) as usize + PAD).min(src.buf.len());
        let s = src.buf[start_rel..take_to].to_vec();
        let hint = end_bit - start_byte * 8;
        (s, hint)
    };

    let item = Item {
        id,
        bytes: slice,
        start_bit: start_bit_in_slice,
        end_bit_hint,
        is_final_region,
        base_byte: start_byte,
    };
    work_tx
        .send(item)
        .map_err(|_| Error::Io(std::io::Error::other("decode workers gone")))
}

/// Public entry: decode a non-seekable gzip stream with the parallel pipeline,
/// streaming decompressed bytes to `sink` in order. Returns
/// `(uncompressed_bytes, chunks_sent, compressed_bytes)`.
pub(crate) fn read_gz_streaming_parallel<R: Read + Send + 'static>(
    reader: R,
    sink: &Sender<Arc<Vec<u8>>>,
    num_threads: usize,
    chunk_bytes: usize,
) -> Result<(u64, u64, u64), Error> {
    let resolve_threads = num_threads.min(4).max(1);

    let abort = Arc::new(AtomicBool::new(false));

    let (work_tx, work_rx) = bounded::<Item>(num_threads);
    let (result_tx, result_rx) = bounded::<Result<DecodeResult, Error>>(num_threads * 2);
    let (resolve_tx, resolve_rx) = bounded::<ResolveJob>(num_threads * 2);
    let (done_tx, done_rx) = bounded::<Result<DecodeResult, Error>>(num_threads * 2);

    // In-flight cap (mirrors the mmap pipeline's `InflightCap`). A decoded chunk
    // carries its output bytes *and* its still-unresolved markers, so without a
    // hard ceiling on how many may be outstanding between dispatch and the sink,
    // a slow consumer lets the sum of every channel's capacity worth of chunks
    // pile up. We model the ceiling as a pool of `cap` tokens: the dispatcher
    // takes one before cutting a chunk and the output stage returns it once the
    // chunk has been handed to the sink. `factor=2`, floor 16 — same policy as
    // the mmap path so RSS tracks the worker count, not the file size.
    let inflight_cap = (num_threads * 2).max(16);
    let (token_tx, token_rx) = bounded::<()>(inflight_cap);
    for _ in 0..inflight_cap {
        let _ = token_tx.send(());
    }

    // Decode pool.
    let mut workers = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        workers.push(thread::spawn(move || {
            let mut scratch: Vec<u16> = Vec::new();
            for item in work_rx {
                let res = decode_item(&item, &mut scratch);
                if result_tx.send(res).is_err() {
                    return;
                }
            }
        }));
    }
    drop(work_rx);
    drop(result_tx);

    // Resolve pool — independent per chunk.
    let mut resolvers = Vec::with_capacity(resolve_threads);
    for _ in 0..resolve_threads {
        let resolve_rx: Receiver<ResolveJob> = resolve_rx.clone();
        let done_tx = done_tx.clone();
        resolvers.push(thread::spawn(move || {
            for mut job in resolve_rx {
                let r = resolve_markers(&mut job.chunk, &job.prev_tail);
                let msg = r.map(|()| DecodeResult {
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

    // Stage A — serial reorder + tail-chain, then dispatch to resolve pool.
    let done_tx_err = done_tx.clone();
    drop(done_tx);
    let stage_a = thread::spawn(move || {
        let mut reorder: BTreeMap<u64, DecodeResult> = BTreeMap::new();
        let mut next_id: u64 = 0;
        let mut prev_tail: Vec<u8> = Vec::new();
        let mut closed = false;
        loop {
            while let Some(res) = reorder.remove(&next_id) {
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
                if resolve_tx.send(job).is_err() || saw_final {
                    return;
                }
                next_id += 1;
            }
            if closed {
                return;
            }
            match result_rx.recv() {
                Ok(Ok(r)) => {
                    reorder.insert(r.id, r);
                }
                Ok(Err(e)) => {
                    let _ = done_tx_err.send(Err(e));
                    return;
                }
                Err(_) => {
                    // All workers done; drain whatever is buffered, in order.
                    closed = true;
                }
            }
        }
    });

    // Dispatcher thread.
    let abort_disp = Arc::clone(&abort);
    let dispatcher = thread::spawn(move || {
        let src = Source::new(reader);
        dispatch(src, work_tx, token_rx, chunk_bytes, &abort_disp)
    });

    // Output stage (this thread) — reorder, validate CRC32 + ISIZE per member,
    // stream to sink in order.
    let mut reorder: BTreeMap<u64, DecodeResult> = BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut total_uc: u64 = 0;
    let mut chunks_sent: u64 = 0;
    let mut cur_crc = crc32fast::Hasher::new();
    let mut cur_uncompressed: u64 = 0;
    let mut member_idx: u32 = 0;
    let mut out_err: Option<Error> = None;
    let mut closed = false;

    'outer: loop {
        while let Some(done) = reorder.remove(&next_id) {
            let bytes = done.chunk.bytes;
            let mut cursor = 0usize;
            for mb in &done.member_boundaries {
                let piece = &bytes[cursor..mb.byte_offset_in_chunk];
                cur_crc.update(piece);
                cur_uncompressed += piece.len() as u64;
                let crc_got = std::mem::replace(&mut cur_crc, crc32fast::Hasher::new()).finalize();
                if crc_got != mb.crc_expected {
                    out_err = Some(Error::Gzip(GzipError::CrcMismatch {
                        expected: mb.crc_expected,
                        got: crc_got,
                        member: member_idx,
                        uncompressed: cur_uncompressed,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                    break 'outer;
                }
                if (cur_uncompressed & 0xFFFF_FFFF) as u32 != mb.isize_expected {
                    out_err = Some(Error::Gzip(GzipError::IsizeMismatch {
                        expected: mb.isize_expected,
                        got: cur_uncompressed,
                        member: member_idx,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                    break 'outer;
                }
                member_idx += 1;
                cur_uncompressed = 0;
                cursor = mb.byte_offset_in_chunk;
            }
            let tail = &bytes[cursor..];
            cur_crc.update(tail);
            cur_uncompressed += tail.len() as u64;

            let n = bytes.len() as u64;
            total_uc += n;
            let saw_final = done.final_block;
            let sink_ok = sink.send(Arc::new(bytes)).is_ok();
            // The chunk has left the pipeline — return its in-flight slot so the
            // dispatcher may admit the next one. (Release even on sink close; we
            // tear down right after.)
            let _ = token_tx.send(());
            if !sink_ok {
                break 'outer;
            }
            chunks_sent += 1;
            next_id += 1;
            if saw_final {
                break 'outer;
            }
        }
        if closed {
            break;
        }
        match done_rx.recv() {
            Ok(Ok(d)) => {
                reorder.insert(d.id, d);
            }
            Ok(Err(e)) => {
                out_err = Some(e);
                break 'outer;
            }
            Err(_) => {
                closed = true;
            }
        }
    }

    // Teardown: signal the dispatcher to stop and drain the channels so every
    // stage's input disconnects and the joins below complete. Dropping
    // `token_tx` is what wakes a dispatcher parked in `token_rx.recv()` (its
    // `abort` flag is only checked at the top of the loop, not while blocked).
    abort.store(true, Ordering::Relaxed);
    drop(token_tx);
    drop(done_rx);
    let _ = stage_a.join();
    for r in resolvers {
        let _ = r.join();
    }
    for w in workers {
        let _ = w.join();
    }
    let disp_res = dispatcher
        .join()
        .unwrap_or_else(|_| Err(Error::Io(std::io::Error::other("dispatcher panicked"))));

    if let Some(e) = out_err {
        return Err(e);
    }
    let compressed = disp_res?;
    Ok((total_uc, chunks_sent, compressed))
}
