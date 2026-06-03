//! Streaming, parallel gzip decoder.

pub mod gzip;
pub mod pipeline;
mod streaming;

pub use gzip::{decode_all, decode_one, GzipError};

static VERBOSE_START: OnceLock<Instant> = OnceLock::new();

/// Seconds elapsed since the first verbose log site fired (process-wide).
pub fn elapsed_since_start() -> f64 {
    VERBOSE_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
}

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use thiserror::Error;

use pipeline::InputBytes;
use rusty_rapidgzip_deflate::{inflate_block, BitReader, DeflateError};

use fastqrab_stringpod::{DualStringPod, StringPod, StringPodBuilder};

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of worker threads. `0` → use available parallelism.
    pub num_threads: usize,
    /// Approximate compressed bytes per work chunk.
    pub chunk_size_bytes: usize,
    /// Print per-member / per-chunk diagnostics to stderr while decoding.
    /// Off by default; CLI exposes `--verbose`. See [`Verbosity`].
    pub verbose: Verbosity,
    /// Optional channel of recycled output buffers. When set, workers pull a
    /// `Vec<u8>` from this channel (or allocate fresh if empty) to use as the
    /// chunk's output buffer. Within a few chunks of steady state every
    /// worker is reusing the same pool of N buffers, so pages stay
    /// faulted-in and the per-chunk page fault cost vanishes. No-op if `None`.
    pub recycle_rx: Option<Receiver<Vec<u8>>>,
    /// Sender paired with `recycle_rx`. The pipeline forwards drained
    /// `Vec<u8>` buffers here after their bytes have been streamed AND
    /// CRC-validated. The pipeline (not the consumer of `sink`) owns the
    /// recycle return path because `sink` emits `Arc<Vec<u8>>` shared with
    /// the CRC validator, and only the pipeline knows when both refs drop.
    pub recycle_tx: Option<Sender<Vec<u8>>>,
}

/// How chatty `read_gz` is on stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Verbosity {
    /// Silent (default).
    #[default]
    Off,
    /// One line per member: which path (parallel / serial), boundary count,
    /// uncompressed bytes. Plus a final summary.
    On,
}

impl Verbosity {
    #[inline]
    pub fn is_on(self) -> bool {
        matches!(self, Verbosity::On)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            num_threads: 0,
            chunk_size_bytes: 4 * 1024 * 1024,
            verbose: Verbosity::Off,
            recycle_rx: None,
            recycle_tx: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DecodeStats {
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub chunks_decoded: u64,
    pub speculation_failures: u64,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("gzip: {0}")]
    Gzip(#[from] GzipError),
    #[error("deflate: {0}")]
    Deflate(#[from] rusty_rapidgzip_deflate::DeflateError),
    #[error("fastq: {0}")]
    Fastq(String),
}

// ── Streaming serial decode path ─────────────────────────────────────────────
//
// Used when the input is a non-seekable source (named pipe, stdin, socket).
// mmap requires a seekable regular file; for everything else we decode one
// deflate block at a time from a growable compressed buffer.
//
// Memory bound: the buffer holds at most one deflate block's worth of
// compressed data (typically 16–64 KiB) plus the read-ahead chunk. After each
// successfully decoded block the consumed bytes are drained from the front so
// the buffer stays compact. The decompressed output is emitted block by block
// to the sink rather than accumulated per member.
//
// Corner case: if inflate_block returns UnexpectedEof we extend the buffer and
// retry from the start of the current block. By the time a full block fits in
// the buffer the retry is cheap (the block is small). Adversarially large
// blocks (e.g. a single stored block spanning GiB) would buffer proportionally,
// but that does not occur in practice for FASTQ.gz.

const STREAM_CHUNK: usize = 64 * 1024;

struct StreamBuf<R> {
    reader: R,
    buf: Vec<u8>,
    /// Sub-byte bit offset into `buf[0]`; always 0..8.
    /// Bits 0..bit_off of `buf[0]` were consumed by the previous block's tail.
    bit_off: u64,
    src_eof: bool,
}

impl<R: Read> StreamBuf<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::with_capacity(STREAM_CHUNK * 2),
            bit_off: 0,
            src_eof: false,
        }
    }

    fn fill_more(&mut self) -> Result<bool, std::io::Error> {
        let old = self.buf.len();
        self.buf.resize(old + STREAM_CHUNK, 0);
        let got = self.reader.read(&mut self.buf[old..])?;
        self.buf.truncate(old + got);
        if got == 0 {
            self.src_eof = true;
        }
        Ok(got > 0)
    }

    fn fill_at_least(&mut self, n: usize) -> Result<(), Error> {
        while self.buf.len() < n {
            if self.src_eof {
                return Err(Error::Gzip(GzipError::Truncated));
            }
            self.fill_more().map_err(Error::Io)?;
        }
        Ok(())
    }

    /// Decode one deflate block into `out`. On UnexpectedEof, extends the
    /// buffer and retries from the block's start. Returns whether BFINAL was set.
    fn decode_block(&mut self, out: &mut Vec<u8>) -> Result<bool, Error> {
        loop {
            let out_check = out.len();
            let mut br = BitReader::new(&self.buf);
            if self.bit_off > 0 {
                br.seek_to_bit(self.bit_off).map_err(Error::Deflate)?;
            }
            match inflate_block(&mut br, out) {
                Ok(bfinal) => {
                    // `br.tell_bit()` is the absolute bit position from buf[0].
                    let pos = br.tell_bit();
                    let consumed = (pos / 8) as usize;
                    self.bit_off = pos & 7;
                    if consumed > 0 {
                        self.buf.drain(..consumed);
                    }
                    return Ok(bfinal);
                }
                Err(DeflateError::UnexpectedEof) => {
                    out.truncate(out_check);
                    if self.src_eof {
                        return Err(Error::Gzip(GzipError::Truncated));
                    }
                    self.fill_more().map_err(Error::Io)?;
                }
                Err(e) => return Err(Error::Deflate(e)),
            }
        }
    }

    /// Discard the remaining bits of the partial byte at buf[0], aligning to
    /// the next byte boundary. No-op when already aligned.
    fn byte_align(&mut self) {
        if self.bit_off > 0 && !self.buf.is_empty() {
            self.buf.drain(..1);
            self.bit_off = 0;
        }
    }

    /// Read exactly N bytes at the current byte-aligned position.
    fn read_bytes<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        debug_assert_eq!(self.bit_off, 0);
        self.fill_at_least(N)?;
        let mut arr = [0u8; N];
        arr.copy_from_slice(&self.buf[..N]);
        self.buf.drain(..N);
        Ok(arr)
    }

    /// Parse the gzip header from the buffer, filling more data as needed.
    /// Drains the header bytes from the buffer.
    fn read_gzip_header(&mut self) -> Result<(), Error> {
        loop {
            match gzip::parse_header(&self.buf) {
                Ok(n) => {
                    self.buf.drain(..n);
                    return Ok(());
                }
                Err(GzipError::Truncated) => {
                    if self.src_eof {
                        return Err(Error::Gzip(GzipError::Truncated));
                    }
                    self.fill_more().map_err(Error::Io)?;
                }
                Err(e) => return Err(Error::Gzip(e)),
            }
        }
    }
}

/// Decode a non-seekable gzip stream (pipe, stdin) serially, emitting
/// decompressed bytes block by block to `sink`.
fn read_gz_streaming<R: Read>(reader: R, sink: &Sender<Arc<Vec<u8>>>) -> Result<(u64, u64), Error> {
    // DEFLATE back-references can reach up to 32 KiB behind the current output
    // position. We therefore maintain a single output Vec across all blocks in a
    // member (so that each block can reference bytes produced by earlier blocks)
    // and only drain the portion that is more than WINDOW bytes old.
    const WINDOW: usize = 32 * 1024;

    let mut sb = StreamBuf::new(reader);
    let mut total_uc = 0u64;
    let mut chunks_sent = 0u64;
    let mut member = 0u32;

    // Prime the buffer.
    sb.fill_more().map_err(Error::Io)?;

    loop {
        if sb.buf.is_empty() {
            break; // clean EOF between members
        }

        sb.read_gzip_header()?;

        let mut crc_hasher = crc32fast::Hasher::new();
        let mut member_uc = 0u64;
        // Single output Vec shared across all blocks in this member so
        // back-references into previous blocks resolve correctly.
        let mut out: Vec<u8> = Vec::with_capacity(WINDOW * 2);
        let mut crc_cursor = 0usize; // bytes in `out` already fed into crc_hasher

        loop {
            let bfinal = sb.decode_block(&mut out)?;

            // Emit bytes produced since last emission, keeping the last WINDOW
            // bytes in `out` as the back-reference history for subsequent blocks.
            let emit_end = if bfinal {
                out.len()
            } else {
                out.len().saturating_sub(WINDOW)
            };
            if emit_end > crc_cursor {
                let new_bytes = &out[crc_cursor..emit_end];
                crc_hasher.update(new_bytes);
                member_uc += new_bytes.len() as u64;
                let chunk = Arc::new(new_bytes.to_vec());
                let _ = sink.send(chunk);
                chunks_sent += 1;
                crc_cursor = emit_end;
            }
            if bfinal {
                out.clear();
                // crc_cursor = 0; never read.
                break;
            }
            // Compact: drain the emitted prefix so `out` doesn't grow without bound.
            if crc_cursor > 0 {
                out.drain(..crc_cursor);
                crc_cursor = 0;
            }
        }

        // Read and validate the 8-byte gzip trailer.
        sb.byte_align();
        let trailer = sb.read_bytes::<8>()?;
        let crc_expected = u32::from_le_bytes(trailer[..4].try_into().unwrap());
        let isize_expected = u32::from_le_bytes(trailer[4..].try_into().unwrap());
        let crc_got = crc_hasher.finalize();
        if crc_got != crc_expected {
            return Err(Error::Gzip(GzipError::CrcMismatch {
                expected: crc_expected,
                got: crc_got,
                member,
                uncompressed: member_uc,
                trailer_byte: 0,
            }));
        }
        if (member_uc & 0xFFFF_FFFF) as u32 != isize_expected {
            return Err(Error::Gzip(GzipError::IsizeMismatch {
                expected: isize_expected,
                got: member_uc,
                member,
                trailer_byte: 0,
            }));
        }

        total_uc += member_uc;
        member += 1;

        // Peek for a next member; if not present we're done.
        if sb.buf.is_empty() && !sb.src_eof {
            sb.fill_more().map_err(Error::Io)?;
        }
    }

    // A valid gzip stream has at least one member.
    if member == 0 {
        return Err(Error::Gzip(GzipError::Truncated));
    }

    Ok((total_uc, chunks_sent))
}

/// Decode `path` and stream decompressed bytes to `sink` in stream order.
///
/// Blocks until EOF or first error. The sink is closed when this returns.
pub fn read_gz(
    path: impl AsRef<Path>,
    sink: Sender<Arc<Vec<u8>>>,
    config: Config,
) -> Result<DecodeStats, Error> {
    let path = path.as_ref();
    let verbose = config.verbose.is_on();
    let t_open = std::time::Instant::now();

    let file = File::open(path)?;
    let meta = file.metadata()?;

    // Non-seekable sources (named pipes, stdin, sockets) and zero-byte regular
    // files cannot be mmap'd. With >1 worker we run the streaming-parallel
    // pipeline (bounded memory, full multi-core throughput); with a single
    // worker speculation is pure overhead, so we stay on the marker-less serial
    // path.
    if !meta.file_type().is_file() || meta.len() == 0 {
        let num_threads = if config.num_threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            config.num_threads
        };
        if num_threads <= 1 {
            if verbose {
                eprintln!(
                    "[rapidgzip +{:.2}s] {}: streaming serial (non-seekable or empty, 1 thread)",
                    elapsed_since_start(),
                    path.display(),
                );
            }
            let (total_uncompressed, chunks_sent) =
                read_gz_streaming(std::io::BufReader::new(file), &sink)?;
            drop(sink);
            return Ok(DecodeStats {
                compressed_bytes: 0,
                uncompressed_bytes: total_uncompressed,
                chunks_decoded: chunks_sent,
                speculation_failures: 0,
            });
        }
        if verbose {
            eprintln!(
                "[rapidgzip +{:.2}s] {}: streaming-parallel (non-seekable), {num_threads} threads, chunk_size={}",
                elapsed_since_start(),
                path.display(),
                config.chunk_size_bytes,
            );
        }
        let (total_uncompressed, chunks_sent, compressed_bytes) =
            streaming::read_gz_streaming_parallel(
                std::io::BufReader::new(file),
                &sink,
                num_threads,
                config.chunk_size_bytes,
            )?;
        drop(sink);
        return Ok(DecodeStats {
            compressed_bytes,
            uncompressed_bytes: total_uncompressed,
            chunks_decoded: chunks_sent,
            speculation_failures: 0,
        });
    }

    // Regular non-empty file: mmap so workers can fault pages in on demand
    // instead of paying for the whole file up front.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    // Best-effort hint for sequential access; OS may ignore.
    let _ = mmap.advise(memmap2::Advice::Sequential);
    let compressed_bytes = mmap.len() as u64;
    let input = Arc::new(InputBytes::Mapped(mmap));
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] {}: mmaped {} bytes in {:.3}s, {} threads, chunk_size={}",
            elapsed_since_start(),
            path.display(),
            compressed_bytes,
            t_open.elapsed().as_secs_f64(),
            if config.num_threads == 0 {
                "auto".to_string()
            } else {
                config.num_threads.to_string()
            },
            config.chunk_size_bytes,
        );
    }

    // BGZF fast-path: if the first member carries a BC FEXTRA subfield, the
    // whole file is bgzip — every member is an independent deflate stream
    // and we can decode them in parallel without speculation or markers.
    let (total_uncompressed, chunks_sent, speculation_failures) =
        if gzip::parse_bgzf_block_size(input.as_slice()).is_some() {
            if verbose {
                eprintln!(
                    "[rapidgzip +{:.2}s] bgzf detected — using fast path",
                    elapsed_since_start(),
                );
            }
            // BGZF members are independent, fully-bounded deflate streams: no block
            // finder, no speculation, hence no false boundaries to recover from.
            let (uc, ch) = pipeline::parallel_decode_bgzf(Arc::clone(&input), &sink, &config)?;
            (uc, ch, 0)
        } else {
            // Parse only the *first* member's header here; the pipeline handles
            // every subsequent member's header inline as it crosses BFINAL.
            let header_len = gzip::parse_header(input.as_slice())?;
            let (uc, _body_consumed, ch, spec) =
                pipeline::parallel_decode_member(Arc::clone(&input), header_len, &sink, &config)?;
            (uc, ch, spec)
        };

    drop(sink);
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] done: {total_uncompressed} uncompressed bytes, {chunks_sent} output chunks",
            elapsed_since_start(),
        );
    }
    Ok(DecodeStats {
        compressed_bytes,
        uncompressed_bytes: total_uncompressed,
        chunks_decoded: chunks_sent,
        speculation_failures,
    })
}

/// One decode chunk's worth of FASTQ, split into per-role columns.
///
/// `names` is a [`StringPod`] of header lines with the leading `@` stripped via
/// an O(1) `cut_start`. `reads` is a [`DualStringPod`] fusing the sequence and
/// quality columns: because every record satisfies `seq.len() == qual.len()`,
/// the two share a single metadata column and the invariant is structural
/// rather than re-checked downstream. The separator (`+`) line is validated and
/// dropped.
///
/// A column is `Storage::FixedLength` when every entry in *this* emission shares
/// a length (the common fixed-read-length case) and `Variable` otherwise — the
/// pod builders start fixed on the first line's length and auto-promote on the
/// first mismatch.
///
/// Across the whole stream each column receives exactly one entry per record, in
/// order; every emitted chunk is record-aligned (`names.len() == reads.len()`).
#[derive(Debug)]
pub struct FastqChunk {
    pub names: StringPod,
    pub reads: DualStringPod,
}

/// A decode chunk handed to a demux worker, with the trailing partial line of
/// the previous chunk to stitch onto this chunk's leading partial line.
struct DemuxJob {
    idx: u64,
    chunk: Arc<Vec<u8>>,
    prev_tail: Vec<u8>,
}

/// A worker's phase-agnostic split of one chunk: four buckets holding the
/// chunk's lines indexed by `line_index_within_stream mod 4` (the worker does
/// not know which absolute role that is — the collector rotates), plus the
/// number of lines completed in the chunk (`= newline count`) so the collector
/// can accumulate the global phase.
struct DemuxResult {
    idx: u64,
    buckets: [StringPod; 4],
    lines: u64,
}

/// Like [`read_gz`], but decode + split into columnar FASTQ and stream a
/// [`FastqChunk`] per decode chunk instead of raw bytes.
///
/// The decode itself runs through the same parallel pipeline as `read_gz`. The
/// expensive newline scan and the copy into per-role columns run in a parallel
/// demux pool: each worker buckets its chunk's lines by `index mod 4` without
/// knowing the absolute phase (decode-chunk boundaries never align to records,
/// and `@`/`+` are ambiguous mid-stream since both are legal quality bytes).
/// A cheap serial stage threads the one-line carry across boundaries and feeds
/// each worker its predecessor's trailing partial line; a cheap serial
/// collector accumulates the global line count, "ring-rotates" each chunk's
/// four buckets onto (name, seq, +, qual), strips the `@`, and validates.
pub fn read_gz_into_fastq(
    path: impl AsRef<Path>,
    sink: Sender<FastqChunk>,
    config: Config,
) -> Result<DecodeStats, Error> {
    let path = path.as_ref().to_path_buf();
    let num_threads = if config.num_threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        config.num_threads
    };
    // The decode pipeline already saturates the cores; the demux pool must stay
    // small or it oversubscribes and the allocator contends. A handful of
    // workers is enough to keep the split off the critical path.
    let demux_threads = std::env::var("RAPIDGZIP_FASTQ_DEMUX_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| num_threads.min(4))
        .max(1);

    // Recycle drained byte buffers back to the decode workers so pages stay
    // faulted-in. We forward this into the decode config (its CRC thread
    // recycles) and the demux workers also return buffers once they've copied
    // the payload into columns.
    let (recycle_tx, recycle_rx) = crossbeam_channel::bounded::<Vec<u8>>(num_threads * 2);
    let mut cfg = config;
    cfg.recycle_rx = Some(recycle_rx);
    cfg.recycle_tx = Some(recycle_tx.clone());

    let (bytes_tx, bytes_rx) = crossbeam_channel::bounded::<Arc<Vec<u8>>>(16);
    let producer = std::thread::spawn(move || read_gz(&path, bytes_tx, cfg));

    // Demux pool — phase-agnostic per-chunk bucketing (the heavy scan + copy).
    let (job_tx, job_rx) = crossbeam_channel::bounded::<DemuxJob>(num_threads * 2);
    let (done_tx, done_rx) =
        crossbeam_channel::bounded::<Result<DemuxResult, Error>>(num_threads * 2);
    let mut workers = Vec::with_capacity(demux_threads);
    for _ in 0..demux_threads {
        let job_rx = job_rx.clone();
        let done_tx = done_tx.clone();
        let recycle_tx = recycle_tx.clone();
        workers.push(std::thread::spawn(move || {
            for job in job_rx {
                let result = demux_chunk(job.idx, &job.chunk, &job.prev_tail);
                // Payload is copied into the columns now; hand the buffer back.
                if let Ok(mut v) = Arc::try_unwrap(job.chunk) {
                    v.clear();
                    let _ = recycle_tx.try_send(v);
                }
                if done_tx.send(result).is_err() {
                    return;
                }
            }
        }));
    }
    drop(job_rx);
    drop(done_tx);

    // Collector — serial, in stream order: rotate buckets onto roles + emit.
    let (tail_tx, tail_rx) = crossbeam_channel::bounded::<Vec<u8>>(1);
    let collector = std::thread::spawn(move || collect(done_rx, tail_rx, &sink));

    // Stage A — serial, cheap: thread the trailing-partial-line carry across
    // boundaries (one backward scan per chunk) and dispatch demux jobs.
    let mut carry: Vec<u8> = Vec::new();
    let mut idx: u64 = 0;
    for chunk in bytes_rx {
        match rposition_nl(&chunk) {
            Some(last_nl) => {
                let next_carry = chunk[last_nl + 1..].to_vec();
                let prev_tail = std::mem::take(&mut carry);
                if job_tx
                    .send(DemuxJob {
                        idx,
                        chunk,
                        prev_tail,
                    })
                    .is_err()
                {
                    break;
                }
                idx += 1;
                carry = next_carry;
            }
            None => {
                // No newline at all (pathological for FASTQ): the whole chunk is
                // the middle of one line. Fold it into the carry; emit nothing.
                carry.extend_from_slice(&chunk);
            }
        }
    }
    drop(job_tx);
    // Final unterminated line (file without a trailing newline), if any.
    let _ = tail_tx.send(carry);
    drop(tail_tx);

    for w in workers {
        let _ = w.join();
    }
    let split_result = collector.join().expect("fastq collector thread panicked");

    drop(recycle_tx);
    let stats = producer.join().expect("decode producer thread panicked")?;
    split_result?;
    Ok(stats)
}

#[inline]
fn strip_cr(line: &[u8]) -> &[u8] {
    match line.last() {
        Some(b'\r') => &line[..line.len() - 1],
        _ => line,
    }
}

#[inline]
fn rposition_nl(data: &[u8]) -> Option<usize> {
    data.iter().rposition(|&c| c == b'\n')
}

/// FASTQ columns address their byte buffer with `u32` offsets, so any single
/// column — and therefore any single read's sequence or quality line — cannot
/// exceed `u32::MAX` bytes. Degenerate/adversarial input (a multi-GiB single
/// read, or a tiny compressed chunk that inflates past 4 GiB) would otherwise
/// overflow the `u32` position in `StringPodBuilder::push` and panic; the guard
/// below turns that into a clean error instead.
const MAX_FASTQ_COLUMN_BYTES: usize = u32::MAX as usize;

/// Reject a push that would grow a FASTQ column past [`MAX_FASTQ_COLUMN_BYTES`]
/// with a clear error rather than letting the underlying `u32` cast panic.
#[inline]
fn fastq_len_guard(current: usize, add: usize) -> Result<(), Error> {
    // `current` is always ≤ u32::MAX (every prior push was guarded), so this
    // sum cannot overflow usize on the 64-bit targets this crate supports.
    if current + add > MAX_FASTQ_COLUMN_BYTES {
        return Err(Error::Fastq(
            "FASTQ read length exceeds the allowed maximum of 4 GiB".to_string(),
        ));
    }
    Ok(())
}

#[inline]
fn push_line(bucket: &mut Option<StringPodBuilder>, est: usize, line: &[u8]) -> Result<(), Error> {
    // Guard before `with_capacity`, which itself casts the entry length to u32
    // and would panic on a >4 GiB line before we ever reach `push`.
    let current = bucket.as_ref().map_or(0, StringPodBuilder::buffer_bytes);
    fastq_len_guard(current, line.len())?;
    bucket
        .get_or_insert_with(|| StringPodBuilder::with_capacity(line.len(), est))
        .push(line);
    Ok(())
}

/// Split one chunk into four buckets keyed by `line_index mod 4`, phase
/// unknown. The line that straddles the *previous* boundary is reassembled
/// from `prev_tail ++ this chunk's leading partial line` and pushed first into
/// bucket 3 — that is the line one position *before* the first fully-contained
/// line (local index 0 → bucket 0), so it shares bucket 0's predecessor slot.
/// After the collector's rotation this lands in the correct role column, in
/// order, with no further copy. `data` is guaranteed to contain ≥1 newline.
fn demux_chunk(idx: u64, data: &[u8], prev_tail: &[u8]) -> Result<DemuxResult, Error> {
    let est = (data.len() / 300).max(16);
    let mut builders: [Option<StringPodBuilder>; 4] = [None, None, None, None];

    let first_nl = data.iter().position(|&c| c == b'\n').expect("≥1 newline");

    // Reassemble the boundary-straddling line and push it into bucket 3. Guard
    // its assembled length up front so an adversarial multi-GiB line is rejected
    // before we materialise the (equally multi-GiB) `split` buffer.
    let head = strip_cr(&data[..first_nl]);
    fastq_len_guard(0, prev_tail.len() + head.len())?;
    let mut split = Vec::with_capacity(prev_tail.len() + head.len());
    split.extend_from_slice(prev_tail);
    split.extend_from_slice(head);
    push_line(&mut builders[3], est, strip_cr(&split))?;

    // Fully-contained lines: between consecutive newlines, local index from 0.
    let mut lines: u64 = 1; // the boundary line, completed by `first_nl`
    let mut local: usize = 0;
    let mut start = first_nl + 1;
    //TODO: Check if this get's any real world performance boost from memchr crate
    while let Some(rel) = data[start..].iter().position(|&c| c == b'\n') {
        let nl = start + rel;
        push_line(&mut builders[local & 3], est, strip_cr(&data[start..nl]))?;
        lines += 1;
        local += 1;
        start = nl + 1;
    }
    // data[start..] is this chunk's trailing partial line — carried by Stage A.

    // Reserve a little slack on every bucket so the collector's per-boundary
    // appends (≤3 lines completing a straddling record) land in place instead
    // of reallocating these (potentially multi-MB) buffers.
    let buckets = builders.map(|b| {
        let mut pod = b
            .map(StringPodBuilder::finish)
            .unwrap_or_else(StringPod::empty);
        pod.reserve_for_appends(4);
        pod
    });
    Ok(DemuxResult {
        idx,
        buckets,
        lines,
    })
}

/// Role-indexed columns for one chunk: `[name, seq, plus, qual]` (role = line
/// index mod 4). The collector keeps the previous chunk's `Cols` as `held`
/// until the next chunk supplies the lines completing its trailing record.
type Cols = [StringPod; 4];

/// Validate and emit one chunk of *whole* records. `cols` must already hold
/// equal-length, record-aligned columns (the collector guarantees this). The
/// `+` separator is validated and dropped; the leading `@` is stripped O(1).
fn emit_records(cols: Cols, emit: &mut impl FnMut(FastqChunk)) -> Result<(), Error> {
    let [mut names, seqs, plus, quals] = cols;
    debug_assert_eq!(names.len(), seqs.len());
    debug_assert_eq!(seqs.len(), quals.len());

    if names.is_empty() {
        return Ok(()); // nothing to emit
    }
    if !names.iter().all(|name| name.first() == Some(&b'@')) {
        return Err(Error::Fastq(
            "header line does not start with '@'".to_string(),
        ));
    }
    if !plus.is_empty() && !plus.iter().all(|p| p.first() == Some(&b'+')) {
        return Err(Error::Fastq(
            "separator line does not start with '+'".to_string(),
        ));
    }
    // Sequence and quality share the FASTQ per-entry length invariant, so fuse
    // them into a single DualStringPod (zero-copy — both byte buffers move in
    // as-is). `try_from_columns` verifies seq.len() == qual.len() for every
    // record and that the two layouts are a constant translation, surfacing any
    // mismatch as an error rather than emitting a malformed chunk.
    let reads =
        DualStringPod::try_from_columns(seqs, quals).map_err(|m| Error::Fastq(m.to_string()))?;
    names.cut_start(1); // drop the leading '@' from every header, O(1)
    emit(FastqChunk { names, reads });
    Ok(())
}

/// Absorb one in-order chunk. The demux buckets are rotated onto roles using
/// the running phase (`global_lines % 4`), then merged into `held` via the
/// "complete the previous chunk's straddling record from this chunk's head"
/// scheme: any record that begins in `held` but spills into this chunk is
/// finished by appending this chunk's leading lines onto `held` (≤3 tiny line
/// copies), and those same lines are front-skipped off this chunk. `held` then
/// holds only whole records and is emitted; this chunk (record-aligned at its
/// head) becomes the new `held`.
fn absorb(
    res: DemuxResult,
    held: &mut Option<Cols>,
    global_lines: &mut u64,
    emit: &mut impl FnMut(FastqChunk),
) -> Result<(), Error> {
    let phase = (*global_lines % 4) as usize; // = held's trailing-record line count
    let g = phase as i64;
    // role r is held by bucket b where (g + 1 + b) % 4 == r.
    let src = |r: i64| ((r - g - 1).rem_euclid(4)) as usize;
    let mut b: [Option<StringPod>; 4] = res.buckets.map(Some);
    let mut cur: Cols = [
        b[src(0)].take().expect("rotation is a permutation"),
        b[src(1)].take().expect("rotation is a permutation"),
        b[src(2)].take().expect("rotation is a permutation"),
        b[src(3)].take().expect("rotation is a permutation"),
    ];
    let avail = res.lines as usize;
    *global_lines += res.lines;

    match held.take() {
        None => {
            // Only reachable at a record boundary (phase 0): this chunk starts a
            // fresh record run.
            debug_assert_eq!(phase, 0);
            *held = Some(cur);
        }
        Some(mut h) => {
            let need = (4 - phase) % 4; // lines to finish held's trailing record
            let take = need.min(avail);
            for role in phase..phase + take {
                // role ∈ [phase, 4): the straddling record's missing lines, in
                // order, at the head of `cur`. Move them onto `held`.
                let line = cur[role].get(0).to_vec();
                fastq_len_guard(h[role].buffer_bytes(), line.len())?;
                h[role].push(&line);
                cur[role].pop_front(1);
            }
            if take == need {
                // held's trailing record is complete → flush it.
                emit_records(h, emit)?;
                // `cur` (front-skipped past the lines we consumed) now starts at
                // a record boundary; it becomes the next `held`.
                *held = if avail - take > 0 { Some(cur) } else { None };
            } else {
                // Consumed all of `cur` without completing the record (tiny
                // chunk straddling >2 chunks); keep accumulating into `held`.
                *held = Some(h);
            }
        }
    }
    Ok(())
}

/// Flush at end of stream: fold the file's final unterminated line (if any,
/// from `tail_rx` / the leftover carry) onto `held`, then emit the remaining
/// whole records. Returns an error if the stream ends mid-record.
fn finish_eof(
    held: Option<Cols>,
    carry: &[u8],
    global_lines: u64,
    emit: &mut impl FnMut(FastqChunk),
) -> Result<(), Error> {
    let Some(mut h) = held else {
        if !carry.is_empty() {
            return Err(Error::Fastq(
                "truncated FASTQ: incomplete record at end of stream".to_string(),
            ));
        }
        return Ok(());
    };
    if !carry.is_empty() {
        let role = (global_lines % 4) as usize;
        let line = strip_cr(carry);
        fastq_len_guard(h[role].buffer_bytes(), line.len())?;
        h[role].push(line);
    }
    let complete = h[0].len().min(h[1].len()).min(h[3].len());
    if h[0].len() != complete || h[1].len() != complete || h[3].len() != complete {
        return Err(Error::Fastq(
            "truncated FASTQ: incomplete record at end of stream".to_string(),
        ));
    }
    emit_records(h, emit)
}

/// Reorder demux results by chunk index and run the serial record-alignment
/// (`absorb` / `finish_eof`), emitting a record-aligned [`FastqChunk`] per
/// input chunk. Runs serially in stream order.
fn collect(
    done_rx: Receiver<Result<DemuxResult, Error>>,
    tail_rx: Receiver<Vec<u8>>,
    sink: &Sender<FastqChunk>,
) -> Result<(), Error> {
    use std::collections::BTreeMap;
    let mut reorder: BTreeMap<u64, DemuxResult> = BTreeMap::new();
    let mut next: u64 = 0;
    let mut global_lines: u64 = 0;
    let mut held: Option<Cols> = None;
    let mut emit = |chunk: FastqChunk| {
        let _ = sink.send(chunk);
    };

    for res in done_rx {
        let res = res?; // a worker hit invalid/oversized input — surface it
        reorder.insert(res.idx, res);
        while let Some(res) = reorder.remove(&next) {
            absorb(res, &mut held, &mut global_lines, &mut emit)?;
            next += 1;
        }
    }
    // Any stragglers (shouldn't happen once done_rx is closed, but be safe).
    while let Some(res) = reorder.remove(&next) {
        absorb(res, &mut held, &mut global_lines, &mut emit)?;
        next += 1;
    }

    let carry = tail_rx.recv().unwrap_or_default();
    finish_eof(held, &carry, global_lines, &mut emit)
}

/// Test hook: run the full demux + record-alignment over an explicit sequence
/// of decode chunks (no gzip, no threads) and return the concatenated
/// `(names, seqs, quals)` columns, each entry newline-terminated. Lets
/// `tests/fastq.rs` assert chunk-boundary independence and the complete-records
/// invariant directly against the real `absorb` / `finish_eof` logic.
#[doc(hidden)]
pub fn fastq_split_for_test(chunks: &[&[u8]]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), Error> {
    let (mut names, mut seqs, mut quals) = (Vec::new(), Vec::new(), Vec::new());
    let mut emit = |c: FastqChunk| {
        for x in c.names.iter() {
            names.extend_from_slice(x);
            names.push(b'\n');
        }
        for x in c.reads.iter_seq() {
            seqs.extend_from_slice(x);
            seqs.push(b'\n');
        }
        for x in c.reads.iter_qual() {
            quals.extend_from_slice(x);
            quals.push(b'\n');
        }
    };

    let mut held: Option<Cols> = None;
    let mut global_lines: u64 = 0;
    let mut carry: Vec<u8> = Vec::new();
    let mut idx: u64 = 0;
    for chunk in chunks {
        match rposition_nl(chunk) {
            Some(last_nl) => {
                let next_carry = chunk[last_nl + 1..].to_vec();
                let prev_tail = std::mem::take(&mut carry);
                let res = demux_chunk(idx, chunk, &prev_tail)?;
                absorb(res, &mut held, &mut global_lines, &mut emit)?;
                idx += 1;
                carry = next_carry;
            }
            None => carry.extend_from_slice(chunk),
        }
    }
    finish_eof(held, &carry, global_lines, &mut emit)?;
    Ok((names, seqs, quals))
}

#[cfg(test)]
mod tests {
    use super::{fastq_len_guard, Config, MAX_FASTQ_COLUMN_BYTES};

    // ── BGZF / gzip member helpers ──────────────────────────────────────────

    /// Build a minimal single-member gzip stream for `data` (stored deflate,
    /// no compression, no extra header fields).
    fn make_gz_member(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);
        let len = data.len() as u16;
        out.push(0x01); // BFINAL=1, BTYPE=00 (stored)
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes()); // NLEN = one's-complement of LEN
        out.extend_from_slice(data);
        out.extend_from_slice(&crc32fast::hash(data).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out
    }

    /// Build a minimal single-member BGZF stream for `data` (stored deflate,
    /// BC FEXTRA subfield, BSIZE = total block size − 1).
    fn make_bgzf_member(data: &[u8]) -> Vec<u8> {
        // Build deflate body first so we know total size.
        let len = data.len() as u16;
        let mut body = vec![0x01]; // BFINAL=1, BTYPE=00
        body.extend_from_slice(&len.to_le_bytes());
        body.extend_from_slice(&(!len).to_le_bytes());
        body.extend_from_slice(data);

        // 10 (fixed header) + 2 (XLEN) + 6 (BC subfield) + body + 8 (trailer)
        let total = 10 + 2 + 6 + body.len() + 8;
        let bsize = (total - 1) as u16;

        let mut out = Vec::new();
        out.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff]);
        out.extend_from_slice(&[0x06, 0x00]); // XLEN = 6
        out.push(b'B');
        out.push(b'C');
        out.extend_from_slice(&[0x02, 0x00]); // SLEN = 2
        out.extend_from_slice(&bsize.to_le_bytes()); // BSIZE−1
        out.extend_from_slice(&body);
        out.extend_from_slice(&crc32fast::hash(data).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out
    }

    /// Write `bytes` to a temp file, decode it via [`read_gz`], and return the
    /// concatenated decompressed output.
    fn decode_gz_bytes(bytes: &[u8]) -> Result<Vec<u8>, super::Error> {
        use std::sync::Arc;
        let path = std::env::temp_dir().join(format!(
            "rr_bgzf_test_{}.gz",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos()
        ));
        std::fs::write(&path, bytes).expect("write temp file");
        let (sink, rx) = crossbeam_channel::unbounded::<Arc<Vec<u8>>>();
        let config = Config {
            num_threads: 2,
            chunk_size_bytes: 1 << 20,
            verbose: super::Verbosity::Off,
            recycle_rx: None,
            recycle_tx: None,
        };
        let result = super::read_gz(&path, sink, config);
        let _ = std::fs::remove_file(&path);
        result?;
        let mut out = Vec::new();
        for chunk in rx {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    // ── BGZF × gz combination tests ─────────────────────────────────────────

    #[test]
    fn bgzf_bgzf_decodes_both_members() {
        let mut input = make_bgzf_member(b"hello ");
        input.extend(make_bgzf_member(b"world"));
        let out = decode_gz_bytes(&input).expect("bgzf+bgzf should decode");
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn gz_gz_decodes_both_members() {
        let mut input = make_gz_member(b"hello ");
        input.extend(make_gz_member(b"world"));
        let out = decode_gz_bytes(&input).expect("gz+gz should decode");
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn bgzf_gz_decodes_both_members() {
        // First member triggers BGZF detection; second is plain gzip.
        let mut input = make_bgzf_member(b"hello ");
        input.extend(make_gz_member(b"world"));
        let out = decode_gz_bytes(&input).expect("bgzf+gz should decode");
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn gz_bgzf_decodes_both_members() {
        // First member is plain gzip so BGZF path is NOT taken; second is BGZF
        // (which is valid gzip and must be handled by the regular path).
        let mut input = make_gz_member(b"hello ");
        input.extend(make_bgzf_member(b"world"));
        let out = decode_gz_bytes(&input).expect("gz+bgzf should decode");
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn len_guard_accepts_up_to_the_limit() {
        // A column filled exactly to u32::MAX is fine; one more byte is not.
        assert!(fastq_len_guard(MAX_FASTQ_COLUMN_BYTES - 10, 10).is_ok());
        assert!(fastq_len_guard(0, MAX_FASTQ_COLUMN_BYTES).is_ok());
    }

    #[test]
    fn len_guard_rejects_oversized_with_clear_message() {
        let err = fastq_len_guard(0, MAX_FASTQ_COLUMN_BYTES + 1).unwrap_err();
        assert!(
            err.to_string()
                .contains("FASTQ read length exceeds the allowed maximum of 4 GiB"),
            "unexpected error: {err}"
        );
        // Also rejects a cumulative overflow (small add onto a near-full column).
        assert!(fastq_len_guard(MAX_FASTQ_COLUMN_BYTES, 1).is_err());
    }
}
