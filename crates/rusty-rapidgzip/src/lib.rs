//! Streaming, parallel gzip decoder.

pub mod deflate;
pub mod gzip;
#[cfg(all(feature = "isal", not(feature = "libdeflate")))]
mod isal_ffi;
#[cfg(feature = "libdeflate")]
mod libdeflate_ffi;
pub mod pipeline;
mod streaming;
// Normally only the active backend's FFI compiles. Under `test` we also allow
// `zlibrs_ffi` to compile alongside libdeflate so the kernel A/B/C microbench
// (`kernel_ab`) can pit ours vs zlib-rs vs libdeflate in a single binary.
#[cfg(all(test, feature = "zlib-rs"))]
mod kernel_ab;
#[cfg(any(
    all(
        feature = "zlib-rs",
        not(any(feature = "libdeflate", feature = "isal"))
    ),
    all(test, feature = "zlib-rs")
))]
mod zlibrs_ffi;

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

use crate::deflate::{inflate_block, BitReader, DeflateError};
use pipeline::InputBytes;

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
    /// Output at most these many bytes and exit
    pub max_bytes_output: Option<usize>,
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
            max_bytes_output: None,
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
    Deflate(#[from] crate::deflate::DeflateError),
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
///
/// With `max_bytes_output = Some(n)`, stops after emitting `n` bytes (the
/// truncated member's CRC/ISIZE is not checked). Also stops early, without
/// error, once the receiving end of `sink` has hung up.
fn read_gz_streaming<R: Read>(
    reader: R,
    sink: &Sender<Arc<Vec<u8>>>,
    max_bytes_output: Option<usize>,
) -> Result<(u64, u64), Error> {
    // DEFLATE back-references can reach up to 32 KiB behind the current output
    // position. We therefore maintain a single output Vec across all blocks in a
    // member (so that each block can reference bytes produced by earlier blocks)
    // and only drain the portion that is more than WINDOW bytes old.
    const WINDOW: usize = 32 * 1024;

    let mut sb = StreamBuf::new(reader);
    let mut total_uc = 0u64;
    let mut chunks_sent = 0u64;
    let mut member = 0u32;
    let mut remaining = max_bytes_output;
    if remaining == Some(0) {
        return Ok((0, 0));
    }

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

            // Budget reached: emit the first `r` not-yet-emitted bytes and stop.
            if let Some(r) = remaining {
                if out.len() - crc_cursor >= r {
                    let _ = sink.send(Arc::new(out[crc_cursor..crc_cursor + r].to_vec()));
                    return Ok((total_uc + member_uc + r as u64, chunks_sent + 1));
                }
            }

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
                let n = new_bytes.len();
                let chunk = Arc::new(new_bytes.to_vec());
                if sink.send(chunk).is_err() {
                    // Receiver gone: nobody wants the rest.
                    return Ok((total_uc + member_uc, chunks_sent));
                }
                chunks_sent += 1;
                if let Some(r) = remaining.as_mut() {
                    *r -= n;
                }
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
        let crc_expected = u32::from_le_bytes(
            trailer[..4]
                .try_into()
                .expect("4-byte slice of the 8-byte trailer array"),
        );
        let isize_expected = u32::from_le_bytes(
            trailer[4..]
                .try_into()
                .expect("4-byte slice of the 8-byte trailer array"),
        );
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
    mut config: Config,
) -> Result<DecodeStats, Error> {
    // Output cap: decode serially so we can stop as soon as the budget is
    // met. (The parallel paths would race ahead over the whole file.)
    let limited = config.max_bytes_output.is_some();
    if limited {
        config.num_threads = 1;
    }

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
            let (total_uncompressed, chunks_sent) = read_gz_streaming(
                std::io::BufReader::new(file),
                &sink,
                config.max_bytes_output,
            )?;
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
    // Best-effort hint for sequential access; OS may ignore. `madvise` (and thus
    // memmap2's `Advice`) is unix-only — on other platforms we simply skip the
    // hint.
    #[cfg(unix)]
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
        // With an output cap we skip the BGZF fast path: BGZF is ordinary
        // multi-member gzip, and the serial member decoder honours the cap.
        if !limited && gzip::parse_bgzf_block_size(input.as_slice()).is_some() {
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

#[cfg(test)]
mod tests {
    use super::Config;

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
        decode_gz_bytes_limited(bytes, None)
    }

    fn decode_gz_bytes_limited(
        bytes: &[u8],
        max_bytes_output: Option<usize>,
    ) -> Result<Vec<u8>, super::Error> {
        use std::sync::Arc;
        // pid + counter: tests run in parallel and must not share a file.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rr_bgzf_test_{}_{}.gz",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, bytes).expect("write temp file");
        let (sink, rx) = crossbeam_channel::unbounded::<Arc<Vec<u8>>>();
        let config = Config {
            num_threads: 2,
            chunk_size_bytes: 1 << 20,
            verbose: super::Verbosity::Off,
            recycle_rx: None,
            recycle_tx: None,
            max_bytes_output,
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

    // ── max_bytes_output ────────────────────────────────────────────────────

    /// Single gzip member made of as many stored deflate blocks as `data`
    /// needs (each ≤ 65535 bytes), so large inputs span many blocks.
    fn make_gz_member_multi(data: &[u8]) -> Vec<u8> {
        let mut out = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];
        let blocks: Vec<&[u8]> = data.chunks(0xFFFF).collect();
        for (i, block) in blocks.iter().enumerate() {
            out.push(u8::from(i + 1 == blocks.len())); // BFINAL on last, BTYPE=00
            let len = block.len() as u16;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&(!len).to_le_bytes());
            out.extend_from_slice(block);
        }
        out.extend_from_slice(&crc32fast::hash(data).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + i / 251) as u8).collect()
    }

    /// Garbage that is not a valid gzip member. Decoding it is an error, so a
    /// successful capped decode proves we stopped before reaching it.
    const GARBAGE: &[u8] = b"this is definitely not a gzip member";

    #[test]
    fn max_bytes_truncates_small_and_large_members() {
        let data = pattern(3 << 20); // 3 MiB → spans many blocks and >1 chunk
        let gz = make_gz_member_multi(&data);
        for n in [
            1,
            4,
            100,
            65535,
            65536,
            200_000,
            (1 << 20) + 12_345,
            data.len(),
        ] {
            let out = decode_gz_bytes_limited(&gz, Some(n)).expect("capped decode");
            assert_eq!(out, &data[..n], "n = {n}");
        }
    }

    #[test]
    fn max_bytes_larger_than_output_returns_everything() {
        let data = pattern(500_000);
        let gz = make_gz_member_multi(&data);
        let out = decode_gz_bytes_limited(&gz, Some(10 << 20)).expect("decode");
        assert_eq!(out, data);
    }

    #[test]
    fn max_bytes_zero_outputs_nothing() {
        let gz = make_gz_member(b"hello");
        assert!(decode_gz_bytes_limited(&gz, Some(0))
            .expect("decode")
            .is_empty());
    }

    #[test]
    fn max_bytes_stops_before_trailing_garbage() {
        let data = pattern(2 << 20);
        let mut gz = make_gz_member_multi(&data);
        gz.extend_from_slice(GARBAGE);
        assert!(
            decode_gz_bytes(&gz).is_err(),
            "uncapped decode must hit the garbage"
        );
        let out = decode_gz_bytes_limited(&gz, Some(1000)).expect("capped decode");
        assert_eq!(out, &data[..1000]);
    }

    #[test]
    fn max_bytes_spans_member_boundary() {
        let mut gz = make_gz_member(b"hello ");
        gz.extend(make_gz_member(b"world"));
        gz.extend_from_slice(GARBAGE);
        let out = decode_gz_bytes_limited(&gz, Some(8)).expect("capped decode");
        assert_eq!(out, b"hello wo");
    }

    #[test]
    fn max_bytes_on_bgzf_skips_fast_path_and_stops_early() {
        let mut gz = make_bgzf_member(b"hello ");
        gz.extend(make_bgzf_member(b"world"));
        gz.extend_from_slice(GARBAGE);
        let out = decode_gz_bytes_limited(&gz, Some(9)).expect("capped decode");
        assert_eq!(out, b"hello wor");
    }

    #[test]
    fn max_bytes_streaming_path_stops_early() {
        // Non-seekable path: drive the streaming decoder from a reader.
        let data = pattern(2 << 20);
        let mut gz = make_gz_member_multi(&data);
        gz.extend_from_slice(GARBAGE);
        for n in [0, 1, 4, 70_000, 1 << 20] {
            let (tx, rx) = crossbeam_channel::unbounded();
            let (uc, _) = super::read_gz_streaming(std::io::Cursor::new(&gz), &tx, Some(n))
                .expect("capped streaming decode");
            drop(tx);
            let out: Vec<u8> = rx.iter().flat_map(|c| c.to_vec()).collect();
            assert_eq!(uc, n as u64);
            assert_eq!(out, &data[..n], "n = {n}");
        }
    }

    #[test]
    fn streaming_path_stops_when_receiver_gone() {
        let data = pattern(2 << 20);
        let mut gz = make_gz_member_multi(&data);
        gz.extend_from_slice(GARBAGE);
        let (tx, rx) = crossbeam_channel::unbounded::<std::sync::Arc<Vec<u8>>>();
        drop(rx);
        // Without the early exit this would decode on and fail on the garbage.
        super::read_gz_streaming(std::io::Cursor::new(&gz), &tx, None)
            .expect("stops quietly once nobody listens");
    }
}
