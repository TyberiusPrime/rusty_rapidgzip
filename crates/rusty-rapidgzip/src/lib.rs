//! Streaming, parallel gzip decoder.

pub mod gzip;
pub mod pipeline;

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
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use thiserror::Error;

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
    /// Which speculative inflate kernel to use for the parallel pipeline.
    /// Defaults to [`InflateKernel::ZlibRs`].
    pub kernel: InflateKernel,
}

/// Which speculative inflate kernel the parallel pipeline uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InflateKernel {
    /// zlib-rs-based speculative decoder (`SpeculativeZlibDecoder`). Default.
    #[default]
    ZlibRs,
    /// Pure-Rust fast inflate kernel (`fast_inflate::decode_until`).
    FastInflate,
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
            kernel: InflateKernel::ZlibRs,
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
    // mmap so workers can fault pages in on demand instead of paying for the
    // whole file up front. For multi-GB inputs the wall-time savings vs.
    // `fs::read` are very large; for small files the mmap setup is cheap.
    let file = File::open(path)?;
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
            if config.num_threads == 0 { "auto".to_string() } else { config.num_threads.to_string() },
            config.chunk_size_bytes,
        );
    }

    // BGZF fast-path: if the first member carries a BC FEXTRA subfield, the
    // whole file is bgzip — every member is an independent deflate stream
    // and we can decode them in parallel without speculation or markers.
    let (total_uncompressed, chunks_sent) = if gzip::parse_bgzf_block_size(input.as_slice()).is_some() {
        if verbose {
            eprintln!(
                "[rapidgzip +{:.2}s] bgzf detected — using fast path",
                elapsed_since_start(),
            );
        }
        pipeline::parallel_decode_bgzf(Arc::clone(&input), &sink, &config)?
    } else {
        // Parse only the *first* member's header here; the pipeline handles
        // every subsequent member's header inline as it crosses BFINAL.
        let header_len = gzip::parse_header(input.as_slice())?;
        let (uc, _body_consumed, ch) =
            pipeline::parallel_decode_member(Arc::clone(&input), header_len, &sink, &config)?;
        (uc, ch)
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
        speculation_failures: 0,
    })
}

