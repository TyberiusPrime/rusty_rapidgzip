//! Streaming, parallel gzip decoder.
//!
//! Phase 0 stub. The real `read_gz` lands in phase 4. Until then this exposes
//! a placeholder that delegates to a serial decode path (also unimplemented),
//! so that the test harness can be wired up against the API shape.

pub mod gzip;
pub mod pipeline;

pub use gzip::{decode_all, decode_one, GzipError};

use std::fs;
use std::path::Path;
use std::sync::Arc;

use crossbeam_channel::Sender;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of worker threads. `0` → use available parallelism.
    pub num_threads: usize,
    /// Approximate compressed bytes per work chunk.
    pub chunk_size_bytes: usize,
    /// Print per-member / per-chunk diagnostics to stderr while decoding.
    /// Off by default; CLI exposes `--verbose`. See [`Verbosity`].
    pub verbose: Verbosity,
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
    Deflate(#[from] rapidgzip_deflate::DeflateError),
}

/// Decode `path` and stream decompressed bytes to `sink` in stream order.
///
/// Blocks until EOF or first error. The sink is closed when this returns.
///
/// **Phase 1**: this is a single-threaded implementation that reads the
/// entire file into memory and runs the serial gzip decoder on it. It
/// matches the eventual API exactly so callers and tests can be written
/// against it now. Phase 4 swaps the body for the parallel pipeline
/// without changing the signature.
pub fn read_gz(
    path: impl AsRef<Path>,
    sink: Sender<Vec<u8>>,
    config: Config,
) -> Result<DecodeStats, Error> {
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    let compressed_bytes = bytes.len() as u64;
    let verbose = config.verbose.is_on();
    if verbose {
        eprintln!(
            "[rapidgzip] {}: {} compressed bytes, {} threads, chunk_size={}",
            path.display(),
            compressed_bytes,
            if config.num_threads == 0 { "auto".to_string() } else { config.num_threads.to_string() },
            config.chunk_size_bytes,
        );
    }

    // BGZF fast-path: if the first member carries a BC FEXTRA subfield, the
    // whole file is bgzip — every member is an independent deflate stream
    // and we can decode them in parallel without speculation or markers.
    let (total_uncompressed, chunks_sent) = if gzip::parse_bgzf_block_size(&bytes).is_some() {
        if verbose {
            eprintln!("[rapidgzip] bgzf detected — using fast path");
        }
        let file: Arc<Vec<u8>> = Arc::new(bytes);
        pipeline::parallel_decode_bgzf(Arc::clone(&file), &sink, &config)?
    } else {
        // Parse only the *first* member's header here; the pipeline handles
        // every subsequent member's header inline as it crosses BFINAL.
        let header_len = gzip::parse_header(&bytes)?;
        let body_start = header_len;
        let body: Arc<Vec<u8>> = Arc::new(bytes[body_start..].to_vec());
        let (uc, _body_consumed, ch) =
            pipeline::parallel_decode_member(Arc::clone(&body), &sink, &config)?;
        (uc, ch)
    };

    drop(sink);
    if verbose {
        eprintln!(
            "[rapidgzip] done: {total_uncompressed} uncompressed bytes, {chunks_sent} output chunks",
        );
    }
    Ok(DecodeStats {
        compressed_bytes,
        uncompressed_bytes: total_uncompressed,
        chunks_decoded: chunks_sent,
        speculation_failures: 0,
    })
}

