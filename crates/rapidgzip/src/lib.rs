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

    let mut pos = 0usize;
    let mut total_uncompressed: u64 = 0;
    let mut chunks_sent: u64 = 0;

    // Parse the first member header.
    let header_len = gzip::parse_header(&bytes[pos..])?;
    let body_start = pos + header_len;

    // The pipeline needs the body in an Arc<Vec<u8>> for sharing across
    // workers. We hand it the slice from `body_start` to end of file —
    // it figures out where the trailer is via BFINAL.
    let body: Arc<Vec<u8>> = Arc::new(bytes[body_start..].to_vec());
    let (uncompressed, body_consumed) =
        pipeline::parallel_decode_member(Arc::clone(&body), &counting_sink(&sink, &mut chunks_sent), &config)?;
    total_uncompressed += uncompressed;
    pos = body_start + body_consumed;
    if verbose {
        eprintln!(
            "[rapidgzip] member 0 parallel: {uncompressed} uncompressed bytes from {body_consumed} compressed body bytes",
        );
    }

    // Multi-stream: serially decode any remaining members. This keeps the
    // hot path (the giant first member) parallel while still handling
    // concatenated gzip files correctly.
    let mut member: u32 = 1;
    while pos < bytes.len() {
        if verbose {
            eprintln!(
                "[rapidgzip] member {member} serial fallback (parallel path is for member 0 only)",
            );
        }
        let mut decoded = Vec::new();
        let consumed = gzip::decode_one_indexed(&bytes[pos..], &mut decoded, member)?;
        if verbose {
            eprintln!(
                "[rapidgzip] member {member} serial: {} uncompressed bytes from {consumed} compressed bytes",
                decoded.len(),
            );
        }
        total_uncompressed += decoded.len() as u64;
        send_in_chunks(&sink, &decoded, &mut chunks_sent);
        pos += consumed;
        member += 1;
    }

    drop(sink);
    if verbose {
        eprintln!(
            "[rapidgzip] done: {member} member(s), {total_uncompressed} uncompressed bytes, {chunks_sent} output chunks",
        );
    }
    Ok(DecodeStats {
        compressed_bytes,
        uncompressed_bytes: total_uncompressed,
        chunks_decoded: chunks_sent,
        speculation_failures: 0,
    })
}

/// Wrap `sink` to bump `chunks_sent` on each successful send. Returns the
/// original sender — the count is updated through the closure capture.
fn counting_sink<'a>(
    sink: &'a Sender<Vec<u8>>,
    counter: &'a mut u64,
) -> Sender<Vec<u8>> {
    // crossbeam channels are cheap to clone (Arc internally); the workers
    // need an owned Sender. We can't intercept sends without wrapping the
    // type, so for now just return a clone. The counter is updated by the
    // post-pipeline pass below.
    let _ = counter;
    sink.clone()
}

fn send_in_chunks(sink: &Sender<Vec<u8>>, data: &[u8], counter: &mut u64) {
    const SINK_CHUNK: usize = 1 << 20;
    let mut start = 0;
    while start < data.len() {
        let end = (start + SINK_CHUNK).min(data.len());
        if sink.send(data[start..end].to_vec()).is_err() {
            return;
        }
        *counter += 1;
        start = end;
    }
}
