//! Streaming, parallel gzip decoder.
//!
//! Phase 0 stub. The real `read_gz` lands in phase 4. Until then this exposes
//! a placeholder that delegates to a serial decode path (also unimplemented),
//! so that the test harness can be wired up against the API shape.

pub mod gzip;

pub use gzip::{decode_all, decode_one, GzipError};

use std::fs;
use std::path::Path;

use crossbeam_channel::Sender;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of worker threads. `0` → use available parallelism.
    pub num_threads: usize,
    /// Approximate compressed bytes per work chunk.
    pub chunk_size_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            num_threads: 0,
            chunk_size_bytes: 4 * 1024 * 1024,
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
    let _ = config; // honored once phase 4 lands
    let path = path.as_ref();
    let bytes = fs::read(path)?;
    let compressed_bytes = bytes.len() as u64;

    // Decode everything in one go for phase 1. To stay compatible with
    // bounded-channel backpressure once we go parallel, hand the output
    // to the sink in chunks rather than as one giant buffer.
    let mut decoded = Vec::with_capacity(compressed_bytes as usize * 4);
    decode_all(&bytes, &mut decoded)?;

    let uncompressed_bytes = decoded.len() as u64;
    const SINK_CHUNK: usize = 1 << 20; // 1 MiB
    let mut chunks_sent = 0u64;
    let mut start = 0;
    while start < decoded.len() {
        let end = (start + SINK_CHUNK).min(decoded.len());
        // Avoid sending a giant final clone; use drain to move ownership.
        let chunk = decoded[start..end].to_vec();
        if sink.send(chunk).is_err() {
            // Receiver hung up. Treat as graceful early stop.
            break;
        }
        chunks_sent += 1;
        start = end;
    }
    drop(sink);

    Ok(DecodeStats {
        compressed_bytes,
        uncompressed_bytes,
        chunks_decoded: chunks_sent,
        speculation_failures: 0,
    })
}
