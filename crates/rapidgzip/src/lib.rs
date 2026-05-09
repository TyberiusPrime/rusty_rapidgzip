//! Streaming, parallel gzip decoder.
//!
//! Phase 0 stub. The real `read_gz` lands in phase 4. Until then this exposes
//! a placeholder that delegates to a serial decode path (also unimplemented),
//! so that the test harness can be wired up against the API shape.

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
    #[error("deflate: {0}")]
    Deflate(#[from] rapidgzip_deflate::DeflateError),
    #[error("not implemented yet (phase 0 stub)")]
    NotImplemented,
}

/// Decode `path` and stream decompressed bytes to `sink` in stream order.
///
/// Blocks until EOF or first error. The sink is closed when this returns.
pub fn read_gz(
    _path: impl AsRef<Path>,
    _sink: Sender<Vec<u8>>,
    _config: Config,
) -> Result<DecodeStats, Error> {
    Err(Error::NotImplemented)
}
