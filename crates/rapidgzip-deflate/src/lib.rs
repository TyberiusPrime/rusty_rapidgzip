//! DEFLATE primitives for rapidgzip_rs.
//!
//! Phase 0: empty skeleton. Real implementation lands in phase 1.

#![forbid(unsafe_code)] // Will relax for bitreader/huffman fast paths in phase 5.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeflateError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid deflate stream: {0}")]
    Invalid(&'static str),
}
