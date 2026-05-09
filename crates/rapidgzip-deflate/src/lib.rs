//! DEFLATE primitives for rapidgzip_rs.

#![forbid(unsafe_code)] // Will relax for hot paths in phase 5 if benchmarks demand it.

use thiserror::Error;

pub mod bitreader;
pub mod huffman;
pub mod inflate;
pub mod tables;

pub use bitreader::BitReader;
pub use huffman::HuffmanDecoder;
pub use inflate::{inflate, inflate_block};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeflateError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid deflate stream: {0}")]
    Invalid(&'static str),
}
