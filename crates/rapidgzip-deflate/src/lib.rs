//! DEFLATE primitives for rapidgzip_rs.

#![forbid(unsafe_code)] // Will relax for hot paths in phase 5 if benchmarks demand it.

use thiserror::Error;

pub mod bitreader;
pub mod block_finder;
pub mod huffman;
pub mod inflate;
pub mod speculative;
pub mod tables;

pub use bitreader::BitReader;
pub use block_finder::find_next_dynamic_block;
pub use huffman::HuffmanDecoder;
pub use inflate::{inflate, inflate_block, read_dynamic_header};
pub use speculative::{
    inflate_block_speculative, inflate_speculative, resolve_markers, tail_window, Marker,
    SpeculativeChunk,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeflateError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid deflate stream: {0}")]
    Invalid(&'static str),
}
