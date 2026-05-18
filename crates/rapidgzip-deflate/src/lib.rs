//! DEFLATE primitives for rapidgzip_rs.

#![deny(unsafe_code)] // Phase-5 hot paths opt in locally via `#[allow(unsafe_code)]`.

use thiserror::Error;

pub mod bitreader;
pub mod block_finder;
pub mod huffman;
pub mod inflate;
pub mod speculative;
pub mod speculative_zlib;
pub mod tables;

pub use bitreader::BitReader;
pub use block_finder::find_next_dynamic_block;
pub use huffman::HuffmanDecoder;
pub use inflate::{inflate, inflate_block, read_dynamic_header};
pub use speculative::{
    inflate_block_speculative, inflate_speculative, resolve_markers, tail_window, Marker,
    SpeculativeChunk,
};
pub use speculative_zlib::SpeculativeZlibDecoder;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeflateError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid deflate stream: {0}")]
    Invalid(&'static str),
}
