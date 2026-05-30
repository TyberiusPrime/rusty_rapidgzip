//! Speculative (mid-stream / no-window) DEFLATE decode support.
//!
//! This crate provides the marker ABI used by `rusty-rapidgzip-deflate` to
//! resolve back-references that reach before a speculatively-decoded chunk's
//! start. The zlib-rs inflate engine that previously lived here has been
//! removed; the in-tree (`rusty-rapidgzip-deflate`) kernels are the only
//! decoders now.

pub mod speculative;
