# rusty-rapidgzip

A streaming, parallel gzip decoder for Rust, inspired by
[rapidgzip](https://github.com/mxmlnkn/rapidgzip) /
[pugz](https://github.com/Piezoid/pugz). Performance is roughly on par with
rapidgzip (C++).

Decompresses large `.gz` files in parallel and streams the decompressed bytes
back to the caller through a bounded channel (so back-pressure is automatic and
thread-count tuning is rarely needed):

```rust
use std::sync::Arc;
use crossbeam_channel::bounded;
use rusty_rapidgzip::{read_gz, Config};

let (tx, rx) = bounded::<Arc<Vec<u8>>>(16);
std::thread::spawn(move || read_gz("huge.gz", tx, Config::default()));
for chunk in rx { /* process bytes in stream order */ }
```

By default, members fully contained in a chunk are decoded through libdeflate
(compiled from vendored C source by `libdeflate-sys`, statically linked — no
external library needed). Build with `--no-default-features` for a pure-Rust
build with no C dependency.

64-bit systems only (the decoder mmaps whole inputs and works in `u64` bit
offsets). No random access / `Read` / `Seek` for now.

The limited `unsafe` (writing into allocated-but-uninitialized buffers, one
unaligned read) is checked under Miri and the fast path is differentially fuzzed
against a safe textbook implementation. See the
[repository](https://github.com/TyberiusPrime/rusty-rapidgzip) for development,
benchmarking and fuzzing docs.

## License

MIT. The code under `src/deflate` is derived from
[zlib-rs](https://github.com/trifectatechfoundation/zlib-rs); see the
[`LICENSE`](https://github.com/TyberiusPrime/rusty-rapidgzip/blob/main/LICENSE)
file for the full notice.
