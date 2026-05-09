# rapidgzip_rs

A streaming, parallel gzip decoder for Rust, inspired by [rapidgzip][1] / [pugz][2].

**Status:** very early — Phase 0 scaffolding. Not usable yet.

## Goal

Decompress huge `.gz` files in parallel and stream the decompressed bytes
back to the caller through a bounded channel:

```rust
use crossbeam_channel::bounded;
use rapidgzip::{read_gz, Config};

let (tx, rx) = bounded::<Vec<u8>>(16);
std::thread::spawn(move || read_gz("huge.fastq.gz", tx, Config::default()));
for chunk in rx { /* process bytes in stream order */ }
```

No random access, no `Read`/`Seek`, no upstream-compatible `.gzi`.

[1]: https://github.com/mxmlnkn/rapidgzip
[2]: https://github.com/Piezoid/pugz

## Layout

- `crates/rapidgzip-deflate/` — bit reader, Huffman decoders, DEFLATE inflate (incl. speculative / no-window mode).
- `crates/rapidgzip/` — gzip framing, parallel pipeline, public API.
- `crates/rapidgzip-bin/` — `rapidgzip-rs` CLI.
- `xtask/` — corpus management & golden-hash test harness.
- `tests/corpus/` — test gz files (gitignored, fetched/built by xtask).

## Phases

See the conversation export in `/project/2026-05-09-085803-*.txt` for
the full plan. Currently in **Phase 0**.
