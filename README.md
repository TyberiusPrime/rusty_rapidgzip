# rapidgzip_rs

A streaming, parallel gzip decoder for Rust, inspired by [rapidgzip][1] / [pugz][2].

Absolutely vibed up with Claude - but hey, gzip inflate is well understood 
problem, right?

## Status

Performance is getting to be even with rapidgzip (cpp).

Robustness needs a thorough test setup and fuzzying, even though
gzip decoding is self-validating thanks to CRC32.

## Basic API

Decompress huge `.gz` files in parallel and stream the decompressed bytes
back to the caller through a bounded channel:

```rust
use crossbeam_channel::bounded;
use rapidgzip::{read_gz, Config};

let (tx, rx) = bounded::<Vec<u8>>(16);
std::thread::spawn(move || read_gz("huge.fastq.gz", tx, Config::default()));
for chunk in rx { /* process bytes in stream order */ }
```

No random access, no `Read`/`Seek`, no upstream-compatible `.gzi` (for now).

[1]: https://github.com/mxmlnkn/rapidgzip
[2]: https://github.com/Piezoid/pugz

## Alternatives

Besides just running rapidgzip in an external process,
there's also a set of [rust bindings](https://github.com/alekseizarubin/rapidgzip-rs/)!
