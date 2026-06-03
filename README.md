# rapidgzip_rs

A streaming, parallel gzip decoder for Rust, inspired by [rapidgzip][1] / [pugz][2].

Absolutely vibed up with Claude - but hey, gzip inflate is well understood 
problem, right?

## Status

Performance is getting to be even with rapidgzip (cpp).

Robustness needs a thorough test setup and fuzzying, even though
gzip decoding is self-validating thanks to CRC32.

## Requirements

64-bit systems only. The decoder assumes a 64-bit address space (it mmaps
whole input files and works in `u64` bit offsets) and is neither built nor
tested on 32-bit targets.

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

## Environment variables

A few knobs are read from the environment. All are optional; the defaults are
what you want in almost every case.

- **`RAPIDGZIP_INFLIGHT`** — caps how many decoded chunks may be outstanding
  between dispatch and the sink, which bounds peak RSS under a slow consumer.
  On by default at a factor of `2` chunks per worker (floored at 16 chunks).
  Set to `0` to disable the cap entirely; any other integer overrides the
  factor; unparsable values fall back to the default.

- **`RAPIDGZIP_FASTQ_DEMUX_THREADS`** — number of workers in the FASTQ demux
  pool used by `read_gz_into_fastq` (the stage that splits decoded bytes into
  name/seq/`+`/qual columns). Defaults to `min(num_threads, 4)`, minimum 1.
  Keep it small: the decode pipeline already saturates the cores, so extra
  demux workers just oversubscribe and contend in the allocator.

[1]: https://github.com/mxmlnkn/rapidgzip
[2]: https://github.com/Piezoid/pugz

## Alternatives

Besides just running rapidgzip in an external process,
there's also a set of [rust bindings](https://github.com/alekseizarubin/rapidgzip-rs/)!
