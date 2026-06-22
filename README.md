# rusty-rapidgzip

A streaming, parallel gzip decoder for Rust, inspired by [rapidgzip][1] / [pugz][2].

Absolutely vibed up with Claude - but hey, gzip inflate is well understood
problem, right?

## Status

Performance is about even with rapidgzip (cpp).

The crate passes a limited size test corpus. 

Miri has been applied to the limited (but unavoidable) use of unsafe.
Unsafe is used to write to allocated but uninitialized data,
and to optimize one unaligned read.

The library has been fuzzed using afl, exercising our
'no panic on invalid input data' policy and verifying
identical output between our fast (unsafe) deflate implementation
and a safe unoptimized 'text book' implementation.


## Requirements

64-bit systems only. The decoder assumes a 64-bit address space (it mmaps
whole input files and works in `u64` bit offsets) and is neither built nor
tested on 32-bit targets.

## Basic API

Decompress huge `.gz` files in parallel and stream the decompressed bytes
back to the caller through a bounded channel:

```rust
use std::sync::Arc;
use crossbeam_channel::bounded;
use rusty_rapidgzip::{read_gz, Config};

let (tx, rx) = bounded::<Arc<Vec<u8>>>(16);
std::thread::spawn(move || read_gz("huge.gz", tx, Config::default()));
for chunk in rx { /* process bytes in stream order */ }
```

No random access, no `Read`/`Seek`, no upstream-compatible `.gzi` (for now).

This has the advantage of applying back-pressure easily, so 
no strict need to fine tune thread counts.

## Environment variables

A few knobs are read from the environment. All are optional; the defaults are
what you want in almost every case.

- **`RAPIDGZIP_INFLIGHT`** — caps how many decoded chunks may be outstanding
  between dispatch and the sink, which bounds peak RSS under a slow consumer.
  On by default at a factor of `2` chunks per worker (floored at 16 chunks).
  Set to `0` to disable the cap entirely; any other integer overrides the
  factor; unparsable values fall back to the default.

[1]: https://github.com/mxmlnkn/rapidgzip
[2]: https://github.com/Piezoid/pugz

## Development

See [DEV.txt](DEV.txt) for how to build, test, fuzz, benchmark, and run the
Miri undefined-behavior checks (`nix build .#checks.x86_64-linux.miri`).

## Alternatives

Besides just running rapidgzip in an external process,
there's also a set of [rust bindings](https://github.com/alekseizarubin/rapidgzip-rs/)!


## Minimum supported rust version

1.85

