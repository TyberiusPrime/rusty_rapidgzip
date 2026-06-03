//! Nested-gzip (gzip-of-gzip) tests — the canonical block-finder false-positive
//! stress case.
//!
//! When you gzip an already-gzipped stream, the inner gzip bytes are
//! incompressible, so the *outer* deflate emits **stored (uncompressed)
//! blocks** whose payload is the raw inner gzip stream. That payload contains
//! genuine dynamic-Huffman block headers (the inner stream's own blocks), which
//! the parallel block finder will happily "find" while scanning the outer
//! stream — even though they are NOT real boundaries of the outer stream.
//!
//! The pipeline must not trust those guesses blindly. It anchors the decode at
//! the true first block and verifies every speculative chunk start against the
//! previous chunk's actual end bit, re-decoding from the correct offset on a
//! mismatch (see `pipeline::parallel_decode_member`). These tests assert:
//!   1. byte-identical output across serial + several parallel thread counts, and
//!   2. that the false-boundary recovery path is actually exercised
//!      (`speculation_failures > 0`) for the nested input at a small chunk size.
//!
//! The rapidgzip-cpp analogue lives in `ChunkData::matchesEncodedOffset` +
//! `GzipChunkFetcher`'s "re-decode from the real offset" fallback. The C++ code
//! even calls out this exact case ("might happen for finding uncompressed
//! deflate blocks because of the byte-padding").
//!
//! All tests skip cleanly when the system `gzip` is unavailable.

use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::bounded;
use rusty_rapidgzip::{decode_all, read_gz, Config, DecodeStats};

/// Compress `payload` into a single gzip member via the system `gzip`.
fn gzip_once(payload: &[u8], level: u32) -> Option<Vec<u8>> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("gzip")
        .args([&format!("-{level}"), "-c", "-n"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdin = child.stdin.take().unwrap();
    let p = payload.to_vec();
    let w = std::thread::spawn(move || stdin.write_all(&p));
    let out = child.wait_with_output().ok()?;
    w.join().ok()?.ok()?;
    out.status.success().then_some(out.stdout)
}

/// Gzip `payload` `depth` times. `layers[i]` is the stream after `i+1` rounds,
/// so `layers.last()` is the fully nested stream and `layers[depth-2]` is what
/// decoding it once must reproduce.
fn gzip_nested(payload: &[u8], depth: usize, level: u32) -> Option<Vec<Vec<u8>>> {
    let mut layers = Vec::with_capacity(depth);
    let mut cur = payload.to_vec();
    for _ in 0..depth {
        cur = gzip_once(&cur, level)?;
        layers.push(cur.clone());
    }
    Some(layers)
}

/// Deterministic pseudo-random ASCII payload (entropy high enough that one gzip
/// pass already yields an incompressible stream, forcing stored outer blocks).
fn ascii_payload(n: usize, seed: u64) -> Vec<u8> {
    let mut s: u64 = seed | 1;
    let mut p = Vec::with_capacity(n);
    while p.len() < n {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        p.push(((s >> 56) as u8 % 95) + 32);
    }
    p
}

fn write_tmp(name: &str, data: &[u8]) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rapidgzip_rs_nested_{name}"));
    std::fs::write(&path, data).unwrap();
    path
}

/// Decode `path` through the file-based pipeline, returning bytes + stats.
fn decode_via_read_gz(path: &std::path::Path, cfg: Config) -> (Vec<u8>, DecodeStats) {
    let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
    let p = path.to_owned();
    let producer = std::thread::spawn(move || read_gz(&p, tx, cfg));
    let mut out = Vec::new();
    for chunk in rx {
        out.extend_from_slice(&chunk);
    }
    let stats = producer.join().expect("producer panicked").expect("read_gz");
    (out, stats)
}

/// Decoding `gzip(gzip(x))` once must reproduce `gzip(x)` exactly, on every
/// path, and the parallel path at a small chunk size must actually hit the
/// false-boundary recovery (the inner stream's headers live inside the outer
/// stream's stored blocks).
#[test]
fn nested_gzip_decodes_correctly_and_triggers_recovery() {
    // Large + high-entropy so the inner stream is several MB of incompressible
    // bytes; the outer gzip then stores it in many uncompressed deflate blocks.
    let original = ascii_payload(8 * 1024 * 1024, 0xC0FFEE);
    let Some(layers) = gzip_nested(&original, 2, 6) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    let inner = &layers[0]; // gzip(original)
    let outer = &layers[1]; // gzip(gzip(original))

    // Byte-slice serial path: decoding the outer stream yields the inner stream.
    let mut serial_out = Vec::new();
    decode_all(outer, &mut serial_out).unwrap();
    assert_eq!(&serial_out, inner, "decode_all(outer) must equal inner");

    let path = write_tmp("double.gz", outer);

    // Tiny chunks force boundary targets to land inside the outer stored blocks,
    // where the finder will latch onto inner-stream headers (false boundaries).
    let mut total_recoveries = 0u64;
    for threads in [1usize, 2, 4, 8] {
        let cfg = Config {
            num_threads: threads,
            chunk_size_bytes: 64 * 1024,
            ..Config::default()
        };
        let (out, stats) = decode_via_read_gz(&path, cfg);
        assert_eq!(&out, inner, "read_gz(outer) must equal inner (threads={threads})");
        total_recoveries += stats.speculation_failures;
    }

    // The parallel paths (threads > 1) must have detected and recovered from at
    // least one false boundary; otherwise this test isn't exercising the path it
    // claims to. (Serial contributes 0 by construction.)
    assert!(
        total_recoveries > 0,
        "expected the block-finder false-boundary recovery to fire on nested gzip, \
         but speculation_failures summed to 0 — the verification path is untested"
    );
    eprintln!("nested gzip: {total_recoveries} false-boundary re-decodes across parallel runs");
}

/// Triple nesting: another incompressible layer, deeper stored-block nesting.
/// Pure correctness guard across paths.
#[test]
fn triple_nested_gzip_decodes_correctly() {
    let original = ascii_payload(2 * 1024 * 1024, 0x1234_5678);
    let Some(layers) = gzip_nested(&original, 3, 9) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    let twice = &layers[1]; // gzip(gzip(original))
    let thrice = &layers[2]; // gzip(gzip(gzip(original)))

    let path = write_tmp("triple.gz", thrice);
    for threads in [1usize, 4] {
        let cfg = Config {
            num_threads: threads,
            chunk_size_bytes: 64 * 1024,
            ..Config::default()
        };
        let (out, _stats) = decode_via_read_gz(&path, cfg);
        assert_eq!(&out, twice, "read_gz(thrice) must equal twice (threads={threads})");
    }
}
