//! Single-threaded inflate-kernel microbenchmark.
//!
//! Purpose: a sensitive, reproducible yardstick for the hot DEFLATE decode
//! loops in `fast_inflate.rs` — the place where (almost) all of this crate's
//! `unsafe` lives. The top-level `rusty-rapidgzip` throughput bench exercises
//! the full *parallel* pipeline, whose thread scheduling noise swamps the
//! small per-byte deltas a localized unsafe→safe rewrite produces. This bench
//! drives each kernel directly, one thread, all input resident in RAM, so a
//! regression in the inner loop shows up cleanly in the before/after diff.
//!
//! Three kernels are measured, covering the whole unsafe surface:
//!   * `inflate_fast`     — serial, window-known fast path.
//!   * `decode_member`    — speculative (marker-tracking) parallel-worker path.
//!   * `decode_until_u16` — windowed u16 sliding-window path.
//!
//! Input is real corpus data from `tests/corpus/*.gz`, re-deflated through the
//! system `gzip` so the gzip frame is a canonical 10-byte header + 8-byte
//! trailer we can strip to recover the raw DEFLATE stream. Throughput is
//! reported over *decompressed* bytes (what the kernel actually produces).
//!
//! Run all kernels over the default fixture set:
//!     cargo bench -p rusty-rapidgzip-deflate --bench inflate_kernels
//! Pick fixtures explicitly (comma-separated basenames, with or without `.gz`):
//!     RAPIDGZIP_BENCH_FILES=canterbury-large-bible.txt,silesia-dickens \
//!         cargo bench -p rusty-rapidgzip-deflate --bench inflate_kernels

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use rusty_rapidgzip_deflate::fast_inflate::{decode_member, decode_until_u16, inflate_fast};
use rusty_rapidgzip_deflate::speculative::SpeculativeChunk;
use rusty_rapidgzip_deflate::BitReader;

/// Default corpus fixtures: one English-text, one DNA (many short matches),
/// one prose (longer matches), one binary spreadsheet. Override with the
/// `RAPIDGZIP_BENCH_FILES` env var.
const DEFAULT_FILES: &[&str] = &[
    "canterbury-large-bible.txt.gz",
    "canterbury-large-E.coli.gz",
    "silesia-dickens.gz",
    "canterbury-kennedy.xls.gz",
];

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .unwrap()
        .join("tests/corpus")
}

/// Pipe `input` through `gzip <args>` and return stdout.
fn run_gzip(args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = Command::new("gzip")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn gzip (is it on PATH?)");
    let mut stdin = child.stdin.take().unwrap();
    let buf = input.to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&buf).unwrap());
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(out.status.success(), "gzip {args:?} failed");
    out.stdout
}

/// Recover a raw DEFLATE stream + its decompressed length from a corpus `.gz`.
///
/// We first decompress to the original payload, then re-compress with
/// `gzip -6 -c -n`. The `-n` guarantees no filename field, so the header is
/// exactly 10 bytes and the trailer exactly 8 (CRC32 + ISIZE) — matching this
/// crate's own test fixtures. Slicing `[10 .. len-8]` yields raw DEFLATE.
fn raw_deflate(path: &Path) -> (Vec<u8>, usize) {
    let gz = std::fs::read(path).expect("read corpus fixture");
    let payload = run_gzip(&["-d", "-c"], &gz);
    let recompressed = run_gzip(&["-6", "-c", "-n"], &payload);
    let mut raw = recompressed[10..recompressed.len() - 8].to_vec();
    // The fast bit reader refills in multi-byte chunks and may read a few
    // bytes past the logical end of the stream. Real callers always have the
    // gzip trailer (and often more) following the DEFLATE body, so this slack
    // is present in practice; mirror the crate's own tests by appending it.
    raw.extend_from_slice(&[0u8; 32]);
    (raw, payload.len())
}

struct Fixture {
    name: String,
    raw: Vec<u8>,
    out_len: usize,
}

fn load_fixtures() -> Vec<Fixture> {
    let dir = corpus_dir();
    let files: Vec<String> = match std::env::var("RAPIDGZIP_BENCH_FILES") {
        Ok(v) => v
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s.ends_with(".gz") {
                    s.to_string()
                } else {
                    format!("{s}.gz")
                }
            })
            .collect(),
        Err(_) => DEFAULT_FILES.iter().map(|s| s.to_string()).collect(),
    };

    let mut fixtures = Vec::new();
    for f in files {
        let path = dir.join(&f);
        if !path.exists() {
            eprintln!("skipping missing fixture: {}", path.display());
            continue;
        }
        let (raw, out_len) = raw_deflate(&path);
        // Friendly label: strip the .gz suffix.
        let name = f.strip_suffix(".gz").unwrap_or(&f).to_string();
        fixtures.push(Fixture { name, raw, out_len });
    }
    if fixtures.is_empty() {
        eprintln!(
            "no corpus fixtures available — populate tests/corpus or set \
             RAPIDGZIP_BENCH_FILES; skipping inflate_kernels bench"
        );
    }
    fixtures
}

/// Sanity-check each kernel against the others before timing, so a correctness
/// regression can't masquerade as a speedup.
fn verify(fx: &Fixture) {
    let mut a = Vec::with_capacity(fx.out_len);
    let mut br = BitReader::new(&fx.raw);
    inflate_fast(&mut br, &mut a).expect("inflate_fast decode");
    assert_eq!(a.len(), fx.out_len, "{}: inflate_fast length", fx.name);

    let mut chunk = SpeculativeChunk::new();
    decode_member(&fx.raw, 0, &mut chunk).expect("decode_member decode");
    assert!(chunk.markers.is_empty(), "{}: unexpected markers", fx.name);
    assert_eq!(chunk.bytes, a, "{}: decode_member mismatch", fx.name);

    let mut chunk_u16 = SpeculativeChunk::new();
    let mut scratch = Vec::new();
    decode_until_u16(&fx.raw, 0, u64::MAX, &mut chunk_u16, &mut scratch)
        .expect("decode_until_u16 decode");
    assert_eq!(chunk_u16.bytes, a, "{}: decode_until_u16 mismatch", fx.name);
}

fn bench(c: &mut Criterion) {
    let fixtures = load_fixtures();

    for fx in &fixtures {
        verify(fx);
    }

    let mut serial = c.benchmark_group("inflate_fast");
    for fx in &fixtures {
        serial.throughput(Throughput::Bytes(fx.out_len as u64));
        serial.bench_with_input(BenchmarkId::from_parameter(&fx.name), fx, |b, fx| {
            b.iter(|| {
                let mut out = Vec::with_capacity(fx.out_len);
                let mut br = BitReader::new(&fx.raw);
                inflate_fast(&mut br, &mut out).unwrap();
                black_box(out.len())
            });
        });
    }
    serial.finish();

    let mut spec = c.benchmark_group("decode_member");
    for fx in &fixtures {
        spec.throughput(Throughput::Bytes(fx.out_len as u64));
        spec.bench_with_input(BenchmarkId::from_parameter(&fx.name), fx, |b, fx| {
            b.iter(|| {
                let mut chunk = SpeculativeChunk::new();
                decode_member(&fx.raw, 0, &mut chunk).unwrap();
                black_box(chunk.bytes.len())
            });
        });
    }
    spec.finish();

    let mut windowed = c.benchmark_group("decode_until_u16");
    for fx in &fixtures {
        windowed.throughput(Throughput::Bytes(fx.out_len as u64));
        windowed.bench_with_input(BenchmarkId::from_parameter(&fx.name), fx, |b, fx| {
            // Reuse the scratch window across iterations, matching the worker's
            // own buffer-recycling — we measure decode, not allocation.
            let mut scratch = Vec::new();
            b.iter(|| {
                let mut chunk = SpeculativeChunk::new();
                decode_until_u16(&fx.raw, 0, u64::MAX, &mut chunk, &mut scratch).unwrap();
                black_box(chunk.bytes.len())
            });
        });
    }
    windowed.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
