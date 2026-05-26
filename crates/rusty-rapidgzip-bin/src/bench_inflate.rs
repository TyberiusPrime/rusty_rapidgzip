//! Apples-to-apples serial-inflate benchmark.
//!
//! Strips the gzip header off the input file, then times raw DEFLATE-body
//! decompression for every available engine (no threading, no framing,
//! no CRC). Use with: `bench-inflate test.gz [iters]`.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use rusty_rapidgzip::gzip;
use rusty_rapidgzip_deflate::{inflate as intree_inflate, safe_inflate, BitReader};
use rusty_rapidgzip_inflate::{Inflate, InflateFlush, Status};

fn run_safe(body: &[u8], expected_out: usize) {
    let mut out = Vec::with_capacity(expected_out);
    safe_inflate::inflate_into(body, &mut out).expect("safe inflate");
    assert_eq!(out.len(), expected_out, "safe: output size mismatch");
}

fn run_intree(body: &[u8], expected_out: usize) {
    // Pad so the LUT-driven decoder never reads past the body.
    let mut padded = Vec::with_capacity(body.len() + 16);
    padded.extend_from_slice(body);
    padded.extend_from_slice(&[0u8; 16]);
    let mut br = BitReader::new(&padded);
    let mut out = Vec::with_capacity(expected_out);
    intree_inflate(&mut br, &mut out).expect("intree inflate");
    assert_eq!(out.len(), expected_out, "intree: output size mismatch");
}

fn run_zlib(body: &[u8], expected_out: usize) {
    // zlib-rs decompress wants a fixed scratch slice; size it to the full
    // decoded length so we exercise one Finish call (mirrors a hot-path
    // single-shot decode of a known-size BGZF block).
    let mut dec = Inflate::new(false, 15);
    let mut scratch = vec![0u8; expected_out];
    let status = dec
        .decompress(body, &mut scratch, InflateFlush::Finish)
        .expect("zlib inflate");
    assert!(matches!(status, Status::StreamEnd), "zlib: expected StreamEnd, got {status:?}");
    assert_eq!(dec.total_out() as usize, expected_out, "zlib: output size mismatch");
}

fn time<F: FnMut()>(label: &str, iters: u32, body_len: usize, out_len: usize, mut f: F) {
    // Warm-up.
    f();
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = t0.elapsed().as_secs_f64();
    let per_iter = elapsed / iters as f64;
    let mb_in_per_s = (body_len as f64 / per_iter) / (1024.0 * 1024.0);
    let mb_out_per_s = (out_len as f64 / per_iter) / (1024.0 * 1024.0);
    println!(
        "{label:>10}: {per_iter:.4}s/iter — {mb_in_per_s:>8.1} MB/s in, {mb_out_per_s:>8.1} MB/s out",
    );
}

fn main() {
    let mut args = env::args().skip(1);
    let path: PathBuf = args
        .next()
        .map(PathBuf::from)
        .expect("usage: bench-inflate <file.gz> [iters] [engines]");
    let iters: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);
    let filter: Option<String> = args.next();

    let raw = fs::read(&path).expect("read input");
    let header_len = gzip::parse_header(&raw).expect("parse gzip header");
    let body = &raw[header_len..raw.len() - 8];

    // Decode once with the safe engine to learn the output size — used for
    // pre-sizing scratch and validating each engine's result.
    let mut out_ref = Vec::new();
    safe_inflate::inflate_into(body, &mut out_ref).expect("ref decode");
    let out_len = out_ref.len();

    println!(
        "input: {} ({} bytes compressed body, {} bytes uncompressed)",
        path.display(),
        body.len(),
        out_len,
    );
    println!("iters: {iters}");
    println!();

    let want = |name: &str| filter.as_deref().map_or(true, |f| f.split(',').any(|x| x == name));

    if want("safe") {
        time("safe", iters, body.len(), out_len, || run_safe(body, out_len));
    }
    if want("intree") {
        time("intree", iters, body.len(), out_len, || run_intree(body, out_len));
    }
    if want("zlib") {
        time("zlib", iters, body.len(), out_len, || run_zlib(body, out_len));
    }
}
