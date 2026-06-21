//! Throwaway A/B microbench: our `u8` kernel vs zlib-rs on a single real
//! deflate member, isolated from the pipeline (no IO, no CRC, no threads).
//!
//! Fixture is prepared out-of-band (see investigation notes):
//!   zcat FASTQ | head -c 96M | gzip -6 -n -c  -> strip 10B hdr + 8B trailer
//!   -> /tmp/fastq_member.deflate  (+ /tmp/fastq_member.plainlen)
//!
//! Run a single kernel (clean perf attribution):
//!   AB_KERNEL=ours  AB_ITERS=20 cargo test --release --features zlib-rs \
//!       -p rusty-rapidgzip kernel_ab::run -- --nocapture --ignored
//!   AB_KERNEL=zlibrs ...
//!   AB_KERNEL=u16    ...   (marker-recording path)
//!
//! Default (no AB_KERNEL) runs all three back-to-back for a quick ratio table.

#![cfg(all(test, feature = "zlib-rs"))]

use std::time::Instant;

use crate::deflate::fast_inflate::{decode_member_u8, decode_member_u8_preload, decode_until_u16};
use crate::deflate::SpeculativeChunk;
use crate::zlibrs_ffi::{self, ZlibOutcome};

fn load_fixture() -> (Vec<u8>, usize) {
    let mut body = std::fs::read("/tmp/fastq_member.deflate").expect("fixture deflate missing");
    body.extend_from_slice(&[0u8; 64]); // refill headroom
    let plainlen: usize = std::fs::read_to_string("/tmp/fastq_member.plainlen")
        .expect("plainlen missing")
        .trim()
        .parse()
        .unwrap();
    (body, plainlen)
}

#[cfg(feature = "zune")]
fn bench_zune(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    use zune_inflate::{DeflateDecoder, DeflateOptions};
    // Decode straight into a reusable scratch (the patched `decode_deflate_into`),
    // isolating the libdeflate-port decode loop — no copy to `out`.
    let mut scratch: Vec<u8> = vec![0u8; plainlen + 4096];
    let opts = DeflateOptions::default().set_size_hint(plainlen + 4096);
    let mut dec = DeflateDecoder::new_with_options(body, opts);
    let len = dec.decode_deflate_into(&mut scratch).unwrap();
    assert_eq!(len, plainlen, "zune: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        let mut dec = DeflateDecoder::new_with_options(body, opts);
        let _ = dec.decode_deflate_into(&mut scratch).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

#[cfg(feature = "isal")]
fn bench_isal(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    use crate::isal_ffi::{decode_member, Decompressor, IsalOutcome};
    let d = Decompressor::default();
    let mut out = Vec::with_capacity(plainlen + 4096);
    out.clear();
    match decode_member(&d, body, 0, u64::MAX, &mut out).unwrap() {
        IsalOutcome::Done(..) => {}
        IsalOutcome::Straddle => panic!("unexpected straddle"),
    }
    assert_eq!(out.len(), plainlen, "isal: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        let _ = decode_member(&d, body, 0, u64::MAX, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

#[cfg(feature = "libdeflate")]
fn bench_libdeflate(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    use crate::libdeflate_ffi::{decode_member, Decompressor, LdOutcome};
    let d = Decompressor::default();
    let mut out = Vec::with_capacity(plainlen + 4096);
    out.clear();
    match decode_member(&d, body, 0, u64::MAX, &mut out).unwrap() {
        LdOutcome::Done(..) => {}
        LdOutcome::Straddle => panic!("unexpected straddle"),
    }
    assert_eq!(out.len(), plainlen, "libdeflate: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        let _ = decode_member(&d, body, 0, u64::MAX, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

fn bench_ours(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut out = Vec::with_capacity(plainlen + 4096);
    // warmup + correctness
    out.clear();
    let _ = decode_member_u8(body, 0, u64::MAX, &mut out).unwrap();
    assert_eq!(out.len(), plainlen, "ours: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        let _ = decode_member_u8(body, 0, u64::MAX, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

fn bench_preload(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut out = Vec::with_capacity(plainlen + 4096);
    out.clear();
    let _ = decode_member_u8_preload(body, 0, u64::MAX, &mut out).unwrap();
    assert_eq!(out.len(), plainlen, "preload: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        let _ = decode_member_u8_preload(body, 0, u64::MAX, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

fn bench_zlibrs(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut d = zlibrs_ffi::Decompressor::default();
    let mut out = Vec::with_capacity(plainlen + 4096);
    out.clear();
    match zlibrs_ffi::decode_member(&mut d, body, 0, u64::MAX, &mut out).unwrap() {
        ZlibOutcome::Done(..) => {}
        ZlibOutcome::Straddle => panic!("unexpected straddle"),
    }
    assert_eq!(out.len(), plainlen, "zlibrs: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        let _ = zlibrs_ffi::decode_member(&mut d, body, 0, u64::MAX, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

/// Mid-stream chunk boundaries (byte/bit offsets) that reproduce the pipeline's
/// marker-saturated regime: each chunk starts speculatively, so back-references
/// reach into the unknown prefix and the u16 path stays engaged (markers keep
/// propagating, handoff to phase-2 almost never fires). Mirrors the real -P5
/// pipeline where 99.7% of bytes decode through phase-1.
fn midstream_chunks(body: &[u8]) -> Vec<(u64, u64)> {
    use crate::deflate::find_next_dynamic_block;
    let total_bits = body.len() as u64 * 8;
    let chunk_bits = 4 * 1024 * 1024u64 * 8; // 4 MiB compressed, the CLI default
    let mut bounds: Vec<u64> = Vec::new();
    let mut c = chunk_bits; // skip the first chunk (it starts at a real member boundary)
    while c < total_bits {
        if let Some(b) = find_next_dynamic_block(body, c, total_bits) {
            if bounds.last() != Some(&b) {
                bounds.push(b);
            }
        }
        c += chunk_bits;
    }
    bounds
        .windows(2)
        .map(|w| (w[0], w[1]))
        .collect()
}

fn bench_u16(body: &[u8], _plainlen: usize, iters: usize) -> f64 {
    let chunks = midstream_chunks(body);
    assert!(chunks.len() >= 4, "need several mid-stream chunks");
    let mut scratch: Vec<u16> = Vec::new();

    // correctness/warmup + count bytes & markers produced
    let mut out_bytes = 0u64;
    let mut markers = 0u64;
    for &(start, end) in &chunks {
        let mut chunk = SpeculativeChunk::default();
        decode_until_u16(body, start, end, &mut chunk, &mut scratch).unwrap();
        out_bytes += chunk.bytes.len() as u64;
        markers += chunk.markers.len() as u64;
    }
    eprintln!(
        "u16 bench: {} mid-stream chunks, {} out bytes, {} markers ({:.0}% of bytes)",
        chunks.len(),
        out_bytes,
        markers,
        markers as f64 / out_bytes as f64 * 100.0
    );

    let t = Instant::now();
    for _ in 0..iters {
        for &(start, end) in &chunks {
            let mut chunk = SpeculativeChunk::default();
            decode_until_u16(body, start, end, &mut chunk, &mut scratch).unwrap();
        }
    }
    let secs = t.elapsed().as_secs_f64();
    (out_bytes as f64 * iters as f64) / secs / 1e6
}

#[test]
#[ignore = "manual microbench; needs /tmp fixture"]
fn run() {
    let (body, plainlen) = load_fixture();
    let iters: usize = std::env::var("AB_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let which = std::env::var("AB_KERNEL").unwrap_or_default();

    eprintln!(
        "fixture: {} deflate bytes -> {} plain bytes, iters={}",
        body.len(),
        plainlen,
        iters
    );

    if std::env::var("AB_BLOCKS").is_ok() {
        use crate::deflate::{inflate::inflate_block, BitReader};
        let mut br = BitReader::new(&body);
        let mut out = Vec::with_capacity(plainlen + 4096);
        let mut n = 0u64;
        loop {
            let bfinal = inflate_block(&mut br, &mut out).unwrap();
            n += 1;
            if bfinal {
                break;
            }
        }
        eprintln!(
            "blocks={} avg_out_per_block={} bytes",
            n,
            out.len() as u64 / n
        );
    }

    match which.as_str() {
        "ours" => eprintln!("ours    : {:8.1} MB/s", bench_ours(&body, plainlen, iters)),
        "preload" => eprintln!(
            "preload : {:8.1} MB/s",
            bench_preload(&body, plainlen, iters)
        ),
        "zlibrs" => eprintln!("zlibrs  : {:8.1} MB/s", bench_zlibrs(&body, plainlen, iters)),
        "u16" => eprintln!("u16     : {:8.1} MB/s", bench_u16(&body, plainlen, iters)),
        #[cfg(feature = "libdeflate")]
        "libdeflate" => eprintln!(
            "libdeflate: {:8.1} MB/s",
            bench_libdeflate(&body, plainlen, iters)
        ),
        #[cfg(feature = "isal")]
        "isal" => eprintln!("isal    : {:8.1} MB/s", bench_isal(&body, plainlen, iters)),
        #[cfg(feature = "zune")]
        "zune" => eprintln!("zune    : {:8.1} MB/s", bench_zune(&body, plainlen, iters)),
        _ => {
            let o = bench_ours(&body, plainlen, iters);
            let p = bench_preload(&body, plainlen, iters);
            let z = bench_zlibrs(&body, plainlen, iters);
            let u = bench_u16(&body, plainlen, iters);
            eprintln!("ours      : {o:8.1} MB/s  (1.00x)");
            eprintln!("preload   : {p:8.1} MB/s  ({:.2}x ours)", p / o);
            eprintln!("zlibrs    : {z:8.1} MB/s  ({:.2}x ours)", z / o);
            #[cfg(feature = "libdeflate")]
            {
                let l = bench_libdeflate(&body, plainlen, iters);
                eprintln!("libdeflate: {l:8.1} MB/s  ({:.2}x ours, {:.2}x zlibrs)", l / o, l / z);
            }
            eprintln!("u16       : {u:8.1} MB/s  ({:.2}x ours)", u / o);
        }
    }
}
