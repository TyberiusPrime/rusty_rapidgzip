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

use crate::deflate::fast_inflate::{
    decode_four_distinct, decode_four_interleaved, decode_member_stepwise, decode_member_u8,
    decode_member_u8_preload, decode_three_interleaved, decode_two_interleaved, decode_until_u16,
};
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

/// Single-stream stepwise baseline (one symbol per loop iter, refill each time).
/// Reference point for the interleave ratio — NOT the production preload number.
fn bench_stepwise(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut out = Vec::with_capacity(plainlen + 4096);
    out.clear();
    decode_member_stepwise(body, &mut out).unwrap();
    assert_eq!(out.len(), plainlen, "stepwise: output length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        out.clear();
        decode_member_stepwise(body, &mut out).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (plainlen as f64 * iters as f64) / secs / 1e6
}

/// Two streams, sequential (decode member A fully, then B). The honest baseline
/// for the interleave comparison: same total work, same per-symbol code.
fn bench_two_sequential(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut a = Vec::with_capacity(plainlen + 4096);
    let mut b = Vec::with_capacity(plainlen + 4096);
    a.clear();
    b.clear();
    decode_member_stepwise(body, &mut a).unwrap();
    decode_member_stepwise(body, &mut b).unwrap();
    assert_eq!(a.len(), plainlen);
    assert_eq!(b.len(), plainlen);

    let t = Instant::now();
    for _ in 0..iters {
        a.clear();
        b.clear();
        decode_member_stepwise(body, &mut a).unwrap();
        decode_member_stepwise(body, &mut b).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (2.0 * plainlen as f64 * iters as f64) / secs / 1e6
}

/// Two streams, interleaved one symbol each per fused step (the experiment).
fn bench_two_interleaved(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut a = Vec::with_capacity(plainlen + 4096);
    let mut b = Vec::with_capacity(plainlen + 4096);
    a.clear();
    b.clear();
    decode_two_interleaved(body, &mut a, body, &mut b).unwrap();
    assert_eq!(a.len(), plainlen, "interleaved A: length mismatch");
    assert_eq!(b.len(), plainlen, "interleaved B: length mismatch");

    let t = Instant::now();
    for _ in 0..iters {
        a.clear();
        b.clear();
        decode_two_interleaved(body, &mut a, body, &mut b).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (2.0 * plainlen as f64 * iters as f64) / secs / 1e6
}

/// Four streams interleaved (find the MLP ceiling).
fn bench_four_interleaved(body: &[u8], plainlen: usize, iters: usize) -> f64 {
    let mut outs: [Vec<u8>; 4] =
        std::array::from_fn(|_| Vec::with_capacity(plainlen + 4096));
    decode_four_interleaved(body, &mut outs).unwrap();
    for o in &outs {
        assert_eq!(o.len(), plainlen, "interleaved4: length mismatch");
    }

    let t = Instant::now();
    for _ in 0..iters {
        for o in &mut outs {
            o.clear();
        }
        decode_four_interleaved(body, &mut outs).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    (4.0 * plainlen as f64 * iters as f64) / secs / 1e6
}

/// Load the four DISTINCT deflate members (prepared out-of-band: decompress the
/// main fixture, split plain into 4 quarters, recompress each as raw deflate).
fn load_distinct() -> Vec<(Vec<u8>, usize)> {
    (0..4)
        .map(|i| {
            let mut body =
                std::fs::read(format!("/tmp/fastq_member_{i}.deflate")).expect("distinct fixture");
            body.extend_from_slice(&[0u8; 64]);
            let plainlen: usize =
                std::fs::read_to_string(format!("/tmp/fastq_member_{i}.plainlen"))
                    .expect("distinct plainlen")
                    .trim()
                    .parse()
                    .unwrap();
            (body, plainlen)
        })
        .collect()
}

/// Validation on DISTINCT working sets: stepwise baseline vs 2/3/4-way interleave,
/// each stream a different deflate member. This is the ship-or-not test (the `il2`
/// bench reuses one fixture, sharing the compressed footprint in cache).
fn bench_distinct(iters: usize) {
    let fx = load_distinct();
    let total_plain: usize = fx.iter().map(|(_, p)| *p).sum();
    let bodies: Vec<&[u8]> = fx.iter().map(|(b, _)| b.as_slice()).collect();
    let plains: Vec<usize> = fx.iter().map(|(_, p)| *p).collect();

    let mut bufs: Vec<Vec<u8>> = fx.iter().map(|(_, p)| Vec::with_capacity(p + 4096)).collect();

    // 1x stepwise: sum of decoding all four members one after another.
    for (i, b) in bodies.iter().enumerate() {
        bufs[i].clear();
        decode_member_stepwise(b, &mut bufs[i]).unwrap();
        assert_eq!(bufs[i].len(), plains[i], "distinct stepwise len mismatch");
    }
    let t = Instant::now();
    for _ in 0..iters {
        for (i, b) in bodies.iter().enumerate() {
            bufs[i].clear();
            decode_member_stepwise(b, &mut bufs[i]).unwrap();
        }
    }
    let s1 = (total_plain as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    // 2-way interleave (members 0&1, then 2&3), distinct inputs.
    let t = Instant::now();
    for _ in 0..iters {
        for c in bufs.iter_mut() {
            c.clear();
        }
        let (a, rest) = bufs.split_at_mut(1);
        let (b, rest) = rest.split_at_mut(1);
        let (cc, dd) = rest.split_at_mut(1);
        decode_two_interleaved(bodies[0], &mut a[0], bodies[1], &mut b[0]).unwrap();
        decode_two_interleaved(bodies[2], &mut cc[0], bodies[3], &mut dd[0]).unwrap();
    }
    let i2 = (total_plain as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    // 3-way interleave (members 0,1,2; member 3 stepwise tail).
    let three_plain = plains[0] + plains[1] + plains[2];
    let t = Instant::now();
    for _ in 0..iters {
        for c in bufs.iter_mut() {
            c.clear();
        }
        let (a, rest) = bufs.split_at_mut(1);
        let (b, rest) = rest.split_at_mut(1);
        let (cc, _) = rest.split_at_mut(1);
        decode_three_interleaved(
            bodies[0], &mut a[0], bodies[1], &mut b[0], bodies[2], &mut cc[0],
        )
        .unwrap();
    }
    let i3 = (three_plain as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    // 4-way interleave, all four distinct.
    let mut outs4: [Vec<u8>; 4] = std::array::from_fn(|i| Vec::with_capacity(plains[i] + 4096));
    decode_four_distinct([bodies[0], bodies[1], bodies[2], bodies[3]], &mut outs4).unwrap();
    for (i, o) in outs4.iter().enumerate() {
        assert_eq!(o.len(), plains[i], "distinct 4-way len mismatch");
    }
    let t = Instant::now();
    for _ in 0..iters {
        for o in outs4.iter_mut() {
            o.clear();
        }
        decode_four_distinct([bodies[0], bodies[1], bodies[2], bodies[3]], &mut outs4).unwrap();
    }
    let i4 = (total_plain as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    // CONTROL: same function (decode_four_distinct) but fed ONE member 4x. If this
    // reproduces ~1.78x while the distinct run is ~1.0x, the gain was a same-input
    // cache/lockstep artifact, not the kernel structure.
    let one = bodies[0];
    let p0 = plains[0];
    let mut outs_same: [Vec<u8>; 4] = std::array::from_fn(|_| Vec::with_capacity(p0 + 4096));
    decode_four_distinct([one, one, one, one], &mut outs_same).unwrap();
    let t = Instant::now();
    for _ in 0..iters {
        for o in outs_same.iter_mut() {
            o.clear();
        }
        decode_four_distinct([one, one, one, one], &mut outs_same).unwrap();
    }
    let i4same = (4.0 * p0 as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    // CONTROL 2: four DISTINCT buffer copies of identical content (distinct
    // addresses, identical control flow). Separates address-sharing (input cache)
    // from control-flow lockstep (branch prediction).
    let copies: Vec<Vec<u8>> = (0..4).map(|_| one.to_vec()).collect();
    let copy_refs: [&[u8]; 4] = [&copies[0], &copies[1], &copies[2], &copies[3]];
    let mut outs_copy: [Vec<u8>; 4] = std::array::from_fn(|_| Vec::with_capacity(p0 + 4096));
    decode_four_distinct(copy_refs, &mut outs_copy).unwrap();
    let t = Instant::now();
    for _ in 0..iters {
        for o in outs_copy.iter_mut() {
            o.clear();
        }
        decode_four_distinct(copy_refs, &mut outs_copy).unwrap();
    }
    let i4copy = (4.0 * p0 as f64 * iters as f64) / t.elapsed().as_secs_f64() / 1e6;

    eprintln!("DISTINCT working sets (4 different deflate members):");
    eprintln!("4x SAME-input : {i4same:8.1} MB/s  (control: shared addr + lockstep)");
    eprintln!("4x copy-input : {i4copy:8.1} MB/s  (control: distinct addr, lockstep)");
    eprintln!("1x stepwise   : {s1:8.1} MB/s  (1.00x)");
    eprintln!("2x interleaved: {i2:8.1} MB/s  ({:.2}x)", i2 / s1);
    eprintln!("3x interleaved: {i3:8.1} MB/s  ({:.2}x)", i3 / s1);
    eprintln!("4x interleaved: {i4:8.1} MB/s  ({:.2}x)", i4 / s1);
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
        "stepwise" => eprintln!(
            "stepwise: {:8.1} MB/s",
            bench_stepwise(&body, plainlen, iters)
        ),
        "il2" => {
            let s = bench_two_sequential(&body, plainlen, iters);
            let i2 = bench_two_interleaved(&body, plainlen, iters);
            let i4 = bench_four_interleaved(&body, plainlen, iters);
            let s1 = bench_stepwise(&body, plainlen, iters);
            eprintln!("1x stepwise   : {s1:8.1} MB/s  (1.00x)");
            eprintln!("2x sequential : {s:8.1} MB/s  ({:.2}x)", s / s1);
            eprintln!("2x interleaved: {i2:8.1} MB/s  ({:.2}x vs 1x)", i2 / s1);
            eprintln!("4x interleaved: {i4:8.1} MB/s  ({:.2}x vs 1x)", i4 / s1);
        }
        "ild" => bench_distinct(iters),
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
