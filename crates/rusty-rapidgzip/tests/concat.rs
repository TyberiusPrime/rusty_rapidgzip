//! Concatenated (multi-member) gzip tests.
//!
//! A gzip file may be the concatenation of several independent members; the
//! decoder must detect each member boundary, validate every member's CRC32 +
//! ISIZE trailer, reset window state between members, and concatenate the
//! decompressed output in order. These tests exercise that across:
//!   * both serial (`-P1`) and parallel decode paths of [`read_gz`],
//!   * the byte-slice serial path [`decode_all`],
//!   * member sizes/levels chosen so member boundaries fall in the *middle* of
//!     speculative chunks (the historically bug-prone case).
//!
//! All tests that need the system `gzip` skip cleanly when it is unavailable.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crossbeam_channel::bounded;
use rusty_rapidgzip::{decode_all, read_gz, Config};

/// Compress `payload` into a single gzip member via the system `gzip`.
/// Returns `None` if `gzip` is not available, so callers can skip.
fn gzip_member(payload: &[u8], level: u32) -> Option<Vec<u8>> {
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

/// Concatenate several gzip members into one multi-member stream.
fn concat_members(payloads: &[(&[u8], u32)]) -> Option<Vec<u8>> {
    let mut gz = Vec::new();
    for (p, level) in payloads {
        gz.extend_from_slice(&gzip_member(p, *level)?);
    }
    Some(gz)
}

fn write_tmp(name: &str, data: &[u8]) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rapidgzip_rs_concat_{name}"));
    std::fs::write(&path, data).unwrap();
    path
}

fn decode_via_read_gz(path: &Path, cfg: Config) -> Vec<u8> {
    let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
    let path = path.to_owned();
    let producer = std::thread::spawn(move || read_gz(&path, tx, cfg));
    let mut out = Vec::new();
    for chunk in rx {
        out.extend_from_slice(&chunk);
    }
    producer
        .join()
        .expect("producer panicked")
        .expect("read_gz");
    out
}

/// Deterministic pseudo-random ASCII payload of length `n` (seeded by `seed`).
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

#[test]
fn concat_hello_world_all_paths() {
    let Some(gz) = concat_members(&[(b"Hello", 6), (b"World", 6)]) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    // Byte-slice serial path.
    let mut out = Vec::new();
    decode_all(&gz, &mut out).unwrap();
    assert_eq!(out, b"HelloWorld");

    // File-based serial + parallel paths.
    let path = write_tmp("hello_world.gz", &gz);
    for threads in [1usize, 2, 4] {
        let cfg = Config {
            num_threads: threads,
            ..Config::default()
        };
        assert_eq!(
            decode_via_read_gz(&path, cfg),
            b"HelloWorld",
            "threads={threads}"
        );
    }
}

#[test]
fn concat_empty_members_interspersed() {
    // Zero-length members are valid 20-byte gzip streams; the decoder must emit
    // nothing for them and still concatenate the rest in order.
    let Some(gz) = concat_members(&[
        (b"", 6),
        (b"alpha", 6),
        (b"", 9),
        (b"", 1),
        (b"beta", 6),
        (b"", 6),
    ]) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    let mut out = Vec::new();
    decode_all(&gz, &mut out).unwrap();
    assert_eq!(out, b"alphabeta");

    let path = write_tmp("empty_interspersed.gz", &gz);
    for threads in [1usize, 4] {
        let cfg = Config {
            num_threads: threads,
            ..Config::default()
        };
        assert_eq!(
            decode_via_read_gz(&path, cfg),
            b"alphabeta",
            "threads={threads}"
        );
    }
}

#[test]
fn concat_mixed_levels_and_sizes() {
    // Members of varied sizes and compression levels concatenated; sizes are
    // not multiples of any chunk size, so member boundaries fall mid-chunk.
    let payloads: Vec<(Vec<u8>, u32)> = vec![
        (ascii_payload(1, 1), 6),
        (ascii_payload(100, 2), 1),
        (ascii_payload(65_537, 3), 9),
        (ascii_payload(1, 4), 6),
        (ascii_payload(300_000, 5), 6),
        (ascii_payload(7, 6), 1),
        (ascii_payload(1_000_000, 7), 9),
    ];
    let refs: Vec<(&[u8], u32)> = payloads.iter().map(|(p, l)| (p.as_slice(), *l)).collect();
    let Some(gz) = concat_members(&refs) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    let mut expected = Vec::new();
    for (p, _) in &payloads {
        expected.extend_from_slice(p);
    }

    let mut out = Vec::new();
    decode_all(&gz, &mut out).unwrap();
    assert_eq!(out, expected, "decode_all");

    let path = write_tmp("mixed.gz", &gz);
    for threads in [1usize, 2, 4] {
        // A small chunk size makes several speculative chunk boundaries land
        // inside the larger members.
        let cfg = Config {
            num_threads: threads,
            chunk_size_bytes: 64 * 1024,
            ..Config::default()
        };
        assert_eq!(
            decode_via_read_gz(&path, cfg),
            expected,
            "threads={threads}"
        );
    }
}

#[test]
fn concat_many_small_members() {
    // Many tiny members stress per-member header/trailer handling and the
    // member-boundary bookkeeping in the parallel pipeline.
    let payloads: Vec<Vec<u8>> = (0..500u32)
        .map(|i| format!("member-{i}-payload\n").into_bytes())
        .collect();
    let refs: Vec<(&[u8], u32)> = payloads.iter().map(|p| (p.as_slice(), 6u32)).collect();
    let Some(gz) = concat_members(&refs) else {
        eprintln!("system gzip unavailable — skipping");
        return;
    };
    let mut expected = Vec::new();
    for p in &payloads {
        expected.extend_from_slice(p);
    }

    let mut out = Vec::new();
    decode_all(&gz, &mut out).unwrap();
    assert_eq!(out, expected, "decode_all");

    let path = write_tmp("many_small.gz", &gz);
    for threads in [1usize, 4] {
        let cfg = Config {
            num_threads: threads,
            ..Config::default()
        };
        assert_eq!(
            decode_via_read_gz(&path, cfg),
            expected,
            "threads={threads}"
        );
    }
}

/// If the committed `synth-concat-members.gz` corpus fixture is present, it must
/// decode to `HelloWorld`. Skips on a fresh checkout without the corpus.
#[test]
fn corpus_concat_fixture_if_present() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("tests/corpus/synth-concat-members.gz");
    if !fixture.exists() {
        eprintln!("no corpus fixture — skipping");
        return;
    }
    for threads in [1usize, 4] {
        let cfg = Config {
            num_threads: threads,
            ..Config::default()
        };
        assert_eq!(
            decode_via_read_gz(&fixture, cfg),
            b"HelloWorld",
            "threads={threads}"
        );
    }
}
