//! FASTQ columnar split tests.
//!
//! The core invariant is **chunk-boundary independence**: the columns must be
//! identical no matter where the decode chopped the stream (`split` is fed an
//! explicit chunking via the `fastq_split_for_test` hook). `split_invariant`
//! runs many chunkings — including byte-by-byte, which forces the carry-stitch
//! at every position — and asserts they all agree, returning the common result.

use rusty_rapidgzip::fastq_split_for_test as split;

type Cols = (Vec<u8>, Vec<u8>, Vec<u8>);

fn chunkings(data: &[u8]) -> Vec<Vec<&[u8]>> {
    let mut v: Vec<Vec<&[u8]>> = vec![vec![data]]; // whole stream as one chunk
    for sz in [1usize, 2, 3, 5, 7, 11, 64] {
        v.push(data.chunks(sz.max(1)).collect());
    }
    v
}

/// Run every chunking; assert they all agree; return the common result (errors
/// compared by message, since `Error` is not `PartialEq`).
fn split_invariant(data: &[u8]) -> Result<Cols, String> {
    let mut common: Option<Result<Cols, String>> = None;
    for chunks in chunkings(data) {
        let n = chunks.len();
        let r = split(&chunks).map_err(|e| e.to_string());
        match &common {
            None => common = Some(r),
            Some(prev) => assert_eq!(prev, &r, "split depends on chunk boundaries ({n} chunks)"),
        }
    }
    common.unwrap()
}

fn cols(names: &str, seqs: &str, quals: &str) -> Cols {
    (names.as_bytes().to_vec(), seqs.as_bytes().to_vec(), quals.as_bytes().to_vec())
}

#[test]
fn basic_two_records() {
    let data = b"@r1 desc\nACGT\n+\nIIII\n@r2\nGG\n+\n##\n";
    let got = split_invariant(data).unwrap();
    assert_eq!(got, cols("r1 desc\nr2\n", "ACGT\nGG\n", "IIII\n##\n"));
}

#[test]
fn empty_input() {
    assert_eq!(split_invariant(b"").unwrap(), cols("", "", ""));
}

#[test]
fn no_trailing_newline() {
    // Final quality line has no terminating '\n' — still a complete record.
    let data = b"@a\nAC\n+\nII\n@b\nGT\n+\n##";
    let got = split_invariant(data).unwrap();
    assert_eq!(got, cols("a\nb\n", "AC\nGT\n", "II\n##\n"));
}

#[test]
fn at_and_plus_in_quality() {
    // Quality strings deliberately start with '@' and contain '+': a
    // content-based scanner would mis-segment here. Newline-counting must not.
    let data = b"@r1\nACGT\n+\n@!+I\n@r2\nTTTT\n+\n+@?J\n";
    let got = split_invariant(data).unwrap();
    assert_eq!(got, cols("r1\nr2\n", "ACGT\nTTTT\n", "@!+I\n+@?J\n"));
}

#[test]
fn crlf_line_endings() {
    let data = b"@r1\r\nACGT\r\n+\r\nIIII\r\n@r2\r\nAC\r\n+\r\n##\r\n";
    let got = split_invariant(data).unwrap();
    // Trailing '\r' is stripped from every field.
    assert_eq!(got, cols("r1\nr2\n", "ACGT\nAC\n", "IIII\n##\n"));
}

#[test]
fn variable_lengths() {
    let data = b"@a\nA\n+\nI\n@b\nACGTACGT\n+\nIIIIIIII\n@c\nACG\n+\nJJJ\n";
    let got = split_invariant(data).unwrap();
    assert_eq!(got, cols("a\nb\nc\n", "A\nACGTACGT\nACG\n", "I\nIIIIIIII\nJJJ\n"));
}

#[test]
fn many_records_stress() {
    // Enough records that small chunkings split inside almost every one.
    let mut data = Vec::new();
    let mut expect_n = String::new();
    let mut expect_s = String::new();
    let mut expect_q = String::new();
    for i in 0..200u32 {
        let len = 1 + (i as usize % 30);
        let seq: String = std::iter::repeat('A').take(len).collect();
        let qual: String = std::iter::repeat('I').take(len).collect();
        data.extend_from_slice(format!("@read{i}\n{seq}\n+\n{qual}\n").as_bytes());
        expect_n.push_str(&format!("read{i}\n"));
        expect_s.push_str(&format!("{seq}\n"));
        expect_q.push_str(&format!("{qual}\n"));
    }
    let got = split_invariant(&data).unwrap();
    assert_eq!(got, cols(&expect_n, &expect_s, &expect_q));
}

#[test]
fn err_header_not_at() {
    let e = split_invariant(b"r1\nACGT\n+\nIIII\n").unwrap_err();
    assert!(e.contains("'@'"), "unexpected error: {e}");
}

#[test]
fn err_separator_not_plus() {
    let e = split_invariant(b"@r1\nACGT\nX\nIIII\n").unwrap_err();
    assert!(e.contains("'+'"), "unexpected error: {e}");
}

#[test]
fn err_seq_qual_length_mismatch() {
    let e = split_invariant(b"@r1\nACGT\n+\nII\n").unwrap_err();
    assert!(e.contains("length mismatch"), "unexpected error: {e}");
}

// ── Truncated-input errors ───────────────────────────────────────────────────
//
// Each test covers a different truncation point within or between records.
// All should produce an error; the split_invariant helper verifies that the
// error is independent of chunk boundaries.

#[test]
fn err_truncated_mid_header() {
    // Truncated before the first newline — only a partial header.
    let e = split_invariant(b"@r1").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_after_header_newline() {
    // Header complete but no sequence line at all.
    let e = split_invariant(b"@r1\n").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_mid_seq() {
    // Sequence started but no terminating newline.
    let e = split_invariant(b"@r1\nAC").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_after_seq_newline() {
    // Sequence complete but no separator line.
    let e = split_invariant(b"@r1\nACGT\n").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_mid_sep() {
    // Separator started ('+') but no terminating newline.
    let e = split_invariant(b"@r1\nACGT\n+").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_after_sep_newline() {
    // Separator complete but no quality line.
    let e = split_invariant(b"@r1\nACGT\n+\n").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_mid_qual() {
    // Quality line shorter than sequence — caught as a length mismatch.
    let e = split_invariant(b"@r1\nACGT\n+\nII").unwrap_err();
    assert!(e.contains("length mismatch") || e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_second_record_after_complete_first() {
    // First record is complete; second record is truncated at its header line.
    // The stream should error, not silently drop the partial record.
    let e = split_invariant(b"@r1\nACGT\n+\nIIII\n@r2\n").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

#[test]
fn err_truncated_second_record_mid_seq() {
    // First record complete; second record has header and partial sequence.
    let e = split_invariant(b"@r1\nACGT\n+\nIIII\n@r2\nGG").unwrap_err();
    assert!(e.contains("truncated"), "unexpected error: {e}");
}

// ── End-to-end through the real decoder (requires system `gzip`) ────────────

fn gzip(payload: &[u8]) -> Option<Vec<u8>> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("gzip")
        .args(["-c", "-n"])
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

#[test]
fn end_to_end_read_gz_into_fastq() {
    use crossbeam_channel::bounded;
    use rusty_rapidgzip::{read_gz_into_fastq, Config, FastqChunk};

    // Build a multi-record FASTQ, gzip it, decode it through the real pipeline
    // across several chunk sizes/thread counts, and verify columns + the
    // complete-records invariant.
    let mut payload = Vec::new();
    let mut exp_n = String::new();
    let mut exp_s = String::new();
    let mut exp_q = String::new();
    for i in 0..5000u32 {
        let len = 1 + (i as usize % 50);
        let seq: String = std::iter::repeat('C').take(len).collect();
        let qual: String = std::iter::repeat('F').take(len).collect();
        payload.extend_from_slice(format!("@seq.{i} x\n{seq}\n+\n{qual}\n").as_bytes());
        exp_n.push_str(&format!("seq.{i} x\n"));
        exp_s.push_str(&format!("{seq}\n"));
        exp_q.push_str(&format!("{qual}\n"));
    }
    let Some(gz) = gzip(&payload) else {
        eprintln!("system gzip unavailable — skipping end-to-end test");
        return;
    };
    let path = std::env::temp_dir().join("rapidgzip_rs_fastq_e2e.gz");
    std::fs::write(&path, &gz).unwrap();

    for threads in [1usize, 4] {
        for chunk_size in [64 * 1024usize, 1 << 20] {
            let (tx, rx) = bounded::<FastqChunk>(8);
            let cfg = Config { num_threads: threads, chunk_size_bytes: chunk_size, ..Config::default() };
            let p = path.clone();
            let producer = std::thread::spawn(move || read_gz_into_fastq(&p, tx, cfg));

            let (mut n, mut s, mut q) = (Vec::new(), Vec::new(), Vec::new());
            for c in rx {
                // Complete-records invariant: equal-length columns. seq/qual
                // share one metadata column in the DualStringPod, so their
                // counts are structurally equal; assert names match too.
                assert_eq!(c.names.len(), c.reads.len());
                for x in c.names.iter() { n.extend_from_slice(x); n.push(b'\n'); }
                for x in c.reads.iter_seq() { s.extend_from_slice(x); s.push(b'\n'); }
                for x in c.reads.iter_qual() { q.extend_from_slice(x); q.push(b'\n'); }
            }
            producer.join().unwrap().unwrap();
            assert_eq!(n, exp_n.as_bytes(), "names t={threads} cs={chunk_size}");
            assert_eq!(s, exp_s.as_bytes(), "seqs t={threads} cs={chunk_size}");
            assert_eq!(q, exp_q.as_bytes(), "quals t={threads} cs={chunk_size}");
        }
    }
}

/// The streaming-parallel pipe path (>1 worker, non-seekable input) must
/// produce byte-identical output to the mmap path, across many decode chunks
/// and multiple gzip members. This is the regression guard for the boundary
/// cutting + cross-member trailer/header handling in `streaming.rs`.
#[test]
#[cfg(unix)]
fn streaming_parallel_pipe_matches_mmap_multichunk() {
    use crossbeam_channel::bounded;
    use rusty_rapidgzip::{read_gz, Config};
    use std::io::Write;
    use std::sync::Arc;

    // ~2 MB uncompressed of varied (compressible but not trivial) FASTQ so that
    // a 64 KiB chunk target yields many speculative chunks with real markers.
    let mut payload = Vec::new();
    for i in 0..12000u32 {
        let len = 60 + (i as usize * 7) % 90;
        let seq: String = (0..len).map(|j| b"ACGT"[((i as usize + j) * 31) % 4] as char).collect();
        let qual: String = std::iter::repeat('I').take(len).collect();
        payload.extend_from_slice(format!("@read{i} run:1:flow\n{seq}\n+\n{qual}\n").as_bytes());
    }

    // Single-member and two-member (concatenated) inputs.
    let Some(gz_a) = gzip(&payload) else {
        eprintln!("system gzip unavailable — skipping streaming-parallel pipe test");
        return;
    };
    let gz_b = gzip(&payload[..payload.len() / 3]).unwrap();
    let mut gz_multi = gz_a.clone();
    gz_multi.extend_from_slice(&gz_b);

    // Collect the whole decompressed stream from `read_gz` for a given input
    // path + config.
    fn decode(path: &std::path::Path, cfg: Config) -> Vec<u8> {
        let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
        let p = path.to_path_buf();
        let producer = std::thread::spawn(move || read_gz(&p, tx, cfg));
        let mut out = Vec::new();
        for chunk in rx {
            out.extend_from_slice(&chunk);
        }
        producer.join().unwrap().unwrap();
        out
    }

    for (label, gz) in [("single", &gz_a), ("multi", &gz_multi)] {
        // Reference: mmap path on a regular file.
        let file_path = std::env::temp_dir().join(format!("rgz_stream_{label}.gz"));
        std::fs::write(&file_path, gz).unwrap();
        let reference = decode(
            &file_path,
            Config { num_threads: 4, chunk_size_bytes: 64 * 1024, ..Config::default() },
        );
        let _ = std::fs::remove_file(&file_path);

        // Streaming-parallel path: same bytes through a named pipe.
        let pipe_path = std::env::temp_dir().join(format!("rgz_stream_{label}.fifo"));
        let _ = std::fs::remove_file(&pipe_path);
        let status = std::process::Command::new("mkfifo").arg(&pipe_path).status().expect("mkfifo");
        assert!(status.success(), "mkfifo failed");

        let writer_path = pipe_path.clone();
        let gz_owned = gz.clone();
        let writer = std::thread::spawn(move || {
            let mut f = std::fs::OpenOptions::new().write(true).open(&writer_path).unwrap();
            f.write_all(&gz_owned).unwrap();
        });

        let got = decode(
            &pipe_path,
            Config { num_threads: 4, chunk_size_bytes: 64 * 1024, ..Config::default() },
        );
        writer.join().expect("writer panicked");
        let _ = std::fs::remove_file(&pipe_path);

        assert_eq!(got, reference, "{label}: streaming-parallel pipe != mmap");
        assert!(got.len() > 1_000_000, "{label}: expected a multi-chunk payload");
    }
}

/// read_gz_into_fastq must work when the input arrives through a named pipe
/// (no mmap available). This exercises the buffered-read fallback.
#[test]
#[cfg(unix)]
fn end_to_end_read_gz_into_fastq_named_pipe() {
    use crossbeam_channel::bounded;
    use rusty_rapidgzip::{read_gz_into_fastq, Config, FastqChunk};
    use std::io::Write;
    use std::process::Command;

    let mut payload = Vec::new();
    let mut exp_n = String::new();
    let mut exp_s = String::new();
    let mut exp_q = String::new();
    for i in 0..500u32 {
        let len = 4 + (i as usize % 20);
        let seq: String = std::iter::repeat('G').take(len).collect();
        let qual: String = std::iter::repeat('J').take(len).collect();
        payload.extend_from_slice(format!("@r{i}\n{seq}\n+\n{qual}\n").as_bytes());
        exp_n.push_str(&format!("r{i}\n"));
        exp_s.push_str(&format!("{seq}\n"));
        exp_q.push_str(&format!("{qual}\n"));
    }
    let Some(gz) = gzip(&payload) else {
        eprintln!("system gzip unavailable — skipping named-pipe fastq test");
        return;
    };

    let pipe_path = std::env::temp_dir().join("rapidgzip_rs_fastq_pipe.fifo");
    let _ = std::fs::remove_file(&pipe_path);
    let status = Command::new("mkfifo").arg(&pipe_path).status().expect("mkfifo");
    assert!(status.success(), "mkfifo failed");

    let writer_path = pipe_path.clone();
    let writer = std::thread::spawn(move || {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&writer_path)
            .expect("open pipe write end");
        f.write_all(&gz).expect("write gz to pipe");
    });

    let (tx, rx) = bounded::<FastqChunk>(8);
    let cfg = Config { num_threads: 2, ..Config::default() };
    let p = pipe_path.clone();
    let producer = std::thread::spawn(move || read_gz_into_fastq(&p, tx, cfg));

    let (mut n, mut s, mut q) = (Vec::new(), Vec::new(), Vec::new());
    for c in rx {
        assert_eq!(c.names.len(), c.reads.len());
        for x in c.names.iter() { n.extend_from_slice(x); n.push(b'\n'); }
        for x in c.reads.iter_seq() { s.extend_from_slice(x); s.push(b'\n'); }
        for x in c.reads.iter_qual() { q.extend_from_slice(x); q.push(b'\n'); }
    }
    writer.join().expect("writer thread panicked");
    producer.join().unwrap().unwrap();

    let _ = std::fs::remove_file(&pipe_path);
    assert_eq!(n, exp_n.as_bytes(), "names");
    assert_eq!(s, exp_s.as_bytes(), "seqs");
    assert_eq!(q, exp_q.as_bytes(), "quals");
}
