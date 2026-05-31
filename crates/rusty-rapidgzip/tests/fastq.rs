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
