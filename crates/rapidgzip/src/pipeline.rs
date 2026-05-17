//! Parallel speculative decode pipeline.
//!
//! Three roles, all running concurrently:
//!
//! 1. **Boundary finder** (main thread, before workers start): scans the
//!    compressed body for dynamic-Huffman block boundaries at roughly
//!    `chunk_size_bytes` intervals. Produces a list of bit offsets.
//! 2. **Workers**: each takes one `(chunk_id, start_bit, end_bit_hint)` and
//!    runs [`inflate_block_speculative`] block-by-block until `end_bit_hint`
//!    is reached or BFINAL is seen. Emits a `SpeculativeChunk`.
//! 3. **Serializer**: receives chunks out of order, buffers them in a
//!    `BTreeMap`, and processes them in `chunk_id` order — resolves markers
//!    against the running 32 KiB tail, then streams resolved bytes to the
//!    caller's sink. Also validates CRC32 + ISIZE in the trailer when the
//!    final block is reached.
//!
//! ## Bounds & assumptions
//!
//! - Single-member gzip only. The caller (`read_gz`) detects multi-stream
//!   and falls back to the serial path for those.
//! - If the block finder fails to locate enough internal boundaries, we
//!   silently degrade to fewer (or one) chunks. A degenerate decode is
//!   serial-equivalent.
//! - On any worker error, the whole pipeline returns that error. We don't
//!   attempt false-boundary recovery in this phase; the block finder's
//!   full-header verification keeps false positives extremely rare in
//!   practice. Phase 5 can add a serial-fallback for the affected range.

use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Sender};

use rapidgzip_deflate::{
    find_next_dynamic_block, inflate_block_speculative, resolve_markers,
    BitReader, SpeculativeChunk,
};

use crate::{Config, Error};

/// One unit of speculative decode work.
struct WorkItem {
    id: u64,
    start_bit: u64,
    /// Exclusive upper bound on `tell_bit()` after each block. The worker
    /// stops when `tell_bit() >= end_bit_hint` after completing a block, or
    /// when BFINAL is reached. `u64::MAX` for the last chunk.
    end_bit_hint: u64,
}

struct WorkResult {
    id: u64,
    chunk: SpeculativeChunk,
    /// Bit position after the worker stopped (start of next chunk if not
    /// final, else the bit position immediately after BFINAL's block).
    end_bit: u64,
    final_block: bool,
}

/// Decode the deflate body of a single gzip member in parallel.
///
/// `body` is the slice from end-of-gzip-header to end-of-file. The caller
/// has not yet consumed the trailer; this function consumes it.
///
/// On success, returns `(uncompressed_bytes, body_bytes_consumed)` —
/// `body_bytes_consumed` includes the 8-byte trailer.
pub fn parallel_decode_member(
    body: Arc<Vec<u8>>,
    sink: &Sender<Vec<u8>>,
    config: &Config,
) -> Result<(u64, usize), Error> {
    let num_threads = effective_threads(config);
    let verbose = config.verbose.is_on();
    let total_bits = (body.len() as u64) * 8;
    let chunk_bits = (config.chunk_size_bytes as u64).max(64 * 1024) * 8;

    // Phase 4a: find block boundaries. Always include 0; then scan forward
    // every `chunk_bits` for the next valid dynamic-block start.
    let mut boundaries: Vec<u64> = vec![0];
    let mut cursor = chunk_bits;
    while cursor < total_bits {
        match find_next_dynamic_block(&body, cursor, total_bits) {
            Some(b) if b > *boundaries.last().unwrap() => {
                boundaries.push(b);
                cursor = b + chunk_bits;
            }
            _ => break,
        }
    }

    // Build work items. The last item has no end_bit_hint (decode until BFINAL).
    let mut work_items: Vec<WorkItem> = Vec::with_capacity(boundaries.len());
    for i in 0..boundaries.len() {
        let start_bit = boundaries[i];
        let end_bit_hint = boundaries
            .get(i + 1)
            .copied()
            .unwrap_or(u64::MAX);
        work_items.push(WorkItem { id: i as u64, start_bit, end_bit_hint });
    }
    let num_chunks = work_items.len();
    if verbose {
        eprintln!(
            "[rapidgzip] pipeline: {} boundaries found → {num_chunks} chunk(s), {num_threads} worker(s)",
            boundaries.len(),
        );
        if num_chunks <= 1 {
            eprintln!(
                "[rapidgzip] pipeline: only one chunk — parallel path degrades to serial-equivalent decode",
            );
        }
    }

    // Channels.
    let (work_tx, work_rx) = bounded::<WorkItem>(num_threads * 2);
    let (result_tx, result_rx) = bounded::<Result<WorkResult, Error>>(num_threads * 2);

    // Spawn workers.
    let mut worker_handles = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let body = Arc::clone(&body);
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let handle = thread::spawn(move || {
            while let Ok(item) = work_rx.recv() {
                let res = decode_one_chunk(&body, &item);
                if result_tx.send(res).is_err() {
                    return;
                }
            }
        });
        worker_handles.push(handle);
    }
    drop(work_rx);
    drop(result_tx);

    // Dispatch work — send all items. The bounded channel applies backpressure.
    let dispatch = thread::spawn(move || {
        for item in work_items {
            if work_tx.send(item).is_err() {
                return;
            }
        }
    });

    // Serializer (run on the calling thread): receive results, reorder,
    // resolve markers, stream bytes out, track CRC32 and uncompressed size.
    let mut reorder: std::collections::BTreeMap<u64, WorkResult> =
        std::collections::BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut prev_tail: Vec<u8> = Vec::new();
    let mut uncompressed: u64 = 0;
    let mut crc = crc32fast::Hasher::new();
    let mut final_end_bit: Option<u64> = None;

    'outer: while next_id < num_chunks as u64 {
        // Drain any ready chunk in order.
        while let Some(mut res) = reorder.remove(&next_id) {
            // Resolve markers against the tail of all previously emitted bytes.
            resolve_markers(&mut res.chunk, &prev_tail).map_err(Error::Deflate)?;
            uncompressed += res.chunk.bytes.len() as u64;
            crc.update(&res.chunk.bytes);
            update_tail(&mut prev_tail, &res.chunk.bytes);

            let saw_final = res.final_block;
            let end_bit_of_this = res.end_bit;

            if sink.send(res.chunk.bytes).is_err() {
                return Ok((uncompressed, body.len()));
            }
            next_id += 1;

            if saw_final {
                // Member ends here. Don't process any later chunks — they
                // belong to a subsequent gzip member (multi-stream case) or
                // are post-EOS junk; either way they don't contribute to
                // this member's CRC/ISIZE.
                final_end_bit = Some(end_bit_of_this);
                break 'outer;
            }
        }
        if next_id >= num_chunks as u64 {
            break;
        }
        let res = result_rx
            .recv()
            .map_err(|_| Error::Io(std::io::Error::other("worker channel closed")))?;
        let res = res?;
        reorder.insert(res.id, res);
    }

    // Drain any remaining results so workers can exit cleanly. Errors from
    // post-final-block chunks are expected (the bits they decoded weren't
    // a valid deflate stream — they were the next member's header / trailer
    // bytes interpreted as deflate) and are intentionally discarded.
    drop(result_rx);

    // Wait for the dispatch and worker threads.
    let _ = dispatch.join();
    for h in worker_handles {
        let _ = h.join();
    }

    // Find the trailer. final_end_bit is the bit position immediately after
    // BFINAL block; byte-align then 8 bytes.
    let body_end_bit = final_end_bit.ok_or_else(|| {
        Error::Io(std::io::Error::other(
            "parallel decode produced no final block",
        ))
    })?;
    let after_align_bit = (body_end_bit + 7) & !7;
    let trailer_byte = (after_align_bit / 8) as usize;
    if trailer_byte + 8 > body.len() {
        return Err(Error::Gzip(crate::GzipError::Truncated));
    }
    let crc_expected = u32::from_le_bytes(body[trailer_byte..trailer_byte + 4].try_into().unwrap());
    let isize_expected = u32::from_le_bytes(
        body[trailer_byte + 4..trailer_byte + 8].try_into().unwrap(),
    );
    let crc_got = crc.finalize();
    if crc_got != crc_expected {
        return Err(Error::Gzip(crate::GzipError::CrcMismatch {
            expected: crc_expected,
            got: crc_got,
            member: 0, // parallel path only handles member 0
            uncompressed,
            trailer_byte: trailer_byte as u64,
        }));
    }
    if (uncompressed & 0xFFFF_FFFF) as u32 != isize_expected {
        return Err(Error::Gzip(crate::GzipError::IsizeMismatch {
            expected: isize_expected,
            got: uncompressed,
            member: 0,
            trailer_byte: trailer_byte as u64,
        }));
    }

    Ok((uncompressed, trailer_byte + 8))
}

fn decode_one_chunk(body: &[u8], item: &WorkItem) -> Result<WorkResult, Error> {
    let mut br = BitReader::new(body);
    br.seek_to_bit(item.start_bit).map_err(Error::Deflate)?;
    let mut chunk = SpeculativeChunk::default();
    let mut final_block = false;
    loop {
        let pos = br.tell_bit();
        // Stop only between blocks, when we've already reached / passed the
        // proposed end. The block finder placed `end_bit_hint` AT a block
        // boundary, so for well-behaved input we land exactly there.
        if pos >= item.end_bit_hint {
            break;
        }
        let bf = inflate_block_speculative(&mut br, &mut chunk).map_err(Error::Deflate)?;
        if bf {
            final_block = true;
            break;
        }
    }
    Ok(WorkResult {
        id: item.id,
        chunk,
        end_bit: br.tell_bit(),
        final_block,
    })
}

fn effective_threads(config: &Config) -> usize {
    let n = if config.num_threads == 0 {
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        config.num_threads
    };
    n.max(1)
}

/// Keep the last 32 KiB of (prev_tail ++ new_bytes) as the next prev_tail.
fn update_tail(prev_tail: &mut Vec<u8>, new_bytes: &[u8]) {
    const WINDOW: usize = 32 * 1024;
    if new_bytes.len() >= WINDOW {
        prev_tail.clear();
        prev_tail.extend_from_slice(&new_bytes[new_bytes.len() - WINDOW..]);
        return;
    }
    prev_tail.extend_from_slice(new_bytes);
    if prev_tail.len() > WINDOW {
        let drop = prev_tail.len() - WINDOW;
        prev_tail.drain(..drop);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read_gz, Config};
    use crossbeam_channel::bounded;
    use sha2::{Digest, Sha256};
    use std::io::Write;
    use std::process::{Command, Stdio};

    /// Build a single gz member via the system `gzip`. Drains stdout on a
    /// worker thread to avoid pipe-buffer deadlock for big payloads.
    fn gz_encode(payload: &[u8], level: u32) -> Vec<u8> {
        let mut child = Command::new("gzip")
            .args([&format!("-{level}"), "-c", "-n"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn gzip");
        let mut stdin = child.stdin.take().unwrap();
        let payload = payload.to_vec();
        let writer = std::thread::spawn(move || stdin.write_all(&payload).unwrap());
        let out = child.wait_with_output().expect("wait gzip");
        writer.join().unwrap();
        out.stdout
    }

    fn decode_via_read_gz(path: &std::path::Path, cfg: Config) -> Vec<u8> {
        let (tx, rx) = bounded::<Vec<u8>>(8);
        let path = path.to_owned();
        let producer = std::thread::spawn(move || read_gz(&path, tx, cfg));
        let mut out = Vec::new();
        for chunk in rx {
            out.extend_from_slice(&chunk);
        }
        producer.join().expect("producer").expect("read_gz");
        out
    }

    fn write_tmp(name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("rapidgzip_rs_test_{name}"));
        std::fs::write(&path, data).unwrap();
        path
    }

    fn ascii_payload(n: usize) -> Vec<u8> {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut p = Vec::with_capacity(n);
        while p.len() < n {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            p.push(((s >> 56) as u8 % 95) + 32);
        }
        p
    }

    fn sha(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        hex::encode(h.finalize())
    }

    /// Same payload, parallel and serial paths must produce identical output.
    #[test]
    fn parallel_matches_serial_large() {
        let payload = ascii_payload(8 * 1024 * 1024);
        let gz = gz_encode(&payload, 6);
        let path = write_tmp("par_vs_serial.gz", &gz);
        for threads in [1usize, 2, 4] {
            let out = decode_via_read_gz(
                &path,
                Config { num_threads: threads, chunk_size_bytes: 1 << 20, ..Config::default() },
            );
            assert_eq!(sha(&out), sha(&payload), "threads={threads}");
        }
    }

    /// Multi-stream: 3 gzip members concatenated. Each member uses a
    /// different payload so we'd notice if the boundary between members
    /// were dropped or duplicated.
    #[test]
    fn multistream_three_members() {
        let a = ascii_payload(100_000);
        let b = ascii_payload(200_017);
        let c = ascii_payload(50_003);
        let mut gz = gz_encode(&a, 6);
        gz.extend_from_slice(&gz_encode(&b, 6));
        gz.extend_from_slice(&gz_encode(&c, 6));
        let mut expected = Vec::new();
        expected.extend_from_slice(&a);
        expected.extend_from_slice(&b);
        expected.extend_from_slice(&c);
        let path = write_tmp("multi3.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config { num_threads: 4, chunk_size_bytes: 64 * 1024, ..Config::default() },
        );
        assert_eq!(sha(&out), sha(&expected));
    }

    /// Large multi-stream: first member is big enough to trigger
    /// multiple-chunk parallel decode, second member exercises the
    /// "stop at BFINAL, hand off to serial multi-stream loop" path.
    #[test]
    fn multistream_large_then_small() {
        let big = ascii_payload(4 * 1024 * 1024);
        let small = ascii_payload(1000);
        let mut gz = gz_encode(&big, 6);
        gz.extend_from_slice(&gz_encode(&small, 6));
        let mut expected = big.clone();
        expected.extend_from_slice(&small);
        let path = write_tmp("multi_big_small.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config { num_threads: 4, chunk_size_bytes: 1 << 20, ..Config::default() },
        );
        assert_eq!(sha(&out), sha(&expected));
    }

    /// Tiny file: smaller than chunk_size. Parallel pipeline must degrade
    /// gracefully to a single chunk.
    #[test]
    fn tiny_file() {
        let payload = b"hello, world\n";
        let gz = gz_encode(payload, 6);
        let path = write_tmp("tiny.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config { num_threads: 4, chunk_size_bytes: 1 << 20, ..Config::default() },
        );
        assert_eq!(out, payload);
    }

    /// Cross-product of {payload sizes} × {chunk sizes} × {thread counts}.
    /// Catches off-by-one issues where chunk boundary lands at member
    /// boundary, single-chunk degenerate cases, and high-contention paths.
    #[test]
    fn matrix_sizes_chunks_threads() {
        for &size in &[1024usize, 65_536, 250_000, 1_000_000, 5_000_000] {
            let payload = ascii_payload(size);
            let gz = gz_encode(&payload, 6);
            let path = write_tmp(&format!("matrix_{size}.gz"), &gz);
            for &cs in &[64 * 1024usize, 1 << 20, 4 << 20] {
                for &nt in &[1usize, 4] {
                    let out = decode_via_read_gz(
                        &path,
                        Config { num_threads: nt, chunk_size_bytes: cs, ..Config::default() },
                    );
                    assert_eq!(
                        sha(&out), sha(&payload),
                        "size={size} chunk={cs} threads={nt}"
                    );
                }
            }
        }
    }

    /// Stored blocks (level 1 on incompressible data) and dynamic blocks
    /// interleaved: the boundary finder skips over stored blocks, so a
    /// worker may decode several block types in a single chunk.
    #[test]
    fn mixed_block_types() {
        let mut payload = ascii_payload(500_000);
        let mut s: u64 = 0xABCDEF0123456789;
        for _ in 0..(500_000 / 8) {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            payload.extend_from_slice(&s.to_le_bytes());
        }
        payload.extend_from_slice(&ascii_payload(500_000));
        let gz = gz_encode(&payload, 1);
        let path = write_tmp("mixed.gz", &gz);
        let out = decode_via_read_gz(
            &path,
            Config { num_threads: 4, chunk_size_bytes: 256 * 1024, ..Config::default() },
        );
        assert_eq!(sha(&out), sha(&payload));
    }

    /// Gzip levels 1..9 should all decode correctly. Different levels
    /// produce different block shapes and Huffman trees.
    #[test]
    fn all_gzip_levels() {
        let payload = ascii_payload(2 * 1024 * 1024);
        for level in 1..=9u32 {
            let gz = gz_encode(&payload, level);
            let path = write_tmp(&format!("level_{level}.gz"), &gz);
            let out = decode_via_read_gz(
                &path,
                Config { num_threads: 4, chunk_size_bytes: 512 * 1024, ..Config::default() },
            );
            assert_eq!(sha(&out), sha(&payload), "level={level}");
        }
    }

    #[test]
    fn update_tail_keeps_last_32k() {
        let mut prev = Vec::new();
        update_tail(&mut prev, &[1u8; 1000]);
        assert_eq!(prev.len(), 1000);
        update_tail(&mut prev, &[2u8; 50_000]);
        assert_eq!(prev.len(), 32 * 1024);
        assert!(prev.iter().all(|&b| b == 2));
        let mut prev = vec![9u8; 30_000];
        update_tail(&mut prev, &[7u8; 5_000]);
        assert_eq!(prev.len(), 32 * 1024);
        // Last 5000 bytes should be 7s.
        assert!(prev[32 * 1024 - 5_000..].iter().all(|&b| b == 7));
    }
}
