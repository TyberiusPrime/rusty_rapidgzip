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

    while next_id < num_chunks as u64 {
        // Drain any ready chunk in order.
        while let Some(mut res) = reorder.remove(&next_id) {
            // Resolve markers against the tail of all previously emitted bytes.
            resolve_markers(&mut res.chunk, &prev_tail).map_err(Error::Deflate)?;
            uncompressed += res.chunk.bytes.len() as u64;
            crc.update(&res.chunk.bytes);

            // Update prev_tail to the last 32 KiB of (prev_tail ++ this chunk).
            update_tail(&mut prev_tail, &res.chunk.bytes);

            // Hand the bytes to the sink.
            if sink.send(res.chunk.bytes).is_err() {
                // Receiver gone — fold up early.
                return Ok((uncompressed, body.len()));
            }
            if res.final_block {
                final_end_bit = Some(res.end_bit);
            }
            next_id += 1;
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

    // Wait for the dispatch and worker threads.
    let _ = dispatch.join();
    drop(result_rx);
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
        }));
    }
    if (uncompressed & 0xFFFF_FFFF) as u32 != isize_expected {
        return Err(Error::Gzip(crate::GzipError::IsizeMismatch {
            expected: isize_expected,
            got: uncompressed,
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

