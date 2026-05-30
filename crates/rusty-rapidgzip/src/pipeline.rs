//! Parallel speculative decode pipeline.
//!
//! Three roles, all running concurrently:
//!
//! 1. **Boundary finder** (main thread, before workers start): scans the
//!    compressed body for dynamic-Huffman block boundaries at roughly
//!    `chunk_size_bytes` intervals. Produces a list of bit offsets.
//! 2. **Workers**: each takes one `(chunk_id, start_bit, end_bit_hint)` and
//!    decodes blocks until `end_bit_hint`
//!    is reached or BFINAL is seen. Emits a `SpeculativeChunk`.
//! 3. **Serializer**: receives chunks out of order, buffers them in a
//!    `BTreeMap`, and processes them in `chunk_id` order — resolves markers
//!    against the running 32 KiB tail, then streams resolved bytes to the
//!    caller's sink. Also validates CRC32 + ISIZE in the trailer when the
//!    final block is reached.
//!
//! ## Bounds & assumptions
//!
//! - Handles multi-member gzip directly: at each BFINAL the worker consumes
//!   the 8-byte trailer + the next gzip header and continues decoding into
//!   the next member, recording a [`MemberBoundary`] at each transition. The
//!   serializer keeps a running CRC32 + uncompressed counter per member and
//!   validates the trailer at every boundary.
//! - If the block finder fails to locate enough internal boundaries, we
//!   silently degrade to fewer (or one) chunks. A degenerate decode is
//!   serial-equivalent.
//! - On any worker error, the whole pipeline returns that error. We don't
//!   attempt false-boundary recovery; the block finder's full-header
//!   verification keeps false positives extremely rare in practice.

use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Sender};

/// Compressed input backing store. Either an mmap'd file (production) or a
/// heap buffer (tests / small inputs). Both deref to `&[u8]`.
#[derive(Debug)]
pub enum InputBytes {
    Owned(Vec<u8>),
    Mapped(memmap2::Mmap),
}

impl InputBytes {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            InputBytes::Owned(v) => v,
            InputBytes::Mapped(m) => m,
        }
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
}

impl std::ops::Deref for InputBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

use rusty_rapidgzip_deflate::{
    find_next_dynamic_block, resolve_markers,
    fast_inflate, BitReader, SpeculativeChunk, SpeculativeZlibDecoder,
};

use crate::{Config, Error, InflateKernel};

/// Per-worker kernel state. `ZlibRs` owns a reusable `SpeculativeZlibDecoder`;
/// `FastInflate` owns a reusable `u16` dense-window scratch buffer (reused
/// across chunks so its pages stay faulted).
enum WorkerKernel {
    ZlibRs(SpeculativeZlibDecoder),
    FastInflate(Vec<u16>),
}

impl WorkerKernel {
    fn new(kernel: InflateKernel) -> Self {
        match kernel {
            InflateKernel::ZlibRs => WorkerKernel::ZlibRs(SpeculativeZlibDecoder::new()),
            InflateKernel::FastInflate => WorkerKernel::FastInflate(Vec::new()),
        }
    }

    fn decode_until(
        &mut self,
        input: &[u8],
        start_bit: u64,
        end_bit_hint: u64,
        chunk: &mut SpeculativeChunk,
    ) -> Result<(u64, bool), rusty_rapidgzip_deflate::DeflateError> {
        match self {
            WorkerKernel::ZlibRs(zdec) => zdec.decode_until(input, start_bit, end_bit_hint, chunk),
            WorkerKernel::FastInflate(scratch) => {
                fast_inflate::decode_until_u16(input, start_bit, end_bit_hint, chunk, scratch)
            }
        }
    }
}

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
    /// Set when the worker reached real EOF after the last member's trailer.
    final_block: bool,
    /// Member transitions encountered while decoding this chunk. Each entry
    /// records where (in `chunk.bytes`) one member ends and the next begins,
    /// plus the trailer (CRC32 / ISIZE) for the just-ended member.
    member_boundaries: Vec<MemberBoundary>,
}

#[derive(Debug, Clone)]
struct MemberBoundary {
    /// Byte offset in `chunk.bytes` where the just-ended member's decoded
    /// output ends. Stable across marker resolution.
    byte_offset_in_chunk: usize,
    crc_expected: u32,
    isize_expected: u32,
    /// Byte offset of the trailer in `body`. For error messages.
    trailer_input_byte: u64,
}

/// Decode the deflate body of one or more concatenated gzip members in
/// parallel.
///
/// `body` is the slice from end-of-first-gzip-header to end-of-file. The
/// caller has parsed and stripped only the *first* member's header; this
/// function handles every member's trailer, every subsequent member's
/// header, and validates CRC32 + ISIZE for each member as it streams.
///
/// On success, returns `(total_uncompressed_bytes, body_bytes_consumed,
/// chunks_sent)` where `chunks_sent` is the number of `Vec<u8>` chunks
/// pushed onto `sink`.
pub fn parallel_decode_member(
    input: Arc<InputBytes>,
    body_offset: usize,
    sink: &Sender<Arc<Vec<u8>>>,
    config: &Config,
) -> Result<(u64, usize, u64), Error> {
    // Inside this function `body` is the deflate body — the slice of `input`
    // starting at `body_offset`. Workers receive the full Arc + the offset
    // so they don't need to know about the gzip header layer.
    let body: &[u8] = &input.as_slice()[body_offset..];
    let num_threads = effective_threads(config);
    let verbose = config.verbose.is_on();
    let recycle_rx = config.recycle_rx.clone();
    let recycle_tx_for_crc = config.recycle_tx.clone();
    let total_bits = (body.len() as u64) * 8;
    let chunk_bits = (config.chunk_size_bytes as u64).max(64 * 1024) * 8;

    // Find block boundaries in parallel. Pick fixed target offsets every
    // `chunk_bits` and search each in parallel; each search is local and
    // independent — the only post-step is dedup + sort.
    let t_scan = std::time::Instant::now();
    let mut boundaries: Vec<u64> = vec![0];
    {
        let mut targets: Vec<u64> = Vec::new();
        let mut c = chunk_bits;
        while c < total_bits {
            targets.push(c);
            c += chunk_bits;
        }
        if !targets.is_empty() {
            let body_ref = &body[..];
            let found: Vec<Option<u64>> = std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(targets.len().min(num_threads));
                // Split targets across `num_threads` workers in round-robin.
                let n = num_threads.min(targets.len()).max(1);
                let chunks: Vec<Vec<usize>> = (0..n)
                    .map(|i| (i..targets.len()).step_by(n).collect())
                    .collect();
                for indices in chunks {
                    let targets = &targets;
                    let body_ref = body_ref;
                    handles.push(s.spawn(move || {
                        indices
                            .into_iter()
                            .map(|i| {
                                let t = targets[i];
                                (i, find_next_dynamic_block(body_ref, t, total_bits))
                            })
                            .collect::<Vec<_>>()
                    }));
                }
                let mut out: Vec<Option<u64>> = vec![None; targets.len()];
                for h in handles {
                    for (i, b) in h.join().unwrap() {
                        out[i] = b;
                    }
                }
                out
            });
            for b in found.into_iter().flatten() {
                if b > *boundaries.last().unwrap() {
                    boundaries.push(b);
                }
            }
        }
    }
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] pipeline: scanned in {:.3}s",
            crate::elapsed_since_start(),
            t_scan.elapsed().as_secs_f64(),
        );
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
            "[rapidgzip +{:.2}s] pipeline: {} boundaries found → {num_chunks} chunk(s), {num_threads} worker(s)",
            crate::elapsed_since_start(),
            boundaries.len(),
        );
        if num_chunks <= 1 {
            eprintln!(
                "[rapidgzip +{:.2}s] pipeline: only one chunk — parallel path degrades to serial-equivalent decode",
                crate::elapsed_since_start(),
            );
        }
    }

    // Channels.
    let (work_tx, work_rx) = bounded::<WorkItem>(num_threads * 2);
    let (result_tx, result_rx) = bounded::<Result<WorkResult, Error>>(num_threads * 2);

    // Spawn workers. Each holds an Arc<InputBytes> and recomputes the body
    // slice locally — keeps the closure 'static while sharing a single
    // backing mmap or buffer across all workers.
    let mut worker_handles = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let input = Arc::clone(&input);
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let recycle_rx = recycle_rx.clone();
        let kernel = config.kernel;
        let handle = thread::spawn(move || {
            let body = &input.as_slice()[body_offset..];
            let mut wk = WorkerKernel::new(kernel);
            while let Ok(item) = work_rx.recv() {
                // Try to pull a recycled output buffer; pages on it are
                // warm so subsequent writes don't take page faults.
                let recycled = recycle_rx
                    .as_ref()
                    .and_then(|r| r.try_recv().ok());
                let res = decode_one_chunk(body, &item, &mut wk, recycled);
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

    // ── CRC validator thread ─────────────────────────────────────────────────
    //
    // Receives fully-resolved chunks in order from the serializer, validates
    // CRC32 + ISIZE off the hot path, and recycles the output buffer.
    let (crc_tx, crc_rx) = bounded::<(Arc<Vec<u8>>, Vec<MemberBoundary>)>(num_threads * 2);
    let crc_handle: thread::JoinHandle<Result<(), Error>> = thread::spawn(move || {
        let mut cur_crc = crc32fast::Hasher::new();
        let mut cur_uncompressed: u64 = 0;
        let mut member_idx: u32 = 0;
        for (bytes_arc, member_boundaries) in crc_rx {
            let bytes: &[u8] = &bytes_arc;
            let mut cursor = 0usize;
            for mb in &member_boundaries {
                let piece = &bytes[cursor..mb.byte_offset_in_chunk];
                cur_crc.update(piece);
                cur_uncompressed += piece.len() as u64;
                let crc_got = std::mem::replace(&mut cur_crc, crc32fast::Hasher::new()).finalize();
                if crc_got != mb.crc_expected {
                    return Err(Error::Gzip(crate::GzipError::CrcMismatch {
                        expected: mb.crc_expected,
                        got: crc_got,
                        member: member_idx,
                        uncompressed: cur_uncompressed,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                }
                if (cur_uncompressed & 0xFFFF_FFFF) as u32 != mb.isize_expected {
                    return Err(Error::Gzip(crate::GzipError::IsizeMismatch {
                        expected: mb.isize_expected,
                        got: cur_uncompressed,
                        member: member_idx,
                        trailer_byte: mb.trailer_input_byte,
                    }));
                }
                member_idx += 1;
                cur_uncompressed = 0;
                cursor = mb.byte_offset_in_chunk;
            }
            let tail = &bytes[cursor..];
            cur_crc.update(tail);
            cur_uncompressed += tail.len() as u64;
            drop(member_boundaries);
            if let (Some(tx), Ok(mut v)) = (
                recycle_tx_for_crc.as_ref(),
                Arc::try_unwrap(bytes_arc),
            ) {
                v.clear();
                let _ = tx.try_send(v);
            }
        }
        Ok(())
    });

    // ── Serializer ───────────────────────────────────────────────────────────
    let body_len = body.len();
    let mut reorder: std::collections::BTreeMap<u64, WorkResult> =
        std::collections::BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut prev_tail: Vec<u8> = Vec::new();
    let mut total_uc: u64 = 0;
    let mut sent: u64 = 0;
    let mut final_byte: Option<usize> = None;
    let mut last_err: Option<Error> = None;

    let mut t_resolve = std::time::Duration::ZERO;
    let mut t_wait    = std::time::Duration::ZERO;
    let mut n_markers: u64 = 0;

    'outer: while next_id < num_chunks as u64 {
        while let Some(mut res) = reorder.remove(&next_id) {
            n_markers += res.chunk.markers.len() as u64;
            // Build the tail for the NEXT chunk (fast: only tail-window markers).
            let t0 = std::time::Instant::now();
            let new_tail = build_prev_tail_fast(&res.chunk, &prev_tail);
            // Resolve this chunk's markers against the OLD prev_tail.
            if let Err(e) = resolve_markers(&mut res.chunk, &prev_tail) {
                last_err = Some(Error::Deflate(e));
                break 'outer;
            }
            t_resolve += t0.elapsed();
            prev_tail = new_tail;

            total_uc += res.chunk.bytes.len() as u64;
            let saw_final = res.final_block;
            if let Some(mb) = res.member_boundaries.last() {
                if saw_final {
                    final_byte = Some(mb.trailer_input_byte as usize + 8);
                }
            }
            let bytes_arc = Arc::new(res.chunk.bytes);
            let _ = crc_tx.send((Arc::clone(&bytes_arc), res.member_boundaries));
            if sink.send(bytes_arc).is_err() {
                drop(crc_tx);
                let _ = crc_handle.join();
                drop(result_rx);
                let _ = dispatch.join();
                for h in worker_handles { let _ = h.join(); }
                return Ok((total_uc, body_len, sent));
            }
            sent += 1;
            next_id += 1;
            if saw_final {
                break 'outer;
            }
        }
        if next_id >= num_chunks as u64 { break; }
        let tw = std::time::Instant::now();
        let res = match result_rx.recv() {
            Ok(r) => r,
            Err(_) => {
                last_err = Some(Error::Io(std::io::Error::other("worker channel closed")));
                break 'outer;
            }
        };
        t_wait += tw.elapsed();
        match res {
            Ok(r)  => { reorder.insert(r.id, r); }
            Err(e) => { last_err = Some(e); break 'outer; }
        }
    }
    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] serializer {next_id} chunks | {n_markers} markers | resolve={:.3}s wait={:.3}s",
            crate::elapsed_since_start(),
            t_resolve.as_secs_f64(),
            t_wait.as_secs_f64(),
        );
    }

    drop(crc_tx);
    drop(result_rx);
    let _ = dispatch.join();
    for h in worker_handles { let _ = h.join(); }

    if let Some(e) = last_err {
        return Err(e);
    }

    crc_handle.join().unwrap_or_else(|_| {
        Err(Error::Io(std::io::Error::other("crc thread panicked")))
    })?;

    let bc = final_byte.ok_or_else(|| {
        Error::Io(std::io::Error::other("parallel decode produced no final block"))
    })?;
    Ok((total_uc, bc, sent))
}

/// Build the next chunk's prev_tail by applying only the markers that land in
/// the last 32 KiB of this chunk's output.
fn build_prev_tail_fast(chunk: &SpeculativeChunk, prev_tail: &[u8]) -> Vec<u8> {
    const WINDOW: usize = 32 * 1024;
    let len = chunk.bytes.len();
    let tail_start = len.saturating_sub(WINDOW);
    let first = chunk.markers.partition_point(|m| (m.out_pos as usize) < tail_start);
    let mut new_tail = chunk.bytes[tail_start..].to_vec();
    for m in &chunk.markers[first..] {
        let pos = m.out_pos as usize;
        if pos >= tail_start + new_tail.len() { break; }
        let dst = pos - tail_start;
        let off = m.prefix_offset as usize;
        if off < prev_tail.len() {
            new_tail[dst] = prev_tail[prev_tail.len() - 1 - off];
        }
    }
    new_tail
}

fn decode_one_chunk(
    body: &[u8],
    item: &WorkItem,
    wk: &mut WorkerKernel,
    recycled_bytes: Option<Vec<u8>>,
) -> Result<WorkResult, Error> {
    let mut br = BitReader::new(body);
    br.seek_to_bit(item.start_bit).map_err(Error::Deflate)?;
    let mut chunk = SpeculativeChunk::default();
    if let Some(mut v) = recycled_bytes {
        v.clear();
        chunk.bytes = v;
    }
    // Optimistic pre-allocate: assume ~3x compression ratio. Worst case
    // (incompressible / stored blocks) we over-reserve harmlessly; common
    // case avoids the cascade of doublings.
    let span_bits = item.end_bit_hint.saturating_sub(item.start_bit);
    if span_bits != u64::MAX && span_bits > 0 {
        let est_output = ((span_bits / 8) as usize).saturating_mul(3);
        chunk.reserve_bytes(est_output.min(64 * 1024 * 1024));
    }
    let mut final_block = false;
    let mut member_boundaries: Vec<MemberBoundary> = Vec::new();

    loop {
        let pos = br.tell_bit();
        if pos >= item.end_bit_hint {
            break;
        }

        let (new_end, hit_bfinal) = wk
            .decode_until(body, pos, item.end_bit_hint, &mut chunk)
            .map_err(Error::Deflate)?;
        br.seek_to_bit(new_end).map_err(Error::Deflate)?;

        if !hit_bfinal {
            // Stopped at a block boundary at/past end_bit_hint — done.
            break;
        }

        // Final block of a member — byte-align and read the 8-byte trailer.
        br.byte_align();
        let after_bit = br.tell_bit();
        debug_assert_eq!(after_bit % 8, 0);
        let trailer_byte = (after_bit / 8) as usize;
        if trailer_byte + 8 > body.len() {
            return Err(Error::Gzip(crate::GzipError::Truncated));
        }
        let crc_expected = u32::from_le_bytes(
            body[trailer_byte..trailer_byte + 4].try_into().unwrap(),
        );
        let isize_expected = u32::from_le_bytes(
            body[trailer_byte + 4..trailer_byte + 8].try_into().unwrap(),
        );
        member_boundaries.push(MemberBoundary {
            byte_offset_in_chunk: chunk.bytes.len(),
            crc_expected,
            isize_expected,
            trailer_input_byte: trailer_byte as u64,
        });

        let after_trailer = trailer_byte + 8;
        if after_trailer >= body.len() {
            // Real EOF — the very last member has been consumed.
            final_block = true;
            br.seek_to_bit((after_trailer as u64) * 8)
                .map_err(Error::Deflate)?;
            break;
        }

        // Another member follows. Parse its gzip header and continue.
        let header_len = crate::gzip::parse_header(&body[after_trailer..])
            .map_err(Error::Gzip)?;
        let next_block_byte = after_trailer + header_len;
        br.seek_to_bit((next_block_byte as u64) * 8)
            .map_err(Error::Deflate)?;
        // Loop: next iteration will inflate the first block of the new member.
    }

    Ok(WorkResult {
        id: item.id,
        chunk,
        final_block,
        member_boundaries,
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


/// BGZF fast-path pipeline.
///
/// BGZF (bgzip / samtools spec) is gzip with a mandatory `BC` FEXTRA subfield
/// giving each member's size. Every member is an independent deflate stream
/// with no back-refs across boundaries, so we can split the file at known
/// byte offsets and decode each member with the plain serial inflater — no
/// speculation, no marker resolution, no boundary scanning.
///
/// `file` is the entire compressed input (every member has its own header).
/// Returns `(total_uncompressed, chunks_sent)`.
pub fn parallel_decode_bgzf(
    file: Arc<InputBytes>,
    sink: &Sender<Arc<Vec<u8>>>,
    config: &Config,
) -> Result<(u64, u64), Error> {
    use std::collections::BTreeMap;

    let num_threads = effective_threads(config);
    let verbose = config.verbose.is_on();

    // Walk member boundaries up front.
    let mut members: Vec<(usize, usize)> = Vec::new();
    let mut pos = 0usize;
    while pos < file.len() {
        let Some(size) = crate::gzip::parse_bgzf_block_size(&file[pos..]) else {
            return Err(Error::Gzip(crate::gzip::GzipError::Truncated));
        };
        let size = size as usize;
        if pos + size > file.len() {
            return Err(Error::Gzip(crate::gzip::GzipError::Truncated));
        }
        members.push((pos, pos + size));
        pos += size;
    }

    // Batch members so each work item covers ~chunk_size_bytes of compressed
    // input. With BGZF blocks typically ~16 KiB compressed, a 4 MiB chunk
    // groups ~256 members — enough amortization to keep workers busy. But
    // for small files we'd end up with one giant batch; cap by the total
    // size divided by (num_threads × 4) so every worker gets several batches.
    let total_compressed: usize = members.iter().map(|(s, e)| e - s).sum();
    let per_worker_target = total_compressed / (num_threads * 4).max(1);
    let target = config
        .chunk_size_bytes
        .min(per_worker_target.max(64 * 1024))
        .max(64 * 1024);
    let mut batches: Vec<(usize, usize)> = Vec::new(); // (start_member, end_member) exclusive
    let mut i = 0;
    while i < members.len() {
        let start = i;
        let mut bytes = 0usize;
        while i < members.len() && (bytes == 0 || bytes < target) {
            bytes += members[i].1 - members[i].0;
            i += 1;
        }
        batches.push((start, i));
    }

    if verbose {
        eprintln!(
            "[rapidgzip +{:.2}s] bgzf: {} members → {} batch(es), {} worker(s)",
            crate::elapsed_since_start(),
            members.len(),
            batches.len(),
            num_threads,
        );
    }

    // Work channel: (batch_id, member_start, member_end_exclusive).
    let (work_tx, work_rx) = bounded::<(u64, usize, usize)>(num_threads * 2);
    let (result_tx, result_rx) =
        bounded::<Result<(u64, Vec<u8>), Error>>(num_threads * 2);

    let engine = crate::gzip::InflateEngine::from_env();
    let mut workers = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let file = Arc::clone(&file);
        let members_ref: Vec<(usize, usize)> = members.clone();
        workers.push(thread::spawn(move || {
            // Per-worker zlib-rs state reused across all members. `reset()`
            // between members keeps the 32 KiB window allocated.
            let mut zdec = (rusty_rapidgzip_inflate::Inflate::new(false, 15), Box::new([0u8; 65_536]));
            while let Ok((id, m_start, m_end)) = work_rx.recv() {
                // BGZF blocks are ≤64 KiB uncompressed; preallocate to avoid
                // Vec growth reallocations inside the inflate hot loop.
                let mut out: Vec<u8> = Vec::with_capacity((m_end - m_start) * 65_536);
                let mut err: Option<Error> = None;
                for mi in m_start..m_end {
                    let (s, e) = members_ref[mi];
                    let res = match engine {
                        crate::gzip::InflateEngine::Zlib => {
                            let (dec, scratch) = &mut zdec;
                            crate::gzip::decode_one_indexed_zlib(
                                &file[s..e],
                                &mut out,
                                mi as u32,
                                dec,
                                scratch.as_mut(),
                            )
                        }
                        crate::gzip::InflateEngine::Safe => {
                            crate::gzip::decode_one_indexed_safe(
                                &file[s..e], &mut out, mi as u32,
                            ).map(|n| { let _ = n; n })
                        }
                        crate::gzip::InflateEngine::Intree => {
                            crate::gzip::decode_one_indexed(
                                &file[s..e], &mut out, mi as u32,
                            )
                        }
                    };
                    match res {
                        Ok(_) => {}
                        Err(ge) => {
                            err = Some(Error::Gzip(ge));
                            break;
                        }
                    }
                }
                let msg = match err {
                    Some(e) => Err(e),
                    None => Ok((id, out)),
                };
                if result_tx.send(msg).is_err() {
                    return;
                }
            }
        }));
    }
    drop(result_tx);

    // Dispatch all work items, then close the work channel.
    let dispatch = thread::spawn(move || {
        for (id, &(s, e)) in batches.iter().enumerate() {
            if work_tx.send((id as u64, s, e)).is_err() {
                return;
            }
        }
        drop(work_tx);
    });

    // Serializer: reorder, stream in order.
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut next_id: u64 = 0;
    let mut total_uncompressed: u64 = 0;
    let mut chunks_sent: u64 = 0;
    let mut last_err: Option<Error> = None;
    for msg in result_rx.iter() {
        match msg {
            Err(e) => {
                last_err = Some(e);
                break;
            }
            Ok((id, bytes)) => {
                pending.insert(id, bytes);
                while let Some(bytes) = pending.remove(&next_id) {
                    total_uncompressed += bytes.len() as u64;
                    if sink.send(Arc::new(bytes)).is_err() {
                        last_err = Some(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "sink closed",
                        )));
                        break;
                    }
                    chunks_sent += 1;
                    next_id += 1;
                }
                if last_err.is_some() {
                    break;
                }
            }
        }
    }

    let _ = dispatch.join();
    for w in workers {
        let _ = w.join();
    }

    if let Some(e) = last_err {
        return Err(e);
    }
    Ok((total_uncompressed, chunks_sent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read_gz, Config, InflateKernel};
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
        let (tx, rx) = bounded::<Arc<Vec<u8>>>(8);
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

    /// zlib-rs backend with many concatenated members, with chunk size
    /// tuned so member boundaries land in the *middle* of speculative chunks
    /// (mirrors the bug observed in the wild: CRC mismatch on member ~22 of
    /// a real `.fastq.gz` file with ~1MB-uncompressed members).
    #[test]
    fn zlib_rs_backend_multistream_member_crosses_chunk_boundary() {
        // Make enough members to span several chunks, with size chosen so
        // chunk boundaries don't align with member ends.
        // Mirrors the real-world failing case: ~1MB uncompressed per
        // member, chunk_size 4MB compressed, many members so several chunk
        // boundaries land mid-member.
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..30u32 {
            let mut p = ascii_payload(1_048_576);
            let suffix = format!("==MEMBER-{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 6));
        }
        let path = write_tmp("zlib_rs_multistream.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 4 * 1024 * 1024,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&expected), "threads={threads}");
        }
    }

    /// Repro of real-world CRC failure: many ~1MB members at level 9
    /// (compressed members are smaller, packing more per 4MB chunk so
    /// boundaries land at varied positions).
    #[test]
    fn multistream_level9() {
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..30u32 {
            let mut p = ascii_payload(1_048_576);
            let suffix = format!("==MEMBER-{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 9));
        }
        let path = write_tmp("multistream_l9.gz", &gz);
        for threads in [1usize, 4] {
            for chunk_sz in [256usize * 1024, 1 << 20, 4 << 20] {
                let out = decode_via_read_gz(
                    &path,
                    Config {
                        num_threads: threads,
                        chunk_size_bytes: chunk_sz,
                        ..Config::default()
                    },
                );
                assert_eq!(sha(&out), sha(&expected), "t={threads} cs={chunk_sz}");
            }
        }
    }

    /// FastInflate kernel: correctness across various payloads and chunk sizes.
    #[test]
    fn fast_inflate_kernel_correctness() {
        // Highly repetitive payload (RLE-heavy) to stress the prefix-overhang
        // propagation path (distance > emitted, with in-buffer extension).
        let mut rle = Vec::new();
        for _ in 0..1000 { rle.extend_from_slice(b"ACGTACGTACGTACGT"); }

        // Random ASCII to exercise literals and varied back-refs.
        let ascii = ascii_payload(4 * 1024 * 1024);

        for (name, payload, level) in [
            ("rle", rle.as_slice(), 6u32),
            ("ascii", ascii.as_slice(), 6),
            ("ascii9", ascii.as_slice(), 9),
        ] {
            let gz = gz_encode(payload, level);
            let path = write_tmp(&format!("fast_{name}.gz"), &gz);
            for threads in [1usize, 4] {
                for chunk_sz in [64 * 1024usize, 1 << 20] {
                    let out = decode_via_read_gz(
                        &path,
                        Config {
                            num_threads: threads,
                            chunk_size_bytes: chunk_sz,
                            kernel: InflateKernel::FastInflate,
                            ..Config::default()
                        },
                    );
                    assert_eq!(
                        sha(&out), sha(payload),
                        "fast_inflate mismatch: payload={name} threads={threads} chunk={chunk_sz}"
                    );
                }
            }
        }
    }

    /// FastInflate kernel: multi-stream (concatenated members) correctness.
    #[test]
    fn fast_inflate_kernel_multistream() {
        let mut gz = Vec::new();
        let mut expected = Vec::new();
        for i in 0..10u32 {
            let mut p = ascii_payload(512 * 1024);
            let suffix = format!("==M{i}==");
            let cut = p.len() - suffix.len();
            p[cut..].copy_from_slice(suffix.as_bytes());
            expected.extend_from_slice(&p);
            gz.extend_from_slice(&gz_encode(&p, 6));
        }
        let path = write_tmp("fast_multistream.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 4 * 1024 * 1024,
                    kernel: InflateKernel::FastInflate,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&expected), "fast_inflate multistream threads={threads}");
        }
    }

    /// Parallel decode produces correct output across a large payload spanning
    /// many chunk boundaries.
    #[test]
    fn parallel_large_payload() {
        let payload = ascii_payload(8 * 1024 * 1024);
        let gz = gz_encode(&payload, 6);
        let path = write_tmp("parallel_large.gz", &gz);
        for threads in [1usize, 4] {
            let out = decode_via_read_gz(
                &path,
                Config {
                    num_threads: threads,
                    chunk_size_bytes: 1 << 20,
                    ..Config::default()
                },
            );
            assert_eq!(sha(&out), sha(&payload), "threads={threads}");
        }
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

}
