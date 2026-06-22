//! `rapidgzip-rs-isolated`: the same decode as `rusty-rapidgzip-bin`, but the
//! decoder runs in a separate child process. Decoded bytes cross a shared-memory
//! SPSC ring (see [`ring`]) instead of an in-process channel, so a fault in the
//! decode engine — libdeflate / ISA-L C+asm, or zlib-rs's hundreds of `unsafe`
//! blocks — is contained to the child and cannot corrupt the consumer.
//!
//! One binary plays both roles: invoked normally it is the **parent/consumer**;
//! re-invoked with the hidden `--decoder-shm <id>` it is the **child/decoder**.
//! Compare against the in-process bin with e.g.
//! `time rusty-rapidgzip-rs-isolated f.gz >/dev/null` vs
//! `time rusty-rapidgzip-rs          f.gz >/dev/null`.

mod direct;
mod ring;
mod shm_alloc;

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use crossbeam_channel::bounded;
use direct::{Backoff, Desc, Direct};
use ring::Ring;
use rusty_rapidgzip::{read_gz, Config, Verbosity};
use shared_memory::ShmemConf;

/// Route large allocations into the shared pool in the decoder child so output
/// buffers land in shared memory (see [`shm_alloc`]). A no-op (System) until the
/// child calls `shm_alloc::init`, so the parent process is unaffected.
#[global_allocator]
static ALLOC: shm_alloc::DualAlloc = shm_alloc::DualAlloc;

#[derive(Parser, Debug)]
#[command(name = "rapidgzip-rs-isolated", version)]
struct Args {
    /// Input .gz file, or `-` for stdin.
    input: PathBuf,
    /// Number of worker threads (0 = auto).
    #[arg(short = 'P', long, default_value_t = 0)]
    threads: usize,
    /// Approximate chunk size in bytes.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,
    /// Shared-memory ring capacity in bytes (rounded up to a power of two). This
    /// is the cross-process in-flight buffer — the analogue of the pipe buffer.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    ring_size: usize,
    /// Zero-copy mode: the decoder writes output straight into the shared pool and
    /// hands the consumer only (offset,len) descriptors — no cross-process byte
    /// copy. Without it, decoded chunks are memcpy'd into a byte ring.
    #[arg(long)]
    direct: bool,
    /// Print throughput diagnostics to stderr.
    #[arg(short = 'v', long)]
    verbose: bool,
    /// Internal: run as the decoder child against this shared-memory OS id.
    #[arg(long, hide = true)]
    decoder_shm: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    match (args.decoder_shm.clone(), args.direct) {
        (Some(os_id), false) => run_decoder(&args, &os_id),
        (Some(os_id), true) => run_decoder_direct(&args, &os_id),
        (None, false) => run_consumer(&args),
        (None, true) => run_consumer_direct(&args),
    }
}

/// Build the decoder child command, shared by both modes.
fn spawn_child(args: &Args, os_id: &str, cap: usize) -> Result<std::process::Child> {
    let exe = std::env::current_exe().context("locate current exe")?;
    let mut cmd = Command::new(exe);
    cmd.arg(&args.input)
        .arg("-P")
        .arg(args.threads.to_string())
        .arg("--chunk-size")
        .arg(args.chunk_size.to_string())
        .arg("--ring-size")
        .arg(cap.to_string())
        .arg("--decoder-shm")
        .arg(os_id);
    if args.direct {
        cmd.arg("--direct");
    }
    if args.verbose {
        cmd.arg("-v");
    }
    cmd.spawn().context("spawn decoder child")
}

/// The decode pipeline wiring shared by both decoder modes: recycle channel +
/// `read_gz` on its own thread, returning the chunk receiver and the producer
/// handle.
fn start_decode(
    args: &Args,
) -> (
    crossbeam_channel::Receiver<Arc<Vec<u8>>>,
    crossbeam_channel::Sender<Vec<u8>>,
    std::thread::JoinHandle<Result<rusty_rapidgzip::DecodeStats, rusty_rapidgzip::Error>>,
) {
    let recycle_cap = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        * 2;
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(recycle_cap);

    let cfg = Config {
        num_threads: args.threads,
        chunk_size_bytes: args.chunk_size,
        verbose: if args.verbose {
            Verbosity::On
        } else {
            Verbosity::Off
        },
        recycle_rx: Some(recycle_rx),
        recycle_tx: Some(recycle_tx.clone()),
    };

    let (tx, rx) = bounded::<Arc<Vec<u8>>>(16);
    let input = if args.input.as_os_str() == "-" {
        PathBuf::from("/dev/stdin")
    } else {
        args.input.clone()
    };
    let producer = std::thread::spawn(move || read_gz(&input, tx, cfg));
    (rx, recycle_tx, producer)
}

/// Parent: own the shared memory, spawn the decoder child, drain the ring to
/// stdout.
fn run_consumer(args: &Args) -> Result<()> {
    let cap = args.ring_size.next_power_of_two();
    let shmem = ShmemConf::new()
        .size(Ring::shared_size(cap))
        .create()
        .map_err(|e| anyhow!("create shared memory: {e}"))?;
    let os_id = shmem.get_os_id().to_string();

    // SAFETY: we own this freshly-created mapping; nobody is attached yet.
    let ring = unsafe { Ring::init(shmem.as_ptr(), cap) };

    let mut child = spawn_child(args, &os_id, cap)?;

    // Watchdog: if the child dies without a clean finish (e.g. the unsafe decode
    // engine segfaults), publish an error so the consumer loop unblocks instead
    // of waiting forever on a ring that will never fill. This is the isolation
    // payoff made observable.
    let watch_ring = ring;
    let watchdog = std::thread::spawn(move || -> std::io::Result<std::process::ExitStatus> {
        let status = child.wait()?;
        if !status.success() {
            watch_ring.fail();
        }
        Ok(status)
    });

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let start = std::time::Instant::now();
    let mut total: u64 = 0;
    while let Some(slice) = ring.read_slice() {
        out.write_all(slice)?;
        let n = slice.len();
        total += n as u64;
        ring.consume(n);
    }
    out.flush()?;
    let elapsed = start.elapsed();

    let status = watchdog
        .join()
        .map_err(|_| anyhow!("watchdog thread panicked"))?
        .context("wait for decoder child")?;

    if ring.errored() || !status.success() {
        bail!("decoder child failed (exit: {status}); output may be truncated");
    }

    if args.verbose {
        let mb = total as f64 / (1024.0 * 1024.0);
        eprintln!(
            "[isolated] {:.1} MB in {:.3} s = {:.1} MB/s (ring {} MiB)",
            mb,
            elapsed.as_secs_f64(),
            mb / elapsed.as_secs_f64(),
            cap / (1024 * 1024),
        );
    }
    // `shmem` drops here, unlinking the mapping. The child has already exited.
    Ok(())
}

/// Child: attach to the parent's ring, run the real decode, copy each decoded
/// chunk into the ring.
fn run_decoder(args: &Args, os_id: &str) -> Result<()> {
    let cap = args.ring_size; // already a power of two from the parent
    let shmem = ShmemConf::new()
        .os_id(os_id)
        .open()
        .map_err(|e| anyhow!("open shared memory {os_id}: {e}"))?;
    // SAFETY: the parent created and initialised this mapping before spawning us.
    let ring = unsafe { Ring::attach(shmem.as_ptr(), cap) };

    let (rx, recycle_tx, producer) = start_decode(args);

    for chunk in rx {
        // The one extra copy versus the in-process bin (which hands over the Arc
        // pointer): this memcpy into shared memory IS the price of isolation.
        ring.produce(&chunk);
        if let Ok(v) = Arc::try_unwrap(chunk) {
            let _ = recycle_tx.try_send(v);
        }
    }
    drop(recycle_tx);

    let decode_result = producer.join();
    match decode_result {
        Ok(Ok(_stats)) => {
            ring.finish();
            Ok(())
        }
        Ok(Err(e)) => {
            ring.fail();
            Err(anyhow!("decode failed: {e}"))
        }
        Err(_) => {
            ring.fail();
            bail!("decoder producer thread panicked");
        }
    }
}

// ============================ direct (zero-copy) ============================

/// Parent, direct mode: own the shared pool, spawn the decoder, read each chunk
/// in place via its descriptor (no byte copy), return the offset when done.
fn run_consumer_direct(args: &Args) -> Result<()> {
    // The pool must hold every output buffer that can be live at once: the
    // pipeline keeps up to max(2·threads,16) chunks in flight, the sink channel
    // holds 16, our forward ring up to `direct::RING`, and the recycle pool
    // 2·cores. Each buffer can be up to 64 MiB; ×2 covers power-of-two allocator
    // waste plus the smaller blocks left behind by Vec growth. `--ring-size` is a
    // floor. (Unlike the copy-mode byte ring, the direct pool need not be pow2.)
    let threads = if args.threads == 0 {
        std::thread::available_parallelism().map_or(4, |n| n.get())
    } else {
        args.threads
    };
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let buffers = (2 * threads).max(16) + 16 + direct::RING as usize + 2 * cores + 8;
    let est = buffers * 64 * 1024 * 1024 * 2;
    let pool_size = args.ring_size.max(est);

    let shmem = ShmemConf::new()
        .size(Direct::shared_size(pool_size))
        .create()
        .map_err(|e| anyhow!("create shared memory: {e}"))?;
    let os_id = shmem.get_os_id().to_string();

    // SAFETY: freshly-created mapping, nobody attached yet.
    let d = unsafe { Direct::init(shmem.as_ptr(), pool_size) };

    let mut child = spawn_child(args, &os_id, pool_size)?;

    let watch = d;
    let watchdog = std::thread::spawn(move || -> std::io::Result<std::process::ExitStatus> {
        let status = child.wait()?;
        // Backstop: once the child has exited, no more chunks can arrive. Mark the
        // stream terminated so the consumer can never spin forever waiting on a
        // `done` flag the child failed to publish (crash, or any exit path that
        // skipped `finish`). `fail` on abnormal exit, plain done on success.
        if status.success() {
            watch.finish();
        } else {
            watch.fail();
        }
        Ok(status)
    });

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let start = std::time::Instant::now();
    let mut total: u64 = 0;
    let mut bo = Backoff::new();
    let dbg = std::env::var_os("ISO_DEBUG").is_some();
    loop {
        if let Some(desc) = d.pop_desc() {
            bo.reset();
            // SAFETY: desc just popped; valid until we return its offset below.
            let slice = unsafe { d.slice(desc) };
            if dbg {
                let got = direct::debug_sum(slice);
                if got != desc.sum {
                    eprintln!(
                        "[iso-debug] MUTATED in flight: off={} len={} child_sum={:x} parent_sum={:x}",
                        desc.off, desc.len, desc.sum, got
                    );
                }
            }
            out.write_all(slice)?;
            total += desc.len;
            while !d.try_push_return(desc.off) {
                bo.snooze();
            }
        } else if d.is_done() {
            // Re-check once: a descriptor published just before `done` is visible
            // after the Acquire that saw `done`. (In practice the child only
            // finishes once every chunk is already returned, so this is empty.)
            if let Some(desc) = d.pop_desc() {
                let slice = unsafe { d.slice(desc) };
                out.write_all(slice)?;
                total += desc.len;
                while !d.try_push_return(desc.off) {
                    bo.snooze();
                }
            } else {
                break;
            }
        } else {
            bo.snooze();
        }
    }
    out.flush()?;
    let elapsed = start.elapsed();

    let status = watchdog
        .join()
        .map_err(|_| anyhow!("watchdog thread panicked"))?
        .context("wait for decoder child")?;

    if d.errored() || !status.success() {
        bail!("decoder child failed (exit: {status}); output may be truncated");
    }

    if args.verbose {
        let mb = total as f64 / (1024.0 * 1024.0);
        eprintln!(
            "[isolated/direct] {:.1} MB in {:.3} s = {:.1} MB/s (pool {} MiB)",
            mb,
            elapsed.as_secs_f64(),
            mb / elapsed.as_secs_f64(),
            pool_size / (1024 * 1024),
        );
    }
    Ok(())
}

/// Reclaim every offset the parent has returned: drop our `Arc` (freeing the pool
/// buffer) or recycle it for a worker to refill.
fn drain_returns(
    d: &Direct,
    inflight: &mut HashMap<u64, Arc<Vec<u8>>>,
    recycle_tx: &crossbeam_channel::Sender<Vec<u8>>,
) {
    let no_recycle = std::env::var_os("ISO_NO_RECYCLE").is_some();
    while let Some(off) = d.pop_return() {
        if let Some(arc) = inflight.remove(&off) {
            if !no_recycle {
                if let Ok(v) = Arc::try_unwrap(arc) {
                    let _ = recycle_tx.try_send(v);
                }
            }
        }
    }
}

/// Child, direct mode: the decode pipeline's output `Vec`s already live in the
/// shared pool (thanks to the global allocator), so we only publish their
/// (offset,len) — no byte copy.
fn run_decoder_direct(args: &Args, os_id: &str) -> Result<()> {
    let pool_size = args.ring_size; // power of two from the parent
    let shmem = ShmemConf::new()
        .os_id(os_id)
        .open()
        .map_err(|e| anyhow!("open shared memory {os_id}: {e}"))?;
    // SAFETY: the parent created and initialised this mapping before spawning us.
    let d = unsafe { Direct::attach(shmem.as_ptr(), pool_size) };

    // Point the global allocator at the pool BEFORE any large allocation or worker
    // thread starts, so output buffers land in shared memory.
    shm_alloc::init(d.pool(), d.pool_size());
    let data_base = d.pool() as usize;
    let pool_end = data_base + d.pool_size();

    let (rx, recycle_tx, producer) = start_decode(args);

    let mut inflight: HashMap<u64, Arc<Vec<u8>>> = HashMap::new();
    let mut bo = Backoff::new();
    let dbg = std::env::var_os("ISO_DEBUG").is_some();
    let mut exhausted = false;

    for chunk in rx {
        drain_returns(&d, &mut inflight, &recycle_tx);

        let ptr = chunk.as_ptr() as usize;
        let (off, holder) = if shm_alloc::in_pool(chunk.as_ptr()) && ptr + chunk.len() <= pool_end {
            ((ptr - data_base) as u64, chunk.clone())
        } else {
            // Fallback: output landed on the System heap (sub-threshold tiny chunk,
            // or pool was full). Copy it into a pool-backed buffer; force a
            // >=THRESHOLD allocation so it actually lands in the pool.
            let mut v: Vec<u8> = Vec::with_capacity(chunk.len().max(shm_alloc::THRESHOLD));
            v.extend_from_slice(&chunk);
            let p = v.as_ptr() as usize;
            if !(shm_alloc::in_pool(v.as_ptr()) && p + v.len() <= pool_end) {
                // Pool can't hold this chunk. Stop publishing and tear down
                // gracefully (see after the loop) — do NOT process::exit here:
                // worker threads are mid-write into pool memory, and exiting under
                // them races teardown into a SIGSEGV.
                exhausted = true;
                break;
            }
            ((p - data_base) as u64, Arc::new(v))
        };
        let desc = Desc {
            off,
            len: chunk.len() as u64,
            sum: if dbg { direct::debug_sum(&chunk) } else { 0 },
        };
        // Insert before publishing so an immediate parent return finds it.
        if dbg && inflight.contains_key(&off) {
            eprintln!("[iso-debug] DUP offset {off} still in-flight (aliasing!)");
        }
        inflight.insert(off, holder);
        bo.reset();
        while !d.try_push_desc(desc) {
            drain_returns(&d, &mut inflight, &recycle_tx);
            bo.snooze();
        }
    }
    drop(recycle_tx);

    if exhausted {
        // Tell the parent to stop, then drain the decode cleanly: dropping `rx`
        // (consumed by the loop) disconnects the sink so the pipeline unwinds and
        // every worker thread joins inside `read_gz`. Only after that is it safe
        // to return (and unmap the pool).
        d.fail();
        let _ = producer.join();
        bail!(
            "shared pool exhausted (in-flight chunks exceeded the pool); \
             raise --ring-size"
        );
    }

    // Wait for the parent to consume and return every published chunk.
    let mut bo = Backoff::new();
    while !inflight.is_empty() {
        let mut progressed = false;
        while let Some(off) = d.pop_return() {
            inflight.remove(&off);
            progressed = true;
        }
        if inflight.is_empty() {
            break;
        }
        if !progressed {
            bo.snooze();
        }
    }

    match producer.join() {
        Ok(Ok(_stats)) => {
            d.finish();
            Ok(())
        }
        Ok(Err(e)) => {
            d.fail();
            Err(anyhow!("decode failed: {e}"))
        }
        Err(_) => {
            d.fail();
            bail!("decoder producer thread panicked");
        }
    }
}
