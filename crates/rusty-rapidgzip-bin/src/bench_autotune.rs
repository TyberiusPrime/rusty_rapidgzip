//! Autotune benchmark: measure *total process CPU-seconds* (not wall-clock) of
//! a full decode under the two regimes where shedding decode workers should pay
//! off — a slow co-located consumer, and an oversubscribed (shared) box.
//!
//! Wall-clock is the wrong metric here: in both regimes the bounded channels
//! already cap throughput at the consumer/contention rate, so wall-time barely
//! moves with worker count. The waste shows up as CPU-seconds — futex wake/block
//! churn (sys), context switches, and cache/bandwidth interference with the
//! consumer. We measure `getrusage(RUSAGE_SELF)` user+sys across the decode.
//!
//! Because the decode pool's gate starts wide open (`desired == ceiling`), `-P N`
//! is exactly "N active workers" — so sweeping `-P` traces the CPU-seconds curve
//! the future autotune controller must navigate. This binary establishes that
//! ground truth; it does not (yet) drive the gate at runtime.
//!
//! ## Regimes
//!
//! * `--consumer-work K`: synthetic CPU-bound consumer (think: an aligner). The
//!   sink-reader loop touches every output byte and folds it through `K` mixing
//!   rounds per byte. `K=0` just touches the bytes (fast-sink baseline). Larger
//!   `K` makes the single-threaded consumer the bottleneck, so excess decode
//!   workers pile into back-pressure and waste cycles.
//! * `--bg-load N`: spawn a *separate child process* burning CPU on `N` threads
//!   to model a shared box. A child (not in-process threads) keeps our
//!   `RUSAGE_SELF` measurement clean — it reflects only the decoder + consumer.
//!
//! Pin the whole invocation with `taskset` (see bench_autotune.sh) to bound the
//! core set both the decoder and any `--bg-load` child compete over.
//!
//! Hidden `--burn N` mode: the process re-execs itself with this flag to act as
//! the background CPU hog; it spins `N` threads forever until the parent kills it.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use rusty_rapidgzip::{read_gz, Config, Verbosity};

#[derive(Parser, Debug)]
#[command(name = "bench-autotune", version)]
struct Args {
    /// Input .gz / .bgzf file.
    input: Option<PathBuf>,
    /// Worker-thread ceiling (== active workers; 0 = auto).
    #[arg(short = 'P', long, default_value_t = 0)]
    threads: usize,
    /// Approximate chunk size in bytes.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,
    /// Synthetic consumer cost: mixing rounds applied per output byte. 0 = just
    /// touch the bytes (fast-sink baseline).
    #[arg(long, default_value_t = 0)]
    consumer_work: u64,
    /// Number of consumer threads. >1 models a CPU-bound *parallel* downstream
    /// (e.g. an aligner) that can absorb cores freed by shedding decode workers
    /// — the regime where mid-run shedding cuts end-to-end wall, not just RSS.
    #[arg(long, default_value_t = 1)]
    consumer_threads: usize,
    /// Spawn a background CPU-burner child on this many threads (shared-box
    /// model). 0 = none. The child is a separate process, excluded from our
    /// CPU-seconds measurement.
    #[arg(long, default_value_t = 0)]
    bg_load: usize,
    /// Emit a single tab-separated row (for sweep scripts) instead of a
    /// human-readable block. Columns: P work bg wall_s cpu_user_s cpu_sys_s
    /// cpu_total_s maxrss_mib uncompressed_gib cpu_s_per_gib.
    #[arg(long)]
    tsv: bool,
    /// Forward pipeline diagnostics (incl. autotune decisions) to stderr.
    #[arg(short = 'v', long)]
    verbose: bool,
    /// Hidden: act as the background CPU hog (spins `N` threads forever).
    #[arg(long, hide = true)]
    burn: Option<usize>,
}

/// Touch every byte and fold it through `work` mixing rounds. Returns an
/// accumulator the caller must consume so the loop can't be optimised away.
#[inline(never)]
fn consume(chunk: &[u8], work: u64) -> u64 {
    let mut acc: u64 = 0;
    for &b in chunk {
        let mut x = b as u64;
        for _ in 0..work {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        }
        acc = acc.wrapping_add(x);
    }
    acc
}

/// `getrusage(RUSAGE_SELF)` user/sys CPU-seconds and peak RSS (MiB).
fn rusage() -> (f64, f64, f64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    // ru_maxrss is KiB on Linux.
    (tv(ru.ru_utime), tv(ru.ru_stime), ru.ru_maxrss as f64 / 1024.0)
}

/// Spawn `bg_load` worth of background CPU contention as a child process.
fn spawn_bg(bg_load: usize) -> Result<Option<Child>> {
    if bg_load == 0 {
        return Ok(None);
    }
    let exe = std::env::current_exe()?;
    let child = Command::new(exe).arg("--burn").arg(bg_load.to_string()).spawn()?;
    // Give it a beat to ramp the burner threads up before we start timing.
    std::thread::sleep(std::time::Duration::from_millis(100));
    Ok(Some(child))
}

fn burn(n: usize) -> ! {
    for _ in 1..n {
        std::thread::spawn(|| {
            let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
            loop {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                std::hint::black_box(x);
            }
        });
    }
    let mut x: u64 = 1;
    loop {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        std::hint::black_box(x);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(n) = args.burn {
        burn(n.max(1));
    }
    let input = args.input.clone().expect("input file required (unless --burn)");

    // Recycle channel: drained Vecs flow back to workers so pages stay faulted
    // (mirrors the real CLI so the bench reflects production memory behaviour).
    let recycle_cap = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4) * 2;
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(recycle_cap);

    let cfg = Config {
        num_threads: args.threads,
        chunk_size_bytes: args.chunk_size,
        verbose: if args.verbose { Verbosity::On } else { Verbosity::Off },
        recycle_rx: Some(recycle_rx),
        recycle_tx: Some(recycle_tx.clone()),
    };

    let mut bg = spawn_bg(args.bg_load)?;

    let (tx, rx) = bounded::<Arc<Vec<u8>>>(16);
    let producer = std::thread::spawn(move || read_gz(&input, tx, cfg));

    // Measure CPU across the decode+consume window only.
    let (u0, s0, _) = rusage();
    let wall0 = Instant::now();

    let work = args.consumer_work;
    let total_uncompressed = std::sync::atomic::AtomicU64::new(0);
    let n_consumers = args.consumer_threads.max(1);

    // Each consumed chunk: fold its bytes (CPU cost), tally length, recycle the
    // Vec. Shared by the inline and pooled paths.
    let consume_chunk = |chunk: Arc<Vec<u8>>, acc: &mut u64| {
        *acc = acc.wrapping_add(consume(&chunk, work));
        total_uncompressed.fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::Relaxed);
        if let Ok(v) = Arc::try_unwrap(chunk) {
            let _ = recycle_tx.try_send(v);
        }
    };

    if n_consumers == 1 {
        let mut acc: u64 = 0;
        for chunk in rx {
            consume_chunk(chunk, &mut acc);
        }
        std::hint::black_box(acc);
    } else {
        // Fan chunks out to a pool of CPU-bound consumer threads. The decode
        // sink feeds this inner channel; shedding decode workers frees cores the
        // consumers can then use.
        std::thread::scope(|s| {
            let (ctx, crx) = bounded::<Arc<Vec<u8>>>(n_consumers * 2);
            for _ in 0..n_consumers {
                let crx = crx.clone();
                let consume_chunk = &consume_chunk;
                s.spawn(move || {
                    let mut acc: u64 = 0;
                    while let Ok(chunk) = crx.recv() {
                        consume_chunk(chunk, &mut acc);
                    }
                    std::hint::black_box(acc);
                });
            }
            drop(crx);
            for chunk in rx {
                if ctx.send(chunk).is_err() {
                    break;
                }
            }
            drop(ctx);
        });
    }
    drop(recycle_tx);
    producer.join().expect("producer panicked")?;
    let total_uncompressed = total_uncompressed.load(std::sync::atomic::Ordering::Relaxed);

    let wall = wall0.elapsed().as_secs_f64();
    let (u1, s1, maxrss) = rusage();
    if let Some(c) = bg.as_mut() {
        let _ = c.kill();
        let _ = c.wait();
    }

    let cpu_user = u1 - u0;
    let cpu_sys = s1 - s0;
    let cpu_total = cpu_user + cpu_sys;
    let gib = total_uncompressed as f64 / (1024.0 * 1024.0 * 1024.0);
    let cpu_per_gib = if gib > 0.0 { cpu_total / gib } else { 0.0 };
    let p = if args.threads == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    } else {
        args.threads
    };

    if args.tsv {
        println!(
            "{p}\t{}\t{}\t{wall:.3}\t{cpu_user:.3}\t{cpu_sys:.3}\t{cpu_total:.3}\t{maxrss:.1}\t{gib:.3}\t{cpu_per_gib:.3}",
            args.consumer_work, args.bg_load,
        );
    } else {
        println!(
            "P={p} consumer_work={} bg_load={}\n  \
             wall      {wall:.3}s\n  \
             cpu_user  {cpu_user:.3}s\n  \
             cpu_sys   {cpu_sys:.3}s\n  \
             cpu_total {cpu_total:.3}s   ({cpu_per_gib:.3} CPU-s/GiB)\n  \
             max_rss   {maxrss:.1} MiB\n  \
             decoded   {gib:.3} GiB",
            args.consumer_work, args.bg_load,
        );
    }
    Ok(())
}
