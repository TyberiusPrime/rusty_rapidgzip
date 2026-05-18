//! `rapidgzip-rs` CLI. Phase 0 stub: parses args, calls `read_gz`, copies
//! channel output to stdout. The decoder itself is not implemented yet.

use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use rapidgzip::{read_gz, Config, Verbosity};

#[derive(Parser, Debug)]
#[command(name = "rapidgzip-rs", version)]
struct Args {
    /// Input .gz file.
    input: PathBuf,
    /// Number of worker threads (0 = auto).
    #[arg(short = 'P', long, default_value_t = 0)]
    threads: usize,
    /// Approximate chunk size in bytes.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,
    /// Print per-member / per-chunk diagnostics to stderr.
    #[arg(short = 'v', long)]
    verbose: bool,
    /// Use zlib-rs (via flate2) for inflate instead of our native deflate
    /// decoder. Applies to both BGZF and regular gzip on the parallel and
    /// serial paths.
    #[arg(long)]
    zlib_rs: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Recycle channel: drained output Vecs flow back to workers so pages
    // stay faulted. Capacity ~= worker pool size; if it fills, drop the
    // Vec rather than block stdout.
    let recycle_cap = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        * 2;
    let (recycle_tx, recycle_rx) = bounded::<Vec<u8>>(recycle_cap);

    let cfg = Config {
        num_threads: args.threads,
        chunk_size_bytes: args.chunk_size,
        verbose: if args.verbose { Verbosity::On } else { Verbosity::Off },
        use_zlib_rs: args.zlib_rs,
        recycle_rx: Some(recycle_rx),
    };

    let (tx, rx) = bounded::<Vec<u8>>(16);
    eprintln!("This is the new binary");

    let input = args.input.clone();
    let producer = std::thread::spawn(move || read_gz(&input, tx, cfg));

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for chunk in rx {
        out.write_all(&chunk)?;
        // Non-blocking: if the recycle channel is full, drop the Vec.
        let _ = recycle_tx.try_send(chunk);
    }
    // Close recycle_tx so any worker still blocked on recv exits cleanly
    // (workers use try_recv though, so this is belt-and-braces).
    drop(recycle_tx);

    producer.join().expect("producer thread panicked")?;
    Ok(())
}
