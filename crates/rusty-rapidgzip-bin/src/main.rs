//! `rapidgzip-rs` CLI: parses args, calls `read_gz`, copies channel output to stdout.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use rusty_rapidgzip::{elapsed_since_start, read_gz, Config, Verbosity};

#[derive(Parser, Debug)]
#[command(name = "rapidgzip-rs", version)]
struct Args {
    /// Input .gz file, or `-` for stdin.
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
        recycle_rx: Some(recycle_rx),
        recycle_tx: Some(recycle_tx.clone()),
        ..Config::default()
    };

    let (tx, rx) = bounded::<Arc<Vec<u8>>>(16);

    // `-` means stdin. `/dev/stdin` lets `read_gz` open it as a normal path:
    // when stdin is a pipe it routes to the streaming decoder, and when it's a
    // redirected regular file (`< foo.gz`) it still mmaps.
    let input = if args.input.as_os_str() == "-" {
        PathBuf::from("/dev/stdin")
    } else {
        args.input.clone()
    };
    let producer = std::thread::spawn(move || read_gz(&input, tx, cfg));

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let start = std::time::Instant::now();
    let mut last_report = start;
    let mut bytes_since_last: u64 = 0;
    let mut total_bytes: u64 = 0;
    for chunk in rx {
        out.write_all(&chunk)?;
        bytes_since_last += chunk.len() as u64;
        total_bytes += chunk.len() as u64;
        if args.verbose {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_report);
            if elapsed.as_secs_f64() >= 1.0 {
                let mbps = (bytes_since_last as f64) / elapsed.as_secs_f64() / (1024.0 * 1024.0);
                let avg = (total_bytes as f64)
                    / now.duration_since(start).as_secs_f64()
                    / (1024.0 * 1024.0);
                eprintln!(
                    "[rapidgzip +{:.2}s] {:.1} MB/s (avg {:.1} MB/s, {:.1} MB total)",
                    elapsed_since_start(),
                    mbps,
                    avg,
                    total_bytes as f64 / (1024.0 * 1024.0),
                );
                last_report = now;
                bytes_since_last = 0;
            }
        }
        // Recycle the inner Vec only if no other Arc reference is live (the
        // CRC validator may still hold one). When it doesn't, the Vec just
        // gets dropped here and the worker allocates fresh next iteration.
        if let Ok(v) = Arc::try_unwrap(chunk) {
            let _ = recycle_tx.try_send(v);
        }
    }
    // Close recycle_tx so any worker still blocked on recv exits cleanly
    // (workers use try_recv though, so this is belt-and-braces).
    drop(recycle_tx);

    producer.join().expect("producer thread panicked")?;
    Ok(())
}
