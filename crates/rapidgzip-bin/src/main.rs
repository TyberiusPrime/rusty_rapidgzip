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
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config {
        num_threads: args.threads,
        chunk_size_bytes: args.chunk_size,
        verbose: if args.verbose { Verbosity::On } else { Verbosity::Off },
    };

    let (tx, rx) = bounded::<Vec<u8>>(16);

    let input = args.input.clone();
    let producer = std::thread::spawn(move || read_gz(&input, tx, cfg));

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for chunk in rx {
        out.write_all(&chunk)?;
    }

    producer.join().expect("producer thread panicked")?;
    Ok(())
}
