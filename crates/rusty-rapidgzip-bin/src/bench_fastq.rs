//! `bench-fastq`: decode a gzip/bgzf FASTQ and split it into columnar records
//! via `read_gz_into_fastq`, writing names / sequences / qualities to three
//! separate files (default `/dev/null`, for measuring decode+split throughput
//! without I/O in the way).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::bounded;
use rusty_rapidgzip::{read_gz_into_fastq, Config, FastqChunk, Verbosity};

#[derive(Parser, Debug)]
#[command(name = "bench-fastq", version)]
struct Args {
    /// Input .gz / .bgzf FASTQ file.
    input: PathBuf,
    /// Number of worker threads (0 = auto).
    #[arg(short = 'P', long, default_value_t = 0)]
    threads: usize,
    /// Approximate chunk size in bytes.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    chunk_size: usize,
    /// Where to write read names (one per line).
    #[arg(long, default_value = "/dev/null")]
    name_out: PathBuf,
    /// Where to write sequences (one per line).
    #[arg(long, default_value = "/dev/null")]
    seq_out: PathBuf,
    /// Where to write qualities (one per line).
    #[arg(long, default_value = "/dev/null")]
    qual_out: PathBuf,
    /// Print decode diagnostics + a throughput summary to stderr.
    #[arg(short = 'v', long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let cfg = Config {
        num_threads: args.threads,
        chunk_size_bytes: args.chunk_size,
        verbose: if args.verbose { Verbosity::On } else { Verbosity::Off },
        ..Config::default()
    };

    let (tx, rx) = bounded::<FastqChunk>(16);

    let input = args.input.clone();
    let producer = std::thread::spawn(move || read_gz_into_fastq(&input, tx, cfg));

    let mut name_w = BufWriter::new(File::create(&args.name_out)?);
    let mut seq_w = BufWriter::new(File::create(&args.seq_out)?);
    let mut qual_w = BufWriter::new(File::create(&args.qual_out)?);

    let start = std::time::Instant::now();
    let mut records: u64 = 0;
    let mut payload: u64 = 0;

    for chunk in rx {
        for name in chunk.names.iter() {
            name_w.write_all(name)?;
            name_w.write_all(b"\n")?;
            payload += name.len() as u64;
        }
        for seq in chunk.reads.iter_seq() {
            seq_w.write_all(seq)?;
            seq_w.write_all(b"\n")?;
            payload += seq.len() as u64;
        }
        for qual in chunk.reads.iter_qual() {
            qual_w.write_all(qual)?;
            qual_w.write_all(b"\n")?;
            payload += qual.len() as u64;
        }
        records += chunk.names.len() as u64;
    }

    name_w.flush()?;
    seq_w.flush()?;
    qual_w.flush()?;

    let stats = producer.join().expect("decode producer thread panicked")?;

    if args.verbose {
        let secs = start.elapsed().as_secs_f64();
        let mib = stats.uncompressed_bytes as f64 / (1024.0 * 1024.0);
        eprintln!(
            "bench-fastq: {records} records, {payload} payload bytes, \
             {:.1} MiB decompressed in {secs:.3}s ({:.1} MiB/s)",
            mib,
            mib / secs,
        );
    }

    Ok(())
}
