//! Corpus management for rapidgzip_rs.
//!
//! Builds a deterministic set of `.gz` test files into `tests/corpus/` and
//! records the sha256 of each file's *decompressed* contents into a sidecar
//! `<file>.gz.sha256`. Tests then compare our decoder's output hash against
//! that recorded ground truth — no need to keep raw decompressed bytes on
//! disk, no need to invoke `gunzip` from inside `cargo test`.
//!
//! Subcommands:
//!   xtask build-corpus   — generate any missing fixtures (idempotent)
//!   xtask hash <file>    — print sha256 of `gunzip <file>`
//!   xtask clean-corpus
//!
//! All inputs are produced from `/dev/urandom`-derived deterministic seeds,
//! base64, or are bundled in the upstream `rapidgzip_cpp/silesia.zip`.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Generate any missing corpus fixtures. Idempotent.
    BuildCorpus {
        /// Skip the multi-GiB fixtures (default: skip).
        #[arg(long, default_value_t = false)]
        big: bool,
    },
    /// Recompute sha256 sidecars for every .gz in the corpus dir.
    RehashCorpus,
    /// Print sha256 of `gunzip <file>` to stdout.
    Hash { file: PathBuf },
    /// Remove all generated corpus files (keeps .gitkeep).
    CleanCorpus,
}

fn corpus_dir() -> PathBuf {
    // xtask is invoked via `cargo run -p xtask`, cwd = workspace root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/corpus")
}

fn main() -> Result<()> {
    let args = Args::parse();
    let dir = corpus_dir();
    fs::create_dir_all(&dir)?;

    match args.cmd {
        Cmd::BuildCorpus { big } => build_corpus(&dir, big),
        Cmd::RehashCorpus => rehash_corpus(&dir),
        Cmd::Hash { file } => {
            let h = sha256_of_gunzip(&file)?;
            println!("{h}");
            Ok(())
        }
        Cmd::CleanCorpus => clean_corpus(&dir),
    }
}

fn build_corpus(dir: &Path, big: bool) -> Result<()> {
    // Tier 1: small handcrafted (cheap, always built).
    write_gz(dir, "empty.gz", &[], 6)?;
    write_gz(dir, "hello.gz", b"hello, world\n", 6)?;
    write_gz(
        dir,
        "ascii_1k.gz",
        &deterministic_ascii(1024),
        6,
    )?;
    write_gz(
        dir,
        "ascii_1m.gz",
        &deterministic_ascii(1024 * 1024),
        6,
    )?;

    // Multi-stream concat: same payload encoded as two concatenated gz streams.
    {
        let payload_a = deterministic_ascii(64 * 1024);
        let payload_b = deterministic_ascii(64 * 1024 + 17); // odd length on purpose
        let mut combined = gz_encode(&payload_a, 6)?;
        combined.extend_from_slice(&gz_encode(&payload_b, 6)?);
        let path = dir.join("multistream.gz");
        fs::write(&path, &combined)?;

        let mut h = Sha256::new();
        h.update(&payload_a);
        h.update(&payload_b);
        let digest = hex::encode(h.finalize());
        fs::write(path.with_extension("gz.sha256"), digest)?;
        println!("built {}", path.display());
    }

    // Stored-block exerciser: highly random bytes defeat compression, so
    // gzip emits stored blocks even at higher levels. (system `gzip` doesn't
    // accept -0, so we can't ask for it explicitly.)
    write_gz(
        dir,
        "incompressible_64k.gz",
        &incompressible(64 * 1024),
        1,
    )?;

    // Fixed-Huffman block — small enough that gzip picks it.
    write_gz(dir, "fixed_small.gz", b"aaaaaaaaaa", 9)?;

    // Large multi-stream: two big random payloads concatenated. This is the
    // shape of files produced by pigz / parallel-gzip pipelines. Exercises
    // the parallel pipeline's "stop at BFINAL, discard later chunks" path.
    {
        let payload_a = deterministic_ascii(8 * 1024 * 1024);
        let payload_b = deterministic_ascii(8 * 1024 * 1024 + 999);
        let mut combined = gz_encode(&payload_a, 6)?;
        combined.extend_from_slice(&gz_encode(&payload_b, 6)?);
        let path = dir.join("multistream_8m_x2.gz");
        fs::write(&path, &combined)?;
        let mut h = Sha256::new();
        h.update(&payload_a);
        h.update(&payload_b);
        let digest = hex::encode(h.finalize());
        fs::write(path.with_extension("gz.sha256"), digest)?;
        println!("built {}", path.display());
    }

    // Many small members (10×): worst-case shape for parallel — first
    // member is small enough that parallel decode barely benefits, and
    // every subsequent member falls through to the serial multi-stream path.
    {
        let mut combined = Vec::new();
        let mut h = Sha256::new();
        for i in 0..10u32 {
            let payload = deterministic_ascii(50_000 + (i * 137) as usize);
            combined.extend_from_slice(&gz_encode(&payload, 6)?);
            h.update(&payload);
        }
        let path = dir.join("multistream_many.gz");
        fs::write(&path, &combined)?;
        fs::write(path.with_extension("gz.sha256"), hex::encode(h.finalize()))?;
        println!("built {}", path.display());
    }

    // Big single-member with random-base64 content (resembles real-world
    // bioinformatics / text payloads that don't compress well). Hits many
    // dynamic blocks → many boundary-finder calls → exercises false-positive
    // rejection in the verifier.
    {
        let payload = deterministic_base64(32 * 1024 * 1024);
        write_gz(dir, "base64_32m.gz", &payload, 6)?;
    }

    if big {
        // Tier 2: heavy fixtures.
        write_gz(
            dir,
            "base64_4gib.gz",
            &deterministic_base64(4u64 * 1024 * 1024 * 1024),
            6,
        )?;
        // Silesia is bundled in the C++ tree as a .zip.
        let silesia_zip = PathBuf::from("/project/rapidgzip_cpp/silesia.zip");
        if silesia_zip.exists() {
            let dest = dir.join("silesia.tar.gz");
            if !dest.exists() {
                bail!(
                    "TODO: extract {} → tar → gzip → {}; not implemented yet",
                    silesia_zip.display(),
                    dest.display()
                );
            }
        }
    }

    Ok(())
}

/// Deterministic pseudo-random ASCII text. We want repeatable corpus across
/// machines without bringing in `rand`.
fn deterministic_ascii(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    while out.len() < n {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let b = ((s >> 56) as u8 % 95) + 32; // printable ASCII
        out.push(b);
    }
    out.truncate(n);
    out
}

/// LCG-derived bytes spanning the full 0..256 range — should mostly resist
/// compression, forcing gzip into stored blocks.
fn incompressible(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    let mut s: u64 = 0xA1B2_C3D4_E5F6_0718;
    while out.len() < n {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.extend_from_slice(&s.to_le_bytes());
    }
    out.truncate(n);
    out
}

fn deterministic_base64(n: u64) -> Vec<u8> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(n as usize);
    let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
    while (out.len() as u64) < n {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.push(alphabet[(s >> 58) as usize & 63]);
    }
    out.truncate(n as usize);
    out
}

/// Write `payload` gzipped at `level` to `<dir>/<name>` and a sha256 sidecar
/// of the *uncompressed* contents to `<dir>/<name>.sha256`.
fn write_gz(dir: &Path, name: &str, payload: &[u8], level: u32) -> Result<()> {
    let path = dir.join(name);
    let sha_path = path.with_extension("gz.sha256");
    if path.exists() && sha_path.exists() {
        return Ok(()); // idempotent
    }
    let gz = gz_encode(payload, level)?;
    fs::write(&path, &gz)?;
    let digest = sha256_of(payload);
    fs::write(&sha_path, &digest)?;
    println!("built {}  sha256={digest}", path.display());
    Ok(())
}

/// Pipe through the system `gzip`. Drain stdout on a worker thread so
/// payloads larger than the OS pipe buffer (~64 KiB) don't deadlock.
fn gz_encode(payload: &[u8], level: u32) -> Result<Vec<u8>> {
    let mut child = Command::new("gzip")
        .arg(format!("-{level}"))
        .arg("-c")
        .arg("-n") // suppress filename + mtime — keeps fixtures byte-stable
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning gzip")?;

    let mut stdin = child.stdin.take().unwrap();
    let payload_owned = payload.to_vec();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        stdin.write_all(&payload_owned)?;
        drop(stdin);
        Ok(())
    });

    let out = child.wait_with_output()?;
    writer.join().expect("stdin writer panicked")?;
    if !out.status.success() {
        bail!("gzip exited with {}", out.status);
    }
    Ok(out.stdout)
}

fn sha256_of(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn sha256_of_gunzip(file: &Path) -> Result<String> {
    // `gunzip` is a shell wrapper in some nix envs and breaks under bwrap.
    // `gzip -dc` is the binary entry point and works in every env we care about.
    let mut child = Command::new("gzip")
        .arg("-dc")
        .arg(file)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning gunzip")?;
    let mut stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = stdout.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    let status = child.wait()?;
    if !status.success() {
        bail!("gunzip exited with {status}");
    }
    Ok(hex::encode(h.finalize()))
}

fn rehash_corpus(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("gz") {
            continue;
        }
        let digest = sha256_of_gunzip(&path)?;
        fs::write(path.with_extension("gz.sha256"), &digest)?;
        println!("{}  sha256={digest}", path.display());
    }
    Ok(())
}

fn clean_corpus(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.file_name().and_then(|s| s.to_str()) == Some(".gitkeep") {
            continue;
        }
        if path.is_file() {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}
