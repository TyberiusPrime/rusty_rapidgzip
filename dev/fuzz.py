#!/usr/bin/env python3
"""AFL fuzzing driver for rusty-rapidgzip.

Usage:
    python dev/fuzz.py              # build + run the decode fuzzer (1 core)
    python dev/fuzz.py build        # build fuzz targets only
    python dev/fuzz.py run [N]      # run the decode fuzzer on N cores (default 1)
    python dev/fuzz.py fastq [N]    # run the FASTQ columnar-split fuzzer on N cores
    python dev/fuzz.py minimize     # minimize corpus
    python dev/fuzz.py crash        # replay crashes

Two targets:
  * fuzz_rapidgzip (run) — gzip/BGZF decode kernels, fast/safe differential.
  * fuzz_fastq (fastq)   — the FASTQ columnar split, with a chunk-boundary
                           invariance oracle (same stream split differently must
                           yield identical columns; exercises the carry-stitch).

The harness drives three decode paths (see MAIN_RS):

  * decode_all              — serial, multi-member inflate (the regular gzip path)
  * decode_one_indexed_fast — the perf-tuned `fast_inflate` kernel used by the
                              BGZF pipeline for each independent member (unsafe)
  * decode_one_indexed_safe — pure-safe reference inflater

  fast vs safe form a differential oracle: on any input both accept they must
  produce byte-identical output and agree on bytes consumed.  A divergence here
  is a real bug in the unsafe fast kernel.

  The parallel pipeline is NOT exercised by this harness — AFL thread-spawn
  overhead per trial kills throughput, and concurrency bugs are better caught by
  integration tests + loom/TSAN.  Use `python dev/fuzz.py run N` to get N-core
  coverage instead.

Multi-core: run N > 1 to launch one AFL master + (N-1) secondary instances, all
sharing the findings directory.  The master runs in the foreground; secondaries
are background subprocesses killed on Ctrl-C.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FUZZ_DIR = PROJECT_ROOT / "fuzz"
CORPUS_DIR = FUZZ_DIR / "corpus"
OUTPUT_DIR = FUZZ_DIR / "findings"
# Separate corpus/output for the FASTQ-split target (`fastq` command): it eats
# the *decompressed* stream, so it wants plain FASTQ text seeds, not gzip.
FASTQ_CORPUS_DIR = FUZZ_DIR / "corpus_fastq"
FASTQ_OUTPUT_DIR = FUZZ_DIR / "findings_fastq"


def binary_path(name="fuzz_rapidgzip"):
    """Resolve a fuzz binary by name, honoring any target-dir override (this repo
    redirects cargo's target dir, so it is not the literal `fuzz/target`)."""
    target_dir = FUZZ_DIR / "target"
    try:
        meta = subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=FUZZ_DIR,
        )
        target_dir = Path(json.loads(meta)["target_directory"])
    except (subprocess.CalledProcessError, FileNotFoundError, KeyError, ValueError):
        pass
    return target_dir / "release" / name


CARGO_TOML = """\
# Detached from the parent /project workspace: this crate lives inside the
# workspace directory but is not a member, so it needs its own (empty) root.
[workspace]

[package]
name = "fuzz-rapidgzip"
version = "0.0.0"
edition = "2021"
publish = false

[[bin]]
name = "fuzz_rapidgzip"
path = "src/main.rs"

[[bin]]
name = "fuzz_fastq"
path = "src/fastq.rs"

[dependencies]
afl = "*"
rusty-rapidgzip = { path = "../crates/rusty-rapidgzip" }
"""

FASTQ_RS = """\
//! Fuzz target for the FASTQ columnar split (the `read_gz_into_fastq` record
//! parser, reached via the `fastq_split_for_test` hook so we feed the
//! decompressed stream directly, chopped into arbitrary chunks).
//!
//! Oracle: **chunk-boundary independence**. The columns must be byte-identical
//! no matter where the stream was split — whole, an irregular data-driven
//! chunking, and (for small inputs) byte-by-byte, which forces the
//! partial-record carry-stitch at every position. Any divergence — in the
//! columns OR in the Ok/Err status — is a real stitcher bug, not just a panic.

use rusty_rapidgzip::{fastq_split_for_test as split, Error};

type Cols = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Irregular chunking with sizes driven by the data bytes (1..=17).
fn derive(data: &[u8]) -> Vec<&[u8]> {
    let mut v = Vec::new();
    let mut i = 0;
    while i < data.len() {
        let step = 1 + (data[i] as usize % 17);
        let end = (i + step).min(data.len());
        v.push(&data[i..end]);
        i = end;
    }
    v
}

fn agree(a: &Result<Cols, Error>, b: &Result<Cols, Error>, label: &str) {
    match (a, b) {
        (Ok(x), Ok(y)) => assert_eq!(x, y, "columns differ between chunkings: {label}"),
        (Err(_), Err(_)) => {}
        _ => panic!("Ok/Err status depends on chunk boundaries: {label}"),
    }
}

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let whole = split(&[data]);
        agree(&whole, &split(&derive(data)), "irregular");
        if data.len() <= 4096 {
            let bb: Vec<&[u8]> = data.chunks(1).collect();
            let bb = if bb.is_empty() { vec![&data[..]] } else { bb };
            agree(&whole, &split(&bb), "byte-by-byte");
        }
    });
}
"""

MAIN_RS = """\
use rusty_rapidgzip::gzip;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        // 1. Whole-file, multi-member serial decode (regular gzip path).
        let mut whole = Vec::new();
        let _ = gzip::decode_all(data, &mut whole);

        // 2 & 3. Single-member decode through both kernels.
        //   * decode_one_indexed_fast: the perf-tuned `fast_inflate` kernel
        //     (contains `unsafe`) used by the BGZF pipeline for each independent
        //     member.
        //   * decode_one_indexed_safe: the pure-safe puff-style reference.
        //
        // Both share header parsing and trailer (CRC + ISIZE) validation, so on
        // success they MUST agree byte-for-byte and on bytes-consumed.  A
        // divergence is a real bug in the unsafe fast kernel, not just a panic.
        let mut fast = Vec::new();
        let mut safe = Vec::new();
        let rf = gzip::decode_one_indexed_fast(data, &mut fast, 0);
        let rs = gzip::decode_one_indexed_safe(data, &mut safe, 0);
        // Fast and safe share header + trailer handling, so they must agree on
        // whether the stream is valid at all.  A one-Ok/one-Err split is a real
        // divergence in the unsafe fast kernel (one path accepting bytes the
        // other rejects), not merely a different error point.
        assert_eq!(
            rf.is_ok(),
            rs.is_ok(),
            "fast/safe disagree on accept/reject: fast={rf:?}, safe={rs:?}"
        );
        if let (Ok(cf), Ok(cs)) = (rf, rs) {
            assert_eq!(cf, cs, "fast/safe disagree on bytes consumed");
            assert_eq!(fast, safe, "fast/safe disagree on decoded output");
        }
    });
}
"""


def ensure_crate():
    src = FUZZ_DIR / "src"
    src.mkdir(parents=True, exist_ok=True)

    # Always rewrite the generated sources so edits here take effect on rebuild.
    (FUZZ_DIR / "Cargo.toml").write_text(CARGO_TOML)
    (src / "main.rs").write_text(MAIN_RS)
    (src / "fastq.rs").write_text(FASTQ_RS)


def _seed_from(glob_dirs, pattern, limit=16_384):
    """Copy fixtures matching `pattern` from each dir into the corpus."""
    copied = 0
    for seed_dir in glob_dirs:
        if not seed_dir.exists():
            continue
        for f in sorted(seed_dir.glob(pattern)):
            if not f.is_file() or f.stat().st_size >= limit:
                continue
            dst = CORPUS_DIR / f"{seed_dir.name}-{f.name}"
            if not dst.exists():
                shutil.copy2(f, dst)
                copied += 1
    return copied


def seed_corpus():
    CORPUS_DIR.mkdir(parents=True, exist_ok=True)

    # Plain gzip streams: prime decode_all / the in-tree + safe single-member
    # decoders.
    gz = _seed_from([PROJECT_ROOT / "tests" / "corpus"], "*.gz")

    # BGZF members: these carry the mandatory `BC` FEXTRA subfield, so they are
    # the primer that keeps the fast/BGZF kernel under coverage — without a real
    # BGZF seed, raw AFL mutations almost never reconstruct a valid member header
    # and the fast path is never reached.
    bgz_dir = (
        PROJECT_ROOT / "rapidgzip_cpp" / "librapidarchive" / "src" / "tests" / "data"
    )
    bgz = _seed_from([bgz_dir], "*.bgz")

    print(f"[*] Seeded corpus: {gz} gzip + {bgz} bgzf -> {CORPUS_DIR}")
    if gz + bgz == 0:
        print("[!] No seeds found — the fuzzer will start from an empty corpus.")


def build():
    ensure_crate()
    print("[*] Building AFL fuzz target …")
    subprocess.check_call(
        ["cargo", "afl", "build", "--release"],
        cwd=FUZZ_DIR,
    )
    print(f"[+] Binary: {binary_path()}")


def _afl_cmd(binary, corpus, output, instance_name):
    return [
        "cargo",
        "afl",
        "fuzz",
        "-i",
        str(corpus),
        "-o",
        str(output),
        "-m",
        "none",
        "-M" if instance_name == "fuzzer00" else "-S",
        instance_name,
        str(binary),
    ]


def run(jobs=1):
    build()  # always rewrite sources + incremental compile; fast no-op if unchanged

    seed_corpus()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    binary = binary_path()
    print(f"[*] Corpus : {CORPUS_DIR}")
    print(f"[*] Output : {OUTPUT_DIR}")
    print(f"[*] Binary : {binary}")
    if jobs > 1:
        print(f"[*] Cores  : {jobs} (1 master + {jobs - 1} secondary)")
    print()

    secondaries = []
    try:
        for i in range(1, jobs):
            name = f"fuzzer{i:02d}"
            print(f"[*] Starting secondary {name} in background …")
            secondaries.append(
                subprocess.Popen(_afl_cmd(binary, CORPUS_DIR, OUTPUT_DIR, name))
            )
        master_name = "fuzzer00" if jobs > 1 else "default"
        master_cmd = (
            _afl_cmd(binary, CORPUS_DIR, OUTPUT_DIR, master_name)
            if jobs > 1
            else [
                "cargo",
                "afl",
                "fuzz",
                "-i",
                str(CORPUS_DIR),
                "-o",
                str(OUTPUT_DIR),
                "-m",
                "none",
                str(binary),
            ]
        )
        subprocess.check_call(master_cmd)
    finally:
        for p in secondaries:
            p.terminate()


def seed_fastq_corpus():
    """Seed the FASTQ-split corpus with small valid FASTQ files. Mutations of
    valid FASTQ are what reach the interesting carry-stitch happy path; without
    them raw AFL bytes almost always fail validation immediately."""
    FASTQ_CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    seeds = {
        "basic.fq": b"@r1 desc\nACGT\n+\nIIII\n@r2\nGGGG\n+\n####\n",
        # quality lines that start with '@' / contain '+' — the ambiguity trap.
        "ambiguous_qual.fq": b"@a\nACGT\n+\n@!+I\n@b\nTTTT\n+\n+@?J\n",
        "varlen.fq": b"@a\nA\n+\nI\n@b\nACGTACGT\n+\nIIIIIIII\n@c\nACG\n+\nJJJ\n",
        "crlf.fq": b"@a\r\nAC\r\n+\r\nII\r\n@b\r\nGT\r\n+\r\n##\r\n",
        "no_final_nl.fq": b"@a\nAC\n+\nII\n@b\nGT\n+\n##",
    }
    copied = 0
    for name, body in seeds.items():
        dst = FASTQ_CORPUS_DIR / name
        if not dst.exists():
            dst.write_bytes(body)
            copied += 1
    print(f"[*] Seeded FASTQ corpus: {copied} new seed(s) -> {FASTQ_CORPUS_DIR}")


def run_fastq(jobs=1):
    build()  # always rewrite sources + incremental compile; fast no-op if unchanged
    binary = binary_path("fuzz_fastq")
    seed_fastq_corpus()
    FASTQ_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[*] Corpus : {FASTQ_CORPUS_DIR}")
    print(f"[*] Output : {FASTQ_OUTPUT_DIR}")
    print(f"[*] Binary : {binary}")
    if jobs > 1:
        print(f"[*] Cores  : {jobs} (1 master + {jobs - 1} secondary)")
    print()
    secondaries = []
    try:
        for i in range(1, jobs):
            name = f"fuzzer{i:02d}"
            print(f"[*] Starting secondary {name} in background …")
            secondaries.append(
                subprocess.Popen(
                    _afl_cmd(binary, FASTQ_CORPUS_DIR, FASTQ_OUTPUT_DIR, name)
                )
            )
        master_cmd = (
            _afl_cmd(binary, FASTQ_CORPUS_DIR, FASTQ_OUTPUT_DIR, "fuzzer00")
            if jobs > 1
            else [
                "cargo",
                "afl",
                "fuzz",
                "-i",
                str(FASTQ_CORPUS_DIR),
                "-o",
                str(FASTQ_OUTPUT_DIR),
                "-m",
                "none",
                str(binary),
            ]
        )
        subprocess.check_call(master_cmd)
    finally:
        for p in secondaries:
            p.terminate()


def minimize():
    # Single-instance AFL writes to output/default/; multi-instance to output/fuzzernn/.
    queue = next(
        (
            OUTPUT_DIR / d / "queue"
            for d in ("default", "fuzzer00")
            if (OUTPUT_DIR / d / "queue").exists()
        ),
        None,
    )
    if queue is None:
        print("[!] No findings to minimize — run the fuzzer first.")
        sys.exit(1)
    mini = OUTPUT_DIR / "minimized"
    mini.mkdir(parents=True, exist_ok=True)
    print("[*] Minimizing corpus …")
    subprocess.check_call(
        [
            "cargo",
            "afl",
            "cmin",
            "-i",
            str(queue),
            "-o",
            str(mini),
            "--",
            str(binary_path()),
        ],
    )
    print(f"[+] Minimized corpus: {mini}")


def replay_crashes():
    crashes_dirs = [
        OUTPUT_DIR / d / "crashes"
        for d in [
            "default",
        ]
        + [f"fuzzer{i:02}" for i in range(0, 65)]
        if (OUTPUT_DIR / d / "crashes").exists()
    ]
    if len(crashes_dirs) == 0:
        print("[!] No crashes directory — run the fuzzer first.")
        sys.exit(1)

    crashes = []
    for dir in crashes_dirs:
        crashes.extend(
            [p for p in dir.iterdir() if p.is_file() and p.name != "README.txt"]
        )
    crashes = sorted(crashes, key=lambda path: path.stat().st_size)
    if not crashes:
        print("[*] No crashes found.")
        return

    binary = binary_path()
    print(f"[*] {len(crashes)} crash(es) to replay:\n")
    for crash in crashes:
        label = crash.name
        print(f"--- {label} ({crash.stat().st_size} bytes) ---")
        print(f"{crash.relative_to(PROJECT_ROOT)}")
        with open(crash, "rb") as fh:
            result = subprocess.run(
                [str(binary)],
                stdin=fh,
                capture_output=True,
                timeout=10,
            )
        print(f"  exit code: {result.returncode}")
        if result.stderr:
            for line in result.stderr.decode(errors="replace").splitlines():
                print(f"  | {line}")
        print()


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    # Optional jobs argument for run/fastq: `python dev/fuzz.py run 4`
    jobs = 1
    if len(sys.argv) > 2 and cmd in ("run", "fastq"):
        try:
            jobs = int(sys.argv[2])
            if jobs < 1:
                raise ValueError
        except ValueError:
            print(f"[!] Jobs must be a positive integer, got {sys.argv[2]!r}")
            sys.exit(1)

    commands = {
        "build": build,
        "run": lambda: run(jobs),
        "fastq": lambda: run_fastq(jobs),
        "minimize": minimize,
        "crash": replay_crashes,
    }

    fn = commands.get(cmd)
    if fn is None:
        print(f"Usage: {sys.argv[0]} [{'|'.join(commands)}] [jobs]")
        sys.exit(1)
    fn()


if __name__ == "__main__":
    main()
