#!/usr/bin/env python3
"""AFL fuzzing driver for rusty-rapidgzip.

Usage:
    python dev/fuzz.py          # build + run fuzzer
    python dev/fuzz.py build    # build fuzz target only
    python dev/fuzz.py run      # run fuzzer (builds first if needed)
    python dev/fuzz.py minimize # minimize corpus
    python dev/fuzz.py crash    # replay crashes

The fast `fast_inflate` kernel is the default decode path now — there is no
`RAPIDGZIP_KERNEL` knob anymore. The harness drives it directly (see MAIN_RS):

  * decode_all              — serial, multi-member, in-tree inflate
  * decode_one_indexed_fast — the perf-tuned `fast_inflate` kernel every BGZF
                              member decodes through (the "fast path")
  * decode_one_indexed_safe — pure-safe reference inflater, used as a
                              differential oracle against the fast kernel

The only remaining engine env var is RAPIDGZIP_INFLATE (intree|safe), which the
harness does NOT set — it calls each kernel explicitly so a single run exercises
all of them regardless of the environment.
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


def binary_path():
    """Resolve the fuzz binary, honoring any target-dir override (this repo
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
    return target_dir / "release" / "fuzz_rapidgzip"

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

[dependencies]
afl = "*"
rusty-rapidgzip = { path = "../crates/rusty-rapidgzip" }
"""

MAIN_RS = """\
use rusty_rapidgzip::gzip;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        // 1. Whole-file, multi-member serial decode (in-tree inflate).
        let mut whole = Vec::new();
        let _ = gzip::decode_all(data, &mut whole);

        // 2 & 3. Single-member decode through the two kernels that matter.
        //   * decode_one_indexed_fast: the perf-tuned `fast_inflate` kernel
        //     (contains `unsafe`) that the BGZF fast-path drives every member
        //     through. This IS the default/fast path — any valid gzip or BGZF
        //     member header takes the input straight into the kernel.
        //   * decode_one_indexed_safe: the pure-safe puff-style reference.
        //
        // Both share header parsing and trailer (CRC + ISIZE) validation, so on
        // success they MUST agree byte-for-byte and on bytes-consumed. That is a
        // differential oracle over the `unsafe` fast kernel: a divergence here is
        // a real bug, not just a panic.
        let mut fast = Vec::new();
        let mut safe = Vec::new();
        let rf = gzip::decode_one_indexed_fast(data, &mut fast, 0);
        let rs = gzip::decode_one_indexed_safe(data, &mut safe, 0);
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


def _seed_from(glob_dirs, pattern, limit=200_000):
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
    bgz_dir = PROJECT_ROOT / "rapidgzip_cpp" / "librapidarchive" / "src" / "tests" / "data"
    bgz = _seed_from([bgz_dir], "*.bgz")

    print(f"[*] Seeded corpus: {gz} gzip + {bgz} bgzf fixtures -> {CORPUS_DIR}")
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


def run():
    if not binary_path().exists():
        build()

    seed_corpus()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    binary = binary_path()
    print(f"[*] Corpus : {CORPUS_DIR}")
    print(f"[*] Output : {OUTPUT_DIR}")
    print(f"[*] Binary : {binary}")
    print()

    subprocess.check_call(
        [
            "cargo", "afl", "fuzz",
            "-i", str(CORPUS_DIR),
            "-o", str(OUTPUT_DIR),
            "-m", "none",
            str(binary),
        ],
    )


def minimize():
    queue = OUTPUT_DIR / "default" / "queue"
    if not queue.exists():
        print("[!] No findings to minimize — run the fuzzer first.")
        sys.exit(1)
    mini = OUTPUT_DIR / "minimized"
    mini.mkdir(parents=True, exist_ok=True)
    print("[*] Minimizing corpus …")
    subprocess.check_call(
        [
            "cargo", "afl", "cmin",
            "-i", str(queue),
            "-o", str(mini),
            "--",
            str(binary_path()),
        ],
    )
    print(f"[+] Minimized corpus: {mini}")


def replay_crashes():
    crashes_dir = OUTPUT_DIR / "default" / "crashes"
    if not crashes_dir.exists():
        print("[!] No crashes directory — run the fuzzer first.")
        sys.exit(1)

    crashes = sorted(
        p for p in crashes_dir.iterdir()
        if p.is_file() and p.name != "README.txt"
    )
    if not crashes:
        print("[*] No crashes found.")
        return

    binary = binary_path()
    print(f"[*] {len(crashes)} crash(es) to replay:\n")
    for crash in crashes:
        label = crash.name
        print(f"--- {label} ({crash.stat().st_size} bytes) ---")
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
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
    else:
        cmd = "run"

    commands = {
        "build": build,
        "run": run,
        "minimize": minimize,
        "crash": replay_crashes,
    }

    fn = commands.get(cmd)
    if fn is None:
        print(f"Usage: {sys.argv[0]} [{'|'.join(commands)}]")
        sys.exit(1)
    fn()


if __name__ == "__main__":
    main()
