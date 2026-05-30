#!/usr/bin/env python3
"""Verify rapidgzip-rs output against reference SHA256 checksums.

Decompresses each .gz file with rapidgzip-rs, computes SHA256 of the
decompressed bytes, and compares against the reference manifest produced
by reference_checksums.py.

Usage:
    python verify_rapidgzip.py                              # verify all
    python verify_rapidgzip.py --binary /path/to/binary     # specify binary
    python verify_rapidgzip.py --id synth-empty             # verify specific datasets
    python verify_rapidgzip.py -v                           # verbose output
"""

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

CORPUS_DIR = Path(__file__).resolve().parent
MANIFEST = CORPUS_DIR / "reference_sums.json"


def find_binary():
    return ["cargo", "run", "--release", "--bin", "rusty-rapidgzip-rs"]
    project_root = CORPUS_DIR.parent.parent
    candidates = [
        project_root / "target" / "release" / "rusty-rapidgzip-rs",
        project_root / "target_claude" / "release" / "rusty-rapidgzip-rs",
        project_root / "target" / "debug" / "rusty-rapidgzip-rs",
        project_root / "target_claude" / "debug" / "rusty-rapidgzip-rs",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    for name in ("rusty-rapidgzip-rs", "rapidgzip-rs"):
        found = shutil.which(name)
        if found:
            return found
    return None


def verify_file(gz_path, binary_path, expected_sha256):
    proc = subprocess.Popen(
        binary_path + [str(gz_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    sha = hashlib.sha256()
    total = 0
    while True:
        chunk = proc.stdout.read(1 << 20)
        if not chunk:
            break
        sha.update(chunk)
        total += len(chunk)
    proc.wait()
    if proc.returncode != 0:
        stderr = proc.stderr.read().decode()
        return False, total, f"exit code {proc.returncode}: {stderr}"
    actual = sha.hexdigest()
    if actual != expected_sha256:
        return (
            False,
            total,
            f"SHA256 mismatch (expected {expected_sha256[:24]}..., "
            f"got {actual[:24]}...)",
        )
    return True, total, "OK"


def main():
    parser = argparse.ArgumentParser(
        description="Verify rapidgzip-rs against reference checksums"
    )
    parser.add_argument("--binary", help="Path to rapidgzip-rs binary")
    parser.add_argument(
        "--id",
        dest="ids",
        nargs="+",
        help="Only verify these dataset IDs",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show verbose output",
    )
    args = parser.parse_args()

    binary = [args.binary] if args.binary else find_binary()
    if not binary:
        print("Error: Could not find rapidgzip-rs binary.")
        print("Use --binary /path/to/rusty-rapidgzip-rs")
        sys.exit(1)

    if not MANIFEST.exists():
        print(f"Error: Reference manifest not found at {MANIFEST}")
        print("Run reference_checksums.py first.")
        sys.exit(1)

    with open(MANIFEST) as f:
        manifest = json.load(f)

    corpus_json = CORPUS_DIR / "corpus.json"
    with open(corpus_json) as f:
        datasets = json.load(f)["datasets"]

    if args.ids:
        id_set = set(args.ids)
        id_to_gzfile = {ds["id"]: ds["gz_file"] for ds in datasets}
        wanted_files = set()
        for wanted_id in args.ids:
            if wanted_id in id_to_gzfile:
                wanted_files.add(id_to_gzfile[wanted_id])
        manifest = {k: v for k, v in manifest.items() if k in wanted_files}

    print(f"Binary:    {binary}")
    print(f"Manifest:  {len(manifest)} entries\n")

    passed = 0
    failed = 0
    skipped = 0
    failures = []

    for gz_file in sorted(manifest.keys()):
        info = manifest[gz_file]
        gz_path = CORPUS_DIR / gz_file
        if not gz_path.exists():
            print(f"[skip] {gz_file} (file not found)")
            skipped += 1
            continue

        expected = info["sha256"]
        ref_size = info.get("raw_size", "?")
        print(f"[test] {gz_file} ...", end=" ", flush=True)

        try:
            ok, got_size, msg = verify_file(gz_path, binary, expected)
        except Exception as e:
            print(f"ERROR {e}")
            failed += 1
            failures.append((gz_file, str(e)))
            raise
            continue

        if ok:
            size_match = ""
            if ref_size != "?" and got_size != ref_size:
                size_match = f" SIZE MISMATCH (ref={ref_size:,}, got={got_size:,})"
            print(f"PASS  {got_size:>14,} bytes{size_match}")
            passed += 1
        else:
            print(f"FAIL  {msg}")
            failed += 1
            failures.append((gz_file, msg))

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} PASS, {failed} FAIL, {skipped} SKIP")

    if failures:
        print(f"\nFailed:")
        for f, reason in failures:
            print(f"  {f}: {reason}")
        sys.exit(1)
    else:
        print("All checks passed!")


if __name__ == "__main__":
    main()
