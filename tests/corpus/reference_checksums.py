#!/usr/bin/env python3
"""Compute SHA256 checksums of decompressed outputs using system gzip.

Reads corpus.json, decompresses each .gz file with `gzip -cd`, computes
the SHA256 of the decompressed bytes, and writes the results to
reference_sums.json in the corpus directory.

Usage:
    python reference_checksums.py                # compute for all .gz files
    python reference_checksums.py --id synth-empty   # specific datasets
    python reference_checksums.py --force            # recompute existing entries
"""

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

CORPUS_DIR = Path(__file__).resolve().parent
CORPUS_JSON = CORPUS_DIR / "corpus.json"
MANIFEST = CORPUS_DIR / "reference_sums.json"


def compute_sha256(gz_path):
    """Decompress with gzip -cd, stream through SHA256 (memory-efficient)."""
    proc = subprocess.Popen(
        ["gzip", "-cd", str(gz_path)],
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
        raise RuntimeError(f"gzip -cd failed (exit {proc.returncode}): {stderr}")
    return sha.hexdigest(), total


def main():
    parser = argparse.ArgumentParser(
        description="Compute reference SHA256 checksums using system gzip"
    )
    parser.add_argument(
        "--id",
        dest="ids",
        nargs="+",
        help="Only process these dataset IDs",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute entries that already exist in the manifest",
    )
    args = parser.parse_args()

    with open(CORPUS_JSON) as f:
        datasets = json.load(f)["datasets"]

    if args.ids:
        id_set = set(args.ids)
        datasets = [d for d in datasets if d["id"] in id_set]

    manifest = {}
    if MANIFEST.exists():
        with open(MANIFEST) as f:
            manifest = json.load(f)

    ok = 0
    skip = 0
    fail = 0

    for ds in datasets:
        gz_path = CORPUS_DIR / ds["gz_file"]
        if not gz_path.exists():
            print(f"[skip] {ds['gz_file']} (file not found)")
            skip += 1
            continue

        if ds["gz_file"] in manifest and not args.force:
            print(f"[skip] {ds['gz_file']} (already in manifest)")
            skip += 1
            continue

        print(f"[calc] {ds['gz_file']} ...", end=" ", flush=True)
        try:
            sha256, raw_size = compute_sha256(gz_path)
            manifest[ds["gz_file"]] = {
                "sha256": sha256,
                "raw_size": raw_size,
                "gz_size": gz_path.stat().st_size,
            }
            print(f"OK  {raw_size:>14,} bytes  sha256:{sha256[:24]}...")
            ok += 1
        except Exception as e:
            print(f"FAILED: {e}")
            fail += 1

    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"\nManifest: {MANIFEST}")
    print(f"Computed: {ok}, Skipped: {skip}, Failed: {fail}, Total entries: {len(manifest)}")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
