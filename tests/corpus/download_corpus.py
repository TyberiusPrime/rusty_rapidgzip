#!/usr/bin/env python3
"""Download/generate the rapidgzip-rs test corpus.

Usage:
    python download_corpus.py                              # download everything
    python download_corpus.py --list                       # list datasets
    python download_corpus.py --category synthetic         # only synthetic
    python download_corpus.py --id synth-empty synth-zeros-1m  # specific IDs
    python download_corpus.py --skip-large 50000000        # skip files > 50MB raw
    python download_corpus.py --force                      # re-download existing
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

CORPUS_DIR = Path(__file__).resolve().parent
CORPUS_JSON = CORPUS_DIR / "corpus.json"


def load_corpus():
    with open(CORPUS_JSON) as f:
        return json.load(f)["datasets"]


def filter_datasets(datasets, args):
    result = datasets
    if args.category:
        cats = set(args.category)
        result = [d for d in result if d["category"] in cats]
    if args.ids:
        id_set = set(args.ids)
        result = [d for d in result if d["id"] in id_set]
    if args.skip_large:
        result = [
            d for d in result if (d.get("raw_size_approx") or 0) <= args.skip_large
        ]
    return result


def list_datasets(datasets):
    print(
        f"  {'ID':<35} {'Raw size':>14}  {'Category':<18} {'Importance':<10} Name"
    )
    print("  " + "-" * 110)
    for d in datasets:
        size = d.get("raw_size_approx")
        size_str = f"{size:>14,}" if size is not None else "       unknown"
        print(
            f"  {d['id']:<35} {size_str}  {d['category']:<18} {d['importance']:<10} {d['name']}"
        )
    total_raw = sum(d.get("raw_size_approx") or 0 for d in datasets)
    print(f"\n  Total: {len(datasets)} datasets, ~{total_raw:,} bytes raw")


def download_file(url, dest, label=""):
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tag = label or dest.name
    print(f"  [{tag}] Downloading {url} ...")
    try:
        urllib.request.urlretrieve(url, str(dest))
    except Exception as e:
        if dest.exists():
            dest.unlink()
        raise RuntimeError(f"Download failed: {e}") from e
    size = dest.stat().st_size
    print(f"  [{tag}] Done ({size:,} bytes)")


class ArchiveCache:
    def __init__(self):
        self._cache_dir = tempfile.mkdtemp(prefix="rapidgzip_corpus_")
        self._paths = {}

    def get(self, url):
        if url not in self._paths:
            fname = hashlib.sha256(url.encode()).hexdigest()[:16]
            path = Path(self._cache_dir) / fname
            if not path.exists():
                download_file(url, path, label=f"archive:{fname}")
            self._paths[url] = path
        return self._paths[url]

    def cleanup(self):
        shutil.rmtree(self._cache_dir, ignore_errors=True)


def extract_member(archive_path, member_path, dest_dir, archive_format=None):
    archive_path = Path(archive_path)
    basename = Path(member_path).name
    is_zip = (
        archive_format == "zip"
        or archive_path.suffix == ".zip"
        or archive_path.name.endswith(".zip")
    )

    if is_zip:
        with zipfile.ZipFile(archive_path) as zf:
            names = zf.namelist()
            target = member_path
            if target not in names:
                candidates = [
                    n
                    for n in names
                    if n.endswith("/" + member_path)
                    or n.endswith("/" + basename)
                    or n == basename
                ]
                if not candidates:
                    raise RuntimeError(
                        f"Member '{member_path}' not found in archive. "
                        f"Available: {names[:20]}"
                    )
                target = candidates[0]
            data = zf.read(target)
            out_path = Path(dest_dir) / basename
            out_path.write_bytes(data)
            return out_path
    else:
        with tarfile.open(archive_path) as tf:
            try:
                member = tf.getmember(member_path)
            except KeyError:
                members = tf.getnames()
                candidates = [
                    m
                    for m in members
                    if m.endswith("/" + member_path)
                    or m.endswith("/" + basename)
                    or m == basename
                ]
                if not candidates:
                    raise RuntimeError(
                        f"Member '{member_path}' not found in archive. "
                        f"Available: {members[:20]}"
                    )
                member = tf.getmember(candidates[0])
            out_path = tf.extract(member, dest_dir)
            extracted = Path(dest_dir) / member.name
            if extracted.exists() and extracted.is_file():
                return extracted
            for root, dirs, files in os.walk(dest_dir):
                for f in files:
                    if f == basename:
                        return Path(root) / f
            raise RuntimeError("Extracted file not found after extraction")


def gzip_file(input_path, output_path):
    result = subprocess.run(
        ["gzip", "-n", "-c", str(input_path)],
        stdout=open(str(output_path), "wb"),
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gzip failed: {result.stderr.decode()}")


def acquire_dataset(dataset, archive_cache, force=False):
    gz_file = CORPUS_DIR / dataset["gz_file"]
    if gz_file.exists() and not force:
        return "skip"

    method = dataset["acquisition"]["method"]
    gz_file.parent.mkdir(parents=True, exist_ok=True)

    if method == "generate":
        cmd = dataset["acquisition"]["command"].replace("{gz_file}", str(gz_file))
        result = subprocess.run(
            cmd, shell=True, cwd=str(CORPUS_DIR), capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Generate command failed: {result.stderr}")

    elif method == "download_gz":
        download_file(dataset["acquisition"]["url"], gz_file)

    elif method == "archive_member":
        archive_url = dataset["acquisition"]["archive_url"]
        archive_path = archive_cache.get(archive_url)
        with tempfile.TemporaryDirectory() as tmpdir:
            extracted = extract_member(
                archive_path,
                dataset["acquisition"]["member_path"],
                tmpdir,
                archive_format=dataset["acquisition"].get("archive_format"),
            )
            gzip_file(extracted, gz_file)

    elif method == "download_and_gzip":
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_name = dataset["gz_file"]
            if raw_name.endswith(".gz"):
                raw_name = raw_name[:-3]
            raw_path = Path(tmpdir) / raw_name
            download_file(dataset["acquisition"]["url"], raw_path)
            gzip_file(raw_path, gz_file)

    elif method == "local_copy":
        src = (CORPUS_DIR / dataset["acquisition"]["path"]).resolve()
        if not src.exists():
            raise RuntimeError(f"Local file not found: {src}")
        shutil.copy2(str(src), str(gz_file))

    else:
        raise RuntimeError(f"Unknown acquisition method: {method}")

    if not gz_file.exists():
        raise RuntimeError("Output file not created")

    return "ok"


def main():
    parser = argparse.ArgumentParser(
        description="Download/generate the rapidgzip-rs test corpus",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--category",
        nargs="+",
        help="Only download datasets in these categories "
        "(synthetic, canterbury, canterbury-large, silesia, large-text, genomics, kernel, text)",
    )
    parser.add_argument(
        "--id",
        dest="ids",
        nargs="+",
        help="Only download datasets with these IDs",
    )
    parser.add_argument(
        "--skip-large",
        type=int,
        metavar="BYTES",
        help="Skip datasets with raw_size_approx > BYTES",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List datasets without downloading",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file exists",
    )
    args = parser.parse_args()

    datasets = load_corpus()
    datasets = filter_datasets(datasets, args)

    if not datasets:
        print("No datasets matched the filter criteria.")
        sys.exit(1)

    if args.list:
        list_datasets(datasets)
        return

    print(f"Acquiring {len(datasets)} datasets into {CORPUS_DIR}/\n")

    cache = ArchiveCache()
    ok = 0
    fail = 0
    skip = 0
    failed_ids = []

    try:
        for ds in datasets:
            gz_path = CORPUS_DIR / ds["gz_file"]
            if gz_path.exists() and not args.force:
                print(f"[skip] {ds['gz_file']} (already exists)")
                skip += 1
                continue

            print(f"\n--- {ds['id']}: {ds['name']} ---")
            try:
                status = acquire_dataset(ds, cache, force=args.force)
                if status == "ok":
                    size = gz_path.stat().st_size
                    print(f"[  ok] {ds['gz_file']} ({size:,} bytes)")
                    ok += 1
                else:
                    skip += 1
            except Exception as e:
                print(f"[FAIL] {ds['gz_file']}: {e}")
                fail += 1
                failed_ids.append(ds["id"])
                if gz_path.exists():
                    gz_path.unlink()
    finally:
        cache.cleanup()

    print(f"\n{'=' * 60}")
    print(f"Results: {ok} acquired, {skip} skipped (existing), {fail} failed")
    if failed_ids:
        print(f"Failed IDs: {', '.join(failed_ids)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
