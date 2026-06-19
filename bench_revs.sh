#!/usr/bin/env bash
# Sweep decode throughput across a jj/git revision range, in an ISOLATED worktree
# (your main checkout is never touched).
#
# For every revision in REVSET (oldest first) it:
#   1. checks the rev out into a detached git worktree
#   2. builds the rapidgzip CLI            -- NOT timed; full log per rev
#   3. runs <bin> -P N FILE >/dev/null     -- timed (user/sys/real)
# Results append to OUT (CSV); build logs land in LOGDIR/build_<change>_<commit>.log.
#
# Usage:
#   ./bench_revs.sh '<jj revset range>' [threads] [iters] [out.csv]
# e.g.
#   ./bench_revs.sh 'b3f0860..main' 5 2 /tmp/rrz_sweep.csv
#
# Env overrides: REPO, FILE, LOGDIR, BENCH_TARGET_DIR.

set -uo pipefail
export LC_ALL=C   # '.' decimals in `time` output + awk comparisons

REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
FILE="${FILE:-$HOME/upstream/fastqrab/large_test/every/incoming/260316_A02023_0323_AHKHKNDRX7/ID136786_all_cells_S1_R1_001.fastq.gz}"

REVSET="${1:?usage: bench_revs.sh '<jj revset range, e.g. X..Y>' [threads] [iters] [out.csv]}"
THREADS="${2:-5}"
ITERS="${3:-2}"                 # timed runs per rev; keep the fastest (min user)
OUT="${4:-/tmp/rrz_sweep.csv}"
LOGDIR="${LOGDIR:-/tmp/rrz_logs}"

# Shared, EXTERNAL target dir: keeps the worktree pristine (clean `git checkout`)
# and reuses build cache across revisions.
BASEDIR=$(mktemp -d /tmp/rrz_bench.XXXXXX)
WT="$BASEDIR/wt"
# Dedicated ABSOLUTE target dir. We deliberately ignore any inherited
# CARGO_TARGET_DIR (it may be relative, which would resolve against cargo's cwd
# and break the binary-path lookup). Override via BENCH_TARGET_DIR if you want to
# persist the cache across runs.
_tgt="${BENCH_TARGET_DIR:-$BASEDIR/target}"
case "$_tgt" in /*) ;; *) _tgt="$PWD/$_tgt" ;; esac
export CARGO_TARGET_DIR="$_tgt"

# --- sanity ---
[ -e "$REPO/.git" ] || { echo "REPO is not a git repo: $REPO" >&2; exit 1; }
[ -r "$REPO/Cargo.toml" ] || { echo "no Cargo.toml at REPO: $REPO" >&2; exit 1; }
[ -r "$FILE" ] || { echo "test file not readable: $FILE" >&2; exit 1; }
command -v cargo >/dev/null || { echo "cargo not on PATH" >&2; exit 1; }
mkdir -p "$LOGDIR"

echo "repo=$REPO"
echo "file=$FILE"
echo "worktree=$WT  target=$CARGO_TARGET_DIR  logs=$LOGDIR"
echo "revset=$REVSET threads=$THREADS iters=$ITERS -> $OUT"; echo

cleanup() {
  git -C "$REPO" worktree remove --force "$WT" >/dev/null 2>&1
  rm -rf "$WT"
}
trap cleanup EXIT

echo "change_id,commit_id,build_ok,user_s,sys_s,real_s,desc" > "$OUT"

# warm page cache once (same file every rev)
cat "$FILE" > /dev/null 2>&1 || true

# print "<pkg> <bin>" for the rapidgzip CLI target of the worktree's tree
detect_bin() {  # $1 = dir
  ( cd "$1" && python3 - <<'PY' 2>/dev/null
import json, subprocess
try:
    m = json.loads(subprocess.check_output(
        ["cargo","metadata","--no-deps","--format-version","1"],
        stderr=subprocess.DEVNULL))
except Exception:
    raise SystemExit
c = [(p["name"], t["name"]) for p in m["packages"] for t in p["targets"]
     if "bin" in t["kind"] and "rapidgzip" in t["name"]]
c.sort(key=lambda x: (not x[1].endswith("rapidgzip-rs"), len(x[1])))
if c: print(c[0][0], c[0][1])
PY
  )
}

# enumerate revisions oldest-first: change_id <tab> commit_id <tab> desc
mapfile -t REVS < <(jj --no-pager -R "$REPO" log --no-graph --reversed -r "$REVSET" \
  -T 'change_id.short() ++ "\t" ++ commit_id.short() ++ "\t" ++ description.first_line() ++ "\n"' 2>/dev/null)
[ "${#REVS[@]}" -gt 0 ] || { echo "no revisions matched '$REVSET'" >&2; exit 1; }
echo "sweeping ${#REVS[@]} revision(s)"; echo

# create the worktree once (at the first commit), then reuse via checkout
first_gid=$(cut -f2 <<<"${REVS[0]}")
git -C "$REPO" worktree add --detach "$WT" "$first_gid" >/dev/null 2>&1 \
  || { echo "could not create worktree at $first_gid" >&2; exit 1; }

for line in "${REVS[@]}"; do
  cid=$(cut -f1 <<<"$line"); gid=$(cut -f2 <<<"$line")
  desc=$(cut -f3- <<<"$line" | tr ',\n' '; ')
  [ -n "$gid" ] || continue
  log="$LOGDIR/build_${cid}_${gid}.log"
  printf '>>> %s %s  %s\n' "$cid" "$gid" "$desc"

  if ! git -C "$WT" checkout --detach -f "$gid" >"$log" 2>&1; then
    echo "    checkout FAILED -> $log"; sed -n '1,4p' "$log" | sed 's/^/      /'
    echo "$cid,$gid,0,,,,$desc" >> "$OUT"; continue
  fi

  # ---- build (UNTIMED), full log per rev ----
  read -r PKG BIN < <(detect_bin "$WT")
  build_ok=0
  if [ -n "${BIN:-}" ]; then
    ( cd "$WT" && cargo build --release -p "$PKG" --bin "$BIN" ) >>"$log" 2>&1 && build_ok=1
  else
    ( cd "$WT" && cargo build --release --bins ) >>"$log" 2>&1 \
      && BIN=$(find "$CARGO_TARGET_DIR/release" -maxdepth 1 -type f -executable -name '*rapidgzip-rs' 2>/dev/null | head -1 | xargs -r basename) \
      && [ -n "${BIN:-}" ] && build_ok=1
  fi

  binpath="$CARGO_TARGET_DIR/release/${BIN:-__none__}"
  if [ "$build_ok" -ne 1 ] || [ ! -x "$binpath" ]; then
    echo "    build FAILED -> $log"; tail -n 5 "$log" | sed 's/^/      /'
    echo "$cid,$gid,0,,,,$desc" >> "$OUT"; continue
  fi

  # ---- time ONLY the decode; keep the fastest of ITERS runs ----
  best_u="" best_s="" best_r=""
  TIMEFORMAT='%U %S %R'
  for ((i=1; i<=ITERS; i++)); do
    tf=$(mktemp)
    { time "$binpath" -P "$THREADS" "$FILE" >/dev/null 2>/dev/null ; } 2>"$tf"
    read -r u s r < "$tf"; rm -f "$tf"
    [ -n "$u" ] || continue
    if [ -z "$best_u" ] || awk "BEGIN{exit !($u < $best_u)}"; then
      best_u=$u best_s=$s best_r=$r
    fi
  done

  printf '    user=%ss sys=%ss real=%ss  (bin=%s)\n' "$best_u" "$best_s" "$best_r" "$BIN"
  echo "$cid,$gid,1,$best_u,$best_s,$best_r,$desc" >> "$OUT"
done

echo; echo "done -> $OUT"
column -t -s, "$OUT" 2>/dev/null || cat "$OUT"
