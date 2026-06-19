# Perf TODO — close the ~30% per-thread decode gap vs C++ rapidgzip

Handoff from a profiling session (2026-06-19). Tree state: HEAD `70da66b`, clean.
The phase1-per-member fix is already committed; nothing else is pending.

## The goal & the diagnosis (verified, not speculation)

On the FASTQ benchmark we trail C++ rapidgzip. The gap is **per-thread phase-2
decode efficiency**, NOT SMT/scaling (proven: at `-P5`, zero SMT contention, we
already burn +26% user CPU — 71s vs C++ 56.36s).

- Benchmark file: `/project/large_test/every/incoming/260316_A02023_0323_AHKHKNDRX7/ID136786_all_cells_S1_R1_001.fastq.gz`
  (12.7 GB compressed → 59.7 GB, ~57k gzip members ~1 MB each).
- Box: 16-core/32-thread Zen5 (Ryzen AI Max 395).
- The hot fn is `crates/rusty-rapidgzip/src/deflate/fast_inflate.rs::decode_compressed`
  (phase-2 u8 inflate = 93% of decode CPU at P5, **881 MiB/s/thread**; C++ ≈ 1150).
- Counters (P5): IPC **2.8** (not classically stalled), **3.2B branch-misses ≈14% of
  cycles**, **16.3 instructions/output-byte**, L1d miss 1.3% (not memory-bound).
- The loop is **critical-path-bound through the `buf` bit-accumulator**: changes that
  lengthen that chain regress even when they cut branches/instructions.

## THE NEXT EXPERIMENT: lazy/conditional bit-buffer refill

This is the one untried structural difference vs C++, and it was **never A/B'd**
(confirmed by Florian — the current unconditional refill was inherited from
zlib-rs by default, not chosen after measuring on this workload).

- Today (`decode_compressed`, ~line 389): we refill **unconditionally every symbol**
  — `buf |= load_le_u64(...) << bits; advance=(63^bits)>>3; byte_pos+=advance; bits|=56;`
  — ~7 instructions/symbol even when the buffer is already full, and the
  `buf |=` is on the critical path every iteration.
- C++ (`librapidarchive/src/filereader/BitReader.hpp::peek`) refills **only when
  `bitsWanted > bitBufferSize`** (predicted-not-taken branch), peeking ≤11 bits for
  the symbol and reading extra bits separately. Refills ~every 6 symbols, not every 1.
- Hypothesis: lazy refill cuts a big slice of our 16.3 insns/byte AND shortens the
  `buf` chain on the ~5/6 symbols that don't need a refill.
- Risk: may just trade instruction-count for refill-branch mispredicts and net zero.
  That's exactly why we A/B it.

### How to do it
- Put it **behind a `cfg!`/const flag** so both refill modes build, to A/B cleanly.
- Keep the near-EOF slow path intact. Maintain the invariant that enough bits are
  present before each LUT lookup + extra-bits read (worst-case L/D pair = 48 bits).
- Apply to BOTH `decode_compressed` (phase2) and, if it wins, `decode_compressed_u16`.

## How to measure (IMPORTANT — wall/user time is ±4s noisy on this box)

Use the **`-v` phase2 MiB/s/thread** metric — it is rock-stable (±1). Interleave
A/B runs to cancel drift:
```
F=/project/large_test/.../ID136786_all_cells_S1_R1_001.fastq.gz
for b in base variant base variant; do /tmp/rrz_$b -P5 -v "$F" 2>&1 >/dev/null \
  | grep 'phase2 ('; done
```
Build: `cargo build --release -p rusty-rapidgzip-bin --bin rusty-rapidgzip-rs`
(target dir is `target_claude/`; CARGO_TARGET_DIR is set in the env).
Correctness: output md5 must stay `4f66df41db8ded3855cbf243d641adda`
(`cmp` against a known-good binary is faster than md5sum on 59.7 GB).
`perf` works: `perf record -g --call-graph dwarf` on a `--profile profiling` build;
`perf stat -d` for IPC/branch-misses (note: perf prints to stderr).

## DO NOT RETRY — already measured, all regressed (phase2 baseline 881 MiB/s)

- Branchless extra-bits (drop `if len_extra>0`/`if dextra>0`): 850, −3.5% (lengthens buf chain).
- Remove double-literal speculation: 878, −1% (it helps us — keep it).
- `LUT_BITS` 10→11: 857, −2% (cache pressure + 2× LUT build over 56k members).
- `RUSTFLAGS=-C target-cpu=native`: phase2 871 vs 881 — NOT a per-thread win.
  (It does cut full-thread wall ~0.5s via memory bandwidth, but costs +12s user CPU.)
- Pipeline knobs `RAPIDGZIP_INFLIGHT` / `RAPIDGZIP_RESOLVE_THREADS`: defaults already optimal.

## C++ reference (source checked out in-tree)

- `librapidarchive/src/rapidgzip/gzip/deflate.hpp`: `readInternalCompressed` (~L1514,
  the simple per-symbol loop), `getDistance` (~L1162), `resolveBackreference` (~L1349,
  uses `std::memcpy`/`memset`). Default litlen = `HuffmanCodingShortBitsCached` LUT_BITS=11.
- `librapidarchive/src/huffman/HuffmanCodingShortBitsCached.hpp`: their `decode()`.
- `librapidarchive/src/filereader/BitReader.hpp`: `peek`/`read`/`seekAfterPeek` (lazy refill).
- deflate.hpp ~L170: their note that big LUTs (DoubleLiteralCached) are slower
  multi-thread because the table thrashes shared L1 when two HW threads share a core.

Our advantages to keep: richer u32 LUT entry (extra-bits packed), double-literal,
LUT_BITS=10. The gap is the refill cadence + inherent literal/length branch mispredicts.
