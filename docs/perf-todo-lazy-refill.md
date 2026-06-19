# Perf TODO — close the ~30% per-thread decode gap vs C++ rapidgzip

> ## ⚠️ CORRECTION (2026-06-19, asm-vs-asm + benchmark): the premise below was WRONG
>
> The "gap vs C++ rapidgzip" is **not a codegen gap, and not 30%.** Measured P5 wall
> on the FASTQ file, all rapidgzip 0.16.0:
>
> | Decoder | P5 wall | vs ours |
> |---|---|---|
> | C++ **ISA-L assembly** (nixpkg/PyPI rapidgzip, igzip) | 11.7 s | 0.82× (19% faster) |
> | **rust (ours)** | 14.3 s | 1.0 |
> | C++ native `readInternalCompressed` `-O3 -march=native` | 24.9 s | **1.75× slower** |
>
> - **We already BEAT rapidgzip's own C++ decoder by ~1.75×**, even at -O3 -march=native.
> - The fast nixpkg build doesn't run `readInternalCompressed` — its `.so` links **ISA-L**
>   and perf confirms the hot path is ISA-L asm (`decode_huffman_code_block_stateless_04`,
>   `decode_len_dist`, …). The "C++ ≈ 1150 MiB/s/thread" below is **ISA-L throughput**.
> - ISA-L's 19% edge = **BMI2 branchless shifts (`shrx`/`shlx`/`bzhi`)** + **software-pipelined
>   LUT loads** that hide the dependent-load latency our serial loop is gated on. Hand-written
>   asm doing latency-hiding LLVM won't do for our source — not a Rust limitation.
> - Closing it is NOT refill micro-opts (exhausted) and NOT a codegen fix. Real levers:
>   (a) link/call **ISA-L or libdeflate** for the non-speculative u8 phase-2 path; or
>   (b) hand-write a software-pipelined BMI2 decoder in Rust. (a) is far higher-ROI — ask Florian.
>
> Everything below (the lazy-refill investigation) is still valid as a record of a dead lever,
> but read it knowing the headline gap is to assembly, not to "C++".

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

## RESULT (2026-06-19): lazy/conditional refill — fully explored, ALL REGRESS, reverted

Both the simple form AND the deeper C++-style rewrite were A/B'd. Every variant
lost. **The unconditional branch-free refill is optimal for this loop.** Tree
clean at HEAD `70da66b`. Interleaved phase2 MiB/s/thread (rock-stable ±2):

| Variant | phase2 | vs base |
|---|---|---|
| **baseline** — unconditional refill `bits\|=56` every symbol + double-literal | **~890** | — |
| simple lazy — one `if bits < 48` gate, double-literal kept | 809 | −9% |
| **split refill** — `if bits<15` (litlen) + `if bits<33` (L/D tail), no double-literal | 860 | −3.5% |
| split refill `bits<20`/`<33` + guarded double-literal re-added | 848 | −5% |

### THE ACTUAL REASON (disassembly, 2026-06-19): LLVM already does it

Disassembling the **baseline** `decode_compressed` settles it. Our source says
"refill unconditionally every symbol (`bits |= 56`)", but LLVM proved the refill
is a no-op when `bits` is high, hoisted the test, and emitted **exactly the
C++-style split lazy refill by itself**:
- litlen refill gated `cmp $0x13,%r13d; ja` → skip when **bits > 19**;
- L/D-tail refill gated `cmp $0x21,%r13d; jae` → skip when **bits ≥ 33** (0x21 =
  33, identical to the hand-derived `NEEDED_PAIR`);
- double-literal kept and scheduled; near-EOF byte-fill unrolled (cmp 24/16/8).

So every hand-rolled lazy refill regressed because **lazy refill was already
there** — hand-writing it duplicates an optimizer transform while disturbing
register allocation/scheduling (and tempts dropping double-literal). The earlier
"Rust codegen can't match C++" gloss was **wrong**: on the refill, LLVM matches
C++'s hand-written structure. The per-thread gap (≈890 vs ≈1150) is **NOT the
refill** and is currently **undiagnosed** — likely the litlen LUT load on the
per-symbol critical path (`shr buf → and 0x3ff → mov (lut) → shr buf`). The next
step is a side-by-side **disassembly + perf cycle-attribution** vs C++'s
`readInternalCompressed`, not another source rewrite.

### Why even the deeper rewrite lost (mechanism)

The deeper rewrite did exactly what C++ does: **split the refill into two
conditional points** so the hot literal path uses a *low* threshold (15) and
refills only ~1-in-6 symbols, while the rare length/distance path does its own
`bits<33` refill. md5 stayed `4f66df41…` for every variant. It recovered most of
the −9% (to −3.5%) — confirming the simple form's loss really was the too-high
threshold (48 → refill every symbol). But it never reached parity, and adding the
machinery to claw back the lost double-literal made it *worse*, not better:

- Re-adding double-literal needs a **mandatory `bits >= LUT_BITS` guard** that
  the baseline doesn't need: with conditional refill, on a skip-iteration the
  high bits of `buf` above the live `bits` count are **stale** (already consumed),
  so a speculative second LUT lookup can read garbage. The baseline's *always-fresh*
  accumulator (it reloads every symbol) makes that guard unnecessary. That one
  extra guard branch cost more than double-literal saved (848 < 860).
- Root cause, consistent with the diagnosis above: this loop is
  **critical-path-bound through `buf`**, not throughput-bound (IPC 2.8). The
  unconditional `buf |= load << bits` is effectively *free* — it hides under the
  dependency-chain latency. Any refill **branch**, even mostly-not-taken at 1/6,
  adds a data-dependent control dependency whose mispredicts flush the pipeline
  and are *not* free. We trade free work for non-free branches → always a loss.

C++'s lazy refill wins on C++'s codegen/hardware; it does **not** translate to
this Rust loop on Zen5. **The lazy-refill lever is fully exhausted — do not
retry any conditional-refill form** (simple, split, peek-small/read-extra, or
double-literal-guarded). The remaining gap vs C++ is structural+SMT-bound; no
cheap per-thread win is left in the refill cadence.

## (original) THE EXPERIMENT: lazy/conditional bit-buffer refill

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
