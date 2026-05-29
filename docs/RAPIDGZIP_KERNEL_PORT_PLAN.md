# Porting rapidgzip's inflate kernel to Rust

A step-by-step plan to replace the zlib-rs-derived inflate kernel in
`rusty-rapidgzip-deflate` with a port of rapidgzip (C++)'s custom kernel.
Target: match or beat C++ rapidgzip per-thread throughput in safe Rust
(with `unsafe` confined to a tiny, audited bit-buffer kernel).

The C++ source we are porting from lives under `/project/rapidgzip_cpp`.
The Rust target lives under `/project/crates/rusty-rapidgzip-deflate`
(plus its consumer crate `rusty-rapidgzip-inflate`, which holds the
existing speculative-decode hook surface).

## Why this port, and what we keep

We already have:

- A pure-safe `safe_inflate.rs` (puff.c-derived) — useful as a correctness
  oracle and as the fallback path on platforms we don't tune for.
- A vendored, patched zlib-rs path (`rusty-rapidgzip-inflate::inflate`)
  with a marker hook for speculative decode. This is the **kernel we are
  replacing** — it carries lots of upstream `unsafe` and still trails
  C++ rapidgzip on per-thread throughput.
- A working parallel pipeline (`rusty-rapidgzip::pipeline`) with
  - CRC fan-out off the serializer critical path,
  - recycle-ownership held by the slowest Arc consumer (the CRC thread),
  - live thread-count autotune.
  We are **not** touching any of this. The C++ rapidgzip orchestration
  layer (`ParallelGzipReader.hpp`, `GzipChunkFetcher.hpp`,
  `ChunkData.hpp`, `WindowMap.hpp`) is **not** part of this port — we
  have a better Rust version already.

What we add:

- A new in-tree kernel (working name: `fast_inflate.rs`) that mirrors
  rapidgzip's `deflate::Block` decode loop with the same Huffman LUT
  strategy and the same wide bit-buffer.
- A safe BitReader that matches the C++ one's refill discipline and
  fast path, while keeping `unsafe` scoped to one unaligned 8-byte
  load.
- Two Huffman LUTs: the standard reversed-bits cached decoder (for
  distances) and the bounded-LUT "short bits cached" decoder (for
  literals/lengths), with `LUT_BITS = 11` as the default (best on the
  FASTQ benchmark in rapidgzip's published timings).
- A runtime toggle to use either the zlib-rs based or new 'fast_inflate'.

## What we do NOT port

| C++ file | Why skipped |
| --- | --- |
| `chunkdecoding/GzipChunk.hpp` | Pipeline orchestration — we have a better Rust version. |
| `MarkerReplacement.hpp` | We already have `speculative::resolve_markers` in `rusty-rapidgzip-deflate/src/speculative.rs`. |
| `gzip/gzip.hpp`, `GzipReader.hpp`, `ParallelGzipReader.hpp`, `GzipChunkFetcher.hpp`, `WindowMap.hpp`, `IndexFileFormat.hpp` | Reader / framing / index — out of scope. |
| `gzip/crc32.hpp` | We use `crc32fast` (CRC fan-out lives in our pipeline). |
| `gzip/zlib.hpp`, `gzip/isal.hpp`, `huffman/HuffmanCodingISAL.hpp`, `huffman/HuffmanCodingDistanceISAL.hpp` | ISA-L bindings. Not the safe-Rust path. We may revisit `std::arch::x86_64` intrinsics in a future iteration; out of scope here. |
| `huffman/HuffmanCodingDoubleLiteralCached.hpp`, `HuffmanCodingShortBitsMultiCached.hpp`, `HuffmanCodingShortBitsCachedDeflate.hpp` | Alternative literal decoders. According to the in-tree benchmark in `deflate.hpp:79–115`, `HuffmanCodingShortBitsCached<LUT=11>` matches or beats them on FASTQ. We port only the winner. |
| `huffman/HuffmanCodingLinearSearch.hpp`, `HuffmanCodingSymbolsPerLength.hpp` (as standalone) | We need `SymbolsPerLength` only as the *base class layer* (min/max code-length plumbing); we fold it into the cached decoders. |
| `huffman/HuffmanCodingReversedBitsCachedCompressed.hpp` | Pre-code decoder used only by rapidgzip's block-finder. Our block finder is separate (`block_finder.rs`); a smaller LUT for the pre-code is fine. |
| `blockfinder/`, `GzipBlockFinder.hpp` | We have `block_finder.rs`. |
| `gzip/GzipAnalyzer.hpp`, `tests/`, `benchmarks/`, `tools/`, `examples/` | Tooling. |

If a benchmark later shows the `DoubleLiteralCached` variant winning on
some specific corpus we care about, it's a drop-in addition — the
decode loop is parameterised over the literal decoder.

## Source map (C++ → Rust)

| C++ file | Rust target | Notes |
| --- | --- | --- |
| `librapidarchive/src/filereader/BitReader.hpp` (LSB path only) | `rusty-rapidgzip-deflate/src/bitreader.rs` (rewrite) | Drop MSB-first templating, drop FileReader plumbing. Keep `read<N>`, `peek(N)`, `seekAfterPeek(N)`, `read(N)`, `refillBitBuffer`. |
| `src/rapidgzip/gzip/definitions.hpp` | constants in `rusty-rapidgzip-deflate/src/tables.rs` | Already partially present. |
| `src/rapidgzip/gzip/RFCTables.hpp` | `tables.rs` (extend) | Static distance & length LUTs + `get_length`, `get_distance` helpers. |
| `src/rapidgzip/gzip/precode.hpp` | not needed for the kernel | (Block-finder file, optional for separate work.) |
| `src/huffman/HuffmanCodingBase.hpp` | private impl detail in `huffman.rs` | `min/max code length`, `initialize_min_max`, `check_code_length_frequencies` (Kraft), `initialize_minimum_code_values`. |
| `src/huffman/HuffmanCodingSymbolsPerLength.hpp` | fold into `huffman.rs` | The "fallback walk" path for codes longer than `LUT_BITS`. |
| `src/rapidgzip/huffman/HuffmanCodingReversedBitsCached.hpp` | `huffman::ReversedBitsCached` in `huffman.rs` | Full-MAX_CODE_LENGTH LUT. Used for the distance decoder and the fixed-Huffman literal decoder. |
| `src/huffman/HuffmanCodingShortBitsCached.hpp` | `huffman::ShortBitsCached` in `huffman.rs` | Bounded LUT_BITS LUT, with fall-back walk. Used for the dynamic-Huffman literal/length decoder. |
| `src/rapidgzip/huffman/HuffmanCodingReversedBitsCachedCompressed.hpp` | `huffman::ReversedBitsCachedSmall` (for pre-code) | Small-alphabet variant; 7-bit LUT for the 19-symbol pre-code. |
| `src/rapidgzip/gzip/deflate.hpp` (the main `Block`/`readInternal*` loop) | `rusty-rapidgzip-deflate/src/fast_inflate.rs` (new) | The kernel. |
| `src/rapidgzip/MarkerReplacement.hpp` | already covered by `speculative.rs` | no port. |

Approximate line budget (post-port):

- `bitreader.rs`: ~300 lines (down from ~995 because we drop the MSB path and the FileReader plumbing).
- `huffman.rs`: ~600 lines (the three coding variants + shared base).
- `tables.rs`: ~150 lines.
- `fast_inflate.rs`: ~1200 lines (down from `deflate.hpp`'s 2005 because we drop statistics, ENABLE_STATISTICS template plumbing, MSC workarounds, and the marker-buffer logic which lives in `speculative.rs`).

Total ~2300 lines of net-new Rust. Plus removals: ~3000 lines of zlib-rs
become deletable once `fast_inflate` is the only kernel.

## Phasing

Each phase ends with a passing test suite. The phases are ordered so
that we can ship after any of them and still have a working build.

### Phase 0 — Baseline measurement (1 day)

Before changing anything:

1. Snapshot the current per-thread throughput on
   `test.fastq.gz` (the benchmark from the existing memory notes):
   `-P1 --zlib-rs > /dev/null`. Record wall, user, sys.
2. Snapshot C++ rapidgzip throughput on the same file, same machine,
   `-P 1`. Build with `LIBRAPIDARCHIVE_WITH_ISAL=OFF` (we are not
   competing with ISA-L; we want the pure-C++ baseline).
3. Snapshot `safe_inflate.rs` throughput on the same file (`-P1` with
   the safe path). This is our floor — `fast_inflate` must beat it.
4. Write all three to `/project/docs/RAPIDGZIP_KERNEL_PORT_BASELINE.md`.

This baseline pins down the win criterion. The plan is "fast_inflate
single-thread throughput ≥ C++ rapidgzip single-thread (no ISA-L) on
fastq within 5%."

### Phase 1 — BitReader (2–3 days)

Port `librapidarchive/src/filereader/BitReader.hpp` to
`rusty-rapidgzip-deflate/src/bitreader.rs`. Specifically:

1. **Strip to LSB-only.** We only ever decode deflate, never bzip2.
   Remove the `MOST_SIGNIFICANT_BITS_FIRST` template axis entirely.
2. **In-memory input only.** Replace the `UniqueFileReader` /
   `m_inputBuffer` / `refillBuffer()` indirection with a plain
   `&'a [u8]` + `pos: usize`. The pipeline already chops input into
   chunks; this kernel never sees a file.
3. **Keep the 64-bit bit buffer**. `m_bitBuffer: u64`,
   `m_bitBufferFree: u8` (free-bits count from the top). The C++
   comment at `definitions.hpp:16` says 64-bit gave +10% over 32-bit.
4. **Public surface**:
   - `read::<const N: u8>() -> u64` — `const N` for the hot path,
     monomorphised per call site (matches `bitReader.read<5>()` etc.).
   - `read(n: u8) -> u64` — runtime-`n` for variable extra-bits.
   - `peek(n: u8) -> u64` and `seek_after_peek(n: u8)` — these are the
     two halves of the Huffman LUT path. They MUST inline.
   - `bytes(buf: &mut [u8])` — for uncompressed blocks (byte-aligned
     fast copy).
   - `align_to_byte()`.
   - `tell_bit() -> u64`.
5. **Refill fast path** (the `read2`/`fillBitBuffer` from C++): if at
   least 8 bytes are left in input, do **one** unaligned little-endian
   `u64` load and treat that as the new bit buffer. This is the only
   `unsafe` block in the bit reader; gate it with
   `#[allow(unsafe_code)]` and an `// SAFETY:` comment that pins the
   precondition `pos + 8 <= input.len()`.
6. **Slow path**: byte-by-byte refill until either the buffer is full
   or input is exhausted. Mirror `fillBitBuffer`. On exhaustion,
   return `Err(DeflateError::UnexpectedEof)` — we do NOT use the C++
   exception-as-control-flow trick (Rust panics are not a
   zero-cost branch).
7. **Inlining**: every method on the hot path gets
   `#[inline(always)]`. The C++ source repeatedly warns
   (`librapidarchive/src/filereader/BitReader.hpp:188–192`) that
   missed inlining costs 30%. Run `cargo asm` (or `objdump -d`) on
   the inflate loop after the port to confirm `peek`/`seek_after_peek`
   are inlined; if not, escalate to a macro.

**Tests for Phase 1**: differential-test against the existing
`rusty-rapidgzip-deflate::BitReader` on the corpus
`tests/data/*.gz` (already in tree) — every read sequence must agree
bit-for-bit. The existing puff-derived `safe_inflate` is also a useful
oracle: feed both bit readers the same deflate stream and assert the
sequence of `(bits, n)` reads matches.

### Phase 2 — Huffman decoders (3–4 days)

Rewrite `rusty-rapidgzip-deflate/src/huffman.rs` so that it exposes
three concrete decoders, all built from one shared base.

1. **Shared base** (private):
   - `MinMax { min_len: u8, max_len: u8 }`.
   - `initialize_min_max(code_lengths: &[u8]) -> Result<MinMax>`.
   - `check_kraft(freqs: &[u16; 16], n: usize) -> Result<()>`
     (rapidgzip's `checkCodeLengthFrequencies` — both Kraft and the
     "no bloating" optimality check from
     `HuffmanCodingBase.hpp:104–109`). The optimality check is what
     lets us bail on malformed streams cheaply; keep it.
   - `initialize_minimum_code_values(freqs) -> [u16; 16]`
     (`HuffmanCodingBase.hpp:117–149`).
   - `SymbolsPerLength { offsets: [u16; 17], symbols: [Symbol; MAX] }`
     and an `initialize_symbols_per_length` helper — these underpin
     the fall-back walk for codes longer than `LUT_BITS`.

2. **`ReversedBitsCached<Symbol, const MAX_SYMS: usize>`**: the
   straight port of `HuffmanCodingReversedBitsCached.hpp`. Full
   `1 << MAX_CODE_LENGTH = 32768` LUT, each entry
   `(length: u8, symbol: Symbol)`. Build cost is ~30 µs; tolerable
   per block. Used for:
   - the distance decoder (`Symbol = u8`, `MAX_SYMS = 30`),
   - the static fixed-Huffman literal decoder
     (`Symbol = u16`, `MAX_SYMS = 288`), built once and reused.

3. **`ShortBitsCached<Symbol, const MAX_SYMS: usize, const LUT_BITS: u8>`**:
   port of `HuffmanCodingShortBitsCached.hpp`. Bounded `1 << LUT_BITS`
   LUT (8 KiB for `LUT_BITS = 11`, `Symbol = u16`). The decode path:
   - `let (len, sym) = lut[reader.peek(lut_bits)]; if len != 0 {
     reader.seek_after_peek(len); return sym; } else { decode_long() }`
   - `decode_long`: walk the canonical Huffman tables (the
     `SymbolsPerLength` arrays). Called only for codes longer than
     `LUT_BITS`, which is rare.
   - Default `LUT_BITS = 11`. Reason: see the bench numbers at
     `deflate.hpp:79–115`; on FASTQ both `LUT=10` and `LUT=11` are
     within noise, `LUT=11` is the rapidgzip default, and 8 KiB still
     fits comfortably in L1 alongside the distance LUT.

4. **`ReversedBitsCachedSmall<Symbol, const MAX_SYMS: usize, const LUT_BITS: u8>`**
   for the pre-code (19 symbols, max length 7). LUT_BITS = 7. Used in
   `read_dynamic_header`. Cheap to build, decode is a single table
   lookup.

5. **Build-cost notes**: the C++ source flags Huffman build cost as
   non-trivial (`deflate.hpp:115`). Don't try to be clever with
   constexpr-in-Rust — keep the build path straightforward,
   `#[inline(never)]` on the build functions, focus inlining on
   `decode`.

**Tests for Phase 2**: differential-test every decoder against the
current `huffman.rs` decoders on every block of a few corpus files.
For each (block header → code lengths) pair, build both, then
decode every reachable code value 0..(1 << max_len) and assert the
decoder returns the same symbol and consumes the same number of bits.

### Phase 3 — RFC tables and dynamic-header reader (1–2 days)

In `tables.rs`:

- Static `DISTANCE_BASE: [u16; 30]` (rapidgzip's `distanceLUT`).
- Static `LENGTH_BASE: [u16; 29]` and `LENGTH_EXTRA: [u8; 29]`
  (rapidgzip's `lengthLUT` plus the trivial extra-bits-count formula
  inlined to a table — Rust const fn evaluator handles it).
- `fn get_length(code: u16, reader: &mut BitReader) -> u16`
  (rapidgzip's `getLength`, RFCTables.hpp:97). Inline.
- `fn get_distance(code: u16, reader: &mut BitReader) -> u16` —
  only the **base** + extra-bits portion. Decoding the *symbol* lives
  in the kernel because it depends on the block's distance HC.

In `fast_inflate.rs::read_dynamic_header`:

- Mirror `Block::readDynamicHuffmanCoding` (`deflate.hpp:1027–1156`).
- Reuse the existing `read_dynamic_header` skeleton from
  `inflate.rs` for buffer layouts; replace its Huffman builders with
  the new `ShortBitsCached` / `ReversedBitsCached` / pre-code builder.
- Keep the unrolled fill-with-zero / repeat-last codes loop from
  `deflate.hpp:411–442` (the `code == 16/17/18` branches with the
  unrolled writes past the end into the safety buffer). The 256-byte
  tail buffer (`LiteralAndDistanceCLBuffer`,
  `deflate.hpp:341`) is what makes branchless overshoot safe.
- **Critical**: check `m_literalCL[END_OF_BLOCK_SYMBOL] != 0`
  (`deflate.hpp:1110`). Without this, malformed streams that omit
  EOB will spin until OOM.

**Tests for Phase 3**: round-trip — read a block header with both the
new code and the existing code, assert the resulting code-length
arrays are byte-for-byte identical and the BitReader state advances
the same number of bits.

### Phase 4 — Decode loop and back-reference resolver (4–6 days)

This is the hot path. In `fast_inflate.rs`:

1. **`struct Block`** holds:
   - the current compression type (`Uncompressed`/`Fixed`/`Dynamic`),
   - the literal HC (`ShortBitsCached<u16, 286, 11>`),
   - the distance HC (`ReversedBitsCached<u8, 30>`),
   - a flag for "at end of block",
   - `decoded_bytes: u64`.
   Note that we do **not** carry rapidgzip's circular `m_window16`
   marker-half-buffer; the marker side-table in `speculative.rs`
   replaces it. Output goes directly into a caller-owned `&mut Vec<u8>`.

2. **`read_header`** ≈ `Block::readHeader` (`deflate.hpp:967`).

3. **Decode dispatch** — three call sites, each a different decode
   loop:
   - `decode_uncompressed`: byte-align, read 16-bit length and its
     one's complement, sanity check, then `BitReader::bytes`. Mirror
     `Block::readInternalUncompressed` (`deflate.hpp:1470`), minus
     the marker logic. Optimised path: large uncompressed blocks
     (≥32 KiB) are not seen in typical fastq but cost us nothing to
     support.
   - `decode_fixed`: identical loop to `decode_dynamic`, with a
     static fixed-Huffman literal decoder and a hardcoded distance
     decoder (5-bit reversed read).
   - `decode_dynamic`: the main loop.

4. **The main loop** mirrors `Block::readInternalCompressed`
   (`deflate.hpp:1514–1582`). Pseudocode:
   ```text
   loop {
       if remaining_output_quota == 0 { return; }
       let sym = literal_hc.decode(reader)?;        // hot
       if sym < 256 {
           push_literal(out, sym as u8);
           continue;
       }
       if sym == 256 { return EndOfBlock; }
       if sym > 285 { return Err(InvalidSymbol); }
       let length = get_length(sym, reader);
       let dist_sym = distance_hc.decode(reader)?;  // hot
       let distance = decode_distance_extra(dist_sym, reader);
       resolve_backreference(out, distance, length)?;
   }
   ```
   - **`push_literal`** and **the LZ77 copy** must NOT bounce through
     the speculative hook on the *common* path. The hook is
     consulted only inside `resolve_backreference` when the distance
     overflows the bytes emitted in this chunk (see step 6).
   - Mirror rapidgzip's separation between `appendToWindow` and
     `appendToWindowUnsafe` (`deflate.hpp:1306`, `1328`) only if a
     profile demands it; in Rust we can let `Vec::push` and
     `extend_from_slice` do the modulo-free thing because we're
     writing into a flat output buffer, not a circular window.

5. **`resolve_backreference`** (replaces `Block::resolveBackreference`,
   `deflate.hpp:1349`):
   - Fast path: `distance <= out.len()` (back-ref fits inside what we
     have already emitted in this chunk). Copy `length` bytes from
     `out[out.len() - distance ..]` to the end. Use
     `out.extend_from_within` (stable as of 1.53) for the
     non-overlapping case (`length <= distance`); for `length >
     distance` (the RLE pattern at the heart of the silesia 258/1
     case), fall back to a byte-by-byte copy loop. **Do not**
     `memcpy` overlapping ranges.
   - **Distance=1 special case**: rapidgzip notes `memset` here is a
     big win (`deflate.hpp:510`, `1394`). Use `Vec::resize` with the
     byte value, or a `slice::fill` over freshly-reserved capacity.
   - **Slow path**: `distance > out.len()` — this is the speculative
     case. Call into
     `rusty_rapidgzip_inflate::speculative::cache_active_ptr()` /
     the hook in `crates/rusty-rapidgzip-inflate/src/speculative.rs`
     to record markers, exactly as the current code does, and emit
     placeholder bytes. Length and the wrap-around-into-emitted-bytes
     transition are handled the same way as today.
   - **`propagate_match`** still gets called unconditionally on the
     fast path, gated by `max_marker_pos`. This is the existing API
     (`speculative.rs:156`) — keep using it. Past 32 KiB of emitted
     output, the early-out makes it a single compare per back-ref.

6. **Speculative hook ABI**: keep the existing `MarkerRec` /
   `SpeculativeContext` types in
   `rusty-rapidgzip-inflate::speculative` — they are stable, used by
   the pipeline, and outlive this kernel swap. The new
   `fast_inflate` is just a new caller of that API.

7. **Length quota**: the C++ loop has `nMaxToDecode`
   (`deflate.hpp:1525`) to bound how much it emits per call. We
   translate this as: the kernel takes `out: &mut Vec<u8>` and a
   `max_emit: usize` budget, returns
   `Ok((bytes_emitted, eob: bool))`. The pipeline drives it in a
   loop until the deflate stream ends.

**Tests for Phase 4**:
- Differential decode the entire `tests/data/*.gz` corpus against
  `safe_inflate.rs`. Assert byte-for-byte equality of output and
  identical EOB positions.
- Run the existing speculative round-trip tests: decode a chunk with
  no prior window → resolve markers → assert equality with a
  full-sequential decode.

### Phase 5 — Wire the kernel into the pipeline (1 day)

1. Add a `fast_inflate` decode entry point to
   `rusty-rapidgzip-deflate::lib`.
2. In `rusty-rapidgzip/src/gzip.rs`, change the engine selector
   (the `--zlib-rs` flag and its default) to route to `fast_inflate`
   by default. Keep `safe_inflate` reachable behind a flag for
   diff-testing. Keep `zlib-rs` reachable for one release cycle as
   an escape hatch.
3. No changes to `pipeline.rs`. The Arc/recycle/CRC fan-out
   architecture is engine-agnostic.

### Phase 6 — Benchmark and tune (3–5 days)

1. Re-run the Phase-0 baseline benchmark with `fast_inflate`. Compare
   wall, user, sys against:
   - the zlib-rs path,
   - `safe_inflate`,
   - C++ rapidgzip (no-ISAL build).
2. Profile with `perf record` + `perf report --no-children` on a
   single-threaded run. Expect the hot tip to be inside
   `decode_dynamic` and `BitReader::peek`. If `propagate_match`
   shows above 2% time on a chunk-aligned workload, investigate
   max_marker_pos cache behaviour.
3. Tune `LUT_BITS` between 10 and 12 if the bench warrants — keep
   the existing one as default unless something else wins
   convincingly.
4. Try the `DoubleLiteralCached` variant **only** if FASTQ
   throughput is still short of C++ rapidgzip; budget another 2 days
   for the port if so.

### Phase 7 — Delete the zlib-rs path (1 day)

Once `fast_inflate` is the default for at least one release and the
fallback path has not been needed:

- Delete `rusty-rapidgzip-inflate/src/inflate.rs` (2486 lines) and
  the surrounding `inflate/` module.
- Keep `rusty-rapidgzip-inflate/src/speculative.rs` —
  `fast_inflate` uses it as the speculative-context ABI. It's small
  (258 lines) and safe.
- Audit and rename: the crate is no longer really "inflate", it's
  "speculation glue + CRC + adler32 + cpu features". Either rename
  or fold its remaining contents into `rusty-rapidgzip-deflate`.

## Risk register

| Risk | Mitigation |
| --- | --- |
| Refill fast path's `unsafe` unaligned load is the project's only `unsafe` in this crate — easy to get wrong on big-endian or with `pos + 8 > len`. | Guard with debug-assert + bounds check. Cover with a `cargo miri` run over the existing differential test corpus. The fast path is 4 lines and trivially auditable. |
| Inlining failures: `peek`/`seek_after_peek` not inlined would tank throughput. | After Phase 1, dump asm for `fast_inflate::decode_dynamic` and require these calls to disappear. CI check optional, manual gate at minimum. |
| `read2`-style refill correctness vs. EOF handling: the C++ source uses exceptions for EOF and pays nothing on the happy path. Our `Result` return will introduce a branch. | Measure: if the branch costs more than ~2%, push the EOF check up to the outer block loop (we know `MAX_RUN_LENGTH = 258` bytes of headroom per back-ref) and let the bit reader assume input is non-empty within a block. |
| `propagate_match` regression: the existing fastpath relies on `max_marker_pos` early-out. Swapping the engine could change call frequency. | Reuse the existing speculative API unchanged — same call sites, same early-out — so this is a no-op risk if Phase 4 follows the plan. |
| Build-time blow-up from monomorphising `read::<N>` for 30+ values of N. | The C++ code monomorphises the same way at template-instantiation time. Rust does too. Cap distinct `N`s by funneling rare values through the runtime `read(n)` path. |
| `cargo expand` confusion when chasing inlining issues. | Use `cargo asm` (the `cargo-asm` crate) or `RUSTFLAGS='--emit=asm'` and grep for the function name. `cargo expand` is for macros, not inlining. |

## Acceptance criteria

- All existing tests in `rusty-rapidgzip`, `rusty-rapidgzip-deflate`,
  `rusty-rapidgzip-inflate` pass with `fast_inflate` as the default
  kernel.
- Single-threaded throughput on `test.fastq.gz` is within 5% of
  C++ rapidgzip (built without ISA-L) on the same machine, and
  strictly faster than the zlib-rs path it replaces.
- Parallel throughput on `test.fastq.gz -P 16 > /dev/null` matches or
  beats the current zlib-rs-based parallel number (≤ 3.7s wall on
  the reference machine, per the existing memory note).
- Total `unsafe` blocks in `rusty-rapidgzip-deflate` are countable
  on one hand and each one carries a `// SAFETY:` comment.
- The `safe_inflate` path remains available and passes a property
  test: `safe_inflate(x) == fast_inflate(x)` for every `x` in the
  fuzz corpus.

## Order of files to touch (cheat sheet for the implementer)

1. `crates/rusty-rapidgzip-deflate/src/bitreader.rs` — full rewrite.
2. `crates/rusty-rapidgzip-deflate/src/tables.rs` — extend with
   length/distance helpers from `RFCTables.hpp`.
3. `crates/rusty-rapidgzip-deflate/src/huffman.rs` — full rewrite,
   exposing three decoders.
4. `crates/rusty-rapidgzip-deflate/src/fast_inflate.rs` — new file,
   the kernel.
5. `crates/rusty-rapidgzip-deflate/src/lib.rs` — re-export
   `fast_inflate`.
6. `crates/rusty-rapidgzip/src/gzip.rs` — route the default decode
   path to `fast_inflate`.
7. Benchmark, tune, then in a separate change set:
   `crates/rusty-rapidgzip-inflate/src/inflate*` — delete.

## References

- C++ source: `/project/rapidgzip_cpp/src/rapidgzip/gzip/deflate.hpp`
  (the kernel), `librapidarchive/src/filereader/BitReader.hpp` (the
  bit reader). The benchmark tables at `deflate.hpp:55–172` are the
  best source of "which variant to port" guidance.
- Rust reference: existing `safe_inflate.rs` (puff-derived,
  unoptimised) as a correctness oracle; existing patched zlib-rs
  path as the performance baseline to beat.
- Speculative hook contract: `rusty-rapidgzip-inflate/src/speculative.rs`
  — unchanged across this port.
