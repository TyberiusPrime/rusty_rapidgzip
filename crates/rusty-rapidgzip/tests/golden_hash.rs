//! Golden-hash test: for every fixture listed in `tests/corpus/reference_sums.json`
//! whose `.gz` file is present, decode via `read_gz` and compare the sha256 of
//! the streamed-out bytes to the recorded ground truth.
//!
//! The reference sums live in a single JSON config (`reference_sums.json`),
//! produced alongside the corpus by `cargo run -p xtask -- build-corpus`. Tests
//! skip cleanly when the JSON or the fixtures are absent, so a fresh checkout
//! doesn't fail CI just because nobody fetched the (large) corpus yet.
//!
//! Large fixtures are skipped by default to keep `cargo test` fast even when a
//! full corpus is present on disk; set `RAPIDGZIP_FULL_CORPUS=1` to check them.

use std::fs;
use std::path::{Path, PathBuf};

use crossbeam_channel::bounded;
use sha2::{Digest, Sha256};

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .join("tests/corpus")
}

/// One fixture's recorded ground truth.
#[derive(Debug, Clone)]
struct RefEntry {
    file: String,
    sha256: String,
    gz_size: u64,
}

// ── minimal, dependency-free JSON reader for `reference_sums.json` ──────────
//
// The file is machine-generated and regular:
//
//   {
//     "<name>.gz": { "sha256": "<hex>", "raw_size": <n>, "gz_size": <n> },
//     ...
//   }
//
// so a line-oriented scan is sufficient and avoids pulling in a JSON crate.

/// If `line` (already a config line) is a top-level `"<name>.gz": {` object key,
/// return the file name.
fn object_key_ending_gz(line: &str) -> Option<String> {
    let line = line.trim();
    let rest = line.strip_prefix('"')?;
    let end = rest.find('"')?;
    let key = &rest[..end];
    if !key.ends_with(".gz") {
        return None;
    }
    let after = rest[end + 1..].trim_start();
    let after = after.strip_prefix(':')?.trim_start();
    after.starts_with('{').then(|| key.to_string())
}

/// Extract a quoted string field, e.g. `"sha256": "abc"` → `Some("abc")`.
fn string_field(line: &str, field: &str) -> Option<String> {
    let line = line.trim();
    let after = line.strip_prefix(&format!("\"{field}\""))?.trim_start();
    let after = after.strip_prefix(':')?.trim_start();
    let after = after.strip_prefix('"')?;
    let end = after.find('"')?;
    Some(after[..end].to_string())
}

/// Extract an unsigned integer field, e.g. `"gz_size": 20` → `Some(20)`.
fn number_field(line: &str, field: &str) -> Option<u64> {
    let line = line.trim();
    let after = line.strip_prefix(&format!("\"{field}\""))?.trim_start();
    let after = after.strip_prefix(':')?.trim_start();
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

fn parse_reference_sums(json: &str) -> Vec<RefEntry> {
    let mut out = Vec::new();
    let mut file: Option<String> = None;
    let mut sha: Option<String> = None;
    let mut size: Option<u64> = None;

    let finalize = |file: &mut Option<String>,
                    sha: &mut Option<String>,
                    size: &mut Option<u64>,
                    out: &mut Vec<RefEntry>| {
        if let (Some(f), Some(s)) = (file.take(), sha.take()) {
            out.push(RefEntry {
                file: f,
                sha256: s,
                gz_size: size.unwrap_or(0),
            });
        }
        *size = None;
    };

    for line in json.lines() {
        if let Some(name) = object_key_ending_gz(line) {
            finalize(&mut file, &mut sha, &mut size, &mut out);
            file = Some(name);
        } else if let Some(v) = string_field(line, "sha256") {
            sha = Some(v);
        } else if let Some(v) = number_field(line, "gz_size") {
            size = Some(v);
        }
    }
    finalize(&mut file, &mut sha, &mut size, &mut out);
    out.sort_by(|a, b| a.file.cmp(&b.file));
    out
}

fn reference_entries() -> Option<Vec<RefEntry>> {
    let path = corpus_dir().join("reference_sums.json");
    let json = fs::read_to_string(&path).ok()?;
    Some(parse_reference_sums(&json))
}

fn decode_and_hash(path: &Path) -> anyhow::Result<String> {
    let (tx, rx) = bounded::<std::sync::Arc<Vec<u8>>>(16);
    let path_owned = path.to_owned();
    let producer = std::thread::spawn(move || {
        rusty_rapidgzip::read_gz(&path_owned, tx, rusty_rapidgzip::Config::default())
    });

    let mut h = Sha256::new();
    for chunk in rx {
        h.update(&**chunk);
    }
    producer.join().expect("producer panicked")?;
    Ok(hex::encode(h.finalize()))
}

/// Default ceiling on compressed fixture size; larger ones are skipped unless
/// `RAPIDGZIP_FULL_CORPUS` is set.
const MAX_DEFAULT_GZ: u64 = 200 * 1024 * 1024;

/// Fixtures that MUST be present and decode correctly. These are the committed,
/// deterministic synthetic fixtures (`tests/corpus/synth-*.gz`, un-ignored in
/// `.gitignore`). Unlike the large external corpus — which is fetched on demand
/// and skipped when absent — a missing required fixture is a hard failure, so
/// CI always exercises a real decode even on a bare checkout.
const REQUIRED_FIXTURES: &[&str] = &[
    "synth-empty.gz",
    "synth-single-byte.gz",
    "synth-concat-members.gz",
    "synth-zeros-1m.gz",
    "synth-spaces-1m.gz",
    "synth-0xff-1m.gz",
    "synth-repeated-pattern.gz",
    "synth-level1.gz",
    "synth-level9.gz",
    "synth-random-1m.gz",
];

fn is_required(name: &str) -> bool {
    REQUIRED_FIXTURES.contains(&name)
}

#[test]
fn golden_hash_all_corpus() {
    let dir = corpus_dir();
    let Some(entries) = reference_entries() else {
        eprintln!(
            "no reference_sums.json at {} — run `cargo run -p xtask -- build-corpus`",
            dir.join("reference_sums.json").display()
        );
        return;
    };
    assert!(
        !entries.is_empty(),
        "reference_sums.json parsed to zero entries — the parser or the file format changed"
    );

    let full = std::env::var_os("RAPIDGZIP_FULL_CORPUS").is_some();
    let mut checked = 0usize;
    let mut absent = 0usize;
    let mut large = 0usize;
    let mut failures = Vec::new();

    for e in entries {
        let path = dir.join(&e.file);
        if !path.exists() {
            // Required fixtures are committed: fail, don't skip.
            if is_required(&e.file) {
                failures.push(format!(
                    "REQUIRED FIXTURE ABSENT: {} — it is committed under tests/corpus/ and must be present",
                    e.file
                ));
            } else {
                absent += 1;
            }
            continue;
        }
        // Required fixtures are tiny; never large-skip them.
        if !full && e.gz_size > MAX_DEFAULT_GZ && !is_required(&e.file) {
            large += 1;
            eprintln!("skip (large; set RAPIDGZIP_FULL_CORPUS=1) {}", e.file);
            continue;
        }
        match decode_and_hash(&path) {
            Ok(actual) if actual == e.sha256 => {
                checked += 1;
                eprintln!("ok  {}", e.file);
            }
            Ok(actual) => failures.push(format!(
                "MISMATCH {}\n  expected {}\n  got      {actual}",
                e.file, e.sha256
            )),
            Err(err) => failures.push(format!("ERROR {}: {err:#}", e.file)),
        }
    }

    eprintln!("golden_hash: {checked} checked, {absent} absent, {large} large-skipped");
    if !failures.is_empty() {
        panic!(
            "{} fixture(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    if checked == 0 {
        eprintln!("no corpus fixtures present to check — that's fine for a fresh checkout");
    }
}

/// Harness self-check independent of the decoder: parse the config and verify
/// every recorded sha256 is well-formed. Always runs (the config is committed).
#[test]
fn corpus_config_self_check() {
    let Some(entries) = reference_entries() else {
        eprintln!("no reference_sums.json — fine for a fresh checkout");
        return;
    };
    assert!(
        !entries.is_empty(),
        "reference_sums.json parsed to zero entries"
    );
    for e in &entries {
        assert_eq!(e.sha256.len(), 64, "{}: bad sha256 length", e.file);
        assert!(
            e.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "{}: non-hex sha256",
            e.file
        );
    }
}

/// Decode every member of a (possibly multi-member) gzip stream with a
/// per-member kernel, returning the concatenated output's sha256. Mirrors
/// `gzip::decode_all`'s member loop but lets us swap the inflate engine.
fn decode_all_with(
    input: &[u8],
    mut decode_member: impl FnMut(&[u8], &mut Vec<u8>, u32) -> Result<usize, rusty_rapidgzip::GzipError>,
) -> Result<String, rusty_rapidgzip::GzipError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut member = 0u32;
    while pos < input.len() {
        let consumed = decode_member(&input[pos..], &mut out, member)?;
        pos += consumed;
        member += 1;
    }
    Ok(hex::encode(Sha256::digest(&out)))
}

/// The committed synthetic fixtures must decode identically through BOTH the
/// perf-tuned `fast_inflate` kernel and the pure-safe `safe_inflate` engine,
/// and both must match the recorded ground truth. A standing differential
/// check of the two engines on real data (the AFL fuzzer covers random input;
/// this pins the engines against the committed corpus on every test run).
#[test]
fn both_engines_agree_on_required_fixtures() {
    use rusty_rapidgzip::gzip::{decode_one_indexed_fast, decode_one_indexed_safe};

    let dir = corpus_dir();
    let entries = reference_entries().expect("reference_sums.json is committed and required");
    let by_name: std::collections::HashMap<&str, &RefEntry> =
        entries.iter().map(|e| (e.file.as_str(), e)).collect();

    for &name in REQUIRED_FIXTURES {
        let entry = by_name
            .get(name)
            .unwrap_or_else(|| panic!("{name} missing from reference_sums.json"));
        let bytes = fs::read(dir.join(name))
            .unwrap_or_else(|e| panic!("required fixture {name} unreadable: {e}"));

        let fast = decode_all_with(&bytes, decode_one_indexed_fast)
            .unwrap_or_else(|e| panic!("fast engine failed on {name}: {e:#}"));
        let safe = decode_all_with(&bytes, decode_one_indexed_safe)
            .unwrap_or_else(|e| panic!("safe engine failed on {name}: {e:#}"));

        assert_eq!(fast, entry.sha256, "fast_inflate mismatch on {name}");
        assert_eq!(safe, entry.sha256, "safe_inflate mismatch on {name}");
    }
}
