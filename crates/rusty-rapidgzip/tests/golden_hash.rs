//! Golden-hash test: for every `<name>.gz` in `tests/corpus/` with a
//! sibling `<name>.gz.sha256`, decode via `read_gz` and compare sha256 of
//! the streamed-out bytes to the recorded ground truth.
//!
//! The corpus is built by `cargo run -p xtask -- build-corpus`. Tests skip
//! cleanly if no corpus is present, so a fresh checkout doesn't fail CI just
//! because nobody ran xtask yet.

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

fn fixtures() -> Vec<(PathBuf, String)> {
    let dir = corpus_dir();
    let Ok(rd) = fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("gz") {
            continue;
        }
        let sha_path = path.with_extension("gz.sha256");
        let Ok(expected) = fs::read_to_string(&sha_path) else {
            continue;
        };
        out.push((path, expected.trim().to_string()));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
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

#[test]
fn golden_hash_all_corpus() {
    let fixtures = fixtures();
    if fixtures.is_empty() {
        eprintln!(
            "no corpus found at {} — run `cargo run -p xtask -- build-corpus`",
            corpus_dir().display()
        );
        return;
    }

    let mut failures = Vec::new();
    for (path, expected) in fixtures {
        match decode_and_hash(&path) {
            Ok(actual) if actual == expected => {
                eprintln!("ok  {}", path.display());
            }
            Ok(actual) => {
                failures.push(format!(
                    "MISMATCH {}\n  expected {expected}\n  got      {actual}",
                    path.display()
                ));
            }
            Err(e) => {
                failures.push(format!("ERROR {}: {e:#}", path.display()));
            }
        }
    }

    if !failures.is_empty() {
        panic!("{} fixtures failed:\n{}", failures.len(), failures.join("\n"));
    }
}

/// Smoke test that doesn't depend on the decoder — just checks the harness
/// itself wires up. Always runs.
#[test]
fn corpus_harness_self_check() {
    let dir = corpus_dir();
    if !dir.exists() {
        eprintln!("no corpus dir — that's fine for a fresh checkout");
        return;
    }
    for (path, expected) in fixtures() {
        assert_eq!(expected.len(), 64, "{}: bad sha256 sidecar", path.display());
    }
}
