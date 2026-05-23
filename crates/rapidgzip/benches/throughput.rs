//! Throughput bench: read every fixture in `tests/corpus/` and feed it
//! through `read_gz` into a /dev/null sink. Reports bytes/second.

use std::fs;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_channel::bounded;

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .unwrap()
        .join("tests/corpus")
}

fn bench_throughput(c: &mut Criterion) {
    let dir = corpus_dir();
    let Ok(rd) = fs::read_dir(&dir) else {
        eprintln!("no corpus dir; skipping throughput bench");
        return;
    };
    let mut group = c.benchmark_group("read_gz");
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("gz") {
            continue;
        }
        let Ok(meta) = fs::metadata(&path) else { continue };
        // Skip files >256 MiB unless explicitly requested via env.
        let big_ok = std::env::var_os("RAPIDGZIP_BENCH_BIG").is_some();
        if !big_ok && meta.len() > 256 * 1024 * 1024 {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        group.throughput(Throughput::Bytes(meta.len()));
        group.bench_with_input(BenchmarkId::from_parameter(&name), &path, |b, path| {
            b.iter(|| {
                let (tx, rx) = bounded::<std::sync::Arc<Vec<u8>>>(16);
                let p = path.clone();
                let h = std::thread::spawn(move || {
                    rapidgzip::read_gz(&p, tx, rapidgzip::Config::default())
                });
                let mut total: u64 = 0;
                for chunk in rx {
                    total += chunk.len() as u64;
                }
                let _ = h.join().unwrap();
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
