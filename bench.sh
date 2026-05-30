#!/usr/bin/env bash
set -eoux pipefail
cargo build --release

CMD=/home/finkernagel/.cache/cargo/target/release/rusty-rapidgzip-rs
export RAPIDGZIP_KERNEL=fast

for P in 1 2 16; do
    CORES="0-$((P - 1))"
    [ "$P" -eq 1 ] && CORES="0"
    echo "=== rusty P=$P ==="
    time taskset -c "$CORES" $CMD ERR2432917_1.fastq.gz -P "$P" >/dev/null
    echo "=== rapidgzip P=$P ==="
    time taskset -c "$CORES" rapidgzip -P "$P" -cd ERR2432917_1.fastq.gz >/dev/null
done
