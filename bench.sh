#!/usr/bin/env bash
set -eoux pipefail
cargo build --release

CMD=/home/finkernagel/.cache/cargo/target/release/rusty-rapidgzip-rs
export RAPIDGZIP_KERNEL=fast 
time taskset -c 0,1 $CMD ERR2432917_1.fastq.gz -P 2 >/dev/null
time taskset -c 0-16 $CMD ERR2432917_1.fastq.gz -P 16 >/dev/null

time $CMD ERR2432917_1.fastq.gz -P 32 >/dev/null
time taskset -c 0,1 rapidgzip -P 2 -cd ERR2432917_1.fastq.gz >/dev/null
time taskset -c 0-16 rapidgzip -P 16 -cd ERR2432917_1.fastq.gz >/dev/null
time rapidgzip -P 32 -cd ERR2432917_1.fastq.gz >/dev/null



