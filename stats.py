import scipy.stats as ss
import numpy as np
import re

baseline = """
    Executed in    4.62 secs    fish           external
   usr time   58.36 secs    0.00 millis   58.36 secs
   sys time    1.55 secs    1.03 millis    1.54 secs

                                                                    [ 4s617 | May 19 08:14PM ]

ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip] pipeline: scanned in 0.035s
[rapidgzip] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    4.34 secs    fish           external
   usr time   56.98 secs  751.00 micros   56.98 secs
   sys time    1.68 secs    0.00 micros    1.68 secs

                                                                    [ 4s339 | May 19 08:15PM ]

ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip] pipeline: scanned in 0.037s
[rapidgzip] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    4.32 secs    fish           external
   usr time   57.35 secs    0.00 micros   57.35 secs
   sys time    1.78 secs  841.00 micros    1.78 secs'
"""

native = """
time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip] pipeline: scanned in 0.039s
[rapidgzip] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    4.53 secs    fish           external
   usr time   58.93 secs  317.00 micros   58.93 secs
   sys time    1.82 secs  254.00 micros    1.82 secs

                                                                    [ 4s530 | May 19 08:30PM ]

ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip] pipeline: scanned in 0.038s
[rapidgzip] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    4.26 secs    fish           external
   usr time   58.55 secs    0.48 millis   58.55 secs
   sys time    1.61 secs    1.43 millis    1.61 secs

                                                                    [ 4s263 | May 19 08:30PM ]

ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip] pipeline: scanned in 0.029s
[rapidgzip] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    4.76 secs    fish           external
   usr time   61.28 secs    1.05 millis   61.28 secs
   sys time    1.71 secs    0.00 millis    1.71 secs
    """


def extract_times(input):
    return {
        "wall": ([float(x) for x in re.findall("Executed in\\s+(\\d+.\\d+)", input)]),
        "usr": [float(x) for x in re.findall("usr time\\s+(\\d+.\\d+)", input)],
        "sys": [float(x) for x in re.findall("sys time\\s+(\\d+.\\d+)", input)],
    }


bl = extract_times(baseline)
native = extract_times(native)

for k in bl:
    p = ss.ttest_ind(bl[k], native[k], equal_var=True)
    print(k, "%.2f" % np.mean(bl[k]), "%.2f" % np.mean(native[k]), p.pvalue)
