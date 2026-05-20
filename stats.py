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
rapidgzip +0.00s] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip +0.00s] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip +0.03s] pipeline: scanned in 0.034s
[rapidgzip +0.03s] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip +1.01s] 14984.5 MB/s (avg 14984.5 MB/s, 15142.9 MB total)
[rapidgzip +2.01s] 15258.3 MB/s (avg 15120.7 MB/s, 30414.3 MB total)
[rapidgzip +3.01s] 14652.8 MB/s (avg 14965.3 MB/s, 45081.1 MB total)
[rapidgzip +3.74s] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    3.90 secs    fish           external
   usr time   60.81 secs    0.22 millis   60.81 secs
   sys time    1.32 secs    1.18 millis    1.32 secs

                                                                    [ 3s896 | May 20 09:03AM ]

 nix develop: /home/finkernagel/upstream/fastqrab/main/ 
ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip +0.00s] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip +0.00s] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip +0.03s] pipeline: scanned in 0.034s
[rapidgzip +0.03s] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip +1.00s] 13850.3 MB/s (avg 13850.3 MB/s, 13853.9 MB total)
[rapidgzip +2.00s] 15234.4 MB/s (avg 14542.4 MB/s, 29095.0 MB total)
[rapidgzip +3.00s] 14629.4 MB/s (avg 14571.5 MB/s, 43777.5 MB total)
[rapidgzip +3.79s] done: 57124037585 uncompressed bytes, 2477 output chunks

________________________________________________________
Executed in    3.96 secs    fish           external
   usr time   62.46 secs    0.11 millis   62.46 secs
   sys time    1.49 secs    1.12 millis    1.49 secs

                                                                    [ 3s957 | May 20 09:03AM ]

 nix develop: /home/finkernagel/upstream/fastqrab/main/ 
ff-m5:~/upstream/fastqrab/large_test
finkernagel>time /home/finkernagel/.cache/cargo/target/release/rapidgzip-rs ID136786_all_cells_S1_R2_001.fastq.gz -P 16 --verbose  --zlib-rs >/dev/null
This is the new binary
[rapidgzip +0.00s] ID136786_all_cells_S1_R2_001.fastq.gz: mmaped 10385290703 bytes in 0.000s, 16 threads, chunk_size=4194304
[rapidgzip +0.00s] pipeline: using zlib-rs speculative backend (--zlib-rs)
[rapidgzip +0.04s] pipeline: scanned in 0.037s
[rapidgzip +0.04s] pipeline: 2477 boundaries found → 2477 chunk(s), 16 worker(s)
[rapidgzip +1.01s] 15115.2 MB/s (avg 15115.2 MB/s, 15208.4 MB total)
[rapidgzip +2.01s] 15365.3 MB/s (avg 15240.0 MB/s, 30592.8 MB total)
[rapidgzip +3.01s] 14851.1 MB/s (avg 15110.6 MB/s, 45452.4 MB total)
[rapidgzip +3.66s] done: 57124037585 uncompressed bytes, 2477 output chunks
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
