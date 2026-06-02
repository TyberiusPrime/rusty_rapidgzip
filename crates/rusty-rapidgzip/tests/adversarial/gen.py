"""Regenerate the adversarial FASTQ fixtures for `tests/adversarial.rs`.

These exercise the u32 per-column byte limit of the StringPod columnar layout
(positions are u32, so any single column / single read line is capped at 4 GiB):

  * more_than_64_mb_after_expansion — many small reads whose decoded size far
    exceeds the pipeline's 64 MiB speculative reservation, but stays well under
    u32::MAX. Must decode correctly (a *correctness* regression guard).

  * 4gb_plus_read — a single read longer than u32::MAX bytes. Must fail with a
    clean "FASTQ read length exceeds the allowed maximum of 4 GiB" error rather
    than panicking in the u32 cast.

After running this, gzip the outputs to match the committed fixtures:

    python gen.py
    gzip -n more_than_64_mb_after_expansion.fastq 4gb_plus_read.fastq
"""


def more_than_64_mb():
    total = 0
    read = "@" + "A" * 256 + "\n" + "A" * 256 + "\n+\n" + "A" * 256 + "\n"
    with open("more_than_64_mb_after_expansion.fastq", "w") as op:
        while total < 65 * 1024**2:
            op.write(read)
            total += len(read)


def more_than_4_gb_read():
    with open("4gb_plus_read.fastq", "w") as op:
        op.write(">Read1\n")
        op.write("A" * (4 * 1024**3 + 100))
        op.write("\n+\n")
        op.write("A" * (4 * 1024**3 + 100))
        op.write("\n")


more_than_64_mb()
# Writes ~8 GiB uncompressed; uncomment to regenerate the 4 GiB fixture.
# more_than_4_gb_read()
