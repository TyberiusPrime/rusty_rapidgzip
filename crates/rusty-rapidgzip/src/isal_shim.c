/*
 * Thin C shim over Intel ISA-L's igzip inflate (feature `isal`).
 *
 * ISA-L's `struct inflate_state` is large and version-sensitive (it embeds the
 * Huffman lookup tables and a 64 KiB+ history window), so rather than mirror it
 * in Rust we let the real header own its layout and expose a tiny, stable ABI:
 * opaque alloc/free plus two one-shot decode entry points that mirror the
 * libdeflate backend exactly (see isal_ffi.rs).
 */
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <isa-l/igzip_lib.h>

/* Allocate one opaque ISA-L inflate state. Returns NULL on OOM. */
void *rrg_isal_alloc(void) {
    return malloc(sizeof(struct inflate_state));
}

void rrg_isal_free(void *p) {
    free(p);
}

/*
 * One-shot raw-DEFLATE decode, mirroring libdeflate_deflate_decompress_ex.
 *
 * Decodes the bare deflate stream `in[0..in_len]` to its final (BFINAL) block
 * into `out[0..out_cap]`. On success *in_used gets the input bytes consumed
 * rounded UP to a whole byte (i.e. the byte-aligned gzip trailer start) and
 * *out_made gets the produced byte count.
 *
 * Returns 0 on success, 2 on output overflow (caller grows & retries), -1 on
 * any decode error or truncated input.
 */
int rrg_isal_inflate_raw(void *st, const unsigned char *in, size_t in_len,
                         unsigned char *out, size_t out_cap,
                         size_t *in_used, size_t *out_made) {
    struct inflate_state *s = (struct inflate_state *)st;
    isal_inflate_init(s);
    s->next_in = (unsigned char *)in;
    s->avail_in = (uint32_t)in_len;
    s->next_out = out;
    s->avail_out = (uint32_t)out_cap;
    s->crc_flag = ISAL_DEFLATE; /* raw deflate, no gzip/zlib wrapper */

    int ret = isal_inflate(s);
    if (ret != ISAL_DECOMP_OK)
        return -1;
    if (s->block_state != ISAL_BLOCK_FINISH)
        return (s->avail_out == 0) ? 2 : -1;

    /* Exact bit length of the deflate stream = consumed_bytes*8 - buffered_bits;
     * round up to the byte boundary where the gzip trailer begins. */
    uint64_t consumed_bytes = (uint64_t)in_len - (uint64_t)s->avail_in;
    uint64_t bits = consumed_bytes * 8 - (uint64_t)s->read_in_length;
    *in_used = (size_t)((bits + 7) / 8);
    *out_made = (size_t)s->total_out;
    return 0;
}

/*
 * One-shot full-gzip-member decode (header + DEFLATE + CRC/ISIZE), mirroring
 * libdeflate_gzip_decompress. Used for self-contained BGZF blocks. ISA-L parses
 * the gzip header and verifies the trailer checksum itself.
 *
 * Returns 0 on success, 2 on output overflow, -1 on error.
 */
int rrg_isal_inflate_gzip(void *st, const unsigned char *in, size_t in_len,
                          unsigned char *out, size_t out_cap, size_t *out_made) {
    struct inflate_state *s = (struct inflate_state *)st;
    isal_inflate_init(s);
    s->next_in = (unsigned char *)in;
    s->avail_in = (uint32_t)in_len;
    s->next_out = out;
    s->avail_out = (uint32_t)out_cap;
    s->crc_flag = ISAL_GZIP; /* parse gzip header + verify CRC32/ISIZE */

    int ret = isal_inflate(s);
    if (ret != ISAL_DECOMP_OK)
        return -1;
    if (s->block_state != ISAL_BLOCK_FINISH)
        return (s->avail_out == 0) ? 2 : -1;

    *out_made = (size_t)s->total_out;
    return 0;
}
