# Vendored zune-inflate 0.2.54 — local patch

Vendored verbatim from crates.io `zune-inflate` 0.2.54 (license: MIT OR
Apache-2.0 OR Zlib), with one additive change for rusty-rapidgzip's
`zune-inflate` decode backend.

## Patch

`src/decoder.rs`: added one public method to `impl DeflateDecoder` —

```rust
pub fn input_position(&self) -> usize {
    self.stream.get_position() + self.position + self.stream.over_read
}
```

This exposes the byte-aligned input offset where a raw `decode_deflate()` run
stopped (the start of any trailing gzip/zlib trailer). The upstream crate
returns only the decoded `Vec<u8>` and computes this expression internally to
locate the wrapper footer (see `decode_zlib`/`decode_gzip`), but never exposes
it. Our speculative per-member path decodes a *bare* DEFLATE body and needs the
trailer offset to read the member's CRC/ISIZE and find the next member, so we
surface it. No behavioural change to upstream code paths.

To re-vendor: download the 0.2.54 `.crate`, replace `src/`, and re-apply the
method above.
