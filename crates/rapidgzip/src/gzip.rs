//! gzip framing on top of [`rapidgzip_deflate`]'s inflate.
//!
//! Implements RFC 1952 framing:
//!   - 10-byte fixed header (magic + CMF + FLG + MTIME + XFL + OS)
//!   - optional FEXTRA / FNAME / FCOMMENT / FHCRC trailers in header
//!   - DEFLATE-compressed body
//!   - 8-byte trailer: CRC32 of decompressed bytes + uncompressed size mod 2^32
//!
//! Multi-stream gzip files (concatenated members) are supported: after each
//! trailer we look for another member-magic, and stop only on real EOF.

use thiserror::Error;

use rapidgzip_deflate::{inflate, BitReader, DeflateError};

const GZ_MAGIC: [u8; 2] = [0x1f, 0x8b];
const CM_DEFLATE: u8 = 8;

const FTEXT: u8 = 1 << 0;
const FHCRC: u8 = 1 << 1;
const FEXTRA: u8 = 1 << 2;
const FNAME: u8 = 1 << 3;
const FCOMMENT: u8 = 1 << 4;
const FRESERVED: u8 = 0b1110_0000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GzipError {
    #[error("not a gzip file (bad magic)")]
    BadMagic,
    #[error("unsupported compression method: {0}")]
    UnsupportedMethod(u8),
    #[error("reserved gzip flag bits set: {0:#x}")]
    ReservedFlags(u8),
    #[error("truncated gzip stream")]
    Truncated,
    #[error("CRC32 mismatch: expected {expected:#x}, got {got:#x}")]
    CrcMismatch { expected: u32, got: u32 },
    #[error("ISIZE mismatch: expected {expected}, got {got}")]
    IsizeMismatch { expected: u32, got: u64 },
    #[error("deflate: {0}")]
    Deflate(#[from] DeflateError),
}

/// Decode all gzip members in `input`, appending decompressed bytes to `out`.
/// Returns the total number of bytes appended.
pub fn decode_all(input: &[u8], out: &mut Vec<u8>) -> Result<u64, GzipError> {
    let mut pos = 0usize;
    let mut total = 0u64;
    while pos < input.len() {
        let start_len = out.len();
        let consumed = decode_one(&input[pos..], out)?;
        total += (out.len() - start_len) as u64;
        pos += consumed;
    }
    Ok(total)
}

/// Decode a single gzip member starting at `input[0]`. Appends decompressed
/// bytes to `out`, returns the number of input bytes consumed.
pub fn decode_one(input: &[u8], out: &mut Vec<u8>) -> Result<usize, GzipError> {
    let header_len = parse_header(input)?;
    let body_start = header_len;

    let initial_out_len = out.len();
    let mut br = BitReader::new(&input[body_start..]);
    inflate(&mut br, out)?;
    br.byte_align();

    // Bits consumed by inflate, rounded up to bytes.
    let body_bits = br.tell_bit();
    debug_assert_eq!(body_bits % 8, 0);
    let body_bytes = (body_bits / 8) as usize;

    let trailer_start = body_start + body_bytes;
    if trailer_start + 8 > input.len() {
        return Err(GzipError::Truncated);
    }
    let crc_expected = u32::from_le_bytes(
        input[trailer_start..trailer_start + 4].try_into().unwrap(),
    );
    let isize_expected = u32::from_le_bytes(
        input[trailer_start + 4..trailer_start + 8].try_into().unwrap(),
    );

    let decoded = &out[initial_out_len..];
    let crc_got = crc32fast::hash(decoded);
    if crc_got != crc_expected {
        return Err(GzipError::CrcMismatch {
            expected: crc_expected,
            got: crc_got,
        });
    }
    let isize_got = decoded.len() as u64;
    if (isize_got & 0xFFFF_FFFF) as u32 != isize_expected {
        return Err(GzipError::IsizeMismatch {
            expected: isize_expected,
            got: isize_got,
        });
    }

    Ok(trailer_start + 8)
}

/// Parse the gzip header, return its length in bytes.
fn parse_header(input: &[u8]) -> Result<usize, GzipError> {
    if input.len() < 10 {
        return Err(GzipError::Truncated);
    }
    if input[0..2] != GZ_MAGIC {
        return Err(GzipError::BadMagic);
    }
    let cm = input[2];
    if cm != CM_DEFLATE {
        return Err(GzipError::UnsupportedMethod(cm));
    }
    let flg = input[3];
    if flg & FRESERVED != 0 {
        return Err(GzipError::ReservedFlags(flg));
    }
    // Skip MTIME (4) + XFL (1) + OS (1) — already covered by the length check.
    let mut pos = 10;

    if flg & FEXTRA != 0 {
        if pos + 2 > input.len() {
            return Err(GzipError::Truncated);
        }
        let xlen =
            u16::from_le_bytes(input[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2 + xlen;
        if pos > input.len() {
            return Err(GzipError::Truncated);
        }
    }
    if flg & FNAME != 0 {
        pos = skip_zstring(input, pos)?;
    }
    if flg & FCOMMENT != 0 {
        pos = skip_zstring(input, pos)?;
    }
    if flg & FHCRC != 0 {
        // 16-bit CRC of header so far. Spec says low 16 bits of CRC32 of
        // header bytes excluding the CRC itself. We don't validate it —
        // payload CRC32 in the trailer is the meaningful integrity check.
        if pos + 2 > input.len() {
            return Err(GzipError::Truncated);
        }
        pos += 2;
    }
    let _ = FTEXT; // declared for completeness; semantic is "ASCII hint"
    Ok(pos)
}

fn skip_zstring(input: &[u8], from: usize) -> Result<usize, GzipError> {
    let rest = input.get(from..).ok_or(GzipError::Truncated)?;
    let zero = rest.iter().position(|&b| b == 0).ok_or(GzipError::Truncated)?;
    Ok(from + zero + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use sha2::{Digest, Sha256};

    fn corpus_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .unwrap()
            .join("tests/corpus")
    }

    /// Decode every fixture and compare sha256 to the recorded sidecar.
    /// Falls through silently if no corpus is present.
    #[test]
    fn corpus_roundtrip() {
        let dir = corpus_dir();
        let Ok(rd) = fs::read_dir(&dir) else {
            eprintln!("no corpus at {} — skipping", dir.display());
            return;
        };
        let mut checked = 0;
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("gz") {
                continue;
            }
            let Ok(expected) =
                fs::read_to_string(path.with_extension("gz.sha256"))
            else {
                continue;
            };
            let expected = expected.trim();

            let bytes = fs::read(&path).expect("read fixture");
            let mut out = Vec::new();
            decode_all(&bytes, &mut out)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let mut h = Sha256::new();
            h.update(&out);
            let got = hex::encode(h.finalize());
            assert_eq!(got, expected, "{}: sha256 mismatch", path.display());
            checked += 1;
            eprintln!("ok  {}", path.display());
        }
        assert!(checked > 0, "corpus directory is empty");
    }
}
