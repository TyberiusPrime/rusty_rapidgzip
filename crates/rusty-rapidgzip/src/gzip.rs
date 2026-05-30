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

use rusty_rapidgzip_deflate::{inflate, safe_inflate, BitReader, DeflateError};

/// Which DEFLATE engine to use for the framed-gzip and BGZF paths.
///
/// The speculative parallel pipeline always uses the in-tree `fast_inflate`
/// kernel (it carries the marker machinery); this selector controls the
/// *window-known* serial paths only (the BGZF per-member decode and the
/// serial-gzip `decode_one_indexed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InflateEngine {
    /// In-tree rapidgzip-deflate inflater (perf-tuned, contains `unsafe`).
    #[default]
    Intree,
    /// Pure-safe puff-style inflater. Currently slower; here for safety and
    /// as the target for iterative perf work.
    Safe,
}

impl InflateEngine {
    /// Read `RAPIDGZIP_INFLATE` and resolve to an engine. Unknown values
    /// fall back to the in-tree engine (the historical default).
    pub fn from_env() -> Self {
        match std::env::var("RAPIDGZIP_INFLATE").as_deref() {
            Ok("safe") => Self::Safe,
            Ok("intree") => Self::Intree,
            _ => Self::default(),
        }
    }
}

const GZ_MAGIC: [u8; 2] = [0x1f, 0x8b];
const CM_DEFLATE: u8 = 8;

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
    #[error(
        "CRC32 mismatch in member #{member} (uncompressed bytes: {uncompressed}, \
         trailer at byte {trailer_byte}): expected {expected:#010x}, got {got:#010x}"
    )]
    CrcMismatch {
        expected: u32,
        got: u32,
        /// 0-based index of the gzip member (most files have just one).
        member: u32,
        /// Total uncompressed bytes in this member.
        uncompressed: u64,
        /// Byte offset of the trailer in the input file.
        trailer_byte: u64,
    },
    #[error(
        "ISIZE mismatch in member #{member} (trailer at byte {trailer_byte}): \
         expected {expected}, got {got}"
    )]
    IsizeMismatch {
        expected: u32,
        got: u64,
        member: u32,
        trailer_byte: u64,
    },
    #[error("deflate: {0}")]
    Deflate(#[from] DeflateError),
}

/// Decode all gzip members in `input`, appending decompressed bytes to `out`.
/// Returns the total number of bytes appended.
pub fn decode_all(input: &[u8], out: &mut Vec<u8>) -> Result<u64, GzipError> {
    let mut pos = 0usize;
    let mut total = 0u64;
    let mut member = 0u32;
    while pos < input.len() {
        let start_len = out.len();
        let consumed = decode_one_indexed(&input[pos..], out, member)?;
        total += (out.len() - start_len) as u64;
        pos += consumed;
        member += 1;
    }
    Ok(total)
}

/// Decode a single gzip member starting at `input[0]`. Appends decompressed
/// bytes to `out`, returns the number of input bytes consumed.
pub fn decode_one(input: &[u8], out: &mut Vec<u8>) -> Result<usize, GzipError> {
    decode_one_indexed(input, out, 0)
}

/// Same as [`decode_one`] but stamps `member` index into error messages so
/// multi-stream callers can pinpoint which member failed.
pub fn decode_one_indexed(
    input: &[u8],
    out: &mut Vec<u8>,
    member: u32,
) -> Result<usize, GzipError> {
    // Engine dispatch for the serial path: `Safe` uses the puff-style
    // inflater, everything else uses the in-tree decoder below.
    if let InflateEngine::Safe = InflateEngine::from_env() {
        return decode_one_indexed_safe(input, out, member);
    }

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
            member,
            uncompressed: decoded.len() as u64,
            trailer_byte: trailer_start as u64,
        });
    }
    let isize_got = decoded.len() as u64;
    if (isize_got & 0xFFFF_FFFF) as u32 != isize_expected {
        return Err(GzipError::IsizeMismatch {
            expected: isize_expected,
            got: isize_got,
            member,
            trailer_byte: trailer_start as u64,
        });
    }

    Ok(trailer_start + 8)
}

/// Same as [`decode_one_indexed`] but uses the pure-safe `safe_inflate`
/// engine. Returns input bytes consumed (header + body + 8-byte trailer).
pub fn decode_one_indexed_safe(
    input: &[u8],
    out: &mut Vec<u8>,
    member: u32,
) -> Result<usize, GzipError> {
    let header_len = parse_header(input)?;
    let initial_out_len = out.len();
    let body_bytes = safe_inflate::inflate_into(&input[header_len..], out)?;

    let trailer_start = header_len + body_bytes;
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
            member,
            uncompressed: decoded.len() as u64,
            trailer_byte: trailer_start as u64,
        });
    }
    let isize_got = decoded.len() as u64;
    if (isize_got & 0xFFFF_FFFF) as u32 != isize_expected {
        return Err(GzipError::IsizeMismatch {
            expected: isize_expected,
            got: isize_got,
            member,
            trailer_byte: trailer_start as u64,
        });
    }
    Ok(trailer_start + 8)
}

/// Parse the gzip header, return its length in bytes.
pub fn parse_header(input: &[u8]) -> Result<usize, GzipError> {
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
    Ok(pos)
}

/// If `input` starts with a BGZF block header, return its total size in bytes
/// (BSIZE + 1). Otherwise return `None`.
///
/// BGZF (samtools spec) is gzip with a mandatory FEXTRA subfield
/// `SI1='B', SI2='C', SLEN=2, BSIZE(u16 LE)` where BSIZE = total block size - 1.
/// Each BGZF member is an independent, complete deflate stream — no back-refs
/// cross member boundaries. That lets the pipeline split the file at known
/// byte offsets and decode each block in parallel with the plain serial
/// inflater (no speculation, no marker resolution).
pub(crate) fn parse_bgzf_block_size(input: &[u8]) -> Option<u32> {
    if input.len() < 12 || input[0..2] != GZ_MAGIC || input[2] != CM_DEFLATE {
        return None;
    }
    let flg = input[3];
    if flg & FEXTRA == 0 {
        return None;
    }
    let xlen = u16::from_le_bytes(input[10..12].try_into().unwrap()) as usize;
    if 12 + xlen > input.len() {
        return None;
    }
    let mut p = 12;
    let end = 12 + xlen;
    while p + 4 <= end {
        let si1 = input[p];
        let si2 = input[p + 1];
        let slen = u16::from_le_bytes(input[p + 2..p + 4].try_into().unwrap()) as usize;
        if p + 4 + slen > end {
            return None;
        }
        if si1 == b'B' && si2 == b'C' && slen == 2 {
            let bsize = u16::from_le_bytes(input[p + 4..p + 6].try_into().unwrap());
            return Some(bsize as u32 + 1);
        }
        p += 4 + slen;
    }
    None
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
