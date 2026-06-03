//! Static DEFLATE tables from RFC 1951.

/// HCLEN code-length code permutation (RFC 1951 §3.2.7).
pub const CODE_LENGTH_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

/// (base, extra_bits) for length codes 257..=285.
/// Index 0 = code 257.
pub const LENGTH_BASE: [u16; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
pub const LENGTH_EXTRA: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];

/// (base, extra_bits) for distance codes 0..=29.
pub const DISTANCE_BASE: [u16; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
pub const DISTANCE_EXTRA: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];

/// Build the fixed Huffman literal/length code lengths (RFC 1951 §3.2.6).
pub fn fixed_literal_lengths() -> Vec<u8> {
    let mut lens = vec![0u8; 288];
    for v in lens.iter_mut().take(144) {
        *v = 8;
    }
    for v in lens.iter_mut().take(256).skip(144) {
        *v = 9;
    }
    for v in lens.iter_mut().take(280).skip(256) {
        *v = 7;
    }
    for v in lens.iter_mut().take(288).skip(280) {
        *v = 8;
    }
    lens
}

/// Fixed Huffman distance code lengths: all 5.
pub fn fixed_distance_lengths() -> Vec<u8> {
    vec![5u8; 30]
}
