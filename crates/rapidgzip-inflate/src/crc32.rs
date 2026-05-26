// Re-export the public crc32 functions from upstream.
pub use zlib_rs::crc32::crc32;

use crate::CRC32_INITIAL_VALUE;

// Crc32Fold is pub(crate) in upstream and not accessible via __internal-api.
// We provide a software implementation that delegates to upstream's crc32()
// (which itself is SIMD-accelerated) so correctness and performance are preserved.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Crc32Fold {
    value: u32,
}

impl Crc32Fold {
    pub const fn new() -> Self {
        Self { value: CRC32_INITIAL_VALUE }
    }

    pub fn fold(&mut self, src: &[u8], _start: u32) {
        self.value = crc32(self.value, src);
    }

    pub fn fold_copy(&mut self, dst: &mut [u8], src: &[u8]) {
        dst[..src.len()].copy_from_slice(src);
        self.fold(src, 0);
    }

    pub fn finish(self) -> u32 {
        self.value
    }
}
