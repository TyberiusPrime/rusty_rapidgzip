use bstr::BStr;
use std::ops::Range;
use std::sync::Arc;

use crate::storage::Storage;

/// A single column of byte strings backed by one shared `Arc<[u8]>` and a
/// columnar metadata layout. Once `finish()`ed by a builder, the byte buffer
/// is immutable; only the metadata can be mutated (cuts, drains).
#[derive(Clone)]
pub struct StringPod {
    // `Arc<Vec<u8>>` (not `Arc<[u8]>`) so `finish` can wrap the builder's Vec
    // as-is — converting to a boxed slice would realloc+copy the whole buffer
    // whenever the builder over-reserved.
    pub(crate) data: Arc<Vec<u8>>,
    pub(crate) storage: Storage,
}

impl StringPod {
    /// An empty pod with no entries and an empty buffer.
    #[must_use]
    pub fn empty() -> Self {
        StringPodBuilder::with_capacity(0, 0).finish()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Get entry `i` as a `&BStr`.
    ///
    /// # Panics
    /// If `i >= self.len()` or the recorded byte range is out of bounds of
    /// the buffer (which would indicate corruption).
    #[must_use]
    pub fn get(&self, i: usize) -> &BStr {
        let range = self.storage.entry_range(i);
        BStr::new(&self.data[range])
    }

    /// Visible length of entry `i` (after cuts).
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn entry_len(&self, i: usize) -> usize {
        self.storage.entry_len(i)
    }

    /// Sum of visible bytes across all entries (what a tight rebuild would need).
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.storage.used_bytes()
    }

    /// Total size of the underlying Arc'd byte buffer, including any bytes
    /// orphaned by `drain` or hidden by `cut_*`.
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.data.len()
    }

    /// `true` if storage is `FixedLength` (all entries share a stride).
    #[must_use]
    pub fn is_fixed_length(&self) -> bool {
        self.storage.current_stride().is_some()
    }

    /// Cut `n` bytes off the start of every entry. O(1).
    pub fn cut_start(&mut self, n: usize) {
        let n_u32 = u32::try_from(n).unwrap_or(u32::MAX);
        self.storage.cut_start(n_u32);
    }

    /// Cut `n` bytes off the end of every entry. O(1).
    pub fn cut_end(&mut self, n: usize) {
        let n_u32 = u32::try_from(n).unwrap_or(u32::MAX);
        self.storage.cut_end(n_u32);
    }

    /// Remove a contiguous range of entries. Promotes `FixedLength` → `Variable`.
    /// Bytes for removed entries remain in the buffer (orphaned).
    ///
    /// # Panics
    /// If `range.end > self.len()` or `range.start > range.end`.
    pub fn drain(&mut self, range: Range<usize>) {
        assert!(range.start <= range.end, "drain range start > end");
        assert!(range.end <= self.len(), "drain range past end of pod");
        self.storage.drain(range);
    }

    /// Append one entry to a *finished* pod, mirroring builder semantics
    /// (stride-fast-path with auto-promotion). The bytes are copied into the
    /// buffer's tail.
    ///
    /// Requires unique ownership of the byte buffer — call before the pod is
    /// shared (cloned / sent). Relies on reserved slack (see
    /// [`reserve_for_appends`](Self::reserve_for_appends)) to avoid
    /// reallocating a large buffer.
    ///
    /// # Panics
    /// If the buffer is shared (`Arc` strong count > 1).
    pub fn push(&mut self, bytes: &[u8]) {
        let data =
            Arc::get_mut(&mut self.data).expect("StringPod::push requires a uniquely-owned buffer");
        if let Some(stride) = self.storage.current_stride() {
            if bytes.len() as u64 == u64::from(stride) {
                data.extend_from_slice(bytes);
                self.storage.builder_push_strided();
                return;
            }
        }
        let start = u32::try_from(data.len()).expect("byte buffer exceeds u32::MAX");
        let stop = u32::try_from(data.len() + bytes.len()).expect("byte buffer exceeds u32::MAX");
        data.extend_from_slice(bytes);
        self.storage.builder_push_position(start, stop);
    }

    /// Drop the first `n` entries from the view. O(1): a byte offset on
    /// `FixedLength`, an entry-index skip on `Variable`. No bytes move.
    pub fn pop_front(&mut self, n: usize) {
        self.storage.pop_front(u32::try_from(n).unwrap_or(u32::MAX));
    }

    /// Truncate the view to at most `len` entries (drops from the back). O(1).
    pub fn truncate(&mut self, len: usize) {
        self.storage.truncate(len);
    }

    /// Ensure the buffer has spare capacity for roughly `n` more average-sized
    /// entries, so subsequent [`push`](Self::push) calls land in place instead
    /// of reallocating a (possibly multi-MB) shared buffer. No-op if the buffer
    /// is already shared.
    pub fn reserve_for_appends(&mut self, n: usize) {
        let per = match self.storage.current_stride() {
            Some(stride) => stride as usize,
            None => self
                .data
                .len()
                .checked_div(self.len())
                .map_or(64, |per| per.max(1)),
        };
        if let Some(data) = Arc::get_mut(&mut self.data) {
            data.reserve(n.saturating_mul(per.max(1)));
        }
    }

    /// Iterate visible entries as `&BStr` in order.
    #[must_use]
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            pod: self,
            front: 0,
            back: self.len(),
        }
    }

    /// Start an alias builder that shares this pod's byte buffer. New entries
    /// pushed via the alias builder will reference bytes inside `self.data`
    /// without copying.
    #[must_use]
    pub fn alias_builder(&self) -> StringPodAliasBuilder {
        StringPodAliasBuilder {
            data: Arc::clone(&self.data),
            positions: Vec::new(),
        }
    }
}

impl std::fmt::Debug for StringPod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringPod")
            .field("len", &self.len())
            .field("fixed_length", &self.is_fixed_length())
            .field("buffer_bytes", &self.buffer_bytes())
            .field("used_bytes", &self.used_bytes())
            .finish()
    }
}

impl<'a> IntoIterator for &'a StringPod {
    type Item = &'a BStr;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

pub struct Iter<'a> {
    pod: &'a StringPod,
    front: usize,
    back: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a BStr;
    fn next(&mut self) -> Option<&'a BStr> {
        if self.front < self.back {
            let r = self.pod.get(self.front);
            self.front += 1;
            Some(r)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            self.back -= 1;
            Some(self.pod.get(self.back))
        } else {
            None
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

// ── owning builder ───────────────────────────────────────────────────────

/// Builds a [`StringPod`] by pushing owned byte strings into a fresh buffer.
/// Starts `FixedLength` with the supplied stride; auto-promotes to `Variable`
/// on the first length mismatch.
pub struct StringPodBuilder {
    data: Vec<u8>,
    storage: Storage,
}

impl StringPodBuilder {
    /// Create a builder reserving `entry_len * count` bytes. If `entry_len > 0`,
    /// storage starts `FixedLength` with stride `entry_len`; otherwise starts
    /// `Variable`.
    ///
    /// # Panics
    /// If `entry_len` exceeds `u32::MAX`.
    #[must_use]
    pub fn with_capacity(entry_len: usize, count: usize) -> Self {
        let byte_cap = entry_len.checked_mul(count).unwrap_or(0);
        let data = Vec::with_capacity(byte_cap);
        let storage = if entry_len == 0 {
            Storage::new_variable(count)
        } else {
            let stride = u32::try_from(entry_len).expect("entry_len exceeds u32");
            Storage::new_fixed(stride, count)
        };
        Self { data, storage }
    }

    /// Push one entry's bytes. If storage is `FixedLength` and `bytes.len()`
    /// doesn't match the stride, promotes to `Variable` first (one-time O(count)).
    ///
    /// # Panics
    /// If the byte buffer would exceed `u32::MAX` total bytes.
    pub fn push(&mut self, bytes: &[u8]) {
        if let Some(stride) = self.storage.current_stride() {
            if bytes.len() as u64 == u64::from(stride) {
                self.data.extend_from_slice(bytes);
                self.storage.builder_push_strided();
                return;
            }
        }
        // Variable (or promoted) path
        let start_usize = self.data.len();
        let start = u32::try_from(start_usize).expect("byte buffer exceeds u32::MAX");
        let stop = u32::try_from(start_usize + bytes.len()).expect("byte buffer exceeds u32::MAX");
        self.data.extend_from_slice(bytes);
        self.storage.builder_push_position(start, stop);
    }

    /// Number of entries pushed so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Bytes written to the buffer so far (including any wasted slack in
    /// `FixedLength` slots — currently none, but documented for symmetry).
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.data.len()
    }

    /// Finalise the builder. Wraps the byte buffer in `Arc<Vec<u8>>` without
    /// reallocating (any over-reserved capacity is retained, not copied away).
    #[must_use]
    pub fn finish(self) -> StringPod {
        StringPod {
            data: Arc::new(self.data),
            storage: self.storage,
        }
    }
}

// ── alias builder ────────────────────────────────────────────────────────

/// Builds a [`StringPod`] whose entries reference bytes in an existing pod's
/// `Arc<[u8]>` without copying. The resulting pod co-owns the source bytes;
/// subsequent mutations of the source pod do not affect the alias pod
/// (snapshot semantics).
pub struct StringPodAliasBuilder {
    data: Arc<Vec<u8>>,
    positions: Vec<(u32, u32)>,
}

impl StringPodAliasBuilder {
    /// Record an alias entry covering `data[start..start+len]` of the source.
    ///
    /// # Panics
    /// If the range is out of bounds of the source buffer, or exceeds `u32`.
    pub fn push_alias(&mut self, start: usize, len: usize) {
        let start_u32 = u32::try_from(start).expect("alias start exceeds u32");
        let len_u32 = u32::try_from(len).expect("alias len exceeds u32");
        let stop_u32 = start_u32
            .checked_add(len_u32)
            .expect("alias start + len exceeds u32");
        assert!(
            (stop_u32 as usize) <= self.data.len(),
            "alias range {}..{} out of source bounds (len {})",
            start,
            start + len,
            self.data.len()
        );
        self.positions.push((start_u32, stop_u32));
    }

    /// Number of alias entries pushed so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Finalise the alias builder. The resulting pod always has Variable
    /// storage (entries reference arbitrary, potentially non-contiguous
    /// regions of the shared buffer).
    #[must_use]
    pub fn finish(self) -> StringPod {
        StringPod {
            data: self.data,
            storage: Storage::Variable {
                positions: self.positions,
                head_skip: 0,
                tail_skip: 0,
                front_skip: 0,
            },
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{StringPod, StringPodBuilder};
    use bstr::BStr;

    fn b(s: &str) -> &[u8] {
        s.as_bytes()
    }

    #[test]
    fn empty_pod() {
        let p = StringPod::empty();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
        assert_eq!(p.used_bytes(), 0);
        assert_eq!(p.buffer_bytes(), 0);
        assert_eq!(p.iter().count(), 0);
    }

    #[test]
    fn fixed_length_basic() {
        let mut bld = StringPodBuilder::with_capacity(3, 4);
        bld.push(b("AAA"));
        bld.push(b("BBB"));
        bld.push(b("CCC"));
        let p = bld.finish();
        assert_eq!(p.len(), 3);
        assert!(p.is_fixed_length());
        assert_eq!(p.get(0), BStr::new("AAA"));
        assert_eq!(p.get(1), BStr::new("BBB"));
        assert_eq!(p.get(2), BStr::new("CCC"));
        assert_eq!(p.used_bytes(), 9);
        assert_eq!(p.buffer_bytes(), 9);
        let collected: Vec<&BStr> = p.iter().collect();
        assert_eq!(
            collected,
            vec![BStr::new("AAA"), BStr::new("BBB"), BStr::new("CCC")]
        );
    }

    #[test]
    fn fixed_length_promotes_on_mismatch() {
        let mut bld = StringPodBuilder::with_capacity(3, 4);
        bld.push(b("AAA"));
        bld.push(b("BB")); // promotes
        bld.push(b("CCC"));
        let p = bld.finish();
        assert!(!p.is_fixed_length());
        assert_eq!(p.len(), 3);
        assert_eq!(p.get(0), BStr::new("AAA"));
        assert_eq!(p.get(1), BStr::new("BB"));
        assert_eq!(p.get(2), BStr::new("CCC"));
        assert_eq!(p.used_bytes(), 8);
    }

    #[test]
    fn variable_from_start() {
        let mut bld = StringPodBuilder::with_capacity(0, 3);
        bld.push(b("hello"));
        bld.push(b("hi"));
        bld.push(b("foobar"));
        let p = bld.finish();
        assert!(!p.is_fixed_length());
        assert_eq!(p.get(0), BStr::new("hello"));
        assert_eq!(p.get(1), BStr::new("hi"));
        assert_eq!(p.get(2), BStr::new("foobar"));
        assert_eq!(p.used_bytes(), 13);
    }

    #[test]
    fn cut_start_fixed() {
        let mut bld = StringPodBuilder::with_capacity(5, 2);
        bld.push(b("HELLO"));
        bld.push(b("WORLD"));
        let mut p = bld.finish();
        p.cut_start(2);
        assert_eq!(p.get(0), BStr::new("LLO"));
        assert_eq!(p.get(1), BStr::new("RLD"));
        assert_eq!(p.entry_len(0), 3);
        assert_eq!(p.used_bytes(), 6);
        assert_eq!(p.buffer_bytes(), 10); // bytes still there
                                          // double cut
        p.cut_start(1);
        assert_eq!(p.get(0), BStr::new("LO"));
    }

    #[test]
    fn cut_end_fixed() {
        let mut bld = StringPodBuilder::with_capacity(5, 2);
        bld.push(b("HELLO"));
        bld.push(b("WORLD"));
        let mut p = bld.finish();
        p.cut_end(2);
        assert_eq!(p.get(0), BStr::new("HEL"));
        assert_eq!(p.get(1), BStr::new("WOR"));
    }

    #[test]
    fn cut_start_then_cut_end_fixed() {
        let mut bld = StringPodBuilder::with_capacity(6, 2);
        bld.push(b("ABCDEF"));
        bld.push(b("UVWXYZ"));
        let mut p = bld.finish();
        p.cut_start(1);
        p.cut_end(1);
        assert_eq!(p.get(0), BStr::new("BCDE"));
        assert_eq!(p.get(1), BStr::new("VWXY"));
    }

    #[test]
    fn cut_oversaturates_fixed() {
        let mut bld = StringPodBuilder::with_capacity(3, 2);
        bld.push(b("AAA"));
        bld.push(b("BBB"));
        let mut p = bld.finish();
        p.cut_start(100);
        assert_eq!(p.get(0), BStr::new(""));
        assert_eq!(p.get(1), BStr::new(""));
        assert_eq!(p.entry_len(0), 0);
    }

    #[test]
    fn cut_oversaturates_variable() {
        let mut bld = StringPodBuilder::with_capacity(0, 2);
        bld.push(b("AB"));
        bld.push(b("XYZWQ"));
        let mut p = bld.finish();
        p.cut_start(3);
        // entry 0 ("AB") is shorter than 3 — saturates to empty
        assert_eq!(p.get(0), BStr::new(""));
        // entry 1 ("XYZWQ") yields "WQ"
        assert_eq!(p.get(1), BStr::new("WQ"));
    }

    #[test]
    fn cut_end_oversaturates_variable() {
        let mut bld = StringPodBuilder::with_capacity(0, 2);
        bld.push(b("AB"));
        bld.push(b("XYZWQ"));
        let mut p = bld.finish();
        p.cut_end(3);
        assert_eq!(p.get(0), BStr::new(""));
        assert_eq!(p.get(1), BStr::new("XY"));
    }

    #[test]
    fn cut_start_and_end_combined_variable() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("ABCDEFG"));
        let mut p = bld.finish();
        p.cut_start(2);
        p.cut_end(2);
        assert_eq!(p.get(0), BStr::new("CDE"));
    }

    #[test]
    fn cut_start_and_end_saturate_to_empty_variable() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("ABC"));
        let mut p = bld.finish();
        p.cut_start(2);
        p.cut_end(5); // would dip negative
        assert_eq!(p.get(0), BStr::new(""));
    }

    #[test]
    fn drain_fixed_promotes() {
        let mut bld = StringPodBuilder::with_capacity(2, 5);
        bld.push(b("AA"));
        bld.push(b("BB"));
        bld.push(b("CC"));
        bld.push(b("DD"));
        bld.push(b("EE"));
        let mut p = bld.finish();
        assert!(p.is_fixed_length());
        p.drain(1..3);
        assert!(!p.is_fixed_length());
        assert_eq!(p.len(), 3);
        assert_eq!(p.get(0), BStr::new("AA"));
        assert_eq!(p.get(1), BStr::new("DD"));
        assert_eq!(p.get(2), BStr::new("EE"));
    }

    #[test]
    fn drain_variable() {
        let mut bld = StringPodBuilder::with_capacity(0, 3);
        bld.push(b("alpha"));
        bld.push(b("beta"));
        bld.push(b("gamma"));
        let mut p = bld.finish();
        p.drain(0..1);
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(0), BStr::new("beta"));
        assert_eq!(p.get(1), BStr::new("gamma"));
    }

    #[test]
    fn drain_with_cut_preserves_cut() {
        let mut bld = StringPodBuilder::with_capacity(4, 3);
        bld.push(b("ABCD"));
        bld.push(b("EFGH"));
        bld.push(b("IJKL"));
        let mut p = bld.finish();
        p.cut_start(1);
        p.cut_end(1);
        p.drain(1..2); // promotes, removes "FG"
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(0), BStr::new("BC"));
        assert_eq!(p.get(1), BStr::new("JK"));
    }

    #[test]
    fn drain_empty_range_noop() {
        let mut bld = StringPodBuilder::with_capacity(2, 3);
        bld.push(b("AA"));
        bld.push(b("BB"));
        bld.push(b("CC"));
        let mut p = bld.finish();
        p.drain(1..1);
        assert!(p.is_fixed_length());
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn iter_double_ended() {
        let mut bld = StringPodBuilder::with_capacity(2, 3);
        bld.push(b("AA"));
        bld.push(b("BB"));
        bld.push(b("CC"));
        let p = bld.finish();
        let mut it = p.iter();
        assert_eq!(it.next().unwrap(), BStr::new("AA"));
        assert_eq!(it.next_back().unwrap(), BStr::new("CC"));
        assert_eq!(it.next().unwrap(), BStr::new("BB"));
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
    }

    #[test]
    fn iter_exact_size() {
        let mut bld = StringPodBuilder::with_capacity(2, 3);
        bld.push(b("AA"));
        bld.push(b("BB"));
        bld.push(b("CC"));
        let p = bld.finish();
        let it = p.iter();
        assert_eq!(it.len(), 3);
        assert_eq!(it.size_hint(), (3, Some(3)));
    }

    #[test]
    fn into_iter_for_reference() {
        let mut bld = StringPodBuilder::with_capacity(2, 2);
        bld.push(b("AA"));
        bld.push(b("BB"));
        let p = bld.finish();
        let mut count = 0;
        for _ in &p {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn clone_is_cheap_arc_share() {
        let mut bld = StringPodBuilder::with_capacity(3, 2);
        bld.push(b("AAA"));
        bld.push(b("BBB"));
        let p = bld.finish();
        let q = p.clone();
        // Same underlying bytes (Arc pointer equality)
        assert!(std::ptr::eq(p.data.as_ref(), q.data.as_ref()));
        assert_eq!(p.get(0), q.get(0));
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn get_out_of_bounds_panics_fixed() {
        let mut bld = StringPodBuilder::with_capacity(2, 2);
        bld.push(b("AA"));
        let p = bld.finish();
        let _ = p.get(5);
    }

    #[test]
    fn alias_builder_basic() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("HELLOWORLD"));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(0, 5); // "HELLO"
        ab.push_alias(5, 5); // "WORLD"
        ab.push_alias(2, 3); // "LLO"
        let aliased = ab.finish();
        assert_eq!(aliased.len(), 3);
        assert_eq!(aliased.get(0), BStr::new("HELLO"));
        assert_eq!(aliased.get(1), BStr::new("WORLD"));
        assert_eq!(aliased.get(2), BStr::new("LLO"));
    }

    #[test]
    fn alias_builder_zero_length_entry() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("hello"));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(3, 0);
        let aliased = ab.finish();
        assert_eq!(aliased.get(0), BStr::new(""));
    }

    #[test]
    #[should_panic(expected = "out of source bounds")]
    fn alias_builder_out_of_bounds_panics() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("hello"));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(3, 10);
    }

    #[test]
    fn alias_pod_snapshot_survives_source_drain() {
        let mut bld = StringPodBuilder::with_capacity(3, 3);
        bld.push(b("AAA"));
        bld.push(b("BBB"));
        bld.push(b("CCC"));
        let mut source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(3, 3); // points at "BBB"
        let aliased = ab.finish();
        // Mutate source — drain promotes and drops entry 1
        source.drain(1..2);
        assert_eq!(source.len(), 2);
        // Alias still sees original bytes
        assert_eq!(aliased.get(0), BStr::new("BBB"));
    }

    #[test]
    fn alias_pod_snapshot_survives_source_cuts() {
        let mut bld = StringPodBuilder::with_capacity(0, 1);
        bld.push(b("ABCDEFGH"));
        let mut source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(0, 8);
        let aliased = ab.finish();
        source.cut_start(3);
        source.cut_end(3);
        // Source is now "DE" visible
        assert_eq!(source.get(0), BStr::new("DE"));
        // Alias still sees the full original
        assert_eq!(aliased.get(0), BStr::new("ABCDEFGH"));
    }

    #[test]
    fn used_bytes_vs_buffer_bytes_after_drain() {
        let mut bld = StringPodBuilder::with_capacity(3, 4);
        bld.push(b("AAA"));
        bld.push(b("BBB"));
        bld.push(b("CCC"));
        bld.push(b("DDD"));
        let mut p = bld.finish();
        assert_eq!(p.used_bytes(), 12);
        assert_eq!(p.buffer_bytes(), 12);
        p.drain(1..3);
        // 2 entries × 3 bytes = 6 visible; 12 in buffer (drain doesn't reclaim)
        assert_eq!(p.used_bytes(), 6);
        assert_eq!(p.buffer_bytes(), 12);
    }

    #[test]
    fn builder_len_tracks() {
        let mut bld = StringPodBuilder::with_capacity(2, 4);
        assert_eq!(bld.len(), 0);
        assert!(bld.is_empty());
        bld.push(b("AB"));
        assert_eq!(bld.len(), 1);
        bld.push(b("CD"));
        assert_eq!(bld.len(), 2);
    }

    #[test]
    fn rebuild_pattern_produces_fixed_again() {
        // Variable old pod, but transform yields uniform output → new pod is FixedLength.
        let mut bld = StringPodBuilder::with_capacity(0, 3);
        bld.push(b("hello"));
        bld.push(b("hi"));
        bld.push(b("yo"));
        let old = bld.finish();
        let mut nb = StringPodBuilder::with_capacity(3, old.len());
        for s in &old {
            let mut buf = vec![b'X'; 3];
            let take = s.len().min(3);
            buf[..take].copy_from_slice(&s[..take]);
            nb.push(&buf);
        }
        let new = nb.finish();
        assert!(new.is_fixed_length());
        assert_eq!(new.len(), 3);
        assert_eq!(new.get(0), BStr::new("hel"));
        assert_eq!(new.get(1), BStr::new("hiX"));
        assert_eq!(new.get(2), BStr::new("yoX"));
    }

    #[test]
    fn fixed_then_variable_zero_byte_entries() {
        // stride=0 means "all entries empty" until something different shows up.
        let mut bld = StringPodBuilder::with_capacity(0, 3);
        bld.push(b(""));
        bld.push(b(""));
        let p = bld.finish();
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(0), BStr::new(""));
        assert_eq!(p.get(1), BStr::new(""));
    }

    #[test]
    fn very_small_push_after_large_promotes_correctly() {
        let mut bld = StringPodBuilder::with_capacity(10, 3);
        bld.push(b("0123456789"));
        bld.push(b("abcdefghij"));
        bld.push(b("x")); // promotes
        let p = bld.finish();
        assert_eq!(p.get(0), BStr::new("0123456789"));
        assert_eq!(p.get(1), BStr::new("abcdefghij"));
        assert_eq!(p.get(2), BStr::new("x"));
    }

    #[test]
    fn debug_format_does_not_panic() {
        let mut bld = StringPodBuilder::with_capacity(2, 2);
        bld.push(b("AB"));
        bld.push(b("CD"));
        let p = bld.finish();
        let s = format!("{p:?}");
        assert!(s.contains("StringPod"));
    }

    // compile-time check: pods are Send+Sync
    #[test]
    fn send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StringPod>();
        assert_send_sync::<super::StringPodBuilder>();
        assert_send_sync::<super::StringPodAliasBuilder>();
    }
}
