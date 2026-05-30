use bstr::BStr;
use std::ops::Range;
use std::sync::Arc;

use crate::storage::Storage;

/// Two parallel byte columns (e.g. sequence + quality) sharing a single
/// metadata layout. The shared metadata makes the per-entry length invariant
/// (`seq.len() == qual.len()` for every entry) structural rather than
/// runtime-checked. Each byte buffer is independently `Arc<[u8]>` so seq
/// can be aliased into a tag column without pinning qual, and vice versa.
#[derive(Clone)]
pub struct DualStringPod {
    pub(crate) seq: Arc<[u8]>,
    pub(crate) qual: Arc<[u8]>,
    pub(crate) storage: Storage,
}

impl DualStringPod {
    #[must_use]
    pub fn empty() -> Self {
        DualStringPodBuilder::with_capacity(0, 0).finish()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Sequence bytes of entry `i`.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn seq(&self, i: usize) -> &BStr {
        let r = self.storage.entry_range(i);
        BStr::new(&self.seq[r])
    }

    /// Quality bytes of entry `i`. Shares the same range as `seq(i)`.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn qual(&self, i: usize) -> &BStr {
        let r = self.storage.entry_range(i);
        BStr::new(&self.qual[r])
    }

    /// Both bytes of entry `i` at once.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn pair(&self, i: usize) -> (&BStr, &BStr) {
        let r = self.storage.entry_range(i);
        (BStr::new(&self.seq[r.clone()]), BStr::new(&self.qual[r]))
    }

    #[must_use]
    pub fn entry_len(&self, i: usize) -> usize {
        self.storage.entry_len(i)
    }

    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.storage.used_bytes()
    }

    /// Size of the seq buffer (qual is guaranteed identical in length).
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.seq.len()
    }

    #[must_use]
    pub fn is_fixed_length(&self) -> bool {
        self.storage.current_stride().is_some()
    }

    pub fn cut_start(&mut self, n: usize) {
        let n_u32 = u32::try_from(n).unwrap_or(u32::MAX);
        self.storage.cut_start(n_u32);
    }

    pub fn cut_end(&mut self, n: usize) {
        let n_u32 = u32::try_from(n).unwrap_or(u32::MAX);
        self.storage.cut_end(n_u32);
    }

    /// # Panics
    /// If `range.end > self.len()` or `range.start > range.end`.
    pub fn drain(&mut self, range: Range<usize>) {
        assert!(range.start <= range.end, "drain range start > end");
        assert!(range.end <= self.len(), "drain range past end of pod");
        self.storage.drain(range);
    }

    #[must_use]
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            pod: self,
            front: 0,
            back: self.len(),
        }
    }

    /// Iterator yielding only the sequence column.
    #[must_use]
    pub fn iter_seq(&self) -> SeqIter<'_> {
        SeqIter {
            pod: self,
            front: 0,
            back: self.len(),
        }
    }

    /// Iterator yielding only the quality column.
    #[must_use]
    pub fn iter_qual(&self) -> QualIter<'_> {
        QualIter {
            pod: self,
            front: 0,
            back: self.len(),
        }
    }

    /// Start an alias builder sharing both byte buffers.
    #[must_use]
    pub fn alias_builder(&self) -> DualStringPodAliasBuilder {
        DualStringPodAliasBuilder {
            seq: Arc::clone(&self.seq),
            qual: Arc::clone(&self.qual),
            positions: Vec::new(),
        }
    }
}

impl std::fmt::Debug for DualStringPod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualStringPod")
            .field("len", &self.len())
            .field("fixed_length", &self.is_fixed_length())
            .field("buffer_bytes", &self.buffer_bytes())
            .field("used_bytes", &self.used_bytes())
            .finish()
    }
}

impl<'a> IntoIterator for &'a DualStringPod {
    type Item = (&'a BStr, &'a BStr);
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

pub struct Iter<'a> {
    pod: &'a DualStringPod,
    front: usize,
    back: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a BStr, &'a BStr);
    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let p = self.pod.pair(self.front);
            self.front += 1;
            Some(p)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.back - self.front;
        (r, Some(r))
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            self.back -= 1;
            Some(self.pod.pair(self.back))
        } else {
            None
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

pub struct SeqIter<'a> {
    pod: &'a DualStringPod,
    front: usize,
    back: usize,
}

impl<'a> Iterator for SeqIter<'a> {
    type Item = &'a BStr;
    fn next(&mut self) -> Option<&'a BStr> {
        if self.front < self.back {
            let s = self.pod.seq(self.front);
            self.front += 1;
            Some(s)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.back - self.front;
        (r, Some(r))
    }
}

impl ExactSizeIterator for SeqIter<'_> {}

pub struct QualIter<'a> {
    pod: &'a DualStringPod,
    front: usize,
    back: usize,
}

impl<'a> Iterator for QualIter<'a> {
    type Item = &'a BStr;
    fn next(&mut self) -> Option<&'a BStr> {
        if self.front < self.back {
            let q = self.pod.qual(self.front);
            self.front += 1;
            Some(q)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.back - self.front;
        (r, Some(r))
    }
}

impl ExactSizeIterator for QualIter<'_> {}

// ── owning builder ───────────────────────────────────────────────────────

pub struct DualStringPodBuilder {
    seq: Vec<u8>,
    qual: Vec<u8>,
    storage: Storage,
}

impl DualStringPodBuilder {
    /// Create a builder reserving `entry_len * count` bytes in each buffer.
    ///
    /// # Panics
    /// If `entry_len` exceeds `u32::MAX`.
    #[must_use]
    pub fn with_capacity(entry_len: usize, count: usize) -> Self {
        let byte_cap = entry_len.checked_mul(count).unwrap_or(0);
        let seq = Vec::with_capacity(byte_cap);
        let qual = Vec::with_capacity(byte_cap);
        let storage = if entry_len == 0 {
            Storage::new_variable(count)
        } else {
            let stride = u32::try_from(entry_len).expect("entry_len exceeds u32");
            Storage::new_fixed(stride, count)
        };
        Self { seq, qual, storage }
    }

    /// Push one entry's seq and qual bytes. Storage promotes if length
    /// differs from the current stride.
    ///
    /// # Panics
    /// If `seq.len() != qual.len()` or the byte buffer would exceed `u32::MAX`.
    pub fn push(&mut self, seq: &[u8], qual: &[u8]) {
        assert!(
            seq.len() == qual.len(),
            "seq.len() {} != qual.len() {}",
            seq.len(),
            qual.len()
        );
        if let Some(stride) = self.storage.current_stride()
            && seq.len() as u64 == u64::from(stride)
        {
            self.seq.extend_from_slice(seq);
            self.qual.extend_from_slice(qual);
            self.storage.builder_push_strided();
            return;
        }
        let start_usize = self.seq.len();
        let start = u32::try_from(start_usize).expect("byte buffer exceeds u32::MAX");
        let stop = u32::try_from(start_usize + seq.len()).expect("byte buffer exceeds u32::MAX");
        self.seq.extend_from_slice(seq);
        self.qual.extend_from_slice(qual);
        self.storage.builder_push_position(start, stop);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.seq.len()
    }

    #[must_use]
    pub fn finish(self) -> DualStringPod {
        DualStringPod {
            seq: Arc::from(self.seq.into_boxed_slice()),
            qual: Arc::from(self.qual.into_boxed_slice()),
            storage: self.storage,
        }
    }
}

// ── alias builder ────────────────────────────────────────────────────────

/// Builds a [`DualStringPod`] whose entries reference bytes in an existing
/// pod's `seq` and `qual` `Arc<[u8]>` buffers without copying. Both buffers
/// are pinned for the alias pod's lifetime (snapshot semantics).
pub struct DualStringPodAliasBuilder {
    seq: Arc<[u8]>,
    qual: Arc<[u8]>,
    positions: Vec<(u32, u32)>,
}

impl DualStringPodAliasBuilder {
    /// Record an alias entry covering bytes `[start..start+len]` in both
    /// the source pod's seq and qual buffers.
    ///
    /// # Panics
    /// If the range is out of bounds or values exceed `u32`.
    pub fn push_alias(&mut self, start: usize, len: usize) {
        let start_u32 = u32::try_from(start).expect("alias start exceeds u32");
        let len_u32 = u32::try_from(len).expect("alias len exceeds u32");
        let stop_u32 = start_u32
            .checked_add(len_u32)
            .expect("alias start + len exceeds u32");
        assert!(
            (stop_u32 as usize) <= self.seq.len(),
            "alias range {}..{} out of source bounds (seq len {})",
            start,
            start + len,
            self.seq.len()
        );
        self.positions.push((start_u32, stop_u32));
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    #[must_use]
    pub fn finish(self) -> DualStringPod {
        DualStringPod {
            seq: self.seq,
            qual: self.qual,
            storage: Storage::Variable {
                positions: self.positions,
                head_skip: 0,
                tail_skip: 0,
            },
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{DualStringPod, DualStringPodBuilder};
    use bstr::BStr;

    fn b(s: &str) -> &[u8] {
        s.as_bytes()
    }

    #[test]
    fn empty_dual_pod() {
        let p = DualStringPod::empty();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
        assert_eq!(p.used_bytes(), 0);
    }

    #[test]
    fn fixed_length_dual_basic() {
        let mut bld = DualStringPodBuilder::with_capacity(3, 2);
        bld.push(b("AAA"), b("###"));
        bld.push(b("BBB"), b("FFF"));
        let p = bld.finish();
        assert!(p.is_fixed_length());
        assert_eq!(p.seq(0), BStr::new("AAA"));
        assert_eq!(p.qual(0), BStr::new("###"));
        assert_eq!(p.seq(1), BStr::new("BBB"));
        assert_eq!(p.qual(1), BStr::new("FFF"));
        let (s, q) = p.pair(0);
        assert_eq!(s, BStr::new("AAA"));
        assert_eq!(q, BStr::new("###"));
    }

    #[test]
    fn dual_promotes_on_length_mismatch() {
        let mut bld = DualStringPodBuilder::with_capacity(3, 2);
        bld.push(b("AAA"), b("###"));
        bld.push(b("BB"), b("FF"));
        let p = bld.finish();
        assert!(!p.is_fixed_length());
        assert_eq!(p.seq(0), BStr::new("AAA"));
        assert_eq!(p.seq(1), BStr::new("BB"));
        assert_eq!(p.qual(1), BStr::new("FF"));
    }

    #[test]
    #[should_panic(expected = "seq.len()")]
    fn push_unequal_lengths_panics() {
        let mut bld = DualStringPodBuilder::with_capacity(0, 1);
        bld.push(b("AAA"), b("##")); // mismatch
    }

    #[test]
    fn cut_start_dual() {
        let mut bld = DualStringPodBuilder::with_capacity(5, 2);
        bld.push(b("HELLO"), b("12345"));
        bld.push(b("WORLD"), b("67890"));
        let mut p = bld.finish();
        p.cut_start(2);
        assert_eq!(p.seq(0), BStr::new("LLO"));
        assert_eq!(p.qual(0), BStr::new("345"));
        assert_eq!(p.seq(1), BStr::new("RLD"));
        assert_eq!(p.qual(1), BStr::new("890"));
    }

    #[test]
    fn cut_end_dual() {
        let mut bld = DualStringPodBuilder::with_capacity(5, 1);
        bld.push(b("HELLO"), b("12345"));
        let mut p = bld.finish();
        p.cut_end(2);
        assert_eq!(p.seq(0), BStr::new("HEL"));
        assert_eq!(p.qual(0), BStr::new("123"));
    }

    #[test]
    fn cuts_apply_identically_to_both() {
        let mut bld = DualStringPodBuilder::with_capacity(0, 2);
        bld.push(b("ABCDEF"), b("uvwxyz"));
        bld.push(b("123"), b("XYZ"));
        let mut p = bld.finish();
        p.cut_start(1);
        p.cut_end(1);
        assert_eq!(p.seq(0), BStr::new("BCDE"));
        assert_eq!(p.qual(0), BStr::new("vwxy"));
        assert_eq!(p.seq(1), BStr::new("2"));
        assert_eq!(p.qual(1), BStr::new("Y"));
    }

    #[test]
    fn dual_drain_promotes() {
        let mut bld = DualStringPodBuilder::with_capacity(2, 4);
        bld.push(b("AA"), b("11"));
        bld.push(b("BB"), b("22"));
        bld.push(b("CC"), b("33"));
        bld.push(b("DD"), b("44"));
        let mut p = bld.finish();
        p.drain(1..3);
        assert!(!p.is_fixed_length());
        assert_eq!(p.len(), 2);
        assert_eq!(p.seq(0), BStr::new("AA"));
        assert_eq!(p.qual(0), BStr::new("11"));
        assert_eq!(p.seq(1), BStr::new("DD"));
        assert_eq!(p.qual(1), BStr::new("44"));
    }

    #[test]
    fn dual_iter_yields_pairs() {
        let mut bld = DualStringPodBuilder::with_capacity(2, 2);
        bld.push(b("AB"), b("12"));
        bld.push(b("CD"), b("34"));
        let p = bld.finish();
        let pairs: Vec<(&BStr, &BStr)> = p.iter().collect();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, BStr::new("AB"));
        assert_eq!(pairs[0].1, BStr::new("12"));
        assert_eq!(pairs[1].0, BStr::new("CD"));
        assert_eq!(pairs[1].1, BStr::new("34"));
    }

    #[test]
    fn iter_seq_and_iter_qual_separate() {
        let mut bld = DualStringPodBuilder::with_capacity(2, 2);
        bld.push(b("AB"), b("12"));
        bld.push(b("CD"), b("34"));
        let p = bld.finish();
        let seqs: Vec<&BStr> = p.iter_seq().collect();
        let quals: Vec<&BStr> = p.iter_qual().collect();
        assert_eq!(seqs, vec![BStr::new("AB"), BStr::new("CD")]);
        assert_eq!(quals, vec![BStr::new("12"), BStr::new("34")]);
    }

    #[test]
    fn dual_clone_shares_arcs() {
        let mut bld = DualStringPodBuilder::with_capacity(2, 1);
        bld.push(b("AB"), b("12"));
        let p = bld.finish();
        let q = p.clone();
        assert!(std::ptr::eq(p.seq.as_ref(), q.seq.as_ref()));
        assert!(std::ptr::eq(p.qual.as_ref(), q.qual.as_ref()));
    }

    #[test]
    fn alias_builder_basic() {
        let mut bld = DualStringPodBuilder::with_capacity(0, 1);
        bld.push(b("HELLOWORLD"), b("FFFFFFFFFF"));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(0, 5); // "HELLO" / "FFFFF"
        ab.push_alias(5, 5); // "WORLD" / "FFFFF"
        let aliased = ab.finish();
        assert_eq!(aliased.len(), 2);
        assert_eq!(aliased.seq(0), BStr::new("HELLO"));
        assert_eq!(aliased.qual(0), BStr::new("FFFFF"));
        assert_eq!(aliased.seq(1), BStr::new("WORLD"));
        assert_eq!(aliased.qual(1), BStr::new("FFFFF"));
    }

    #[test]
    fn alias_pod_survives_source_drain() {
        let mut bld = DualStringPodBuilder::with_capacity(4, 3);
        bld.push(b("AAAA"), b("1111"));
        bld.push(b("BBBB"), b("2222"));
        bld.push(b("CCCC"), b("3333"));
        let mut source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(4, 4); // points at "BBBB" / "2222"
        let aliased = ab.finish();
        source.drain(1..2);
        assert_eq!(source.len(), 2);
        assert_eq!(aliased.seq(0), BStr::new("BBBB"));
        assert_eq!(aliased.qual(0), BStr::new("2222"));
    }

    #[test]
    fn alias_pod_pins_qual_independently() {
        // Even when only seq seems to matter, qual must remain accessible
        // for tag-column usages that later query qualities at hit ranges.
        let mut bld = DualStringPodBuilder::with_capacity(0, 1);
        bld.push(b("ACGTACGT"), b("!\"#$%&'("));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(2, 4);
        let aliased = ab.finish();
        drop(source);
        assert_eq!(aliased.seq(0), BStr::new("GTAC"));
        assert_eq!(aliased.qual(0), BStr::new("#$%&"));
    }

    #[test]
    #[should_panic(expected = "out of source bounds")]
    fn alias_out_of_bounds_panics() {
        let mut bld = DualStringPodBuilder::with_capacity(0, 1);
        bld.push(b("hello"), b("xxxxx"));
        let source = bld.finish();
        let mut ab = source.alias_builder();
        ab.push_alias(3, 10);
    }

    #[test]
    fn dual_send_sync_check() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DualStringPod>();
        assert_send_sync::<super::DualStringPodBuilder>();
        assert_send_sync::<super::DualStringPodAliasBuilder>();
    }

    #[test]
    fn debug_format_does_not_panic() {
        let mut bld = DualStringPodBuilder::with_capacity(2, 1);
        bld.push(b("AB"), b("12"));
        let p = bld.finish();
        let s = format!("{p:?}");
        assert!(s.contains("DualStringPod"));
    }
}
