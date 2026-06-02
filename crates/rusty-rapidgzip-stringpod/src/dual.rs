use bstr::BStr;
use std::ops::Range;
use std::sync::Arc;

use crate::single::StringPod;
use crate::storage::Storage;

/// Error returned by [`DualStringPod::try_from_columns`] when two columns
/// cannot be fused into one [`DualStringPod`] without copying — i.e. they are
/// not a valid sequence + quality pair sharing a single metadata layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnError {
    /// The two columns have a different number of entries.
    EqualCountViolated,
    /// Per-entry counts match, but some record's sequence and quality strings
    /// have different lengths.
    UnequalLengths,
    /// Per-entry lengths match, but the two byte layouts are not a constant
    /// translation of each other (entry starts differ by a non-constant
    /// offset), so they cannot share one metadata column. In practice this
    /// means the input isn't well-formed FASTQ.
    NotATranslation,
}

impl std::fmt::Display for ColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            ColumnError::EqualCountViolated => {
                "sequence and quality columns have a different number of entries"
            }
            ColumnError::UnequalLengths => {
                "sequence/quality entry length mismatch: a record's seq and qual differ in length"
            }
            ColumnError::NotATranslation => {
                "sequence and quality byte layouts are not a constant translation of each other"
            }
        };
        f.write_str(msg)
    }
}

impl std::error::Error for ColumnError {}

/// Two parallel byte columns (e.g. sequence + quality) sharing a single
/// metadata layout. The shared metadata makes the per-entry length invariant
/// (`seq.len() == qual.len()` for every entry) structural rather than
/// runtime-checked. Each byte buffer is independently `Arc<[u8]>` so seq
/// can be aliased into a tag column without pinning qual, and vice versa.
#[derive(Clone)]
pub struct DualStringPod {
    // `Arc<Vec<u8>>` (not `Arc<[u8]>`) so `finish` wraps the builder Vecs as-is
    // rather than reallocating away any over-reserved capacity.
    pub(crate) seq: Arc<Vec<u8>>,
    pub(crate) qual: Arc<Vec<u8>>,
    pub(crate) storage: Storage,

    seq_first_byte: usize,
    qual_first_byte: usize,
}

impl DualStringPod {
    #[must_use]
    pub fn empty() -> Self {
        DualStringPodBuilder::with_capacity(0, 0).finish()
    }

    /// Fuse two columns (e.g. FASTQ sequence + quality) into a single dual pod
    /// **without copying any bytes**: both byte buffers are moved in as-is and a
    /// single shared metadata column describes their *relative* per-entry
    /// layout. The two `*_first_byte` offsets record where each column's entry 0
    /// actually starts, so the buffers may live at a constant offset from each
    /// other (the "stray bytes at the start" from the 'pull into previous block'
    /// schema) and still share one metadata column.
    ///
    /// This is the zero-copy bridge from a columnar split that builds `seq` and
    /// `qual` as independent [`StringPod`]s to the structural
    /// `seq[i].len() == qual[i].len()` invariant a [`DualStringPod`] enforces.
    ///
    /// The two columns must have the same entry count and, for every entry, the
    /// same length. Their entry starts must also be a constant translation of
    /// each other — which, for lockstep-built columns, holds exactly when the
    /// input is well-formed FASTQ.
    ///
    /// # Errors
    ///
    /// - [`ColumnError::EqualCountViolated`] when `seq.len() != qual.len()`.
    /// - [`ColumnError::UnequalLengths`] when some record's seq and qual differ
    ///   in length.
    /// - [`ColumnError::NotATranslation`] when the per-entry starts are not a
    ///   constant translation of each other — at which point it's time to tell
    ///   the consumer that this ain't FASTQ.
    pub fn try_from_columns(seq: StringPod, qual: StringPod) -> Result<Self, ColumnError> {
        if seq.len() != qual.len() {
            return Err(ColumnError::EqualCountViolated);
        }

        // Fast path: both columns are fixed-length with an identical *relative*
        // layout (same stride / cut overlay / count). Only their `front_byte`
        // may differ — that constant offset is captured by the two
        // `*_first_byte` fields, so we keep the strided metadata (true O(1),
        // no positions vec) and share it between both buffers.
        if let (
            Storage::FixedLength {
                stride: seq_stride,
                head_skip: seq_head_skip,
                visible_len: seq_visible_len,
                count: seq_count,
                front_byte: seq_front_byte,
            },
            Storage::FixedLength {
                stride: qual_stride,
                head_skip: qual_head_skip,
                visible_len: qual_visible_len,
                front_byte: qual_front_byte,
                ..
            },
        ) = (&seq.storage, &qual.storage)
        {
            if seq_stride == qual_stride
                && seq_head_skip == qual_head_skip
                && seq_visible_len == qual_visible_len
            {
                let storage = Storage::FixedLength {
                    stride: *seq_stride,
                    head_skip: *seq_head_skip,
                    visible_len: *seq_visible_len,
                    count: *seq_count,
                    // Entry 0 is now relative to each column's own first byte.
                    front_byte: 0,
                };
                return Ok(DualStringPod {
                    seq: seq.data,
                    qual: qual.data,
                    storage,
                    seq_first_byte: *seq_front_byte as usize,
                    qual_first_byte: *qual_front_byte as usize,
                });
            }
        }

        // General path: validate the per-entry length invariant and the
        // constant-translation invariant in a single pass, building a shared
        // metadata column whose positions are relative to each column's entry-0
        // start. Bytes are never touched — only this positions vec is built.
        let n = seq.len();
        let mut positions: Vec<(u32, u32)> = Vec::with_capacity(n);
        let mut base_seq = 0usize;
        let mut base_qual = 0usize;
        for i in 0..n {
            let sr = seq.storage.entry_range(i);
            let qr = qual.storage.entry_range(i);
            if (sr.end - sr.start) != (qr.end - qr.start) {
                return Err(ColumnError::UnequalLengths);
            }
            if i == 0 {
                base_seq = sr.start;
                base_qual = qr.start;
            } else if (sr.start - base_seq) != (qr.start - base_qual) {
                // Entry starts drift apart — not a constant translation.
                return Err(ColumnError::NotATranslation);
            }
            let rel_start =
                u32::try_from(sr.start - base_seq).expect("entry start exceeds u32::MAX");
            let rel_end = u32::try_from(sr.end - base_seq).expect("entry end exceeds u32::MAX");
            positions.push((rel_start, rel_end));
        }

        let storage = Storage::Variable {
            positions,
            head_skip: 0,
            tail_skip: 0,
            front_skip: 0,
        };
        Ok(DualStringPod {
            seq: seq.data,
            qual: qual.data,
            storage,
            seq_first_byte: base_seq,
            qual_first_byte: base_qual,
        })
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
        BStr::new(&self.seq[r.start + self.seq_first_byte..r.end + self.seq_first_byte])
    }

    /// Quality bytes of entry `i`. Shares the same range as `seq(i)`.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn qual(&self, i: usize) -> &BStr {
        let r = self.storage.entry_range(i);
        BStr::new(&self.qual[r.start + self.qual_first_byte..r.end + self.qual_first_byte])
    }

    /// Both bytes of entry `i` at once.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    #[must_use]
    pub fn pair(&self, i: usize) -> (&BStr, &BStr) {
        let r = self.storage.entry_range(i);
        (
            BStr::new(&self.seq[r.start + self.seq_first_byte..r.end + self.seq_first_byte]),
            BStr::new(&self.qual[r.start + self.qual_first_byte..r.end + self.qual_first_byte]),
        )
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
        if let Some(stride) = self.storage.current_stride() {
            if seq.len() as u64 == u64::from(stride) {
                self.seq.extend_from_slice(seq);
                self.qual.extend_from_slice(qual);
                self.storage.builder_push_strided();
                return;
            }
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
            seq: Arc::new(self.seq),
            qual: Arc::new(self.qual),
            storage: self.storage,
            seq_first_byte: 0,
            qual_first_byte: 0,
        }
    }
}

// ── alias builder ────────────────────────────────────────────────────────

/// Builds a [`DualStringPod`] whose entries reference bytes in an existing
/// pod's `seq` and `qual` `Arc<[u8]>` buffers without copying. Both buffers
/// are pinned for the alias pod's lifetime (snapshot semantics).
pub struct DualStringPodAliasBuilder {
    seq: Arc<Vec<u8>>,
    qual: Arc<Vec<u8>>,
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
                front_skip: 0,
            },
            seq_first_byte: 0,
            qual_first_byte: 0,
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{ColumnError, DualStringPod, DualStringPodBuilder};
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

    // ── from_columns ──────────────────────────────────────────────────────

    use crate::single::{StringPod, StringPodBuilder};

    fn seq_pod(entries: &[&str]) -> StringPod {
        let mut bld = StringPodBuilder::with_capacity(0, entries.len());
        for e in entries {
            bld.push(b(e));
        }
        bld.finish()
    }

    /// Build a `FixedLength`-storage pod (stride = entry length).
    fn fixed_pod(stride: usize, entries: &[&str]) -> StringPod {
        let mut bld = StringPodBuilder::with_capacity(stride, entries.len());
        for e in entries {
            bld.push(b(e));
        }
        bld.finish()
    }

    #[test]
    fn from_columns_zero_copy() {
        let seq = seq_pod(&["ACGT", "TTTT", "GGGG"]);
        let qual = seq_pod(&["IIII", "FFFF", "####"]);
        // Buffers we expect to be reused (no copy) — capture the pointers.
        let seq_ptr = seq.data.as_ptr();
        let qual_ptr = qual.data.as_ptr();
        let dual = DualStringPod::try_from_columns(seq, qual).unwrap();
        assert_eq!(dual.len(), 3);
        assert_eq!(dual.seq(0), BStr::new("ACGT"));
        assert_eq!(dual.qual(0), BStr::new("IIII"));
        assert_eq!(dual.seq(2), BStr::new("GGGG"));
        assert_eq!(dual.qual(2), BStr::new("####"));
        // Zero-copy: the dual pod's buffers are the very same allocations.
        assert_eq!(dual.seq.as_ptr(), seq_ptr);
        assert_eq!(dual.qual.as_ptr(), qual_ptr);
    }

    #[test]
    fn from_columns_fixed_fast_path_keeps_strided() {
        // Both columns FixedLength with identical layout → strided metadata
        // is kept (no positions vec) and bytes are shared.
        let seq = fixed_pod(4, &["ACGT", "TTTT", "GGGG"]);
        let qual = fixed_pod(4, &["IIII", "FFFF", "####"]);
        let seq_ptr = seq.data.as_ptr();
        let qual_ptr = qual.data.as_ptr();
        let dual = DualStringPod::try_from_columns(seq, qual).unwrap();
        assert!(dual.is_fixed_length());
        assert_eq!(dual.seq(1), BStr::new("TTTT"));
        assert_eq!(dual.qual(1), BStr::new("FFFF"));
        assert_eq!(dual.seq.as_ptr(), seq_ptr);
        assert_eq!(dual.qual.as_ptr(), qual_ptr);
    }

    #[test]
    fn from_columns_constant_offset_no_copy() {
        // seq carries a stray leading entry (dropped via pop_front), so its
        // first byte sits 4 bytes past qual's. The shared metadata plus the two
        // per-column offsets must still index both correctly — and without
        // copying any bytes.
        let mut seq = fixed_pod(4, &["XXXX", "ACGT", "TTTT"]);
        seq.pop_front(1); // front_byte now 4; entries "ACGT","TTTT"
        let qual = fixed_pod(4, &["IIII", "FFFF"]); // front_byte 0
        let seq_ptr = seq.data.as_ptr();
        let qual_ptr = qual.data.as_ptr();
        let dual = DualStringPod::try_from_columns(seq, qual).unwrap();
        assert_eq!(dual.len(), 2);
        assert_eq!(dual.seq(0), BStr::new("ACGT"));
        assert_eq!(dual.qual(0), BStr::new("IIII"));
        assert_eq!(dual.seq(1), BStr::new("TTTT"));
        assert_eq!(dual.qual(1), BStr::new("FFFF"));
        // Still zero-copy despite the divergent offsets.
        assert_eq!(dual.seq.as_ptr(), seq_ptr);
        assert_eq!(dual.qual.as_ptr(), qual_ptr);
    }

    #[test]
    fn from_columns_variable_lengths() {
        let seq = seq_pod(&["A", "ACGTACGT", "ACG"]);
        let qual = seq_pod(&["I", "FFFFFFFF", "JJJ"]);
        let dual = DualStringPod::try_from_columns(seq, qual).unwrap();
        let pairs: Vec<(&BStr, &BStr)> = dual.iter().collect();
        assert_eq!(pairs[1].0, BStr::new("ACGTACGT"));
        assert_eq!(pairs[1].1, BStr::new("FFFFFFFF"));
    }

    #[test]
    fn from_columns_entry_length_mismatch_errors() {
        let seq = seq_pod(&["ACGT", "TT"]);
        let qual = seq_pod(&["IIII", "F"]); // entry 1: 2 != 1
        let err = DualStringPod::try_from_columns(seq, qual).unwrap_err();
        assert_eq!(err, ColumnError::UnequalLengths);
        assert!(err.to_string().contains("length mismatch"));
    }

    #[test]
    fn from_columns_count_mismatch_errors() {
        let seq = seq_pod(&["ACGT", "TTTT"]);
        let qual = seq_pod(&["IIII"]);
        let err = DualStringPod::try_from_columns(seq, qual).unwrap_err();
        assert_eq!(err, ColumnError::EqualCountViolated);
    }

    #[test]
    fn from_columns_non_constant_offset_errors() {
        // Equal per-entry lengths, but qual has an orphaned gap (from a drain)
        // so the entry starts drift apart — not a constant translation.
        let seq = seq_pod(&["ACGT", "TTTT"]);
        let mut qual = seq_pod(&["IIII", "ZZZZ", "FFFF"]);
        qual.drain(1..2); // entries "IIII"(0..4), "FFFF"(8..12)
        let err = DualStringPod::try_from_columns(seq, qual).unwrap_err();
        assert_eq!(err, ColumnError::NotATranslation);
    }
}
