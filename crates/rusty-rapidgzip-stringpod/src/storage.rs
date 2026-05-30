use std::ops::Range;

/// Columnar storage layout. Crate-private; pods and builders own one of these
/// and the corresponding byte buffer(s).
#[derive(Debug, Clone)]
pub(crate) enum Storage {
    /// All entries share a stride; per-entry offsets are implicit (`i * stride`).
    /// `head_skip` and `visible_len` form the global cut overlay, so cuts are
    /// O(1) — the stride itself never changes.
    FixedLength {
        stride: u32,
        head_skip: u32,
        visible_len: u32,
        count: u32,
    },
    /// Sparse `(start, stop)` positions into the byte buffer. Supports both
    /// contiguous (push-built) and non-contiguous (alias-built) entries.
    /// `head_skip` and `tail_skip` form the cut overlay, applied per entry
    /// with saturation against the entry's own length.
    Variable {
        positions: Vec<(u32, u32)>,
        head_skip: u32,
        tail_skip: u32,
    },
}

impl Storage {
    pub(crate) fn new_fixed(stride: u32, count_capacity: usize) -> Self {
        let _ = count_capacity; // positions Vec doesn't exist yet
        Storage::FixedLength {
            stride,
            head_skip: 0,
            visible_len: stride,
            count: 0,
        }
    }

    pub(crate) fn new_variable(count_capacity: usize) -> Self {
        Storage::Variable {
            positions: Vec::with_capacity(count_capacity),
            head_skip: 0,
            tail_skip: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Storage::FixedLength { count, .. } => *count as usize,
            Storage::Variable { positions, .. } => positions.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The byte range of entry `i` after the cut overlay is applied.
    ///
    /// # Panics
    /// If `i >= self.len()`.
    pub(crate) fn entry_range(&self, i: usize) -> Range<usize> {
        match *self {
            Storage::FixedLength {
                stride,
                head_skip,
                visible_len,
                count,
            } => {
                assert!(i < count as usize, "StringPod index {i} out of bounds");
                let base = (i as u64).wrapping_mul(u64::from(stride));
                let start = base.saturating_add(u64::from(head_skip));
                let stop = start.saturating_add(u64::from(visible_len));
                let start_u =
                    usize::try_from(start).expect("entry start exceeds usize on this platform");
                let stop_u =
                    usize::try_from(stop).expect("entry stop exceeds usize on this platform");
                start_u..stop_u
            }
            Storage::Variable {
                ref positions,
                head_skip,
                tail_skip,
            } => {
                let (raw_start, raw_stop) = positions[i];
                let entry_len = raw_stop.saturating_sub(raw_start);
                let head = head_skip.min(entry_len);
                let remaining = entry_len - head;
                let tail = tail_skip.min(remaining);
                let start = raw_start.saturating_add(head);
                let stop = raw_stop.saturating_sub(tail);
                (start as usize)..(stop as usize)
            }
        }
    }

    pub(crate) fn entry_len(&self, i: usize) -> usize {
        let r = self.entry_range(i);
        r.end - r.start
    }

    pub(crate) fn cut_start(&mut self, n: u32) {
        match self {
            Storage::FixedLength {
                stride,
                head_skip,
                visible_len,
                ..
            } => {
                let new_head = (*head_skip).saturating_add(n).min(*stride);
                let delta = new_head - *head_skip;
                *head_skip = new_head;
                *visible_len = visible_len.saturating_sub(delta);
            }
            Storage::Variable { head_skip, .. } => {
                *head_skip = head_skip.saturating_add(n);
            }
        }
    }

    pub(crate) fn cut_end(&mut self, n: u32) {
        match self {
            Storage::FixedLength { visible_len, .. } => {
                *visible_len = visible_len.saturating_sub(n);
            }
            Storage::Variable { tail_skip, .. } => {
                *tail_skip = tail_skip.saturating_add(n);
            }
        }
    }

    /// Sum of visible bytes across all entries (what a tight rebuild would need).
    pub(crate) fn used_bytes(&self) -> usize {
        match *self {
            Storage::FixedLength {
                visible_len, count, ..
            } => (visible_len as usize) * (count as usize),
            Storage::Variable {
                ref positions,
                head_skip,
                tail_skip,
            } => positions
                .iter()
                .map(|&(s, e)| {
                    let entry_len = e.saturating_sub(s);
                    let head = head_skip.min(entry_len);
                    let rem = entry_len - head;
                    let tail = tail_skip.min(rem);
                    (entry_len - head - tail) as usize
                })
                .sum(),
        }
    }

    /// Materialise per-entry positions and drop the `FixedLength` layout. The
    /// current cut overlay (`head_skip`, `visible_len`) is baked into the
    /// positions so the resulting `Variable` storage has `head_skip = 0` and
    /// `tail_skip = 0`. No-op if already `Variable`.
    pub(crate) fn promote_to_variable(&mut self) {
        if let Storage::FixedLength {
            stride,
            head_skip,
            visible_len,
            count,
        } = *self
        {
            let mut positions = Vec::with_capacity(count as usize);
            for i in 0..count {
                let base = i.wrapping_mul(stride);
                let start = base.saturating_add(head_skip);
                let stop = start.saturating_add(visible_len);
                positions.push((start, stop));
            }
            *self = Storage::Variable {
                positions,
                head_skip: 0,
                tail_skip: 0,
            };
        }
    }

    /// Drain a range of entries. Promotes `FixedLength` to `Variable` first
    /// (the orphaned bytes stay in the buffer).
    ///
    /// # Panics
    /// If the range is invalid.
    pub(crate) fn drain(&mut self, range: Range<usize>) {
        if range.start == range.end {
            return;
        }
        self.promote_to_variable();
        match self {
            Storage::Variable { positions, .. } => {
                positions.drain(range);
            }
            Storage::FixedLength { .. } => {
                // cov:excl-start
                unreachable!("just promoted to Variable")
                // cov:excl-stop
            }
        }
    }

    // ── builder-side helpers ──────────────────────────────────────────────

    /// Returns the current stride if `FixedLength`, else None.
    pub(crate) fn current_stride(&self) -> Option<u32> {
        match *self {
            Storage::FixedLength { stride, .. } => Some(stride),
            Storage::Variable { .. } => None,
        }
    }

    /// Append metadata for a new entry assumed to be stride-sized.
    /// Caller must have verified bytes match `current_stride()`.
    ///
    /// # Panics
    /// If storage is not `FixedLength`.
    pub(crate) fn builder_push_strided(&mut self) {
        match self {
            Storage::FixedLength { count, .. } => {
                *count = count
                    .checked_add(1)
                    .expect("StringPod count exceeded u32::MAX");
            }
            Storage::Variable { .. } => panic!("builder_push_strided on Variable storage"),
        }
    }

    /// Append metadata for a new entry at `(start, stop)` in the byte buffer.
    /// Promotes `FixedLength` to `Variable` if necessary.
    ///
    /// # Panics
    /// If `start > stop` or values exceed u32.
    pub(crate) fn builder_push_position(&mut self, start: u32, stop: u32) {
        assert!(start <= stop, "start {start} > stop {stop}");
        self.promote_to_variable();
        match self {
            Storage::Variable { positions, .. } => positions.push((start, stop)),
            Storage::FixedLength { .. } => {
                // cov:excl-start
                unreachable!("just promoted")
                // cov:excl-stop
            }
        }
    }
}
