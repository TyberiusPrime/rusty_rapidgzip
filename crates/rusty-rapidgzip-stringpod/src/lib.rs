//! Cache-friendly columnar storage for many small byte strings (FASTQ reads,
//! qualities, names, tag values).
//!
//! Two public types:
//! - [`StringPod`]: one column of strings, one `Arc<[u8]>` byte buffer plus
//!   columnar metadata.
//! - [`DualStringPod`]: two byte buffers (e.g. sequence + quality) that share
//!   a single metadata column. Per-entry length invariant is structural.
//!
//! Both are built via a `*Builder` (owning a `Vec<u8>` while pushing) or a
//! `*AliasBuilder` (sharing an existing pod's `Arc<[u8]>` and recording byte
//! ranges into it without copying). The latter exists for tag columns that
//! snapshot subsequences of reads.
//!
//! Storage starts `FixedLength` for the common "all entries equal stride"
//! case and auto-promotes to `Variable` the first time it sees a different
//! length. Promotion is one O(count) cost; afterwards everything stays
//! Variable for the pod's lifetime.
//!
//! `cut_start` / `cut_end` apply as a global head/tail overlay — O(1) on
//! both variants, no bytes are touched. `drain` removes entries but leaves
//! their bytes orphaned in the buffer (rebuild via a new pod if reclamation
//! matters).

mod dual;
mod single;
mod storage;

pub use dual::{ColumnError, DualStringPod, DualStringPodAliasBuilder, DualStringPodBuilder};
pub use single::{StringPod, StringPodAliasBuilder, StringPodBuilder};
