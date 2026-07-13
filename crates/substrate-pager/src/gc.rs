//! Garbage collection.
//!
//! A page is live if any reachable manifest references it, or if an in-flight transaction has
//! staged it. Everything else is swept.
//!
//! # The rule that keeps this safe
//!
//! **Liveness is recomputed from the manifests, every time. There is no refcount file, and there
//! never will be** (CLAUDE.md rule 9).
//!
//! A refcount file is a second source of truth about which bytes are alive. When it disagrees
//! with the manifests — and eventually it will, because a crash lands between the decrement and
//! the fsync — the disagreement is resolved in favour of deleting live data, and the corruption
//! is discovered months later by a customer. Recomputing is slower. It is also incapable of that
//! failure, and this is a trade we make gladly.
//!
//! # Interruption is normal
//!
//! GC computes the entire live set *before* deleting anything. So an interrupted sweep leaves
//! garbage behind, which the next sweep collects. It cannot leave a live page deleted, because
//! by the time any deletion happens the live set is already known and complete.

/// What a sweep did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Pages deleted because no reachable manifest referenced them.
    pub pages_swept: u64,
    /// Pages kept because something still points at them.
    pub pages_retained: u64,
    /// Manifests deleted because they were not reachable from any live root.
    ///
    /// These are typically the abandoned suffix of a rewind — the hypothesis an agent tried and
    /// discarded.
    pub manifests_swept: u64,
    /// Manifests kept: the live roots and their ancestors.
    pub manifests_retained: u64,
}

impl GcStats {
    /// True if the sweep found nothing to collect.
    pub fn is_clean(&self) -> bool {
        self.pages_swept == 0 && self.manifests_swept == 0
    }

    /// Total objects removed.
    pub fn swept(&self) -> u64 {
        self.pages_swept + self.manifests_swept
    }
}

impl std::fmt::Display for GcStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "swept {} pages ({} retained), {} manifests ({} retained)",
            self.pages_swept, self.pages_retained, self.manifests_swept, self.manifests_retained
        )
    }
}
