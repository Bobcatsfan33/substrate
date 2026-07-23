//! The **warm set**: the object ids a session actually faulted from object storage, remembered so the
//! *next* wake can fetch them in one concurrent batch instead of pointer-chasing them serially.
//!
//! It is deliberately **bounded** — a working-set hint, not a whole-database manifest. Once the cap is
//! reached it stops recording; the point is to cheaply prefetch the handful of objects a wake needs,
//! and a hint that grew without limit would turn a lazy wake back into eager hydration, which is the
//! exact thing tiering exists to avoid.
//!
//! It records *faults* (objects that had to come from the remote), because those — and only those —
//! are what a cold wake will have to fetch again. A local hit is already resident and costs nothing to
//! wake. The set is a **hint**: it is content-addressed downstream, so a stale entry can never produce
//! a wrong byte — at worst it prefetches an object the read does not end up needing (a wasted GET,
//! never a wrong one), which is why applying it needs no validation step.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Mutex;
use substrate_pager::{ManifestId, PageId};

/// A bounded, deduplicated record of the manifest and page objects a session faulted.
pub struct HotSet {
    inner: Mutex<Inner>,
    /// The most ids of EACH kind the set will hold. A working-set bound, not a database size.
    cap: usize,
}

#[derive(Default)]
struct Inner {
    pages: Vec<PageId>,
    page_seen: HashSet<PageId>,
    manifests: Vec<ManifestId>,
    manifest_seen: HashSet<ManifestId>,
    /// Whether the cap was ever hit — so a caller can tell a *complete* working set from a truncated one.
    saturated: bool,
}

impl HotSet {
    /// A new, empty warm-set recorder bounded at `cap` ids per kind.
    pub fn new(cap: usize) -> Self {
        HotSet {
            inner: Mutex::new(Inner::default()),
            cap,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock means a thread panicked mid-record; the vectors are still a valid list of
        // ids (a push is atomic w.r.t. the lock), so recover rather than propagate a panic.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Note that a page object was faulted. Deduped; dropped past the cap.
    pub fn record_page(&self, id: PageId) {
        let mut inner = self.lock();
        if inner.pages.len() >= self.cap {
            inner.saturated = true;
            return;
        }
        if inner.page_seen.insert(id) {
            inner.pages.push(id);
        }
    }

    /// Note that a manifest object was faulted. Deduped; dropped past the cap.
    pub fn record_manifest(&self, id: ManifestId) {
        let mut inner = self.lock();
        if inner.manifests.len() >= self.cap {
            inner.saturated = true;
            return;
        }
        if inner.manifest_seen.insert(id) {
            inner.manifests.push(id);
        }
    }

    /// A snapshot of the warm set so far — what `sleep()` persists into the wake token.
    pub fn snapshot(&self) -> HotSetSnapshot {
        let inner = self.lock();
        HotSetSnapshot {
            manifests: inner.manifests.clone(),
            pages: inner.pages.clone(),
        }
    }

    /// Whether the cap was ever hit — the working set was larger than the bound, so the snapshot is a
    /// (still-useful) prefix, not the whole set.
    pub fn saturated(&self) -> bool {
        self.lock().saturated
    }
}

/// The serializable warm set carried in a [`WakeToken`](crate::WakeToken).
///
/// Manifests first, then pages — the order the speculative wake wants them: the manifest chain resolves
/// what the pages *mean*, and both are fetched in one concurrent batch regardless, but keeping the
/// grouping explicit makes the two `get_batch` calls obvious.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotSetSnapshot {
    /// The overlay/history manifest chain the last wake resolved.
    #[serde(default)]
    pub manifests: Vec<ManifestId>,
    /// The pages the last wake's reads faulted.
    #[serde(default)]
    pub pages: Vec<PageId>,
}

impl HotSetSnapshot {
    /// Nothing to prefetch — an old token (no warm set) or a wake that faulted nothing.
    pub fn is_empty(&self) -> bool {
        self.manifests.is_empty() && self.pages.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(n: u8) -> PageId {
        PageId::of(&[n; 16])
    }

    #[test]
    fn records_deduped_and_ordered() {
        let hs = HotSet::new(64);
        hs.record_page(pid(3));
        hs.record_page(pid(1));
        hs.record_page(pid(3)); // dup
        let snap = hs.snapshot();
        assert_eq!(snap.pages, vec![pid(3), pid(1)]);
        assert!(!hs.saturated());
    }

    #[test]
    fn bounded_at_cap_and_reports_saturation() {
        let hs = HotSet::new(2);
        hs.record_page(pid(1));
        hs.record_page(pid(2));
        hs.record_page(pid(3)); // over cap → dropped
        assert_eq!(hs.snapshot().pages.len(), 2);
        assert!(hs.saturated());
    }

    #[test]
    fn snapshot_of_empty_is_empty() {
        assert!(HotSet::new(8).snapshot().is_empty());
    }
}
