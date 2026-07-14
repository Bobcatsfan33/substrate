//! Integrity scrubbing: finding corruption before a customer does.
//!
//! # The problem this solves
//!
//! Every read verifies its page (docs/02 §3.1), so corruption is never *served*. But a page that is
//! never read is never checked — and the pages nobody reads are exactly the pages that sit on a disk
//! for two years quietly rotting. You discover them on the day you finally need them, which is
//! usually the day something else has already gone wrong.
//!
//! So: walk the store, re-hash everything, and say what you find.
//!
//! # What a scrub does not do
//!
//! **It does not repair.** [`Scrubber`] produces a [`CorruptionReport`] and stops.
//!
//! That is a deliberate layering decision. `substrate-pager` cannot repair a page, because repair
//! means fetching a healthy copy from object storage, and the pager does not know object storage
//! exists (CLAUDE.md rule 2). `substrate-store` consumes the report and does the repair — it has the
//! healthy replica, and content addressing means it can *prove* the replacement is correct before
//! installing it.
//!
//! The alternative — a pager that "fixes" corruption itself — would mean the lowest, most
//! safety-critical layer of the engine deciding, on its own, to overwrite data it has just proven it
//! cannot read. That is not a repair. That is a second corruption event with better manners.

use crate::cas::Cas;
use crate::error::Result;
use crate::manifest::{ManifestId, ManifestStore};
use crate::metrics::Metrics;
use crate::page::{PageId, PageIdSet};
use std::sync::Arc;

/// What is wrong with a store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CorruptionReport {
    /// Pages whose bytes no longer hash to their id. **Bit rot, a failing disk, or tampering.**
    ///
    /// These are still *referenced* by a live manifest, which means the database needs them and
    /// cannot read them. Every one of these is a page of somebody's data.
    pub corrupt: Vec<PageId>,

    /// Pages a live manifest references that are **not in the store at all**.
    ///
    /// In a healthy engine this list is impossible: GC refuses to collect a referenced page, and the
    /// tiering layer refuses to evict a page it has not confirmed durable. A non-empty list here
    /// means something outside substrate deleted data, or one of those two guarantees has a bug —
    /// and either way it is the most serious thing this engine can report.
    pub missing: Vec<PageId>,

    /// Manifests that could not be read or did not verify.
    pub bad_manifests: Vec<ManifestId>,

    /// Pages checked and found healthy.
    pub healthy: u64,

    /// Pages present in the store that no live manifest references.
    ///
    /// Not a problem — this is garbage, and GC's job. Reported because "why is my disk full" deserves
    /// a number rather than a shrug.
    pub unreferenced: u64,
}

impl CorruptionReport {
    /// True if the store is intact.
    pub fn is_healthy(&self) -> bool {
        self.corrupt.is_empty() && self.missing.is_empty() && self.bad_manifests.is_empty()
    }

    /// How many objects are damaged.
    pub fn damaged(&self) -> usize {
        self.corrupt.len() + self.missing.len() + self.bad_manifests.len()
    }
}

impl std::fmt::Display for CorruptionReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_healthy() {
            return write!(
                f,
                "healthy: {} pages verified, {} unreferenced",
                self.healthy, self.unreferenced
            );
        }
        write!(
            f,
            "DAMAGED: {} corrupt page(s), {} missing page(s), {} bad manifest(s); \
             {} pages verified healthy",
            self.corrupt.len(),
            self.missing.len(),
            self.bad_manifests.len(),
            self.healthy
        )
    }
}

/// Re-verifies a store's integrity.
///
/// Designed to be run continuously in the background, a slice at a time — a scrub that saturates the
/// disk is a scrub an operator turns off, and a scrub that is turned off is not a scrub.
pub struct Scrubber {
    cas: Arc<dyn Cas>,
    manifests: Arc<dyn ManifestStore>,
    metrics: Arc<dyn Metrics>,
}

impl Scrubber {
    /// Build a scrubber over a store's CAS and manifests.
    pub fn new(
        cas: Arc<dyn Cas>,
        manifests: Arc<dyn ManifestStore>,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        Scrubber {
            cas,
            manifests,
            metrics,
        }
    }

    /// Verify every page reachable from these manifests, and every manifest in the chain.
    ///
    /// `live` is the same set GC takes: branch heads and tags. Pages nobody references are counted,
    /// not verified — spending disk bandwidth checksumming garbage would be a strange thing to do.
    pub fn scrub(&self, live: &[ManifestId], resolved: &[PageIdSet]) -> Result<CorruptionReport> {
        let mut report = CorruptionReport::default();

        // Manifests first: an unreadable manifest means we do not even know which pages matter.
        for id in live {
            if self.manifests.get(*id).is_err() {
                report.bad_manifests.push(*id);
            }
        }

        let referenced: PageIdSet = resolved.iter().flatten().copied().collect();

        for page_id in &referenced {
            match self.cas.get(*page_id) {
                Ok(_) => report.healthy += 1,
                Err(e) if e.is_corruption() => {
                    // `is_corruption` covers both "the bytes are wrong" and "the bytes are gone",
                    // and the difference matters to whoever has to fix it.
                    match self.cas.contains(*page_id) {
                        Ok(true) => {
                            report.corrupt.push(*page_id);
                            self.metrics.corruption_detected(*page_id);
                        }
                        _ => report.missing.push(*page_id),
                    }
                }
                // Any other error (a transient i/o failure, a permissions problem) is not evidence
                // of corruption, and reporting it as such would send an operator hunting a disk
                // fault that does not exist.
                Err(e) => return Err(e),
            }
        }

        for page_id in self.cas.list()? {
            if !referenced.contains(&page_id) {
                report.unreferenced += 1;
            }
        }

        Ok(report)
    }
}

impl std::fmt::Debug for Scrubber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scrubber").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::{FsCas, MemCas};
    use crate::manifest::MemManifestStore;
    use crate::metrics::NoMetrics;
    use crate::page::{Page, PageHasher, DEFAULT_PAGE_SIZE};

    #[test]
    fn a_healthy_store_reports_healthy() -> Result<()> {
        let cas: Arc<dyn Cas> = Arc::new(MemCas::new(PageHasher::Unkeyed));
        let manifests: Arc<dyn ManifestStore> = Arc::new(MemManifestStore::new());

        let page = Page::new(
            &PageHasher::Unkeyed,
            b"good bytes".to_vec(),
            DEFAULT_PAGE_SIZE,
        )?;
        cas.put(&page)?;

        let scrubber = Scrubber::new(cas, manifests, Arc::new(NoMetrics));
        let referenced: PageIdSet = [page.id()].into_iter().collect();
        let report = scrubber.scrub(&[], &[referenced])?;

        assert!(report.is_healthy());
        assert_eq!(report.healthy, 1);
        assert_eq!(report.unreferenced, 0);
        Ok(())
    }

    #[test]
    fn rot_on_disk_is_found_even_though_nobody_read_the_page() -> Result<()> {
        // The whole point of scrubbing: this page is never read by anyone. Without a scrub, its
        // corruption is discovered on the day it is finally needed, which is always a bad day.
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = Arc::new(FsCas::open(dir.path(), PageHasher::Unkeyed)?);

        let page = Page::new(
            &PageHasher::Unkeyed,
            b"honest bytes".to_vec(),
            DEFAULT_PAGE_SIZE,
        )?;
        cas.put(&page)?;

        // Rot it, behind the engine's back.
        let hex = page.id().to_hex();
        let path = dir
            .path()
            .join("pages")
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(&hex);
        std::fs::write(&path, b"tampered!!!!").expect("corrupt the page");

        let scrubber = Scrubber::new(cas, Arc::new(MemManifestStore::new()), Arc::new(NoMetrics));
        let referenced: PageIdSet = [page.id()].into_iter().collect();
        let report = scrubber.scrub(&[], &[referenced])?;

        assert!(!report.is_healthy());
        assert_eq!(report.corrupt, vec![page.id()]);
        assert_eq!(report.healthy, 0);
        assert!(report.to_string().contains("DAMAGED"));
        Ok(())
    }

    #[test]
    fn a_referenced_page_that_is_simply_gone_is_reported_as_missing_not_corrupt() -> Result<()> {
        // These are different failures with different causes and different fixes. Conflating them
        // sends an operator looking for a failing disk when what they have is a GC bug.
        let cas: Arc<dyn Cas> = Arc::new(MemCas::new(PageHasher::Unkeyed));
        let scrubber = Scrubber::new(cas, Arc::new(MemManifestStore::new()), Arc::new(NoMetrics));

        let absent = PageId::of(b"never stored");
        let referenced: PageIdSet = [absent].into_iter().collect();
        let report = scrubber.scrub(&[], &[referenced])?;

        assert_eq!(report.missing, vec![absent]);
        assert!(report.corrupt.is_empty());
        Ok(())
    }

    #[test]
    fn garbage_is_counted_not_verified() -> Result<()> {
        let cas: Arc<dyn Cas> = Arc::new(MemCas::new(PageHasher::Unkeyed));

        let live = Page::new(&PageHasher::Unkeyed, b"live".to_vec(), DEFAULT_PAGE_SIZE)?;
        let junk = Page::new(&PageHasher::Unkeyed, b"junk".to_vec(), DEFAULT_PAGE_SIZE)?;
        cas.put(&live)?;
        cas.put(&junk)?;

        let scrubber = Scrubber::new(cas, Arc::new(MemManifestStore::new()), Arc::new(NoMetrics));
        let referenced: PageIdSet = [live.id()].into_iter().collect();
        let report = scrubber.scrub(&[], &[referenced])?;

        assert!(report.is_healthy());
        assert_eq!(report.healthy, 1);
        assert_eq!(
            report.unreferenced, 1,
            "the junk page is garbage, not damage"
        );
        Ok(())
    }
}
