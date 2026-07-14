//! Comparing manifests.
//!
//! Two shapes:
//!
//! - [`PageDiff`] — "what changed between A and B", used for replication, restore, and
//!   telling a human what a commit did.
//! - [`ThreeWayDiff`] — "A and B both descend from base; who touched what", which is the
//!   **input to LoomDB's merge engine** (docs/03 §3.1). It is designed for that consumer.
//!
//! Diffing is cheap because a manifest is just an ordered map of page ids: comparing two
//! databases is comparing two sorted lists of hashes, not reading any data.

use crate::manifest::{ManifestId, PageMap};
use crate::page::{LogicalPageNo, PageId};
use std::collections::BTreeSet;
use std::ops::RangeInclusive;

/// What happened to one logical page between two manifests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageChange {
    /// The page exists in both, with different content.
    Modified {
        /// Content in the "from" manifest.
        from: PageId,
        /// Content in the "to" manifest.
        to: PageId,
    },
    /// The page exists only in the "to" manifest.
    Added(PageId),
    /// The page existed in "from" and is gone in "to".
    Removed(PageId),
}

/// The difference between two manifests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageDiff {
    /// Every changed logical page, in ascending order.
    pub changes: Vec<(LogicalPageNo, PageChange)>,
}

impl PageDiff {
    /// True if the two manifests describe identical data.
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// How many logical pages changed.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// The changed logical pages, coalesced into contiguous ranges.
    ///
    /// Callers that ship bytes (replication, restore) care about ranges, not individual pages:
    /// one request for pages 100–199 beats a hundred requests.
    ///
    /// ```
    /// # use substrate_pager::{PageDiff, PageChange, PageId};
    /// let diff = PageDiff { changes: vec![
    ///     (1, PageChange::Added(PageId::of(b"a"))),
    ///     (2, PageChange::Added(PageId::of(b"b"))),
    ///     (9, PageChange::Added(PageId::of(b"c"))),
    /// ]};
    /// assert_eq!(diff.ranges(), vec![1..=2, 9..=9]);
    /// ```
    pub fn ranges(&self) -> Vec<RangeInclusive<LogicalPageNo>> {
        let mut ranges: Vec<RangeInclusive<LogicalPageNo>> = Vec::new();
        for &(page_no, _) in &self.changes {
            match ranges.last_mut() {
                // Extend the open range if this page is the next one along.
                Some(last) if *last.end() + 1 == page_no => *last = *last.start()..=page_no,
                _ => ranges.push(page_no..=page_no),
            }
        }
        ranges
    }

    /// Compute the difference between two **resolved** page maps.
    ///
    /// Takes maps rather than manifests because a manifest may be an overlay, which knows only what
    /// it changed — diffing two overlays directly would compare their *deltas* rather than their
    /// databases, and quietly report that two identical databases differ.
    pub fn between(from: &PageMap, to: &PageMap) -> PageDiff {
        let page_nos: BTreeSet<LogicalPageNo> = from.keys().chain(to.keys()).copied().collect();

        let mut changes = Vec::new();
        for page_no in page_nos {
            match (from.get(&page_no).copied(), to.get(&page_no).copied()) {
                (Some(a), Some(b)) if a != b => {
                    changes.push((page_no, PageChange::Modified { from: a, to: b }))
                }
                (Some(_), Some(_)) => {} // identical content — content addressing makes this free
                (None, Some(b)) => changes.push((page_no, PageChange::Added(b))),
                (Some(a), None) => changes.push((page_no, PageChange::Removed(a))),
                // Page numbers come from the union of both maps, so at least one side always
                // has the page and this arm is unreachable. We skip rather than `unreachable!()`
                // because library code does not panic (CLAUDE.md rule 6): if the impossible
                // happens, dropping a page from a diff beats killing a process that is holding
                // someone's database open.
                (None, None) => {}
            }
        }
        PageDiff { changes }
    }
}

/// How one logical page relates across a three-way comparison.
///
/// This is the vocabulary LoomDB's merge engine reasons in (docs/03 §3.1), so it names the
/// *situations a merge must decide about*, not the raw byte facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageClass {
    /// Neither branch touched it. Nothing to decide.
    Unchanged,
    /// Only A changed it. Take A's.
    AOnly,
    /// Only B changed it. Take B's.
    BOnly,
    /// Both changed it, to *the same content*. Convergent — take it, no conflict.
    ///
    /// This is not a curiosity: two agents independently deriving the same fact from the same
    /// source produce byte-identical pages, and content addressing detects it for free. Calling
    /// it a conflict would generate an enormous amount of pointless merge work.
    BothSame,
    /// Both changed it, differently. Only the merge policy can decide.
    ///
    /// `None` on a side means that side **deleted** the page. Delete-versus-modify reaches the
    /// policy as a conflict like any other: we will not quietly decide that "delete beats edit"
    /// or the reverse, because in an agent's memory those are very different outcomes.
    Conflict {
        /// Content in the merge base. `None` if neither side's ancestor had the page.
        base: Option<PageId>,
        /// Content on branch A. `None` if A deleted it.
        a: Option<PageId>,
        /// Content on branch B. `None` if B deleted it.
        b: Option<PageId>,
    },
}

/// The classification of every logical page across branches A and B against their merge base.
///
/// Consumed by LoomDB's merge engine, which applies typed rules (additive types merge
/// arithmetically, temporal facts resolve by validity and provenance rank, everything else
/// goes to a `MergePolicy` callback).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreeWayDiff {
    /// The merge base both branches descend from.
    pub base: ManifestId,
    /// Branch A.
    pub a: ManifestId,
    /// Branch B.
    pub b: ManifestId,
    /// Every logical page that either branch touched, and how. Ascending order.
    ///
    /// Unchanged pages are **omitted** — a merge of a 1 TiB database where two agents each
    /// touched three pages must produce three entries, not sixteen million.
    pub entries: Vec<(LogicalPageNo, PageClass)>,
}

impl ThreeWayDiff {
    /// Classify A and B against their merge base.
    pub fn compute(
        base_id: ManifestId,
        a_id: ManifestId,
        b_id: ManifestId,
        base: &PageMap,
        a: &PageMap,
        b: &PageMap,
    ) -> ThreeWayDiff {
        let page_nos: BTreeSet<LogicalPageNo> = base
            .keys()
            .chain(a.keys())
            .chain(b.keys())
            .copied()
            .collect();

        let mut entries = Vec::new();
        for page_no in page_nos {
            let (base_p, a_p, b_p) = (
                base.get(&page_no).copied(),
                a.get(&page_no).copied(),
                b.get(&page_no).copied(),
            );

            // Four cases. That is genuinely all there is, and writing it as anything cleverer
            // than four cases would be a disservice to whoever debugs a bad merge at 3am.
            let class = if a_p == b_p {
                // Both sides agree on the content.
                if a_p == base_p {
                    continue; // nobody touched it — omit entirely
                }
                PageClass::BothSame // both made the *same* change: convergence, not conflict
            } else if a_p == base_p {
                PageClass::BOnly // A never moved, so B is the only author
            } else if b_p == base_p {
                PageClass::AOnly // B never moved, so A is the only author
            } else {
                // Both moved, and they disagree.
                PageClass::Conflict {
                    base: base_p,
                    a: a_p,
                    b: b_p,
                }
            };
            entries.push((page_no, class));
        }

        ThreeWayDiff {
            base: base_id,
            a: a_id,
            b: b_id,
            entries,
        }
    }

    /// Every page that needs a decision from the merge policy.
    pub fn conflicts(&self) -> impl Iterator<Item = (LogicalPageNo, PageClass)> + '_ {
        self.entries
            .iter()
            .copied()
            .filter(|(_, class)| matches!(class, PageClass::Conflict { .. }))
    }

    /// True if this merge can proceed without any policy decision.
    pub fn is_clean(&self) -> bool {
        self.conflicts().next().is_none()
    }

    /// How many pages either branch touched.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if neither branch changed anything.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn m(pages: &[(LogicalPageNo, &[u8])]) -> PageMap {
        pages
            .iter()
            .map(|(no, bytes)| (*no, PageId::of(bytes)))
            .collect()
    }

    fn ids() -> (ManifestId, ManifestId, ManifestId) {
        (
            ManifestId::from_bytes([0; 32]),
            ManifestId::from_bytes([1; 32]),
            ManifestId::from_bytes([2; 32]),
        )
    }

    #[test]
    fn diff_reports_add_modify_remove() {
        let from = m(&[(0, b"a"), (1, b"b")]);
        let to = m(&[(0, b"a"), (1, b"b-changed"), (2, b"c")]);
        let diff = PageDiff::between(&from, &to);

        assert_eq!(diff.len(), 2, "page 0 is unchanged and must not appear");
        assert_eq!(
            diff.changes[0],
            (
                1,
                PageChange::Modified {
                    from: PageId::of(b"b"),
                    to: PageId::of(b"b-changed")
                }
            )
        );
        assert_eq!(diff.changes[1], (2, PageChange::Added(PageId::of(b"c"))));

        let back = PageDiff::between(&to, &from);
        assert_eq!(back.changes[1], (2, PageChange::Removed(PageId::of(b"c"))));
    }

    #[test]
    fn identical_manifests_diff_to_nothing() {
        let a = m(&[(0, b"x"), (4, b"y")]);
        assert!(PageDiff::between(&a, &a.clone()).is_empty());
    }

    #[test]
    fn ranges_coalesce_contiguous_pages() {
        let from = m(&[]);
        let to = m(&[(1, b"a"), (2, b"b"), (3, b"c"), (10, b"d"), (11, b"e")]);
        assert_eq!(PageDiff::between(&from, &to).ranges(), vec![1..=3, 10..=11]);
    }

    #[test]
    fn three_way_classifies_every_case() {
        let (bi, ai, b_i) = ids();
        let base = m(&[(0, b"same"), (1, b"base"), (2, b"base"), (3, b"base")]);
        let a = m(&[
            (0, b"same"),       // untouched by both
            (1, b"a-edit"),     // A only
            (2, b"base"),       // untouched by A
            (3, b"conflict-a"), // both edited, differently
            (4, b"convergent"), // both added the same content
        ]);
        let b = m(&[
            (0, b"same"),
            (1, b"base"),
            (2, b"b-edit"), // B only
            (3, b"conflict-b"),
            (4, b"convergent"),
        ]);

        let diff = ThreeWayDiff::compute(bi, ai, b_i, &base, &a, &b);
        let classes: BTreeMap<_, _> = diff.entries.iter().copied().collect();

        assert!(!classes.contains_key(&0), "unchanged pages must be omitted");
        assert_eq!(classes[&1], PageClass::AOnly);
        assert_eq!(classes[&2], PageClass::BOnly);
        assert!(matches!(classes[&3], PageClass::Conflict { .. }));
        assert_eq!(
            classes[&4],
            PageClass::BothSame,
            "two agents deriving the same fact is convergence, not conflict"
        );

        assert!(!diff.is_clean());
        assert_eq!(diff.conflicts().count(), 1);
    }

    #[test]
    fn a_merge_where_only_one_side_moved_is_clean() {
        let (bi, ai, b_i) = ids();
        let base = m(&[(0, b"base")]);
        let a = m(&[(0, b"a-edit")]);
        let b = base.clone(); // B never moved

        let diff = ThreeWayDiff::compute(bi, ai, b_i, &base, &a, &b);
        assert!(diff.is_clean());
        assert_eq!(diff.entries, vec![(0, PageClass::AOnly)]);
    }

    #[test]
    fn delete_versus_modify_is_a_conflict_we_refuse_to_guess() {
        let (bi, ai, b_i) = ids();
        let base = m(&[(0, b"base")]);
        let a = m(&[]); // A deleted the page
        let b = m(&[(0, b"b-edit")]); // B edited it

        let diff = ThreeWayDiff::compute(bi, ai, b_i, &base, &a, &b);
        assert_eq!(diff.conflicts().count(), 1);
    }

    #[test]
    fn a_huge_database_with_a_tiny_change_produces_a_tiny_diff() {
        // The property that makes merge tractable at all: cost is proportional to what changed,
        // not to how much data exists.
        let pages: Vec<(LogicalPageNo, &[u8])> =
            (0..10_000).map(|i| (i, b"cold" as &[u8])).collect();
        let base = m(&pages);
        let mut a = base.clone();
        a.insert(4_242, PageId::of(b"one hot page"));
        let b = base.clone();

        let (bi, ai, b_i) = ids();
        let diff = ThreeWayDiff::compute(bi, ai, b_i, &base, &a, &b);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff.entries[0].0, 4_242);
    }
}
