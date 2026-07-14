//! Differential testing against a model oracle.
//!
//! # Why this file exists
//!
//! This storage engine was written quickly. That is a legitimate reason for anyone about to put
//! their data in it to be suspicious, and the answer to suspicion is not a README full of
//! adjectives — it is a **second implementation**, so naive that it is obviously correct, which
//! the real engine is tested against under randomized operation sequences.
//!
//! The model ([`substrate_pager::model::ModelStore`]) copies the entire database on every fork
//! and every snapshot. It is absurdly slow and completely, boringly right. The real engine does
//! the same thing in O(1) with content-addressed manifests. If the two ever disagree about what
//! a database contains, one of them is wrong — and we find out here, in milliseconds, rather
//! than in a customer's incident channel eight months from now.
//!
//! The four properties P1 must hold, all asserted below:
//!
//! 1. **Fork isolation** — a write to a fork is never visible in the base.
//! 2. **Snapshot immutability** — a snapshot never changes, no matter what happens afterwards.
//! 3. **Diff correctness** — the diff of two manifests names exactly the pages that differ.
//! 4. **GC never collects a live page** — the one bug in this crate that would be unrecoverable.

use proptest::prelude::*;
use std::collections::BTreeMap;
use substrate_pager::model::{ModelSnapshotId, ModelSnapshots, ModelStore};
use substrate_pager::{
    LogicalPageNo, ManifestId, PageStore, Pager, PagerError, StoreConfig, MIN_PAGE_SIZE,
};

/// Small pages and a small address space, so random sequences actually collide on the same
/// pages — which is where the interesting bugs live. A 64 KiB page and a u64 address space
/// would let a random walk touch every page exactly once and prove nothing.
const PAGE_SIZE: usize = MIN_PAGE_SIZE;
const MAX_PAGE_NO: LogicalPageNo = 8;

fn config() -> StoreConfig {
    StoreConfig {
        page_size: PAGE_SIZE,
        ..Default::default()
    }
}

/// One step in a randomized session.
#[derive(Clone, Debug)]
enum Op {
    Write {
        store: usize,
        page_no: LogicalPageNo,
        content: u8,
    },
    Remove {
        store: usize,
        page_no: LogicalPageNo,
    },
    Snapshot {
        store: usize,
    },
    /// Fork from a previously taken snapshot, creating a new store.
    Fork {
        snapshot: usize,
    },
    Rewind {
        store: usize,
        snapshot: usize,
    },
    /// Sweep, with every store head and every snapshot as a live root.
    Gc,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0usize..4, 0..MAX_PAGE_NO, any::<u8>())
            .prop_map(|(store, page_no, content)| Op::Write { store, page_no, content }),
        1 => (0usize..4, 0..MAX_PAGE_NO)
            .prop_map(|(store, page_no)| Op::Remove { store, page_no }),
        2 => (0usize..4).prop_map(|store| Op::Snapshot { store }),
        2 => (0usize..8).prop_map(|snapshot| Op::Fork { snapshot }),
        1 => (0usize..4, 0usize..8).prop_map(|(store, snapshot)| Op::Rewind { store, snapshot }),
        1 => Just(Op::Gc),
    ]
}

/// Page content is derived from a single byte so the model and the engine agree trivially on
/// what "the same bytes" means. The length varies so we are not only ever testing one size.
fn content_of(byte: u8) -> Vec<u8> {
    vec![byte; 1 + (byte as usize % 64)]
}

/// The real engine and the model, run in lockstep.
struct World {
    real: Vec<Box<dyn PageStore>>,
    model: Vec<ModelStore>,
    /// The model's snapshot table — global and shared, exactly as the engine's manifests are.
    model_snaps: ModelSnapshots,
    /// Snapshots, paired: the engine's `ManifestId` and the model's handle.
    snapshots: Vec<(ManifestId, ModelSnapshotId)>,
}

impl World {
    fn new() -> Result<Self, PagerError> {
        let root = Pager::in_memory(config())?;
        Ok(World {
            real: vec![Box::new(root)],
            model: vec![ModelStore::new()],
            model_snaps: ModelSnapshots::new(),
            snapshots: Vec::new(),
        })
    }

    fn apply(&mut self, op: &Op) -> Result<(), PagerError> {
        match *op {
            Op::Write {
                store,
                page_no,
                content,
            } => {
                let Some(idx) = self.store_idx(store) else {
                    return Ok(());
                };
                let bytes = content_of(content);
                let mut txn = self.real[idx].begin()?;
                self.real[idx].write(&mut txn, page_no, bytes.clone())?;
                self.real[idx].commit(txn)?;
                self.model[idx].write(page_no, bytes);
            }
            Op::Remove { store, page_no } => {
                let Some(idx) = self.store_idx(store) else {
                    return Ok(());
                };
                let mut txn = self.real[idx].begin()?;
                self.real[idx].remove(&mut txn, page_no)?;
                self.real[idx].commit(txn)?;
                self.model[idx].remove(page_no);
            }
            Op::Snapshot { store } => {
                let Some(idx) = self.store_idx(store) else {
                    return Ok(());
                };
                let real = self.real[idx].snapshot()?;
                let model = self.model_snaps.take(self.model[idx].head());
                self.snapshots.push((real, model));
            }
            Op::Fork { snapshot } => {
                let Some((real_snap, model_snap)) = self.snapshot_at(snapshot) else {
                    return Ok(());
                };
                let Some(state) = self.model_snaps.get(model_snap) else {
                    return Ok(());
                };
                // Fork from store 0: every store in this world shares one CAS, which is what
                // makes the GC property below meaningful.
                let forked_real = self.real[0].fork(&real_snap)?;
                let forked_model = ModelStore::forked_from(state);
                self.real.push(forked_real);
                self.model.push(forked_model);
            }
            Op::Rewind { store, snapshot } => {
                let (Some(idx), Some((real_snap, model_snap))) =
                    (self.store_idx(store), self.snapshot_at(snapshot))
                else {
                    return Ok(());
                };
                // Any manifest is a legal rewind target for any store sharing the CAS — the
                // engine's manifests are globally addressable and immutable, so this is
                // `git reset --hard <any commit>`, including one made on a sibling branch. The
                // model must offer the same, which is why its snapshot table is shared.
                let Some(state) = self.model_snaps.get(model_snap).cloned() else {
                    return Ok(());
                };
                self.real[idx].rewind(&real_snap)?;
                self.model[idx].rewind(&state);
            }
            Op::Gc => {
                // Live roots: every store's head, plus every snapshot anyone still holds.
                let mut roots: Vec<ManifestId> = self.real.iter().map(|s| s.head()).collect();
                roots.extend(self.snapshots.iter().map(|(id, _)| *id));
                self.real[0].gc(&roots)?;
            }
        }
        Ok(())
    }

    fn store_idx(&self, requested: usize) -> Option<usize> {
        (!self.real.is_empty()).then(|| requested % self.real.len())
    }

    fn snapshot_at(&self, requested: usize) -> Option<(ManifestId, ModelSnapshotId)> {
        if self.snapshots.is_empty() {
            return None;
        }
        self.snapshots
            .get(requested % self.snapshots.len())
            .copied()
    }

    /// The engine's view of a store, as a plain map — directly comparable to the model.
    fn real_state(&self, idx: usize) -> Result<BTreeMap<LogicalPageNo, Vec<u8>>, PagerError> {
        let store = &self.real[idx];
        // resolve(), not manifest.body: an overlay manifest names only what it CHANGED, so reading
        // its local map would report a fraction of the database.
        let pages = store.resolve(&store.head())?;
        let mut out = BTreeMap::new();
        for page_no in pages.keys() {
            out.insert(*page_no, store.read_head(*page_no)?.into_bytes());
        }
        Ok(out)
    }

    /// The whole point: after every single operation, the engine and the model must agree about
    /// the contents of every store, and every snapshot ever taken must still read back exactly
    /// as it did when it was taken.
    fn assert_agrees(&self) -> Result<(), TestCaseError> {
        for idx in 0..self.real.len() {
            let real = self
                .real_state(idx)
                .map_err(|e| TestCaseError::fail(format!("store {idx} unreadable: {e}")))?;
            let model: BTreeMap<LogicalPageNo, Vec<u8>> = self.model[idx]
                .head()
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();

            prop_assert_eq!(
                real,
                model,
                "engine and model disagree about the contents of store {}",
                idx
            );
        }
        Ok(())
    }

    /// Property 2 + 4 together: every snapshot ever taken is still readable and still holds
    /// exactly what it held when it was taken — including after arbitrary writes, rewinds, and
    /// garbage collections have happened since.
    fn assert_snapshots_intact(&self) -> Result<(), TestCaseError> {
        for (i, (real_snap, model_snap)) in self.snapshots.iter().enumerate() {
            let pages = match self.real[0].resolve(real_snap) {
                Ok(m) => m,
                Err(e) => {
                    return Err(TestCaseError::fail(format!(
                        "snapshot {i} ({real_snap}) was collected while still live: {e}"
                    )))
                }
            };
            for page_no in pages.keys() {
                let real = self.real[0].read(real_snap, *page_no).map_err(|e| {
                    TestCaseError::fail(format!(
                        "snapshot {i}: page {page_no} unreadable — GC collected a live page: {e}"
                    ))
                })?;
                let expected = self.model_snaps.read_at(*model_snap, *page_no);
                prop_assert_eq!(
                    Some(real.as_bytes()),
                    expected,
                    "snapshot {} page {} changed after the fact — snapshots must be immutable",
                    i,
                    page_no
                );
            }
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The headline property: under any sequence of writes, removes, snapshots, forks, rewinds,
    /// and garbage collections, the real engine agrees with the model about every store's
    /// contents, every snapshot remains intact, and GC never collects a live page.
    #[test]
    fn engine_agrees_with_model_under_random_sessions(
        ops in prop::collection::vec(op_strategy(), 1..60)
    ) {
        let mut world = World::new().map_err(|e| TestCaseError::fail(e.to_string()))?;

        for op in &ops {
            world.apply(op).map_err(|e| TestCaseError::fail(format!("{op:?} failed: {e}")))?;
            world.assert_agrees()?;
            world.assert_snapshots_intact()?;
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 1, on its own, stated as bluntly as it can be: **a write to a fork is never
    /// visible in the base.** This is the guarantee FlockDB's tenant isolation and LoomDB's
    /// session branches are both built on, so it gets its own test rather than only living
    /// inside the big one.
    #[test]
    fn a_fork_can_never_be_seen_by_its_base(
        writes in prop::collection::vec((0..MAX_PAGE_NO, any::<u8>()), 1..24)
    ) {
        let base = Pager::in_memory(config()).map_err(|e| TestCaseError::fail(e.to_string()))?;

        // Seed the base with known content on every page.
        let mut txn = base.begin().map_err(|e| TestCaseError::fail(e.to_string()))?;
        for page_no in 0..MAX_PAGE_NO {
            base.write(&mut txn, page_no, b"BASE".to_vec())
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
        }
        let v1 = base.commit(txn).map_err(|e| TestCaseError::fail(e.to_string()))?;

        let fork = base.fork(&v1).map_err(|e| TestCaseError::fail(e.to_string()))?;

        // Scribble all over the fork.
        for (page_no, content) in &writes {
            let mut txn = fork.begin().map_err(|e| TestCaseError::fail(e.to_string()))?;
            fork.write(&mut txn, *page_no, content_of(*content))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            fork.commit(txn).map_err(|e| TestCaseError::fail(e.to_string()))?;
        }

        // The base must be exactly as it was. Not "mostly". Not "eventually".
        for page_no in 0..MAX_PAGE_NO {
            let page = base.read_head(page_no)
                .map_err(|e| TestCaseError::fail(format!("base page {page_no}: {e}")))?;
            prop_assert_eq!(page.as_bytes(), b"BASE", "the fork leaked into the base at page {}", page_no);
        }
        // And the snapshot the fork was taken from is likewise untouched.
        for page_no in 0..MAX_PAGE_NO {
            let page = base.read(&v1, page_no)
                .map_err(|e| TestCaseError::fail(format!("snapshot page {page_no}: {e}")))?;
            prop_assert_eq!(page.as_bytes(), b"BASE");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 3: the diff between two manifests names **exactly** the pages that differ —
    /// no page that is the same, no page that differs left out.
    #[test]
    fn diff_names_exactly_the_pages_that_changed(
        initial in prop::collection::vec((0..MAX_PAGE_NO, any::<u8>()), 0..12),
        changes in prop::collection::vec((0..MAX_PAGE_NO, any::<u8>()), 0..12),
    ) {
        let db = Pager::in_memory(config()).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let mut model = ModelStore::new();
        let mut snaps = ModelSnapshots::new();

        let mut txn = db.begin().map_err(|e| TestCaseError::fail(e.to_string()))?;
        for (page_no, content) in &initial {
            db.write(&mut txn, *page_no, content_of(*content))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            model.write(*page_no, content_of(*content));
        }
        let before = db.commit(txn).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let model_before = snaps.take(model.head());

        let mut txn = db.begin().map_err(|e| TestCaseError::fail(e.to_string()))?;
        for (page_no, content) in &changes {
            db.write(&mut txn, *page_no, content_of(*content))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            model.write(*page_no, content_of(*content));
        }
        let after = db.commit(txn).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let model_after = snaps.take(model.head());

        let real: Vec<LogicalPageNo> = db.diff(&before, &after)
            .map_err(|e| TestCaseError::fail(e.to_string()))?
            .changes.iter().map(|(no, _)| *no).collect();
        let expected = snaps.diff(model_before, model_after);

        prop_assert_eq!(real, expected, "diff disagreed with the model");
    }
}

/// GC, stated as the property that actually matters: **after any garbage collection, every page
/// referenced by any live manifest is still readable.**
///
/// This is the one bug in this crate that would be unrecoverable. A lost page is not an error
/// message — it is a customer's data, gone, discovered later.
#[test]
fn gc_never_collects_a_page_that_something_still_points_at() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    // Build some history: three commits, each snapshotted.
    let mut snapshots = Vec::new();
    for round in 0..3u8 {
        let mut txn = db.begin()?;
        for page_no in 0..MAX_PAGE_NO {
            db.write(&mut txn, page_no, content_of(round * 10 + page_no as u8))?;
        }
        snapshots.push(db.commit(txn)?);
    }

    // A fork from the middle of history, which is then written to.
    let fork = db.fork(&snapshots[1])?;
    let mut txn = fork.begin()?;
    fork.write(&mut txn, 0, b"only on the fork".to_vec())?;
    let fork_head = fork.commit(txn)?;

    // Rewind the base to its first snapshot. The suffix is now abandoned — but it is still
    // referenced by `snapshots`, so it must survive.
    db.rewind(&snapshots[0])?;

    let live = [snapshots[0], snapshots[1], snapshots[2], fork_head];
    let stats = db.gc(&live)?;

    // Every page of every live manifest must still be readable.
    for manifest_id in &live {
        let pages = db.resolve(manifest_id)?;
        for page_no in pages.keys() {
            db.read(manifest_id, *page_no)
                .map_err(|e| panic!("GC collected a live page {page_no} of {manifest_id}: {e}"))?;
        }
    }
    assert_eq!(stats.pages_swept, 0, "nothing was garbage: {stats}");

    // Now genuinely abandon the fork and the tip, and sweep again. This time there IS garbage,
    // and it must go — a GC that never collects anything is just a memory leak with good manners.
    let live = [snapshots[0]];
    let stats = db.gc(&live)?;
    assert!(
        stats.pages_swept > 0,
        "the abandoned branch should have been swept: {stats}"
    );
    assert!(
        stats.manifests_swept > 0,
        "abandoned manifests should have been swept: {stats}"
    );

    // ...and what remains is still perfectly readable.
    let pages = db.resolve(&snapshots[0])?;
    for page_no in pages.keys() {
        db.read(&snapshots[0], *page_no)?;
    }
    Ok(())
}

/// GC must not collect pages an *open transaction* has staged but not yet committed.
///
/// This is the race the pin registry exists for (see `cas::PinRegistry`): the commit protocol
/// makes page bytes durable *before* the record that references them, so for a window they look
/// exactly like garbage. A GC that runs in that window would delete them, and the commit would
/// then succeed while pointing at bytes that no longer exist — a corrupt database produced by
/// two individually-correct operations.
#[test]
fn gc_does_not_collect_pages_staged_by_an_open_transaction() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"staged, not yet committed".to_vec())?;

    // GC runs, and knows nothing about this transaction except that its pages are pinned.
    let stats = db.gc(&[db.head()])?;
    assert_eq!(
        stats.pages_swept, 0,
        "an in-flight transaction's pages were collected: {stats}"
    );

    // The commit must still work, and the data must be there.
    let committed = db.commit(txn)?;
    assert_eq!(
        db.read(&committed, 0)?.as_bytes(),
        b"staged, not yet committed"
    );
    Ok(())
}

/// An *abandoned* transaction's pages, by contrast, are garbage and must eventually be swept —
/// otherwise every rolled-back write leaks disk forever.
#[test]
fn gc_collects_pages_from_an_abandoned_transaction() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    {
        let mut txn = db.begin()?;
        db.write(&mut txn, 0, b"this will be thrown away".to_vec())?;
        // txn drops here without committing: the pin is released.
    }

    let stats = db.gc(&[db.head()])?;
    assert_eq!(
        stats.pages_swept, 1,
        "an abandoned write must not leak disk forever: {stats}"
    );
    Ok(())
}

/// Snapshots are immutable, stated on its own: a snapshot taken before a change reads the same
/// after it, forever.
#[test]
fn a_snapshot_does_not_change_when_the_database_does() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"as it was".to_vec())?;
    let past = db.commit(txn)?;

    for round in 0..32u8 {
        let mut txn = db.begin()?;
        db.write(&mut txn, 0, content_of(round))?;
        db.commit(txn)?;

        assert_eq!(
            db.read(&past, 0)?.as_bytes(),
            b"as it was",
            "the past changed after write {round}"
        );
    }
    Ok(())
}

/// Committing identical content twice must not grow history, because a manifest's identity is
/// its content. This is what stops a chatty writer from filling a disk with duplicate manifests.
#[test]
fn rewriting_identical_content_is_a_no_op() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"same".to_vec())?;
    let first = db.commit(txn)?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"same".to_vec())?;
    let second = db.commit(txn)?;

    assert_eq!(
        first, second,
        "identical content must produce an identical manifest id"
    );
    Ok(())
}
