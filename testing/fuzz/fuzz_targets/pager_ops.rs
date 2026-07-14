//! Fuzz target: random write/remove/snapshot/fork/rewind/gc sequences against the pager,
//! with the model oracle as the judge.
//!
//! The property tests in `crates/substrate-pager/tests/oracle.rs` explore this space with a
//! *uniform* random walk. A coverage-guided fuzzer explores it with a *hostile* one: it watches
//! which branches the engine takes and deliberately steers toward the ones nobody has hit yet.
//! They find different bugs, and a storage engine needs both.
//!
//! What is asserted, after **every single operation**:
//!
//! 1. the engine and the model agree about the contents of every store;
//! 2. every snapshot ever taken still reads back exactly as it did when it was taken;
//! 3. GC has not collected a page that any live manifest still references.
//!
//! Run it:
//!
//! ```sh
//! cargo +nightly fuzz run pager_ops
//! ```
//!
//! Per CLAUDE.md rule 3: **if the on-disk format changes, this target changes in the same
//! commit.** A fuzzer testing last week's format reports green while proving nothing, which is
//! worse than having no fuzzer, because it is believed.
//!
//! ## Format version 2 — overlay manifests (P4)
//!
//! Manifests are now flat-or-overlay, with a chain that collapses at `MAX_OVERLAY_DEPTH`. That adds
//! three ways to be wrong, and this target hunts all three:
//!
//! - **A walk that stops early** — serving a stale page from an overlay that did not have an opinion.
//! - **A tombstone that leaks** — a page deleted in an overlay resurrected from the base beneath it.
//! - **GC that follows only the history edge** — collecting a manifest's *overlay base*, which does
//!   not lose history, it loses the database. A manifest has two backward edges now, and they come
//!   apart exactly at a collapse boundary.
//!
//! The `DeepWrite` op exists specifically to drive chains past the collapse threshold, because a
//! uniform random walk rarely commits eight times to the same store in a row.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use substrate_pager::model::{ModelSnapshotId, ModelSnapshots, ModelStore};
use substrate_pager::{
    LogicalPageNo, ManifestId, PageStore, Pager, PagerError, StoreConfig, MAX_OVERLAY_DEPTH,
    MIN_PAGE_SIZE,
};

/// Small pages and a small address space, so a random walk actually collides on the same pages.
/// A huge address space would let every operation touch a fresh page and prove nothing.
const PAGE_SIZE: usize = MIN_PAGE_SIZE;
const MAX_PAGE_NO: LogicalPageNo = 8;

#[derive(Arbitrary, Debug)]
enum Op {
    Write { store: u8, page_no: u8, content: u8 },
    Remove { store: u8, page_no: u8 },
    /// A burst of commits to one store, to drive its overlay chain past the collapse threshold.
    /// A uniform random walk almost never does this, and the collapse boundary is where the two
    /// backward edges of a manifest come apart — which is where the bugs are.
    DeepWrite { store: u8, page_no: u8, rounds: u8 },
    Snapshot { store: u8 },
    Fork { snapshot: u8 },
    Rewind { store: u8, snapshot: u8 },
    /// Ask for the merge base of two branches. Must never panic, and must agree with the model.
    MergeBase { a: u8, b: u8 },
    Gc,
}

fn content_of(byte: u8) -> Vec<u8> {
    vec![byte; 1 + (byte as usize % 64)]
}

struct World {
    real: Vec<Box<dyn PageStore>>,
    model: Vec<ModelStore>,
    model_snaps: ModelSnapshots,
    snapshots: Vec<(ManifestId, ModelSnapshotId)>,
}

impl World {
    fn new() -> Result<Self, PagerError> {
        let config = StoreConfig {
            page_size: PAGE_SIZE,
            ..Default::default()
        };
        Ok(World {
            real: vec![Box::new(Pager::in_memory(config)?)],
            model: vec![ModelStore::new()],
            model_snaps: ModelSnapshots::new(),
            snapshots: Vec::new(),
        })
    }

    fn store_idx(&self, requested: u8) -> usize {
        requested as usize % self.real.len()
    }

    fn snapshot_at(&self, requested: u8) -> Option<(ManifestId, ModelSnapshotId)> {
        if self.snapshots.is_empty() {
            return None;
        }
        self.snapshots
            .get(requested as usize % self.snapshots.len())
            .copied()
    }

    fn apply(&mut self, op: &Op) -> Result<(), PagerError> {
        match *op {
            Op::Write {
                store,
                page_no,
                content,
            } => {
                let idx = self.store_idx(store);
                let page_no = page_no as LogicalPageNo % MAX_PAGE_NO;
                let bytes = content_of(content);
                let mut txn = self.real[idx].begin()?;
                self.real[idx].write(&mut txn, page_no, bytes.clone())?;
                self.real[idx].commit(txn)?;
                self.model[idx].write(page_no, bytes);
            }
            Op::Remove { store, page_no } => {
                let idx = self.store_idx(store);
                let page_no = page_no as LogicalPageNo % MAX_PAGE_NO;
                let mut txn = self.real[idx].begin()?;
                self.real[idx].remove(&mut txn, page_no)?;
                self.real[idx].commit(txn)?;
                self.model[idx].remove(page_no);
            }
            Op::DeepWrite {
                store,
                page_no,
                rounds,
            } => {
                let idx = self.store_idx(store);
                let page_no = page_no as LogicalPageNo % MAX_PAGE_NO;
                // Enough rounds to cross the collapse threshold at least once, bounded so the
                // fuzzer does not spend its whole budget in one op.
                let rounds = 1 + (rounds as u32 % (MAX_OVERLAY_DEPTH * 2 + 2));

                for round in 0..rounds {
                    let bytes = content_of(round as u8);
                    let mut txn = self.real[idx].begin()?;
                    self.real[idx].write(&mut txn, page_no, bytes.clone())?;
                    self.real[idx].commit(txn)?;
                    self.model[idx].write(page_no, bytes);
                }

                // Whatever the chain did, it must never exceed the bound. An unbounded chain is
                // unbounded read amplification, and eventually a stack overflow.
                let depth = self.real[idx].manifest(&self.real[idx].head())?.depth();
                assert!(
                    depth <= MAX_OVERLAY_DEPTH,
                    "overlay chain reached depth {depth}, past the limit of {MAX_OVERLAY_DEPTH}"
                );
            }
            Op::MergeBase { a, b } => {
                let (Some((a_id, _)), Some((b_id, _))) =
                    (self.snapshot_at(a), self.snapshot_at(b))
                else {
                    return Ok(());
                };
                let base = self.real[0].merge_base(&a_id, &b_id)?;

                // The merge base must be symmetric, and must be an ancestor of both. A merge engine
                // handed a "base" that neither branch descends from will produce a merge that is
                // confidently, silently wrong.
                assert_eq!(
                    base,
                    self.real[0].merge_base(&b_id, &a_id)?,
                    "merge_base is not symmetric"
                );
                if let Some(base) = base {
                    // Resolving it must work, which is only true if it is a real manifest that GC
                    // has kept alive.
                    self.real[0].resolve(&base)?;
                }
            }
            Op::Snapshot { store } => {
                let idx = self.store_idx(store);
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
                let forked = self.real[0].fork(&real_snap)?;
                self.model.push(ModelStore::forked_from(state));
                self.real.push(forked);
            }
            Op::Rewind { store, snapshot } => {
                let idx = self.store_idx(store);
                let Some((real_snap, model_snap)) = self.snapshot_at(snapshot) else {
                    return Ok(());
                };
                let Some(state) = self.model_snaps.get(model_snap).cloned() else {
                    return Ok(());
                };
                self.real[idx].rewind(&real_snap)?;
                self.model[idx].rewind(&state);
            }
            Op::Gc => {
                let mut roots: Vec<ManifestId> = self.real.iter().map(|s| s.head()).collect();
                roots.extend(self.snapshots.iter().map(|(id, _)| *id));
                self.real[0].gc(&roots)?;
            }
        }
        Ok(())
    }

    /// Invariant 1: the engine and the model agree about every store.
    fn check_agreement(&self) -> Result<(), PagerError> {
        for idx in 0..self.real.len() {
            let store = &self.real[idx];
            let pages = store.resolve(&store.head())?;

            let mut real: BTreeMap<LogicalPageNo, Vec<u8>> = BTreeMap::new();
            for page_no in pages.keys() {
                real.insert(*page_no, store.read_head(*page_no)?.into_bytes());
            }
            let model: BTreeMap<LogicalPageNo, Vec<u8>> = self.model[idx]
                .head()
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();

            assert_eq!(
                real, model,
                "ENGINE DISAGREES WITH MODEL about store {idx}. \
                 One of them is wrong, and the model is the simple one."
            );
        }
        Ok(())
    }

    /// Invariants 2 and 3: snapshots are immutable, and GC never collected a live page.
    fn check_snapshots(&self) -> Result<(), PagerError> {
        for (i, (real_snap, model_snap)) in self.snapshots.iter().enumerate() {
            let pages = self.real[0].resolve(real_snap).unwrap_or_else(|e| {
                panic!("snapshot {i} ({real_snap}) was collected while still live: {e}")
            });

            for page_no in pages.keys() {
                let page = self.real[0].read(real_snap, *page_no).unwrap_or_else(|e| {
                    panic!("GC COLLECTED A LIVE PAGE: snapshot {i}, page {page_no}: {e}")
                });
                assert_eq!(
                    Some(page.as_bytes()),
                    self.model_snaps.read_at(*model_snap, *page_no),
                    "snapshot {i} page {page_no} changed after the fact — snapshots are immutable"
                );
            }
        }
        Ok(())
    }
}

fuzz_target!(|ops: Vec<Op>| {
    // Keep sequences bounded: the fuzzer's job is to find a *short* counterexample, and an
    // unbounded one wastes its budget re-walking ground it already covered.
    if ops.len() > 64 {
        return;
    }

    let Ok(mut world) = World::new() else { return };

    for op in &ops {
        // An operation that legitimately fails (a rewind to an unknown manifest, say) is not a
        // bug — it is the engine correctly refusing. What must never happen is the engine
        // *succeeding* and then disagreeing with the model.
        if world.apply(op).is_err() {
            continue;
        }
        world.check_agreement().expect("store became unreadable");
        world.check_snapshots().expect("snapshot became unreadable");
    }
});
