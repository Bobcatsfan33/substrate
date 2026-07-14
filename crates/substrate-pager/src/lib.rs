//! # substrate-pager
//!
//! Immutable, content-addressed pages with **O(1) fork, snapshot, and rewind**.
//!
//! This is the bottom of the stack. Two products stand on it — [FlockDB] (thousands of small
//! analytical databases that sleep in object storage) and [LoomDB] (an agent-native database
//! whose sessions are branches) — and they stand on it for the same reason: forking a database
//! costs nothing, so you can afford to have a great many of them.
//!
//! [FlockDB]: https://github.com/Bobcatsfan33/flockdb
//! [LoomDB]: https://github.com/Bobcatsfan33/loomdb
//!
//! ## The idea in one example
//!
//! ```
//! use substrate_pager::{Pager, PageStore, StoreConfig};
//! # fn main() -> Result<(), substrate_pager::PagerError> {
//! let db = Pager::in_memory(StoreConfig::default())?;
//!
//! let mut txn = db.begin()?;
//! db.write(&mut txn, 0, b"the original".to_vec())?;
//! let v1 = db.commit(txn)?;              // a snapshot is just this id
//!
//! let experiment = db.fork(&v1)?;        // O(1). copies nothing.
//! let mut txn = experiment.begin()?;
//! experiment.write(&mut txn, 0, b"a wild idea".to_vec())?;
//! experiment.commit(txn)?;
//!
//! // Two databases now. The base never noticed.
//! assert_eq!(db.read_head(0)?.as_bytes(), b"the original");
//! assert_eq!(experiment.read_head(0)?.as_bytes(), b"a wild idea");
//! # Ok(())
//! # }
//! ```
//!
//! Fork isolation here is not *enforced*, it is **structural**: a manifest is an immutable value,
//! and the fork is holding a different one. There is no code path that could violate it, which is
//! a much stronger statement than "we checked".
//!
//! ## How it works
//!
//! A page is a byte block whose identity *is* its content: `PageId = BLAKE3(bytes)`. A
//! [`Manifest`] maps logical page numbers to those ids, and is itself content-addressed. So a
//! database *state* is a single 32-byte value, and:
//!
//! | Operation | What actually happens | Cost |
//! |---|---|---|
//! | [`snapshot`](PageStore::snapshot) | remember a `ManifestId` | O(1) |
//! | [`fork`](PageStore::fork) | start a new head at a `ManifestId` | O(1) |
//! | [`rewind`](PageStore::rewind) | move a head to an older `ManifestId` | O(1) |
//! | [`diff`](PageStore::diff) | compare two sorted lists of hashes | O(changed) |
//!
//! No bytes are copied in any of them.
//!
//! ## Durability
//!
//! Page bytes reach the CAS, fsync'd, *before* anything references them. A crash before the
//! commit leaves orphaned pages — durable, unreferenced, and harmless until GC sweeps them. A
//! crash after it is a committed transaction. There is no state in between, and that is the whole
//! durability guarantee. See `docs/02` §3.1.
//!
//! Every read re-hashes and verifies. Corruption is detected, never served.
//!
//! ## What this crate will not do
//!
//! - **No `async`.** The core is pure and synchronous, because deterministic replay and crash
//!   injection require deterministic execution.
//! - **No panics.** No `unwrap`, no `expect`, no `panic!` in library code. A panic in a storage
//!   engine is an unplanned process death, and an unplanned process death during a commit is
//!   precisely the disaster crash recovery exists to survive.
//! - **No network.** Ever. Object storage lives in `substrate-store`, behind this same trait.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(rust_2018_idioms)]
// Tests are the one place a panic is the correct response to the impossible: a failing
// assertion *should* stop the run. CLAUDE.md rule 6 bans panics in library code, not in the
// code that proves the library is right.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod branch;
mod cas;
mod clock;
mod diff;
mod error;
mod gc;
mod manifest;
mod page;
mod pager;
/// Crash-injection harness. Enabled by the `test-util` feature; never in a release build.
#[cfg(feature = "test-util")]
pub mod testing;
mod vfs;

pub use branch::{BranchTree, RefName};
pub use cas::{Cas, FsCas, MemCas};
pub use clock::{Clock, ManualClock, SystemClock};
pub use diff::{PageChange, PageClass, PageDiff, ThreeWayDiff};
pub use error::{PagerError, Result};
pub use gc::GcStats;
pub use manifest::{
    FsManifestStore, Manifest, ManifestBody, ManifestId, ManifestStore, MemManifestStore,
    PageChanges, PageMap, MANIFEST_FORMAT_VERSION, MAX_OVERLAY_DEPTH,
};
pub use page::{
    LogicalPageNo, Page, PageHasher, PageId, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, MIN_PAGE_SIZE,
};
pub use pager::{PageStore, Pager, StoreConfig, Txn};
pub use vfs::{std_vfs, StdVfs, Vfs};

/// The model oracle: a deliberately naive reference implementation of what a page store *means*.
///
/// This is not a test helper. It is the argument that this engine is correct.
///
/// A storage engine written quickly — and this one was written largely by an AI — has a trust
/// problem, and the rebuttal is not enthusiasm. It is a second implementation, so simple that it
/// is *obviously* right (a map of maps, no CAS, no manifests, no cleverness), which the real
/// engine is differentially tested against under randomized operation sequences. When they
/// disagree, one of them is wrong, and we find out in seconds instead of in a customer's incident
/// channel.
///
/// See `tests/oracle.rs` for the property tests that drive it.
pub mod model {
    use crate::page::LogicalPageNo;
    use std::collections::{BTreeMap, BTreeSet};

    /// A database state, as a human would describe it: these pages hold these bytes.
    pub type ModelState = BTreeMap<LogicalPageNo, Vec<u8>>;

    /// A handle to a frozen state. Stands in for `ManifestId`.
    pub type ModelSnapshotId = u64;

    /// Every snapshot ever taken, shared by every store — exactly as manifests are.
    ///
    /// This mirrors a real property of the engine that is easy to miss: manifests are globally
    /// content-addressed and immutable, so a snapshot is not owned by the store that took it.
    /// *Any* store sharing the CAS can rewind onto it, the way `git reset --hard` can move your
    /// branch onto someone else's commit. An earlier version of this model gave each store its
    /// own private snapshot table, and the property tests caught the divergence immediately —
    /// which is precisely what a model oracle is for.
    #[derive(Debug, Default, Clone)]
    pub struct ModelSnapshots {
        states: BTreeMap<ModelSnapshotId, ModelState>,
        next_id: ModelSnapshotId,
    }

    impl ModelSnapshots {
        /// An empty snapshot table.
        pub fn new() -> Self {
            ModelSnapshots::default()
        }

        /// Freeze a state and return a handle to it.
        pub fn take(&mut self, state: &ModelState) -> ModelSnapshotId {
            let id = self.next_id;
            self.next_id += 1;
            self.states.insert(id, state.clone());
            id
        }

        /// The state a snapshot froze. `None` if the handle is unknown.
        pub fn get(&self, id: ModelSnapshotId) -> Option<&ModelState> {
            self.states.get(&id)
        }

        /// Read one page as of a snapshot.
        pub fn read_at(&self, id: ModelSnapshotId, page_no: LogicalPageNo) -> Option<&[u8]> {
            self.states.get(&id)?.get(&page_no).map(|v| v.as_slice())
        }

        /// Which logical pages differ between two snapshots.
        pub fn diff(&self, a: ModelSnapshotId, b: ModelSnapshotId) -> Vec<LogicalPageNo> {
            let (Some(a), Some(b)) = (self.states.get(&a), self.states.get(&b)) else {
                return Vec::new();
            };
            a.keys()
                .chain(b.keys())
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|no| a.get(no) != b.get(no))
                .collect()
        }
    }

    /// The naive reference implementation of a store: a head, which is just a map.
    ///
    /// Forking copies the whole database. Snapshotting copies the whole database. That is
    /// wildly inefficient and completely, boringly correct — which is the entire point. The real
    /// engine achieves the same *semantics* in O(1) with content-addressed manifests, and this
    /// model exists to prove that it does.
    #[derive(Debug, Default, Clone)]
    pub struct ModelStore {
        head: ModelState,
    }

    impl ModelStore {
        /// A new, empty store.
        pub fn new() -> Self {
            ModelStore::default()
        }

        /// A store whose head is a copy of a frozen state — the model's `fork`.
        pub fn forked_from(state: &ModelState) -> Self {
            ModelStore {
                head: state.clone(),
            }
        }

        /// Write bytes to a logical page.
        pub fn write(&mut self, page_no: LogicalPageNo, bytes: Vec<u8>) {
            self.head.insert(page_no, bytes);
        }

        /// Remove a logical page.
        pub fn remove(&mut self, page_no: LogicalPageNo) {
            self.head.remove(&page_no);
        }

        /// Move the head onto a frozen state — the model's `rewind`.
        pub fn rewind(&mut self, state: &ModelState) {
            self.head = state.clone();
        }

        /// Read a logical page from the head.
        pub fn read(&self, page_no: LogicalPageNo) -> Option<&[u8]> {
            self.head.get(&page_no).map(|v| v.as_slice())
        }

        /// The current state.
        pub fn head(&self) -> &ModelState {
            &self.head
        }
    }
}
