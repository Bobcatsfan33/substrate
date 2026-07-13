//! # substrate-wal
//!
//! The write-ahead log, and the commit protocol built on it.
//!
//! This crate owns **the commit point**. Nothing in substrate is committed until a CRC-protected
//! record has been fsync'd here, and everything else in the engine is arranged so that that single
//! fsync is the only moment that matters.
//!
//! ## The protocol
//!
//! ```text
//! 1. page bytes → CAS, fsync           durable, but nothing references them yet
//! 2. WAL commit record, fsync          ◄── THE COMMIT POINT
//! 3. install the manifest              now readers can see it
//! ```
//!
//! Crash **before 2** → the CAS holds unreferenced pages. That is indistinguishable from garbage,
//! because it *is* garbage. GC sweeps it. Nothing is corrupt.
//!
//! Crash **between 2 and 3** → the transaction happened. Recovery replays the log, re-derives the
//! byte-identical manifest (the commit record carries the timestamp precisely so it can), installs
//! it, and honours the commit.
//!
//! There is no third case, because step 2 is one fsync of one record with one CRC. It landed, or
//! it did not.
//!
//! ## The guarantee
//!
//! > After a crash at **any byte boundary**, the recovered store equals **some prefix of committed
//! > transactions.** No torn state. No lost commit.
//!
//! That sentence is the product, and `testing/fuzz` earns it the hard way: it kills the write path
//! at every byte in turn, recovers, and checks.
//!
//! ## Example
//!
//! ```
//! use substrate_pager::{std_vfs, StoreConfig};
//! use substrate_wal::DurableStore;
//!
//! # fn main() -> Result<(), substrate_wal::WalError> {
//! let dir = tempfile::tempdir().expect("tempdir");
//!
//! // Write and commit.
//! {
//!     let db = DurableStore::open(std_vfs(), dir.path(), StoreConfig::default())?;
//!     let mut txn = db.begin()?;
//!     db.write(&mut txn, 0, b"survives a power cut".to_vec())?;
//!     db.commit(txn)?;
//! }   // the process "dies" here — nothing was closed cleanly
//!
//! // Reopen. Recovery replays the log.
//! let db = DurableStore::open(std_vfs(), dir.path(), StoreConfig::default())?;
//! let recovery = db.recover()?;
//! assert_eq!(recovery.committed_txns, 1);
//! assert_eq!(db.read_head(0)?.as_bytes(), b"survives a power cut");
//! # Ok(())
//! # }
//! ```
//!
//! ## No `async`, no cleverness
//!
//! Deterministic replay and crash injection require deterministic execution, so this crate is pure
//! and synchronous (CLAUDE.md rule 7). It is also deliberately boring: where there was a clever way
//! and an obvious way, we took the obvious one.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(rust_2018_idioms)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod error;
mod record;
mod wal;

pub use error::{Result, WalError};
pub use record::{
    Lsn, ReadOutcome, Record, RecordKind, TxnWrites, FRAME_HEADER_BYTES, MAX_RECORD_BYTES,
};
pub use wal::{Recovery, Wal, SEGMENT_TARGET_BYTES};

use std::path::Path;
use std::sync::{Arc, Mutex};
use substrate_pager::{
    Clock, LogicalPageNo, ManifestId, Page, PageId, PageStore, Pager, StoreConfig, SystemClock,
    Txn, Vfs,
};

/// A page store whose commits survive a crash.
///
/// [`Pager`] on its own gives you content-addressed pages and O(1) forks, but its notion of "the
/// current head" lives in memory. `DurableStore` wraps it with the log, so the head survives the
/// process — and so a crash lands on a transaction boundary rather than inside one.
pub struct DurableStore {
    pager: Arc<Pager>,
    wal: Mutex<Wal>,
    clock: Arc<dyn Clock>,
}

impl DurableStore {
    /// Open (creating if absent) a durable store rooted at `dir`.
    ///
    /// Does not replay — call [`DurableStore::recover`]. Recovery is an explicit act, because a
    /// silent one buried inside a constructor is a recovery nobody audits.
    pub fn open(vfs: Arc<dyn Vfs>, dir: impl AsRef<Path>, config: StoreConfig) -> Result<Self> {
        DurableStore::open_with_clock(vfs, dir, config, Arc::new(SystemClock))
    }

    /// Open with a caller-supplied clock, so replay can be made byte-for-byte reproducible.
    pub fn open_with_clock(
        vfs: Arc<dyn Vfs>,
        dir: impl AsRef<Path>,
        config: StoreConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        let pager = Pager::open_with(Arc::clone(&vfs), dir, config, Arc::clone(&clock))?;
        let wal = Wal::open(vfs, dir)?;
        Ok(DurableStore {
            pager: Arc::new(pager),
            wal: Mutex::new(wal),
            clock,
        })
    }

    /// Replay the log, restoring the head the store had before it died.
    pub fn recover(&self) -> Result<Recovery> {
        self.lock_wal()?.recover(&self.pager)
    }

    /// Begin a transaction.
    pub fn begin(&self) -> Result<Txn> {
        Ok(self.pager.begin()?)
    }

    /// Stage a page write. **Step 1**: the bytes become durable in the CAS here, but nothing
    /// references them until the commit record lands.
    pub fn write(&self, txn: &mut Txn, page_no: LogicalPageNo, bytes: Vec<u8>) -> Result<PageId> {
        Ok(self.pager.write(txn, page_no, bytes)?)
    }

    /// Stage the removal of a logical page.
    pub fn remove(&self, txn: &mut Txn, page_no: LogicalPageNo) -> Result<()> {
        Ok(self.pager.remove(txn, page_no)?)
    }

    /// Commit. When this returns `Ok`, the transaction has happened — even if the process is
    /// killed before its caller runs another line.
    pub fn commit(&self, txn: Txn) -> Result<ManifestId> {
        let base = txn.base();
        let created_at_ms = self.clock.now_ms();

        // Derive the manifest without persisting anything. Pure computation, no side effects.
        let Some((manifest, id)) = self.pager.derive_next(base, txn.writes(), created_at_ms)?
        else {
            return Ok(base); // the transaction changes nothing
        };

        // STEP 2 — THE COMMIT POINT. One fsync of one CRC-protected record. After this returns,
        // the transaction is durable no matter what happens next.
        self.lock_wal()?.commit(txn.writes(), id, created_at_ms)?;

        // STEP 3 — make it visible. A crash before this line is survivable: recovery re-derives
        // the identical manifest from the log and installs it. That is *why* step 2 is allowed to
        // be the commit point.
        self.pager.install(&manifest)?;

        drop(txn); // the pages are referenced by a durable manifest now; release the pins
        Ok(id)
    }

    /// Persist the current head as a checkpoint and drop the log history behind it.
    ///
    /// Recovery then starts here rather than at the beginning of time, which is what keeps
    /// recovery bounded however long the database has been running.
    pub fn checkpoint(&self) -> Result<Lsn> {
        let head = self.pager.head();
        self.lock_wal()?.checkpoint(head)
    }

    /// Read a page from the current head.
    pub fn read_head(&self, page_no: LogicalPageNo) -> Result<Page> {
        Ok(self.pager.read_head(page_no)?)
    }

    /// Read a page as of a manifest.
    pub fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page> {
        Ok(self.pager.read(manifest, page_no)?)
    }

    /// The current head.
    pub fn head(&self) -> ManifestId {
        self.pager.head()
    }

    /// The underlying pager, for fork, diff, and GC.
    pub fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    fn lock_wal(&self) -> Result<std::sync::MutexGuard<'_, Wal>> {
        // A poisoned lock means a thread panicked while holding the log. Recovering the guard is
        // safe — the log's in-memory state is a couple of integers, and the *file* is the source
        // of truth — and it beats propagating a panic through a storage engine (CLAUDE.md rule 6).
        Ok(self.wal.lock().unwrap_or_else(|e| e.into_inner()))
    }
}
