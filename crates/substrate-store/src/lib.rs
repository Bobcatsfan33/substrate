//! # substrate-store
//!
//! Object-storage tiering, and the two operations the economics depend on: **sleep** and **wake**.
//!
//! ## Why this crate is the product
//!
//! A database that costs nothing while idle changes what you can afford to build. You can give every
//! one of forty thousand customers a real, isolated database. You can give every agent session its
//! own branch. Both ideas are absurd if an idle database costs a container, and obvious if it costs
//! the price of its bytes in S3.
//!
//! ```text
//! sleep(db)   →  every page and the manifest are durable in object storage
//!             →  drop all local state
//!             →  return a WakeToken   (a manifest pointer. that is the whole database.)
//!
//! wake(token) →  fetch the manifest eagerly   (small — one round trip)
//!             →  fetch pages lazily           (only what the query actually touches)
//!             →  first row in < 250 ms
//! ```
//!
//! Sleeping is not a degraded state. It is the **default** state. A fleet of ten thousand databases
//! has, at any instant, perhaps fifty awake.
//!
//! ## The rule that makes it safe
//!
//! **A page is evictable only once it is confirmed durable in object storage.** Not queued.
//! Confirmed. A cache that can evict the only copy of a page is not a cache, it is a delete — and
//! [`TieredCas`] refuses, growing past its budget instead. Over-budget is a performance problem;
//! evicting live data is not a problem, it is an obituary.
//!
//! ## Pools are a security boundary
//!
//! A store belongs to exactly one pool, and **pools never share pages even when their hashes are
//! identical** (docs/02 §9.1). The pool is the first component of every object key, so two pools do
//! not collide — not because we check, but because they are writing to different places. It costs
//! cross-pool dedup and buys the guarantee that data cannot flow between classification boundaries
//! through the storage layer.
//!
//! ## Async lives here, and only here
//!
//! `substrate-pager` and `substrate-wal` are synchronous, because deterministic replay and crash
//! injection require deterministic execution (CLAUDE.md rule 7). This crate is where tokio appears.
//! A cache miss on the synchronous read path blocks on the async fetch, which **requires a
//! multi-threaded runtime** — see [`tier`] for why we chose that over making the whole engine async.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(rust_2018_idioms)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod error;
mod remote;
pub mod tier;

pub use error::{Result, StoreError};
pub use remote::RemoteTier;
pub use tier::{TierStats, TieredCas};

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use substrate_pager::{
    std_vfs, Cas, FsCas, FsManifestStore, Manifest, ManifestId, ManifestStore, PageStore, Pager,
    StoreConfig,
};

/// Everything you need to bring a sleeping database back.
///
/// This is the entire database, as far as anyone else is concerned: a pool, a manifest id, and a
/// page size. The data is in object storage, addressed by content, and the manifest names every page
/// of it.
///
/// A million sleeping databases is a million of these — which is why they fit on a laptop
/// (docs/02 §9.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeToken {
    /// The dedup pool. Pools never share pages (docs/02 §9.1).
    pub pool: String,
    /// The manifest that *is* the database's state.
    pub manifest: ManifestId,
    /// The page size the store was created with.
    pub page_size: usize,
}

impl WakeToken {
    /// Serialize, for a registry to hold.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|source| StoreError::Codec {
            op: "encode",
            what: "wake token",
            source,
        })
    }

    /// Parse.
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|source| StoreError::Codec {
            op: "decode",
            what: "wake token",
            source,
        })
    }
}

/// A page store whose durable home is object storage, and whose local disk is only a cache.
pub struct TieredStore {
    pager: Arc<Pager>,
    cas: Arc<TieredCas>,
    manifests: Arc<dyn ManifestStore>,
    remote: RemoteTier,
    root: PathBuf,
    config: StoreConfig,
}

impl TieredStore {
    /// Open a tiered store, spawning the background uploader.
    ///
    /// Must be called from a **multi-threaded** tokio runtime.
    pub async fn open(
        root: impl AsRef<Path>,
        remote: RemoteTier,
        config: StoreConfig,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let vfs = std_vfs();

        let local: Arc<dyn Cas> = Arc::new(FsCas::open_with_vfs(
            Arc::clone(&vfs),
            &root,
            config.hasher.clone(),
        )?);
        let cas = TieredCas::new(local, remote.clone(), config.hasher.clone())?;
        tokio::spawn(Arc::clone(&cas).upload_loop());

        let manifests: Arc<dyn ManifestStore> =
            Arc::new(FsManifestStore::open_with_vfs(vfs, &root)?);

        let pager = Arc::new(Pager::from_parts(
            Arc::clone(&cas) as Arc<dyn Cas>,
            Arc::clone(&manifests),
            config.clone(),
        )?);

        Ok(TieredStore {
            pager,
            cas,
            manifests,
            remote,
            root,
            config,
        })
    }

    /// The underlying pager: fork, snapshot, diff, GC.
    pub fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    /// Cache statistics.
    pub fn stats(&self) -> TierStats {
        self.cas.stats()
    }

    /// Trim the local cache to a byte budget. Only durable pages are evicted.
    pub fn evict_to(&self, max_bytes: u64) -> Result<u64> {
        self.cas.evict_to(max_bytes)
    }

    /// Upload everything and wait. After this returns, the local cache holds nothing unique.
    pub async fn flush(&self) -> Result<()> {
        self.cas.flush().await
    }

    /// **Sleep.** Make everything durable remotely, drop local state, hand back the pointer.
    ///
    /// The order is the same discipline as the commit protocol: *make it durable elsewhere, verify,
    /// and only then throw away the copy you have.* If the flush fails we drop nothing and the
    /// database stays awake. A `sleep` that loses data is not a feature — it is a bug with good
    /// marketing.
    pub async fn sleep(&self) -> Result<WakeToken> {
        let head = self.pager.head();

        // 1. Every page reaches object storage. The only step that can fail; if it does, we stop
        //    here with the database intact and awake.
        self.cas.flush().await?;

        // 2. The manifest follows the pages, never precedes them. A manifest in object storage
        //    referencing pages that are not there yet is a database that wakes up broken.
        let manifest = self.manifests.get(head)?;
        let key = self.remote.manifest_key(head);
        self.remote.put(&key, manifest.encode()?).await?;

        // 3. Only now is it safe to throw the local copy away. `drop_local` re-checks that nothing
        //    is un-uploaded and refuses if anything is — belt and braces, because this is the one
        //    place in the engine where we deliberately delete data.
        self.cas.drop_local()?;

        Ok(WakeToken {
            pool: self.remote.pool().to_string(),
            manifest: head,
            page_size: self.config.page_size,
        })
    }

    /// **Wake.** Restore a sleeping database.
    ///
    /// The manifest is fetched **eagerly** — it is small, and nothing can be read without it. Pages
    /// are fetched **lazily**, on the first read that touches them. That is the whole trick behind
    /// the 250 ms target (docs/02 §7): waking a 100 GB database does not move 100 GB. It moves one
    /// manifest, and then only what the query actually reads.
    pub async fn wake(
        root: impl AsRef<Path>,
        remote: RemoteTier,
        token: &WakeToken,
    ) -> Result<Self> {
        // The token names a pool and this tier is bound to a pool. If they disagree, something is
        // trying to wake a database into the wrong classification boundary.
        remote.guard_pool(&token.pool)?;

        let config = StoreConfig {
            page_size: token.page_size,
            pool: token.pool.clone(),
            ..Default::default()
        };
        let store = TieredStore::open(root, remote, config).await?;

        let key = store.remote.manifest_key(token.manifest);
        let bytes = store
            .remote
            .get(&key)
            .await?
            .ok_or_else(|| StoreError::PageLost(token.manifest.to_hex()))?;
        let manifest = Manifest::decode(&bytes)?;

        store.manifests.put(&manifest)?;
        store.pager.set_head_to(token.manifest)?;

        Ok(store)
    }

    /// Where the local cache lives.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The pool this store belongs to.
    pub fn pool(&self) -> &str {
        self.remote.pool()
    }
}

impl std::fmt::Debug for TieredStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredStore")
            .field("pool", &self.remote.pool())
            .field("root", &self.root)
            .field("stats", &self.stats())
            .finish()
    }
}

/// What a repair pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RepairReport {
    /// Pages re-fetched from object storage and verified.
    pub repaired: Vec<String>,
    /// Pages that were damaged locally **and** unavailable or damaged remotely.
    ///
    /// These are lost. Not "probably fine" — lost. The only honest thing to do is say so, loudly,
    /// with the ids, so an operator can go to their backups rather than discover it later.
    pub unrepairable: Vec<String>,
}

impl RepairReport {
    /// True if everything damaged was fixed.
    pub fn is_complete(&self) -> bool {
        self.unrepairable.is_empty()
    }
}

impl std::fmt::Display for RepairReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_complete() {
            return write!(
                f,
                "repaired {} page(s) from object storage",
                self.repaired.len()
            );
        }
        write!(
            f,
            "repaired {} page(s); {} PAGE(S) COULD NOT BE REPAIRED — damaged locally and \
             unavailable remotely. These are lost; restore from backup.",
            self.repaired.len(),
            self.unrepairable.len()
        )
    }
}

impl TieredStore {
    /// Repair the damage a scrub found, by re-fetching healthy copies from object storage.
    ///
    /// # Why this lives here and not in the pager
    ///
    /// Repair means fetching a known-good replica, and `substrate-pager` does not know object
    /// storage exists (CLAUDE.md rule 2). It finds the damage and hands over a
    /// [`CorruptionReport`](substrate_pager::CorruptionReport); this consumes it.
    ///
    /// # Why this is safe
    ///
    /// Content addressing. A page's id *is* the hash of its bytes, so a replacement fetched from
    /// object storage can be **proven** correct before it is installed — we do not have to trust the
    /// remote copy, we can check it. If the remote copy is also damaged, it fails the same check and
    /// we report the page as unrepairable rather than swapping one corruption for another.
    ///
    /// This is the payoff for hashing everything. In a system without content addressing, "restore
    /// this page from the replica" is an act of faith.
    pub async fn repair(&self, report: &substrate_pager::CorruptionReport) -> Result<RepairReport> {
        let mut out = RepairReport::default();
        let damaged = report.corrupt.iter().chain(report.missing.iter());

        for page_id in damaged {
            let key = self.remote.page_key(*page_id);

            let Some(bytes) = self.remote.get(&key).await? else {
                out.unrepairable.push(page_id.to_hex());
                continue;
            };

            // Verify BEFORE installing. The remote copy is not trusted — it is checked.
            let page =
                match substrate_pager::Page::new(&self.config.hasher, bytes, self.config.page_size)
                {
                    Ok(page) if page.id() == *page_id => page,
                    _ => {
                        // The replica is damaged too. Say so. Installing it would replace a corruption
                        // we know about with one we do not.
                        out.unrepairable.push(page_id.to_hex());
                        continue;
                    }
                };

            // The local CAS is write-once, so the damaged file must go before the good one lands.
            let local = self.pager.cas();
            local.remove(*page_id)?;
            local.put(&page)?;
            out.repaired.push(page_id.to_hex());
        }

        Ok(out)
    }
}
