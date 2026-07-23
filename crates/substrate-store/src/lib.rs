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
mod hotset;
mod manifests;
mod remote;
pub mod tier;

pub use error::{Result, StoreError};
pub use hotset::{HotSet, HotSetSnapshot};
pub use manifests::TieredManifestStore;
pub use remote::RemoteTier;
pub use tier::{TierStats, TieredCas};

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use substrate_pager::{
    std_vfs, Cas, FsCas, Manifest, ManifestId, ManifestStore, Page, PageId, PageStore, Pager,
    Result as PagerResult, StoreConfig,
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
    /// The **warm set** the last active session faulted — the objects the next wake will most likely
    /// need. `wake()` speculatively prefetches these in one concurrent batch, collapsing the serial
    /// manifest-chain walk into a single round-trip on a hit. A **hint only**: content-addressing means
    /// a stale entry can never serve a wrong byte, so applying it needs no validation. `#[serde(default)]`
    /// keeps tokens written before this field readable — they simply wake without the head start.
    #[serde(default)]
    pub hot_set: HotSetSnapshot,
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

/// How many objects of each kind the warm-set recorder holds — a working-set bound. A wake that faults
/// its way past this is a scan, not a point wake, and its warm set is a (still-useful) prefix. 512
/// pages + 512 manifests is far above any point-query wake and far below "the whole database".
const HOT_SET_CAP: usize = 512;

/// A page store whose durable home is object storage, and whose local disk is only a cache.
pub struct TieredStore {
    pager: Arc<Pager>,
    cas: Arc<TieredCas>,
    manifests: Arc<TieredManifestStore>,
    remote: RemoteTier,
    root: PathBuf,
    config: StoreConfig,
    /// The warm set this store is learning: every object it faults from the remote is recorded here, and
    /// [`sleep`](Self::sleep) snapshots it into the wake token so the next wake can prefetch it.
    hot_set: Arc<HotSet>,
}

/// Run one best-effort speculative prefetch batch, absorbing both its `Result` and any panic.
///
/// The wake prefetch drives its GETs with `block_on` on the runtime handle from a detached thread. If
/// the caller tears the runtime down while a prefetch is still in flight, tokio's timer/reactor calls
/// panic ("a Tokio 1.x context ... is being shutdown"). That is a shutdown *race*, not a fault in the
/// fetch, and this work is explicitly best-effort: an unwound prefetch just means the next read faults
/// normally — no head start, never a wrong byte, because everything it touches is content-addressed and
/// hash-verified on the way into the cache. So we `catch_unwind` and move on rather than let a detached
/// background thread surface a shutdown-race panic. This is the one panic-suppression in the crate,
/// justified because there is nothing to observe after it and nothing it can corrupt.
fn prefetch<T>(f: impl FnOnce() -> PagerResult<T>) {
    // AssertUnwindSafe: the captured tiers hold interior-mutable state (mutexes), but on unwind we
    // observe none of it — the closure is dropped and the prefetch abandoned — so unwind safety holds.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = f();
    }));
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

        // One warm set, shared by both tiers, so a wake's manifest faults and page faults land in the
        // same learned set the next wake prefetches.
        let hot_set = Arc::new(HotSet::new(HOT_SET_CAP));

        let local: Arc<dyn Cas> = Arc::new(FsCas::open_with_vfs(
            Arc::clone(&vfs),
            &root,
            config.hasher.clone(),
        )?);
        let cas = TieredCas::with_hot_set(
            local,
            remote.clone(),
            config.hasher.clone(),
            Some(Arc::clone(&hot_set)),
        )?;
        tokio::spawn(Arc::clone(&cas).upload_loop());

        // Manifests tier too, and they MUST — an overlay manifest is unreadable without its base,
        // and P4 made overlays the normal case. See `manifests.rs`.
        let local_manifests = manifests::local_manifests(vfs, &root)?;
        let manifests = TieredManifestStore::with_hot_set(
            local_manifests,
            remote.clone(),
            Some(Arc::clone(&hot_set)),
        )?;

        let pager = Arc::new(Pager::from_parts(
            Arc::clone(&cas) as Arc<dyn Cas>,
            Arc::clone(&manifests) as Arc<dyn ManifestStore>,
            config.clone(),
        )?);

        Ok(TieredStore {
            pager,
            cas,
            manifests,
            remote,
            root,
            config,
            hot_set,
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

    /// **Fetch many pages at once, coalescing the object-storage GETs** — see [`TieredCas::get_batch`].
    ///
    /// A product maps its own fault set to page ids (from a manifest closure, a learned warm set, the
    /// pages a query touches) and calls this to warm them in one concurrent, deduplicated batch instead
    /// of a series of single-page faults. Byte-identical to N serial [`PageStore::read`]s, all-or-nothing.
    /// The single-page path is unchanged; this is purely additive.
    pub fn get_batch(&self, ids: &[PageId]) -> PagerResult<Vec<Page>> {
        self.cas.get_batch(ids)
    }

    /// Trim the local cache to a byte budget. Only durable pages are evicted.
    pub fn evict_to(&self, max_bytes: u64) -> Result<u64> {
        self.cas.evict_to(max_bytes)
    }

    /// Upload everything and wait. After this returns, the local cache holds nothing unique.
    pub async fn flush(&self) -> Result<()> {
        self.cas.flush().await
    }

    /// The **warm set** this session has learned so far — every object it has faulted from the remote.
    ///
    /// [`sleep`](Self::sleep) folds exactly this into the wake token it returns; this accessor exposes
    /// it independently, for a consumer that wants to persist or inspect the working set without
    /// putting the database to sleep (or grafting it onto an existing token to pre-warm a wake). A hint
    /// only — content-addressing means nothing depends on it for correctness.
    pub fn warm_set(&self) -> HotSetSnapshot {
        self.hot_set.snapshot()
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

        // 2. The manifests follow the pages, never precede them. A manifest in object storage that
        //    references pages that are not there yet is a database that wakes up broken.
        //
        //    And it is manifestS, plural. The head alone is not enough: an OVERLAY manifest cannot
        //    resolve the pages it did not touch without its base, and P4 made overlays the normal
        //    case. Uploading only the head — which is what this used to do — produced a woken
        //    database that could read whatever the top overlay happened to hold and nothing else.
        //    We upload the head's whole ancestry: the storage edge (so it can be read) and the
        //    history edge (so its past survives).
        self.manifests.upload_closure(head).await?;

        // 3. Only now is it safe to throw the local copy away. `drop_local` re-checks that nothing
        //    is un-uploaded and refuses if anything is — belt and braces, because this is the one
        //    place in the engine where we deliberately delete data.
        self.cas.drop_local()?;

        Ok(WakeToken {
            pool: self.remote.pool().to_string(),
            manifest: head,
            page_size: self.config.page_size,
            // Snapshot what this session faulted, so the next wake can prefetch it. A hint; nothing
            // depends on it for correctness.
            hot_set: self.hot_set.snapshot(),
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

        // Eagerly: the head manifest. One round trip, and nothing can be read without it.
        //
        // Everything BEHIND it — the overlay chain it resolves through, and the parents that are its
        // history — arrives on demand through the manifest tier. That is what keeps waking a 100 GB
        // database from moving 100 GB.
        let key = store.remote.manifest_key(token.manifest);
        let bytes = store
            .remote
            .get(&key)
            .await?
            .ok_or_else(|| StoreError::PageLost(token.manifest.to_hex()))?;
        let manifest = Manifest::decode(&bytes)?;

        store.manifests.put(&manifest)?;
        store.pager.set_head_to(token.manifest)?;

        // Speculatively warm the objects the last session faulted — the learned warm set. This is the
        // lever that collapses the serial manifest-chain pointer-chase into one concurrent round-trip:
        // the ids are already known (from the token), so the whole chain and its pages are fetched at
        // once instead of walked one hop at a time.
        //
        // It runs on a DEDICATED thread, not a runtime worker, so its blocking wait cannot consume a
        // worker the caller's reads need — `get_batch` here drives its GETs on the runtime's workers and
        // this thread only waits, so it genuinely OVERLAPS the caller's `block_in_place` reads rather
        // than queuing behind them. It RACES those reads into the same caches, coordinated per-object by
        // the fault gates, so neither double-fetches the other's object.
        //
        // Best-effort, and safe by construction: everything is content-addressed, so a stale hint fetches
        // an object the read may not need (a wasted GET, never a wrong byte) and the read faults what it
        // actually needs normally — no validation step, and no added latency on a miss because the waste
        // is concurrent. A whole-batch failure (an object GC'd since sleep) just means no head start.
        if !token.hot_set.is_empty() {
            let cas = Arc::clone(&store.cas);
            let manifests = Arc::clone(&store.manifests);
            let pages = token.hot_set.pages.clone();
            let chain = token.hot_set.manifests.clone();
            std::thread::spawn(move || {
                // Two threads so the manifest batch and the page batch overlap in ONE round-trip, not two.
                let m = std::thread::spawn(move || prefetch(|| manifests.get_batch(&chain)));
                prefetch(|| cas.get_batch(&pages));
                let _ = m.join();
            });
        }

        Ok(store)
    }

    /// Make several heads durable in object storage — pages **and** the full manifest ancestry of
    /// each.
    ///
    /// `sleep()` handles one head, which is all a single database has. **LoomDB has many**: every
    /// branch is a head, and putting a tenant to sleep must not quietly drop the branches nobody
    /// happened to be looking at.
    pub async fn ensure_durable(&self, heads: &[ManifestId]) -> Result<()> {
        self.cas.flush().await?;
        for head in heads {
            self.manifests.upload_closure(*head).await?;
        }
        Ok(())
    }

    /// Drop every locally cached page. Only legal once everything is durable remotely.
    pub fn drop_local(&self) -> Result<()> {
        self.cas.drop_local()
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
