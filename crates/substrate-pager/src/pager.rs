//! The page store: the one door to durable state.
//!
//! **No crate writes a file or an S3 object directly** (CLAUDE.md rule 2). Everything that must
//! persist goes through [`PageStore`], which is what makes encryption, tiering, integrity
//! scrubbing, air-gap enforcement, and metrics implementable in exactly one place.
//!
//! # The shape of it
//!
//! ```text
//! Pager ──► head: ManifestId ──► Manifest { 0 → PageId(aa..), 1 → PageId(bb..) }
//!   │                                              │
//!   └──► CAS  ─────────────────────────────────────┴──► the actual bytes
//! ```
//!
//! A fork is a second `Pager` pointing at the same manifest and sharing the same CAS. It copies
//! nothing. Writes to it produce *new* manifests, and the base's head never moves — so fork
//! isolation is not enforced, it is **structural**. There is no code path that could violate it,
//! because a manifest is an immutable value and the base is holding a different one.

use crate::cas::{Cas, CasHandle, FsCas, MemCas, PinRegistry};
use crate::clock::{Clock, SystemClock};
use crate::diff::{PageDiff, ThreeWayDiff};
use crate::error::{PagerError, Result};
use crate::gc::GcStats;
use crate::manifest::{
    FsManifestStore, Manifest, ManifestId, ManifestStore, MemManifestStore, PageChanges, PageMap,
    MAX_OVERLAY_DEPTH,
};
use crate::page::{validate_page_size, LogicalPageNo, Page, PageHasher, PageId, DEFAULT_PAGE_SIZE};
use crate::vfs::{std_vfs, Vfs};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// How a store is created.
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Page size in bytes. Fixed for the life of the store — changing it would change every
    /// page's identity, which is to say it would be a different store.
    pub page_size: usize,
    /// How page identity is computed. [`PageHasher::Keyed`] is mandatory for CUI pools
    /// (docs/02 §9.1).
    pub hasher: PageHasher,
    /// The dedup pool this store belongs to.
    ///
    /// **A store belongs to exactly one pool, and pools never share pages even when hashes
    /// match.** There is no setting that turns this off. It costs cross-pool deduplication and
    /// buys the guarantee that data cannot flow between two classification boundaries through
    /// the storage layer.
    pub pool: String,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            page_size: DEFAULT_PAGE_SIZE,
            hasher: PageHasher::Unkeyed,
            pool: "default".to_string(),
        }
    }
}

/// An in-progress set of page writes. Nothing here is visible to any reader until [`commit`].
///
/// [`commit`]: PageStore::commit
///
/// # Why staged pages are pinned
///
/// The commit protocol writes page bytes to the CAS *before* the commit record that references
/// them (docs/02 §3.1). In between, a page is durable but unreferenced — which is precisely what
/// GC hunts for. So staging a write pins the page, and dropping or committing the transaction
/// unpins it. Without that, a GC running alongside an open transaction would delete pages the
/// transaction is about to commit, and the commit would succeed while pointing at bytes that no
/// longer exist.
pub struct Txn {
    /// `None` means "remove this logical page" — a truncation.
    writes: BTreeMap<LogicalPageNo, Option<PageId>>,
    /// The manifest this transaction was begun against.
    base: ManifestId,
    pins: Arc<PinRegistry>,
    pinned: Vec<PageId>,
}

impl Txn {
    /// How many logical pages this transaction touches.
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// True if the transaction would change nothing.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// The manifest this transaction is layered on top of.
    pub fn base(&self) -> ManifestId {
        self.base
    }

    /// The staged writes. `None` means the logical page is being removed.
    ///
    /// `substrate-wal` reads this to log the transaction before it is applied.
    pub fn writes(&self) -> &BTreeMap<LogicalPageNo, Option<PageId>> {
        &self.writes
    }
}

impl Drop for Txn {
    /// Release the pins. An abandoned transaction's pages become collectable garbage — durable,
    /// unreferenced, and harmless until the next GC sweeps them.
    fn drop(&mut self) {
        for id in &self.pinned {
            self.pins.unpin(*id);
        }
    }
}

/// The durable-state interface every layer above substrate uses.
///
/// See docs/02 §5.1. The operations that sound expensive — snapshot, fork — are O(1) and copy
/// no bytes, because a manifest is a value rather than a location.
pub trait PageStore: Send + Sync {
    /// Read one logical page as of a manifest. The content hash is verified on every read.
    fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page>;

    /// Read one logical page as of this store's current head.
    fn read_head(&self, page_no: LogicalPageNo) -> Result<Page>;

    /// Begin a transaction against the current head.
    fn begin(&self) -> Result<Txn>;

    /// Stage a page write. Content-addressed: writing identical bytes twice is one page.
    ///
    /// The bytes reach the CAS (and are fsync'd) here, but nothing references them until
    /// [`commit`](PageStore::commit). That ordering is the durability guarantee, not an
    /// optimisation — see docs/02 §3.1.
    fn write(&self, txn: &mut Txn, page_no: LogicalPageNo, bytes: Vec<u8>) -> Result<PageId>;

    /// Stage the removal of a logical page.
    fn remove(&self, txn: &mut Txn, page_no: LogicalPageNo) -> Result<()>;

    /// The commit point. Returns the manifest that is now durable and visible.
    fn commit(&self, txn: Txn) -> Result<ManifestId>;

    /// This store's current manifest.
    fn head(&self) -> ManifestId;

    /// O(1). Serialize the current manifest and return its id. Not one byte is copied.
    fn snapshot(&self) -> Result<ManifestId>;

    /// O(1). A new store sharing this CAS, with its own head.
    ///
    /// **Writes to the fork are never visible in the base.** This is the guarantee both products
    /// are built on: FlockDB's per-tenant isolation and LoomDB's per-session branches are the
    /// same call.
    fn fork(&self, from: &ManifestId) -> Result<Box<dyn PageStore>>;

    /// O(1). Move this store's head to another manifest.
    ///
    /// The abandoned suffix stays readable until GC — which is what lets an agent explore three
    /// hypotheses, discard two, and still audit what it discarded.
    fn rewind(&self, to: &ManifestId) -> Result<()>;

    /// Which logical pages differ between two manifests.
    fn diff(&self, a: &ManifestId, b: &ManifestId) -> Result<PageDiff>;

    /// Three-way classification against a merge base. The input to LoomDB's merge engine.
    fn diff3(&self, base: &ManifestId, a: &ManifestId, b: &ManifestId) -> Result<ThreeWayDiff>;

    /// Sweep every page and manifest unreachable from the given live manifests.
    ///
    /// Refcounts are **recomputed here from the manifests themselves**, never read from a
    /// counter file (CLAUDE.md rule 9). A counter file is a second source of truth about
    /// liveness, and a corrupt one silently deletes live data.
    fn gc(&self, live_manifests: &[ManifestId]) -> Result<GcStats>;

    /// Load a manifest by id.
    fn manifest(&self, id: &ManifestId) -> Result<Manifest>;

    /// Materialise a manifest's complete page map, walking its overlay chain.
    ///
    /// O(pages). Use [`PageStore::lookup`] when you want one page — that is O(depth), and depth is
    /// bounded by `MAX_OVERLAY_DEPTH`.
    fn resolve(&self, id: &ManifestId) -> Result<PageMap>;

    /// Resolve one logical page through the overlay chain. O(depth), bounded at 8.
    fn lookup(&self, id: &ManifestId, page_no: LogicalPageNo) -> Result<Option<PageId>>;

    /// The most recent common ancestor of two manifests in the commit DAG.
    ///
    /// This is the **merge base**: the point the two branches agreed, and the third input every
    /// three-way merge needs. `None` if they share no history at all.
    fn merge_base(&self, a: &ManifestId, b: &ManifestId) -> Result<Option<ManifestId>>;

    /// The page size this store was created with.
    fn page_size(&self) -> usize;

    /// The dedup pool this store belongs to.
    fn pool(&self) -> &str;
}

/// The reference implementation of [`PageStore`].
pub struct Pager {
    cas: CasHandle,
    manifests: Arc<dyn ManifestStore>,
    /// Decoded manifests, kept in memory.
    ///
    /// # Why this is safe, and why it is necessary
    ///
    /// Manifests are **immutable and content-addressed**, so a cached one can never be stale: a
    /// different manifest is a different id. There is no invalidation protocol here because there is
    /// nothing to invalidate.
    ///
    /// It is necessary because without it, every single page read deserialized an entire manifest
    /// from bincode. On a 1 GiB database that is sixteen thousand entries, and the benchmark put a
    /// number on it: **1.9 ms to read one page.** Reads are supposed to be the cheap thing.
    ///
    /// Shared across every fork of a store, because forks share history and therefore share the
    /// manifests worth caching.
    cache: Arc<Mutex<ManifestCache>>,
    head: Mutex<ManifestId>,
    config: StoreConfig,
    clock: Arc<dyn Clock>,
}

/// A bounded cache of decoded manifests.
///
/// Deliberately dumb: when it is full, it is cleared. A proper LRU would evict better, and would
/// also be a second data structure to keep in step with the first — for a cache whose entries are
/// all equally valid and cheap to rebuild, that complexity buys very little. (CLAUDE.md rule 10.)
#[derive(Default)]
struct ManifestCache {
    entries: HashMap<ManifestId, Arc<Manifest>>,
}

impl ManifestCache {
    /// Roughly 32 MiB of manifests for a 1 GiB database. Small enough not to matter, large enough
    /// that an overlay chain and its base always fit.
    const CAPACITY: usize = 2_048;

    fn get(&self, id: &ManifestId) -> Option<Arc<Manifest>> {
        self.entries.get(id).cloned()
    }

    fn insert(&mut self, id: ManifestId, manifest: Arc<Manifest>) {
        if self.entries.len() >= Self::CAPACITY {
            self.entries.clear();
        }
        self.entries.insert(id, manifest);
    }

    fn forget(&mut self, id: &ManifestId) {
        self.entries.remove(id);
    }
}

impl Pager {
    /// Create or open a store rooted at a directory.
    ///
    /// ```
    /// # use substrate_pager::{Pager, PageStore, StoreConfig};
    /// # fn main() -> Result<(), substrate_pager::PagerError> {
    /// let dir = tempfile::tempdir().expect("tempdir");
    /// let db = Pager::open(dir.path(), StoreConfig::default())?;
    ///
    /// let mut txn = db.begin()?;
    /// db.write(&mut txn, 0, b"first page".to_vec())?;
    /// let v1 = db.commit(txn)?;
    ///
    /// // Fork it. This copies nothing.
    /// let branch = db.fork(&v1)?;
    /// let mut txn = branch.begin()?;
    /// branch.write(&mut txn, 0, b"changed on the branch".to_vec())?;
    /// branch.commit(txn)?;
    ///
    /// // The base is untouched.
    /// assert_eq!(db.read_head(0)?.as_bytes(), b"first page");
    /// assert_eq!(branch.read_head(0)?.as_bytes(), b"changed on the branch");
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(root: impl AsRef<Path>, config: StoreConfig) -> Result<Self> {
        Pager::open_with(std_vfs(), root, config, Arc::new(SystemClock))
    }

    /// Open a store on a caller-supplied filesystem and clock.
    ///
    /// This is the seam the crash-injection harness reaches through. Hand it a [`Vfs`] that dies
    /// at a chosen byte and a [`Clock`] that does not move, and every durable write in the engine
    /// becomes both killable and deterministic — which is the only way to *prove* that a crash at
    /// any byte boundary leaves some prefix of committed transactions, rather than merely
    /// believing it.
    pub fn open_with(
        vfs: Arc<dyn Vfs>,
        root: impl AsRef<Path>,
        config: StoreConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        validate_page_size(config.page_size)?;
        Self::guard_keyed_hash(&config)?;

        let root = root.as_ref();
        let cas = FsCas::open_with_vfs(Arc::clone(&vfs), root, config.hasher.clone())?;
        let manifests = FsManifestStore::open_with_vfs(vfs, root)?;

        Pager::assemble(Arc::new(cas), Arc::new(manifests), config, clock)
    }

    /// Create an in-memory store. Tests, and ephemeral forks.
    pub fn in_memory(config: StoreConfig) -> Result<Self> {
        validate_page_size(config.page_size)?;
        Self::guard_keyed_hash(&config)?;
        Pager::assemble(
            Arc::new(MemCas::new(config.hasher.clone())),
            Arc::new(MemManifestStore::new()),
            config,
            Arc::new(SystemClock),
        )
    }

    /// An in-memory store with a caller-supplied clock, so tests can be deterministic.
    pub fn in_memory_with_clock(config: StoreConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        validate_page_size(config.page_size)?;
        Self::guard_keyed_hash(&config)?;
        Pager::assemble(
            Arc::new(MemCas::new(config.hasher.clone())),
            Arc::new(MemManifestStore::new()),
            config,
            clock,
        )
    }

    /// With the `keyed-hash` feature compiled in, an unkeyed store cannot be created.
    ///
    /// A CUI build must not be *configurable* back into plaintext-confirmable page identity
    /// (docs/02 §9.1). The feature is the policy; this is where it bites.
    fn guard_keyed_hash(config: &StoreConfig) -> Result<()> {
        #[cfg(feature = "keyed-hash")]
        if matches!(config.hasher, PageHasher::Unkeyed) {
            return Err(PagerError::UnkeyedStoreInKeyedBuild);
        }
        let _ = config; // the parameter is only read under the feature
        Ok(())
    }

    fn assemble(
        cas: Arc<dyn Cas>,
        manifests: Arc<dyn ManifestStore>,
        config: StoreConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        // A brand-new store starts at an empty root manifest. Opening an existing store finds
        // its head via the WAL (substrate-wal) — the pager alone has no notion of "the latest",
        // deliberately: inventing one here would be a second source of truth about what is
        // committed, and the WAL is the only one allowed to have that opinion.
        let root = Manifest::empty(config.page_size);
        let head = manifests.put(&root)?;

        Ok(Pager {
            cas: CasHandle {
                cas,
                pins: Arc::new(PinRegistry::default()),
            },
            manifests,
            cache: Arc::new(Mutex::new(ManifestCache::default())),
            head: Mutex::new(head),
            config,
            clock,
        })
    }

    /// Open a store positioned at an existing manifest — how `substrate-wal` restores a head
    /// after recovery, and how `substrate-store` wakes a sleeping database.
    pub fn at(&self, manifest: ManifestId) -> Result<Pager> {
        if !self.manifests.contains(manifest)? {
            return Err(PagerError::MissingManifest(manifest.to_hex()));
        }
        Ok(Pager {
            cas: self.cas.clone(),
            manifests: Arc::clone(&self.manifests),
            // Forks share the cache. They share history, so they share the manifests worth caching.
            cache: Arc::clone(&self.cache),
            head: Mutex::new(manifest),
            config: self.config.clone(),
            clock: Arc::clone(&self.clock),
        })
    }

    /// Compute the manifest a transaction *would* produce, without persisting anything.
    ///
    /// This is the seam that lets `substrate-wal` implement the commit protocol in the right
    /// order (docs/02 §3.1): derive the manifest, write the WAL commit record and fsync it —
    /// **that** is the commit point — and only then install the manifest. A crash between the
    /// fsync and the install is harmless, because recovery re-derives the identical manifest
    /// from the log and installs it. Idempotent, by construction.
    ///
    /// Returns `None` if the transaction changes nothing.
    pub fn derive_next(
        &self,
        base: ManifestId,
        writes: &PageChanges,
        created_at_ms: u64,
    ) -> Result<Option<(Manifest, ManifestId)>> {
        let base_manifest = self.fetch(base)?;

        // Look up ONLY the pages this transaction touches — O(changed × depth), with depth bounded
        // at 8. Resolving the whole base here would cost O(pages), which is exactly the cost overlays
        // exist to avoid, and doing it was measurably wrong: the benchmark showed a one-page commit
        // to a 1 GiB database taking 3.7 ms and scaling with the database rather than the change.
        let mut changes_anything = false;
        let mut page_count = base_manifest.page_count as i64;

        for (&page_no, &content) in writes {
            let existing = self.lookup_page(base, page_no)?;
            match (existing, content) {
                (Some(old), Some(new)) if old == new => {} // rewriting identical bytes
                (None, None) => {}                         // removing a page that is not there
                (None, Some(_)) => {
                    changes_anything = true;
                    page_count += 1;
                }
                (Some(_), None) => {
                    changes_anything = true;
                    page_count -= 1;
                }
                _ => changes_anything = true,
            }
        }

        // A transaction that rewrites the bytes already there changes nothing, and must not append a
        // duplicate manifest to history. Otherwise an idempotent retry — a writer replaying the same
        // batch after a timeout, which is NORMAL — grows the manifest DAG forever while the database
        // stands still.
        if !changes_anything {
            return Ok(None);
        }
        let page_count = page_count.max(0) as u64;

        // Flatten or overlay, decided purely by the base's depth so that replay reproduces the same
        // choice (see manifest.rs). Only the flattening branch pays O(pages), and only one commit in
        // MAX_OVERLAY_DEPTH takes it.
        let next = if base_manifest.must_flatten() {
            let resolved = self.resolve_manifest(base, &base_manifest)?;
            Manifest::flatten_onto(base, &base_manifest, &resolved, writes, created_at_ms)
        } else {
            Manifest::overlay_on(base, &base_manifest, writes, page_count, created_at_ms)
        };

        let id = next.id()?;
        Ok(Some((next, id)))
    }

    /// Persist a manifest and make it this store's head.
    ///
    /// Idempotent: manifests are content-addressed, so installing one twice is one manifest.
    /// Recovery leans on that hard.
    pub fn install(&self, manifest: &Manifest) -> Result<ManifestId> {
        let id = self.manifests.put(manifest)?;
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::new(manifest.clone()));
        self.set_head(id);
        Ok(id)
    }

    /// Assemble a store from a caller-supplied CAS and manifest store.
    ///
    /// This is the seam `substrate-store` reaches through to slide an object-storage tier
    /// underneath the pager without the pager knowing anything about it — which is the whole point
    /// of CLAUDE.md rule 2. The pager does not know whether a page came from a local disk, a cache,
    /// or an S3 bucket in another region, and it must not: the moment it does, tiering stops being
    /// implementable in one place.
    pub fn from_parts(
        cas: Arc<dyn Cas>,
        manifests: Arc<dyn ManifestStore>,
        config: StoreConfig,
    ) -> Result<Self> {
        validate_page_size(config.page_size)?;
        Self::guard_keyed_hash(&config)?;
        Pager::assemble(cas, manifests, config, Arc::new(SystemClock))
    }

    /// The canonical empty root manifest for this store, persisting it if needed.
    ///
    /// **Recovery always replays from a fixed base — this, or a checkpoint — never from whatever
    /// the head happens to be right now.** An earlier version fell back to the current head, which
    /// worked perfectly the first time and diverged the second: replaying an already-replayed log
    /// derived the first transaction from the *recovered* head rather than the root, producing a
    /// manifest that did not match the commit record. The store then refused to open at all.
    ///
    /// Recovery runs after a crash. A crash *during* recovery is not exotic — it is Tuesday — so
    /// recovery has to be idempotent, and idempotence starts with a fixed base.
    pub fn root_manifest(&self) -> Result<ManifestId> {
        let root = Manifest::empty(self.config.page_size);
        self.manifests.put(&root)
    }

    /// Point this store at an already-persisted manifest, without touching the CAS.
    pub fn set_head_to(&self, id: ManifestId) -> Result<()> {
        if !self.manifests.contains(id)? {
            return Err(PagerError::MissingManifest(id.to_hex()));
        }
        self.set_head(id);
        Ok(())
    }

    /// The store's manifest store, for crates that must persist manifests during recovery.
    pub fn manifest_store(&self) -> Arc<dyn ManifestStore> {
        Arc::clone(&self.manifests)
    }

    fn head_id(&self) -> ManifestId {
        // A poisoned lock means a thread panicked while holding the head. The head is a single
        // `Copy` id — it cannot be torn — so recovering it is safe and beats propagating a panic
        // into a storage engine (CLAUDE.md rule 6).
        *self.head.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_head(&self, id: ManifestId) {
        *self.head.lock().unwrap_or_else(|e| e.into_inner()) = id;
    }

    /// Every manifest reachable from these roots, following parent pointers.
    ///
    /// Parents are reachable because history is what makes rewind, diff, and audit possible.
    /// Collecting a live manifest's parent would leave a dangling edge in the DAG and silently
    /// destroy the very history LoomDB's provenance layer depends on.
    fn reachable_manifests(&self, roots: &[ManifestId]) -> Result<HashSet<ManifestId>> {
        let mut seen = HashSet::new();
        let mut stack: Vec<ManifestId> = roots.to_vec();

        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            // A root that does not exist is a caller error, not a reason to sweep everything.
            // Bailing out here is the conservative choice: we would rather retain garbage than
            // collect something we merely failed to read.
            let manifest = self.fetch(id)?;

            // The HISTORY edge. Parents are reachable because history is what makes rewind, diff,
            // and audit possible.
            if let Some(parent) = manifest.parent {
                stack.push(parent);
            }

            // The STORAGE edge — and this one is not optional. An overlay manifest is *unreadable*
            // without its base: the base holds the pages the overlay did not touch. Collecting a
            // live manifest's overlay base would not lose history, it would lose the DATABASE.
            //
            // The two edges usually coincide, so it is very easy to write this loop following only
            // `parent` and never notice. They come apart exactly at a collapse boundary, where a
            // flattened manifest has a parent but no base — and at a rewind, where a head's parent
            // chain and its overlay chain diverge.
            if let Some(base) = manifest.overlay_base() {
                stack.push(base);
            }
        }
        Ok(seen)
    }

    /// Load a manifest, decoding it at most once.
    fn fetch(&self, id: ManifestId) -> Result<Arc<Manifest>> {
        if let Some(hit) = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
        {
            return Ok(hit);
        }
        let manifest = Arc::new(self.manifests.get(id)?);
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::clone(&manifest));
        Ok(manifest)
    }

    /// Walk an overlay chain to a complete page map.
    fn resolve_manifest(&self, id: ManifestId, manifest: &Manifest) -> Result<PageMap> {
        // Collect the chain from this manifest down to the flat one at its base.
        let mut chain: Vec<Arc<Manifest>> = Vec::new();
        let mut current = Arc::new(manifest.clone());

        loop {
            match current.overlay_base() {
                None => break, // flat: the bottom of the chain
                Some(base) => {
                    chain.push(current);
                    current = self.fetch(base)?;
                }
            }
            // Depth is bounded by construction (MAX_OVERLAY_DEPTH), but a corrupt or hostile
            // manifest could claim otherwise, and an unbounded walk on untrusted input is how a
            // storage engine becomes a denial of service against itself.
            if chain.len() > MAX_OVERLAY_DEPTH as usize + 1 {
                return Err(PagerError::MissingManifest(format!(
                    "{id}: overlay chain exceeds the maximum depth of {MAX_OVERLAY_DEPTH}"
                )));
            }
        }

        let mut pages = current.flat_pages().cloned().unwrap_or_default();

        // Apply the overlays from the bottom up — oldest change first, newest last, so the newest
        // wins. Reversing this silently serves stale data.
        for overlay in chain.iter().rev() {
            if let Some(changes) = overlay.changes() {
                for (&page_no, &content) in changes {
                    match content {
                        Some(page) => {
                            pages.insert(page_no, page);
                        }
                        None => {
                            pages.remove(&page_no);
                        }
                    }
                }
            }
        }
        Ok(pages)
    }

    /// Resolve one page through the chain, without materialising the whole map.
    fn lookup_page(&self, id: ManifestId, page_no: LogicalPageNo) -> Result<Option<PageId>> {
        let mut current = self.fetch(id)?;
        let mut hops = 0u32;

        loop {
            match current.local_lookup(page_no) {
                // This manifest has an opinion: either the content, or a definite removal. Either
                // way we stop. Continuing past a tombstone would resurrect the base's copy.
                Some(answer) => return Ok(answer),
                // It says nothing. Ask the base.
                None => match current.overlay_base() {
                    Some(base) => {
                        hops += 1;
                        if hops > MAX_OVERLAY_DEPTH + 1 {
                            return Err(PagerError::MissingManifest(format!(
                                "overlay chain exceeds the maximum depth of {MAX_OVERLAY_DEPTH}"
                            )));
                        }
                        current = self.fetch(base)?;
                    }
                    // A flat manifest always has an opinion (`local_lookup` returns `Some`), so
                    // reaching here means the chain ended without one. Nothing has the page.
                    None => return Ok(None),
                },
            }
        }
    }

    /// Every ancestor of a manifest in the commit DAG, including itself.
    fn ancestors(&self, id: ManifestId) -> Result<Vec<ManifestId>> {
        let mut out = Vec::new();
        let mut current = Some(id);
        while let Some(next) = current {
            out.push(next);
            current = self.fetch(next)?.parent;
        }
        Ok(out)
    }
}

impl PageStore for Pager {
    fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page> {
        let page_id =
            self.lookup_page(*manifest, page_no)?
                .ok_or_else(|| PagerError::PageNotFound {
                    page_no,
                    manifest: manifest.to_hex(),
                })?;
        self.cas.cas.get(page_id)
    }

    fn read_head(&self, page_no: LogicalPageNo) -> Result<Page> {
        self.read(&self.head_id(), page_no)
    }

    fn begin(&self) -> Result<Txn> {
        Ok(Txn {
            writes: BTreeMap::new(),
            base: self.head_id(),
            pins: Arc::clone(&self.cas.pins),
            pinned: Vec::new(),
        })
    }

    fn write(&self, txn: &mut Txn, page_no: LogicalPageNo, bytes: Vec<u8>) -> Result<PageId> {
        let page = Page::new(&self.config.hasher, bytes, self.config.page_size)?;
        let id = page.id();

        // Pin BEFORE the bytes land, so there is no instant in which the page is on disk and
        // collectable. Ordering matters here for exactly the same reason it matters in commit.
        self.cas.pins.pin(id);
        txn.pinned.push(id);

        // Step 1 of the commit protocol: bytes durable in the CAS, still unreferenced.
        self.cas.cas.put(&page)?;

        txn.writes.insert(page_no, Some(id));
        Ok(id)
    }

    fn remove(&self, txn: &mut Txn, page_no: LogicalPageNo) -> Result<()> {
        txn.writes.insert(page_no, None);
        Ok(())
    }

    fn commit(&self, txn: Txn) -> Result<ManifestId> {
        // An empty transaction commits to the manifest it started from.
        if txn.writes.is_empty() {
            return Ok(txn.base);
        }

        let Some((next, _)) = self.derive_next(txn.base, &txn.writes, self.clock.now_ms())? else {
            return Ok(txn.base); // the transaction changes nothing
        };

        // The manifest is the commit record at this level. (substrate-wal turns this into a genuine
        // fsync'd WAL record with an LSN; at the pager alone, manifest durability IS the commit.)
        let id = self.manifests.put(&next)?;
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::new(next));

        // Only now is it visible to readers.
        self.set_head(id);
        Ok(id)
    }

    fn head(&self) -> ManifestId {
        self.head_id()
    }

    fn snapshot(&self) -> Result<ManifestId> {
        // O(1) and genuinely trivial: the current manifest is already durable and already has
        // an id, so a snapshot is *remembering a number*. This is why FlockDB can afford to take
        // a pre-migration snapshot of all ten thousand databases unconditionally.
        Ok(self.head_id())
    }

    fn fork(&self, from: &ManifestId) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(self.at(*from)?))
    }

    fn rewind(&self, to: &ManifestId) -> Result<()> {
        if !self.manifests.contains(*to)? {
            return Err(PagerError::MissingManifest(to.to_hex()));
        }
        self.set_head(*to);
        Ok(())
    }

    fn diff(&self, a: &ManifestId, b: &ManifestId) -> Result<PageDiff> {
        Ok(PageDiff::between(&self.resolve(a)?, &self.resolve(b)?))
    }

    fn diff3(&self, base: &ManifestId, a: &ManifestId, b: &ManifestId) -> Result<ThreeWayDiff> {
        Ok(ThreeWayDiff::compute(
            *base,
            *a,
            *b,
            &self.resolve(base)?,
            &self.resolve(a)?,
            &self.resolve(b)?,
        ))
    }

    fn gc(&self, live_manifests: &[ManifestId]) -> Result<GcStats> {
        // 1. Recompute liveness from the manifests themselves. No counter file exists, and none
        //    ever will (CLAUDE.md rule 9).
        let reachable = self.reachable_manifests(live_manifests)?;

        let mut live_pages: HashSet<PageId> = HashSet::new();
        for id in &reachable {
            // Resolve, do not just read the local body: an overlay only names the pages it CHANGED.
            // Treating an overlay's changes as its full page set would mark every untouched page of
            // every branch as garbage, and GC would delete the database.
            live_pages.extend(self.resolve(id)?.into_values());
        }

        // 2. Pages staged by in-flight transactions are roots too. They are durable and not yet
        //    referenced by any manifest — the exact profile of garbage — but they are about to
        //    be committed.
        live_pages.extend(self.cas.pins.pinned());

        // 3. Sweep. Deletion happens strictly after the live set is fully computed, so an
        //    interrupted GC can only ever leave garbage behind, never remove something live.
        //    Being interrupted is normal; being wrong is not.
        let mut stats = GcStats::default();
        for id in self.cas.cas.list()? {
            if live_pages.contains(&id) {
                stats.pages_retained += 1;
            } else {
                self.cas.cas.remove(id)?;
                stats.pages_swept += 1;
            }
        }
        for id in self.manifests.list()? {
            if reachable.contains(&id) {
                stats.manifests_retained += 1;
            } else {
                self.manifests.remove(id)?;
                // Evict it, or a later read would be served from a cache entry whose backing object
                // we have just deleted — a use-after-free with extra steps.
                self.cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .forget(&id);
                stats.manifests_swept += 1;
            }
        }
        Ok(stats)
    }

    fn manifest(&self, id: &ManifestId) -> Result<Manifest> {
        self.manifests.get(*id)
    }

    fn resolve(&self, id: &ManifestId) -> Result<PageMap> {
        let manifest = self.manifests.get(*id)?;
        self.resolve_manifest(*id, &manifest)
    }

    fn lookup(&self, id: &ManifestId, page_no: LogicalPageNo) -> Result<Option<PageId>> {
        self.lookup_page(*id, page_no)
    }

    fn merge_base(&self, a: &ManifestId, b: &ManifestId) -> Result<Option<ManifestId>> {
        // Walk A's ancestry into a set, then walk B's upward until we hit it. The first hit is the
        // most recent common ancestor, because we walk B newest-first.
        let a_ancestors: HashSet<ManifestId> = self.ancestors(*a)?.into_iter().collect();
        for candidate in self.ancestors(*b)? {
            if a_ancestors.contains(&candidate) {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    fn page_size(&self) -> usize {
        self.config.page_size
    }

    fn pool(&self) -> &str {
        &self.config.pool
    }
}
