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
use crate::manifest::{FsManifestStore, Manifest, ManifestId, ManifestStore, MemManifestStore};
use crate::page::{validate_page_size, LogicalPageNo, Page, PageHasher, PageId, DEFAULT_PAGE_SIZE};
use std::collections::{BTreeMap, HashSet};
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

    /// The page size this store was created with.
    fn page_size(&self) -> usize;

    /// The dedup pool this store belongs to.
    fn pool(&self) -> &str;
}

/// The reference implementation of [`PageStore`].
pub struct Pager {
    cas: CasHandle,
    manifests: Arc<dyn ManifestStore>,
    head: Mutex<ManifestId>,
    config: StoreConfig,
    clock: Arc<dyn Clock>,
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
        validate_page_size(config.page_size)?;
        Self::guard_keyed_hash(&config)?;

        let root = root.as_ref();
        let cas = FsCas::open(root, config.hasher.clone())?;
        let manifests = FsManifestStore::open(root)?;

        Pager::assemble(
            Arc::new(cas),
            Arc::new(manifests),
            config,
            Arc::new(SystemClock),
        )
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
        let root = Manifest::empty(config.page_size, clock.now_ms());
        let head = manifests.put(&root)?;

        Ok(Pager {
            cas: CasHandle {
                cas,
                pins: Arc::new(PinRegistry::default()),
            },
            manifests,
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
            head: Mutex::new(manifest),
            config: self.config.clone(),
            clock: Arc::clone(&self.clock),
        })
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
            // collect something we simply failed to read.
            let manifest = self.manifests.get(id)?;
            if let Some(parent) = manifest.parent {
                stack.push(parent);
            }
        }
        Ok(seen)
    }
}

impl PageStore for Pager {
    fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page> {
        let manifest_data = self.manifests.get(*manifest)?;
        let page_id = manifest_data
            .get(page_no)
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
        // An empty transaction commits to the manifest it started from. It does not append a
        // duplicate to history — content addressing would give it the same id anyway, but being
        // explicit costs nothing and makes the intent legible.
        if txn.writes.is_empty() {
            return Ok(txn.base);
        }

        let base = self.manifests.get(txn.base)?;
        let next = base.derive(&txn.writes, txn.base, self.clock.now_ms());

        // A transaction that rewrites the bytes already there changes nothing, and must not
        // append a duplicate manifest to history. Otherwise an idempotent retry — a writer
        // replaying the same batch after a timeout, which is *normal* — grows the manifest DAG
        // forever while the database stands still.
        if next.pages == base.pages {
            return Ok(txn.base);
        }

        // Step 2 of the commit protocol: the manifest is the commit record here. Once this
        // returns, the transaction has happened. (substrate-wal turns this into a genuine
        // fsync'd WAL record with an LSN; at the pager level, manifest durability *is* the
        // commit point.)
        let id = self.manifests.put(&next)?;

        // Step 3: now, and only now, is it visible to readers.
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
        Ok(PageDiff::between(
            &self.manifests.get(*a)?,
            &self.manifests.get(*b)?,
        ))
    }

    fn diff3(&self, base: &ManifestId, a: &ManifestId, b: &ManifestId) -> Result<ThreeWayDiff> {
        Ok(ThreeWayDiff::compute(
            *base,
            *a,
            *b,
            &self.manifests.get(*base)?,
            &self.manifests.get(*a)?,
            &self.manifests.get(*b)?,
        ))
    }

    fn gc(&self, live_manifests: &[ManifestId]) -> Result<GcStats> {
        // 1. Recompute liveness from the manifests themselves. No counter file exists, and none
        //    ever will (CLAUDE.md rule 9).
        let reachable = self.reachable_manifests(live_manifests)?;

        let mut live_pages: HashSet<PageId> = HashSet::new();
        for id in &reachable {
            let manifest = self.manifests.get(*id)?;
            live_pages.extend(manifest.referenced_pages());
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
                stats.manifests_swept += 1;
            }
        }
        Ok(stats)
    }

    fn manifest(&self, id: &ManifestId) -> Result<Manifest> {
        self.manifests.get(*id)
    }

    fn page_size(&self) -> usize {
        self.config.page_size
    }

    fn pool(&self) -> &str {
        &self.config.pool
    }
}
