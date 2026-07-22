//! The tiered CAS: a local cache in front of object storage.
//!
//! ```text
//!   read  ──► local CAS ──hit──► bytes
//!                 │
//!               miss
//!                 │
//!                 ▼
//!            object storage ──► verify hash ──► fill local ──► bytes
//!
//!   write ──► local CAS (durable, fsync'd) ──► queue upload ──► object storage
//!                                                                     │
//!                                              mark durable ◄─────────┘
//!                                                    │
//!                                        NOW, and only now, evictable
//! ```
//!
//! # The one rule that makes eviction safe
//!
//! **A page is evictable only once it is confirmed durable in object storage.** Not "queued for
//! upload". Not "probably uploaded". Confirmed.
//!
//! This single rule is why cache eviction cannot lose data, and it is worth more than any eviction
//! policy. An LRU that can evict a page whose only copy is local is not a cache — it is a delete.
//!
//! # The sync/async tradeoff, stated openly
//!
//! `PageStore` and the `Cas` trait beneath it are **synchronous** (CLAUDE.md rule 7), because
//! deterministic replay and crash injection require deterministic execution, and because the SQL
//! kernel above us (DuckDB) is synchronous too. Object storage is **asynchronous**.
//!
//! So a cache miss blocks: [`TieredCas::get`] enters the tokio runtime, awaits the fetch, and
//! returns. We considered the alternative — making the entire engine `async` all the way down to the
//! pager — and rejected it. It would buy a non-blocking miss and cost us deterministic replay, which
//! is the property every durability guarantee in this engine rests on. Blocking one worker thread on
//! an S3 GET is a price worth paying; being unable to prove that recovery is correct is not.
//!
//! **This requires a multi-threaded tokio runtime.** A cache miss on a current-thread runtime cannot
//! block without deadlocking, and we would rather say so here than have someone discover it in
//! production.

use crate::error::{Result, StoreError};
use crate::remote::RemoteTier;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use substrate_pager::{Cas, Page, PageHasher, PageId, PagerError, Result as PagerResult};
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore};

/// The most object-storage GETs a single [`TieredCas::get_batch`] keeps in flight at once.
///
/// Coalescing here is **dedupe-by-object**, not ranging — a content-addressed store keys one object per
/// page, so distinct pages are distinct objects with nothing between them to range over. The win is
/// overlapping the distinct objects' round-trips over one pooled, keep-alive client instead of paying
/// them one at a time. Bounded because across a wide-area link a *burst* of fresh connections contends
/// more than it parallelises (the sibling engine measured a per-page fan-out lose to serial for exactly
/// this reason). This width, paired with connection reuse, is the knob the wide-area harnesses
/// calibrate; 16 is the correctness-phase default, not a tuned latency figure.
const BATCH_FETCH_WIDTH: usize = 16;

/// What the tier knows about every page it has seen.
#[derive(Default)]
struct TierState {
    /// Pages confirmed present in object storage. **Only these may be evicted.**
    durable: HashSet<PageId>,
    /// Pages in the local CAS whose upload has not been confirmed. Never evictable.
    pending: HashSet<PageId>,
    /// Logical clock per page, for LRU. Cheap, and good enough — an exact LRU would need a
    /// linked hash map and would not evict meaningfully better pages.
    touched: HashMap<PageId, u64>,
    /// Approximate bytes held locally, for eviction decisions.
    local_bytes: u64,
    /// Sizes, so eviction knows what it is reclaiming.
    sizes: HashMap<PageId, u64>,
    clock: u64,
}

/// Cache statistics. Wired into the metrics hooks in P5.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TierStats {
    /// Reads served from the local cache.
    pub hits: u64,
    /// Reads that had to go to object storage.
    pub misses: u64,
    /// Pages uploaded.
    pub uploads: u64,
    /// Pages evicted from the local cache.
    pub evictions: u64,
    /// Bytes currently held locally.
    pub local_bytes: u64,
    /// Pages known durable in object storage.
    pub durable: u64,
    /// Pages written locally but not yet confirmed remote. These are unevictable.
    pub pending_upload: u64,
}

/// A local CAS backed by object storage.
pub struct TieredCas {
    local: Arc<dyn Cas>,
    remote: RemoteTier,
    hasher: PageHasher,
    handle: Handle,
    state: Mutex<TierState>,
    stats: Mutex<TierStats>,
    /// Woken whenever there is something to upload.
    work: Arc<Notify>,
}

impl TieredCas {
    /// Wrap a local CAS with an object-storage tier.
    ///
    /// Must be called from within a **multi-threaded** tokio runtime, or with a handle to one.
    pub fn new(local: Arc<dyn Cas>, remote: RemoteTier, hasher: PageHasher) -> Result<Arc<Self>> {
        let handle = Handle::try_current().map_err(|_| StoreError::NoRuntime)?;
        Ok(Arc::new(TieredCas {
            local,
            remote,
            hasher,
            handle,
            state: Mutex::new(TierState::default()),
            stats: Mutex::new(TierStats::default()),
            work: Arc::new(Notify::new()),
        }))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, TierState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn stats_mut(&self) -> std::sync::MutexGuard<'_, TierStats> {
        self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Current cache statistics.
    pub fn stats(&self) -> TierStats {
        let state = self.state();
        let mut stats = *self.stats_mut();
        stats.local_bytes = state.local_bytes;
        stats.durable = state.durable.len() as u64;
        stats.pending_upload = state.pending.len() as u64;
        stats
    }

    /// The pool this store belongs to.
    pub fn pool(&self) -> &str {
        self.remote.pool()
    }

    /// **Fetch many pages at once, coalescing the object-storage GETs.**
    ///
    /// Returns the pages in the same order as `ids`, each **byte-identical** to what [`Cas::get`] would
    /// return for that id — the differential oracle (`tests/batch_fetch.rs`) proves this for arbitrary
    /// sets. Pages already in the local cache are served from it; the rest are **deduplicated to their
    /// distinct backing objects** (one content-hashed object per page, so identical pages share a single
    /// GET) and fetched **concurrently** across those objects over the tier's one pooled, keep-alive
    /// client, each hash-verified on arrival exactly as the single-page path verifies it.
    ///
    /// The win over N serial [`Cas::get`] calls is that the distinct objects' round-trips overlap in
    /// time. Width is bounded by [`BATCH_FETCH_WIDTH`]; there is no *ranging* and no over-fetch, because
    /// a content-addressed store has no intervening pages between two objects.
    ///
    /// **All-or-nothing.** If any object cannot be fetched — a failed GET, a missing object, or bytes
    /// whose hash does not match the id — the whole call returns `Err`, nothing is written to the cache
    /// for objects not proven good, and **no partial set is ever returned as success**. A torn batch
    /// must never masquerade as a complete one — the same rule the single-page path holds.
    pub fn get_batch(&self, ids: &[PageId]) -> PagerResult<Vec<Page>> {
        // 1. Serve local hits; collect the DISTINCT missing objects (dedupe by content id).
        let mut resolved: HashMap<PageId, Page> = HashMap::new();
        let mut to_fetch: Vec<PageId> = Vec::new();
        let mut seen: HashSet<PageId> = HashSet::new();
        for &id in ids {
            if !seen.insert(id) {
                continue; // a duplicate id resolves to the same page — one GET, not two
            }
            match self.local.get(id) {
                Ok(page) => {
                    self.touch(id, page.len() as u64);
                    self.stats_mut().hits += 1;
                    resolved.insert(id, page);
                }
                Err(PagerError::MissingPage(_)) => to_fetch.push(id),
                Err(e) => return Err(e),
            }
        }

        // 2. Fetch the distinct missing objects concurrently, bounded width, over the pooled client.
        if !to_fetch.is_empty() {
            self.stats_mut().misses += to_fetch.len() as u64;
            let remote = &self.remote;
            let sem = Arc::new(Semaphore::new(BATCH_FETCH_WIDTH));
            let fetched: Vec<(PageId, Vec<u8>)> = self.block(async {
                let pending = to_fetch.iter().map(|&id| {
                    let sem = Arc::clone(&sem);
                    let key = remote.page_key(id);
                    async move {
                        // Bound concurrency. A closed semaphore cannot happen here (we own it), so a
                        // failed acquire proceeds unbounded rather than panicking (rule 6).
                        let _permit = sem.acquire_owned().await.ok();
                        match remote.get(&key).await {
                            Ok(Some(bytes)) => Ok((id, bytes)),
                            // Neither tier has it — same meaning as the single-page path's `None`.
                            Ok(None) => Err(PagerError::MissingPage(id)),
                            Err(e) => Err(PagerError::backend(e)),
                        }
                    }
                });
                // Short-circuits on the first error and drops the rest, so a failed or missing object
                // aborts the batch before anything is written to the cache.
                futures::future::try_join_all(pending).await
            })?;

            // 3. Verify EVERY object before filling ANY — a corrupt page fails the whole batch with the
            //    cache untouched, so a partial fill can never be mistaken for a complete one.
            let mut verified: Vec<(PageId, Page)> = Vec::with_capacity(fetched.len());
            for (id, bytes) in fetched {
                let page = Page::new(&self.hasher, bytes, usize::MAX)?;
                if page.id() != id {
                    return Err(PagerError::CorruptPage {
                        expected: id,
                        actual: page.id(),
                        len: page.len(),
                    });
                }
                verified.push((id, page));
            }
            for (id, page) in verified {
                self.local.put(&page)?;
                {
                    let mut state = self.state();
                    state.durable.insert(id);
                    state.pending.remove(&id);
                }
                self.touch(id, page.len() as u64);
                resolved.insert(id, page);
            }
        }

        // 4. Return the pages in the caller's order (duplicates resolve to the same page).
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            match resolved.get(&id) {
                Some(page) => out.push(page.clone()),
                // Unreachable: every id was a local hit or in to_fetch, and to_fetch all resolved.
                None => return Err(PagerError::MissingPage(id)),
            }
        }
        Ok(out)
    }

    /// Enter the runtime from synchronous code.
    ///
    /// See the module docs. Inside a runtime we must hand the thread back with `block_in_place`
    /// (which is why a multi-threaded runtime is required); outside one, we can simply block.
    fn block<F: std::future::Future>(&self, fut: F) -> F::Output {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => self.handle.block_on(fut),
        }
    }

    fn touch(&self, id: PageId, size: u64) {
        let mut state = self.state();
        state.clock += 1;
        let clock = state.clock;
        state.touched.insert(id, clock);
        if state.sizes.insert(id, size).is_none() {
            state.local_bytes += size;
        }
    }

    /// Upload every page that is not yet confirmed durable, and wait for it.
    ///
    /// This is what `sleep()` calls. When it returns `Ok`, **every page this store holds is in
    /// object storage** — which is the precondition for dropping the local copy without losing
    /// anything.
    pub async fn flush(&self) -> Result<()> {
        loop {
            let batch: Vec<PageId> = {
                let state = self.state();
                state.pending.iter().copied().collect()
            };
            if batch.is_empty() {
                return Ok(());
            }

            for id in batch {
                // The bytes must come from the local CAS: they are the only copy.
                let page = self.local.get(id)?;
                let key = self.remote.page_key(id);
                self.remote.put(&key, page.as_bytes().to_vec()).await?;

                let mut state = self.state();
                state.pending.remove(&id);
                state.durable.insert(id);
                drop(state);
                self.stats_mut().uploads += 1;
            }
        }
    }

    /// Run the background uploader until the store is dropped.
    ///
    /// Spawned by [`TieredStore::open`]. Uploads are best-effort and idempotent; `flush()` is the
    /// authoritative path, and a failure here simply leaves the page `pending` — which means
    /// unevictable, which means safe.
    pub async fn upload_loop(self: Arc<Self>) {
        loop {
            self.work.notified().await;
            // An error here is not fatal. The page stays pending, stays local, stays unevictable,
            // and the next flush or notification retries it. The failure mode of a failed upload is
            // "the cache gets bigger", not "the data is gone".
            let _ = self.flush().await;
        }
    }

    /// Evict least-recently-used pages until the local cache is under `max_bytes`.
    ///
    /// **Only durable pages are candidates.** A page that has not been confirmed in object storage
    /// is skipped no matter how cold it is — evicting it would be deleting the only copy.
    ///
    /// Consequence, and it is the correct one: a store whose uploads are failing will grow past
    /// `max_bytes` rather than lose data. That is backpressure, and it is visible in
    /// [`TierStats::pending_upload`].
    pub fn evict_to(&self, max_bytes: u64) -> Result<u64> {
        let mut evicted = 0u64;

        loop {
            let victim = {
                let state = self.state();
                if state.local_bytes <= max_bytes {
                    break;
                }
                // Coldest durable page. `pending` pages are not candidates — that is the rule.
                state
                    .durable
                    .iter()
                    .filter(|id| !state.pending.contains(id))
                    .min_by_key(|id| state.touched.get(id).copied().unwrap_or(0))
                    .copied()
            };

            let Some(id) = victim else {
                // Nothing left that is safe to evict. The cache stays over budget, and that is the
                // right answer: over-budget is a performance problem, and evicting a non-durable
                // page is a data-loss problem.
                break;
            };

            self.local.remove(id)?;

            let mut state = self.state();
            state.durable.remove(&id);
            state.touched.remove(&id);
            if let Some(size) = state.sizes.remove(&id) {
                state.local_bytes = state.local_bytes.saturating_sub(size);
                evicted += size;
            }
            drop(state);
            self.stats_mut().evictions += 1;
        }

        Ok(evicted)
    }

    /// Drop every locally cached page. Used by `sleep()` *after* `flush()`.
    pub fn drop_local(&self) -> Result<()> {
        // Refuse to drop anything that is not durable remotely. `sleep()` flushes first, so this
        // should never fire — but "should never" is exactly the class of assumption that loses
        // people's data, so we check.
        {
            let state = self.state();
            if !state.pending.is_empty() {
                return Err(StoreError::PageLost(format!(
                    "{} page(s) are not yet durable in object storage; \
                     dropping local state now would lose them",
                    state.pending.len()
                )));
            }
        }

        for id in self.local.list()? {
            self.local.remove(id)?;
        }
        let mut state = self.state();
        state.durable.clear();
        state.touched.clear();
        state.sizes.clear();
        state.local_bytes = 0;
        Ok(())
    }
}

impl Cas for TieredCas {
    // NOTE: these return the *pager's* Result, not this crate's. The `Cas` trait belongs to
    // substrate-pager, and a trait impl speaks the trait's language.
    fn put(&self, page: &Page) -> PagerResult<()> {
        // Local first, and fsync'd. The page is durable *somewhere* before we return, which is what
        // the commit protocol (docs/02 §3.1) requires. The upload is what makes it durable
        // *elsewhere*, and until it lands, this page cannot be evicted.
        self.local.put(page)?;

        let id = page.id();
        let size = page.len() as u64;
        {
            let mut state = self.state();
            if !state.durable.contains(&id) {
                state.pending.insert(id);
            }
        }
        self.touch(id, size);
        self.work.notify_one();
        Ok(())
    }

    fn get(&self, id: PageId) -> PagerResult<Page> {
        match self.local.get(id) {
            Ok(page) => {
                self.touch(id, page.len() as u64);
                self.stats_mut().hits += 1;
                return Ok(page);
            }
            Err(PagerError::MissingPage(_)) => {} // fall through to the remote tier
            Err(e) => return Err(e),
        }

        self.stats_mut().misses += 1;

        // Cache miss. Block on the fetch — see the module docs for why this is the right trade.
        let key = self.remote.page_key(id);
        let bytes = self
            .block(async { self.remote.get(&key).await })
            .map_err(PagerError::backend)?;

        let Some(bytes) = bytes else {
            // Neither tier has it. Eviction refuses to touch a non-durable page, so this is not
            // reachable through any code path we control — which means if it happens, something
            // outside us deleted it, and saying so is more useful than a generic not-found.
            return Err(PagerError::MissingPage(id));
        };

        // Verify on arrival. Bytes that crossed a network are exactly the bytes we want to
        // re-hash: content addressing means a corrupted download cannot masquerade as the page.
        let page = Page::new(&self.hasher, bytes, usize::MAX)?;
        if page.id() != id {
            return Err(PagerError::CorruptPage {
                expected: id,
                actual: page.id(),
                len: page.len(),
            });
        }

        // Fill the local cache. It came *from* object storage, so it is durable there by
        // definition — mark it evictable immediately.
        self.local.put(&page)?;
        {
            let mut state = self.state();
            state.durable.insert(id);
            state.pending.remove(&id);
        }
        self.touch(id, page.len() as u64);

        Ok(page)
    }

    fn contains(&self, id: PageId) -> PagerResult<bool> {
        if self.local.contains(id)? {
            return Ok(true);
        }
        Ok(self.state().durable.contains(&id))
    }

    fn remove(&self, id: PageId) -> PagerResult<()> {
        // GC removing a page removes it from the local cache. Removing it from object storage is a
        // separate, deliberate operation: a page that is still referenced by a *sleeping* database
        // is live even though no local manifest mentions it, and deleting remote objects on a local
        // GC pass would be how we quietly destroy a customer's hibernating databases.
        self.local.remove(id)?;
        let mut state = self.state();
        state.durable.remove(&id);
        state.pending.remove(&id);
        state.touched.remove(&id);
        if let Some(size) = state.sizes.remove(&id) {
            state.local_bytes = state.local_bytes.saturating_sub(size);
        }
        Ok(())
    }

    fn list(&self) -> PagerResult<Vec<PageId>> {
        self.local.list()
    }
}

impl std::fmt::Debug for TieredCas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredCas")
            .field("pool", &self.remote.pool())
            .field("stats", &self.stats())
            .finish()
    }
}
