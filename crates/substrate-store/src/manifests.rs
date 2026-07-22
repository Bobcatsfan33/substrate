//! Manifests, tiered to object storage.
//!
//! # Why this exists — a bug that hid for a whole phase
//!
//! In P3, `sleep()` uploaded exactly one manifest: the head. That was correct, because every manifest
//! was **self-contained** — a complete map of every logical page.
//!
//! P4 introduced **overlay manifests**. An overlay records only what *changed* and defers everything
//! else to its base. Nobody went back and asked what that does to sleep, and the answer is: it breaks
//! it. An overlay without its base cannot resolve the pages it did not touch, so a woken database
//! could read whatever the top overlay happened to hold and would fail on everything else.
//!
//! The lifecycle test did not catch it, because it wrote every page in a single commit — so every page
//! *was* in the top overlay and the walk never needed the base. **A test that only ever exercises the
//! easy path reports green while proving nothing**, and this one did so for an entire phase.
//!
//! # The fix, and why it is a tier rather than a bigger upload
//!
//! Manifests now read through to object storage exactly as pages do. `sleep()` uploads the head *and
//! its whole ancestry* — both edges, the overlay base **and** the history parent, which is the same
//! closure GC calls "reachable". Manifests are small; losing history would not be.
//!
//! `wake()` fetches only the head. Everything else — the overlay chain beneath it, and the parents
//! behind it — arrives on demand. That is what keeps waking a 100 GB database from moving 100 GB.

use crate::error::{Result, StoreError};
use crate::remote::RemoteTier;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use substrate_pager::{
    Manifest, ManifestId, ManifestStore, PagerError, Result as PagerResult, Vfs,
};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

/// A local manifest store backed by object storage.
///
/// Same sync/async trade as [`crate::TieredCas`], for the same reason: the pager is synchronous
/// because deterministic replay and crash injection require deterministic execution, so a miss
/// blocks. **Requires a multi-threaded tokio runtime.**
pub struct TieredManifestStore {
    local: Arc<dyn ManifestStore>,
    remote: RemoteTier,
    handle: Handle,
    /// Manifests confirmed present in object storage.
    durable: Mutex<HashSet<ManifestId>>,
}

impl TieredManifestStore {
    /// Wrap a local manifest store with an object-storage tier.
    pub fn new(local: Arc<dyn ManifestStore>, remote: RemoteTier) -> Result<Arc<Self>> {
        let handle = Handle::try_current().map_err(|_| StoreError::NoRuntime)?;
        Ok(Arc::new(TieredManifestStore {
            local,
            remote,
            handle,
            durable: Mutex::new(HashSet::new()),
        }))
    }

    fn durable(&self) -> std::sync::MutexGuard<'_, HashSet<ManifestId>> {
        self.durable.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn block<F: std::future::Future>(&self, fut: F) -> F::Output {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => self.handle.block_on(fut),
        }
    }

    /// Upload a manifest **and its entire ancestry** — both edges.
    ///
    /// A manifest has two backward edges, and they come apart exactly where you stop paying
    /// attention (see `substrate-pager`'s GC):
    ///
    /// - **`overlay_base`** is a *storage* edge. Without it the manifest is **unreadable** — it cannot
    ///   resolve the pages it did not touch. Omitting this is the bug this module exists to fix.
    /// - **`parent`** is a *history* edge. Without it the database still reads, but its past is gone,
    ///   and a database whose past is gone is not one LoomDB's provenance layer can work with.
    ///
    /// So we upload both. Manifests are small; losing either would not be.
    pub async fn upload_closure(&self, head: ManifestId) -> Result<usize> {
        let mut uploaded = 0usize;
        let mut seen: HashSet<ManifestId> = HashSet::new();
        let mut stack = vec![head];

        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }

            let manifest = self.local.get(id)?;

            let key = self.remote.manifest_key(id);
            self.remote.put(&key, manifest.encode()?).await?;
            self.durable().insert(id);
            uploaded += 1;

            if let Some(base) = manifest.overlay_base() {
                stack.push(base); // the STORAGE edge — without it the manifest cannot be read
            }
            if let Some(parent) = manifest.parent {
                stack.push(parent); // the HISTORY edge — without it the past is gone
            }
        }
        Ok(uploaded)
    }

    /// How many manifests are known durable remotely.
    pub fn durable_count(&self) -> usize {
        self.durable().len()
    }

    /// **Fetch many manifests at once, coalescing the object-storage GETs** — the manifest twin of
    /// [`TieredCas::get_batch`](crate::TieredCas::get_batch).
    ///
    /// A cold wake resolves the overlay chain by *pointer-chasing* — head → overlay-base → base, each
    /// id inside the previous — which is inescapably serial and un-batchable. But once a wake's chain
    /// has been *learned* (the warm set), its ids are all known up front, so they can be fetched in one
    /// concurrent, deduped batch — collapsing the pointer-chase into a single round-trip. That is the
    /// only context in which batching manifests helps, and it is exactly the learned-warm-set wake.
    ///
    /// Returns the manifests in the same order as `ids`, each **byte-identical** to what [`get`] would
    /// return and each **hash-verified** on arrival (a corrupt manifest is a corrupt view of the whole
    /// database — the worst object in the system). **All-or-nothing**: a failed GET, a missing object,
    /// or a hash mismatch fails the whole batch; nothing is written to the cache for objects not proven
    /// good, and no partial set is ever returned as success.
    ///
    /// [`get`]: ManifestStore::get
    pub fn get_batch(&self, ids: &[ManifestId]) -> PagerResult<Vec<Manifest>> {
        let mut resolved: HashMap<ManifestId, Manifest> = HashMap::new();
        let mut to_fetch: Vec<ManifestId> = Vec::new();
        let mut seen: HashSet<ManifestId> = HashSet::new();
        for &id in ids {
            if !seen.insert(id) {
                continue;
            }
            match self.local.get(id) {
                Ok(manifest) => {
                    resolved.insert(id, manifest);
                }
                Err(PagerError::MissingManifest(_)) => to_fetch.push(id),
                Err(e) => return Err(e),
            }
        }

        if !to_fetch.is_empty() {
            let remote = &self.remote;
            // Manifests are few (a chain of a handful), so this width is never the binding constraint;
            // it matches the page path's bound for one obvious knob. See TieredCas::get_batch.
            let sem = Arc::new(Semaphore::new(16));
            let fetched: Vec<(ManifestId, Vec<u8>)> = self.block(async {
                let pending = to_fetch.iter().map(|&id| {
                    let sem = Arc::clone(&sem);
                    let key = remote.manifest_key(id);
                    async move {
                        let _permit = sem.acquire_owned().await.ok();
                        match remote.get(&key).await {
                            Ok(Some(bytes)) => Ok((id, bytes)),
                            Ok(None) => Err(PagerError::MissingManifest(id.to_hex())),
                            Err(e) => Err(PagerError::backend(e)),
                        }
                    }
                });
                futures::future::try_join_all(pending).await
            })?;

            // Verify EVERY manifest before filling ANY — a corrupt one fails the whole batch with the
            // cache untouched, so a partial fill can never be mistaken for a complete one.
            let mut verified: Vec<(ManifestId, Manifest)> = Vec::with_capacity(fetched.len());
            for (id, bytes) in fetched {
                let manifest = Manifest::decode(&bytes)?;
                if manifest.id()? != id {
                    return Err(PagerError::MissingManifest(format!(
                        "{} (bytes fetched from object storage hash to something else)",
                        id.to_hex()
                    )));
                }
                verified.push((id, manifest));
            }
            for (id, manifest) in verified {
                self.local.put(&manifest)?;
                self.durable().insert(id);
                resolved.insert(id, manifest);
            }
        }

        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            match resolved.get(&id) {
                Some(manifest) => out.push(manifest.clone()),
                None => return Err(PagerError::MissingManifest(id.to_hex())),
            }
        }
        Ok(out)
    }
}

impl ManifestStore for TieredManifestStore {
    fn put(&self, manifest: &Manifest) -> PagerResult<ManifestId> {
        self.local.put(manifest)
    }

    fn get(&self, id: ManifestId) -> PagerResult<Manifest> {
        match self.local.get(id) {
            Ok(manifest) => return Ok(manifest),
            Err(PagerError::MissingManifest(_)) => {} // fall through to the remote tier
            Err(e) => return Err(e),
        }

        let key = self.remote.manifest_key(id);
        let bytes = self
            .block(async { self.remote.get(&key).await })
            .map_err(PagerError::backend)?;

        let Some(bytes) = bytes else {
            return Err(PagerError::MissingManifest(id.to_hex()));
        };

        // Verify on arrival. A manifest is content-addressed, so a corrupted download cannot
        // masquerade as the real one — and a corrupt manifest is a corrupt view of the *entire*
        // database, which is the worst object in this system.
        let manifest = Manifest::decode(&bytes)?;
        if manifest.id()? != id {
            return Err(PagerError::MissingManifest(format!(
                "{} (bytes fetched from object storage hash to something else)",
                id.to_hex()
            )));
        }

        // Fill the local cache. It came *from* object storage, so it is durable there by definition.
        self.local.put(&manifest)?;
        self.durable().insert(id);
        Ok(manifest)
    }

    fn contains(&self, id: ManifestId) -> PagerResult<bool> {
        if self.local.contains(id)? {
            return Ok(true);
        }
        if self.durable().contains(&id) {
            return Ok(true);
        }

        // Ask object storage. This matters more than it looks: `Pager::at` — which is what `fork()`
        // and every "open at this commit" call goes through — checks `contains` before it will touch
        // a manifest. A `contains` that only knows about the local disk reports "missing" for every
        // manifest of a database that has just WOKEN, and the woken database refuses to open itself.
        //
        // A HEAD request is cheap. Being unable to open a database you just restored is not.
        let key = self.remote.manifest_key(id);
        let exists = self
            .block(async { self.remote.exists(&key).await })
            .map_err(PagerError::backend)?;

        if exists {
            self.durable().insert(id);
        }
        Ok(exists)
    }

    fn remove(&self, id: ManifestId) -> PagerResult<()> {
        // GC removing a manifest removes the LOCAL copy. Removing it from object storage is a
        // separate, deliberate act: a manifest that is still a live branch head of a *sleeping*
        // database is live even though nothing local refers to it, and deleting remote objects on a
        // local GC pass is how you quietly destroy a customer's hibernating databases.
        self.local.remove(id)?;
        self.durable().remove(&id);
        Ok(())
    }

    fn list(&self) -> PagerResult<Vec<ManifestId>> {
        self.local.list()
    }
}

impl std::fmt::Debug for TieredManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredManifestStore")
            .field("durable", &self.durable_count())
            .finish()
    }
}

/// A `Vfs`-backed local manifest store, for the tier to sit on.
pub(crate) fn local_manifests(
    vfs: Arc<dyn Vfs>,
    root: &std::path::Path,
) -> Result<Arc<dyn ManifestStore>> {
    Ok(Arc::new(substrate_pager::FsManifestStore::open_with_vfs(
        vfs, root,
    )?))
}
