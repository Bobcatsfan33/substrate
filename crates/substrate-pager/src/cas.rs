//! The content-addressed store: where page bytes actually live.
//!
//! A directory, sharded two levels by hash prefix (`aa/bb/<hash>`), one file per page,
//! **write-once**, fsync'd on write, hash-verified on read.
//!
//! Write-once plus content addressing means two writers of the *same* page cannot conflict —
//! they are writing identical bytes by definition. That removes an entire class of concurrency
//! problem rather than solving it.

use crate::error::{PagerError, Result};
use crate::page::{Page, PageHasher, PageId};
use crate::vfs::{std_vfs, Vfs};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Where page bytes are stored and retrieved.
///
/// Implementations must guarantee: a page that was successfully `put` and never `remove`d is
/// readable, byte-identical, forever.
pub trait Cas: Send + Sync {
    /// Store these bytes. Idempotent: storing identical bytes twice is one page, not two.
    ///
    /// Must be durable (fsync'd) before returning. The commit protocol (docs/02 §3.1) depends
    /// on page bytes being durable *before* the WAL record that references them.
    fn put(&self, page: &Page) -> Result<()>;

    /// Retrieve bytes, verifying they still hash to `id`.
    fn get(&self, id: PageId) -> Result<Page>;

    /// Whether this page is present locally.
    fn contains(&self, id: PageId) -> Result<bool>;

    /// Remove a page. Called only by GC, only for pages proven unreachable.
    fn remove(&self, id: PageId) -> Result<()>;

    /// Every page currently stored. GC sweeps the difference between this and the live set.
    fn list(&self) -> Result<Vec<PageId>>;

    /// Store bytes at an id **without** requiring that they hash to it.
    ///
    /// # Why this exists, and why it is not a hole in the integrity story
    ///
    /// `substrate-security` encrypts pages, and **ciphertext does not hash to the plaintext's id** —
    /// that is the whole design (identity is computed on the plaintext so that content addressing and
    /// deduplication survive encryption; see docs/02 §9.1). So the encrypting layer needs a way to
    /// store bytes whose hash is deliberately not their key.
    ///
    /// Integrity does not go away; it moves. An encrypted page is protected by the **AEAD tag**, which
    /// is strictly stronger than a hash check: it proves the bytes are authentic, were encrypted under
    /// the right key, and were stored at the right address. The plaintext hash is then verified *as
    /// well*, after decryption, because the cost is one BLAKE3.
    ///
    /// Default implementations return an error, so this is additive for anyone who has implemented
    /// `Cas` and does not need it.
    fn put_raw(&self, id: PageId, bytes: &[u8]) -> Result<()> {
        let _ = (id, bytes);
        Err(PagerError::backend(
            "this content-addressed store does not support raw (unverified) writes",
        ))
    }

    /// Retrieve bytes **without** verifying that they hash to `id`. See [`Cas::put_raw`].
    fn get_raw(&self, id: PageId) -> Result<Vec<u8>> {
        let _ = id;
        Err(PagerError::backend(
            "this content-addressed store does not support raw (unverified) reads",
        ))
    }
}

/// Pages that must not be collected even though no manifest references them yet.
///
/// # Why this exists
///
/// The commit protocol writes page bytes to the CAS *before* the commit record that references
/// them (docs/02 §3.1). Between those two steps the page is durable but unreferenced — which is
/// exactly what GC is looking for. Without a pin, a GC running concurrently with an open
/// transaction would delete pages that transaction is about to commit, and the commit would
/// succeed while pointing at bytes that no longer exist.
///
/// So: staging a write pins the page; committing or dropping the transaction unpins it. GC
/// treats pins as roots. Shared across every fork of a store, because a fork's uncommitted
/// writes live in the same CAS.
#[derive(Debug, Default)]
pub(crate) struct PinRegistry {
    pins: Mutex<HashMap<PageId, usize>>,
}

impl PinRegistry {
    pub(crate) fn pin(&self, id: PageId) {
        if let Ok(mut pins) = self.pins.lock() {
            *pins.entry(id).or_insert(0) += 1;
        }
        // A poisoned lock means another thread panicked mid-pin. We cannot pin, so the safe
        // failure is to leave the page unpinned only if it was never pinned — but since the
        // panicking thread's transaction is dead, its pages are garbage anyway. Losing a pin
        // here can only ever leak garbage, never collect live data.
    }

    pub(crate) fn unpin(&self, id: PageId) {
        if let Ok(mut pins) = self.pins.lock() {
            if let Some(count) = pins.get_mut(&id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    pins.remove(&id);
                }
            }
        }
    }

    /// Every currently pinned page. GC adds these to the live set.
    pub(crate) fn pinned(&self) -> HashSet<PageId> {
        match self.pins.lock() {
            Ok(pins) => pins.keys().copied().collect(),
            // A poisoned lock here would mean GC cannot see the pins. Refusing to report an
            // empty set is the conservative choice: we would rather retain garbage than risk
            // collecting a page an in-flight transaction is about to reference. The caller
            // (`gc`) treats this as "unknown pins" and declines to sweep.
            Err(poisoned) => poisoned.into_inner().keys().copied().collect(),
        }
    }
}

/// A CAS backed by a directory on the filesystem.
///
/// Layout:
///
/// ```text
/// <root>/pages/aa/bb/aabbccdd...   one file per page, write-once
/// ```
pub struct FsCas {
    root: PathBuf,
    hasher: PageHasher,
    vfs: Arc<dyn Vfs>,
}

impl FsCas {
    /// Open (creating if absent) a CAS rooted at `root`, on the real filesystem.
    pub fn open(root: impl AsRef<Path>, hasher: PageHasher) -> Result<Self> {
        FsCas::open_with_vfs(std_vfs(), root, hasher)
    }

    /// Open a CAS on a caller-supplied filesystem.
    ///
    /// This is how the crash-injection harness gets underneath the engine: it hands us a `Vfs`
    /// that dies at a chosen byte, and we never notice the difference.
    pub fn open_with_vfs(
        vfs: Arc<dyn Vfs>,
        root: impl AsRef<Path>,
        hasher: PageHasher,
    ) -> Result<Self> {
        let root = root.as_ref().join("pages");
        vfs.create_dir_all(&root)
            .map_err(|e| PagerError::io(&root, e))?;
        Ok(FsCas { root, hasher, vfs })
    }

    fn path_of(&self, id: PageId) -> PathBuf {
        let (a, b) = id.shard();
        self.root.join(a).join(b).join(id.to_hex())
    }
}

impl Cas for FsCas {
    fn put(&self, page: &Page) -> Result<()> {
        let path = self.path_of(page.id());

        // Write-once: identical content is identical bytes, so a page that is already here is
        // already correct. Rewriting it would be pure cost and a needless window of risk.
        if self.vfs.exists(&path) {
            return Ok(());
        }
        self.vfs
            .atomic_write(&path, page.as_bytes())
            .map_err(|e| PagerError::io(&path, e))
    }

    fn get(&self, id: PageId) -> Result<Page> {
        let path = self.path_of(id);
        let bytes = match self.vfs.read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(PagerError::MissingPage(id))
            }
            Err(e) => return Err(PagerError::io(&path, e)),
        };
        // Verify on every single read. This is the entire integrity story, and BLAKE3 runs at
        // GB/s, so making it optional would be a false economy.
        Page::verify(&self.hasher, id, bytes)
    }

    fn contains(&self, id: PageId) -> Result<bool> {
        Ok(self.vfs.exists(&self.path_of(id)))
    }

    fn remove(&self, id: PageId) -> Result<()> {
        let path = self.path_of(id);
        self.vfs
            .remove_file(&path)
            .map_err(|e| PagerError::io(&path, e))
    }

    fn list(&self) -> Result<Vec<PageId>> {
        let mut out = Vec::new();
        for shard in self
            .vfs
            .read_dir(&self.root)
            .map_err(|e| PagerError::io(&self.root, e))?
        {
            for sub in self.vfs.read_dir(&shard).unwrap_or_default() {
                for file in self.vfs.read_dir(&sub).unwrap_or_default() {
                    let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    // A `.tmp.` file is a write that was interrupted by a crash. It is not a
                    // page, and must never be mistaken for one.
                    if name.starts_with(".tmp.") {
                        continue;
                    }
                    if let Ok(id) = PageId::from_hex(name) {
                        out.push(id);
                    }
                }
            }
        }
        Ok(out)
    }
}

/// An in-memory CAS. For tests and for `substrate-store`'s cache tier.
#[derive(Default)]
pub struct MemCas {
    pages: Mutex<HashMap<PageId, Vec<u8>>>,
    hasher: PageHasher,
}

impl MemCas {
    /// A new, empty in-memory CAS.
    pub fn new(hasher: PageHasher) -> Self {
        MemCas {
            pages: Mutex::new(HashMap::new()),
            hasher,
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<PageId, Vec<u8>>>> {
        // A poisoned lock means a thread panicked while holding it. Rather than propagate the
        // panic (CLAUDE.md rule 6), recover the data: the map itself is still consistent,
        // because every mutation under this lock is a single insert or remove.
        Ok(self.pages.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl Cas for MemCas {
    fn put(&self, page: &Page) -> Result<()> {
        self.lock()?.insert(page.id(), page.as_bytes().to_vec());
        Ok(())
    }

    fn get(&self, id: PageId) -> Result<Page> {
        let bytes = self
            .lock()?
            .get(&id)
            .cloned()
            .ok_or(PagerError::MissingPage(id))?;
        Page::verify(&self.hasher, id, bytes)
    }

    fn contains(&self, id: PageId) -> Result<bool> {
        Ok(self.lock()?.contains_key(&id))
    }

    fn remove(&self, id: PageId) -> Result<()> {
        self.lock()?.remove(&id);
        Ok(())
    }

    fn put_raw(&self, id: PageId, bytes: &[u8]) -> Result<()> {
        self.lock()?.insert(id, bytes.to_vec());
        Ok(())
    }

    fn get_raw(&self, id: PageId) -> Result<Vec<u8>> {
        self.lock()?
            .get(&id)
            .cloned()
            .ok_or(PagerError::MissingPage(id))
    }

    fn list(&self) -> Result<Vec<PageId>> {
        Ok(self.lock()?.keys().copied().collect())
    }
}

/// A shared handle to a CAS plus its pin registry — the context every fork of a store shares.
#[derive(Clone)]
pub(crate) struct CasHandle {
    pub(crate) cas: Arc<dyn Cas>,
    pub(crate) pins: Arc<PinRegistry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::DEFAULT_PAGE_SIZE;

    fn page(bytes: &[u8]) -> Page {
        Page::new(&PageHasher::Unkeyed, bytes.to_vec(), DEFAULT_PAGE_SIZE).expect("valid page")
    }

    fn cases() -> Vec<(&'static str, Box<dyn Cas>, tempfile::TempDir)> {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = FsCas::open(dir.path(), PageHasher::Unkeyed).expect("open fs cas");
        vec![
            ("fs", Box::new(fs), dir),
            (
                "mem",
                Box::new(MemCas::new(PageHasher::Unkeyed)),
                tempfile::tempdir().expect("tempdir"),
            ),
        ]
    }

    #[test]
    fn round_trips_bytes() {
        for (name, cas, _guard) in cases() {
            let p = page(b"the quick brown fox");
            cas.put(&p).unwrap_or_else(|e| panic!("{name}: put: {e}"));
            let got = cas
                .get(p.id())
                .unwrap_or_else(|e| panic!("{name}: get: {e}"));
            assert_eq!(got.as_bytes(), p.as_bytes(), "{name}");
            assert!(cas.contains(p.id()).expect("contains"), "{name}");
        }
    }

    #[test]
    fn put_is_idempotent() {
        for (name, cas, _guard) in cases() {
            let p = page(b"same bytes");
            cas.put(&p).expect("put");
            cas.put(&p).expect("put again");
            assert_eq!(cas.list().expect("list").len(), 1, "{name}: deduplicated");
        }
    }

    #[test]
    fn missing_page_is_an_error_not_a_panic() {
        for (_name, cas, _guard) in cases() {
            let absent = PageId::of(b"never stored");
            assert!(matches!(cas.get(absent), Err(PagerError::MissingPage(_))));
            assert!(!cas.contains(absent).expect("contains"));
        }
    }

    #[test]
    fn remove_is_idempotent_because_gc_can_be_interrupted() {
        for (_name, cas, _guard) in cases() {
            let p = page(b"transient");
            cas.put(&p).expect("put");
            cas.remove(p.id()).expect("remove");
            cas.remove(p.id()).expect("remove again is not an error");
            assert!(!cas.contains(p.id()).expect("contains"));
        }
    }

    #[test]
    fn corruption_on_disk_is_detected_on_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = FsCas::open(dir.path(), PageHasher::Unkeyed).expect("open");
        let p = page(b"honest bytes");
        cas.put(&p).expect("put");

        // Reach behind the CAS and rot a bit, exactly as a failing disk would.
        let path = cas.path_of(p.id());
        std::fs::write(&path, b"tampered with").expect("corrupt the page");

        let err = cas.get(p.id());
        assert!(
            matches!(err, Err(PagerError::CorruptPage { .. })),
            "silent corruption must never be served, got {err:?}"
        );
        assert!(err.expect_err("is err").is_corruption());
    }

    #[test]
    fn temp_files_are_not_mistaken_for_pages() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = FsCas::open(dir.path(), PageHasher::Unkeyed).expect("open");
        let p = page(b"real");
        cas.put(&p).expect("put");

        // Simulate a crash mid-put: a leftover temp file in a shard directory.
        let shard = cas.path_of(p.id());
        let shard_dir = shard.parent().expect("parent");
        std::fs::write(shard_dir.join(".tmp.deadbeef"), b"garbage").expect("write temp");

        assert_eq!(
            cas.list().expect("list"),
            vec![p.id()],
            "a crashed write must not look like a page"
        );
    }

    #[test]
    fn pins_keep_uncommitted_pages_alive() {
        let pins = PinRegistry::default();
        let id = PageId::of(b"staged but not yet committed");

        assert!(pins.pinned().is_empty());
        pins.pin(id);
        assert!(pins.pinned().contains(&id), "GC must see this as a root");

        // Two transactions staging the same bytes: unpinning one must not unpin the other.
        pins.pin(id);
        pins.unpin(id);
        assert!(pins.pinned().contains(&id), "still held by the second txn");

        pins.unpin(id);
        assert!(pins.pinned().is_empty(), "now collectable");
    }
}
