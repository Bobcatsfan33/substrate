//! Manifests: the complete state of one database at one instant.
//!
//! A manifest is a **value**, not a location — an ordered map from logical page number to
//! [`PageId`], plus metadata, itself content-addressed.
//!
//! That single property is why the expensive-sounding operations are free:
//!
//! | Operation | What actually happens |
//! |---|---|
//! | snapshot | remember a `ManifestId` |
//! | fork     | start a new overlay on top of a `ManifestId` |
//! | rewind   | point a branch at an older `ManifestId` |
//!
//! No bytes are copied in any of them. That is the entire trick (docs/02 §3.1).

use crate::error::{PagerError, Result};
use crate::page::{LogicalPageNo, PageId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The content hash of a serialized [`Manifest`].
///
/// Manifests are content-addressed for the same reason pages are: an id that *is* the content
/// cannot be stale, cannot be ambiguous, and cannot silently refer to something that changed
/// underneath it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ManifestId([u8; 32]);

impl ManifestId {
    /// The raw 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Construct from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        ManifestId(bytes)
    }

    /// Lowercase hex, 64 characters.
    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for byte in self.0 {
            use fmt::Write as _;
            let _ = write!(s, "{byte:02x}");
        }
        s
    }

    /// Parse from 64 hex characters.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 64 {
            return Err(PagerError::MalformedId(s.to_string()));
        }
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            let byte = s
                .get(i * 2..i * 2 + 2)
                .ok_or_else(|| PagerError::MalformedId(s.to_string()))?;
            *slot =
                u8::from_str_radix(byte, 16).map_err(|_| PagerError::MalformedId(s.to_string()))?;
        }
        Ok(ManifestId(out))
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..12])
    }
}

impl fmt::Debug for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ManifestId({})", &self.to_hex()[..12])
    }
}

/// The current on-disk manifest format version.
///
/// Bumping this is a format change, which per CLAUDE.md rule 3 requires a fuzz-target update
/// in the same commit.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// The complete state of a database at one instant.
///
/// # Determinism
///
/// The page map is a [`BTreeMap`], so serialization is in ascending logical-page order,
/// always. This is not a stylistic preference: **deterministic replay** (docs/02 §3.1) requires
/// that replaying the same WAL twice produces byte-identical manifests, and a `HashMap`'s
/// iteration order would silently destroy that property — and with it, every guarantee that
/// depends on recovery being verifiable.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version, for upgrade paths.
    pub format_version: u32,
    /// The whole database: every logical page, and the content that is currently at it.
    pub pages: BTreeMap<LogicalPageNo, PageId>,
    /// The manifest this one was derived from. `None` for a root manifest.
    ///
    /// This is the edge that makes the manifest DAG, and it is what branch trees (P4) walk.
    pub parent: Option<ManifestId>,
    /// When this manifest was committed, as milliseconds since the Unix epoch.
    ///
    /// Wall-clock, used for human-facing history and point-in-time restore. Never used for an
    /// internal decision — those use monotonic time (docs/02 §9.2), because an operator moving
    /// the clock must not be able to break the engine.
    pub created_at_ms: u64,
    /// The schema version of the data in these pages. Owned by the layer above (flock/loom).
    pub schema_version: u32,
    /// The page size of the store that produced this manifest.
    pub page_size: u32,
}

impl Manifest {
    /// An empty root manifest for a new store.
    pub fn empty(page_size: usize, created_at_ms: u64) -> Self {
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            pages: BTreeMap::new(),
            parent: None,
            created_at_ms,
            schema_version: 0,
            page_size: page_size as u32,
        }
    }

    /// This manifest's id: the hash of its serialized bytes.
    ///
    /// Two manifests with identical content have identical ids — so committing a transaction
    /// that changes nothing is a no-op that reuses the existing manifest rather than growing
    /// history with a duplicate.
    pub fn id(&self) -> Result<ManifestId> {
        let bytes = self.encode()?;
        Ok(ManifestId(*blake3::hash(&bytes).as_bytes()))
    }

    /// Serialize deterministically.
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|source| PagerError::Codec {
            op: "encode",
            source,
        })
    }

    /// Deserialize.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|source| PagerError::Codec {
            op: "decode",
            source,
        })
    }

    /// What content is at this logical page, if any.
    pub fn get(&self, page_no: LogicalPageNo) -> Option<PageId> {
        self.pages.get(&page_no).copied()
    }

    /// How many logical pages this database has.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// True if the database has no pages.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Every page this manifest references. The unit of liveness for GC.
    pub fn referenced_pages(&self) -> impl Iterator<Item = PageId> + '_ {
        self.pages.values().copied()
    }

    /// Derive a child manifest by applying a set of page writes.
    ///
    /// A write of `None` removes the logical page (a truncation).
    pub(crate) fn derive(
        &self,
        writes: &BTreeMap<LogicalPageNo, Option<PageId>>,
        parent: ManifestId,
        created_at_ms: u64,
    ) -> Manifest {
        let mut pages = self.pages.clone();
        for (&page_no, &content) in writes {
            match content {
                Some(id) => {
                    pages.insert(page_no, id);
                }
                None => {
                    pages.remove(&page_no);
                }
            }
        }
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            pages,
            parent: Some(parent),
            created_at_ms,
            schema_version: self.schema_version,
            page_size: self.page_size,
        }
    }
}

impl fmt::Debug for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manifest")
            .field("pages", &self.pages.len())
            .field("parent", &self.parent)
            .field("schema_version", &self.schema_version)
            .finish()
    }
}

/// Where manifests are persisted.
///
/// Separate from the page CAS because manifests are the **roots of liveness** (docs/02 §3.1):
/// GC recomputes what is alive by reading manifests, so a manifest is never itself swept by
/// the page sweep. Keeping them in their own namespace makes that impossible to get wrong by
/// accident.
pub trait ManifestStore: Send + Sync {
    /// Persist a manifest and return its id. Idempotent — content-addressed.
    fn put(&self, manifest: &Manifest) -> Result<ManifestId>;
    /// Load a manifest.
    fn get(&self, id: ManifestId) -> Result<Manifest>;
    /// Whether this manifest is present.
    fn contains(&self, id: ManifestId) -> Result<bool>;
    /// Remove a manifest. GC only, and only when proven unreachable.
    fn remove(&self, id: ManifestId) -> Result<()>;
    /// Every manifest currently stored.
    fn list(&self) -> Result<Vec<ManifestId>>;
}

/// Manifests on the filesystem, under `<root>/manifests/aa/<id>`.
pub struct FsManifestStore {
    root: PathBuf,
}

impl FsManifestStore {
    /// Open (creating if absent) a manifest store rooted at `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().join("manifests");
        std::fs::create_dir_all(&root).map_err(|e| PagerError::io(&root, e))?;
        Ok(FsManifestStore { root })
    }

    fn path_of(&self, id: ManifestId) -> PathBuf {
        let hex = id.to_hex();
        self.root.join(&hex[0..2]).join(&hex)
    }
}

impl ManifestStore for FsManifestStore {
    fn put(&self, manifest: &Manifest) -> Result<ManifestId> {
        let id = manifest.id()?;
        let path = self.path_of(id);
        if path.exists() {
            return Ok(id); // content-addressed: already correct
        }
        let dir = path
            .parent()
            .ok_or_else(|| PagerError::io(&path, std::io::Error::other("no parent dir")))?;
        std::fs::create_dir_all(dir).map_err(|e| PagerError::io(dir, e))?;

        // Same write-temp-fsync-rename-fsync-dir dance as the CAS: a half-written manifest is
        // a corrupt database root, which is the worst thing on this disk.
        let tmp = dir.join(format!(".tmp.{}", id.to_hex()));
        {
            let mut file = std::fs::File::create(&tmp).map_err(|e| PagerError::io(&tmp, e))?;
            file.write_all(&manifest.encode()?)
                .map_err(|e| PagerError::io(&tmp, e))?;
            file.sync_all().map_err(|e| PagerError::io(&tmp, e))?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| PagerError::io(&path, e))?;
        let dir_handle = std::fs::File::open(dir).map_err(|e| PagerError::io(dir, e))?;
        dir_handle.sync_all().map_err(|e| PagerError::io(dir, e))?;
        Ok(id)
    }

    fn get(&self, id: ManifestId) -> Result<Manifest> {
        let path = self.path_of(id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(PagerError::MissingManifest(id.to_hex()))
            }
            Err(e) => return Err(PagerError::io(&path, e)),
        };
        let manifest = Manifest::decode(&bytes)?;
        // Manifests are content-addressed, so verify on read exactly as pages are. A corrupt
        // manifest is a corrupt view of the entire database; it must never be trusted quietly.
        let actual = manifest.id()?;
        if actual != id {
            return Err(PagerError::MissingManifest(format!(
                "{} (bytes on disk hash to {})",
                id.to_hex(),
                actual.to_hex()
            )));
        }
        Ok(manifest)
    }

    fn contains(&self, id: ManifestId) -> Result<bool> {
        Ok(self.path_of(id).exists())
    }

    fn remove(&self, id: ManifestId) -> Result<()> {
        let path = self.path_of(id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PagerError::io(&path, e)),
        }
    }

    fn list(&self) -> Result<Vec<ManifestId>> {
        let mut out = Vec::new();
        let shards = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(PagerError::io(&self.root, e)),
        };
        for shard in shards {
            let shard = shard.map_err(|e| PagerError::io(&self.root, e))?;
            let files = match std::fs::read_dir(shard.path()) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for file in files {
                let file = file.map_err(|e| PagerError::io(shard.path(), e))?;
                let name = file.file_name();
                let Some(name) = name.to_str() else { continue };
                if name.starts_with(".tmp.") {
                    continue;
                }
                if let Ok(id) = ManifestId::from_hex(name) {
                    out.push(id);
                }
            }
        }
        Ok(out)
    }
}

/// Manifests in memory. Tests, and the cache tier.
#[derive(Default)]
pub struct MemManifestStore {
    manifests: Mutex<BTreeMap<ManifestId, Vec<u8>>>,
}

impl MemManifestStore {
    /// A new, empty in-memory manifest store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<ManifestId, Vec<u8>>> {
        self.manifests.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl ManifestStore for MemManifestStore {
    fn put(&self, manifest: &Manifest) -> Result<ManifestId> {
        let id = manifest.id()?;
        self.lock().insert(id, manifest.encode()?);
        Ok(id)
    }

    fn get(&self, id: ManifestId) -> Result<Manifest> {
        let bytes = self
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| PagerError::MissingManifest(id.to_hex()))?;
        Manifest::decode(&bytes)
    }

    fn contains(&self, id: ManifestId) -> Result<bool> {
        Ok(self.lock().contains_key(&id))
    }

    fn remove(&self, id: ManifestId) -> Result<()> {
        self.lock().remove(&id);
        Ok(())
    }

    fn list(&self) -> Result<Vec<ManifestId>> {
        Ok(self.lock().keys().copied().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::DEFAULT_PAGE_SIZE;

    fn manifest_with(pages: &[(LogicalPageNo, &[u8])]) -> Manifest {
        let mut m = Manifest::empty(DEFAULT_PAGE_SIZE, 1_700_000_000_000);
        for (no, bytes) in pages {
            m.pages.insert(*no, PageId::of(bytes));
        }
        m
    }

    #[test]
    fn identical_manifests_have_identical_ids() -> Result<()> {
        let a = manifest_with(&[(0, b"x"), (1, b"y")]);
        let b = manifest_with(&[(1, b"y"), (0, b"x")]); // inserted in the other order
        assert_eq!(
            a.id()?,
            b.id()?,
            "insertion order must not affect identity — BTreeMap, not HashMap"
        );
        Ok(())
    }

    #[test]
    fn encoding_is_deterministic() -> Result<()> {
        // Deterministic replay (docs/02 §3.1) stands on this. If encoding ever varies run to
        // run, recovery stops being verifiable and every other guarantee is unfounded.
        let m = manifest_with(&[(7, b"seven"), (2, b"two"), (99, b"ninety-nine")]);
        let first = m.encode()?;
        for _ in 0..64 {
            assert_eq!(m.encode()?, first);
        }
        Ok(())
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let m = manifest_with(&[(0, b"a"), (5, b"b")]);
        let decoded = Manifest::decode(&m.encode()?)?;
        assert_eq!(decoded, m);
        assert_eq!(decoded.id()?, m.id()?);
        Ok(())
    }

    #[test]
    fn derive_applies_writes_and_removals() -> Result<()> {
        let base = manifest_with(&[(0, b"a"), (1, b"b")]);
        let base_id = base.id()?;

        let mut writes = BTreeMap::new();
        writes.insert(1, Some(PageId::of(b"b-prime")));
        writes.insert(2, Some(PageId::of(b"c")));
        writes.insert(0, None); // truncate page 0

        let child = base.derive(&writes, base_id, 1_700_000_001_000);
        assert_eq!(child.get(0), None);
        assert_eq!(child.get(1), Some(PageId::of(b"b-prime")));
        assert_eq!(child.get(2), Some(PageId::of(b"c")));
        assert_eq!(child.parent, Some(base_id));
        // The base is untouched — manifests are values.
        assert_eq!(base.get(0), Some(PageId::of(b"a")));
        Ok(())
    }

    #[test]
    fn stores_round_trip_and_verify() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let stores: Vec<Box<dyn ManifestStore>> = vec![
            Box::new(FsManifestStore::open(dir.path())?),
            Box::new(MemManifestStore::new()),
        ];
        for store in stores {
            let m = manifest_with(&[(0, b"a"), (1, b"b")]);
            let id = store.put(&m)?;
            assert_eq!(id, m.id()?);
            assert_eq!(store.get(id)?, m);
            assert!(store.contains(id)?);
            assert_eq!(store.list()?, vec![id]);
            store.remove(id)?;
            assert!(!store.contains(id)?);
            store.remove(id)?; // idempotent
        }
        Ok(())
    }

    #[test]
    fn a_corrupt_manifest_on_disk_is_never_trusted() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = FsManifestStore::open(dir.path())?;
        let m = manifest_with(&[(0, b"a")]);
        let id = store.put(&m)?;

        // Rewrite the file with a *different but valid* manifest — the nastiest case, because
        // it decodes cleanly and only the hash reveals the substitution.
        let other = manifest_with(&[(0, b"attacker's page")]);
        std::fs::write(store.path_of(id), other.encode()?).expect("tamper");

        assert!(
            matches!(store.get(id), Err(PagerError::MissingManifest(_))),
            "a manifest whose bytes do not hash to its id must be refused"
        );
        Ok(())
    }
}
