//! Manifests: the complete state of one database at one instant.
//!
//! A manifest is a **value**, not a location — and it is itself content-addressed. That single
//! property is why the expensive-sounding operations are free:
//!
//! | Operation | What actually happens |
//! |---|---|
//! | snapshot | remember a `ManifestId` |
//! | fork     | start a new head at a `ManifestId` |
//! | rewind   | point a branch at an older `ManifestId` |
//!
//! No bytes are copied in any of them (docs/02 §3.1).
//!
//! # Flat and overlay manifests
//!
//! A manifest comes in two shapes:
//!
//! - **Flat** — the complete map of every logical page to its content.
//! - **Overlay** — only what *changed*, relative to a base manifest.
//!
//! The reason is a cost that only shows up at scale. If every manifest were flat, committing a
//! one-page change to a 1 GiB database would write out a map of sixteen thousand entries — roughly
//! 650 KiB of manifest to record 64 KiB of data. On *every* commit. An overlay records the one page
//! that changed, and nothing else.
//!
//! The price is read amplification: resolving a logical page walks the overlay chain. So a chain is
//! never allowed to grow past [`MAX_OVERLAY_DEPTH`] — at that point the next commit writes a **flat**
//! manifest instead, collapsing the chain. Reads are bounded at 8 hops; writes cost the size of the
//! change, plus one flattening every eight commits.
//!
//! ## Why collapse happens at commit, not in the background
//!
//! docs/02 describes collapsing "in the background". We do it **inline at commit time, decided purely
//! by the base manifest's depth** — and the difference matters more than it looks.
//!
//! A background collapse produces a *different manifest id* for the *same logical state*, at a moment
//! nobody chose. Replaying the same WAL would then produce a different manifest depending on whether
//! the collapser had run yet — which destroys deterministic replay, and deterministic replay is the
//! property every durability guarantee in this engine rests on (docs/02 §3.1).
//!
//! Deciding from `base.depth()` alone makes flattening a pure function of the history, so replay
//! reproduces it exactly. It costs a latency spike on one commit in eight, instead of amortising that
//! cost into a background task. We take that trade gladly.

use crate::error::{PagerError, Result};
use crate::page::{LogicalPageNo, PageId};
use crate::vfs::{std_vfs, Vfs};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The longest overlay chain permitted before a commit writes a flat manifest instead.
///
/// Eight is the number in docs/02 §3.1, and the read-overhead target that goes with it (**< 20 %**
/// versus a flat manifest at this depth) is enforced by `benches/branch.rs`. Raise it and writes get
/// cheaper while reads get slower; lower it and the reverse.
pub const MAX_OVERLAY_DEPTH: u32 = 8;

/// The content hash of a serialized [`Manifest`].
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

/// The on-disk manifest format version.
///
/// **Bumping this is a format change**, which per CLAUDE.md rule 3 requires a fuzz-target update in
/// the same commit. Version 2 introduced overlay manifests.
pub const MANIFEST_FORMAT_VERSION: u32 = 2;

/// A page map: every logical page, and the content currently at it.
pub type PageMap = BTreeMap<LogicalPageNo, PageId>;

/// The changes an overlay records. `None` means the logical page was removed.
pub type PageChanges = BTreeMap<LogicalPageNo, Option<PageId>>;

/// What a manifest actually holds.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestBody {
    /// The complete map. Self-contained: resolving a page needs nothing else.
    Flat(PageMap),
    /// Only what changed relative to `base`.
    Overlay {
        /// The manifest this one layers on top of.
        base: ManifestId,
        /// The changed pages. `None` removes a page.
        changes: PageChanges,
    },
}

/// The complete state of a database at one instant.
///
/// # Determinism
///
/// Page maps are [`BTreeMap`]s, so serialization is in ascending logical-page order, always. This is
/// not a stylistic preference: **deterministic replay** (docs/02 §3.1) requires that replaying the
/// same WAL twice produces byte-identical manifests, and a `HashMap`'s iteration order would
/// silently destroy that property — and with it every guarantee that depends on recovery being
/// verifiable.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version, for upgrade paths.
    pub format_version: u32,
    /// Flat, or an overlay on something flatter.
    pub body: ManifestBody,
    /// The manifest this one was committed on top of. `None` for a root.
    ///
    /// This is the **history** edge — the commit DAG that branch trees walk and that merge-base
    /// computation searches. It is *not* the same thing as an overlay's `base`, which is a
    /// **storage** edge. They usually coincide; after a collapse they do not, because a flattened
    /// manifest still has a parent but no overlay base at all.
    pub parent: Option<ManifestId>,
    /// When this manifest was committed, in milliseconds since the Unix epoch.
    ///
    /// Wall-clock, for human-facing history and point-in-time restore. Never used for an internal
    /// decision — those use monotonic time (docs/02 §9.2), because an operator moving the clock must
    /// not be able to break the engine.
    pub created_at_ms: u64,
    /// The schema version of the data in these pages. Owned by the layer above.
    pub schema_version: u32,
    /// The page size of the store that produced this manifest.
    pub page_size: u32,
    /// How many overlays deep this manifest sits. `0` for flat.
    ///
    /// Bounded by [`MAX_OVERLAY_DEPTH`], which is what bounds read amplification.
    pub depth: u32,
    /// How many logical pages the database has, effectively.
    ///
    /// Cached, so `len()` does not have to resolve the whole chain.
    pub page_count: u64,
}

impl Manifest {
    /// The canonical empty root manifest for a store of this page size.
    ///
    /// # Why `created_at_ms` is zero and not "now"
    ///
    /// The root manifest's identity must be a pure function of the store's shape — nothing else. An
    /// earlier version stamped it with the wall clock, and the effect was quietly disastrous:
    /// reopening a store produced a root with a *different* timestamp, hence a different
    /// `ManifestId`, hence a different base for replay — so recovery rebuilt a database that did not
    /// match the one the commit records described, and refused to open it.
    ///
    /// Deterministic replay begins at a deterministic root.
    pub fn empty(page_size: usize) -> Self {
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            body: ManifestBody::Flat(PageMap::new()),
            parent: None,
            created_at_ms: 0,
            schema_version: 0,
            page_size: page_size as u32,
            depth: 0,
            page_count: 0,
        }
    }

    /// This manifest's id: the hash of its serialized bytes.
    pub fn id(&self) -> Result<ManifestId> {
        Ok(ManifestId(*blake3::hash(&self.encode()?).as_bytes()))
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

    /// How many overlays deep. `0` is flat.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// True if this manifest is self-contained — no chain to walk.
    pub fn is_flat(&self) -> bool {
        matches!(self.body, ManifestBody::Flat(_))
    }

    /// The manifest this one overlays, if any. A **storage** edge, not a history edge.
    pub fn overlay_base(&self) -> Option<ManifestId> {
        match &self.body {
            ManifestBody::Flat(_) => None,
            ManifestBody::Overlay { base, .. } => Some(*base),
        }
    }

    /// The complete map, if this manifest is flat.
    pub fn flat_pages(&self) -> Option<&PageMap> {
        match &self.body {
            ManifestBody::Flat(pages) => Some(pages),
            ManifestBody::Overlay { .. } => None,
        }
    }

    /// The changes this overlay records, if it is one.
    pub fn changes(&self) -> Option<&PageChanges> {
        match &self.body {
            ManifestBody::Flat(_) => None,
            ManifestBody::Overlay { changes, .. } => Some(changes),
        }
    }

    /// How many logical pages the database has. O(1) — the count is cached.
    pub fn len(&self) -> usize {
        self.page_count as usize
    }

    /// True if the database has no pages.
    pub fn is_empty(&self) -> bool {
        self.page_count == 0
    }

    /// Look this manifest up *locally*, without consulting its base.
    ///
    /// - `Some(Some(id))` — this manifest sets the page.
    /// - `Some(None)` — this manifest **removes** the page. Stop; do not consult the base.
    /// - `None` — this manifest says nothing about it; ask the base.
    ///
    /// The middle case is the one that is easy to get wrong, and getting it wrong resurrects deleted
    /// data from underneath its own tombstone.
    pub(crate) fn local_lookup(&self, page_no: LogicalPageNo) -> Option<Option<PageId>> {
        match &self.body {
            ManifestBody::Flat(pages) => Some(pages.get(&page_no).copied()),
            ManifestBody::Overlay { changes, .. } => changes.get(&page_no).copied(),
        }
    }

    /// Build the manifest a set of writes produces on top of `base`.
    ///
    /// Chooses flat or overlay **purely from the base's depth**, which is what makes the choice
    /// deterministic and therefore replayable. `resolved_base` is consulted only when flattening,
    /// and for the page count.
    /// Build an **overlay** on top of `base`, recording only what changed.
    ///
    /// `page_count` is supplied by the caller, which computed it from per-page lookups rather than
    /// by resolving the whole base — because resolving the base on every commit would cost O(pages),
    /// which is precisely the cost overlays exist to avoid. Doing that was, in fact, the first
    /// version of this code, and the benchmark caught it: committing one page to a 1 GiB database
    /// took 3.7 ms and scaled with the database rather than with the change.
    pub(crate) fn overlay_on(
        base_id: ManifestId,
        base: &Manifest,
        changes: &PageChanges,
        page_count: u64,
        created_at_ms: u64,
    ) -> Manifest {
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            body: ManifestBody::Overlay {
                base: base_id,
                changes: changes.clone(),
            },
            parent: Some(base_id),
            created_at_ms,
            schema_version: base.schema_version,
            page_size: base.page_size,
            depth: base.depth + 1,
            page_count,
        }
    }

    /// Collapse the chain: materialise `resolved_base`, apply `changes`, and start again at depth 0.
    ///
    /// Costs O(pages), and happens once every [`MAX_OVERLAY_DEPTH`] commits.
    pub(crate) fn flatten_onto(
        base_id: ManifestId,
        base: &Manifest,
        resolved_base: &PageMap,
        changes: &PageChanges,
        created_at_ms: u64,
    ) -> Manifest {
        let mut pages = resolved_base.clone();
        for (&page_no, &content) in changes {
            match content {
                Some(id) => {
                    pages.insert(page_no, id);
                }
                None => {
                    pages.remove(&page_no);
                }
            }
        }
        let page_count = pages.len() as u64;

        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            body: ManifestBody::Flat(pages),
            parent: Some(base_id),
            created_at_ms,
            schema_version: base.schema_version,
            page_size: base.page_size,
            depth: 0,
            page_count,
        }
    }

    /// Whether a commit on top of this manifest must flatten the chain.
    ///
    /// Decided **purely from depth**, which is what makes it a pure function of the history — and
    /// therefore replayable. See the module docs.
    pub(crate) fn must_flatten(&self) -> bool {
        self.depth >= MAX_OVERLAY_DEPTH
    }
}

impl fmt::Debug for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manifest")
            .field("shape", &if self.is_flat() { "flat" } else { "overlay" })
            .field("depth", &self.depth)
            .field("pages", &self.page_count)
            .field("parent", &self.parent)
            .finish()
    }
}

/// Where manifests are persisted.
///
/// Separate from the page CAS because manifests are the **roots of liveness** (docs/02 §3.1): GC
/// recomputes what is alive by reading manifests, so a manifest is never itself swept by the page
/// sweep. Keeping them in their own namespace makes that impossible to get wrong by accident.
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
    vfs: Arc<dyn Vfs>,
}

impl FsManifestStore {
    /// Open (creating if absent) a manifest store rooted at `root`, on the real filesystem.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        FsManifestStore::open_with_vfs(std_vfs(), root)
    }

    /// Open a manifest store on a caller-supplied filesystem (crash injection).
    pub fn open_with_vfs(vfs: Arc<dyn Vfs>, root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().join("manifests");
        vfs.create_dir_all(&root)
            .map_err(|e| PagerError::io(&root, e))?;
        Ok(FsManifestStore { root, vfs })
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
        if self.vfs.exists(&path) {
            return Ok(id); // content-addressed: already there means already correct
        }
        // Atomic: a half-written manifest is a corrupt view of the entire database, which is the
        // worst object on this disk. It appears complete, or it does not appear.
        self.vfs
            .atomic_write(&path, &manifest.encode()?)
            .map_err(|e| PagerError::io(&path, e))?;
        Ok(id)
    }

    fn get(&self, id: ManifestId) -> Result<Manifest> {
        let path = self.path_of(id);
        let bytes = match self.vfs.read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(PagerError::MissingManifest(id.to_hex()))
            }
            Err(e) => return Err(PagerError::io(&path, e)),
        };
        let manifest = Manifest::decode(&bytes)?;
        // Manifests are content-addressed, so verify on read exactly as pages are. A manifest whose
        // bytes do not hash to its id has been corrupted or substituted, and must never be trusted
        // quietly.
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
        Ok(self.vfs.exists(&self.path_of(id)))
    }

    fn remove(&self, id: ManifestId) -> Result<()> {
        let path = self.path_of(id);
        self.vfs
            .remove_file(&path)
            .map_err(|e| PagerError::io(&path, e))
    }

    fn list(&self) -> Result<Vec<ManifestId>> {
        let mut out = Vec::new();
        for shard in self
            .vfs
            .read_dir(&self.root)
            .map_err(|e| PagerError::io(&self.root, e))?
        {
            for file in self.vfs.read_dir(&shard).unwrap_or_default() {
                let Some(name) = file.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
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

    fn flat(pages: &[(LogicalPageNo, &[u8])]) -> Manifest {
        let mut m = Manifest::empty(DEFAULT_PAGE_SIZE);
        let map: PageMap = pages
            .iter()
            .map(|(no, bytes)| (*no, PageId::of(bytes)))
            .collect();
        m.page_count = map.len() as u64;
        m.body = ManifestBody::Flat(map);
        m
    }

    #[test]
    fn identical_manifests_have_identical_ids() -> Result<()> {
        let a = flat(&[(0, b"x"), (1, b"y")]);
        let b = flat(&[(1, b"y"), (0, b"x")]); // inserted in the other order
        assert_eq!(
            a.id()?,
            b.id()?,
            "insertion order must not affect identity — BTreeMap, not HashMap"
        );
        Ok(())
    }

    #[test]
    fn encoding_is_deterministic() -> Result<()> {
        // Deterministic replay stands on this. If encoding varied run to run, recovery would stop
        // being verifiable and every other guarantee would be unfounded.
        let m = flat(&[(7, b"seven"), (2, b"two"), (99, b"ninety-nine")]);
        let first = m.encode()?;
        for _ in 0..64 {
            assert_eq!(m.encode()?, first);
        }
        Ok(())
    }

    #[test]
    fn round_trips_through_bytes() -> Result<()> {
        let m = flat(&[(0, b"a"), (5, b"b")]);
        let decoded = Manifest::decode(&m.encode()?)?;
        assert_eq!(decoded, m);
        assert_eq!(decoded.id()?, m.id()?);
        Ok(())
    }

    #[test]
    fn a_shallow_commit_produces_an_overlay_not_a_copy() -> Result<()> {
        let base = flat(&[(0, b"a"), (1, b"b")]);
        let base_id = base.id()?;

        let mut changes = PageChanges::new();
        changes.insert(1, Some(PageId::of(b"b-prime")));
        changes.insert(2, Some(PageId::of(b"c")));
        changes.insert(0, None); // truncate page 0

        let child = Manifest::overlay_on(base_id, &base, &changes, 2, 1);

        assert!(!child.is_flat(), "a shallow chain should stay an overlay");
        assert_eq!(child.depth(), 1);
        assert_eq!(child.overlay_base(), Some(base_id));
        assert_eq!(child.parent, Some(base_id));
        assert_eq!(child.len(), 2, "2 pages, minus page 0, plus page 2");

        // The overlay records the CHANGE, not the database. That is the whole point.
        assert_eq!(child.changes().map(|c| c.len()), Some(3));

        // And the base is untouched — manifests are values.
        assert_eq!(base.len(), 2);
        Ok(())
    }

    #[test]
    fn a_chain_at_the_depth_limit_collapses_to_flat() -> Result<()> {
        let mut base = flat(&[(0, b"a")]);
        base.depth = MAX_OVERLAY_DEPTH; // pretend we are already 8 overlays deep
        let base_id = base.id()?;
        let resolved: PageMap = [(0, PageId::of(b"a"))].into_iter().collect();

        let mut changes = PageChanges::new();
        changes.insert(1, Some(PageId::of(b"b")));

        assert!(base.must_flatten(), "depth 8 must trigger a collapse");
        let child = Manifest::flatten_onto(base_id, &base, &resolved, &changes, 1);

        assert!(
            child.is_flat(),
            "the chain must collapse at the depth limit"
        );
        assert_eq!(child.depth(), 0, "and the depth resets");
        assert_eq!(child.overlay_base(), None);
        // A collapsed manifest still has a *history* parent, even though it has no *storage* base.
        assert_eq!(child.parent, Some(base_id));
        assert_eq!(
            child.flat_pages().map(|p| p.len()),
            Some(2),
            "flattening must materialise the base's pages, not just the change"
        );
        Ok(())
    }

    #[test]
    fn a_removal_stops_the_walk_instead_of_falling_through_to_the_base() -> Result<()> {
        // The subtle one. An overlay that *removes* a page must report "removed" — not "I don't
        // know, ask my base" — or the base's copy is resurrected from under its own tombstone.
        let base = flat(&[(0, b"a")]);
        let base_id = base.id()?;
        let resolved: PageMap = [(0, PageId::of(b"a"))].into_iter().collect();

        let mut changes = PageChanges::new();
        changes.insert(0, None);
        let _ = &resolved;
        let child = Manifest::overlay_on(base_id, &base, &changes, 0, 1);

        assert_eq!(
            child.local_lookup(0),
            Some(None),
            "a tombstone must be reported as a definite removal"
        );
        assert_eq!(
            child.local_lookup(1),
            None,
            "an untouched page must fall through to the base"
        );
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
            let m = flat(&[(0, b"a"), (1, b"b")]);
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
        let m = flat(&[(0, b"a")]);
        let id = store.put(&m)?;

        // Rewrite the file with a *different but valid* manifest — the nastiest case, because it
        // decodes cleanly and only the hash reveals the substitution.
        let other = flat(&[(0, b"attacker's page")]);
        std::fs::write(store.path_of(id), other.encode()?).expect("tamper");

        assert!(
            matches!(store.get(id), Err(PagerError::MissingManifest(_))),
            "a manifest whose bytes do not hash to its id must be refused"
        );
        Ok(())
    }
}
