# substrate v1.0 — the frozen API and the compatibility promise

> **Status:** This is the surface `flock-*` and `loom-*` are permitted to depend on. Everything else
> in these crates is an implementation detail, and will change without warning.

---

## The promise

Substrate follows **semantic versioning**, and we mean it:

- **Patch** (`1.0.x`) — bug fixes. No API change, no on-disk format change.
- **Minor** (`1.x.0`) — additions only. New methods, new types, new features. **Code that compiled
  against `1.0` compiles against every `1.x`, and a store written by `1.0` is readable by every
  `1.x`.**
- **Major** (`2.0`) — we may break things. We will not do this casually, and never without a
  migration path that does not require downtime.

**On-disk formats are part of the API.** A `ManifestId` written today must resolve to the same bytes
in five years, or every guarantee in this engine is a lie: content addressing means an id *is* a
claim about content, and a format change that alters what a hash means silently invalidates every
snapshot, every fork, and every audit trail anyone ever took.

Format versions are recorded in the data (`MANIFEST_FORMAT_VERSION`, and the WAL record header). A
format change requires a major version **and** a reader that still understands the old one.

---

## What is frozen

### `substrate-pager`

The core. Pure, synchronous, no network, no panics.

| Type | What it is |
| --- | --- |
| `PageId` | The content hash of a page. `BLAKE3(bytes)`, or keyed (§CUI below). |
| `Page` | An immutable block of bytes and its id. |
| `LogicalPageNo` | The address a database uses. `u64`. |
| `PageHasher` | `Unkeyed` or `Keyed([u8; 32])`. Fixed at store creation. |
| `Manifest` / `ManifestId` | The complete state of a database at one instant, content-addressed. |
| `ManifestBody` | `Flat(PageMap)` or `Overlay { base, changes }`. |
| `PageMap` / `PageChanges` | Resolved page maps, and the deltas an overlay records. |
| `BranchTree` | Named heads and tags over the manifest DAG. |
| `PageDiff` / `ThreeWayDiff` / `PageClass` | Comparison results. `ThreeWayDiff` is LoomDB's merge input. |
| `GcStats` / `CorruptionReport` | What a sweep and a scrub found. |
| `StoreConfig` | Page size, hasher, pool. |
| `Txn` | An in-flight set of writes. |
| `PagerError` | `#[non_exhaustive]`. New variants may appear in a minor version — match with a `_` arm. |

**Traits.**

```rust
pub trait PageStore: Send + Sync {
    fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page>;
    fn read_head(&self, page_no: LogicalPageNo) -> Result<Page>;

    fn begin(&self) -> Result<Txn>;
    fn write(&self, txn: &mut Txn, page_no: LogicalPageNo, bytes: Vec<u8>) -> Result<PageId>;
    fn remove(&self, txn: &mut Txn, page_no: LogicalPageNo) -> Result<()>;
    fn commit(&self, txn: Txn) -> Result<ManifestId>;

    fn head(&self) -> ManifestId;
    fn snapshot(&self) -> Result<ManifestId>;                       // O(1)
    fn fork(&self, from: &ManifestId) -> Result<Box<dyn PageStore>>; // O(1)
    fn rewind(&self, to: &ManifestId) -> Result<()>;                 // O(1)

    fn diff(&self, a: &ManifestId, b: &ManifestId) -> Result<PageDiff>;
    fn diff3(&self, base: &ManifestId, a: &ManifestId, b: &ManifestId) -> Result<ThreeWayDiff>;
    fn merge_base(&self, a: &ManifestId, b: &ManifestId) -> Result<Option<ManifestId>>;

    fn resolve(&self, id: &ManifestId) -> Result<PageMap>;                            // O(pages)
    fn lookup(&self, id: &ManifestId, page_no: LogicalPageNo) -> Result<Option<PageId>>; // O(depth)

    fn gc(&self, live_manifests: &[ManifestId]) -> Result<GcStats>;
    fn manifest(&self, id: &ManifestId) -> Result<Manifest>;
    fn page_size(&self) -> usize;
    fn pool(&self) -> &str;
}

pub trait Cas: Send + Sync { /* put, get, contains, remove, list */ }
pub trait ManifestStore: Send + Sync { /* put, get, contains, remove, list */ }
pub trait Vfs: Send + Sync { /* create_dir_all, atomic_write, append, read, truncate, ... */ }
pub trait Clock: Send + Sync { fn now_ms(&self) -> u64; }
pub trait Metrics: Send + Sync { /* all methods have no-op defaults */ }
```

`PageStore` is **not** sealed: implementing it is a supported thing to do (a mock, a proxy, an
encrypting wrapper). But note that new methods on a trait are a **breaking change for implementors**
even when they are additive for callers, so `PageStore` will not gain methods before 2.0. Anything we
want to add goes on `Pager` as an inherent method instead.

### `substrate-wal`

| Item | What it is |
| --- | --- |
| `DurableStore` | A `Pager` whose commits survive a crash. **This is what products should use.** |
| `Wal` | The log itself, for anyone building their own commit protocol. |
| `Lsn` | A log sequence number. Monotonic, never reused, never reset. |
| `Recovery` | What a replay did — including `torn_tail`, which is what a crash looks like. |
| `Record` / `RecordKind` | The on-disk log format. **Frozen.** |
| `WalError` | `#[non_exhaustive]`. |

### `substrate-store`

| Item | What it is |
| --- | --- |
| `TieredStore` | A store whose durable home is object storage and whose disk is a cache. |
| `WakeToken` | A sleeping database, in about twenty bytes. **Serialization is stable.** |
| `RemoteTier` | Pool-scoped object storage. |
| `TierStats` / `RepairReport` | Cache behaviour, and what a repair fixed or could not. |
| `StoreError` | `#[non_exhaustive]`. |

`WakeToken`'s JSON is a stable format: a fleet registry will store millions of them, and a token
written by `1.0` must wake in `1.9`.

---

## What is *not* frozen

Everything else. Specifically:

- **`substrate_pager::testing`** (the `test-util` feature) — `CrashVfs`, `MemVfs`, `Rng`. Test
  scaffolding, and it will change whenever the tests need it to.
- **`substrate_pager::model`** — the oracle. It exists to be *obviously correct*, not stable.
- Anything `pub(crate)`, and anything not named above.
- Benchmarks, fuzz targets, and the shape of `Debug` output.

---

## Rules for consumers

These are not suggestions; they are the conditions under which the guarantees hold.

**1. Use `DurableStore`, not `Pager`, unless you are implementing your own commit protocol.**
A bare `Pager` keeps its head in memory. It is durable in the sense that its *pages* are durable, but
"which manifest is current" does not survive the process. `substrate-wal` is what makes a commit a
commit.

**2. `gc()` takes every live root, and it is on you to give it all of them.**
Branch heads. Tags. Snapshots you are still holding. Manifests a sleeping database points at. GC
recomputes liveness from exactly what you hand it — pass an incomplete set and it will cheerfully
delete the rest, because from where it is standing, that is garbage. Use `BranchTree::roots()`.

**3. A `TieredStore` needs a multi-threaded tokio runtime.**
A cache miss on the synchronous read path blocks on an async fetch. On a current-thread runtime that
deadlocks. This is a real constraint and we would rather you read it here.

**4. Match `PagerError` and friends with a `_` arm.**
They are `#[non_exhaustive]`. New variants arrive in minor versions.

**5. Do not assume a manifest is flat.**
`Manifest::flat_pages()` returns `None` for an overlay. Use `PageStore::resolve()` to get the whole
map, or `PageStore::lookup()` for a single page. Reading an overlay's local body and treating it as
the database will give you a fraction of the data and no error.

**6. Pools are a boundary, not a namespace.**
Two stores in different pools never share a page, even when the bytes are identical. This costs
deduplication and buys the guarantee that data cannot cross a classification boundary through the
storage layer. There is no setting that turns it off.

---

## The CUI build

`--features keyed-hash` changes page identity to `BLAKE3_keyed(pool_key, plaintext)`.

It is **a mutually exclusive build mode, not an additive feature**: with it compiled in, constructing
an unkeyed store fails at the door, with no override. That is the entire point — a CUI deployment
must not be *configurable* back into plaintext-confirmable page identity by a tired operator at 2am.

It changes the on-disk format in the sense that page ids differ, so a keyed store and an unkeyed store
are not interchangeable, and never will be. See docs/02 §9.1 for the threat model this closes
(membership confirmation within a dedup scope) and what it costs (no cross-pool dedup, which §9.1
already required).

---

## The airgap build

`--features airgap` removes all outbound networking **at compile time**. The only permitted endpoint
is the configured object store.

This is not a runtime toggle you can misconfigure. It is an amputation an auditor can verify by
reading the binary. CI runs the full suite inside a network-isolated container, and a test that needs
the network to pass is a test that fails.

---

## What we will break, and when

We will bump to 2.0 — with a migration path, and not casually — if we ever need to:

- change the on-disk manifest or WAL record format in a way old readers cannot handle;
- add a method to `PageStore` (breaking for implementors);
- change what `PageId` means (it never will: it is `BLAKE3` of the bytes, and that is the foundation
  everything else stands on).

We will not break:

- the meaning of a `ManifestId`;
- the durability guarantee;
- the promise that `Degraded` licensing never stops a read or a write (docs/02 §9.2).

If we ever have to choose between an API we regret and a customer's data, we will keep the API we
regret.
