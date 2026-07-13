# 02 — Embedded Single-Node Engine Architecture (FlockDB)

> **Status:** Authoritative. This document, together with [03](./03-agent-native-database-architecture.md)
> and [04](./04-flockdb-loomdb-unified-roadmap.md), is the architecture of record.
> Code that contradicts this document is a bug in the code or a bug in this document —
> resolve it here first, then in code.

---

## §1 — The problem

The dominant assumption in analytical databases is that there is **one big database**. Storage,
query planning, and operations are all built around a single large corpus that is always resident,
always warm, and always paying for compute.

An enormous and growing class of workloads is the exact opposite shape: **very many small
databases**, each of which is idle almost all of the time.

- A B2B SaaS with 40,000 customers wants per-customer analytics with hard tenant isolation.
- An observability vendor wants a database per service per environment.
- A quant shop wants a database per backtest.
- An agent platform wants a database per session (see [doc 03](./03-agent-native-database-architecture.md)).

Today those users pick one of two bad options:

1. **One giant shared database with a `tenant_id` column.** Cheap, but tenant isolation is a
   `WHERE` clause away from a breach, noisy neighbours are structural, per-tenant schema
   evolution is impossible, and "delete customer 12345 completely" is a research project.
2. **One database instance per tenant.** Correct, but every idle tenant burns a container, a
   connection pool, and a monthly bill. At 40,000 tenants the operational surface is the product.

FlockDB takes the second option and removes its cost. The unit of isolation is a **real, separate
database**, and an idle database costs *the price of its bytes in object storage and nothing else*.

### §1.1 — The two things that must be true

Everything in this document exists to make two claims true:

- **Databases are cheap to have.** Creating one is a metadata write. Forking one is O(1).
  Snapshotting is O(1). An idle one costs no compute. Ten thousand of them fit on a laptop.
- **Databases are fast to wake.** A query addressed to a sleeping database returns in
  **under 250ms**, cold, from object storage. Not "eventually", not "after a restore job".

If either claim fails, the product has no reason to exist.

---

## §2 — Product shape

FlockDB is two things that ship together and are licensed differently.

| Layer | What it is | License |
| --- | --- | --- |
| **Embedded engine** (`flock-core`, `flock-kernel`, `flock-sync` on `substrate-*`) | A library. Embed it and you have a forkable, sleepable, S3-backed analytical database in-process. Think "DuckDB with git-like branching and infinite hibernation." | Apache-2.0 |
| **Fleet plane** (`flockd`) | A control plane that manages 10,000+ of those databases: registry, wake-on-query scheduler, cross-database fan-out, and fleet-wide schema migrations. | BSL (source-available) |

The engine is open because durability claims that cannot be audited are worthless. The fleet plane
is the commercial surface because operating ten thousand databases — not reading one — is the hard
part enterprises will pay to not do.

### §2.1 — Non-goals

Stated plainly so nobody builds toward them by accident:

- **Not an OLTP database.** No row-level concurrency control, no long-lived interactive
  transactions, no attempt at high-rate single-row updates. Single-writer per database.
- **Not a distributed query engine.** We do not shard one logical table across machines. Fan-out
  (§3.2) is *many databases, one query* — the opposite of sharding, and much easier.
- **Not a log/observability store.** We are not competing with ClickHouse on ingest throughput or
  scan cost over a single petabyte-scale firehose. Our shape is many small corpora, not one big one.
  See [doc 03 §9.4](./03-agent-native-database-architecture.md#94--where-this-design-does-not-help)
  for the honest boundary.
- **Not a new SQL dialect.** We host a proven kernel (DuckDB). We add storage semantics beneath it,
  not language semantics above it.

---

## §3 — Architecture

Two planes, deliberately separable. The engine never calls the fleet plane. The fleet plane only
ever consumes the engine's public API.

```
                    ┌──────────────────────────────────────────────┐
                    │  FLEET PLANE  (flockd, BSL)                  │
                    │                                              │
                    │  Registry   Scheduler   Fan-out   Migrations │
                    └────────────────────┬─────────────────────────┘
                                         │  public API only
                    ┌────────────────────▼─────────────────────────┐
                    │  ENGINE  (flock-core / flock-kernel, Apache) │
                    │                                              │
                    │   SqlKernel (DuckDB)   Db handle   export    │
                    └────────────────────┬─────────────────────────┘
                                         │  PageStore trait only
   ┌─────────────────────────────────────▼──────────────────────────────────────┐
   │  SUBSTRATE  (shared with LoomDB — see doc 03)                              │
   │                                                                            │
   │   substrate-pager   substrate-wal   substrate-store   substrate-security   │
   │   pages/CAS/fork    commit/recovery  S3 tier/sleep     encrypt/license     │
   └────────────────────────────────────────────────────────────────────────────┘
```

### §3.1 — The single-node engine

#### Pages and content addressing

The atom of durable state is an **immutable page**: a byte block, 64 KiB by default, fixed at store
creation. A page's identity *is* its content:

```
PageId = BLAKE3(page_bytes)
```

Content addressing is the load-bearing decision in this architecture. Everything valuable falls out
of it:

- **Forking is free.** A fork shares every page with its parent by construction; it copies nothing.
- **Deduplication is automatic.** Ten thousand databases from the same template store one copy of
  the template's pages.
- **Caching is trivial and safe.** A page fetched from object storage can never be stale, because
  a different content is a different id. No invalidation, ever.
- **Integrity is inherent.** Re-hash on read and you have detected any corruption, anywhere in the
  path, without a separate checksum scheme.

The cost is a real one and we state it up front: hashing the **plaintext** means identical plaintext
produces identical `PageId`s, which leaks membership across any two stores that share a dedup scope.
See §9.1 for the threat model and the keyed-hash mode that closes it.

#### The CAS

Pages live in a local **content-addressed store**: a directory, sharded two levels by hash prefix
(`aa/bb/<hash>`), one file per page, write-once, fsync'd on write, hash-verified on read. Write-once
plus content addressing means concurrent writers of the *same* page cannot conflict — they are
writing identical bytes.

#### Manifests

A **manifest** is the complete state of one database at one instant:

```rust
struct Manifest {
    pages: OrderedMap<LogicalPageNo, PageId>,  // the whole database
    parent: Option<ManifestId>,                // the DAG edge
    created_at: Timestamp,
    schema_version: u32,
}

ManifestId = BLAKE3(serialized_manifest)   // manifests are content-addressed too
```

A manifest is a *value*, not a location. This is why snapshot is O(1): a snapshot is just
"remember this ManifestId." It is why fork is O(1): a fork is "start a new overlay on top of this
ManifestId." It is why rewind is O(1): rewind is "point the branch at that older ManifestId."

There is no copy anywhere in that list. That is the entire trick.

#### The WAL

Pages reach the CAS before they are ever referenced. The WAL therefore does not contain page bytes —
it contains **ordering**. Append-only segments, 4 MiB target, each record
`(lsn, op, crc32c)` where `op` is a manifest operation or a page-write reference.

**The commit protocol, in this order, no exceptions:**

```
1. write page bytes to CAS, fsync           ← content is durable but unreferenced (harmless garbage)
2. append WAL commit record, fsync          ← THE COMMIT POINT. atomic. before this, nothing happened.
3. update in-memory manifest                ← now readers see it
```

A crash before step 2 leaves orphan pages in the CAS that GC later sweeps — no corruption, because
nothing references them. A crash after step 2 is a committed transaction that recovery will replay.
There is no window in which a transaction is half-committed, because the commit is a single fsync'd
record.

**Recovery** replays the WAL forward from the last checkpointed manifest. Replay is deterministic
(same WAL ⇒ byte-identical manifest, always) and idempotent (replaying a record twice is a no-op).
Checkpointing periodically persists the current manifest and truncates WAL history behind it.

> This is the most safety-critical code we will write. Boring and obvious beats clever, every time.
> The invariant the crash-injection suite proves: **after a crash at any byte boundary, the
> recovered store equals some prefix of committed transactions.** No torn state. No lost commit.

#### Branch trees

Forks of forks, to arbitrary depth, with parent pointers — a DAG of manifests per database, with
named branches and tags. Reads resolve through the overlay chain (newest overlay first, falling
back to base). To bound read amplification, a chain deeper than **N=8** is collapsed in the
background by materializing a flattened manifest; the flattened manifest is semantically identical,
so collapsing is invisible and can be interrupted safely.

**Three-way diff** (branches A, B, and their merge base) classifies every logical page as
`Unchanged | AOnly | BOnly | Conflict`. This output type exists to be consumed by LoomDB's merge
engine ([doc 03 §3.1](./03-agent-native-database-architecture.md#p1--branchable-state)) — it is
designed for that consumer, not for humans.

#### Garbage collection

A page is live if any live manifest references it. `gc(live_manifests: &[ManifestId])` sweeps
everything else.

Refcounts are **recomputed from manifests on recovery, never trusted from a counter file.** A
counter file is a second source of truth about liveness, and a corrupt one silently deletes live
data. Manifests are the only source of truth. GC is allowed to be slower in exchange for being
impossible to get catastrophically wrong.

#### Sync

Single-writer, N-reader. The writer ships sealed WAL segments and periodic manifest checkpoints to
object storage; replicas tail and replay them, exposing read-only handles. Replicas expose
`lag() -> Duration` and `wait_for(lsn, timeout)`. A replica **never** serves a partially-applied
segment — it advances its visible manifest only at commit boundaries.

### §3.2 — The fleet plane

#### Registry

The catalogue of every database: name, pool, manifest head, schema version, size, labels,
last-active. SQLite-backed for single-node; Postgres behind a feature flag for HA. gRPC + JSON.

The registry is authoritative for *where a database is* and *what version it is*. It is never
authoritative for *what a database contains* — that is the manifest's job, and duplicating it would
create a second source of truth we would eventually have to reconcile.

#### Scheduler — wake-on-query

The heart of the economics.

```
query arrives for db "acme-prod"
  → registry: sleeping, manifest head = M
  → scheduler: lease a worker process from the pool
  → worker: substrate wake(WakeToken(M))  — fetch manifest eagerly, pages lazily
  → execute query, stream Arrow back
  → idle timeout expires → sleep(db) → local state dropped
```

Sleeping is not a degraded state; it is the **default** state. A fleet of 10,000 databases has, at
any instant, perhaps 50 awake. **Target: p99 wake-to-first-row < 250ms.**

Workers are plain OS processes today. Firecracker microVMs are the path when hostile multi-tenancy
demands a hardware isolation boundary (§8) — the scheduler interface is written so that swap is a
backend change, not a redesign.

#### Fan-out query service

One query, many databases. `flock fleet query "<sql>" --on 'label_selector'`.

```
1. PRUNE     registry statistics (per-DB min/max + bloom filters, collected by background
             maintenance) eliminate databases that cannot possibly match. For a selective
             predicate this must eliminate >95% of the fleet. Pruning is the whole game:
             the fastest query against 10,000 databases is the one that only touches 30.
2. SCATTER   compile once, scatter to workers. Workers read database SNAPSHOTS, never live
             WALs — a fan-out query can never see a torn or moving state.
3. STREAM    Arrow partials stream back as they complete; LIMIT/top-k terminates early and
             cancels outstanding work.
4. MERGE     union, plus re-aggregation for the supported set: count / sum / min / max / avg,
             with group-by.
```

`avg` re-aggregates as `(sum, count)` pairs and divides at the end — averaging averages is wrong and
we will not ship it.

Anything outside that set (cross-database joins, median, `DISTINCT` across databases, window
functions spanning databases) is **rejected with an error that names the escape hatch**, not
silently approximated. A wrong number is worse than an error.

#### Migration orchestrator

Fleet-wide schema change without a fleet-wide outage.

```
plan     = declarative schema (migrations dir + fleet manifest) diffed against the registry
execute  = canary cohorts (configurable %), health check between cohorts,
           automatic halt + report when the failure threshold trips
rollback = instant — every database gets a pre-migration SNAPSHOT before it is touched
```

That last line is the payoff for everything in §3.1. Pre-migration snapshots are **free** (O(1),
zero copy), so we take one unconditionally for every database, always. Rollback of a botched
migration across 10,000 databases is a pointer move per database.

---

## §4 — Roadmap

**Phase 0 — the embedded engine (OSS launch surface).**
Substrate (pager, WAL, object-store tiering). `flock-kernel` + `flock-core`. The `flock` CLI and
Python bindings. The five-command quickstart that ends with *"I forked my database and queried both
copies."* Success = someone who has never heard of us forks a database in 90 seconds.

**Phase 1 — replication and time travel.**
`flock-sync`: WAL shipping, read replicas, replica freshness API, point-in-time restore to any
timestamp or LSN as a new branch.

**Phase 2 — the fleet plane.**
`flockd`: registry, wake-on-query scheduler, fan-out with pruning, migration orchestrator with
canary cohorts. Success = the 10,000-database simulation passes on a laptop.

**Phase 3 — the enterprise/air-gap surface.**
Offline licensing, airgap profile, CUI pools, Firecracker isolation, signed offline update bundles.
Success = install and run for 120 days with no egress and a drifting clock. (§9)

---

## §5 — Repository layout and the frozen interfaces

Three repositories. Substrate is a *dependency*, not a destination — a LoomDB user never visits it,
exactly as a DuckDB user never visits Apache Arrow.

```
substrate/                  (Apache-2.0)  the shared engine — this doc's §3.1
  crates/
    substrate-pager/        pages, CAS, manifests, branch trees, GC   (pure sync, no async)
    substrate-wal/          segments, commit protocol, recovery       (pure sync)
    substrate-store/        object storage, tiering, sleep/wake       (async, tokio)
    substrate-security/     page encryption, offline licensing
  testing/
    fuzz/                   cargo-fuzz targets + crash injection
    integration/            cross-crate lifecycle tests
  docs/                     02, 03, 04, substrate-api.md, threat-model.md

flockdb/                    one product, one repo
  crates/                   (Apache-2.0)
    flock-core/             the Db handle and public API
    flock-kernel/           SqlKernel trait + DuckDB implementation
    flock-sync/             WAL shipping, replicas, PITR
    flock-cli/              the `flock` binary
    flock-py/               pyo3 bindings
  fleet/                    (BSL — source-available, NOT Apache)
    flockd/                 registry, scheduler, fan-out, migrations
  testing/fleet-sim/        the 10,000-database simulation

loomdb/                     one product, one repo — see doc 03
```

**The dependency rule, which is not negotiable:** `flock-*` and `loom-*` may depend on `substrate-*`.
They may **never** depend on each other. Separate repositories make this structurally impossible
rather than merely discouraged, which is why it is worth the cost of a versioned dependency.

### §5.1 — `PageStore` (substrate-pager)

The one door to durable state. **No crate writes a file or an S3 object directly.** If code needs
bytes to persist, it goes through here — which is what makes encryption, tiering, integrity
scrubbing, and air-gap enforcement implementable in exactly one place.

```rust
pub trait PageStore: Send + Sync {
    /// Read one logical page as of a manifest. Verifies the content hash on every read.
    fn read(&self, manifest: &ManifestId, page_no: LogicalPageNo) -> Result<Page>;

    /// Stage a page write. Content-addressed: writing identical bytes twice is one page.
    /// Not durable until `commit`.
    fn write(&self, page_no: LogicalPageNo, bytes: &[u8]) -> Result<PageId>;

    /// The commit point (§3.1). Returns the manifest that is now durable.
    fn commit(&self, txn: Txn) -> Result<ManifestId>;

    /// O(1). Serialize the current manifest and return its id. No page is copied.
    fn snapshot(&self) -> Result<ManifestId>;

    /// O(1). A new store sharing this CAS, with a private overlay. Writes to the fork are
    /// NEVER visible in the base. This is the isolation guarantee both products are built on.
    fn fork(&self, from: &ManifestId) -> Result<Box<dyn PageStore>>;

    /// Which logical pages differ between two manifests.
    fn diff(&self, a: &ManifestId, b: &ManifestId) -> Result<PageDiff>;

    /// Three-way classification against a merge base. Consumed by LoomDB's merge engine.
    fn diff3(&self, base: &ManifestId, a: &ManifestId, b: &ManifestId) -> Result<ThreeWayDiff>;

    /// Sweep every page unreferenced by any live manifest. Refcounts are RECOMPUTED here,
    /// never read from a counter file (§3.1).
    fn gc(&self, live_manifests: &[ManifestId]) -> Result<GcStats>;
}
```

Style rules that go with it: **no `async` in `substrate-pager` or `substrate-wal`** — the core is
pure, synchronous, and testable without a runtime. `async` appears only at the store and protocol
layers, via tokio. Errors are `thiserror` enums, one per crate. No `unwrap()` outside tests.

### §5.2 — `SqlKernel` (flock-kernel)

```rust
pub trait SqlKernel: Send {
    fn open(store: Arc<dyn PageStore>, opts: KernelOpts) -> Result<Self> where Self: Sized;
    fn query(&mut self, sql: &str) -> Result<ArrowStream>;
    fn execute(&mut self, sql: &str) -> Result<u64>;
    /// Flush kernel state into pages and return the durable manifest.
    fn checkpoint(&mut self) -> Result<ManifestId>;
    /// The escape hatch (§6.2): write a vanilla .duckdb file.
    fn export(&mut self, path: &Path) -> Result<()>;
}
```

**Implementation note, and be honest in the code about which path was taken.** The ideal is a
DuckDB filesystem hook backed by `PageStore` — DuckDB believes it has a file; we hand it pages. If
`duckdb-rs` does not expose that seam, the fallback is: DuckDB owns a temp file, and `flock-core`
syncs file ↔ pages at transaction boundaries. The fallback is correct but copies more. **Correct
first, fast later** — and document the choice in `flock-kernel`'s crate docs.

### §5.3 — `flock-core` public API

```rust
Flock::open(path_or_pool, db_name) -> Db

Db::query(sql)          -> ArrowStream
Db::execute(sql)        -> u64
Db::fork(name)          -> Db          // O(1)
Db::snapshot()          -> ManifestId  // O(1)
Db::restore(manifest)   -> ()          // O(1)
Db::sleep()             -> WakeToken
Db::export_duckdb(path) -> ()          // §6.2
```

---

## §6 — Interoperability

### §6.1 — Arrow everywhere

Query results are Arrow. Not a bespoke row format, not JSON. Arrow is how the Python bindings, the
CLI, the fan-out service, and every downstream tool speak without conversion cost.

### §6.2 — `export_duckdb` — the escape hatch

`Db::export_duckdb(path)` writes a **vanilla, standard, we-are-not-in-the-loop `.duckdb` file.**

This is a product decision, not a feature. The single largest objection to adopting a new storage
engine is *"what if you disappear, or I hate you."* The answer must be a one-line command that hands
the user their data in a format with an ecosystem, and no dependency on us. Anything less makes us
a hostage-taker, and serious buyers can smell it.

The export path is tested in CI on every commit. It is not allowed to rot.

---

## §7 — Performance targets

Numbers we hold ourselves to. A regression against any of these is a release blocker, not a
follow-up ticket.

| Operation | Target | Why this number |
| --- | --- | --- |
| Fork a database | **< 1 ms** | Must be cheap enough to do per-request without thinking. |
| Snapshot | **< 1 ms** | Same. Pre-migration snapshots must be free enough to take unconditionally. |
| Wake from object storage (p99, first row) | **< 250 ms** | The line between "hibernation" and "restore job". Above this, sleeping stops being invisible. |
| Overlay-chain read overhead at depth 8 vs flat | **< 20 %** | Deep branch trees must stay usable, or the collapse threshold is load-bearing rather than an optimisation. |
| TPC-H SF0.1 through the stack vs raw DuckDB | **< 15 % overhead** | Our storage layer must not make the query engine we host look bad. |
| Fan-out pruning, selective predicate | **> 95 % of fleet pruned** | Fan-out that touches every database is just a slow full scan. |

---

## §8 — Isolation

Worker processes are the default. For hostile multi-tenancy — where the threat model includes a
tenant *deliberately* attacking the engine through crafted SQL or a crafted database file — process
isolation is insufficient and the boundary must be hardware-assisted: **Firecracker microVMs**, one
per awake database, with the CAS mounted read-only and the overlay private.

The scheduler talks to an abstract `WorkerBackend`. Processes now, microVMs later, and no
architectural change in between. This is deliberate: we do not want to discover in year two that the
scheduler assumed a shared address space.

---

## §9 — Air-gap, CUI, and the regulated deployment

The segment that pays the most and competes the least: classified, defense, and regulated
environments that cannot phone home, cannot pull an update, and cannot assume a correct clock.

### §9.1 — Threat model and dedup scope

**A store belongs to exactly one named pool. Pools never share pages, even when hashes are
identical. The pool name is part of the object key prefix.** There is no configuration that turns
this off.

This costs deduplication across pools and buys the thing that matters: the guarantee that data
cannot flow between two classification boundaries through the storage layer.

**The plaintext-hashing tradeoff, stated plainly.** `PageId = BLAKE3(plaintext)` — we hash the
plaintext and encrypt for storage (§9.2), so we can verify both on read. That means an adversary who
can observe PageIds and who *guesses* a page's plaintext can confirm the guess by hashing it. Within
a dedup scope, this leaks **membership**: "does any database in this pool contain exactly this
page?" For public or single-tenant data, this is an acceptable price for dedup and cache sharing.

For **CUI and classified pools it is not acceptable**, and there the keyed-hash mode
(`PageId = BLAKE3_keyed(pool_key, plaintext)`) is **mandatory, not optional**. A per-pool key means
identical plaintext in two pools produces different PageIds, membership cannot be confirmed without
the key, and dedup is confined to the pool — which §9.1 already requires anyway. The cost is zero
cross-pool dedup, which we were already forgoing.

`docs/threat-model.md` carries the full write-up. The feature flag is `keyed-hash`.

### §9.2 — Offline licensing, and the clock

License = a signed token (Ed25519 now; structured so ML-DSA can be added as a **second** signature
without a format break) with `{licensee, features, not_before, not_after, grace_days}`, verified
against a compiled-in public key. No network. Ever.

**Enforcement returns `Ok` | `Warning(days_left)` | `Degraded`. It never hard-stops.**

This is a hard rule with a real reason. If a license expiry can stop a database from serving reads
in a facility that cannot phone home for a renewal, then we have built a weapon that fires at our
own customer during an incident. `Degraded` disables **fleet-plane administrative features only**.
**Reads and writes never stop.** Not on expiry, not on a corrupt license file, not on a missing one.

**Clock handling**, because air-gapped enclaves drift and operators change the clock:

- All *internal* decisions (timeouts, leases, backoff, idle-sleep) use the **monotonic** clock.
  Nothing internal can be broken by moving the wall clock.
- The wall clock is used for **license checks only**, against a persisted **high-water mark that
  never moves backward.** Set the clock back five years and the license does not un-expire.
- Tolerance for **±30 days** of legitimate enclave drift before a warning is raised.

The clock-jump scenarios in the licensing tests are not hypothetical — they are the ones that happen.

### §9.3 — Capacity worksheet

The numbers an operator needs before they buy hardware.

```
Per sleeping database:      manifest only, resident in registry
                            ≈ 8 bytes/page × (db_size / 64 KiB) + ~200 B header
                            → a 1 GiB database sleeps in ≈ 128 KiB of manifest.
                            → 10,000 sleeping 1 GiB databases ≈ 1.2 GiB of manifests.  Laptop-class.

Per awake database:         DuckDB working set + hot pages
                            ≈ 64 MiB baseline + working set

Local CAS cache:            sized to the working set of AWAKE databases, not the fleet.
                            Eviction is LRU and ONLY evicts pages confirmed durable remotely.
                            A page not yet uploaded is NEVER evictable — that rule is what makes
                            cache eviction incapable of losing data.

Object storage:             sum of unique pages across the pool, post-dedup, plus sealed WAL
                            segments behind the last checkpoint.

Egress:                     zero in the airgap profile, enforced at compile time (§9.4).
```

### §9.4 — The airgap feature flag

`--features airgap` is not a runtime toggle; it is a **compile-time amputation.** With it enabled,
the only permitted network endpoint in the entire binary is the configured object-store URL.
Everything else — telemetry, update checks, license servers, crash reporting — is *not conditionally
disabled, it is not compiled in.* You cannot configure your way back into egress, and an auditor can
verify that by reading the binary rather than trusting our word.

CI runs the full test suite inside a network-isolated container. A test that needs the network to
pass is a test that fails.

---

## §10 — What must be true before we believe any of this

This engine is being written largely by an AI, at speed. That is a legitimate reason for a buyer to
distrust it, and the answer is not enthusiasm — it is **evidence a skeptic can check.**

1. **Model-based oracles.** Every core primitive (pages, forks, branch trees, merges) has a simple,
   obviously-correct, in-memory reference implementation. Property tests assert the real engine and
   the model agree under randomized operation sequences. When they disagree, one of them is wrong,
   and we find out in seconds rather than in a customer's incident channel.
2. **Crash injection.** A filesystem wrapper that can kill a write at any byte boundary. The
   property, asserted over 10,000+ randomized runs: after any crash and recovery, the store equals
   some prefix of committed transactions.
3. **Deterministic replay.** The same WAL replayed twice yields byte-identical manifests. If replay
   is not deterministic, recovery is not verifiable, and no other guarantee here means anything.
4. **Fuzzing as a gate, not a chore.** Format changes require a fuzz-target update in the same
   commit.

If these tests are ever skipped "for now", the correct response is to stop and re-run them. A
database with soft foundations is worse than no database, because people trust it with things they
cannot get back.
