<div align="center">

# substrate

**A storage engine where forking a database costs nothing.**

Immutable content-addressed pages · O(1) fork, snapshot, and rewind · crash-safe by construction

[![CI](https://github.com/Bobcatsfan33/substrate/actions/workflows/ci.yml/badge.svg)](https://github.com/Bobcatsfan33/substrate/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust 1.82+](https://img.shields.io/badge/rust-1.82%2B-orange.svg)](https://www.rust-lang.org)

</div>

---

## What this is

Substrate is the storage engine underneath two databases:

- **[FlockDB](https://github.com/Bobcatsfan33/flockdb)** — thousands of small analytical databases that sleep in object storage and wake in under 250ms. Give all 40,000 of your customers a *real* isolated database instead of a `tenant_id` column.
- **[LoomDB](https://github.com/Bobcatsfan33/loomdb)** — an agent-native database whose sessions are branches. An agent tries three hypotheses, merges the one that worked, rewinds the two that didn't — and every write records what it was derived from.

Two products, two markets, **one hard problem**:

> Fork a database in under a millisecond. Sleep a million of them in object storage. Wake one in 250ms. Never lose a committed write.

This repository solves that problem, once. You probably don't want to depend on it directly — you want one of the two databases above. It lives in the open because a durability claim nobody can audit is worth nothing.

## The idea

A page is a block of bytes whose **identity is its content**: `PageId = BLAKE3(bytes)`. A *manifest* maps logical page numbers to those ids, and is itself content-addressed. So the entire state of a database is one 32-byte value — which means:

| Operation | What actually happens | Cost |
|---|---|---|
| `snapshot()` | remember a `ManifestId` | **O(1)** |
| `fork()` | start a new head at a `ManifestId` | **O(1)** |
| `rewind()` | move a head to an older `ManifestId` | **O(1)** |
| `diff()` | compare two sorted lists of hashes | O(*changed*) |

Not one byte is copied in any of them.

```rust
use substrate_pager::{Pager, PageStore, StoreConfig};

let db = Pager::in_memory(StoreConfig::default())?;

let mut txn = db.begin()?;
db.write(&mut txn, 0, b"the original".to_vec())?;
let v1 = db.commit(txn)?;             // a snapshot is just this id

let experiment = db.fork(&v1)?;       // copies nothing
let mut txn = experiment.begin()?;
experiment.write(&mut txn, 0, b"a wild idea".to_vec())?;
experiment.commit(txn)?;

// Two databases now. The base never noticed.
assert_eq!(db.read_head(0)?.as_bytes(),         b"the original");
assert_eq!(experiment.read_head(0)?.as_bytes(), b"a wild idea");
```

Fork isolation here is not *enforced* — it is **structural**. A manifest is an immutable value, and the fork is holding a different one. There is no code path that could leak a write from a fork into its base, which is a considerably stronger claim than "we added a check."

## Why you should be suspicious of this, and what we did about it

This engine was written fast, and largely by an AI. If you are about to put data you cannot afford to lose into it, that should worry you. It worries us. Enthusiasm is not a rebuttal, so here is the evidence instead:

**A model oracle.** Every core primitive has a second implementation — a naive map-of-maps that copies the whole database on every fork, is absurdly slow, and is *obviously* correct. Property tests run randomized operation sequences against both and assert they agree about every byte. When they disagree, one of them is wrong, and we find out in milliseconds instead of in your incident channel. It has already caught real bugs in the real engine.

**Coverage-guided fuzzing.** The same invariants, driven by a fuzzer that watches which branches the engine takes and steers toward the ones nobody has hit. A quarter of a million executions per minute, and the on-disk format cannot change without the fuzz target changing in the same commit.

**Crash injection.** A filesystem layer that kills writes at any byte boundary — inside a page write, inside a WAL record, between the commit fsync and the manifest install. **10,000 randomized crash-and-recover cycles in CI** (50,000 locally), plus a run against a real disk where `fsync` is a genuine syscall. The property: after a crash anywhere, the recovered store equals **some prefix of committed transactions**, and anything `commit()` acknowledged is still there.

It has already found three real durability bugs — including a recovery that was not idempotent, which would have corrupted any database that crashed *while recovering from a crash*.

**Deterministic replay.** The same log replayed twice yields byte-identical manifests. If recovery isn't deterministic, it isn't verifiable, and nothing else here means anything.

We would rather you read the tests than the marketing.

## Design rules we don't break

These are in [CLAUDE.md](CLAUDE.md), and each exists because breaking it produces a specific, expensive failure:

- **Commit ordering is sacred.** Page bytes → CAS (fsync) → WAL commit record (fsync) → manifest update. A crash before the commit record leaves orphaned pages that GC sweeps. A crash after it is a committed transaction. There is no state in between.
- **Liveness comes from manifests, never a counter file.** GC recomputes what is alive by reading manifests. A refcount file is a second source of truth about which bytes are alive, and a corrupt one silently deletes live data.
- **No panics in library code.** No `unwrap`, no `expect`. A panic in a storage engine is an unplanned process death, and an unplanned process death during a commit is precisely the disaster crash recovery exists to survive.
- **No `async` in the core.** Deterministic replay and crash injection require deterministic execution.
- **No network. Anywhere.** Except one object-storage client, and `--features airgap` removes even the possibility at compile time — an amputation an auditor can verify by reading the binary, not a config flag you can get wrong.

## Status — `substrate-v0.2`

**P1–P4 complete.**

- `substrate-pager` — content-addressed pages, the CAS, manifests, O(1) fork/snapshot/rewind, three-way diff, crash-safe GC. Model oracle + fuzz target.
- `substrate-wal` — the write-ahead log and the commit protocol. Deterministic, idempotent recovery. **10,000 crash-and-recover cycles in CI.**
- `substrate-store` — object-storage tiering. **`sleep()` a database into S3 and it costs the price of its bytes; `wake()` it and the first row comes back in under 250ms.** Pool-scoped keys, so two classification boundaries cannot share a page even when the bytes are identical.

```rust
let token = db.sleep().await?;       // the whole database is now ~20 bytes of meaning
// ...wipe the machine...
let db = TieredStore::wake(new_disk, remote, &token).await?;   // and it's back
```

The manifest is fetched eagerly; pages are fetched lazily, on the first read that touches them. Waking a 100 GB database does not move 100 GB.

- `substrate-pager` (P4) — **branch trees at depth.** Forks of forks of forks, overlay manifests that collapse at depth 8, merge-base computation, named branches and tags.

### Measured (`cargo bench -p substrate-pager`)

| Operation | Target | Measured |
|---|---|---|
| fork | < 1 ms | **98 ns** — flat from 100 to 16,384 pages |
| snapshot | < 1 ms | **15 ns** |
| read one page, overlay depth 8 vs flat | < 20 % overhead | **+1.4 %** (64 KiB pages) |
| three-way diff, 1 GiB logical, 16 changed pages | scales with *changed* | **7.4 ms** |

The benchmarks caught two real performance bugs the moment they existed: every page read was deserializing an entire manifest (**1.9 ms → 180 ns**), and every one-page commit was resolving the whole base manifest, so overlays were paying for themselves and delivering nothing (**3.7 ms → 577 µs**). Both fixes are in. The honest caveat is recorded in [docs/02 §7.1](docs/02-embedded-single-node-engine-architecture.md).

Next: hardening to a frozen v1.0 API (P5), encryption and offline licensing (P6).

Not yet: SQL (that's FlockDB), agents (that's LoomDB), or a stable API. **Do not build on this until v1.0 is tagged.**

## Layout

```
crates/
  substrate-pager/     pages, CAS, manifests, branch trees, GC     (sync — no async, ever)
  substrate-wal/       segments, commit protocol, recovery         (sync)
  substrate-store/     object storage, tiering, sleep/wake         (async, tokio)
testing/
  fuzz/                cargo-fuzz targets + crash injection
  integration/         cross-crate lifecycle tests
docs/                  the architecture of record — read 02, 03, 04
```

## Build

```bash
cargo test --workspace                                   # unit + property + doc tests
cargo test --workspace --features substrate-pager/airgap # must pass with no network
cargo clippy --workspace --all-targets -- -D warnings

cargo +nightly fuzz run --fuzz-dir testing/fuzz pager_ops   # the oracle, hostile edition
```

## Reading order

1. [`docs/04`](docs/04-flockdb-loomdb-unified-roadmap.md) — why one engine and two products
2. [`docs/02`](docs/02-embedded-single-node-engine-architecture.md) — the engine and the fleet plane (FlockDB)
3. [`docs/03`](docs/03-agent-native-database-architecture.md) — branch/merge, provenance, taint-and-recall (LoomDB)
4. [`CLAUDE.md`](CLAUDE.md) — the rules, and the reason for each

## License

Apache-2.0. The engine is open because durability you cannot audit is a rumour.

</div>
