# CLAUDE.md — Substrate

**Read this file, then [docs/02](docs/02-embedded-single-node-engine-architecture.md),
[docs/03](docs/03-agent-native-database-architecture.md), and
[docs/04](docs/04-flockdb-loomdb-unified-roadmap.md) before writing any code.** Those docs are the
architecture of record. Code that contradicts them is a bug in the code, or a bug in the docs —
resolve it in the docs first, then in code.

Substrate is the storage engine underneath two products: **FlockDB** (embedded analytical DB +
fleet manager) and **LoomDB** (agent-native DB). It is a *dependency*, not a destination. Users of
either product never visit this repository; contributors and auditors do.

---

## The rules

These are not style preferences. Each one exists because violating it produces a specific,
expensive failure, named below.

### 1. Crate dependency direction is one-way

`substrate-*` crates are the shared foundation. `flock-*` and `loom-*` crates (in the separate
`flockdb` and `loomdb` repositories) may depend on `substrate-*`. They may **never** depend on each
other.

> **Why:** the moment LoomDB depends on FlockDB code, the engine has forked to serve two masters and
> a small team is maintaining two databases. Separate repositories make this structurally impossible
> rather than merely discouraged.

Within substrate, the direction is `pager ← wal ← store ← security`. A lower crate never imports a
higher one.

### 2. All durable state goes through `PageStore`

No crate writes a file or an S3 object directly. If bytes need to persist, they go through the
`PageStore` trait (docs/02 §5.1).

> **Why:** encryption, tiering, integrity scrubbing, air-gap enforcement, and metrics are then
> implementable in exactly one place. A single direct `File::create` elsewhere silently opts that
> data out of all five.

The only code permitted to touch the filesystem or the network is the CAS backend inside
`substrate-pager`, the segment writer inside `substrate-wal`, and the object-storage client inside
`substrate-store`.

### 3. Every substrate change ships with its tests

A change is not complete without:

- **unit tests**,
- **a fuzz-target update if the on-disk format changed**, and
- **a deterministic-replay test.**

> **Why:** a format change without a fuzz update means the fuzzer is testing last week's format and
> reporting green. That is worse than no fuzzer, because it is believed.

### 4. The oracles are not optional

Every core primitive has a simple, obviously-correct, in-memory **model implementation**, and
property tests assert the real engine agrees with the model under randomized operation sequences.
This applies to: pages/forks (P1), branch trees and merges (P4), and — in LoomDB — merge and
taint (L1, L2).

> **Why:** this engine is written fast and largely by an AI. That is a legitimate reason for a buyer
> to distrust it, and the rebuttal is not enthusiasm — it is a differential test that has tried ten
> thousand times to break it. If tests are ever skipped "for now," **stop and re-run them.** A
> database with soft foundations is worse than no database, because people trust it with data they
> cannot get back.

### 5. No network calls anywhere except `substrate-store`'s object-storage client

Everything must compile and pass tests with `--features airgap`, which removes all outbound
networking **at compile time**.

> **Why:** airgap is not a runtime toggle you can misconfigure. It is an amputation an auditor can
> verify by reading the binary. A test that needs the network to pass is a test that fails.

### 6. Errors: `thiserror` per crate. No `unwrap()` outside tests

One error enum per crate. No `unwrap()`, no `expect()`, no `panic!()` in library code. Ever.

> **Why:** a panic in a storage engine is an unplanned process death, and an unplanned process death
> during a commit is exactly the case the crash-recovery suite exists to survive. Do not manufacture
> the disaster you are defending against.

`clippy::unwrap_used` and `clippy::expect_used` are denied in library code.

### 7. No `async` in `substrate-pager` or `substrate-wal`

The core is pure, synchronous, and testable without a runtime. `async` (tokio) appears only in
`substrate-store` and in protocol layers.

> **Why:** deterministic replay and crash injection require deterministic execution. A future that
> can be polled in a different order on a different day is not a foundation you can prove anything
> about.

### 8. Commit ordering is sacred

```
1. page bytes → CAS, fsync          (durable but unreferenced — harmless garbage)
2. WAL commit record, fsync         ← THE COMMIT POINT. atomic.
3. in-memory manifest update        (now visible to readers)
```

Never reorder these. Never batch step 2 with step 3. A crash between 1 and 2 leaves orphan pages
that GC sweeps; a crash after 2 is a committed transaction that recovery replays. There is no
window in between, and that is the entire durability guarantee.

### 9. Liveness comes from manifests, never from a counter

GC recomputes refcounts from live manifests on recovery. There is no counter file, and there never
will be.

> **Why:** a counter file is a second source of truth about which data is alive. A corrupt one
> silently deletes live pages, and you find out months later.

### 10. Prefer boring

This is the most safety-critical code in the company. When there is a clever way and an obvious way,
take the obvious one and leave a comment saying what the clever one would have been.

---

## Layout

```
crates/
  substrate-pager/     pages, CAS, manifests, branch trees, GC     (sync, no deps on wal/store)
  substrate-wal/       segments, commit protocol, recovery         (sync)
  substrate-store/     object storage, tiering, sleep/wake         (async, tokio)
  substrate-security/  page encryption, offline licensing          (P6)
testing/
  fuzz/                cargo-fuzz targets + crash injection
  integration/         cross-crate lifecycle tests
docs/                  02, 03, 04 (architecture of record), substrate-api.md, threat-model.md
```

## Working commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --features airgap        # must pass with no network
cargo +nightly fuzz run <target>                # testing/fuzz
```

## Performance targets (docs/02 §7 — a regression blocks a release)

| Operation | Target |
| --- | --- |
| fork / snapshot | < 1 ms |
| wake from object storage (p99, first row) | < 250 ms |
| overlay-chain read overhead at depth 8 | < 20 % vs flat |
| TPC-H SF0.1 through the stack | < 15 % over raw DuckDB |

## Definition of done

- [ ] `cargo fmt --all --check` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test --workspace` green
- [ ] `cargo test --workspace --features airgap` green
- [ ] fuzz target updated if the on-disk format changed
- [ ] model oracle still agrees with the implementation
- [ ] no `unwrap()` / `expect()` / `panic!()` added to library code
- [ ] public items have doc comments with examples
