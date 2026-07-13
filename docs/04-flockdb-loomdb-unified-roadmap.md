# 04 — FlockDB + LoomDB: The Unified Roadmap

> **Status:** Authoritative sequencing. Reads with [02](./02-embedded-single-node-engine-architecture.md)
> (FlockDB) and [03](./03-agent-native-database-architecture.md) (LoomDB).

---

## §1 — One engine, two products

Two products, two markets, two go-to-motions — and **one hard problem underneath both.**

> Fork a database in under a millisecond. Sleep a million of them in object storage. Wake one in
> 250ms. Never lose a committed write.

Solve that once and you can build:

- **FlockDB** — because if databases are free to have and fast to wake, you can give every one of
  your 40,000 customers a real, isolated, per-tenant analytical database instead of a `tenant_id`
  column.
- **LoomDB** — because if databases are free to *fork*, an agent's session can be a branch: it can
  try three hypotheses, keep the one that worked, and rewind the rest.

The same `fork()` call. Two entirely different businesses.

**This is the bet.** A small team cannot build two databases. It *can* build one storage engine and
two thin products, provided the engine is genuinely shared and genuinely frozen. Which is why:

- Substrate is its own repository, versioned and semver-frozen at v1.0.
- `flock-*` and `loom-*` may depend on `substrate-*` and **never on each other.**
- Substrate hits v1.0 **before** either product starts. No exceptions, no "we'll stabilise it later."

The failure mode we are engineering against is the one that kills every platform play: the shared
core quietly forks to serve two masters, and eighteen months later you are maintaining two engines
with one team.

---

## §2 — Sequencing

**Order is load-bearing.**

```
   P0 ─► P1 ─► P2 ─► P3 ─► P4 ─► P5 ─► P6            SUBSTRATE  ──────► tag v1.0
        pager  wal  store branch harden security                          │
                                                                          │
                        ┌─────────────────────────────────────────────────┴──┐
                        │                                                    │
                        ▼                                                    ▼
              F1 ─► F2 ─► F3 ─► F4 ─► F5                    L1 ─► L2 ─► L3 ─► L4 ─► L5
              kernel cli sync fleet fanout                  branch prov mem  mcp  harden
                        FLOCKDB                                        LOOMDB
```

Substrate P0→P6 is strictly serial: the WAL cannot be written before pages exist, tiering cannot be
written before the WAL, and hardening cannot happen before there is something to harden.

**After P6, the two product tracks are independent and parallelisable.** LoomDB does not depend on a
single line of FlockDB code — it depends on substrate v1.x. Two engineers (or two Claude Code
instances on separate branches, in separate repositories) can run F and L concurrently.

---

## §3 — The quarters

### Q1 — Substrate to v1.0 *(P0–P6)*

The foundation. Nothing ships to anyone.

| | |
| --- | --- |
| **P0** | Workspace, CLAUDE.md, CI (fmt / clippy -D warnings / test / **no-egress**). |
| **P1** | Content-addressed page store. Pages, CAS, manifests, fork/snapshot/diff, crash-safe GC. Property tests with a **model oracle**; fuzz target. |
| **P2** | WAL + crash recovery. Commit protocol, deterministic replay, checkpointing. **Crash-injection harness — 10,000 randomized runs, green.** |
| **P3** | Object-storage tiering. MinIO via testcontainers. **sleep/wake < 250ms.** Pool isolation. Airgap flag. → *tag substrate-v0.1* |
| **P4** | Branch trees at depth. Overlay collapse at N=8, three-way diff, O(1) rewind, criterion benchmarks, **model-oracle fuzzing.** → *tag substrate-v0.2* |
| **P5** | Hardening. API freeze, integrity scrubbing, metrics hooks, extended fuzz session, `docs/substrate-api.md`. → **tag substrate-v1.0** |
| **P6** | Encryption (XChaCha20-Poly1305, key hierarchy, keyed-hash mode for CUI) + offline licensing (Ed25519, ML-DSA-ready, never-hard-stop, high-water-mark clock). |

**Exit criterion:** the crash-injection suite passes 10,000 runs, the model oracles agree under
randomized fuzzing, and the API is frozen. *Only then* does anyone build a product on it.

### Q2 — FlockDB to OSS launch *(F1–F3)*

The first thing a stranger can use.

| | |
| --- | --- |
| **F1** | `flock-kernel` (DuckDB via `SqlKernel`) + `flock-core` (`Db` handle). TPC-H SF0.1, **< 15% overhead**. |
| **F2** | The `flock` CLI + Python bindings (`pip install flockdb`). **The five-command quickstart, tested verbatim in CI.** |
| **F3** | `flock-sync`: WAL shipping, read replicas, point-in-time restore. Chaos suite green. |

**The launch artifact is the quickstart**, and it ends on the line that sells the product:

```bash
pip install flockdb
flock init sales.db
flock query sales.db "CREATE TABLE t AS SELECT * FROM 'sales.parquet'"
flock fork sales.db --as experiment          # < 1ms. no bytes copied.
flock query experiment "DELETE FROM t WHERE region = 'EMEA'"
flock query sales.db  "SELECT count(*) FROM t"   # untouched. two databases now.
```

**Exit criterion:** someone who has never heard of us forks a database within 90 seconds of landing
on the README.

### Q3 — LoomDB to v0.1 and the flag-plant *(L1–L4, parallel with Q2)*

The category claim.

| | |
| --- | --- |
| **L1** | `loom-branch`: sessions-as-branches, capability tokens, the typed merge engine. Model-oracle property tests. |
| **L2** | `loom-provenance`: `WriteEnvelope` enforced at the write path, engine-captured read-sets, the derivation DAG, **taint → RecallPlan**. |
| **L3** | `loom-memory` (episodic / semantic-bitemporal / procedural) + `loom-planner` v0 retrieval with citations. |
| **L4** | `loomd`: the MCP server. AQL v0. → *tag loomdb-v0.1* |

#### §3.1 — The Q3 demo *(this is a specification, and L4's end-to-end test scripts it verbatim)*

A scripted scenario, **no LLM required to run it** — a fake agent client drives the MCP surface.
It is the launch demo, and its output must read as a narrative, not as test logs.

```
1. OPEN      Agent opens a session.  → forks the tenant base image. <100ms.
2. BRANCH    Three hypotheses, three branches: h1, h2, h3.
3. WRITE     Each branch writes facts. Every write carries a WriteEnvelope: actor, intent,
             and an ENGINE-CAPTURED derived_from — including a shared upstream source, S.
4. MERGE     h2 won. Merge it. Additive facts merge arithmetically; one genuine conflict
             surfaces a MergeConflictReport an LLM could actually act on.
5. REWIND    h1 and h3 are rewound. O(1). They remain auditable — nothing is destroyed.
6. AUDIT     `loom audit` over the DAG tells the whole story: who wrote what, derived from
             what, under whose delegated authority.
7. TAINT     Source S turns out to be poisoned.
             taint(S) → a RecallPlan naming EXACTLY the writes downstream of S — across the
             merged branch AND the rewound ones — and NOTHING else.
             Dry run first. Execute is a separate, token-gated call.
```

Step 7 is the entire company. Every other database on earth answers *"we don't know what it
touched."*

**Exit criterion:** the demo runs green in CI, and the taint report is legible to a person who has
never read the code.

### Q4 — Fleet plane, hardening, and the regulated market *(F4–F5, L5)*

Where the revenue is.

| | |
| --- | --- |
| **F4** | `flockd`: registry, wake-on-query scheduler, airgap profile. **10,000-database simulation on a laptop.** |
| **F5** | Fan-out with registry pruning (**>95% pruned**) + the migration orchestrator with canary cohorts and free pre-migration snapshots. |
| **L5** | Airgap certification suite, signed offline update bundles, the two long soaks, `docs/operations.md`. → *tag flockdb-v0.2, loomdb-v0.2* |

The airgap certification suite is the gate on the segment that pays most and competes least:

- 120-day accelerated clock,
- ±30-day wall-clock jumps mid-run,
- license expiry mid-run — **reads and writes must not stop**,
- storage exhaustion — **clean backpressure, never corruption**.

**Exit criterion:** both soaks run a full CI-nightly window with zero errors and flat memory. Flat
memory is not a nice-to-have; a slow leak in a process designed to stay up for a year is a
guaranteed outage with a long fuse.

---

## §4 — What we are actually selling

Worth writing down, because it changes what we build when a tradeoff comes.

**FlockDB sells an economic argument.** Per-tenant isolation without per-tenant cost. The buyer is a
platform engineer with 40,000 customers and a `tenant_id` column they are afraid of. The pitch is a
number: what they pay now versus what they pay when idle databases cost nothing.

**LoomDB sells a risk argument.** The buyer is whoever owns the blast radius when an agent writes
something wrong into the system of record. The pitch is a question nobody else can answer:
*"A source you trusted turns out to have been poisoned. Which of your agent's decisions are
downstream of it, and how do you undo exactly those?"*

Different buyers, different budgets, different sales cycles — one engine, one team.

---

## §5 — The honesty clause

This engine is being built fast, and largely by an AI. That is a legitimate reason for a database
buyer to distrust it. **Enthusiasm is not a rebuttal; evidence is.**

Which is why the model-based oracles (P1, P4, L1, L2), the crash-injection suite (P2), and the
deterministic-replay tests are not a testing strategy. They are **the product's credibility**, and
they are non-negotiable:

> If any prompt's tests get skipped "for now" — stop and re-run them. A database with soft
> foundations is worse than no database, because people trust it with data they cannot get back.

Two claims we hold ourselves to publicly:

1. **We will not ship a number we cannot reproduce.** Every target in doc 02 §7 has a benchmark in
   the repository, and every one that regresses blocks a release.
2. **We will say where we are weak.** Doc 03 §9.4 lists what this design does *not* do, in writing,
   in the repository, before a customer discovers it in a POC. The plaintext-hashing membership leak
   is documented in the threat model rather than buried.

A storage engine earns trust exactly once, and it does it by being unembarrassed about its limits.
