# 03 — Agent-Native Database Architecture (LoomDB)

> **Status:** Authoritative. Reads alongside [02](./02-embedded-single-node-engine-architecture.md)
> (the shared engine) and [04](./04-flockdb-loomdb-unified-roadmap.md) (the sequencing).

---

## §1 — Why agents break databases

Every database in production was designed for a client that is either a human or a deterministic
program. Both of those clients share assumptions that an LLM agent violates on its first request.

**Assumption 1: the client knows what it wants to do.** A transaction is a plan. Agents do not have
plans; they have hypotheses. An agent wants to *try* something, look at the result, and abandon it
without a trace if it was wrong. The database primitive for "try and abandon" is `ROLLBACK`, which
is useless here because the agent needs to *keep the abandoned attempt around, compare it against
two other attempts, and merge the winner.* No database does that.

**Assumption 2: writes are trustworthy because the client is trusted.** An agent's write is a
*derivation*: it read six documents of unknown provenance, one of which may have been poisoned by
whoever wrote that web page, and produced a fact. Six months later that source turns out to be
compromised. The question "which of my 400,000 stored facts are downstream of that source, and what
do I do about them?" is unanswerable in every database on the market. You cannot answer it with an
audit log, because an audit log records *that* a write happened, not *what it was derived from*.

**Assumption 3: isolation is per-connection.** Agents are recursive and delegating. Agent A spawns B
and C, hands each a slice of authority, and they write concurrently. "Which pages may this agent
touch?" needs to be a *provable* property of a token, not a convention enforced by whichever code
path happens to run.

**Assumption 4: retrieval is the application's problem.** So the application bolts a vector index
onto a database that knows nothing about it, and every agent framework reimplements a bad version of
the same context-packing loop. Meanwhile the database — the thing that actually knows what was
written, by whom, when, and superseding what — sits there answering `SELECT`s.

LoomDB is a database whose primitives are the ones agents actually need: **branch, merge, rewind,
provenance, taint, retrieve.**

### §1.1 — The one-sentence version

> **LoomDB gives an agent a database it can branch like git, that records where every fact came
> from, and that can tell you — and undo — exactly what a poisoned input contaminated.**

---

## §2 — Product shape

An **agent-native database**, delivered as an MCP server (`loomd`) plus an embeddable Rust library.

The agent is a first-class client. It speaks MCP, gets a session, and that session *is* a branch of
the tenant's state. It can fork three hypotheses, write freely in each, merge the one that worked,
and rewind the two that didn't — and every write it made carries a signed record of what it was
derived from.

**Non-goals**, stated so nobody drifts into them:

- **Not a vector database.** We have vector indexes because retrieval needs them. We are not
  competing on ANN recall benchmarks, and if someone wants a pure vector store they should buy one.
- **Not an agent framework.** No prompt templates, no chains, no orchestration. We are the *memory
  and audit substrate underneath* whatever framework the user already picked.
- **Not a SIEM.** See §9.4.

---

## §3 — Architecture

Three layers. Each depends only on the one below it.

```
   ┌───────────────────────────────────────────────────────────────────────┐
   │  PROTOCOL     loomd — MCP server. The agent's whole world.            │
   │               open_session read write branch merge rewind             │
   │               retrieve audit taint                                    │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  MEMORY       loom-memory        loom-planner                         │
   │               episodic           retrieve(goal, budget) ─► PackedContext
   │               semantic  (bitemporal, superseded — never deleted)      │
   │               procedural (skills + success counters)                  │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  CORE         loom-branch                    loom-provenance          │
   │               sessions, capability tokens    WriteEnvelope, DAG,      │
   │               branch/merge/rewind            taint → RecallPlan       │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  SUBSTRATE    fork/snapshot/diff3/gc · WAL · S3 tiering · security    │
   │               (doc 02 §3.1 — the SAME engine FlockDB runs on)         │
   └───────────────────────────────────────────────────────────────────────┘
```

That bottom box is the reason this is buildable by a small team. "Fork a database in under a
millisecond, sleep a million of them in S3, never lose a committed write" is one very hard problem.
We solve it once, in substrate, and two products stand on it.

### §3.1 — The three primitives

#### P1 — Branchable state

**A session is a branch.** `open_session(tenant, meta)` forks the tenant's base image (substrate
`fork`, O(1), target **< 100 ms warm**) and returns a handle plus a capability token. A million idle
sessions are a million manifests — bytes in object storage, no compute.

**Capability tokens are the isolation mechanism.**

```rust
CapabilityToken = signed { session, branch_scope, expiry }
```

Every operation verifies the token covers the target branch, and — this is the part that has to hold
under an adversary — **there is no code path anywhere in LoomDB that touches a page outside the
token's branch scope.** Not a debug path, not an admin path, not a "just this once" internal helper.
The property tests assert it and the design exists to make it checkable rather than merely intended.

**Merge semantics.** The merge engine consumes substrate's three-way diff (doc 02 §3.1) and applies
typed rules, in this order:

| Class | Rule |
| --- | --- |
| **Additive** (counters, sets, append-only logs) | Merge arithmetically. Two branches each incrementing a counter by 3 yields +6, not a conflict. This is most agent concurrency. |
| **Temporal facts** | Resolve by validity interval, then by **provenance rank** — a fact derived from a first-party source outranks one derived from a scraped page. (Rank is a constant stub until P2 lands, then it comes from the DAG.) |
| **Everything else** | Hand to a `MergePolicy` callback. The application decides; we do not guess. |
| **Non-mergeable** | Return a `MergeConflictReport`. |

The `MergeConflictReport` is serde-serializable **and designed to be read by an LLM**: it carries
human-readable context strings, not just page numbers. The consumer of a merge conflict in this
system is very often a language model deciding what to do about it, and a report it cannot
understand is a report that produces a bad decision. Design for that reader.

`rewind(branch, manifest)` is O(1) — a pointer move. The abandoned suffix survives until GC, which
is what makes "explore three hypotheses and throw two away" cost nothing and *stay auditable*.

#### P2 — Provenance (the flagship)

**The write path rejects any write without a valid envelope.** This is enforced at `loom-branch`'s
write entry point — not as an optional middleware, not as a decorator someone can forget. If
provenance is bypassable, it is decorative, and a decorative audit trail is worse than none because
it is *believed*.

```rust
struct WriteEnvelope {
    actor:        ActorId,        // which agent/human/tool
    session:      SessionId,
    branch:       BranchId,
    context_hash: Hash,           // hash of the context that produced this write
    delegation:   Vec<ActorId>,   // A→B→C: the chain of authority, not just the last hop
    derived_from: Vec<SourceRef>, // ENGINE-CAPTURED read-set + caller-supplied external URIs
    intent:       String,         // why, in the agent's own words
    signature:    Signature,      // Ed25519 now, ML-DSA as a second signature — non-repudiable
}
```

**`derived_from` is engine-captured, not caller-supplied.** During a session transaction the engine
records which records and pages were actually read, and attaches that read-set to the next write
automatically. Callers may *add* external source URIs on top; they cannot *omit* what they read.
An agent — or an attacker steering one — cannot launder a derivation by simply not mentioning it.

Envelopes and their `derived_from` edges persist as a **derivation DAG** in a per-tenant system
store (on substrate, its own pool), indexed by actor, session, source, and time.

**Taint and recall — the capability nothing else has.**

```
taint(source_ref)  →  walk the DAG downstream, across every branch and session of the tenant
                   →  RecallPlan {
                          ordered list of (branch, commit, compensating action)
                          where action = rewind boundary | targeted tombstone
                      }
                   →  dry-run report, formatted for a human to read and approve
                   →  execute() is a SEPARATE, token-gated call
```

Read that as an incident: *a source you trusted turns out to have been poisoned or compromised.* In
every other system, the answer is "we don't know what it touched" followed by a very expensive
guess. Here the answer is a plan that reverts **exactly** the contaminated writes, across branches
you forgot existed, **and nothing else** — with a dry run you can read first.

Taint never auto-executes. A system that can silently delete a tenant's data on a signal is a system
that can be turned into a weapon. Propose, then execute on an explicit, token-gated command.

#### P3 — Memory and retrieval

Three typed stores per tenant, all on substrate, all requiring envelopes:

- **Episodic** — an append-only event log. What happened.
- **Semantic** — entity-facts, **bitemporal**: every fact carries `[valid_from, valid_to]` and is
  **superseded, never deleted.** "What did we believe about ACME's revenue, on the 3rd of March,
  and what do we believe now?" are different questions with different answers, and both must be
  answerable. Destroying the old answer to store the new one is how you make an agent's history
  unauditable.
- **Procedural** — a tool/skill registry with success counters. What has worked before.

**Indexes:** full-text (tantivy) and vector (usearch HNSW), both **branch-aware**. v0 is per-branch
delta indexes consulted overlay-then-base. If that proves too slow, the fallback is rebuild-on-fork
behind a flag — **with an issue filed.** What we do *not* do is silently ship an index that returns
results from the wrong branch. A retrieval layer that leaks another branch's facts is a correctness
bug wearing a performance costume.

**`loom-planner` v0:**

```
retrieve(goal, budget_tokens, constraints) -> PackedContext

  candidates  ← vector-k ∪ BM25 ∪ recency ∪ entity-graph 1-hop
  score       ← weighted relevance
  pack        ← greedy under token budget, deduplicated
  emit        ← packed block + PER-ITEM CITATIONS (source refs from the provenance DAG)
```

Every retrieved item carries a citation because the provenance layer already knows where it came
from. Heuristics only in v0 — no learned model — but the scoring interface is designed so one drops
in later without touching callers.

The metric that matters is **tokens per correct answer**, not recall@k. An agent's context window is
the scarcest resource in the system; a retrieval layer that spends 8,000 tokens to answer what 900
would have answered is failing even at perfect recall.

### §3.2 — The protocol layer

`loomd` is an MCP server. The agent's entire world is the tool list, so **the tool descriptions are
prompts** — write them for an LLM, with few-shot examples, and make every error message state the
*corrective action*:

> `branch 'b7' is not covered by your capability token; call branch() from your session root first`

Not `ERR_SCOPE_VIOLATION`. The consumer of that string is a model that must decide what to do next,
and a good error message is the difference between recovery and a retry loop.

---

## §5 — The `AgentStore` API

The complete surface. Every call carries the session capability token; `loomd` verifies before
dispatch.

```rust
pub trait AgentStore {
    fn open_session(&self, tenant: TenantId, meta: SessionMeta)
        -> Result<(SessionHandle, CapabilityToken)>;

    fn read(&self, tok: &CapabilityToken, q: Query)   -> Result<Records>;
    fn write(&self, tok: &CapabilityToken, w: Write, env: WriteEnvelope) -> Result<CommitId>;
    //                                              ^^^^^^^^^^^^^^^^^^^^ not optional. ever.

    fn branch(&self, tok: &CapabilityToken, from: BranchId, name: &str)
        -> Result<(BranchId, CapabilityToken)>;
    fn merge(&self, tok: &CapabilityToken, src: BranchId, dst: BranchId, policy: MergePolicy)
        -> Result<MergeOutcome>;   // Merged(CommitId) | Conflict(MergeConflictReport)
    fn rewind(&self, tok: &CapabilityToken, branch: BranchId, to: CommitId) -> Result<()>;

    fn retrieve(&self, tok: &CapabilityToken, goal: &str, budget: TokenBudget, c: Constraints)
        -> Result<PackedContext>;

    fn audit(&self, tok: &CapabilityToken, aql: &str)      -> Result<AuditResult>;
    fn taint(&self, tok: &CapabilityToken, src: SourceRef) -> Result<RecallPlan>;
}
```

**AQL v0** — a small, deliberately un-Turing-complete query surface over the derivation DAG: by
actor, session, source, time-range, and derivation-depth. Exposed through the `audit` tool and a
human CLI (`loom audit "<aql>"`). Small on purpose: this is the surface an auditor and a possibly-
compromised agent both touch, and it must be impossible to weaponise.

---

## §9 — Air-gap, CUI, and the security thesis

### §9.1 — Deployment

Same rules as doc 02 §9, and for the same reason: the customers who most need an auditable,
poisoning-resistant agent memory are disproportionately in environments with no egress. `loomd
--profile airgap` compiles out all networking except the object store and the MCP listener.
Pool-scoped dedup, keyed-hash mode mandatory for CUI (doc 02 §9.1), offline licenses.

### §9.2 — Why this is a security product, not an observability one

Elasticsearch and ClickHouse pivoted into observability because their data shape *is* observability:
one enormous append-only firehose, columnar scan, time-ordered. Security (SIEM) is observability with
detection rules bolted on, so the pivot was nearly free for them.

**We do not have that shape and we will not chase it** (§9.4). But this design lends itself to
security very hard, in a direction nobody is occupying:

**1. Taint-and-recall is incident response for AI systems.** A source is discovered to be poisoned,
compromised, or simply wrong. Walk the derivation DAG downstream, across every branch and session,
and produce a plan that reverts exactly the contaminated decisions. This is data-poisoning
containment, and today the industry's answer is "retrain and hope." It is the single most valuable
thing in this repository.

**2. Capability tokens give agents a provable blast radius.** "This agent could not have touched
that data" is a statement we can defend structurally, because no code path exists that would let it.
Compare with the industry standard — an agent with a database credential and a system prompt
politely asking it to behave.

**3. Signed envelopes make the trail non-repudiable.** Ed25519 now, ML-DSA as a second signature for
post-quantum longevity. Not "our log says X happened" but "here is a signature over what happened,
by whom, derived from what, under whose delegated authority."

**4. Content-addressed immutable pages make the whole database tamper-evident.** Every state is a
hash; every history is a chain of hashes. You can *prove* what the database contained at 14:32 on
the day of the breach, and branch off that historical manifest to investigate — without touching
production. That is chain-of-custody and WORM-adjacent (17a-4, CJIS) as a property of the data
structure, not a compliance feature bolted on top.

**5. Bitemporal supersession means the truth is never destroyed.** The record of what we believed,
and when we stopped believing it, survives.

Put together, the category is **forensics and containment for AI agents** — an EU AI Act / NIST AI
RMF line item with budget attached, and no incumbent.

### §9.3 — Licensing

Per doc 02 §9.2, with the same absolute rule: enforcement is `Ok | Warning(days) | Degraded`, and
**Degraded never stops reads or writes.** For LoomDB this matters more, not less: a license lapse
that froze an agent's memory mid-incident would be a self-inflicted outage during exactly the moment
the audit trail is most needed.

### §9.4 — Where this design does *not* help (read this before writing marketing copy)

Honesty here is cheaper than a failed enterprise POC.

- **We are not a SIEM.** No detection engine, no rules language, no threat intel, no correlation at
  scale. If someone wants to ingest a petabyte of firewall logs, we are the wrong answer and we
  should say so in the first meeting.
- **We are not a log analytics engine.** Our shape is many small corpora, not one big firehose.
  Positioned against ClickHouse on ingest throughput or scan cost, we lose on day one.
- **Fan-out is not real-time detection.** Fan-out across 10,000 databases is an analytical
  operation, not a sub-second alerting path.
- **We have no endpoint or network telemetry story.** None. Not a roadmap item.
- **Plaintext content-addressing leaks membership within a dedup scope** (doc 02 §9.1). It is a real
  weakness, it is why keyed-hash mode is mandatory for CUI pools, and it goes in the threat model
  rather than in a footnote.

The security value is in **provenance, containment, isolation, and tamper-evidence for AI agents** —
not in watching a network. Anyone who pitches this as a SIEM is setting up a POC we will fail.

---

## §10 — What must be true

Same standard as doc 02 §10, and one addition specific to this layer:

**The merge engine and the recall planner get model-based oracles.** A simple, obviously-correct,
in-memory reference implementation of branch/merge and of DAG-walk/taint, against which the real
implementation is differentially tested under randomized operation sequences.

The reason is not test coverage. It is that `taint()` returning an *incomplete* plan is the worst
failure this system can have: it tells a customer their poisoned data is contained when it is not.
That failure must be impossible-by-construction, not caught-in-review — and the only way to earn that
claim is a property test that has tried ten thousand times to make it fail.
