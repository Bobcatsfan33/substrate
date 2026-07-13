# 03 — Agent-Native Database Architecture (LoomDB)

> **Status:** Authoritative. Reads alongside [02](./02-embedded-single-node-engine-architecture.md)
> (the shared engine), [04](./04-flockdb-loomdb-unified-roadmap.md) (sequencing), and
> [05](./05-loomdb-test-spec.md) (the acceptance tests and integrity invariants that decide whether
> any of this is real).
>
> **Revision note (v2).** This document was substantially rewritten after a review against an
> independent design for the same category. Four things changed, and they matter:
>
> 1. **An action layer.** The previous version governed what an agent *wrote* and said nothing about
>    what an agent *did*. That was a hole big enough to walk a company through, and it made the
>    taint-and-recall demo quietly dishonest — see §4.4.
> 2. **Observations and claims are now different objects.** A source record and a model's belief
>    about it are not the same kind of thing, and conflating them makes "what did the agent actually
>    know" unanswerable.
> 3. **Policy governs information *flow*, not just branch scope.** Capability tokens answer "may you
>    write here." They never answered "may this data reach that model, that output, that action" —
>    which is where prompt injection and exfiltration actually live.
> 4. **Merge happens at record granularity, not page granularity.** The previous design would have
>    reported conflicts between facts that do not conflict. See §3.3.

---

## §1 — Why agents break databases

Every database in production was designed for a client that is either a human or a deterministic
program. Both share assumptions an LLM agent violates on its first request.

**The client knows what it wants to do.** A transaction is a plan. Agents do not have plans; they
have hypotheses. An agent wants to *try* something, look at the result, and abandon it. The database
primitive for that is `ROLLBACK`, which is useless here, because the agent needs to *keep* the
abandoned attempt, compare it against two others, and merge the winner.

**Writes are trustworthy because the client is trusted.** An agent's write is a *derivation*: it read
six documents of unknown provenance, one of which may have been poisoned by whoever wrote that web
page, and produced a fact. Six months later that source is found to be compromised. "Which of my
400,000 stored facts are downstream of it, and what do I do about them?" is unanswerable in every
database on the market — an audit log records *that* a write happened, not *what it was derived
from*.

**The agent only reads and writes.** It does not. It suspends accounts, closes tickets, moves money,
and files reports. A database that records an agent's beliefs but not its *effects* is auditing the
harmless half.

**Isolation is per-connection.** Agents are recursive and delegating. A spawns B and C, hands each a
slice of authority, and they write concurrently. "Which data may this agent touch, and which data may
*influence* what it produces?" must be a provable property, not a convention.

**Retrieval is the application's problem.** So every framework bolts a vector index onto a database
that knows nothing about it, and reimplements a bad context-packing loop — while the database, the
thing that actually knows what was written, by whom, superseding what, sits there answering
`SELECT`s.

LoomDB's primitives are the ones agents actually need: **observe, claim, branch, merge, rewind,
retrieve, act, taint.**

### §1.1 — The one-sentence version

> **LoomDB gives an agent a database it can branch like git, that records where every belief came
> from and every action it authorized, and that can tell you — and undo — exactly what a poisoned
> input contaminated.**

---

## §2 — Product shape

An **agent-native database**, delivered as an MCP server (`loomd`) plus an embeddable Rust library.

The agent is a first-class client. It speaks MCP, gets a session, and that session *is* a branch of
the tenant's state. It forks three hypotheses, writes freely in each, merges the one that worked, and
rewinds the two that didn't — and every write carries a signed record of what it was derived from,
while every external effect passes a gate that checks whether it was allowed.

**Non-goals**, so nobody drifts into them:

- **Not a vector database.** We have vector indexes because retrieval needs them. We are not
  competing on ANN recall benchmarks.
- **Not an agent framework.** No prompt templates, no chains, no orchestration. We are the memory,
  audit, and control substrate *underneath* whatever framework the user already picked.
- **Not a SIEM.** See §9.5.
- **Not a distributed services platform.** LoomDB is one binary and one dependency (object storage).
  A design that needs Postgres, ClickHouse, Kafka, a text-index cluster, and a vector store to answer
  "what did the agent know" has moved the complexity into the operator's lap.

---

## §3 — Architecture

Four layers. Each depends only on the one below it.

```
   ┌───────────────────────────────────────────────────────────────────────┐
   │  PROTOCOL     loomd — MCP server. The agent's whole world.            │
   │               open_session read observe claim branch merge rewind     │
   │               retrieve propose_action audit taint                     │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  GOVERNANCE   loom-policy              loom-action                    │
   │               read / influence /       propose → authorize → execute  │
   │               disclosure / action      idempotent · receipts ·        │
   │               information FLOW         compensation                   │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  MEMORY       loom-memory              loom-planner                   │
   │               observations (raw)       retrieve(goal, budget)         │
   │               claims (bitemporal,        ─► PackedContext + citations │
   │                 superseded, never                                     │
   │                 deleted, evidence-      loom-provenance               │
   │                 bearing)                WriteEnvelope · derivation DAG│
   │               procedural (skills)       taint ─► RecallPlan           │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  CORE         loom-branch — sessions, capability tokens,              │
   │               record-level merge over substrate's page-level diff3    │
   └───────────────────────────┬───────────────────────────────────────────┘
   ┌───────────────────────────▼───────────────────────────────────────────┐
   │  SUBSTRATE    fork/snapshot/diff3/gc · WAL · S3 tiering · security    │
   │               (doc 02 §3.1 — the SAME engine FlockDB runs on)         │
   └───────────────────────────────────────────────────────────────────────┘
```

That bottom box is why this is buildable by a small team. "Fork a database in under a millisecond,
sleep a million of them in object storage, never lose a committed write" is one very hard problem. We
solve it once, in substrate, and two products stand on it.

**It is also why we can afford branches at all.** A design that stores agent state in a
general-purpose database has to make every projection branch-aware and then expire branches on a TTL,
because idle branches cost money. On substrate a branch is a manifest pointer, an idle branch is
bytes in S3, and a TTL is an optimisation rather than a necessity.

### §3.1 — P1: Branchable state

**A session is a branch.** `open_session(tenant, meta)` forks the tenant's base image (substrate
`fork`, O(1), target **< 100 ms warm**) and returns a handle plus a capability token. A million idle
sessions are a million manifests — bytes in object storage, no compute.

**Capability tokens are the *scope* mechanism.**

```rust
CapabilityToken = signed { session, branch_scope, expiry }
```

Every operation verifies the token covers the target branch, and — this must hold under an adversary
— **there is no code path in LoomDB that touches a page outside the token's branch scope.** Not a
debug path, not an admin path, not a "just this once" helper.

> **What tokens do *not* do, stated plainly.** A capability token answers "*may you write here*". It
> does **not** answer "*may this data influence what you produce, or what you do*". An earlier version
> of this document claimed tokens gave agents "a provable blast radius"; that is true of branch scope
> and false of information flow, and the gap is exactly where prompt injection lives. Flow is §5's
> job, not the token's.

`rewind(branch, commit)` is O(1) — a pointer move. The abandoned suffix survives until GC, which is
what makes "explore three hypotheses, throw two away" cost nothing *and stay auditable*.

### §3.2 — Observations and claims are different objects

This is the distinction the whole memory layer hangs on.

| | **Observation** | **Claim** |
|---|---|---|
| What it is | A record we received from a source | A statement we *believe*, possibly wrongly |
| Who made it | The world | Us — a rule, a model, or a human |
| Can it be wrong? | Yes, but it is still *what the source said* | Yes, and then it is *our* mistake |
| Deleted? | Never. Corrected by a later observation. | Never. Superseded, contradicted, or invalidated. |

```rust
struct Observation {
    id: ObservationId,
    source: SourceRef,          // which system, which record, what trust class
    observed_at: Option<Time>,  // when it was true in the world (may be unknown)
    ingested_at: Time,          // when we learned it
    payload_hash: Hash,
    trust_class: TrustClass,    // VerifiedSystem | UserSupplied | ThirdParty | Untrusted
}

struct Claim {
    id: ClaimId,
    predicate: Predicate,
    subject: EntityId,
    object: Value,
    valid: Interval,            // when it holds in the WORLD  (bitemporal, see below)
    known: Interval,            // when WE believed it          (assigned by the engine, immutable)
    confidence: Confidence,     // value + method + calibration context
    method: Method,             // Direct | Rule | Statistical | LanguageModel | Human | Imported
    evidence: Vec<Ref>,         // observations and/or other claims. MAY BE EMPTY — see below.
    status: ClaimStatus,        // Asserted | Superseded | Contradicted | Stale | Invalidated | Expired
}
```

**An observation is not automatically a claim.** The identity provider said this account signed in
from Belarus. That is an observation. "This account is compromised" is a claim *derived from* it, by a
method, with a confidence, and it can be wrong in ways the observation cannot.

**Bitemporality.** `valid` is when the statement holds in the world; `known` is when LoomDB believed
it. They move independently: an observation that arrives today may describe last week (`known` starts
today, `valid` starts last week). Corrections *close* a `known` interval and open a new one; they
never overwrite. Unknown bounds are an explicit `Unknown`, never a guessed timestamp.

Every as-of query answers against both axes, and **the response always states the `valid_at` and
`known_at` it used**, including when they defaulted to "now" — otherwise the answer is not
reproducible, and an unreproducible audit is not an audit.

**Confidence carries its method.** Values produced by unrelated methods are never averaged without a
declared aggregation rule. A 0.8 from a calibrated rule set and a 0.8 from a language model are not
the same number and must not be arithmetically combined as though they were.

#### The invariant that makes this worth having

> **A claim with no evidence cannot authorize an action.**

An unsupported claim may be stored — agents speculate, and forbidding that would just push
speculation somewhere we cannot see it. But it is marked `evidence: []`, it is *ineligible* to
justify an external effect, and the action gateway (§4) refuses it. Enforced in code, not in review.

### §3.3 — Merge, at the right granularity

**Merge happens at record granularity. Substrate's page-level `diff3` is a fast prefilter, not the
merge.**

This is a correction to the previous design, and it was not a nitpick. Substrate classifies *pages* —
a physical 64 KiB unit. Two agents writing two unrelated facts that happen to land in the same page
would have been reported as a conflict. That is a merge engine that lies.

```
substrate.diff3(base, a, b)          →  which PAGES changed          (cheap, physical)
    ↓
decode records in those pages        →  which RECORDS changed        (the real question)
    ↓
typed merge rules per record         →  merged records, or conflicts (semantic)
    ↓
replay as NEW commits on the target  →  policy re-evaluated at merge time
```

The last line matters as much as the first: **a merge is not a copy of the branch's pages into the
base.** It is a proposed set of new writes, re-validated — because the world moved while the branch
was off exploring, and a write that was allowed when the branch forked may not be allowed now.

Typed rules, applied in order:

| Class | Rule |
| --- | --- |
| **Additive** (counters, sets, append-only logs) | Merge arithmetically. Two branches each incrementing by 3 yields +6, not a conflict. This is most agent concurrency. |
| **Claims about the same predicate+subject** | Resolve by validity interval, then by **provenance rank** — a claim derived from a `VerifiedSystem` observation outranks one derived from an `Untrusted` scrape. |
| **Convergent** (both branches produced identical content) | Take it. Two agents deriving the same fact from the same source is *convergence*, and content addressing detects it for free. Calling it a conflict would generate enormous pointless work. |
| **Everything else** | Hand to a `MergePolicy` callback. The application decides; we do not guess. |
| **Non-mergeable** | Return a `MergeConflictReport`. |

The `MergeConflictReport` is serde-serializable **and written to be read by an LLM**: human-readable
context strings, not page numbers. The consumer of a merge conflict here is very often a language
model deciding what to do next, and a report it cannot understand produces a bad decision.

### §3.4 — P2: Provenance

**The write path rejects any write without a valid envelope.** Enforced at `loom-branch`'s write
entry point — not middleware, not a decorator someone can forget. A bypassable audit trail is worse
than none, because it is *believed*.

```rust
struct WriteEnvelope {
    actor:        ActorId,        // which agent/human/tool
    session:      SessionId,
    branch:       BranchId,
    context_hash: Hash,           // hash of the context that produced this write
    delegation:   Vec<ActorId>,   // A→B→C: the whole chain of authority, not just the last hop
    derived_from: Vec<Ref>,       // ENGINE-CAPTURED read-set + caller-supplied external URIs
    intent:       String,         // why, in the agent's own words
    policy:       PolicyDecisionId, // which policy version allowed this, and on what inputs
    signature:    Signature,      // Ed25519 now, ML-DSA as a second signature — non-repudiable
}
```

**`derived_from` is engine-captured, not caller-supplied.** During a session transaction the engine
records which records and pages were actually read, and attaches that read-set to the next write.
Callers may *add* external source URIs; they cannot *omit* what they read. An agent — or an attacker
steering one — cannot launder a derivation by declining to mention it.

Envelopes and their `derived_from` edges persist as a **derivation DAG** in a per-tenant system store
(on substrate, its own pool), indexed by actor, session, source, and time.

#### Staleness: the scalpel

When an observation is corrected or invalidated, every claim downstream of it is marked **`Stale`**
and pushed onto a recalculation queue. Stale claims remain readable and auditable — but they are
**excluded from action-eligible queries** until recomputed.

This is the everyday mechanism, and it is deliberately *softer* than taint. Most of the time the right
answer to "an input changed" is not "revert history"; it is "stop letting that conclusion authorize
anything until you have re-derived it."

#### Taint and recall: the sledgehammer

```
taint(source_ref)  →  walk the DAG downstream, across every branch and session of the tenant
                   →  RecallPlan {
                          reversible: [(branch, commit, rewind-boundary | targeted tombstone)],
                          IRREVERSIBLE: [(action_id, receipt, compensation | escalation)],
                      }
                   →  dry-run report, written for a human to read and approve
                   →  execute() is a SEPARATE, token-gated call
```

Read that as an incident: *a source you trusted turns out to have been poisoned.* Everywhere else the
answer is "we don't know what it touched." Here it is a plan that reverts **exactly** the contaminated
writes, across branches you forgot existed, **and nothing else**.

> **§3.4.1 — The honest part, and the reason this section was rewritten.**
>
> **Taint cannot undo an action.** Rewinding a manifest reverts *writes*. It does not un-suspend an
> account, un-send an email, or un-wire money. The previous version of this document implied that
> taint reverted everything downstream of a poisoned source, and that was false in the way that gets
> a company sued.
>
> A `RecallPlan` therefore has **two sections**, and the irreversible one is listed **first** in every
> report:
>
> - **Reversible** — writes and claims. Rewind or tombstone. We do this.
> - **Irreversible** — actions that already had an effect in the world. For each, the plan names the
>   action, its receipt, and either a registered **compensating action** (`suspend_account` →
>   `restore_account`) or, where none exists, an explicit **escalation to a human**, because inventing
>   one would be worse than admitting there isn't one.
>
> A taint report that shows six reverted writes and silently omits the account it suspended is not an
> audit tool. It is a liability.

Taint never auto-executes. A system that can silently delete a tenant's data on a signal is a system
that can be turned into a weapon. Propose, then execute on an explicit, token-gated command.

### §3.5 — P3: Memory and retrieval

Typed stores per tenant, all on substrate, all requiring envelopes:

- **Observations** — append-only. What sources told us. Corrected, never overwritten.
- **Claims** — bitemporal, evidence-bearing, superseded rather than deleted (§3.2). "What did we
  believe about ACME's revenue on 3 March, and what do we believe now?" are different questions with
  different answers, and both must be answerable.
- **Episodic** — task trajectories: goal, plan, actions, outcome, evaluation. What happened.
- **Procedural** — a tool/skill registry with success counters. What has worked before.

**Indexes:** full-text (tantivy) and vector (usearch HNSW), both **branch-aware**. v0 is per-branch
delta indexes consulted overlay-then-base. If that is too slow, the fallback is rebuild-on-fork behind
a flag — **with an issue filed.** What we do *not* do is silently ship an index that returns results
from the wrong branch. A retrieval layer that leaks another branch's facts is a correctness bug
wearing a performance costume.

**`loom-planner` v0:**

```
retrieve(goal, budget_tokens, constraints) -> PackedContext

  candidates  ← vector-k ∪ BM25 ∪ recency ∪ entity-graph 1-hop
  filter      ← INFLUENCE POLICY (§5) — applied BEFORE packing, not after
  score       ← weighted relevance, penalising Stale and low-confidence claims
  pack        ← greedy under token budget, deduplicated
  emit        ← packed block + PER-ITEM CITATIONS (source refs from the provenance DAG)
```

Every retrieved item carries a citation, because the provenance layer already knows where it came
from. Heuristics only in v0 — no learned model — but the scoring interface is designed so one drops in
later without touching callers.

The metric that matters is **tokens per correct answer**, not recall@k. An agent's context window is
the scarcest resource in the system; a retrieval layer that spends 8,000 tokens to answer what 900
would have answered is failing even at perfect recall.

---

## §4 — The action layer

Everything above governs what an agent *believes*. This governs what it *does*, and it is the half
that has teeth.

### §4.1 — The rule

> **No agent process may call an external tool directly. Ever. Every side effect goes through the
> action gateway.**

Not because we distrust the agent's intentions — because an agent is a program whose control flow is
decided by text it read, and some of that text was written by someone who wants your accounts
suspended.

### §4.2 — The lifecycle

```
propose  →  the agent asks. It never acts.
            { action_type, target, parameters, justification_claims, idempotency_key }

authorize → deterministic code, not a model, checks ALL of:
              • the claims cited actually exist and are not Stale/Contradicted/Expired
              • they meet the policy's evidence, freshness, and confidence thresholds
              • NO cited claim is unsupported (§3.2 invariant)
              • information-flow policy permits this data to authorize this effect (§5)
              • the branch is allowed to act at all — SIMULATION BRANCHES ARE DENY-BY-DEFAULT
              • human approval, where the policy requires it

execute   → idempotent, keyed. The connector may be called at most once per key.

settle    → Succeeded(receipt) | Failed(reason) | INDETERMINATE
```

### §4.3 — `Indeterminate` is a first-class outcome

A connector times out. Did the account get suspended or not? **We do not know**, and a system that
guesses is a system that either double-suspends or reports a success that never happened.

`Indeterminate` is a terminal-until-reconciled state, surfaced to the operator, and it blocks nothing
else. This is one of the places where being unglamorous is the entire value.

### §4.4 — Actions are what make taint honest

Every action record links to the claims that justified it, which link through the derivation DAG to
the observations beneath them. So `taint(source)` reaches actions the same way it reaches writes —
and, per §3.4.1, reports them in the section it cannot undo.

An action's **compensation** is registered with the connector, not invented at incident time:

```rust
Connector {
    execute:      fn(params, idempotency_key) -> Receipt,
    compensate:   Option<fn(receipt) -> Receipt>,   // None is an ALLOWED and honest answer
}
```

`compensate: None` means the recall plan escalates to a human. That is a real answer. Fabricating a
compensating action for an irreversible effect is not.

---

## §5 — Policy: information flow, not just access

Access control asks "may you *read* this row." That question is not sufficient for an agent, because
the agent will read a document and then let it *steer* a model, an output, and a tool call.

Five questions, five policies:

| Policy | The question |
| --- | --- |
| **Read** | May this actor retrieve this object? |
| **Influence** | May this data enter a model context, a rule evaluation, an embedding, or a derived claim — *for this purpose*? |
| **Disclosure** | May the resulting output be shown to this audience, and with what redaction? |
| **Action** | May this effect occur, given actor, evidence, confidence, freshness, branch, approval, and target? |
| **Retention** | How long may direct *and derived* forms persist, and what happens on expiry? |

**Influence is the one nobody implements, and it is the one that stops the attack.** A document an
agent is permitted to *read* may still be forbidden from *influencing* a customer-facing answer or a
`suspend_account` call. Labels propagate along the derivation DAG: a claim derived from restricted
evidence inherits the restriction, and the retrieval planner filters on it **before packing the
context** (§3.5) rather than trying to scrub the model's output afterwards.

Policy decisions are **versioned, recorded, and referenced from the envelope** (`policy:
PolicyDecisionId`). "Which policy version allowed this, evaluated against which inputs" is a question
with an exact answer.

**Fail closed.** If the policy layer cannot render a decision, protected reads and *all* external
actions are denied. There is no configuration that fails open for actions.

---

## §6 — The `AgentStore` API

Every call carries the session capability token; `loomd` verifies before dispatch.

```rust
pub trait AgentStore {
    fn open_session(&self, tenant: TenantId, meta: SessionMeta)
        -> Result<(SessionHandle, CapabilityToken)>;

    // --- memory ---
    fn observe(&self, tok: &Tok, obs: Observation, env: WriteEnvelope) -> Result<ObservationId>;
    fn claim(&self, tok: &Tok, claim: Claim, env: WriteEnvelope)       -> Result<ClaimId>;
    //                                        ^^^^^^^^^^^^^^^^^^^ not optional. ever.
    fn read(&self, tok: &Tok, q: Query, at: AsOf)                      -> Result<Records>;

    // --- branching ---
    fn branch(&self, tok: &Tok, from: BranchId, name: &str) -> Result<(BranchId, CapabilityToken)>;
    fn merge(&self, tok: &Tok, src: BranchId, dst: BranchId, policy: MergePolicy)
        -> Result<MergeOutcome>;   // Merged(CommitId) | Conflict(MergeConflictReport)
    fn rewind(&self, tok: &Tok, branch: BranchId, to: CommitId) -> Result<()>;

    // --- retrieval ---
    fn retrieve(&self, tok: &Tok, goal: &str, budget: TokenBudget, c: Constraints)
        -> Result<PackedContext>;   // influence-filtered, cited

    // --- doing things in the world ---
    fn propose_action(&self, tok: &Tok, a: ProposedAction) -> Result<ActionRecord>;
    fn action_status(&self, tok: &Tok, id: ActionId)       -> Result<ActionRecord>;
    //  there is NO `execute` on this trait. Agents propose. The gateway acts.

    // --- accountability ---
    fn audit(&self, tok: &Tok, aql: &str)      -> Result<AuditResult>;
    fn taint(&self, tok: &Tok, src: SourceRef) -> Result<RecallPlan>;   // dry run
    fn execute_recall(&self, tok: &Tok, plan: RecallPlanId) -> Result<RecallOutcome>;
}
```

Note what is absent: no `execute_action`, and no `write` that skips an envelope. The API cannot
express the unsafe thing.

**AQL v0** — a small, deliberately un-Turing-complete query surface over the derivation DAG: by actor,
session, source, time-range, derivation-depth, action, and policy decision. Exposed through the
`audit` tool and a human CLI (`loom audit "<aql>"`). Small on purpose: this surface is touched by an
auditor and by a possibly-compromised agent, and it must be impossible to weaponise.

---

## §7 — Tool descriptions are prompts

`loomd` is an MCP server. The agent's entire world is the tool list, so the tool descriptions **are**
prompts: write them for an LLM, with few-shot examples, and make every error state the *corrective
action*.

> `branch 'b7' is not covered by your capability token; call branch() from your session root first`

> `claim clm_88 is Stale (its evidence obs_41 was corrected at 14:02). Re-derive it before citing it
> to justify an action.`

Not `ERR_SCOPE_VIOLATION`. The consumer of that string is a model deciding what to do next, and a good
error message is the difference between recovery and a retry loop.

---

## §8 — Integrity invariants

These hold at all times, and [doc 05](./05-loomdb-test-spec.md) tests every one of them.

1. No write exists without a valid, signed `WriteEnvelope`.
2. No claim exists without either evidence or an explicit unsupported status — and an unsupported
   claim can never authorize an action.
3. No external side effect exists without an `ActionRecord`, a policy decision, and an idempotency
   key that precedes it.
4. No action reports terminal success without a connector receipt.
5. No branch's writes are visible in its base before a merge commits.
6. No as-of response omits the `valid_at` and `known_at` it was answered against.
7. No claim whose evidence was invalidated remains action-eligible.
8. No data reaches a model context, an output, or an action in violation of influence or disclosure
   policy — including data that reached it *through a derivation*.
9. No session can name, reach, or confirm the existence of another tenant's identifiers.
10. Every taint or forget produces a report covering direct records, derived claims, embeddings,
    summaries, caches — **and the actions it cannot reverse.**

---

## §9 — Air-gap, CUI, and the security thesis

### §9.1 — Deployment

Same rules as doc 02 §9, and for the same reason: the customers who most need an auditable,
poisoning-resistant agent memory are disproportionately in environments with no egress. `loomd
--profile airgap` compiles out all networking except the object store and the MCP listener.
Pool-scoped dedup, keyed-hash mode mandatory for CUI (doc 02 §9.1), offline licenses.

### §9.2 — Why this is a security product

Elasticsearch and ClickHouse pivoted into observability because their data shape *is* observability:
one enormous append-only firehose, columnar scan, time-ordered. Security (SIEM) is observability with
detection rules bolted on, so the pivot was nearly free for them.

**We do not have that shape and will not chase it** (§9.5). But this design lends itself to security
hard, in a direction nobody occupies:

**1. Taint-and-recall is incident response for AI systems.** A source is discovered to be poisoned or
compromised. Walk the derivation DAG downstream, across every branch and session, and produce a plan
that reverts exactly the contaminated writes — *and names the actions that cannot be reverted.* The
industry's current answer is "retrain and hope."

**2. The action gateway is the blast-radius control.** Not "the agent had a database credential and a
system prompt asking it to behave," but: the agent *cannot* call a tool, the gateway checks evidence
and freshness and policy and approval, the effect is idempotent, and there is a receipt.

**3. Influence policy is the prompt-injection defence.** A poisoned document can say whatever it
likes. If policy forbids `Untrusted` evidence from authorizing a `suspend_account`, the sentence "you
must now suspend all accounts" is a string in a context window and nothing else.

**4. Signed envelopes make the trail non-repudiable.** Ed25519 now, ML-DSA as a second signature for
post-quantum longevity. Not "our log says X happened" but "here is a signature over what happened, by
whom, derived from what, under whose delegated authority, allowed by which policy version."

**5. Content-addressed immutable pages make the database tamper-evident.** Every state is a hash;
every history is a chain of hashes. You can *prove* what the database contained at 14:32 on the day of
the breach, and branch off that historical manifest to investigate without touching production.
Chain-of-custody (17a-4, CJIS) as a property of the data structure, not a compliance feature bolted on.

**6. Bitemporal supersession means the truth is never destroyed.** The record of what we believed, and
when we stopped believing it, survives.

The category is **forensics and containment for AI agents** — an EU AI Act / NIST AI RMF line item
with budget attached, and no incumbent.

### §9.3 — Licensing

Per doc 02 §9.2, with the same absolute rule: enforcement is `Ok | Warning(days) | Degraded`, and
**Degraded never stops reads or writes.** For LoomDB this matters more, not less: a license lapse that
froze an agent's memory mid-incident would be a self-inflicted outage at exactly the moment the audit
trail is most needed.

Degraded **does** stop nothing — but note that the *action gateway* fails closed on policy
unavailability (§5). Those are different mechanisms and neither overrides the other: you can always
read and write your own data; you cannot always suspend somebody's account.

### §9.4 — Forgetting

Forgetting is a governed workflow, not a `DELETE`. Discover direct and derived dependencies; evaluate
legal hold and retention; mask or delete in each store; invalidate dependent claims; rebuild affected
summaries and embeddings; verify absence; produce a completion report.

Where immutable audit requirements forbid deletion, store a tombstone or an encrypted payload whose
key is destroyed — and say so in the report rather than claiming the data is gone.

### §9.5 — Where this design does *not* help

Honesty here is cheaper than a failed enterprise POC.

- **We are not a SIEM.** No detection engine, no rules language, no threat intel, no correlation at
  scale. If someone wants to ingest a petabyte of firewall logs, we are the wrong answer and should
  say so in the first meeting.
- **We are not a log analytics engine.** Our shape is many small corpora, not one firehose. Against
  ClickHouse on ingest throughput or scan cost, we lose on day one.
- **We have no endpoint or network telemetry story.** None. Not a roadmap item.
- **Plaintext content-addressing leaks membership within a dedup scope** (doc 02 §9.1). A real
  weakness; it is why keyed-hash mode is mandatory for CUI pools, and it lives in the threat model
  rather than a footnote.
- **We cannot un-ring a bell.** Taint reverts writes. It escalates actions. Any pitch that implies
  otherwise is one incident away from being a deposition.

---

## §10 — What must be true

Same standard as doc 02 §10, plus one specific to this layer:

**The merge engine, the recall planner, and the policy engine get model-based oracles.** A simple,
obviously-correct, in-memory reference implementation of branch/merge, of DAG-walk/taint, and of
policy evaluation, against which the real implementation is differentially tested under randomized
operation sequences.

The reason is not coverage. It is that `taint()` returning an *incomplete* plan is the worst failure
this system can have: it tells a customer their poisoned data is contained when it is not. That must
be impossible-by-construction, not caught-in-review — and the only way to earn the claim is a property
test that has tried ten thousand times to break it.

[Doc 05](./05-loomdb-test-spec.md) is the acceptance catalog. A capability is not done when it works;
it is done when the test that would have caught it failing is green.
