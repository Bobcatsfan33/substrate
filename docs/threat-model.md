# Threat model

> Referenced from [docs/02 §9.1](./02-embedded-single-node-engine-architecture.md). This is the
> honest version, including what we do **not** defend against.

---

## What we defend

| Threat | Defence | Where |
| --- | --- | --- |
| **A stolen disk, a leaked bucket, a decommissioned drive, a backup that ends up somewhere it shouldn't** | XChaCha20-Poly1305 per page. The bytes at rest are ciphertext. | `substrate-security::crypt` |
| **Bit rot; a failing disk; a storage layer that silently returns wrong bytes** | Every page is re-hashed on every read. Corruption is detected, never served. A background scrubber finds rot in pages *nobody has read*. | `substrate-pager`, `scrub.rs` |
| **A tampered page** | The AEAD tag. On an unencrypted store, the content hash. | `crypt::open` |
| **A page moved to a different address** by someone with write access to storage | The page id is bound into the AEAD's additional authenticated data. Ciphertext lifted from page A and stored at page B fails to authenticate. Without this, an attacker could shuffle valid, correctly-encrypted pages between addresses and produce a database that decrypts perfectly and says something entirely different. | `crypt::aad_for` |
| **One database's key compromised** | Key hierarchy: pool master → per-database data keys, derived one-way. A data key cannot be walked back to the master, nor sideways to a sibling. | `keys.rs` |
| **Data crossing a classification boundary through the storage layer** | Pools. A store belongs to exactly one, the pool is the first component of every object key, and pools never share a page even when the bytes are identical. Not a check — they are physically different objects in different places. | `substrate-store::remote` |
| **A key file left group- or world-readable** | Refused, not warned about. A warning about a secret is a warning that scrolls past. | `keys.rs` |
| **An operator winding the clock back to un-expire a licence** | A high-water-mark clock that never moves backward. Set the clock back five years and the licence does not un-expire. | `license.rs` |
| **A forged or edited licence** | Ed25519 over canonical claim bytes. ML-DSA slot reserved in the format today, so adding a post-quantum signature later is not a format break. | `license.rs` |
| **A crash at any byte boundary** | The commit protocol, and a suite that kills the write path at every byte in turn and checks. 50,000 cycles. | `substrate-wal` |
| **Unexpected egress** | `--features airgap` removes networking at compile time. Not a runtime toggle — an amputation an auditor can verify by reading the binary. | all crates |

---

## §1 — The plaintext-hashing tradeoff

**This is the most important section in this document, and it describes a real weakness.**

Page identity is computed on the **plaintext**:

```
PageId = BLAKE3(plaintext)        # identity
stored = XChaCha20-Poly1305(...)  # storage
```

### Why it has to be this way

If the id were the hash of the *ciphertext*, two identical pages would get two different ids
(different nonces), and:

- deduplication collapses to nothing;
- a fork stops being free, because it can no longer share pages by construction;
- the cache stops being trivially coherent;
- every property in docs/02 §3.1 goes with them.

The engine would still work. It would simply no longer be worth having.

### What it leaks

**Membership.** An adversary who can observe `PageId`s — from the object store's key listing, say —
and who can **guess** a page's plaintext, can hash the guess and confirm it:

> *"Does any database in this pool contain a page whose contents are exactly
> `SALARY OF EMPLOYEE 4471 IS 220000`?"*

They cannot *read* anything. They can *confirm* a guess. For low-entropy, structured, guessable
content, that distinction is thinner than it sounds — this is the classic weakness of convergent
encryption, and we are not going to pretend otherwise.

### What closes it

The **`keyed-hash` build**: `PageId = BLAKE3_keyed(pool_key, plaintext)`.

A guess is then unconfirmable without the pool key, and identical plaintext in two pools produces
different ids. Deduplication is confined to the pool — which docs/02 §9.1 already requires, so in a
CUI deployment this costs nothing that was on offer.

**For CUI and classified pools this mode is mandatory, not advisory.** It is a *mutually exclusive
build mode*, not a feature flag: with it compiled in, constructing an unkeyed store fails at the
door, with no override, because a CUI deployment must not be *configurable* back into the weak mode
by a tired operator at 2am.

### The decision, plainly

| Deployment | Mode | Why |
| --- | --- | --- |
| Public / single-tenant / non-sensitive | Unkeyed | Dedup and cache sharing are worth more than resistance to a membership oracle over data that is not secret. |
| Multi-tenant with confidential data | Unkeyed, **one pool per tenant** | The pool boundary already prevents cross-tenant confirmation. |
| **CUI, classified, regulated** | **`keyed-hash`. Not optional.** | A membership oracle over classified content is a disclosure. |

---

## §2 — The convergent nonce

The AEAD nonce is derived, not random:

```
nonce = BLAKE3_keyed(data_key, "substrate-page-nonce-v1" || page_id)[..24]
```

The instinct that a deterministic nonce is dangerous is a **correct instinct**, and it deserves a real
answer rather than a reassurance.

Nonce reuse breaks a stream cipher when **the same key and nonce encrypt different plaintexts** — the
keystream cancels under XOR and both plaintexts fall out. That cannot happen here, because the nonce
is a function of the page id, and the page id is a function of the plaintext. **Different plaintext ⇒
different nonce.**

The only way to reuse a nonce is to encrypt the *same plaintext* under the *same key* — which produces
the same ciphertext, and reveals exactly one fact: that the two pages are identical. Content
addressing already told you that, out loud, in the id.

What determinism buys is that encryption is **idempotent**, which a write-once, content-addressed,
deduplicating store requires. Random nonces would mean the same page encrypts to different bytes each
time, and the CAS would have no way to know it already had it.

XChaCha20's 192-bit nonce gives an enormous margin on the birthday bound. It is not a number anyone
reaches.

---

## §3 — What we do **not** defend against

Stated plainly, because a threat model that only lists victories is marketing.

- **An attacker with root on a running host.** They can read the keys out of memory. Encryption at
  rest is not encryption in use, and we do not claim otherwise. (Confidential computing — SEV-SNP,
  TDX — is the answer to that threat, and it is not in this repository.)
- **A compromised key provider.** If your KMS hands out the master key, the pool is open. We have made
  the blast radius per-database rather than per-pool; we have not made it zero.
- **Traffic analysis.** Access patterns to object storage leak *which* pages are hot, how large a
  database is, and roughly when it changed. We encrypt contents, not behaviour.
- **A malicious *authorised* writer.** Someone with legitimate write access can write wrong data, and
  it will be faithfully, durably, and verifiably stored. Substrate makes it *tamper-evident* — you
  can prove what the database contained and when it changed — but it cannot tell a bad decision from
  a good one. (That is what LoomDB's provenance layer is for, and even that produces evidence, not
  prevention.)
- **Denial of service.** We bound recursion, allocation from untrusted length prefixes, and overlay
  chain depth. We do not rate-limit; that is the caller's job.
- **Side channels.** No constant-time guarantees beyond what the underlying crypto crates provide.
- **A licence that someone simply patches out of the binary.** Offline licensing is a *contractual*
  mechanism, not a security boundary, and treating it as one would lead us to make it hostile —
  which is exactly what §9.2's never-hard-stop rule exists to prevent.

---

## §4 — The rule we will not trade away

> **Licence enforcement never stops a read or a write.** Not on expiry, not on a corrupt licence file,
> not on a missing one, not on a clock that has jumped to 2031.

This is a security decision, not a commercial one. Our customers run in facilities that cannot phone
home for a renewal — that is what "offline licence" *means*. A licence check that could stop a
database from serving reads in an air-gapped facility is a remote kill switch we have built into our
own customer's infrastructure and then lost the key to.

`Degraded` disables **fleet-plane administration**. It does not disable the database. If we ever have
to choose between enforcing a licence and a customer's ability to read their own data during an
incident, we enforce nothing.
