//! **The differential oracle for `get_batch` — correctness before any latency number.**
//!
//! The load-bearing property is that a batched fetch returns **byte-identical** pages to N serial
//! `get`s, for ANY set of page ids — because a wrong dedupe, a wrong order, or a torn assembly would
//! *silently corrupt* rather than error. So this hammers `get_batch` against two references at once:
//! the seed bytes it should return (ground truth) and the single-page `get` path (the thing it must be
//! interchangeable with), over hundreds of randomised sets with duplicates, repeats, and every length
//! from empty to over-2N.
//!
//! Plus **all-or-nothing**: a missing object, a failed GET, or a hash that does not match must fail the
//! *whole* batch — never a partial set returned as success. A torn batch must never masquerade as a
//! complete one, the same rule the single-page path and loom's NodeStore hold.

use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::sync::Arc;
use substrate_pager::{Cas, MemCas, Page, PageHasher, PageId, PagerError};
use substrate_store::{RemoteTier, TieredCas};

const PAGE_SIZE: usize = 4096;
const N_SEED: usize = 64;

/// A tiny deterministic RNG — the oracle must be reproducible (no wall-clock, no OS randomness).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

fn page_content(seed: usize) -> Vec<u8> {
    (0..PAGE_SIZE)
        .map(|i| seed.wrapping_mul(31).wrapping_add(i) as u8)
        .collect()
}

/// Seed `N_SEED` distinct pages straight into a remote tier (bypassing the manifest layer — `get_batch`
/// operates on page ids alone). Returns their ids and contents, index-aligned.
async fn seed(remote: &RemoteTier, hasher: &PageHasher) -> (Vec<PageId>, Vec<Vec<u8>>) {
    let mut ids = Vec::with_capacity(N_SEED);
    let mut contents = Vec::with_capacity(N_SEED);
    for s in 0..N_SEED {
        let bytes = page_content(s);
        let page = Page::new(hasher, bytes.clone(), PAGE_SIZE).expect("valid page");
        remote
            .put(&remote.page_key(page.id()), bytes.clone())
            .await
            .expect("seed put");
        ids.push(page.id());
        contents.push(bytes);
    }
    (ids, contents)
}

/// A fresh, COLD tiered cache over the shared remote — nothing local, so every read faults.
fn cold_tier(remote: &RemoteTier, hasher: &PageHasher) -> Arc<TieredCas> {
    let local: Arc<dyn Cas> = Arc::new(MemCas::new(hasher.clone()));
    TieredCas::new(local, remote.clone(), hasher.clone()).expect("build tier")
}

#[tokio::test(flavor = "multi_thread")]
async fn get_batch_is_byte_identical_to_n_serial_gets_for_any_set() {
    let hasher = PageHasher::Unkeyed;
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "oracle");
    let (ids, contents) = seed(&remote, &hasher).await;

    let mut rng = Rng(0x0B0BCA75);
    for case in 0..400u64 {
        // A random set: length 0..=2N, each element a random seeded page — so duplicates and repeats
        // happen constantly, which is exactly where a dedupe-and-reassemble bug hides.
        let len = rng.below(2 * N_SEED + 1);
        let set_idx: Vec<usize> = (0..len).map(|_| rng.below(N_SEED)).collect();
        let set: Vec<PageId> = set_idx.iter().map(|&i| ids[i]).collect();

        // REFERENCE 1: N serial gets, on a fresh cold tier.
        let serial_tier = cold_tier(&remote, &hasher);
        let serial: Vec<Page> = set
            .iter()
            .map(|&id| serial_tier.get(id).expect("serial get"))
            .collect();

        // THE THING UNDER TEST: get_batch on an independent fresh cold tier.
        let batch_tier = cold_tier(&remote, &hasher);
        let batch = batch_tier.get_batch(&set).expect("get_batch");

        assert_eq!(batch.len(), set.len(), "case {case}: wrong page count");
        for (j, &idx) in set_idx.iter().enumerate() {
            // REFERENCE 2: the ground-truth seed bytes.
            assert_eq!(
                batch[j].as_bytes(),
                contents[idx].as_slice(),
                "case {case} pos {j}: batch bytes != seed truth"
            );
            assert_eq!(
                batch[j].as_bytes(),
                serial[j].as_bytes(),
                "case {case} pos {j}: batch != N serial gets"
            );
            assert_eq!(
                batch[j].id(),
                serial[j].id(),
                "case {case} pos {j}: id mismatch"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn get_batch_mixes_local_hits_and_remote_fetches() {
    let hasher = PageHasher::Unkeyed;
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "oracle");
    let (ids, contents) = seed(&remote, &hasher).await;

    let tier = cold_tier(&remote, &hasher);
    // Warm a couple of pages through the single-page path first, so the batch below is a genuine mix.
    tier.get(ids[3]).expect("warm 3");
    tier.get(ids[7]).expect("warm 7");

    // Warm (3, 7), cold (0, 10), and duplicates of each.
    let idx = [7usize, 0, 3, 7, 10, 3, 0];
    let set: Vec<PageId> = idx.iter().map(|&i| ids[i]).collect();
    let got = tier.get_batch(&set).expect("get_batch");
    assert_eq!(got.len(), set.len());
    for (j, &i) in idx.iter().enumerate() {
        assert_eq!(got[j].as_bytes(), contents[i].as_slice(), "pos {j}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_object_fails_the_whole_batch() {
    let hasher = PageHasher::Unkeyed;
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "oracle");
    let (ids, _) = seed(&remote, &hasher).await;

    // An id that was never seeded — a constant-fill page, which `page_content` (whose bytes increment
    // with position) can never produce, so it cannot collide with any seeded page. Neither tier has it.
    let ghost = Page::new(&hasher, vec![0xAB; PAGE_SIZE], PAGE_SIZE)
        .expect("valid page")
        .id();
    let tier = cold_tier(&remote, &hasher);
    let set = vec![ids[0], ids[1], ghost, ids[2]];
    match tier.get_batch(&set) {
        Err(PagerError::MissingPage(m)) => assert_eq!(m, ghost),
        other => panic!("a missing object must fail the whole batch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_object_fails_the_whole_batch_and_never_a_partial_set() {
    let hasher = PageHasher::Unkeyed;
    let backend = Arc::new(InMemory::new());
    let remote = RemoteTier::new(backend.clone(), "oracle");
    let (ids, _) = seed(&remote, &hasher).await;

    // Overwrite ids[1]'s object with bytes whose hash does not match its key — a corrupted download.
    // Straight through the raw backend, because RemoteTier::put is idempotent-skip (it will not
    // overwrite an existing content-addressed object — which is correct, but not what we need here).
    backend
        .put(&remote.page_key(ids[1]), b"tampered".to_vec().into())
        .await
        .expect("tamper");
    let tier = cold_tier(&remote, &hasher);
    let set = vec![ids[0], ids[1], ids[2]];
    match tier.get_batch(&set) {
        Err(PagerError::CorruptPage { expected, .. }) => assert_eq!(expected, ids[1]),
        other => panic!("a corrupt object must fail the whole batch, got {other:?}"),
    }
    // And it did not return a partial set as success — the whole call is Err, above.
}
