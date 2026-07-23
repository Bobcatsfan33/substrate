//! **The concurrency gate for the warm set** — the freeze-sensitive-core equivalent of the every-byte
//! crash sweep. The *data* correctness of piece 2 is free (content-addressed, all-or-nothing, a read
//! only ever uses objects by content id), so the risk lives entirely in **concurrency and liveness**,
//! and that is where this points.
//!
//! It drives many overlapping faults at the tier at once — the exact shape of the speculative prefetch
//! racing the caller's reads — over a store that **counts every GET and delays it** to widen the race
//! window, and proves three things:
//!
//! - **No double-fetch.** Each object is fetched from the remote **at most once**, even when a crowd of
//!   threads faults it simultaneously. This is the whole point of the fault gate; without it, N racers
//!   would issue N GETs of the same object — exactly the redundancy that made the sibling engine's
//!   fan-out *lose* to serial.
//! - **No deadlock / no starvation.** Every fault completes (the test simply returns; a hang is a
//!   failure), across the `get`/`get_batch` mix that the read path and the prefetch use.
//! - **Correctness under the race.** Every faulted object is byte-identical to what was seeded.

use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use substrate_pager::{Cas, MemCas, Page, PageHasher, PageId};
use substrate_store::{RemoteTier, TieredCas};

/// An object store that counts every GET per key and delays it, so concurrent faults of the same object
/// genuinely overlap in the fetch window — the only way a double-fetch could happen if the gate failed.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counts: Mutex<HashMap<String, u64>>,
    delaying: AtomicBool,
    total_gets: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            counts: Mutex::new(HashMap::new()),
            delaying: AtomicBool::new(true),
            total_gets: AtomicU64::new(0),
        })
    }
    fn reset(&self) {
        self.counts.lock().unwrap().clear();
        self.total_gets.store(0, Ordering::SeqCst);
    }
    fn max_gets_for_any_key(&self) -> u64 {
        self.counts
            .lock()
            .unwrap()
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }
    fn total(&self) -> u64 {
        self.total_gets.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        {
            let mut c = self.counts.lock().unwrap();
            *c.entry(location.to_string()).or_insert(0) += 1;
        }
        self.total_gets.fetch_add(1, Ordering::SeqCst);
        if self.delaying.load(Ordering::SeqCst) {
            // Widen the race window so two racers reach the fetch at the same time. If the gate did not
            // serialize them, both would be counted here — which is exactly what the assertion catches.
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        self.inner.get_opts(location, options).await
    }
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn delete(&self, location: &ObjPath) -> OsResult<()> {
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&ObjPath>) -> BoxStreamAlias<'_> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjPath, to: &ObjPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

type BoxStreamAlias<'a> = futures::stream::BoxStream<'a, OsResult<ObjectMeta>>;

const N_SEED: usize = 32;
const PAGE_SIZE: usize = 4096;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn content(seed: usize) -> Vec<u8> {
    (0..PAGE_SIZE).map(|i| (seed * 31 + i) as u8).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_faults_never_double_fetch_and_never_hang() {
    let hasher = PageHasher::Unkeyed;
    let backend = InMemory::new();
    let counting = CountingStore::new(Arc::new(backend));
    let remote = RemoteTier::new(Arc::clone(&counting) as Arc<dyn ObjectStore>, "concurrency");

    // Seed N distinct pages straight into the remote (PUTs, not counted).
    let mut ids: Vec<PageId> = Vec::new();
    for s in 0..N_SEED {
        let bytes = content(s);
        let page = Page::new(&hasher, bytes.clone(), PAGE_SIZE).unwrap();
        remote
            .put(&remote.page_key(page.id()), bytes)
            .await
            .unwrap();
        ids.push(page.id());
    }
    let ids = Arc::new(ids);

    // ROUNDS of a crowd faulting overlapping subsets of a COLD tier. A fresh tier per round = a cold
    // cache, so every round genuinely fetches from the remote and the gate is exercised, not bypassed.
    for round in 0..40u64 {
        let cas = TieredCas::new(
            Arc::new(MemCas::new(hasher.clone())),
            remote.clone(),
            hasher.clone(),
        )
        .unwrap();
        counting.reset();

        // 8 racers, each a dedicated OS thread (so `get`/`get_batch`'s blocking wait drives GETs on the
        // runtime workers and the racers genuinely overlap). Each faults a random overlapping subset —
        // half via single-page `get`, half via `get_batch` — so both fault paths race each other on the
        // same objects.
        let mut handles = Vec::new();
        for w in 0..8u64 {
            let cas = Arc::clone(&cas);
            let ids = Arc::clone(&ids);
            handles.push(std::thread::spawn(move || {
                let mut rng = Rng(0xC0FFEE ^ (round << 8) ^ w);
                // A random subset, heavily overlapping across workers (drawn from the same small pool).
                let k = 4 + rng.below(N_SEED - 4);
                let subset: Vec<PageId> = (0..k).map(|_| ids[rng.below(N_SEED)]).collect();
                if w % 2 == 0 {
                    // Single-page reads.
                    for &id in &subset {
                        let page = cas.get(id).expect("get");
                        assert_eq!(page.id(), id, "get returned the wrong page");
                    }
                } else {
                    // Batched reads.
                    let pages = cas.get_batch(&subset).expect("get_batch");
                    for (p, &id) in pages.iter().zip(&subset) {
                        assert_eq!(p.id(), id, "get_batch returned the wrong page");
                    }
                }
            }));
        }
        for h in handles {
            h.join()
                .expect("a racer thread panicked (deadlock, double-panic, or assertion)");
        }

        // The gate's promise: no object was fetched from the remote more than once this round, even
        // though a crowd faulted overlapping sets of them simultaneously through both paths.
        let max = counting.max_gets_for_any_key();
        assert!(
            max <= 1,
            "round {round}: an object was fetched {max} times — the fault gate let a double-fetch through"
        );

        // The same promise stated globally: total GETs this round cannot exceed the number of
        // distinct objects, since no object was fetched twice. A gate failure that double-fetched a
        // few objects while others stayed single would still trip `max`, but this catches the case
        // where the redundancy is spread thinly across many objects.
        assert!(
            counting.total() <= N_SEED as u64,
            "round {round}: {} total GETs across at most {N_SEED} distinct objects — redundant fetching",
            counting.total()
        );
    }
}
