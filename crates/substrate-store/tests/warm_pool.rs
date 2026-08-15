//! **The warm-pool liveness/resource gate.**
//!
//! Correctness is free here — connections are transport, they never serve bytes, so a warm/cold/dead
//! connection can only change latency, never what a read returns. So the risk this points at is the
//! other three properties the warm pool must have, and this proves them over a probe store that counts
//! and delays every HEAD:
//!
//! - **Bounded (no descriptor leak).** It never holds more than `min_idle` connections in flight at
//!   once, however many refresh cycles run — because each cycle awaits its whole batch before the next.
//! - **Self-healing.** Every probe HEAD *fails* (the probe key does not exist, and a dead connection
//!   errors the same way); the maintainer must ignore that and keep refreshing, not die on the first
//!   error — so a reaped connection is re-opened next cycle.
//! - **Never blocks a wake.** A read runs to completion promptly while the pool is refreshing — the two
//!   share only the `ObjectStore` handle, no lock.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use substrate_store::{WarmPool, WarmPoolConfig};

/// An object store that counts and delays every HEAD, records peak concurrency, and can be told to make
/// every HEAD fail — to stand in for both a missing probe key and a dead connection.
#[derive(Debug)]
struct ProbeStore {
    inner: Arc<dyn ObjectStore>,
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    heads: AtomicUsize,
    fail: AtomicBool,
}
impl ProbeStore {
    fn new() -> Arc<Self> {
        Arc::new(ProbeStore {
            inner: Arc::new(InMemory::new()),
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            fail: AtomicBool::new(true),
        })
    }
}
impl std::fmt::Display for ProbeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ProbeStore {
    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        if options.head {
            let n = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_inflight.fetch_max(n, Ordering::SeqCst);
            self.heads.fetch_add(1, Ordering::SeqCst);
            // Widen the window so overlapping cycles would be caught by `max_inflight`.
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            // Always an error — a missing probe key, or a dead connection. The maintainer must not die on it.
            let _ = self.fail.load(Ordering::SeqCst);
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "keepalive probe".into(),
            });
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
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<ObjPath>>,
    ) -> BoxStream<'static, OsResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_pool_is_bounded_self_healing_and_never_blocks_a_read() {
    const MIN_IDLE: usize = 4;
    let store = ProbeStore::new();
    let backend: Arc<dyn ObjectStore> = store.clone();

    // Seed a real object so a concurrent read has something to fetch.
    backend
        .put(&ObjPath::from("p/hello"), PutPayload::from_static(b"hi"))
        .await
        .unwrap();

    let pool = WarmPool::spawn(
        Arc::clone(&backend),
        "p",
        WarmPoolConfig {
            min_idle: MIN_IDLE,
            interval: Duration::from_millis(20),
        },
    );

    // Let ~12 refresh cycles run.
    tokio::time::sleep(Duration::from_millis(260)).await;

    // 1. Self-healing: it kept issuing heartbeats across many cycles despite every HEAD erroring.
    let heads = store.heads.load(Ordering::SeqCst);
    assert!(
        heads >= MIN_IDLE * 5,
        "warm pool issued only {heads} probes — it stopped refreshing (did it die on an error?)"
    );

    // 2. Bounded: never more than min_idle connections in flight — overlapping cycles would leak fds.
    let peak = store.max_inflight.load(Ordering::SeqCst);
    assert!(
        peak <= MIN_IDLE,
        "warm pool held {peak} concurrent connections, over min_idle {MIN_IDLE} — a descriptor leak"
    );

    // 3. Never blocks a read: a GET runs to completion promptly while the pool is refreshing.
    let t = Instant::now();
    let got = backend.get(&ObjPath::from("p/hello")).await.unwrap();
    let bytes = got.bytes().await.unwrap();
    assert_eq!(&bytes[..], b"hi");
    assert!(
        t.elapsed() < Duration::from_millis(150),
        "a read took {:?} while the warm pool ran — it was blocked",
        t.elapsed()
    );

    drop(pool); // stops the keepalive; connections idle out on their own.
}
