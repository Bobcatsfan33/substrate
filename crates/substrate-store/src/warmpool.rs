//! **A warm keep-alive connection pool** — the transport half of the ~1 RTT wake.
//!
//! # Why this exists
//!
//! Hydration (`TieredStore::hydrate`) collapses a hot re-wake to *one concurrent round-trip* — but only
//! if the object-store connections it uses are already open. S3's REST API is HTTP/1.1, so a batch of
//! `N` concurrent GETs needs `N` connections, and a *cold* one pays a fresh TLS handshake: one extra
//! round-trip, measured (AT-047) as the difference between a hot re-wake at **~1 RTT** on a warm pool and
//! **~2.3 RTT** on a cold one. A continuously-loaded server keeps its pool warm as a side effect; a
//! server that wakes a tenant after an idle gap does not, and reqwest's pool has no *minimum* — it only
//! caps the maximum. This closes that gap: it holds `min_idle` connections open so a wake, however
//! bursty, finds a warm connection per fetch.
//!
//! # What it is, and what it deliberately is not
//!
//! A background task that, every `interval`, issues `min_idle` concurrent **HEAD**s against a probe key
//! and awaits them all. The HEAD round-trip is what keeps a connection alive; a 404 (the probe key does
//! not exist) is expected and ignored — we want the connection, not the object. Awaiting the whole batch
//! before the next cycle bounds in-flight requests to `min_idle`, so the pool never grows without limit
//! (no file-descriptor leak), and a connection the server reaped is simply re-opened on the next cycle
//! (self-healing). It shares nothing with the wake path but the `ObjectStore` handle — no lock a wake
//! could block on.
//!
//! **Correctness is free here:** connections are transport, they never serve bytes, so a warm, cold,
//! dead, or reaped connection can only change *latency*, never *what a read returns*. The risk this
//! module is built and verified against is therefore purely liveness and resource: never block a wake,
//! never leak descriptors, always replace a dead connection.

use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::task::JoinHandle;

/// How to keep the pool warm.
#[derive(Clone, Debug)]
pub struct WarmPoolConfig {
    /// How many idle keep-alive connections to hold warm. The sensible default is the **hydrate
    /// concurrency width** ([`BATCH_FETCH_WIDTH`](crate::tier)), so a hot re-wake's concurrent batch
    /// finds one warm connection per fetch. Beyond this many concurrent fetches a wake still pays
    /// handshakes — which is why the guarantee is scoped "~1 RTT **up to `min_idle` connections**".
    pub min_idle: usize,
    /// How often to refresh. Must be shorter than the object store's server-side idle timeout (~20 s for
    /// S3), or connections are reaped between refreshes and the pool is cold when a wake arrives.
    pub interval: Duration,
}

impl Default for WarmPoolConfig {
    fn default() -> Self {
        // 16 = the batch-fetch width; 10 s comfortably under S3's ~20 s idle reap.
        WarmPoolConfig {
            min_idle: 16,
            interval: Duration::from_secs(10),
        }
    }
}

/// A running warm-pool maintainer. Keeps `min_idle` connections warm until dropped.
///
/// Dropping it aborts the background task — the keepalive stops and the connections idle out naturally.
/// It owns nothing the rest of the engine needs, so dropping it never affects correctness or a
/// concurrent wake.
pub struct WarmPool {
    handle: JoinHandle<()>,
}

impl WarmPool {
    /// Start keeping `config.min_idle` connections to `backend` warm, probing under `pool_prefix`.
    ///
    /// Must be called from within a tokio runtime (it spawns a task). Typically called once, at server
    /// startup, on the long-lived object-store client every wake shares.
    pub fn spawn(backend: Arc<dyn ObjectStore>, pool_prefix: &str, config: WarmPoolConfig) -> Self {
        // A benign, per-pool probe key. It need not exist — a HEAD that 404s still opens/refreshes the
        // connection, which is all we want.
        let probe = ObjPath::from(format!("{pool_prefix}/.substrate-keepalive-probe"));
        let min_idle = config.min_idle.max(1);
        let interval = config.interval;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                warm_once(&backend, &probe, min_idle).await;
            }
        });
        WarmPool { handle }
    }

    /// Refresh the pool once, synchronously (awaiting the batch). Exposed for tests and for a caller that
    /// wants to warm the pool immediately before a known-imminent burst rather than wait a cycle.
    pub async fn warm_now(backend: &Arc<dyn ObjectStore>, pool_prefix: &str, min_idle: usize) {
        let probe = ObjPath::from(format!("{pool_prefix}/.substrate-keepalive-probe"));
        warm_once(backend, &probe, min_idle.max(1)).await;
    }
}

impl Drop for WarmPool {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Issue `min_idle` concurrent HEADs and await them all — opening/refreshing that many connections, and
/// never more (the await bounds in-flight requests, so no descriptor leak). Errors (a 404, or a
/// connection that died) are ignored: the point is the connection, and a dead one is re-opened here.
async fn warm_once(backend: &Arc<dyn ObjectStore>, probe: &ObjPath, min_idle: usize) {
    let pending = (0..min_idle).map(|_| {
        let backend = Arc::clone(backend);
        let probe = probe.clone();
        async move {
            let _ = backend.head(&probe).await;
        }
    });
    futures::future::join_all(pending).await;
}
