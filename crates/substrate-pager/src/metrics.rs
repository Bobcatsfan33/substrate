//! Metrics hooks.
//!
//! # Why substrate does not depend on a metrics library
//!
//! Because you already have one, and it is not the one we would have picked.
//!
//! A storage engine that hard-depends on `prometheus`, or `metrics`, or `opentelemetry`, forces that
//! choice on every product built on it — and on every product built on *those*. So substrate defines
//! a trait, ships a no-op implementation, and lets the layer above wire it to whatever it already
//! runs. The whole surface is [`Metrics`], and the default costs nothing: [`NoMetrics`] compiles to
//! nothing at every call site.
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use substrate_pager::Metrics;
//!
//! #[derive(Default, Debug)]
//! struct MyMetrics {
//!     page_reads: AtomicU64,
//!     cache_misses: AtomicU64,
//! }
//!
//! impl Metrics for MyMetrics {
//!     fn page_read(&self, _bytes: usize, _hit: bool) {
//!         self.page_reads.fetch_add(1, Ordering::Relaxed);
//!     }
//! }
//! ```
//!
//! # What is worth measuring, and why these
//!
//! Every hook here answers a question an operator asks at 3am:
//!
//! - **cache hit rate** — "is this database awake, or is it thrashing against S3?"
//! - **WAL fsync latency** — the single number that bounds how many transactions per second the
//!   engine can commit. If it moves, everything moves.
//! - **wake latency** — the promise in docs/02 §7 is under 250 ms. This is how you know it is kept.
//! - **GC stats** — "why is my disk full?" has an answer that is not a guess.
//! - **corruption** — the one that should always be zero, and that you must be told about the
//!   instant it is not.

use crate::gc::GcStats;
use crate::page::PageId;
use std::time::Duration;

/// Where substrate reports what it is doing.
///
/// Every method has a no-op default, so an implementation only overrides what it cares about. Calls
/// are on hot paths: **do not block, do not allocate, do not take a contended lock.** An
/// implementation that makes a page read slow has turned an observability tool into an outage.
pub trait Metrics: Send + Sync + std::fmt::Debug {
    /// A page was read. `hit` is false if it had to be fetched from a slower tier.
    fn page_read(&self, bytes: usize, hit: bool) {
        let _ = (bytes, hit);
    }

    /// A page was written to durable storage.
    fn page_write(&self, bytes: usize) {
        let _ = bytes;
    }

    /// A transaction committed, and how long the durability step took.
    ///
    /// For `substrate-wal` this is the fsync — the number that bounds commit throughput.
    fn commit(&self, pages: usize, durability_latency: Duration) {
        let _ = (pages, durability_latency);
    }

    /// A database woke from object storage, and how long it took to serve its first read.
    ///
    /// The target is p99 < 250 ms (docs/02 §7). This is the hook that tells you whether it holds.
    fn wake(&self, latency: Duration) {
        let _ = latency;
    }

    /// A garbage collection finished.
    fn gc(&self, stats: GcStats, elapsed: Duration) {
        let _ = (stats, elapsed);
    }

    /// **A page failed its integrity check.**
    ///
    /// This should be identically zero, forever. If it is not, an operator needs to know within
    /// seconds — bit rot spreads, and a corrupt page that is quietly re-read is a corrupt page that
    /// eventually gets committed into a manifest somebody trusts.
    fn corruption_detected(&self, page: PageId) {
        let _ = page;
    }

    /// A corrupt page was repaired from a healthy replica (usually object storage).
    fn corruption_repaired(&self, page: PageId) {
        let _ = page;
    }
}

/// The default: measure nothing, cost nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoMetrics;

impl Metrics for NoMetrics {}
