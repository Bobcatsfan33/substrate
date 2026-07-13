//! Time.
//!
//! Two kinds, and confusing them is how storage engines get broken by an operator changing the
//! system clock (docs/02 §9.2):
//!
//! - **Monotonic** — every *internal* decision: timeouts, leases, backoff, idle-sleep. Cannot go
//!   backwards, so nothing internal can be broken by moving the wall clock.
//! - **Wall clock** — recorded in manifests for human-facing history and point-in-time restore,
//!   and used for license checks (against a high-water mark that never moves backward). Never
//!   used to decide anything internal.
//!
//! The trait exists so tests can be deterministic. A manifest's `created_at_ms` participates in
//! its content hash, so a test that needs byte-identical manifests across runs needs a clock that
//! does not move — and deterministic replay is a property we are required to prove, not hope for.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of wall-clock time, in milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current wall-clock time.
    fn now_ms(&self) -> u64;
}

/// The real clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        // A clock set before 1970 is not a reason to kill a database. Clamp to the epoch and
        // carry on: the only consumers are human-facing history and license checks, and both
        // are better served by an obviously-wrong timestamp than by a panic.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A clock the test drives by hand.
///
/// ```
/// # use substrate_pager::{Clock, ManualClock};
/// let clock = ManualClock::new(1_700_000_000_000);
/// assert_eq!(clock.now_ms(), 1_700_000_000_000);
/// clock.advance_ms(5_000);
/// assert_eq!(clock.now_ms(), 1_700_000_005_000);
/// ```
#[derive(Debug)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// A clock frozen at this instant.
    pub fn new(now_ms: u64) -> Self {
        ManualClock {
            now: AtomicU64::new(now_ms),
        }
    }

    /// Move time forward.
    pub fn advance_ms(&self, delta: u64) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }

    /// Move time to an arbitrary point — including backwards, which is the whole point: the
    /// clock-jump scenarios in docs/02 §9.2 are not hypothetical, they are what happens.
    pub fn set_ms(&self, now_ms: u64) {
        self.now.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}
