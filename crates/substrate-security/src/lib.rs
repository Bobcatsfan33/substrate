//! # substrate-security
//!
//! Page encryption at rest, and offline licensing.
//!
//! ## Encryption sits *below* content addressing
//!
//! ```text
//!   plaintext ──► BLAKE3 ──► PageId              identity is the hash of the PLAINTEXT
//!       │
//!       └──► XChaCha20-Poly1305 ──► the disk     storage holds only CIPHERTEXT
//! ```
//!
//! This is the only arrangement in which content addressing survives. Hash the *ciphertext* and two
//! identical pages get two different ids, at which point deduplication dies, forks stop being free,
//! and every property in docs/02 §3.1 goes with them.
//!
//! **The cost, stated plainly:** an adversary who can see `PageId`s and who can *guess* a page's
//! contents can confirm the guess by hashing it. Within a dedup scope that leaks membership. For CUI
//! and classified pools that is not acceptable, and the `keyed-hash` build closes it — see [`crypt`]
//! and docs/02 §9.1. That build is not an option in those deployments, it is the deployment.
//!
//! ## Licensing never stops a read
//!
//! > `Ok` | `Warning(days)` | `Degraded`. **Never a hard stop.** Not on expiry, not on a corrupt
//! > licence, not on a missing one, not on a clock that has jumped to 2031.
//!
//! Our customers run in facilities that cannot phone home for a renewal — that is what "offline
//! licence" *means*. A licence that could stop a database from serving reads in an air-gapped
//! facility would be a weapon that fires at our own customer, during an incident, with no way to
//! disarm it. `Degraded` disables fleet-plane administration. It does not disable the database.
//!
//! See [`license`] for the clock-jump handling: internal decisions use monotonic time, licence checks
//! use a **high-water mark that never moves backward**, so setting the clock back does not un-expire
//! anything, and a legitimate ±30-day enclave drift is tolerated quietly.

#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(rust_2018_idioms)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod error;

pub mod crypt;
pub mod keys;
pub mod license;

pub use error::{Result, SecurityError};
pub use keys::{DataKey, FileKeyProvider, KeyProvider, PoolMasterKey};
pub use license::{
    Enforcement, HighWaterClock, License, LicenseClaims, Status, CLOCK_DRIFT_TOLERANCE_DAYS,
};

use std::sync::Arc;
use substrate_pager::{Cas, Page, PageHasher, PageId, PagerError, Result as PagerResult};

/// A CAS that encrypts on the way down and decrypts on the way up.
///
/// Wrap any [`Cas`] in this and the pages on disk — or in object storage — are ciphertext, while
/// everything above it in the engine continues to work with plaintext and content-addressed ids, and
/// never learns that encryption happened.
///
/// That is the whole point of routing durable state through one trait (CLAUDE.md rule 2): encryption
/// is implementable in exactly one place, and no other crate has to know.
///
/// ```
/// # use std::sync::Arc;
/// # use substrate_pager::{Cas, MemCas, Page, PageHasher, DEFAULT_PAGE_SIZE};
/// # use substrate_security::{EncryptedCas, PoolMasterKey};
/// # fn main() -> Result<(), substrate_security::SecurityError> {
/// let inner = Arc::new(MemCas::new(PageHasher::Unkeyed));
/// let key = PoolMasterKey::from_bytes([7; 32]).derive_data_key("acme-prod");
/// let cas = EncryptedCas::new(inner.clone(), key, PageHasher::Unkeyed);
///
/// let page = Page::new(&PageHasher::Unkeyed, b"secret rows".to_vec(), DEFAULT_PAGE_SIZE)?;
/// cas.put(&page)?;
///
/// // The engine sees plaintext...
/// assert_eq!(cas.get(page.id())?.as_bytes(), b"secret rows");
/// // ...and the disk holds ciphertext.
/// assert_ne!(inner.get_raw(page.id())?, b"secret rows");
/// # Ok(())
/// # }
/// ```
pub struct EncryptedCas {
    inner: Arc<dyn Cas>,
    key: DataKey,
    hasher: PageHasher,
}

impl EncryptedCas {
    /// Wrap a CAS with encryption, using one database's data key.
    pub fn new(inner: Arc<dyn Cas>, key: DataKey, hasher: PageHasher) -> Arc<Self> {
        Arc::new(EncryptedCas { inner, key, hasher })
    }

    /// Build one from a key provider, for a given pool and database.
    pub fn from_provider(
        inner: Arc<dyn Cas>,
        provider: &dyn KeyProvider,
        pool: &str,
        database: &str,
        hasher: PageHasher,
    ) -> Result<Arc<Self>> {
        let master = provider.master_key(pool)?;
        Ok(EncryptedCas::new(
            inner,
            master.derive_data_key(database),
            hasher,
        ))
    }
}

impl Cas for EncryptedCas {
    fn put(&self, page: &Page) -> PagerResult<()> {
        // The id is the hash of the PLAINTEXT — computed by the caller, before we ever see it. We
        // encrypt for storage and store the ciphertext under that id.
        let sealed =
            crypt::seal(&self.key, page.id(), page.as_bytes()).map_err(PagerError::backend)?;

        // The inner CAS would ordinarily verify that bytes hash to their id — and ciphertext does
        // not. So we hand it a page whose "content" is the ciphertext but whose *storage key* is the
        // plaintext id, using the raw put. `EncryptedCas::get` does the real verification: the AEAD
        // tag, and then the plaintext hash.
        self.inner.put_raw(page.id(), &sealed)
    }

    fn get(&self, id: PageId) -> PagerResult<Page> {
        let sealed = self.inner.get_raw(id)?;
        let plaintext = crypt::open(&self.key, id, &sealed).map_err(|e| match e {
            // Ciphertext that does not authenticate is corruption, and must be reported as
            // corruption — that is what makes the scrubber and the repair path work on an encrypted
            // store exactly as they do on a plaintext one.
            SecurityError::Decrypt { .. } | SecurityError::PlaintextMismatch { .. } => {
                PagerError::CorruptPage {
                    expected: id,
                    actual: id,
                    len: sealed.len(),
                }
            }
            other => PagerError::backend(other),
        })?;

        Page::new(&self.hasher, plaintext, usize::MAX)
    }

    fn put_raw(&self, id: PageId, bytes: &[u8]) -> PagerResult<()> {
        self.inner.put_raw(id, bytes)
    }

    fn get_raw(&self, id: PageId) -> PagerResult<Vec<u8>> {
        self.inner.get_raw(id)
    }

    fn contains(&self, id: PageId) -> PagerResult<bool> {
        self.inner.contains(id)
    }

    fn remove(&self, id: PageId) -> PagerResult<()> {
        self.inner.remove(id)
    }

    fn list(&self) -> PagerResult<Vec<PageId>> {
        self.inner.list()
    }
}

impl std::fmt::Debug for EncryptedCas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedCas").finish()
    }
}
