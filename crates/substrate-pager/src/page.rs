//! Pages and page identity.
//!
//! A page is an immutable block of bytes, and its identity *is* its content:
//!
//! ```text
//! PageId = BLAKE3(page_bytes)
//! ```
//!
//! Content addressing is the load-bearing decision in this architecture (docs/02 §3.1).
//! Forking is free because a fork shares every page by construction. Deduplication is
//! automatic. Caching is safe without invalidation, because different content is a different
//! id. Integrity is inherent: re-hash on read and corruption anywhere in the path is detected.
//!
//! # The tradeoff, stated plainly
//!
//! We hash the **plaintext** (and encrypt below content addressing — see `substrate-security`).
//! An adversary who can observe `PageId`s and who *guesses* a page's plaintext can confirm the
//! guess by hashing it. Within a dedup scope this leaks membership. For CUI and classified
//! pools this is not acceptable, and the `keyed-hash` feature makes `PageId` a keyed hash over
//! a per-pool key, which closes it. See `docs/02` §9.1 and `docs/threat-model.md`.

use crate::error::{PagerError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The default page size: 64 KiB.
///
/// Large enough that per-page overhead (a 32-byte id in a manifest) is noise, small enough
/// that a copy-on-write of one row does not rewrite a megabyte.
pub const DEFAULT_PAGE_SIZE: usize = 64 * 1024;

/// The smallest page size a store may be created with.
pub const MIN_PAGE_SIZE: usize = 4 * 1024;

/// The largest page size a store may be created with.
pub const MAX_PAGE_SIZE: usize = 16 * 1024 * 1024;

/// A set of page ids. What GC and the scrubber both traffic in.
pub type PageIdSet = std::collections::HashSet<PageId>;

/// A logical page number: the address a database uses, independent of where the bytes live.
///
/// The indirection between a `LogicalPageNo` and a [`PageId`] is what makes fork, snapshot,
/// and rewind O(1) — they rearrange the mapping, never the bytes.
pub type LogicalPageNo = u64;

/// The content hash of a page. 32 bytes of BLAKE3.
///
/// Two pages with the same `PageId` have identical bytes. This is relied upon everywhere:
/// it is why a fork copies nothing, and why a cached page can never be stale.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageId([u8; 32]);

impl PageId {
    /// Compute the id of these bytes.
    ///
    /// Identical content always yields the same id; different content never does.
    ///
    /// ```
    /// # use substrate_pager::PageId;
    /// let a = PageId::of(b"hello");
    /// let b = PageId::of(b"hello");
    /// let c = PageId::of(b"world");
    /// assert_eq!(a, b);      // identical content is the same page
    /// assert_ne!(a, c);
    /// ```
    pub fn of(bytes: &[u8]) -> Self {
        PageId(*blake3::hash(bytes).as_bytes())
    }

    /// Compute a page id keyed by a per-pool secret.
    ///
    /// Identical plaintext in two pools yields *different* ids, so an adversary who guesses a
    /// page's contents cannot confirm the guess without the key, and dedup cannot span pools.
    /// This is what the `keyed-hash` feature makes mandatory for CUI pools (docs/02 §9.1).
    ///
    /// ```
    /// # use substrate_pager::PageId;
    /// let pool_a = [1u8; 32];
    /// let pool_b = [2u8; 32];
    /// // The same plaintext is a different page in a different pool:
    /// assert_ne!(PageId::of_keyed(&pool_a, b"secret"), PageId::of_keyed(&pool_b, b"secret"));
    /// // ...and is not confirmable by hashing the guess without the key:
    /// assert_ne!(PageId::of_keyed(&pool_a, b"secret"), PageId::of(b"secret"));
    /// ```
    pub fn of_keyed(pool_key: &[u8; 32], bytes: &[u8]) -> Self {
        PageId(*blake3::keyed_hash(pool_key, bytes).as_bytes())
    }

    /// The raw 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Construct from raw bytes. Does not verify that any page hashes to this.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        PageId(bytes)
    }

    /// Lowercase hex, 64 characters.
    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for byte in self.0 {
            use fmt::Write as _;
            // Writing to a String is infallible; the Result is discarded deliberately
            // rather than unwrapped (CLAUDE.md rule 6).
            let _ = write!(s, "{byte:02x}");
        }
        s
    }

    /// Parse from 64 hex characters.
    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 64 {
            return Err(PagerError::MalformedId(s.to_string()));
        }
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            let byte = s
                .get(i * 2..i * 2 + 2)
                .ok_or_else(|| PagerError::MalformedId(s.to_string()))?;
            *slot =
                u8::from_str_radix(byte, 16).map_err(|_| PagerError::MalformedId(s.to_string()))?;
        }
        Ok(PageId(out))
    }

    /// The two-level shard prefix this page is stored under: `("aa", "bb")`.
    ///
    /// Sharding keeps any one directory from holding millions of entries, which some
    /// filesystems handle poorly and every operator handles poorly.
    pub(crate) fn shard(&self) -> (String, String) {
        let hex = self.to_hex();
        (hex[0..2].to_string(), hex[2..4].to_string())
    }
}

impl fmt::Display for PageId {
    /// Abbreviated to 12 hex characters — enough to be unambiguous in a log, short enough to read.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.to_hex()[..12])
    }
}

impl fmt::Debug for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", &self.to_hex()[..12])
    }
}

/// How a store computes page identity.
///
/// Fixed at store creation and recorded in the store header, because changing it would change
/// every page's id — which is to say, it would be a different store.
///
/// With the `keyed-hash` feature compiled in, [`PageHasher::Unkeyed`] is rejected at store
/// creation: a CUI build cannot be configured back into plaintext-confirmable identity
/// (docs/02 §9.1).
#[derive(Clone, Default, PartialEq, Eq)]
pub enum PageHasher {
    /// `PageId = BLAKE3(plaintext)`. Dedup works across the pool; membership is confirmable
    /// by an adversary who can guess a page's contents. Fine for public or single-tenant data.
    #[default]
    Unkeyed,
    /// `PageId = BLAKE3_keyed(pool_key, plaintext)`. Membership is not confirmable without the
    /// key, and dedup is confined to the pool. Mandatory for CUI and classified pools.
    Keyed([u8; 32]),
}

impl PageHasher {
    /// Compute the id of these bytes under this store's identity scheme.
    pub fn hash(&self, bytes: &[u8]) -> PageId {
        match self {
            PageHasher::Unkeyed => PageId::of(bytes),
            PageHasher::Keyed(key) => PageId::of_keyed(key, bytes),
        }
    }
}

impl fmt::Debug for PageHasher {
    /// Never prints the key. A pool key in a log file is a pool key in a log aggregator.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PageHasher::Unkeyed => write!(f, "PageHasher::Unkeyed"),
            PageHasher::Keyed(_) => write!(f, "PageHasher::Keyed(<redacted>)"),
        }
    }
}

/// An immutable block of page bytes, with its id.
///
/// There is no way to mutate a `Page`. To change data you write a *new* page; the old one
/// remains valid for every manifest that still references it. That is what makes snapshots
/// free and history durable.
#[derive(Clone, PartialEq, Eq)]
pub struct Page {
    id: PageId,
    bytes: Vec<u8>,
}

impl Page {
    /// Build a page from bytes, computing its id.
    ///
    /// Rejects anything larger than the store's page size — a page that does not fit is a
    /// caller bug we surface immediately rather than a truncation we discover later.
    ///
    /// ```
    /// # use substrate_pager::{Page, PageHasher, DEFAULT_PAGE_SIZE};
    /// let page = Page::new(&PageHasher::Unkeyed, b"some rows".to_vec(), DEFAULT_PAGE_SIZE)?;
    /// assert_eq!(page.as_bytes(), b"some rows");
    /// # Ok::<(), substrate_pager::PagerError>(())
    /// ```
    pub fn new(hasher: &PageHasher, bytes: Vec<u8>, page_size: usize) -> Result<Self> {
        if bytes.len() > page_size {
            return Err(PagerError::PageTooLarge {
                actual: bytes.len(),
                page_size,
            });
        }
        Ok(Page {
            id: hasher.hash(&bytes),
            bytes,
        })
    }

    /// This page's content hash.
    pub fn id(&self) -> PageId {
        self.id
    }

    /// The page's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the page, yielding its bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// How many bytes this page holds.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// True if the page holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Re-hash the bytes and confirm they still match the id they are stored under.
    ///
    /// Called on every read from the CAS. This is the whole integrity story: a page that
    /// does not hash to its own id has been corrupted, and we would rather fail loudly than
    /// serve it.
    pub(crate) fn verify(hasher: &PageHasher, id: PageId, bytes: Vec<u8>) -> Result<Self> {
        let actual = hasher.hash(&bytes);
        if actual != id {
            return Err(PagerError::CorruptPage {
                expected: id,
                actual,
                len: bytes.len(),
            });
        }
        Ok(Page { id, bytes })
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Validate a page size at store creation.
pub(crate) fn validate_page_size(size: usize) -> Result<()> {
    if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size) || !size.is_power_of_two() {
        return Err(PagerError::InvalidPageSize(size));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_content_is_the_same_page() {
        assert_eq!(PageId::of(b"abc"), PageId::of(b"abc"));
        assert_ne!(PageId::of(b"abc"), PageId::of(b"abd"));
    }

    #[test]
    fn hex_round_trips() -> Result<()> {
        let id = PageId::of(b"round trip");
        assert_eq!(PageId::from_hex(&id.to_hex())?, id);
        Ok(())
    }

    #[test]
    fn malformed_hex_is_rejected_not_panicked() {
        assert!(PageId::from_hex("nope").is_err());
        assert!(PageId::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn oversized_pages_are_rejected() {
        let err = Page::new(
            &PageHasher::Unkeyed,
            vec![0u8; MIN_PAGE_SIZE + 1],
            MIN_PAGE_SIZE,
        );
        assert!(matches!(err, Err(PagerError::PageTooLarge { .. })));
    }

    #[test]
    fn verify_catches_corruption() {
        let h = PageHasher::Unkeyed;
        let page = Page::new(&h, b"honest bytes".to_vec(), DEFAULT_PAGE_SIZE).expect("valid page");
        // Simulate bit rot: the same id, different bytes.
        let err = Page::verify(&h, page.id(), b"tampered!!!!".to_vec());
        assert!(matches!(err, Err(PagerError::CorruptPage { .. })));
    }

    #[test]
    fn keyed_hashing_defeats_membership_confirmation() {
        // The whole point of keyed-hash mode (docs/02 §9.1): an adversary who guesses the
        // plaintext cannot confirm it, and the same plaintext in another pool is a different id.
        let pool_a = PageHasher::Keyed([7u8; 32]);
        let pool_b = PageHasher::Keyed([9u8; 32]);
        let guess = b"the salary of employee 4471 is 220000";

        assert_ne!(pool_a.hash(guess), PageId::of(guess));
        assert_ne!(pool_a.hash(guess), pool_b.hash(guess));
        // ...but it is still deterministic within its own pool, or nothing works.
        assert_eq!(pool_a.hash(guess), pool_a.hash(guess));
    }

    #[test]
    fn pool_keys_are_never_printed() {
        let redacted = format!("{:?}", PageHasher::Keyed([0xAB; 32]));
        assert!(!redacted.contains("171") && !redacted.contains("ab"));
        assert!(redacted.contains("redacted"));
    }

    #[test]
    fn page_sizes_are_validated() {
        assert!(validate_page_size(DEFAULT_PAGE_SIZE).is_ok());
        assert!(validate_page_size(3).is_err()); // too small
        assert!(validate_page_size(96 * 1024).is_err()); // not a power of two
        assert!(validate_page_size(usize::MAX).is_err()); // too large
    }
}
