//! Error types for `substrate-pager`.
//!
//! One `thiserror` enum for the whole crate (CLAUDE.md rule 6). Library code never panics:
//! a panic in a storage engine is an unplanned process death, and an unplanned process death
//! during a commit is precisely the disaster crash recovery exists to survive.

use crate::page::PageId;
use std::path::PathBuf;

/// The result type returned by every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, PagerError>;

/// Everything that can go wrong in the pager.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PagerError {
    /// A page was requested that no live manifest maps.
    #[error("logical page {page_no} is not present in manifest {manifest}")]
    PageNotFound {
        /// The logical page number that was requested.
        page_no: u64,
        /// The manifest it was requested from.
        manifest: String,
    },

    /// The content-addressed store has no object with this id.
    ///
    /// In a healthy store this means the page was garbage collected while still referenced —
    /// i.e. a GC bug — or the CAS directory was tampered with.
    #[error("page {0} is missing from the content-addressed store")]
    MissingPage(PageId),

    /// A manifest was requested that does not exist.
    #[error("manifest {0} does not exist")]
    MissingManifest(String),

    /// Bytes read back from the CAS did not hash to the id they were stored under.
    ///
    /// This is silent corruption — bit rot, a failing disk, or tampering. It is always
    /// reported, never repaired in place, and never ignored.
    #[error("integrity failure: page {expected} hashed to {actual} on read ({} bytes)", .len)]
    CorruptPage {
        /// The id the page was stored under.
        expected: PageId,
        /// What the bytes actually hash to now.
        actual: PageId,
        /// How many bytes were read.
        len: usize,
    },

    /// A page larger than the store's configured page size was offered.
    #[error("page is {actual} bytes, which exceeds this store's {page_size}-byte page size")]
    PageTooLarge {
        /// Size of the offered page.
        actual: usize,
        /// The store's configured page size.
        page_size: usize,
    },

    /// The page size given at store creation is not usable.
    #[error("page size {0} is invalid: must be a power of two between 4 KiB and 16 MiB")]
    InvalidPageSize(usize),

    /// A store was created without keyed page identity in a build that requires it.
    ///
    /// The `keyed-hash` feature exists so a CUI or classified build **cannot be configured back**
    /// into plaintext-confirmable page identity (docs/02 §9.1). This is that guard firing.
    #[error(
        "this build requires keyed page identity (feature `keyed-hash`): \
         construct StoreConfig with PageHasher::Keyed(pool_key), not PageHasher::Unkeyed"
    )]
    UnkeyedStoreInKeyedBuild,

    /// An existing store was opened with a different page size than it was created with.
    #[error("store was created with a {existing}-byte page size, but was opened with {requested}")]
    PageSizeMismatch {
        /// The page size recorded in the store.
        existing: usize,
        /// The page size the caller asked for.
        requested: usize,
    },

    /// Serialization or deserialization of a manifest failed.
    #[error("failed to {op} manifest: {source}")]
    Codec {
        /// `"encode"` or `"decode"`.
        op: &'static str,
        /// The underlying bincode error.
        #[source]
        source: Box<bincode::ErrorKind>,
    },

    /// An id was parsed from a string that is not a valid 32-byte hex hash.
    #[error("{0:?} is not a valid content hash (expected 64 hex characters)")]
    MalformedId(String),

    /// A branch or tag name is already taken.
    #[error("{kind} {name:?} already exists; moving it would discard what it points at")]
    RefExists {
        /// `"branch"` or `"tag"`.
        kind: &'static str,
        /// The name.
        name: String,
    },

    /// A branch or tag does not exist.
    #[error("no such {kind}: {name:?}")]
    NoSuchRef {
        /// `"branch"` or `"tag"`.
        kind: &'static str,
        /// The name.
        name: String,
    },

    /// A storage backend beneath the pager failed.
    ///
    /// The pager does not know whether a page lives on a local disk, in a cache, or in an S3 bucket
    /// in another region — and it must not (CLAUDE.md rule 2). When a backend it cannot see fails,
    /// this is how the failure reaches it without dragging that backend's error type into the core.
    #[error("storage backend failed: {0}")]
    Backend(String),

    /// The filesystem said no.
    #[error("i/o error at {path}: {source}")]
    Io {
        /// The path involved, so the operator knows where to look.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
}

impl PagerError {
    /// Attach a path to a bare [`std::io::Error`].
    ///
    /// An i/o error without a path tells an operator that something failed but not what to
    /// go and look at, so we never construct one.
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        PagerError::Io {
            path: path.into(),
            source,
        }
    }

    /// Wrap a failure from a backend the pager cannot see (object storage, a cache tier).
    pub fn backend(detail: impl std::fmt::Display) -> Self {
        PagerError::Backend(detail.to_string())
    }

    /// True if this error indicates on-disk corruption rather than a logical mistake.
    ///
    /// Callers use this to decide whether to re-fetch the page from object storage
    /// (see `substrate-store`) rather than surface a failure to the user.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            PagerError::CorruptPage { .. } | PagerError::MissingPage(_)
        )
    }
}
