//! Errors for `substrate-store`.

use std::path::PathBuf;

/// The result type for every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, StoreError>;

/// Everything that can go wrong in the tiering layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// A page is in neither the local cache nor object storage.
    ///
    /// This is the one error in this crate that should never happen. Eviction refuses to touch a
    /// page that is not confirmed durable remotely, so a page that is missing from both tiers means
    /// something deleted it out from under us.
    #[error(
        "page {0} is in neither the local cache nor object storage — this should be impossible"
    )]
    PageLost(String),

    /// Object storage said no.
    #[error("object storage error on key {key}: {source}")]
    Remote {
        /// The key we were reaching for.
        key: String,
        /// Why it failed.
        #[source]
        source: Box<object_store::Error>,
    },

    /// A wake token could not be read.
    #[error("wake token is malformed: {0}")]
    MalformedToken(String),

    /// A store in pool A was asked for an object belonging to pool B.
    ///
    /// **Pools never share pages, even when hashes match** (docs/02 §9.1). There is no setting that
    /// turns this off, and this error is what it looks like when something tries.
    #[error("pool boundary violation: this store belongs to pool {ours:?}, but the request named {theirs:?}")]
    PoolBoundary {
        /// The pool this store belongs to.
        ours: String,
        /// The pool that was named.
        theirs: String,
    },

    /// A blocking read from object storage was attempted with no tokio runtime available.
    ///
    /// The `PageStore` read path is synchronous by design (CLAUDE.md rule 7), so a cache miss has to
    /// block on the async fetch. That requires a runtime handle. See `TieredCas` for why this
    /// tradeoff is the right one.
    #[error(
        "a page missed the local cache and must be fetched from object storage, but there is no \
         tokio runtime on this thread. Construct the store from within a runtime, or pass a Handle."
    )]
    NoRuntime,

    /// The pager refused an operation.
    #[error(transparent)]
    Pager(#[from] substrate_pager::PagerError),

    /// The write-ahead log refused an operation.
    #[error(transparent)]
    Wal(#[from] substrate_wal::WalError),

    /// Serialization failed.
    #[error("failed to {op} {what}: {source}")]
    Codec {
        /// `"encode"` or `"decode"`.
        op: &'static str,
        /// What we were trying to handle.
        what: &'static str,
        /// The underlying error.
        #[source]
        source: serde_json::Error,
    },

    /// The local filesystem said no.
    #[error("i/o error at {path}: {source}")]
    Io {
        /// Where.
        path: PathBuf,
        /// Why.
        #[source]
        source: std::io::Error,
    },
}

impl StoreError {
    /// Wrap an object-storage failure with the key it happened on.
    pub fn remote(key: impl Into<String>, source: object_store::Error) -> Self {
        StoreError::Remote {
            key: key.into(),
            source: Box::new(source),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        StoreError::Io {
            path: path.into(),
            source,
        }
    }
}
