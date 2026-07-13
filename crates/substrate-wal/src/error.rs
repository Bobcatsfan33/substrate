//! Errors for `substrate-wal`.

use std::path::PathBuf;

/// The result type for every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, WalError>;

/// Everything that can go wrong in the log.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WalError {
    /// Replaying the log produced a different manifest than the one the commit record names.
    ///
    /// This is the loudest error in the engine, and it is deliberately unrecoverable. It means
    /// the log and the manifest disagree about what the database contains — so *either* could be
    /// the truth, and installing one would be a guess. We would rather refuse to open a database
    /// than silently serve a version of it that nobody committed.
    ///
    /// In practice it means one of: a format change without a migration, a bug in `derive`, or
    /// storage that lied about a durable write.
    #[error(
        "replay diverged at lsn {lsn}: the log rebuilds manifest {actual}, \
         but the commit record says {expected}. The log and the manifests disagree; \
         refusing to guess which is real."
    )]
    ReplayDiverged {
        /// Where the divergence was found.
        lsn: u64,
        /// The manifest the commit record claims.
        expected: String,
        /// The manifest replay actually produced.
        actual: String,
    },

    /// Recovery could not complete.
    #[error("recovery failed: {0}")]
    Recovery(String),

    /// A record's payload exceeds the maximum. Refuse rather than allocate.
    #[error("record is {actual} bytes, which exceeds the {max}-byte maximum")]
    RecordTooLarge {
        /// The offending size.
        actual: usize,
        /// The limit.
        max: usize,
    },

    /// Serialization or deserialization of a record failed.
    #[error("failed to {op} wal record: {source}")]
    Codec {
        /// `"encode"` or `"decode"`.
        op: &'static str,
        /// The underlying bincode error.
        #[source]
        source: Box<bincode::ErrorKind>,
    },

    /// The pager refused an operation during commit or replay.
    #[error(transparent)]
    Pager(#[from] substrate_pager::PagerError),

    /// The filesystem said no.
    #[error("i/o error at {path}: {source}")]
    Io {
        /// Where.
        path: PathBuf,
        /// Why.
        #[source]
        source: std::io::Error,
    },
}

impl WalError {
    /// Attach a path to a bare [`std::io::Error`]. An i/o error without a path tells an operator
    /// that something broke but not what to go and look at.
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        WalError::Io {
            path: path.into(),
            source,
        }
    }
}
