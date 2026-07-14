//! Errors for `substrate-security`.

use std::path::PathBuf;
use substrate_pager::PageId;

/// The result type for every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, SecurityError>;

/// Everything that can go wrong.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SecurityError {
    /// Encryption failed. Should be impossible; the AEAD does not fail on well-formed input.
    #[error("failed to encrypt a page")]
    Encrypt,

    /// A page did not decrypt.
    ///
    /// The wrong key, tampered ciphertext, or a page lifted from one address and stored at another —
    /// the AEAD does not distinguish, and neither should we, because from a defender's point of view
    /// they are the same event: **someone or something changed bytes we trusted.**
    #[error("page {page} failed to decrypt: wrong key, tampered ciphertext, or a relocated page")]
    Decrypt {
        /// The page that failed.
        page: PageId,
    },

    /// Decryption succeeded, but the plaintext does not hash to the id it was stored under.
    ///
    /// The AEAD should have caught this. If we are here, there is a bug in this crate.
    #[error("page {page} decrypted, but the plaintext does not hash to its id — this is a bug")]
    PlaintextMismatch {
        /// The page that failed.
        page: PageId,
    },

    /// A pool has no master key.
    #[error("no master key for pool {pool:?}; create one before opening a store in it")]
    NoKeyForPool {
        /// The pool.
        pool: String,
    },

    /// A pool already has a master key, and overwriting it would destroy the pool.
    #[error(
        "pool {pool:?} already has a master key. Overwriting it would make every page in the pool \
         permanently unreadable; if you mean to rotate, derive a new key and re-seal."
    )]
    KeyExists {
        /// The pool.
        pool: String,
    },

    /// A key file is readable by someone other than its owner.
    #[error(
        "key file {path} has mode {mode:o} — it is readable by group or world. \
         Refusing to load it. Set it to 0600."
    )]
    KeyPermissions {
        /// The offending file.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },

    /// A key file is not 32 bytes.
    #[error("key file {path} is malformed: expected exactly 32 bytes of key material")]
    MalformedKey {
        /// The offending file.
        path: PathBuf,
    },

    /// A licence signature did not verify.
    ///
    /// **This does not stop the database.** It produces `Status::Degraded`, and `Degraded` disables
    /// fleet-plane administration and nothing else (docs/02 §9.2).
    #[error("licence signature is invalid")]
    LicenseSignature,

    /// The OS entropy source failed.
    #[error("could not read entropy from the operating system")]
    Entropy,

    /// Serialization failed.
    #[error("failed to encode or decode {what}: {source}")]
    Codec {
        /// What we were handling.
        what: &'static str,
        /// Why it failed.
        #[source]
        source: serde_json::Error,
    },

    /// The pager refused an operation.
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

impl SecurityError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        SecurityError::Io {
            path: path.into(),
            source,
        }
    }
}
