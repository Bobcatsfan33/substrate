//! The key hierarchy, and where keys come from.
//!
//! ```text
//!   KeyProvider  ──►  PoolMasterKey        one per pool. Never touches a page.
//!                          │
//!                          ├──► DataKey("acme-prod")      derived, per database
//!                          ├──► DataKey("globex-prod")
//!                          └──► DataKey("initech-dev")
//! ```
//!
//! # Why a hierarchy at all
//!
//! So that compromising one database's key does not hand over the pool. The master key derives, but
//! never encrypts; the data keys encrypt, but cannot be walked back to the master. That is the entire
//! benefit, and it is worth the small amount of machinery.
//!
//! Derivation is BLAKE3's `derive_key` — a KDF, domain-separated, one-way. Given `DataKey("acme")`
//! you cannot recover the master, and you cannot derive `DataKey("globex")`.
//!
//! # Rotation
//!
//! Rotating a **data key** means re-encrypting that database's pages. Rotating the **master** means
//! re-deriving every data key, which means re-encrypting everything in the pool. Neither is free, and
//! pretending otherwise in the API would be a lie an operator discovers at the worst moment. The
//! honest interface is: derive a new key, re-seal, swap. There is no magic.

use crate::error::{Result, SecurityError};
use std::path::{Path, PathBuf};
use zeroize::{Zeroize, ZeroizeOnDrop};

/// The domain separator for data-key derivation.
const DATA_KEY_CONTEXT: &str = "substrate-data-key-v1";

/// A pool's master key. **Derives keys; never encrypts a page.**
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct PoolMasterKey([u8; 32]);

impl PoolMasterKey {
    /// Wrap 32 bytes of key material.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        PoolMasterKey(bytes)
    }

    /// Generate a fresh master key from the OS entropy source.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0u8; 32];
        getrandom(&mut bytes)?;
        Ok(PoolMasterKey(bytes))
    }

    /// Derive the data key for one database.
    ///
    /// One-way and domain-separated: holding this data key gives you nothing about the master, and
    /// nothing about any other database's key.
    pub fn derive_data_key(&self, database: &str) -> DataKey {
        let mut hasher = blake3::Hasher::new_derive_key(DATA_KEY_CONTEXT);
        hasher.update(&self.0);
        hasher.update(database.as_bytes());
        DataKey(*hasher.finalize().as_bytes())
    }

    /// The raw bytes. For persisting to a KMS or a key file — nothing else.
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for PoolMasterKey {
    /// Never prints the key. A key in a log file is a key in a log aggregator, a key in a support
    /// bundle, and a key in a screenshot.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PoolMasterKey(<redacted>)")
    }
}

/// The key that actually encrypts one database's pages.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct DataKey([u8; 32]);

impl DataKey {
    /// The raw bytes, for the AEAD.
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataKey(<redacted>)")
    }
}

/// Where master keys come from.
///
/// A file today; a KMS or an HSM later. The trait exists so that adding one does not touch a single
/// line of the encryption path — and so that an air-gapped deployment, which has no KMS to call, can
/// use a file without that being a downgrade or a special case.
pub trait KeyProvider: Send + Sync + std::fmt::Debug {
    /// Fetch a pool's master key.
    fn master_key(&self, pool: &str) -> Result<PoolMasterKey>;
}

/// Master keys in files on disk, one per pool.
///
/// The obvious question is "isn't a key on disk insecure?" — and the answer depends entirely on what
/// you are defending against. It does nothing against an attacker who has root on the box. It does
/// everything against a stolen disk, a leaked S3 bucket, a decommissioned drive, or a backup that
/// ends up somewhere it should not — which is how storage data *actually* leaks. In an air-gapped
/// facility with no KMS to call, it is also the only option there is.
///
/// Key files are created with mode `0600`, and a key file with looser permissions is **refused**
/// rather than warned about, because a warning about a secret is a warning nobody reads.
#[derive(Debug)]
pub struct FileKeyProvider {
    dir: PathBuf,
}

impl FileKeyProvider {
    /// Open (creating if absent) a key directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| SecurityError::io(&dir, e))?;
        Ok(FileKeyProvider { dir })
    }

    fn path_of(&self, pool: &str) -> PathBuf {
        self.dir.join(format!("{pool}.key"))
    }

    /// Create a pool's master key, if it does not already exist.
    ///
    /// Refuses to overwrite. Silently replacing a master key would render every page in the pool
    /// permanently unreadable, which is not a thing an API should let you do by mistake.
    pub fn create_pool_key(&self, pool: &str) -> Result<PoolMasterKey> {
        let path = self.path_of(pool);
        if path.exists() {
            return Err(SecurityError::KeyExists {
                pool: pool.to_string(),
            });
        }

        let key = PoolMasterKey::generate()?;
        std::fs::write(&path, key.expose()).map_err(|e| SecurityError::io(&path, e))?;
        restrict_permissions(&path)?;
        Ok(key)
    }
}

impl KeyProvider for FileKeyProvider {
    fn master_key(&self, pool: &str) -> Result<PoolMasterKey> {
        let path = self.path_of(pool);

        let bytes = std::fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => SecurityError::NoKeyForPool {
                pool: pool.to_string(),
            },
            _ => SecurityError::io(&path, e),
        })?;

        check_permissions(&path)?;

        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| SecurityError::MalformedKey { path: path.clone() })?;
        Ok(PoolMasterKey::from_bytes(bytes))
    }
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| SecurityError::io(path, e))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn check_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)
        .map_err(|e| SecurityError::io(path, e))?
        .permissions()
        .mode()
        & 0o777;

    // Group- or world-readable. Refuse, rather than warn — a warning about a leaked secret is a
    // warning that scrolls past.
    if mode & 0o077 != 0 {
        return Err(SecurityError::KeyPermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn getrandom(buf: &mut [u8]) -> Result<()> {
    use rand_core::RngCore;
    rand_core::OsRng
        .try_fill_bytes(buf)
        .map_err(|_| SecurityError::Entropy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_keys_are_distinct_per_database() {
        let master = PoolMasterKey::from_bytes([1u8; 32]);
        let a = master.derive_data_key("acme");
        let b = master.derive_data_key("globex");

        assert_ne!(a.expose(), b.expose());
        // ...and deterministic, or a restart would lose every page.
        assert_eq!(master.derive_data_key("acme").expose(), a.expose());
    }

    #[test]
    fn a_data_key_reveals_nothing_about_the_master() {
        let master = PoolMasterKey::from_bytes([1u8; 32]);
        let derived = master.derive_data_key("acme");
        assert_ne!(derived.expose(), master.expose());
    }

    #[test]
    fn two_pools_derive_different_keys_for_the_same_database_name() {
        // Pools are a boundary. Two pools with a database of the same name must not share a key,
        // or the boundary is decorative.
        let a = PoolMasterKey::from_bytes([1u8; 32]).derive_data_key("prod");
        let b = PoolMasterKey::from_bytes([2u8; 32]).derive_data_key("prod");
        assert_ne!(a.expose(), b.expose());
    }

    #[test]
    fn keys_are_never_printed() {
        let master = PoolMasterKey::from_bytes([0xAB; 32]);
        let debug = format!("{master:?} {:?}", master.derive_data_key("acme"));
        assert!(debug.contains("redacted"));
        assert!(!debug.contains("171") && !debug.contains("ab"));
    }

    #[test]
    fn file_provider_round_trips() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = FileKeyProvider::open(dir.path())?;

        let created = provider.create_pool_key("acme")?;
        let loaded = provider.master_key("acme")?;
        assert_eq!(created.expose(), loaded.expose());
        Ok(())
    }

    #[test]
    fn creating_a_pool_key_twice_is_refused() -> Result<()> {
        // Overwriting a master key renders every page in the pool permanently unreadable. That must
        // not be something an API lets you do by accident.
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = FileKeyProvider::open(dir.path())?;
        provider.create_pool_key("acme")?;
        assert!(matches!(
            provider.create_pool_key("acme"),
            Err(SecurityError::KeyExists { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_missing_pool_key_is_a_clear_error_not_a_silent_default() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = FileKeyProvider::open(dir.path())?;
        assert!(matches!(
            provider.master_key("never-created"),
            Err(SecurityError::NoKeyForPool { .. })
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn a_world_readable_key_file_is_refused_not_warned_about() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let provider = FileKeyProvider::open(dir.path())?;
        provider.create_pool_key("acme")?;

        let path = dir.path().join("acme.key");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("loosen permissions");

        assert!(
            matches!(
                provider.master_key("acme"),
                Err(SecurityError::KeyPermissions { .. })
            ),
            "a world-readable master key must be refused; a warning is a warning nobody reads"
        );
        Ok(())
    }
}
