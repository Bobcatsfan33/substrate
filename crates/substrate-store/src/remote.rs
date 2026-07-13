//! Object storage: the durable tier.
//!
//! # Keys are pool-scoped, and that is a security boundary
//!
//! ```text
//! <pool>/pages/aa/bb/<page-hash>
//! <pool>/manifests/<manifest-hash>
//! <pool>/wal/<segment>.wal
//! ```
//!
//! **A store belongs to exactly one pool, and pools never share pages even when their hashes are
//! identical** (docs/02 §9.1). The pool is the *first path component*, so two pools cannot collide
//! even in principle — not because we check, but because they are writing to different places.
//!
//! It costs cross-pool deduplication. It buys the guarantee that data cannot flow between two
//! classification boundaries through the storage layer, which is the entire reason a CUI customer
//! will talk to us.
//!
//! # Content addressing makes the cache trivially correct
//!
//! A key is a hash of the bytes it holds. So a cached object can never be stale — different content
//! is a different key. There is no invalidation protocol, no TTL, no versioning, and no cache-
//! coherence bug, because the only way to change what a key means is to change what a hash means.

use crate::error::{Result, StoreError};
use bytes::Bytes;
use object_store::{path::Path as ObjPath, ObjectStore};
use std::sync::Arc;
use substrate_pager::{ManifestId, PageId};

/// Object storage, scoped to one pool.
#[derive(Clone)]
pub struct RemoteTier {
    backend: Arc<dyn ObjectStore>,
    pool: String,
}

impl RemoteTier {
    /// Bind an object store to a pool.
    ///
    /// The pool name becomes the key prefix for everything this store ever writes. It cannot be
    /// changed afterwards, because doing so would orphan every object already written.
    pub fn new(backend: Arc<dyn ObjectStore>, pool: impl Into<String>) -> Self {
        RemoteTier {
            backend,
            pool: pool.into(),
        }
    }

    /// The pool this tier is bound to.
    pub fn pool(&self) -> &str {
        &self.pool
    }

    /// The key a page lives at.
    ///
    /// Sharded two levels, exactly like the local CAS — object stores do not have directories, but
    /// prefix listing gets slow with millions of keys under one prefix, and the sharding keeps
    /// `list` operations useful for the integrity scrubber.
    pub fn page_key(&self, id: PageId) -> ObjPath {
        let hex = id.to_hex();
        ObjPath::from(format!(
            "{}/pages/{}/{}/{}",
            self.pool,
            &hex[0..2],
            &hex[2..4],
            hex
        ))
    }

    /// The key a manifest lives at.
    pub fn manifest_key(&self, id: ManifestId) -> ObjPath {
        ObjPath::from(format!("{}/manifests/{}", self.pool, id.to_hex()))
    }

    /// The key a sealed WAL segment lives at.
    pub fn segment_key(&self, segment: u64) -> ObjPath {
        ObjPath::from(format!("{}/wal/{segment:012}.wal", self.pool))
    }

    /// Upload bytes.
    ///
    /// Content-addressed keys are **write-once**: if the object already exists, its bytes are
    /// already correct, and rewriting them would be pure cost. We check first because a HEAD is
    /// dramatically cheaper than a PUT, and because most pages in a fleet of forked databases are
    /// already there.
    pub async fn put(&self, key: &ObjPath, bytes: Vec<u8>) -> Result<()> {
        if self.exists(key).await? {
            return Ok(());
        }
        self.backend
            .put(key, Bytes::from(bytes).into())
            .await
            .map_err(|e| StoreError::remote(key.as_ref(), e))?;
        Ok(())
    }

    /// Download bytes. `None` if the object is not there.
    pub async fn get(&self, key: &ObjPath) -> Result<Option<Vec<u8>>> {
        match self.backend.get(key).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| StoreError::remote(key.as_ref(), e))?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::remote(key.as_ref(), e)),
        }
    }

    /// Whether the object exists.
    pub async fn exists(&self, key: &ObjPath) -> Result<bool> {
        match self.backend.head(key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StoreError::remote(key.as_ref(), e)),
        }
    }

    /// Refuse to touch anything outside this store's pool.
    ///
    /// A belt-and-braces check on top of a structural guarantee. The key layout already makes
    /// cross-pool access impossible; this makes an *attempt* loud instead of silently reading
    /// nothing.
    pub fn guard_pool(&self, named: &str) -> Result<()> {
        if named != self.pool {
            return Err(StoreError::PoolBoundary {
                ours: self.pool.clone(),
                theirs: named.to_string(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Debug for RemoteTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTier")
            .field("pool", &self.pool)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn tier(pool: &str) -> RemoteTier {
        RemoteTier::new(Arc::new(InMemory::new()), pool)
    }

    #[tokio::test]
    async fn round_trips() -> Result<()> {
        let tier = tier("default");
        let id = PageId::of(b"hello");
        let key = tier.page_key(id);

        assert!(!tier.exists(&key).await?);
        assert_eq!(tier.get(&key).await?, None);

        tier.put(&key, b"hello".to_vec()).await?;
        assert!(tier.exists(&key).await?);
        assert_eq!(tier.get(&key).await?, Some(b"hello".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn put_is_idempotent() -> Result<()> {
        let tier = tier("default");
        let key = tier.page_key(PageId::of(b"x"));
        tier.put(&key, b"x".to_vec()).await?;
        tier.put(&key, b"x".to_vec()).await?;
        assert_eq!(tier.get(&key).await?, Some(b"x".to_vec()));
        Ok(())
    }

    #[test]
    fn pools_cannot_collide_even_on_identical_content() {
        // The CUI guarantee, at the level where it is actually enforced: the same bytes in two
        // pools are two different objects, in two different places. Not "we check" — they simply
        // are not the same key.
        let secret = PageId::of(b"TROOP MOVEMENT 0400");
        let cui = tier("cui-secret");
        let public = tier("public");

        assert_ne!(cui.page_key(secret), public.page_key(secret));
        assert!(cui.page_key(secret).as_ref().starts_with("cui-secret/"));
        assert!(public.page_key(secret).as_ref().starts_with("public/"));
    }

    #[test]
    fn crossing_a_pool_boundary_is_loud() {
        let cui = tier("cui-secret");
        assert!(cui.guard_pool("cui-secret").is_ok());
        assert!(matches!(
            cui.guard_pool("public"),
            Err(StoreError::PoolBoundary { .. })
        ));
    }
}
