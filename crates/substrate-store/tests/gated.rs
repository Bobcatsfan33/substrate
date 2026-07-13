//! An object store whose uploads you can hold open.
//!
//! # Why this exists
//!
//! The safety property of the whole tiering layer is:
//!
//! > **A page is evictable only once it is confirmed durable in object storage.**
//!
//! A test for that has to catch the engine in the window where a page exists *nowhere but locally*
//! and demand that it be evicted. Against a fast in-memory backend that window is essentially zero —
//! the uploader always wins the race — so the test would pass **without ever running the scenario it
//! exists to check.** It did, in fact, do exactly that until an assertion was added to catch it, and
//! a test that reports green while proving nothing is worse than no test at all.
//!
//! So: a backend that will not complete an upload until we say so. Now the window is as wide as we
//! like, and the property is actually tested rather than merely hoped for.

#![allow(dead_code)] // used by the lifecycle suite; not every test needs every helper

use futures::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Wraps any object store and holds `put` open until released.
#[derive(Debug)]
pub struct GatedStore {
    inner: Arc<dyn ObjectStore>,
    open: AtomicBool,
    released: Arc<Notify>,
    puts_blocked: AtomicU64,
}

impl GatedStore {
    /// A store whose uploads are **blocked** from the start.
    pub fn closed(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(GatedStore {
            inner,
            open: AtomicBool::new(false),
            released: Arc::new(Notify::new()),
            puts_blocked: AtomicU64::new(0),
        })
    }

    /// Let the uploads through. Wakes everything currently waiting.
    pub fn open_gate(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.released.notify_waiters();
    }

    /// How many uploads have been held at the gate.
    pub fn blocked(&self) -> u64 {
        self.puts_blocked.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if !self.open.load(Ordering::SeqCst) {
            self.puts_blocked.fetch_add(1, Ordering::SeqCst);
            // Hold here until someone opens the gate. The page stays local-only, and therefore —
            // if the engine is correct — unevictable.
            while !self.open.load(Ordering::SeqCst) {
                self.released.notified().await;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjPath) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjPath, to: &ObjPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
