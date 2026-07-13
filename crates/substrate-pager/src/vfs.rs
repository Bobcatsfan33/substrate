//! The filesystem, as a seam.
//!
//! Every byte this engine makes durable goes through [`Vfs`]. Not because we plan to run on
//! anything but a real filesystem, but because **a durability guarantee you cannot test is a
//! durability guarantee you do not have.**
//!
//! The property we are required to prove (docs/02 §3.1) is:
//!
//! > after a crash at *any byte boundary*, the recovered store equals **some prefix of committed
//! > transactions** — no torn state, no lost commit.
//!
//! You cannot prove that by pulling the power cord ten thousand times. You prove it by owning the
//! write path, and killing it exactly where you choose. That is what this trait is for, and
//! `CrashVfs` in the test harness is the thing that does the killing.
//!
//! # The two durable write patterns
//!
//! - [`Vfs::atomic_write`] — for pages and manifests. Write to a temp file, fsync the *contents*,
//!   rename into place, then fsync the *directory*. A reader therefore sees the file complete or
//!   not at all. (Fsyncing the directory is the step everyone forgets: without it the file's
//!   contents survive the crash but its name does not, and you are left with an orphaned inode.)
//! - [`Vfs::append`] — for the WAL, where a torn tail is *expected* and handled by CRC. This is
//!   the one place a partial write is allowed to reach the disk, because the log is designed to
//!   detect and discard it.

use std::io;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The filesystem operations substrate needs, and no others.
///
/// Deliberately tiny. Every method here is a place a crash can happen, so each one is a
/// liability, and the smallest surface that works is the right one.
pub trait Vfs: Send + Sync + std::fmt::Debug {
    /// Create a directory and all its parents.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Durably write a file that either appears complete or does not appear at all.
    ///
    /// Implementations must fsync the contents *and* the containing directory before returning.
    /// Used for pages and manifests, where a half-written file is a corrupt database.
    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Append to a file and fsync, creating it if absent.
    ///
    /// A crash here may leave a **partial record** on disk. That is expected and permitted: the
    /// WAL puts a CRC on every record precisely so a torn tail is detectable, and recovery
    /// truncates at the first record that fails to verify.
    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Read a whole file.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Cut a file down to `len` bytes and fsync it.
    ///
    /// Recovery uses this to discard a torn tail. It matters more than it looks: if a partial
    /// record were left in place, the next append would land *after* the tear, and replay — which
    /// stops at the first record that fails its CRC — would never reach it. The transaction would
    /// be durable on disk and invisible forever, which is the definition of a lost commit.
    fn truncate(&self, path: &Path, len: u64) -> io::Result<()>;

    /// Whether the path exists.
    fn exists(&self, path: &Path) -> bool;

    /// Delete a file. Deleting an absent file is success — GC and recovery are both re-runnable.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// List the entries directly under a directory. An absent directory lists as empty.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
}

/// The real filesystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdVfs;

impl StdVfs {
    /// fsync a directory, so that a create or rename inside it is itself durable.
    fn fsync_dir(path: &Path) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }
}

impl Vfs for StdVfs {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let dir = path
            .parent()
            .ok_or_else(|| io::Error::other("path has no parent directory"))?;
        std::fs::create_dir_all(dir)?;

        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("path has no file name"))?;
        let tmp = dir.join(format!(".tmp.{file_name}"));

        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?; // contents durable...
        }
        std::fs::rename(&tmp, path)?; // ...then atomically named...
        Self::fsync_dir(dir)?; // ...and the name is durable too.
        Ok(())
    }

    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let existed = path.exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        if !existed {
            // A brand-new segment's *name* must be durable too, or recovery cannot find the
            // records we just fsynced into it.
            if let Some(dir) = path.parent() {
                Self::fsync_dir(dir)?;
            }
        }
        Ok(())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(len)?;
        file.sync_all()
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        match std::fs::read_dir(path) {
            Ok(entries) => entries.map(|e| e.map(|e| e.path())).collect(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }
}

/// The default filesystem handle.
pub fn std_vfs() -> Arc<dyn Vfs> {
    Arc::new(StdVfs)
}
