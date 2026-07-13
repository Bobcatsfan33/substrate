//! Crash injection: a filesystem that dies exactly where you tell it to.
//!
//! Enabled by the `test-util` feature. Not compiled into a release build.
//!
//! # Why this exists
//!
//! The claim we make (docs/02 §3.1) is:
//!
//! > after a crash at **any byte boundary**, the recovered store equals **some prefix of committed
//! > transactions** — no torn state, no lost commit.
//!
//! You cannot establish that by pulling a power cord ten thousand times. You establish it by
//! owning the write path and killing it precisely, at every byte, over and over, and checking
//! what came back. That is what [`CrashVfs`] is for.
//!
//! # What it models faithfully
//!
//! - **A torn append.** A crash partway through appending to the WAL leaves the bytes written so
//!   far *on disk*. This is the case the CRC on every record exists to catch, and it is the one
//!   that actually happens.
//! - **Atomicity of `atomic_write`.** Pages and manifests are written to a temp file and renamed,
//!   so a crash leaves the file either complete or absent — never half-written. A `.tmp.` file may
//!   be left behind, and the store must not mistake it for real data.
//! - **The machine is gone.** Once the budget is spent, *every* subsequent write fails. A crashed
//!   process does not get to finish its work.
//!
//! # What it does not model, stated honestly
//!
//! [`MemVfs`] makes every accepted write immediately durable, so it does not model an fsync that
//! was never issued, nor a disk that reorders or lies about durability. That is a real gap — the
//! kind of gap that eats storage engines — and it is why the crash suite *also* runs against the
//! real filesystem, where `fsync` is a genuine syscall. This layer exists to explore the torn-write
//! state space exhaustively and fast; the real-FS runs exist to make sure we are exploring the
//! right space.

use crate::error::Result;
use crate::vfs::Vfs;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// An in-memory filesystem.
///
/// Fast enough to run ten thousand crash-and-recover cycles in seconds, which is the difference
/// between a property we assert and a property we merely hope for.
#[derive(Debug, Default)]
pub struct MemVfs {
    files: Mutex<BTreeMap<PathBuf, Vec<u8>>>,
    dirs: Mutex<BTreeSet<PathBuf>>,
}

impl MemVfs {
    /// A new, empty filesystem.
    pub fn new() -> Arc<Self> {
        Arc::new(MemVfs::default())
    }

    fn files(&self) -> std::sync::MutexGuard<'_, BTreeMap<PathBuf, Vec<u8>>> {
        self.files.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn dirs(&self) -> std::sync::MutexGuard<'_, BTreeSet<PathBuf>> {
        self.dirs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Total bytes stored. Useful for asserting a crash actually truncated something.
    pub fn total_bytes(&self) -> usize {
        self.files().values().map(|v| v.len()).sum()
    }
}

impl Vfs for MemVfs {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut dirs = self.dirs();
        let mut cur = PathBuf::new();
        for part in path.components() {
            cur.push(part);
            dirs.insert(cur.clone());
        }
        Ok(())
    }

    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            self.create_dir_all(parent)?;
        }
        // Atomic by construction: the map either has the new value or the old one.
        self.files().insert(path.to_path_buf(), bytes.to_vec());
        Ok(())
    }

    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            self.create_dir_all(parent)?;
        }
        self.files()
            .entry(path.to_path_buf())
            .or_default()
            .extend_from_slice(bytes);
        Ok(())
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.files()
            .get(path)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        let mut files = self.files();
        let file = files
            .get_mut(path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
        file.truncate(len as usize);
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        self.files().contains_key(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.files().remove(path);
        Ok(())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = BTreeSet::new();
        for file in self.files().keys() {
            if file.parent() == Some(path) {
                out.insert(file.clone());
            }
        }
        for dir in self.dirs().iter() {
            if dir.parent() == Some(path) {
                out.insert(dir.clone());
            }
        }
        Ok(out.into_iter().collect())
    }
}

/// A filesystem that dies after a chosen number of bytes have been written.
///
/// Wrap any [`Vfs`] in one of these, give it a budget, and the `budget`-th written byte is the
/// last one that reaches the disk. Everything after that fails, forever — because the machine is
/// gone, and a dead process does not get to finish its work.
#[derive(Debug)]
pub struct CrashVfs {
    inner: Arc<dyn Vfs>,
    budget: AtomicI64,
    crashed: AtomicBool,
}

impl CrashVfs {
    /// A filesystem that will die after `budget` bytes of writes.
    pub fn with_budget(inner: Arc<dyn Vfs>, budget: i64) -> Arc<Self> {
        Arc::new(CrashVfs {
            inner,
            budget: AtomicI64::new(budget),
            crashed: AtomicBool::new(false),
        })
    }

    /// A filesystem that never dies. For the recovery half of a crash test.
    pub fn unlimited(inner: Arc<dyn Vfs>) -> Arc<Self> {
        CrashVfs::with_budget(inner, i64::MAX)
    }

    /// Whether the crash has happened yet.
    pub fn has_crashed(&self) -> bool {
        self.crashed.load(Ordering::SeqCst)
    }

    /// Bytes of write budget left.
    pub fn remaining(&self) -> i64 {
        self.budget.load(Ordering::SeqCst).max(0)
    }

    fn dead() -> io::Error {
        io::Error::other("simulated crash: the machine is gone (CrashVfs budget exhausted)")
    }

    /// Claim `want` bytes of budget. Returns how many were granted — possibly fewer, which is a
    /// **torn write**: those bytes reach the disk and then the power fails.
    fn claim(&self, want: usize) -> usize {
        if self.crashed.load(Ordering::SeqCst) {
            return 0;
        }
        let want = want as i64;
        let remaining = self.budget.load(Ordering::SeqCst);
        if remaining >= want {
            self.budget.fetch_sub(want, Ordering::SeqCst);
            return want as usize;
        }
        self.budget.store(0, Ordering::SeqCst);
        self.crashed.store(true, Ordering::SeqCst);
        remaining.max(0) as usize
    }
}

impl Vfs for CrashVfs {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        if self.has_crashed() {
            return Err(Self::dead());
        }
        self.inner.create_dir_all(path)
    }

    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let granted = self.claim(bytes.len());
        if granted < bytes.len() {
            // The crash landed inside the temp-file write, before the rename. The final file
            // therefore never appears — which is the entire point of writing to a temp file and
            // renaming. A page or manifest is complete or absent, never half.
            return Err(Self::dead());
        }
        self.inner.atomic_write(path, bytes)
    }

    fn append(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let granted = self.claim(bytes.len());
        if granted < bytes.len() {
            // A TORN WRITE. The bytes we managed to write are really on the disk, and the rest
            // never happened. This is the case the whole WAL design exists to survive: the record
            // that was half-written will fail its CRC, recovery will stop there, and the
            // transaction it belonged to will simply not have happened.
            if granted > 0 {
                self.inner.append(path, &bytes[..granted])?;
            }
            return Err(Self::dead());
        }
        self.inner.append(path, bytes)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        // Reads are free: a crashed machine that has been rebooted can read its disk.
        self.inner.read(path)
    }

    fn truncate(&self, path: &Path, len: u64) -> io::Result<()> {
        if self.has_crashed() {
            return Err(Self::dead());
        }
        self.inner.truncate(path, len)
    }

    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        if self.has_crashed() {
            return Err(Self::dead());
        }
        self.inner.remove_file(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(path)
    }
}

/// A deterministic pseudo-random generator.
///
/// The crash suite runs ten thousand randomized scenarios, and when one of them fails we need to
/// replay *exactly that one*. `rand` gives no such promise across versions; this does, in six
/// lines, forever.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seed it. The same seed always produces the same scenario.
    pub fn new(seed: u64) -> Self {
        // A zero state would produce only zeroes, which is the one seed a user is most likely
        // to pass by hand.
        Rng(seed | 1)
    }

    /// The next value. xorshift64*, which is not cryptographic and does not need to be.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        self.next_u64() % n
    }

    /// A byte.
    pub fn byte(&mut self) -> u8 {
        (self.next_u64() & 0xFF) as u8
    }
}

/// Convenience: an in-memory filesystem that will crash after `budget` bytes.
pub fn crashing_mem_vfs(budget: i64) -> (Arc<MemVfs>, Arc<CrashVfs>) {
    let disk = MemVfs::new();
    let vfs = CrashVfs::with_budget(disk.clone(), budget);
    (disk, vfs)
}

/// Reopen a crashed disk with a filesystem that works again — i.e. the machine rebooted.
pub fn reboot(disk: Arc<MemVfs>) -> Arc<dyn Vfs> {
    disk
}

/// Ensure the module is usable from a `Result`-returning test without extra imports.
#[doc(hidden)]
pub fn ok() -> Result<()> {
    Ok(())
}
