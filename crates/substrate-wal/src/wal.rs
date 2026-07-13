//! The write-ahead log: segments, the commit protocol, and recovery.
//!
//! # The commit protocol, and why the order is not negotiable
//!
//! ```text
//! 1. page bytes → CAS, fsync           durable, but nothing references them yet
//! 2. WAL commit record, fsync          ◄── THE COMMIT POINT. atomic. before this, nothing happened.
//! 3. install the manifest              now readers can see it
//! ```
//!
//! Crash **before step 2**: the CAS holds pages no manifest references. They are indistinguishable
//! from garbage, which is exactly what they are, and GC sweeps them. Nothing is corrupt.
//!
//! Crash **after step 2, before step 3**: the transaction *happened*. Recovery replays the log,
//! re-derives the identical manifest (the record carries everything needed, including the
//! timestamp), installs it, and the commit is honoured.
//!
//! There is no window in between, because step 2 is a single fsync of a single CRC-protected
//! record. It either landed or it did not.
//!
//! # What recovery guarantees
//!
//! > After a crash at **any byte boundary**, the recovered store equals **some prefix of committed
//! > transactions.** No torn state. No lost commit.
//!
//! That sentence is the product. `testing/fuzz` kills the write path at every byte in turn and
//! asserts it.

use crate::error::{Result, WalError};
use crate::record::{Lsn, ReadOutcome, Record, RecordKind, TxnWrites};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use substrate_pager::{ManifestId, Pager, Vfs};

/// The target size of a WAL segment before it is sealed and a new one started.
///
/// Segments exist so that checkpointing can delete history without rewriting a single enormous
/// file. 4 MiB is small enough that a checkpoint frees space promptly, and large enough that
/// sealing is rare compared to committing.
pub const SEGMENT_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// What a recovery did. Worth logging: an operator who has just had a crash wants to know
/// precisely how much survived, and "it's fine" is not an answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Committed transactions replayed from the log.
    pub committed_txns: u64,
    /// Records read and verified.
    pub records_replayed: u64,
    /// Whether the log ended in a torn record — i.e. we crashed mid-write.
    ///
    /// Not an error. This is what a crash *looks like*, and finding it means the log did its job.
    pub torn_tail: bool,
    /// Page writes discarded because the transaction that staged them never committed.
    pub uncommitted_writes_discarded: u64,
    /// The last LSN that is durable and applied.
    pub durable_lsn: Lsn,
}

/// The write-ahead log.
///
/// Owns the commit point. Nothing in this engine is committed until a record has been fsync'd here.
pub struct Wal {
    vfs: Arc<dyn Vfs>,
    dir: PathBuf,
    /// The segment currently being appended to.
    current: u64,
    /// Bytes written to the current segment, for sealing.
    current_bytes: u64,
    /// The next LSN to hand out.
    next_lsn: Lsn,
}

impl Wal {
    /// Open (creating if absent) a log in `dir`.
    ///
    /// Does **not** replay: call [`Wal::recover`] for that. Opening and recovering are separate
    /// so that recovery is an explicit, auditable act rather than a side effect of a constructor.
    pub fn open(vfs: Arc<dyn Vfs>, dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().join("wal");
        vfs.create_dir_all(&dir)
            .map_err(|e| WalError::io(&dir, e))?;

        let segments = Self::segments_in(&*vfs, &dir)?;
        let current = segments.last().copied().unwrap_or(0);
        let current_bytes = if segments.is_empty() {
            0
        } else {
            vfs.read(&Self::segment_path(&dir, current))
                .map(|b| b.len() as u64)
                .unwrap_or(0)
        };

        Ok(Wal {
            vfs,
            dir,
            current,
            current_bytes,
            next_lsn: 0,
        })
    }

    fn segment_path(dir: &Path, n: u64) -> PathBuf {
        dir.join(format!("{n:012}.wal"))
    }

    /// Every segment in the log, in order.
    fn segments_in(vfs: &dyn Vfs, dir: &Path) -> Result<Vec<u64>> {
        let mut out: Vec<u64> = vfs
            .read_dir(dir)
            .map_err(|e| WalError::io(dir, e))?
            .iter()
            .filter_map(|p| {
                let name = p.file_name()?.to_str()?;
                name.strip_suffix(".wal")?.parse::<u64>().ok()
            })
            .collect();
        out.sort_unstable();
        Ok(out)
    }

    /// The path of the checkpoint marker.
    fn checkpoint_path(&self) -> PathBuf {
        self.dir.join("CHECKPOINT")
    }

    /// Append records and fsync. **Everything in `records` becomes durable together, or not at all.**
    ///
    /// One `append` means one fsync, and one fsync is the expensive thing in a database. Writing
    /// a transaction's page records and its commit record in a single call is therefore both
    /// faster *and* safer than writing them separately: there is no interval in which some of a
    /// transaction's records are durable and the rest are not, because a crash mid-append leaves
    /// a torn record, and replay discards everything from the tear onward — including the commit.
    fn append(&mut self, records: &[Record]) -> Result<()> {
        let mut buf = Vec::new();
        for record in records {
            buf.extend_from_slice(&record.encode()?);
        }

        let path = Self::segment_path(&self.dir, self.current);
        self.vfs
            .append(&path, &buf)
            .map_err(|e| WalError::io(&path, e))?;

        self.current_bytes += buf.len() as u64;

        // Seal and roll over. Done *after* the append, never in the middle of a transaction: a
        // transaction must never straddle two segments, or a crash could leave its writes in one
        // sealed segment and its commit record in a segment that was never created.
        if self.current_bytes >= SEGMENT_TARGET_BYTES {
            self.current += 1;
            self.current_bytes = 0;
        }
        Ok(())
    }

    /// Log a committed transaction. **This is the commit point.**
    ///
    /// When this returns `Ok`, the transaction has happened — even if the process dies before the
    /// manifest is installed. Recovery will re-derive it.
    ///
    /// `manifest` is the manifest the transaction produces, and `created_at_ms` is the timestamp
    /// baked into it. Both are recorded so that replay reconstructs the identical manifest rather
    /// than a merely equivalent one.
    pub fn commit(
        &mut self,
        writes: &TxnWrites,
        manifest: ManifestId,
        created_at_ms: u64,
    ) -> Result<Lsn> {
        let mut records = Vec::with_capacity(writes.len() + 1);

        for (&page_no, &page) in writes {
            records.push(Record {
                lsn: self.take_lsn(),
                kind: RecordKind::Write { page_no, page },
            });
        }

        let commit_lsn = self.take_lsn();
        records.push(Record {
            lsn: commit_lsn,
            kind: RecordKind::Commit {
                manifest,
                created_at_ms,
            },
        });

        self.append(&records)?;
        Ok(commit_lsn)
    }

    /// Record that everything up to now is captured by `manifest`, and drop the history behind it.
    ///
    /// Recovery then starts here rather than at the beginning of time, which is what keeps
    /// recovery time bounded no matter how long the database has been running.
    pub fn checkpoint(&mut self, manifest: ManifestId) -> Result<Lsn> {
        let lsn = self.take_lsn();
        self.append(&[Record {
            lsn,
            kind: RecordKind::Checkpoint { manifest },
        }])?;

        // The marker goes down atomically *after* the record is durable. If we crash between the
        // two, the checkpoint record is in the log but the marker is not: recovery simply replays
        // from further back and arrives at the same place. Slower, identical, safe.
        let marker = Record {
            lsn,
            kind: RecordKind::Checkpoint { manifest },
        }
        .encode()?;
        let path = self.checkpoint_path();
        self.vfs
            .atomic_write(&path, &marker)
            .map_err(|e| WalError::io(&path, e))?;

        // Only now is it safe to delete segments. Truncating history before the marker is durable
        // would be deleting the only record of transactions the marker has not yet captured.
        self.truncate_segments_before(self.current)?;
        Ok(lsn)
    }

    /// Delete every segment strictly before `keep_from`.
    fn truncate_segments_before(&self, keep_from: u64) -> Result<()> {
        for segment in Self::segments_in(&*self.vfs, &self.dir)? {
            if segment < keep_from {
                let path = Self::segment_path(&self.dir, segment);
                self.vfs
                    .remove_file(&path)
                    .map_err(|e| WalError::io(&path, e))?;
            }
        }
        Ok(())
    }

    fn take_lsn(&mut self) -> Lsn {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        lsn
    }

    /// The LSN that will be assigned next.
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Replay the log onto `pager`, restoring the head it had before the crash.
    ///
    /// Deterministic and idempotent: replaying the same log twice yields byte-identical manifests
    /// and the same head. Both properties are tested, because recovery that is not deterministic
    /// is recovery that cannot be verified, and every other guarantee in this engine rests on it.
    pub fn recover(&mut self, pager: &Pager) -> Result<Recovery> {
        let mut recovery = Recovery::default();

        // Replay begins at a FIXED base: the checkpoint if there is one, otherwise the canonical
        // empty root. Never from the pager's current head — see `Pager::root_manifest`. Starting
        // from the head works flawlessly the first time and diverges on the second, which is
        // precisely the run that happens when a machine crashes while recovering from a crash.
        //
        // `captured_through` is the LSN the checkpoint already accounts for. Every record at or
        // below it has ALREADY been folded into the checkpoint manifest, so replaying it would
        // apply the same transaction twice — deriving from the checkpointed head rather than from
        // the head that transaction actually saw, and producing a manifest that does not match its
        // own commit record. The store would then refuse to open. A checkpoint that makes a
        // database unopenable is worse than no checkpoint at all.
        let mut captured_through: Option<Lsn> = None;

        let mut head = match self.read_checkpoint()? {
            Some((manifest, lsn)) => {
                pager.set_head_to(manifest).map_err(|e| {
                    WalError::Recovery(format!("checkpoint manifest unusable: {e}"))
                })?;
                self.next_lsn = lsn + 1;
                captured_through = Some(lsn);
                manifest
            }
            None => {
                let root = pager.root_manifest()?;
                pager
                    .set_head_to(root)
                    .map_err(|e| WalError::Recovery(format!("root manifest unusable: {e}")))?;
                root
            }
        };

        let mut pending: TxnWrites = BTreeMap::new();

        for segment in Self::segments_in(&*self.vfs, &self.dir)? {
            let path = Self::segment_path(&self.dir, segment);
            let bytes = self.vfs.read(&path).map_err(|e| WalError::io(&path, e))?;

            let mut offset = 0usize;
            loop {
                match Record::decode(&bytes[offset..]) {
                    ReadOutcome::Torn => {
                        // The end of the log. Either we have run out of bytes cleanly, or we
                        // crashed mid-write and the tail is garbage.
                        if offset < bytes.len() {
                            recovery.torn_tail = true;
                            // Cut the log back to the last good record. If we left the partial
                            // bytes there, the next append would land *after* them, and replay —
                            // which stops at the first bad record — would never see it again. The
                            // transaction would be durable and invisible: a lost commit.
                            self.vfs
                                .truncate(&path, offset as u64)
                                .map_err(|e| WalError::io(&path, e))?;
                        }
                        self.current = segment;
                        self.current_bytes = offset as u64;
                        break;
                    }
                    ReadOutcome::Record { record, consumed } => {
                        offset += consumed;
                        self.next_lsn = self.next_lsn.max(record.lsn + 1);

                        // Already captured by the checkpoint. The bytes are still on disk (a
                        // checkpoint only deletes whole sealed segments), but the transaction has
                        // already happened as far as the manifest is concerned.
                        if captured_through.is_some_and(|cp| record.lsn <= cp) {
                            continue;
                        }
                        recovery.records_replayed += 1;

                        match record.kind {
                            RecordKind::Write { page_no, page } => {
                                pending.insert(page_no, page);
                            }

                            RecordKind::Commit {
                                manifest,
                                created_at_ms,
                            } => {
                                let base = head;

                                let derived = pager
                                    .derive_next(base, &pending, created_at_ms)
                                    .map_err(|e| {
                                        WalError::Recovery(format!(
                                            "replaying commit at lsn {}: {e}",
                                            record.lsn
                                        ))
                                    })?;

                                match derived {
                                    Some((next, id)) if id == manifest => {
                                        pager.install(&next).map_err(|e| {
                                            WalError::Recovery(format!(
                                                "installing manifest at lsn {}: {e}",
                                                record.lsn
                                            ))
                                        })?;
                                        head = id;
                                        recovery.committed_txns += 1;
                                        recovery.durable_lsn = record.lsn;
                                    }
                                    // Replay produced a *different* database than the one that was
                                    // committed. That is not something to paper over: it means the
                                    // log and the manifest disagree, and installing either one
                                    // would be a guess about which is real. Stop, loudly.
                                    Some((_, id)) => {
                                        return Err(WalError::ReplayDiverged {
                                            lsn: record.lsn,
                                            expected: manifest.to_hex(),
                                            actual: id.to_hex(),
                                        })
                                    }
                                    // The transaction is a no-op against this base — which happens
                                    // when it is replayed a second time onto a head that already
                                    // has it. Idempotence, working as intended.
                                    None => {
                                        head = base;
                                        recovery.durable_lsn = record.lsn;
                                    }
                                }
                                pending.clear();
                            }

                            RecordKind::Checkpoint { manifest } => {
                                pager.set_head_to(manifest).map_err(|e| {
                                    WalError::Recovery(format!(
                                        "checkpoint at lsn {} unusable: {e}",
                                        record.lsn
                                    ))
                                })?;
                                head = manifest;
                                recovery.durable_lsn = record.lsn;
                                pending.clear();
                            }
                        }
                    }
                }
            }
        }

        // Writes staged by a transaction whose commit record never landed. The pages are in the
        // CAS, durable and unreferenced — garbage, and GC's problem. They are emphatically not a
        // transaction, and pretending otherwise would be inventing a commit the caller never made.
        recovery.uncommitted_writes_discarded = pending.len() as u64;

        pager
            .set_head_to(head)
            .map_err(|e| WalError::Recovery(format!("final head unusable: {e}")))?;
        Ok(recovery)
    }

    /// Read the checkpoint marker, if one is durable.
    fn read_checkpoint(&self) -> Result<Option<(ManifestId, Lsn)>> {
        let path = self.checkpoint_path();
        if !self.vfs.exists(&path) {
            return Ok(None);
        }
        let bytes = self.vfs.read(&path).map_err(|e| WalError::io(&path, e))?;

        match Record::decode(&bytes) {
            ReadOutcome::Record { record, .. } => match record.kind {
                RecordKind::Checkpoint { manifest } => Ok(Some((manifest, record.lsn))),
                // A checkpoint file holding something other than a checkpoint is nonsense. Ignore
                // it and replay from the start: slower, and certainly correct.
                _ => Ok(None),
            },
            // A torn checkpoint marker means we crashed while writing it. The log behind it is
            // still complete, so replaying from the beginning gets the same answer.
            ReadOutcome::Torn => Ok(None),
        }
    }
}
