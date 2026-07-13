//! WAL records and their on-disk framing.
//!
//! # The frame
//!
//! ```text
//! ┌────────────┬────────────┬──────────────────────┐
//! │ len: u32   │ crc32c:u32 │ payload: len bytes   │
//! └────────────┴────────────┴──────────────────────┘
//!      LE           LE          bincode(Record)
//! ```
//!
//! The CRC covers the payload. That is the whole crash-safety mechanism at this level: a record
//! that was half-written when the power failed will not verify, and recovery stops there.
//!
//! # What is *not* in here
//!
//! **Page bytes.** The WAL never contains page contents. Pages are already durable in the CAS
//! before any record referencing them is written (docs/02 §3.1), so the log only needs to record
//! *ordering* — which pages became which logical pages, and when that became true.
//!
//! That keeps the log small (a commit of a hundred pages is a few kilobytes, not several
//! megabytes), which keeps the fsync fast, which is the single thing that determines how many
//! transactions per second this engine can commit.

use crate::error::{Result, WalError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use substrate_pager::{LogicalPageNo, ManifestId, PageId};

/// A log sequence number. Monotonic, never reused, never reset.
pub type Lsn = u64;

/// The maximum size of a single record's payload.
///
/// A frame claiming a payload larger than this is corrupt, not merely unexpected — and treating
/// it as corrupt is what stops a garbage length prefix from making us allocate several gigabytes
/// during recovery. Recovery must survive hostile bytes, not just unlucky ones.
pub const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// The size of the frame header: `len` + `crc32c`.
pub const FRAME_HEADER_BYTES: usize = 8;

/// What a record says.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordKind {
    /// One logical page of a transaction that has not committed yet.
    ///
    /// `page` is `None` when the logical page is being removed.
    Write {
        /// The logical page being written.
        page_no: LogicalPageNo,
        /// The content now at that page — already durable in the CAS. `None` removes the page.
        page: Option<PageId>,
    },

    /// **The commit point.** Everything logged since the last commit is now durable, together.
    ///
    /// Carries the manifest the transaction produces, and the timestamp that went into it.
    /// Recovery re-derives the manifest from the preceding `Write` records and **checks that it
    /// matches** — so a replay that would produce a different database than the one that was
    /// committed is caught, rather than silently installed.
    ///
    /// Carrying `created_at_ms` in the record (rather than reading the clock during replay) is
    /// what makes replay deterministic: the same log always yields byte-identical manifests, no
    /// matter when it is replayed.
    Commit {
        /// The manifest this transaction produces.
        manifest: ManifestId,
        /// The wall-clock time baked into that manifest.
        created_at_ms: u64,
    },

    /// A durable marker that everything up to this LSN is captured by `manifest`.
    ///
    /// Recovery starts here instead of at the beginning of time, and segments entirely behind a
    /// checkpoint can be deleted.
    Checkpoint {
        /// The manifest that captures all history up to this point.
        manifest: ManifestId,
    },
}

/// One record in the log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// This record's sequence number.
    pub lsn: Lsn,
    /// What it says.
    pub kind: RecordKind,
}

impl Record {
    /// Frame this record for the log: `len | crc32c | payload`.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let payload = bincode::serialize(self).map_err(|source| WalError::Codec {
            op: "encode",
            source,
        })?;

        if payload.len() > MAX_RECORD_BYTES {
            return Err(WalError::RecordTooLarge {
                actual: payload.len(),
                max: MAX_RECORD_BYTES,
            });
        }

        let crc = crc32c::crc32c(&payload);
        let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);
        Ok(frame)
    }

    /// Try to read one record from the front of `bytes`.
    ///
    /// Returns the record and how many bytes it consumed, or [`ReadOutcome::Torn`] if the bytes
    /// end mid-record or fail their CRC.
    ///
    /// **A torn record is not an error.** It is the expected shape of a crash: the log's last
    /// write did not finish. Recovery treats it as the end of the log, truncates there, and moves
    /// on. Only a record that is *complete and self-consistent* is allowed to have happened.
    pub fn decode(bytes: &[u8]) -> ReadOutcome {
        if bytes.len() < FRAME_HEADER_BYTES {
            return ReadOutcome::Torn;
        }

        let mut len_buf = [0u8; 4];
        let mut crc_buf = [0u8; 4];
        len_buf.copy_from_slice(&bytes[0..4]);
        crc_buf.copy_from_slice(&bytes[4..8]);
        let len = u32::from_le_bytes(len_buf) as usize;
        let expected_crc = u32::from_le_bytes(crc_buf);

        // A length prefix is untrusted input. Garbage here must not become a huge allocation.
        if len == 0 || len > MAX_RECORD_BYTES {
            return ReadOutcome::Torn;
        }

        let end = FRAME_HEADER_BYTES + len;
        let Some(payload) = bytes.get(FRAME_HEADER_BYTES..end) else {
            return ReadOutcome::Torn; // the record was cut off by the crash
        };

        if crc32c::crc32c(payload) != expected_crc {
            return ReadOutcome::Torn; // the bytes are there, but they are not what was written
        }

        match bincode::deserialize::<Record>(payload) {
            Ok(record) => ReadOutcome::Record {
                record,
                consumed: end,
            },
            // The CRC passed but the payload will not decode. That is not a torn write — a torn
            // write would fail the CRC. It means the bytes were written by a different version of
            // this format, and guessing would be worse than stopping.
            Err(_) => ReadOutcome::Torn,
        }
    }
}

/// The result of trying to read a record.
#[derive(Debug)]
pub enum ReadOutcome {
    /// A complete, CRC-verified record.
    Record {
        /// The record.
        record: Record,
        /// How many bytes it occupied, including the frame header.
        consumed: usize,
    },
    /// The bytes end mid-record, or the record does not verify. This is the end of the log.
    Torn,
}

/// The writes of one transaction, keyed by logical page. `None` removes the page.
pub type TxnWrites = BTreeMap<LogicalPageNo, Option<PageId>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> Record {
        Record {
            lsn: 42,
            kind: RecordKind::Commit {
                manifest: ManifestId::from_bytes([7; 32]),
                created_at_ms: 1_700_000_000_000,
            },
        }
    }

    #[test]
    fn round_trips() -> Result<()> {
        let original = record();
        let frame = original.encode()?;
        match Record::decode(&frame) {
            ReadOutcome::Record { record, consumed } => {
                assert_eq!(record, original);
                assert_eq!(consumed, frame.len());
            }
            ReadOutcome::Torn => panic!("a record we just wrote must decode"),
        }
        Ok(())
    }

    #[test]
    fn encoding_is_deterministic() -> Result<()> {
        // Deterministic replay stands on this. If the same record ever encodes differently, the
        // same log can replay to two different databases, and recovery stops being verifiable.
        let r = record();
        let first = r.encode()?;
        for _ in 0..64 {
            assert_eq!(r.encode()?, first);
        }
        Ok(())
    }

    #[test]
    fn a_record_cut_off_anywhere_is_torn_not_a_panic() -> Result<()> {
        // This is the crash, in miniature: the power failed partway through the write. Every
        // possible cut must be survivable, at every single byte.
        let frame = record().encode()?;
        for cut in 0..frame.len() {
            assert!(
                matches!(Record::decode(&frame[..cut]), ReadOutcome::Torn),
                "a record cut at byte {cut} must read as torn"
            );
        }
        Ok(())
    }

    #[test]
    fn a_single_flipped_bit_is_caught_by_the_crc() -> Result<()> {
        let frame = record().encode()?;
        for byte in FRAME_HEADER_BYTES..frame.len() {
            for bit in 0..8 {
                let mut corrupted = frame.clone();
                corrupted[byte] ^= 1 << bit;
                assert!(
                    matches!(Record::decode(&corrupted), ReadOutcome::Torn),
                    "a bit flip at byte {byte} bit {bit} slipped past the CRC"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn a_hostile_length_prefix_does_not_allocate_the_universe() {
        // A garbage length must be rejected on sight. Recovery reads bytes it does not trust,
        // and "allocate whatever the file says" is how a corrupt log becomes an OOM kill.
        let mut frame = vec![0u8; FRAME_HEADER_BYTES + 4];
        frame[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(Record::decode(&frame), ReadOutcome::Torn));

        // ...and so must a zero length.
        frame[0..4].copy_from_slice(&0u32.to_le_bytes());
        assert!(matches!(Record::decode(&frame), ReadOutcome::Torn));
    }

    #[test]
    fn trailing_bytes_after_a_record_are_left_for_the_next_read() -> Result<()> {
        let mut buf = record().encode()?;
        let first_len = buf.len();
        buf.extend_from_slice(&record().encode()?);

        match Record::decode(&buf) {
            ReadOutcome::Record { consumed, .. } => assert_eq!(consumed, first_len),
            ReadOutcome::Torn => panic!("the first record is intact"),
        }
        Ok(())
    }
}
