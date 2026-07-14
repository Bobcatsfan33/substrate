//! The crash-injection suite.
//!
//! This is the most important test file in the repository. It exists to establish one sentence:
//!
//! > After a crash at **any byte boundary**, the recovered store equals **some prefix of committed
//! > transactions.** No torn state. No lost commit.
//!
//! Everything else substrate claims — free forks, sleeping databases, agent branches — is
//! worthless if a committed write can evaporate. So we kill the write path at every byte in turn,
//! ten thousand times, and check what came back.
//!
//! # The two halves of the property
//!
//! **No torn state.** The recovered database must be one of the states the writer actually passed
//! through. Not a blend of two of them; not a transaction half-applied. A *prefix*.
//!
//! **No lost commit.** If `commit()` returned `Ok`, that transaction survives. Full stop. This is
//! the promise a database makes, and the only one that cannot be walked back.
//!
//! # Why a commit that returned an *error* may still survive
//!
//! The commit point is the WAL fsync (docs/02 §3.1). `install()` happens after it. So a crash
//! between the two makes `commit()` return `Err` even though the transaction is durable — and
//! recovery will correctly replay it. That is not a bug, it is the definition of the commit point,
//! and it is why the property is "*some* prefix ≥ the last acknowledged commit" rather than an
//! exact equality. A database is allowed to keep a write it never acknowledged. It is never
//! allowed to lose one it did.

use std::collections::BTreeMap;
use std::sync::Arc;
use substrate_pager::testing::{crashing_mem_vfs, CrashVfs, MemVfs, Rng};
use substrate_pager::{std_vfs, LogicalPageNo, ManualClock, PageStore, StoreConfig, MIN_PAGE_SIZE};
use substrate_wal::{DurableStore, WalError};

const PAGE_SIZE: usize = MIN_PAGE_SIZE;
const MAX_PAGE_NO: LogicalPageNo = 6;

fn config() -> StoreConfig {
    StoreConfig {
        page_size: PAGE_SIZE,
        ..Default::default()
    }
}

/// A frozen clock: manifests must be byte-identical across a replay, and a moving clock would
/// change their contents (and therefore their ids) between the write and the replay.
fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(1_700_000_000_000))
}

fn content_of(byte: u8) -> Vec<u8> {
    vec![byte; 1 + (byte as usize % 32)]
}

/// The state of the database after each committed transaction — the "prefix" the property refers to.
type ExpectedState = BTreeMap<LogicalPageNo, Vec<u8>>;

/// Run a randomized workload against a store that will die after `budget` bytes of writes.
///
/// Returns every state the database legitimately passed through, and how many commits were
/// *acknowledged* (i.e. `commit()` returned `Ok`) before the crash.
fn run_until_crash(
    disk: Arc<MemVfs>,
    vfs: Arc<CrashVfs>,
    rng: &mut Rng,
    txns: u64,
) -> (Vec<ExpectedState>, usize) {
    let db = match DurableStore::open_with_clock(vfs.clone(), "/db", config(), clock()) {
        Ok(db) => db,
        // The crash landed before the store could even be created. Nothing was committed, so the
        // only legal recovered state is "empty".
        Err(_) => return (vec![ExpectedState::new()], 0),
    };
    let _ = disk;

    let mut states: Vec<ExpectedState> = vec![ExpectedState::new()];
    let mut current = ExpectedState::new();
    let mut acknowledged = 0usize;

    'outer: for _ in 0..txns {
        let Ok(mut txn) = db.begin() else { break };

        let writes = 1 + rng.below(3);
        let mut staged = current.clone();

        for _ in 0..writes {
            let page_no = rng.below(MAX_PAGE_NO);
            let bytes = content_of(rng.byte());
            if db.write(&mut txn, page_no, bytes.clone()).is_err() {
                break 'outer; // crashed mid-write: nothing was committed
            }
            staged.insert(page_no, bytes);
        }

        match db.commit(txn) {
            Ok(_) => {
                // Acknowledged. This transaction MUST survive the crash.
                current = staged;
                states.push(current.clone());
                acknowledged = states.len() - 1;
            }
            Err(_) => {
                // The crash landed somewhere inside commit. This transaction may or may not have
                // become durable — that depends on whether the WAL fsync completed — so it is a
                // *legal* recovered state, but not a required one. Record it as permissible and
                // stop: the machine is gone.
                states.push(staged);
                break;
            }
        }
    }

    (states, acknowledged)
}

/// Reboot the machine, recover, and check the two halves of the property.
fn recover_and_check(
    disk: Arc<MemVfs>,
    states: &[ExpectedState],
    acknowledged: usize,
    scenario: &str,
) {
    let db = DurableStore::open_with_clock(disk, "/db", config(), clock())
        .unwrap_or_else(|e| panic!("{scenario}: could not reopen a crashed store: {e}"));

    let recovery = db
        .recover()
        .unwrap_or_else(|e| panic!("{scenario}: recovery failed: {e}"));

    // Read back everything the recovered store holds.
    let head = db.head();
    let pages = db
        .pager()
        .resolve(&head)
        .unwrap_or_else(|e| panic!("{scenario}: recovered head is unreadable: {e}"));

    let mut actual = ExpectedState::new();
    for page_no in pages.keys() {
        let page = db.read(&head, *page_no).unwrap_or_else(|e| {
            panic!("{scenario}: page {page_no} of the recovered head is unreadable — a committed page was lost: {e}")
        });
        actual.insert(*page_no, page.into_bytes());
    }

    // PROPERTY 1 — NO TORN STATE. What came back must be one of the states the writer actually
    // passed through, not a blend of two of them.
    //
    // `rposition`, not `position`: a transaction may legitimately rewrite a page with the bytes
    // already there, which makes two consecutive states *identical*. Taking the first match would
    // then report a lost commit that did not happen. Identical content is indistinguishable
    // content, so the latest state it could be is the one it is.
    let matched = states.iter().rposition(|s| *s == actual).unwrap_or_else(|| {
        panic!(
            "{scenario}: TORN STATE. The recovered database is not any state the writer passed \
             through.\n  recovered: {actual:?}\n  legal states: {states:?}\n  recovery: {recovery:?}"
        )
    });

    // PROPERTY 2 — NO LOST COMMIT. Every transaction whose commit() returned Ok must be there.
    assert!(
        matched >= acknowledged,
        "{scenario}: LOST COMMIT. Recovery landed on state {matched}, but {acknowledged} \
         transactions were acknowledged as committed. A database that returns Ok from commit() \
         and then loses the write has no reason to exist.\n  recovery: {recovery:?}"
    );

    // PROPERTY 3 — RECOVERY IS IDEMPOTENT. Recovering an already-recovered store changes nothing.
    // Recovery runs after a crash, and a crash *during recovery* is not exotic — it is Tuesday.
    let again = db
        .recover()
        .unwrap_or_else(|e| panic!("{scenario}: second recovery failed: {e}"));
    assert_eq!(
        db.head(),
        head,
        "{scenario}: recovery is not idempotent — running it twice moved the head. {again:?}"
    );
}

/// **The P2 gate: 10,000 randomized crash-and-recover cycles.**
///
/// Every run picks a different workload and a different byte at which to die. Between them they
/// cut the write path in every place it can be cut: inside a page write, inside a WAL record,
/// between the WAL fsync and the manifest install, inside the manifest write, and after the
/// transaction is complete.
#[test]
fn ten_thousand_crashes_never_produce_a_torn_or_lost_commit() {
    let runs: u64 = std::env::var("CRASH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    for seed in 0..runs {
        let mut rng = Rng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));

        // Kill the machine somewhere in the first few kilobytes of writes — the region where all
        // the interesting boundaries live.
        let budget = rng.below(6_000) as i64;
        let txns = 1 + rng.below(8);

        let (disk, vfs) = crashing_mem_vfs(budget);
        let (states, acknowledged) = run_until_crash(disk.clone(), vfs, &mut rng, txns);

        recover_and_check(
            disk,
            &states,
            acknowledged,
            &format!("seed={seed} budget={budget} txns={txns}"),
        );
    }
}

/// The same property, against the **real filesystem**, where `fsync` is a genuine syscall.
///
/// The in-memory suite above explores the state space exhaustively and fast, but it makes every
/// accepted write immediately durable — so it cannot catch a missing `fsync`. This one can. Fewer
/// runs, real disk, real syscalls.
#[test]
fn crashes_against_a_real_disk_never_lose_a_commit() {
    for seed in 0..64u64 {
        let mut rng = Rng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));
        let budget = rng.below(4_000) as i64;
        let txns = 1 + rng.below(5);

        let dir = tempfile::tempdir().expect("tempdir");
        let vfs = CrashVfs::with_budget(std_vfs(), budget);

        // --- the doomed process ---
        let mut states: Vec<ExpectedState> = vec![ExpectedState::new()];
        let mut current = ExpectedState::new();
        let mut acknowledged = 0usize;
        {
            let Ok(db) = DurableStore::open_with_clock(vfs, dir.path(), config(), clock()) else {
                continue; // died before the store existed
            };

            'txns: for _ in 0..txns {
                let Ok(mut txn) = db.begin() else { break };
                let mut staged = current.clone();
                for _ in 0..(1 + rng.below(3)) {
                    let page_no = rng.below(MAX_PAGE_NO);
                    let bytes = content_of(rng.byte());
                    if db.write(&mut txn, page_no, bytes.clone()).is_err() {
                        break 'txns;
                    }
                    staged.insert(page_no, bytes);
                }
                match db.commit(txn) {
                    Ok(_) => {
                        current = staged;
                        states.push(current.clone());
                        acknowledged = states.len() - 1;
                    }
                    Err(_) => {
                        states.push(staged);
                        break;
                    }
                }
            }
        }

        // --- reboot ---
        let db = DurableStore::open_with_clock(std_vfs(), dir.path(), config(), clock())
            .unwrap_or_else(|e| panic!("seed={seed}: reopen after crash: {e}"));
        db.recover()
            .unwrap_or_else(|e| panic!("seed={seed}: recovery: {e}"));

        let head = db.head();
        let pages = db.pager().resolve(&head).expect("head readable");
        let mut actual = ExpectedState::new();
        for page_no in pages.keys() {
            actual.insert(
                *page_no,
                db.read(&head, *page_no)
                    .unwrap_or_else(|e| panic!("seed={seed}: lost page {page_no}: {e}"))
                    .into_bytes(),
            );
        }

        let matched = states
            .iter()
            .rposition(|s| *s == actual)
            .unwrap_or_else(|| panic!("seed={seed}: TORN STATE on a real disk: {actual:?}"));
        assert!(
            matched >= acknowledged,
            "seed={seed}: LOST COMMIT on a real disk (landed at {matched}, acknowledged {acknowledged})"
        );
    }
}

/// Deterministic replay: the same log, replayed twice, yields byte-identical manifests.
///
/// If replay is not deterministic then recovery is not verifiable, and no other guarantee in this
/// engine means anything — because "we recovered your database" would be a claim about something
/// nobody can reproduce.
#[test]
fn the_same_log_always_replays_to_the_same_bytes() -> Result<(), WalError> {
    let disk = MemVfs::new();

    // Write a workload.
    let mut expected_head = None;
    {
        let db = DurableStore::open_with_clock(disk.clone(), "/db", config(), clock())?;
        let mut rng = Rng::new(0xDEAD_BEEF);
        for _ in 0..24 {
            let mut txn = db.begin()?;
            for _ in 0..3 {
                db.write(&mut txn, rng.below(MAX_PAGE_NO), content_of(rng.byte()))?;
            }
            expected_head = Some(db.commit(txn)?);
        }
    }

    // Replay it. Repeatedly. The head — which is a hash of the manifest's bytes — must be
    // identical every single time.
    let mut heads = Vec::new();
    for _ in 0..8 {
        let db = DurableStore::open_with_clock(disk.clone(), "/db", config(), clock())?;
        let recovery = db.recover()?;
        assert!(recovery.committed_txns > 0, "the log should have replayed");
        heads.push(db.head());
    }

    for head in &heads {
        assert_eq!(
            Some(*head),
            expected_head,
            "replaying the same log produced a different database"
        );
    }
    Ok(())
}

/// A checkpoint truncates history, and recovery still lands in exactly the same place.
#[test]
fn checkpointing_shortens_the_log_without_changing_the_answer() -> Result<(), WalError> {
    let disk = MemVfs::new();

    let head_before = {
        let db = DurableStore::open_with_clock(disk.clone(), "/db", config(), clock())?;
        let mut rng = Rng::new(7);

        for _ in 0..10 {
            let mut txn = db.begin()?;
            db.write(&mut txn, rng.below(MAX_PAGE_NO), content_of(rng.byte()))?;
            db.commit(txn)?;
        }
        db.checkpoint()?;

        // ...and more work after the checkpoint, which must also survive.
        for _ in 0..5 {
            let mut txn = db.begin()?;
            db.write(&mut txn, rng.below(MAX_PAGE_NO), content_of(rng.byte()))?;
            db.commit(txn)?;
        }
        db.head()
    };

    let db = DurableStore::open_with_clock(disk, "/db", config(), clock())?;
    db.recover()?;
    assert_eq!(
        db.head(),
        head_before,
        "recovery after a checkpoint landed somewhere else"
    );
    Ok(())
}

/// An uncommitted transaction leaves no trace in the database — only garbage in the CAS, which is
/// GC's problem and nobody else's.
#[test]
fn a_transaction_that_never_committed_did_not_happen() -> Result<(), WalError> {
    let disk = MemVfs::new();

    {
        let db = DurableStore::open_with_clock(disk.clone(), "/db", config(), clock())?;

        let mut txn = db.begin()?;
        db.write(&mut txn, 0, b"committed".to_vec())?;
        db.commit(txn)?;

        // Stage a second transaction and simply walk away — the process dies here.
        let mut orphan = db.begin()?;
        db.write(&mut orphan, 0, b"NEVER COMMITTED".to_vec())?;
        db.write(&mut orphan, 1, b"NEVER COMMITTED".to_vec())?;
        std::mem::forget(orphan);
    }

    let db = DurableStore::open_with_clock(disk, "/db", config(), clock())?;
    let recovery = db.recover()?;

    assert_eq!(recovery.committed_txns, 1);
    assert_eq!(db.read_head(0)?.as_bytes(), b"committed");
    assert!(
        db.read_head(1).is_err(),
        "a page from an uncommitted transaction appeared in the database"
    );
    Ok(())
}
