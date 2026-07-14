//! Branch trees at depth: overlay chains, collapse, merge bases, and deep forks.
//!
//! P4's job is to make forks-of-forks-of-forks work *and stay fast*. Three things have to be true:
//!
//! 1. A deep chain of overlays reads back exactly the same database a flat manifest would.
//! 2. The chain never grows without bound — it collapses at [`MAX_OVERLAY_DEPTH`].
//! 3. GC understands that an overlay is **unreadable without its base**, and never collects one.
//!
//! Number three is the one that would destroy a customer's data, and it is the one that is easy to
//! get wrong, because a manifest has *two* different backward edges — its history `parent` and its
//! storage `overlay_base` — and they come apart exactly where you stop paying attention.

use std::collections::BTreeMap;
use substrate_pager::{
    LogicalPageNo, PageStore, Pager, PagerError, StoreConfig, MAX_OVERLAY_DEPTH, MIN_PAGE_SIZE,
};

fn config() -> StoreConfig {
    StoreConfig {
        page_size: MIN_PAGE_SIZE,
        ..Default::default()
    }
}

fn write(db: &dyn PageStore, page_no: LogicalPageNo, bytes: &[u8]) -> Result<(), PagerError> {
    let mut txn = db.begin()?;
    db.write(&mut txn, page_no, bytes.to_vec())?;
    db.commit(txn)?;
    Ok(())
}

/// A chain of commits reads back correctly at every depth, and collapses at the limit.
#[test]
fn overlay_chains_stay_correct_and_collapse_at_the_limit() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    // Seed a flat base with 20 pages.
    let mut txn = db.begin()?;
    for page_no in 0..20u64 {
        db.write(&mut txn, page_no, format!("base-{page_no}").into_bytes())?;
    }
    db.commit(txn)?;
    assert!(db.manifest(&db.head())?.is_flat() || db.manifest(&db.head())?.depth() == 1);

    // Now commit one page at a time, well past the depth limit, and check the whole database after
    // every single commit. The bug this catches is an overlay walk that stops early, or that applies
    // its changes in the wrong order and serves stale content.
    let mut expected: BTreeMap<LogicalPageNo, Vec<u8>> = (0..20u64)
        .map(|n| (n, format!("base-{n}").into_bytes()))
        .collect();

    let mut depths = Vec::new();

    for round in 0..(MAX_OVERLAY_DEPTH * 3) {
        let page_no = (round as u64) % 20;
        let bytes = format!("round-{round}").into_bytes();
        write(&db, page_no, &bytes)?;
        expected.insert(page_no, bytes);

        let manifest = db.manifest(&db.head())?;
        depths.push(manifest.depth());

        assert!(
            manifest.depth() <= MAX_OVERLAY_DEPTH,
            "the overlay chain grew to depth {} — read amplification is now unbounded",
            manifest.depth()
        );

        // The whole database, every time.
        let resolved = db.resolve(&db.head())?;
        assert_eq!(
            resolved.len(),
            expected.len(),
            "page count drifted at round {round}"
        );
        for (page_no, want) in &expected {
            assert_eq!(
                db.read_head(*page_no)?.as_bytes(),
                want.as_slice(),
                "page {page_no} read back wrong at round {round} (depth {})",
                manifest.depth()
            );
        }
    }

    // And it really did collapse — the depth came back down more than once.
    let collapses = depths.windows(2).filter(|w| w[1] < w[0]).count();
    assert!(
        collapses >= 2,
        "the chain never collapsed; depths were {depths:?}"
    );
    Ok(())
}

/// A removal inside an overlay must not be undone by the base underneath it.
///
/// This is the tombstone bug: if the walk treats "this overlay doesn't mention the page" and "this
/// overlay deleted the page" as the same thing, the base's copy comes back from the dead.
#[test]
fn a_deleted_page_stays_deleted_through_a_deep_chain() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    write(&db, 0, b"alive")?;
    write(&db, 1, b"also alive")?;

    let mut txn = db.begin()?;
    db.remove(&mut txn, 0)?;
    db.commit(txn)?;

    // Pile more overlays on top. The deletion must survive every one of them, including the
    // flattening that eventually happens.
    for round in 0..(MAX_OVERLAY_DEPTH * 2) {
        write(&db, 1, format!("round-{round}").into_bytes().as_slice())?;

        assert!(
            db.read_head(0).is_err(),
            "page 0 was resurrected from under its own tombstone at round {round}"
        );
        assert!(!db.resolve(&db.head())?.contains_key(&0));
    }
    Ok(())
}

/// **GC must never collect a manifest's overlay base.** An overlay without its base is unreadable.
#[test]
fn gc_never_collects_an_overlay_base() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    let mut txn = db.begin()?;
    for page_no in 0..10u64 {
        db.write(&mut txn, page_no, format!("v0-{page_no}").into_bytes())?;
    }
    db.commit(txn)?;

    // A chain of overlays, each touching one page. The head depends on every one of them for the
    // pages it did *not* touch.
    for round in 0..(MAX_OVERLAY_DEPTH - 1) {
        write(
            &db,
            round as u64,
            format!("v1-{round}").into_bytes().as_slice(),
        )?;
    }

    let head = db.head();
    let manifest = db.manifest(&head)?;
    assert!(!manifest.is_flat(), "we want an overlay head for this test");
    assert!(manifest.depth() > 1, "and a chain worth collecting");

    // GC with ONLY the head as a root. Everything else in the chain has no branch pointing at it —
    // it is reachable only through the head's overlay base, which is the whole trap.
    let stats = db.gc(&[head])?;

    // Every page of the head must still be readable. If GC followed only `parent` and not
    // `overlay_base`, this is where the database evaporates.
    let pages = db.resolve(&head)?;
    assert_eq!(pages.len(), 10);
    for page_no in pages.keys() {
        db.read(&head, *page_no).map_err(|e| {
            panic!(
                "GC collected an overlay base: page {page_no} of the head is gone ({e}). {stats}"
            )
        })?;
    }
    Ok(())
}

/// The merge base of two branches is the point they last agreed.
#[test]
fn merge_base_finds_the_fork_point() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    write(&db, 0, b"common history")?;
    write(&db, 1, b"still common")?;
    let fork_point = db.head();

    // Branch A carries on.
    write(&db, 2, b"only on A")?;
    write(&db, 3, b"also only on A")?;
    let a = db.head();

    // Branch B forks from the fork point and diverges.
    let branch_b = db.fork(&fork_point)?;
    write(&*branch_b, 2, b"only on B")?;
    let b = branch_b.head();

    assert_eq!(
        db.merge_base(&a, &b)?,
        Some(fork_point),
        "the merge base must be the last manifest both branches share"
    );
    // Symmetric.
    assert_eq!(db.merge_base(&b, &a)?, Some(fork_point));
    // A branch's merge base with itself is itself.
    assert_eq!(db.merge_base(&a, &a)?, Some(a));
    // An ancestor's merge base with its descendant is the ancestor.
    assert_eq!(db.merge_base(&fork_point, &a)?, Some(fork_point));
    Ok(())
}

/// The three-way diff over a real fork classifies exactly what each side did.
#[test]
fn three_way_diff_over_a_real_fork() -> Result<(), PagerError> {
    use substrate_pager::PageClass;

    let db = Pager::in_memory(config())?;

    let mut txn = db.begin()?;
    for page_no in 0..4u64 {
        db.write(&mut txn, page_no, b"base".to_vec())?;
    }
    let base = db.commit(txn)?;

    // A edits page 1, and page 3.
    write(&db, 1, b"A")?;
    write(&db, 3, b"conflicting-A")?;
    let a = db.head();

    // B forks from base, edits page 2, and page 3 differently.
    let branch_b = db.fork(&base)?;
    write(&*branch_b, 2, b"B")?;
    write(&*branch_b, 3, b"conflicting-B")?;
    let b = branch_b.head();

    let merge_base = db
        .merge_base(&a, &b)?
        .ok_or(PagerError::MalformedId("no merge base".into()))?;
    assert_eq!(merge_base, base);

    let diff = db.diff3(&merge_base, &a, &b)?;
    let classes: BTreeMap<_, _> = diff.entries.iter().copied().collect();

    assert!(
        !classes.contains_key(&0),
        "an untouched page must not appear"
    );
    assert_eq!(classes[&1], PageClass::AOnly);
    assert_eq!(classes[&2], PageClass::BOnly);
    assert!(matches!(classes[&3], PageClass::Conflict { .. }));
    assert_eq!(diff.conflicts().count(), 1);
    Ok(())
}

/// Forks of forks of forks, to real depth, all isolated from each other.
#[test]
fn deep_fork_trees_stay_isolated() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;
    write(&db, 0, b"root")?;

    // A chain of 12 nested forks, each writing its own marker at its own page.
    let mut stores: Vec<Box<dyn PageStore>> = Vec::new();
    let mut current = db.head();

    for level in 0..12u64 {
        let fork = db.fork(&current)?;
        write(
            &*fork,
            level + 1,
            format!("level-{level}").into_bytes().as_slice(),
        )?;
        current = fork.head();
        stores.push(fork);
    }

    // Each level sees its own marker and every marker beneath it — and NONE above it.
    for (level, store) in stores.iter().enumerate() {
        for below in 0..=level {
            assert_eq!(
                store.read_head(below as u64 + 1)?.as_bytes(),
                format!("level-{below}").as_bytes(),
                "level {level} lost the marker from level {below}"
            );
        }
        for above in (level + 1)..12 {
            assert!(
                store.read_head(above as u64 + 1).is_err(),
                "level {level} can see level {above}'s write — a fork leaked backwards"
            );
        }
    }

    // And the original base never saw any of it.
    assert_eq!(db.resolve(&db.head())?.len(), 1);
    Ok(())
}

/// Rewind is an O(1) pointer move, and the abandoned suffix stays readable until GC.
#[test]
fn rewind_abandons_without_destroying() -> Result<(), PagerError> {
    let db = Pager::in_memory(config())?;

    write(&db, 0, b"v1")?;
    let v1 = db.head();
    write(&db, 0, b"v2")?;
    let v2 = db.head();
    write(&db, 0, b"v3")?;
    let v3 = db.head();

    db.rewind(&v1)?;
    assert_eq!(db.read_head(0)?.as_bytes(), b"v1");

    // The abandoned suffix is still there. This is what makes "try three hypotheses and discard two"
    // auditable rather than merely cheap (docs/03 §3.1).
    assert_eq!(db.read(&v2, 0)?.as_bytes(), b"v2");
    assert_eq!(db.read(&v3, 0)?.as_bytes(), b"v3");

    // ...until GC, with only v1 live, sweeps it.
    let stats = db.gc(&[v1])?;
    assert!(
        stats.manifests_swept > 0,
        "the abandoned suffix should be collectable: {stats}"
    );
    assert!(db.manifest(&v3).is_err(), "v3 should be gone");
    assert_eq!(db.read_head(0)?.as_bytes(), b"v1", "and v1 is untouched");
    Ok(())
}
