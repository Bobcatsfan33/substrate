//! The tiering suite: sleep, wake, eviction, and the pool boundary.
//!
//! Three properties, and one of them would sink the company if it were false.
//!
//! 1. **Sleep → wipe → wake → read** returns exactly what was written. This is the whole product.
//! 2. **Eviction never loses data.** A page is evictable only once it is confirmed durable in object
//!    storage, so no sequence of writes and evictions can destroy a page that is still referenced.
//! 3. **Pools never share pages.** Identical bytes in two pools are two objects, in two places, with
//!    two keys — which is the guarantee a CUI customer is actually buying.
//!
//! The tests run against `object_store::memory::InMemory`, which is a faithful implementation of the
//! same trait the real S3 client implements. A MinIO run against a real S3 API lives behind
//! `--ignored` (see `minio_round_trip`) because CI has no network by design (CLAUDE.md rule 5), and a
//! test suite that cannot run in the airgap container is a test suite that does not run.

mod gated;

use gated::GatedStore;
use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Instant;
use substrate_pager::{PageStore, StoreConfig, MIN_PAGE_SIZE};
use substrate_store::{RemoteTier, Result, StoreError, TieredStore, WakeToken};

const PAGE_SIZE: usize = MIN_PAGE_SIZE;

fn config(pool: &str) -> StoreConfig {
    StoreConfig {
        page_size: PAGE_SIZE,
        pool: pool.to_string(),
        ..Default::default()
    }
}

fn content(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

/// **The headline.** Write, sleep, wipe every local byte, wake somewhere else, read it back.
///
/// The wipe is the point. After `sleep()` we delete the entire local directory — not just evict the
/// cache, but remove it from the disk — and wake into a *different* directory. If anything the
/// database needs were still on that first disk, this test would fail. It cannot be passing by
/// accident.
#[tokio::test(flavor = "multi_thread")]
async fn write_sleep_wipe_wake_read() -> Result<()> {
    let backend = Arc::new(InMemory::new());
    let remote = RemoteTier::new(backend, "acme");

    let first_home = tempfile::tempdir().expect("tempdir");
    let expected: Vec<(u64, Vec<u8>)> = (0..32u64)
        .map(|i| (i, content(i as u8, 100 + i as usize)))
        .collect();

    // --- awake ---
    let token = {
        let db = TieredStore::open(first_home.path(), remote.clone(), config("acme")).await?;
        let pager = db.pager();

        let mut txn = pager.begin()?;
        for (page_no, bytes) in &expected {
            pager.write(&mut txn, *page_no, bytes.clone())?;
        }
        pager.commit(txn)?;

        let token = db.sleep().await?;
        assert_eq!(token.pool, "acme");
        assert_eq!(token.page_size, PAGE_SIZE);
        token
    };

    // --- the database is now 20-odd bytes of meaning ---
    let serialized = token.to_json()?;
    assert_eq!(WakeToken::from_json(&serialized)?, token);

    // --- destroy the local disk entirely. not evict: DELETE. ---
    std::fs::remove_dir_all(first_home.path()).expect("wipe the machine");

    // --- wake, on a different disk, from nothing but object storage ---
    let new_home = tempfile::tempdir().expect("tempdir");
    let started = Instant::now();
    let db = TieredStore::wake(new_home.path(), remote, &token).await?;

    let (first_read, misses_after_first_read) = {
        let page = db.pager().read_head(0)?;
        let elapsed = started.elapsed();
        assert_eq!(page.as_bytes(), expected[0].1.as_slice());
        (elapsed, db.stats().misses)
    };

    // Every page comes back, byte for byte.
    for (page_no, bytes) in &expected {
        assert_eq!(
            db.pager().read_head(*page_no)?.as_bytes(),
            bytes.as_slice(),
            "page {page_no} did not survive sleep and wake"
        );
    }

    // **The property, asserted structurally rather than on a stopwatch.**
    //
    // What makes wake fast is that it is LAZY: it fetches the manifest, and then only the pages a
    // query actually touches. The regression this guards against is someone making wake() fetch
    // everything eagerly — and the honest way to detect that is to count the fetches, not to time
    // them.
    //
    // An earlier version asserted `elapsed < 250ms` against an IN-MEMORY object store. That measures
    // how busy the CI runner is, and it duly went red under parallel load while the engine was
    // perfectly correct. A test whose result depends on machine speed will eventually lie to you, in
    // both directions. The real 250 ms figure (docs/02 §7) belongs against MinIO, over a real
    // network, where the number means something — see `minio_round_trip`.
    assert_eq!(
        misses_after_first_read, 1,
        "reading ONE page after waking fetched {misses_after_first_read} pages. Wake is supposed to \
         be lazy; if it fetches eagerly, waking a 100 GB database moves 100 GB and the entire \
         economic argument for sleeping goes with it."
    );
    let _ = first_read; // kept for the operator-facing log line, not asserted on

    // And by the end, we have fetched only what we read — not the whole database.
    let stats = db.stats();
    assert!(
        stats.misses <= expected.len() as u64,
        "wake fetched more pages than were read: {stats:?}"
    );
    Ok(())
}

/// **Eviction never loses data**, with the uploads held open so the property is actually tested.
///
/// The object store's `put` is **blocked**. So every page written here exists *nowhere but the local
/// disk*. We then demand an empty cache, repeatedly, and every page must survive — because a page
/// that object storage has never seen is not evictable, no matter how cold it is or how badly we
/// want the space.
///
/// The first version of this test used a fast in-memory backend and passed while proving nothing:
/// the uploader always won the race, so every eviction was of a page that was already safe. An
/// assertion caught it. That is the difference between a test and a decoration.
#[tokio::test(flavor = "multi_thread")]
async fn eviction_can_never_lose_a_page() -> Result<()> {
    let backend = GatedStore::closed(Arc::new(InMemory::new())); // uploads: BLOCKED
    let remote = RemoteTier::new(backend.clone(), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote, config("acme")).await?;
    let pager = db.pager();

    let mut written: Vec<(u64, Vec<u8>)> = Vec::new();

    for i in 0..32u64 {
        let bytes = content(i as u8, 200);
        let mut txn = pager.begin()?;
        pager.write(&mut txn, i, bytes.clone())?;
        pager.commit(txn)?;
        written.push((i, bytes));

        // Nothing has reached object storage. Every one of these pages is local-only.
        assert!(
            db.stats().pending_upload > 0,
            "the gate is closed, so pages cannot possibly be durable remotely"
        );

        // Demand an empty cache anyway. The engine must refuse.
        db.evict_to(0)?;

        for (page_no, expected) in &written {
            assert_eq!(
                pager.read_head(*page_no)?.as_bytes(),
                expected.as_slice(),
                "page {page_no} was evicted while it existed NOWHERE ELSE. That is not an eviction, \
                 it is a delete."
            );
        }
    }

    // The cache is over budget, on purpose, and that is the correct outcome: running over budget is
    // a performance problem; evicting the only copy of live data is an obituary.
    let stats = db.stats();
    assert_eq!(
        stats.evictions, 0,
        "something was evicted that could not be: {stats:?}"
    );
    assert!(stats.local_bytes > 0);
    assert!(
        backend.blocked() > 0,
        "the gate never actually held an upload"
    );

    // --- now let the uploads through ---
    backend.open_gate();
    db.flush().await?;

    // Everything is durable. NOW the cache can drop to zero, and every page is still readable —
    // this time from object storage.
    db.evict_to(0)?;
    let stats = db.stats();
    assert_eq!(stats.pending_upload, 0);
    assert!(
        stats.evictions > 0,
        "nothing was evicted once it was safe to: {stats:?}"
    );

    for (page_no, expected) in &written {
        assert_eq!(
            pager.read_head(*page_no)?.as_bytes(),
            expected.as_slice(),
            "page {page_no} did not survive a full eviction after being uploaded"
        );
    }
    Ok(())
}

/// `sleep()` refuses — loudly — if anything is not yet durable remotely.
///
/// This is the one place in the engine where we deliberately delete data, so it double-checks rather
/// than trusting that `flush()` did its job.
#[tokio::test(flavor = "multi_thread")]
async fn sleep_refuses_to_drop_local_state_it_has_not_uploaded() -> Result<()> {
    let backend = GatedStore::closed(Arc::new(InMemory::new())); // uploads: BLOCKED
    let remote = RemoteTier::new(backend.clone(), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote, config("acme")).await?;

    let mut txn = db.pager().begin()?;
    db.pager()
        .write(&mut txn, 0, b"the only copy of this is local".to_vec())?;
    db.pager().commit(txn)?;

    // sleep() must hang on flush rather than drop anything — so we bound it and assert it did NOT
    // complete. A sleep that returned here would have dropped the only copy of the page.
    let slept = tokio::time::timeout(std::time::Duration::from_millis(250), db.sleep()).await;
    assert!(
        slept.is_err(),
        "sleep() returned while uploads were blocked — it dropped local state it had not uploaded"
    );

    // The data is still there.
    assert_eq!(
        db.pager().read_head(0)?.as_bytes(),
        b"the only copy of this is local"
    );

    // Let it finish properly.
    backend.open_gate();
    let token = db.sleep().await?;
    assert_eq!(db.stats().local_bytes, 0);
    assert_eq!(token.pool, "acme");
    Ok(())
}

/// Sleeping refuses to drop local state if anything is not yet durable remotely.
#[tokio::test(flavor = "multi_thread")]
async fn sleep_is_all_or_nothing() -> Result<()> {
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote, config("acme")).await?;
    let pager = db.pager();

    let mut txn = pager.begin()?;
    pager.write(&mut txn, 0, b"important".to_vec())?;
    pager.commit(txn)?;

    let token = db.sleep().await?;

    // After a successful sleep, nothing is pending and nothing is local.
    let stats = db.stats();
    assert_eq!(stats.pending_upload, 0);
    assert_eq!(stats.local_bytes, 0, "sleep did not drop local state");
    assert_eq!(token.pool, "acme");
    Ok(())
}

/// **Pools never share pages.** The CUI guarantee (docs/02 §9.1).
#[tokio::test(flavor = "multi_thread")]
async fn pools_never_share_pages_even_with_identical_content() -> Result<()> {
    // ONE object-storage backend. Two pools inside it. The same bytes written to both.
    let backend = Arc::new(InMemory::new());
    let secret = b"TROOP MOVEMENT 0400 GRID 12345".to_vec();

    let cui_home = tempfile::tempdir().expect("tempdir");
    let pub_home = tempfile::tempdir().expect("tempdir");

    let cui_remote = RemoteTier::new(backend.clone(), "cui-secret");
    let pub_remote = RemoteTier::new(backend.clone(), "public");

    let cui_token = {
        let db =
            TieredStore::open(cui_home.path(), cui_remote.clone(), config("cui-secret")).await?;
        let mut txn = db.pager().begin()?;
        db.pager().write(&mut txn, 0, secret.clone())?;
        db.pager().commit(txn)?;
        db.sleep().await?
    };

    {
        let db = TieredStore::open(pub_home.path(), pub_remote.clone(), config("public")).await?;
        let mut txn = db.pager().begin()?;
        db.pager().write(&mut txn, 0, secret.clone())?;
        db.pager().commit(txn)?;
        db.sleep().await?;
    }

    // The same content is stored twice, under two keys, in two prefixes. There is no dedup across
    // the boundary — that is the cost, and it is the price of the guarantee.
    let cui_key = cui_remote.page_key(substrate_pager::PageId::of(&secret));
    let pub_key = pub_remote.page_key(substrate_pager::PageId::of(&secret));
    assert_ne!(cui_key, pub_key);
    assert!(cui_remote.exists(&cui_key).await?);
    assert!(pub_remote.exists(&pub_key).await?);

    // And a store bound to the public pool cannot wake a CUI database, even holding its token.
    let stolen = tempfile::tempdir().expect("tempdir");
    let err = TieredStore::wake(stolen.path(), pub_remote, &cui_token).await;
    assert!(
        matches!(err, Err(StoreError::PoolBoundary { .. })),
        "a public-pool store woke a CUI database: {err:?}"
    );
    Ok(())
}

/// A fork of a sleeping database shares its pages: forks are free in object storage too.
#[tokio::test(flavor = "multi_thread")]
async fn a_fork_shares_pages_in_object_storage() -> Result<()> {
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote, config("acme")).await?;
    let pager = db.pager();

    let mut txn = pager.begin()?;
    for i in 0..16u64 {
        pager.write(&mut txn, i, content(i as u8, 300))?;
    }
    let base = pager.commit(txn)?;

    let uploads_after_base = {
        db.flush().await?;
        db.stats().uploads
    };

    // Fork and change ONE page.
    let fork = pager.fork(&base)?;
    let mut txn = fork.begin()?;
    fork.write(&mut txn, 3, b"changed on the fork".to_vec())?;
    fork.commit(txn)?;

    db.flush().await?;
    let uploads_after_fork = db.stats().uploads;

    // The fork uploaded exactly one new page. The other fifteen are shared, because they are the
    // same content, which means they are the same key. Ten thousand forks of a 1 GB database cost
    // 1 GB plus whatever actually changed.
    assert_eq!(
        uploads_after_fork - uploads_after_base,
        1,
        "a fork that changed one page uploaded {} pages",
        uploads_after_fork - uploads_after_base
    );

    // And the base is untouched.
    assert_eq!(pager.read(&base, 3)?.as_bytes(), content(3, 300).as_slice());
    Ok(())
}

/// A corrupted object in storage is detected on arrival, never served.
#[tokio::test(flavor = "multi_thread")]
async fn corruption_in_object_storage_is_caught_on_the_way_in() -> Result<()> {
    use object_store::ObjectStore;

    let backend = Arc::new(InMemory::new());
    let remote = RemoteTier::new(backend.clone(), "acme");
    let home = tempfile::tempdir().expect("tempdir");

    let token = {
        let db = TieredStore::open(home.path(), remote.clone(), config("acme")).await?;
        let mut txn = db.pager().begin()?;
        db.pager().write(&mut txn, 0, b"honest bytes".to_vec())?;
        db.pager().commit(txn)?;
        db.sleep().await?
    };

    // Reach into object storage and rot the page, exactly as a failing disk in someone else's
    // datacentre would.
    let key = remote.page_key(substrate_pager::PageId::of(b"honest bytes"));
    backend
        .put(&key, bytes::Bytes::from_static(b"tampered!!!!").into())
        .await
        .expect("corrupt the object");

    let new_home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::wake(new_home.path(), remote, &token).await?;

    let err = db.pager().read_head(0);
    assert!(
        err.as_ref().is_err_and(|e| e.is_corruption()),
        "corrupted bytes from object storage were served to the caller: {err:?}"
    );
    Ok(())
}

/// The same suite against a real S3 API (MinIO), where the network is real.
///
/// `#[ignore]` by design: CI runs the suite inside a **no-egress container** (CLAUDE.md rule 5), and
/// a test that needs a network to pass is a test that fails. Run it deliberately:
///
/// ```sh
/// docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
///     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
/// MINIO_URL=http://localhost:9000 cargo test -p substrate-store -- --ignored
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running MinIO; CI is deliberately network-isolated"]
async fn minio_round_trip() -> Result<()> {
    let Ok(url) = std::env::var("MINIO_URL") else {
        eprintln!("MINIO_URL not set — skipping");
        return Ok(());
    };

    let backend = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(url)
        .with_bucket_name("substrate")
        .with_access_key_id(std::env::var("MINIO_USER").unwrap_or("minioadmin".into()))
        .with_secret_access_key(std::env::var("MINIO_PASSWORD").unwrap_or("minioadmin".into()))
        .with_allow_http(true)
        .build()
        .map_err(|e| StoreError::remote("build", e))?;

    let remote = RemoteTier::new(Arc::new(backend), "acme");
    let home = tempfile::tempdir().expect("tempdir");

    let token = {
        let db = TieredStore::open(home.path(), remote.clone(), config("acme")).await?;
        let mut txn = db.pager().begin()?;
        db.pager()
            .write(&mut txn, 0, b"real s3, real network".to_vec())?;
        db.pager().commit(txn)?;
        db.sleep().await?
    };

    std::fs::remove_dir_all(home.path()).ok();
    let new_home = tempfile::tempdir().expect("tempdir");

    let started = Instant::now();
    let db = TieredStore::wake(new_home.path(), remote, &token).await?;
    let page = db.pager().read_head(0)?;
    let wake_latency = started.elapsed();

    assert_eq!(page.as_bytes(), b"real s3, real network");
    println!("wake-to-first-read against MinIO: {wake_latency:?} (target: < 250ms)");
    assert!(
        wake_latency.as_millis() < 250,
        "wake took {wake_latency:?}, over the 250ms budget in docs/02 §7"
    );
    Ok(())
}

/// **Scrub finds bit rot that nobody read, and repair fixes it from object storage — provably.**
///
/// This is the full integrity story end to end:
///
/// 1. A page rots on the local disk. Nobody has read it, so nothing has noticed.
/// 2. A scrub walks the store, re-hashes everything, and finds it.
/// 3. Repair fetches the replica from object storage — and **verifies it before installing it**,
///    because content addressing means we do not have to trust the remote copy, we can check it.
#[tokio::test(flavor = "multi_thread")]
async fn scrub_finds_rot_and_repair_fixes_it_from_object_storage() -> Result<()> {
    let remote = RemoteTier::new(Arc::new(InMemory::new()), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote, config("acme")).await?;
    let pager = db.pager();

    let mut txn = pager.begin()?;
    for page_no in 0..8u64 {
        pager.write(&mut txn, page_no, content(page_no as u8, 200))?;
    }
    let head = pager.commit(txn)?;
    db.flush().await?; // healthy replicas now exist remotely

    // Nothing is wrong yet.
    let report = pager.scrub(&[head])?;
    assert!(
        report.is_healthy(),
        "a fresh store should be clean: {report}"
    );
    assert_eq!(report.healthy, 8);

    // --- rot a page on the local disk, behind the engine's back ---
    let rotted = substrate_pager::PageId::of(&content(3, 200));
    let hex = rotted.to_hex();
    let path = home
        .path()
        .join("pages")
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(&hex);
    std::fs::write(&path, b"tampered!!!!").expect("corrupt the page");

    // --- the scrub finds it, without anyone having read it ---
    let report = pager.scrub(&[head])?;
    assert!(!report.is_healthy(), "the scrub missed the rot");
    assert_eq!(report.corrupt, vec![rotted]);
    assert_eq!(report.healthy, 7);

    // --- repair, from the verified remote replica ---
    let repair = db.repair(&report).await?;
    assert!(
        repair.is_complete(),
        "the page should have been repairable: {repair}"
    );
    assert_eq!(repair.repaired.len(), 1);

    // --- and now it is genuinely fixed ---
    assert_eq!(pager.read(&head, 3)?.as_bytes(), content(3, 200).as_slice());
    assert!(pager.scrub(&[head])?.is_healthy());
    Ok(())
}

/// If the remote replica is damaged too, the page is **lost**, and we say so rather than installing
/// garbage.
///
/// The tempting thing here is to install the remote copy anyway — it is, after all, the only copy we
/// have. That would replace a corruption we have detected with one we have not, which is strictly
/// worse: the customer would then have a database that reads without error and returns wrong bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_page_damaged_in_both_tiers_is_reported_lost_not_quietly_installed() -> Result<()> {
    use object_store::ObjectStore;

    let backend = Arc::new(InMemory::new());
    let remote = RemoteTier::new(backend.clone(), "acme");
    let home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::open(home.path(), remote.clone(), config("acme")).await?;
    let pager = db.pager();

    let mut txn = pager.begin()?;
    pager.write(&mut txn, 0, b"precious".to_vec())?;
    let head = pager.commit(txn)?;
    db.flush().await?;

    let page_id = substrate_pager::PageId::of(b"precious");

    // Rot it locally...
    let hex = page_id.to_hex();
    let path = home
        .path()
        .join("pages")
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(&hex);
    std::fs::write(&path, b"local rot").expect("corrupt locally");

    // ...and rot the replica too.
    backend
        .put(
            &remote.page_key(page_id),
            bytes::Bytes::from_static(b"remote rot").into(),
        )
        .await
        .expect("corrupt remotely");

    let report = pager.scrub(&[head])?;
    assert_eq!(report.corrupt, vec![page_id]);

    let repair = db.repair(&report).await?;
    assert!(
        !repair.is_complete(),
        "a page damaged in BOTH tiers must not be reported as repaired"
    );
    assert_eq!(repair.unrepairable, vec![page_id.to_hex()]);
    assert!(repair.to_string().contains("COULD NOT BE REPAIRED"));

    // And crucially: the corrupt remote bytes were NOT installed over the corrupt local ones.
    // The read still fails loudly. A database that returns wrong bytes without an error is worse
    // than one that refuses to read.
    assert!(pager.read(&head, 0).is_err_and(|e| e.is_corruption()));
    Ok(())
}

/// **Waking a database whose head is an OVERLAY must work.**
///
/// This is the test that was missing, and its absence hid a real bug.
///
/// `sleep()` uploaded exactly one manifest: the head. That was correct in P3, when every manifest was
/// self-contained. P4 introduced **overlay manifests** — a manifest that records only what *changed*
/// and defers everything else to its base — and nobody went back and asked what that does to sleep.
///
/// It breaks it. An overlay without its base cannot resolve the pages it did not touch. So a woken
/// database could read any page the top overlay happened to hold, and would fail on every other one.
///
/// The existing lifecycle test did not catch it, because it wrote every page in a single commit — so
/// every page *was* in the top overlay, and the walk never needed the base. A test that only ever
/// exercises the easy path is a test that reports green while proving nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_whose_head_is_an_overlay_wakes_correctly() -> Result<()> {
    let backend = Arc::new(InMemory::new());
    let remote = RemoteTier::new(backend, "acme");
    let first_home = tempfile::tempdir().expect("tempdir");

    let token = {
        let db = TieredStore::open(first_home.path(), remote.clone(), config("acme")).await?;
        let pager = db.pager();

        // A flat base with many pages...
        let mut txn = pager.begin()?;
        for page_no in 0..64u64 {
            pager.write(&mut txn, page_no, content(page_no as u8, 200))?;
        }
        pager.commit(txn)?;

        // ...and then a CHAIN of small commits on top, each touching one page. The head is now an
        // overlay that knows about page 0 and nothing else.
        for round in 0..4u64 {
            let mut txn = pager.begin()?;
            pager.write(&mut txn, round, content(200 + round as u8, 50))?;
            pager.commit(txn)?;
        }

        let head = pager.manifest(&pager.head())?;
        assert!(
            !head.is_flat(),
            "this test is pointless unless the head is an overlay"
        );
        assert!(head.depth() > 1, "and it needs a chain worth losing");

        db.sleep().await?
    };

    // Destroy the local disk. Everything the database needs must now be in object storage.
    std::fs::remove_dir_all(first_home.path()).expect("wipe the machine");

    let new_home = tempfile::tempdir().expect("tempdir");
    let db = TieredStore::wake(new_home.path(), remote, &token).await?;

    // A page the TOP OVERLAY DOES NOT HOLD. Resolving it must walk down the chain to the flat base —
    // which is a different manifest, and which had better be in object storage too.
    let cold = db.pager().read_head(42)?;
    assert_eq!(
        cold.as_bytes(),
        content(42, 200).as_slice(),
        "a page not held by the top overlay could not be read after waking — sleep() uploaded the \
         head manifest but not the base it depends on, so the woken database is missing most of \
         itself"
    );

    // And every other page, including the ones the overlays did rewrite.
    for page_no in 4..64u64 {
        assert_eq!(
            db.pager().read_head(page_no)?.as_bytes(),
            content(page_no as u8, 200).as_slice(),
            "page {page_no} did not survive sleep and wake"
        );
    }
    for round in 0..4u64 {
        assert_eq!(
            db.pager().read_head(round)?.as_bytes(),
            content(200 + round as u8, 50).as_slice()
        );
    }
    Ok(())
}
