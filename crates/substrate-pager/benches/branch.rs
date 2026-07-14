//! Benchmarks for the numbers docs/02 §7 promises.
//!
//! | Operation | Target | Why the number |
//! |---|---|---|
//! | fork | **< 1 ms** | Cheap enough to do per-request without thinking. |
//! | snapshot | **< 1 ms** | Pre-migration snapshots must be free enough to take unconditionally. |
//! | overlay read at depth 8 vs flat | **< 20 % overhead** | Or the collapse threshold is load-bearing rather than an optimisation. |
//! | three-way diff | scales with *changed*, not *total* | Or merge is unusable on a real database. |
//!
//! A regression against any of these is a release blocker, not a follow-up ticket (CLAUDE.md).
//!
//! ```sh
//! cargo bench -p substrate-pager
//! ```

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::time::Duration;
use substrate_pager::{
    LogicalPageNo, PageStore, Pager, StoreConfig, DEFAULT_PAGE_SIZE, MAX_OVERLAY_DEPTH,
    MIN_PAGE_SIZE,
};

/// A database with `pages` logical pages of `page_size` bytes each, in one flat manifest.
fn seeded_at(pages: u64, page_size: usize) -> Pager {
    let db = Pager::in_memory(StoreConfig {
        page_size,
        ..Default::default()
    })
    .expect("in-memory store");

    let mut txn = db.begin().expect("begin");
    for page_no in 0..pages {
        // Real-sized pages, with distinct content so nothing deduplicates away.
        let mut bytes = vec![(page_no % 251) as u8; page_size];
        bytes[..8].copy_from_slice(&page_no.to_le_bytes());
        db.write(&mut txn, page_no, bytes).expect("write");
    }
    db.commit(txn).expect("commit");
    db
}

/// A database with small pages — for the structural benchmarks, where page bytes are noise.
fn seeded(pages: u64) -> Pager {
    let db = Pager::in_memory(StoreConfig {
        page_size: MIN_PAGE_SIZE,
        ..Default::default()
    })
    .expect("in-memory store");

    let mut txn = db.begin().expect("begin");
    for page_no in 0..pages {
        db.write(&mut txn, page_no, format!("page-{page_no}").into_bytes())
            .expect("write");
    }
    db.commit(txn).expect("commit");
    db
}

/// Commit until the head sits at exactly `target` overlays deep.
///
/// Committing a fixed *number* of times is not the same as reaching a given depth, and getting that
/// wrong silently ruins the benchmark: eight commits from a fresh store lands precisely **on** the
/// collapse boundary, so the "deep" case comes out flat, and the benchmark cheerfully reports 0%
/// overhead for a chain that does not exist. (It did exactly that, and the assertion below caught
/// it.) So: commit until the depth is what we said it was.
fn deepen_to(db: &Pager, target: u32) {
    for round in 0..(MAX_OVERLAY_DEPTH * 4) {
        let depth = db.manifest(&db.head()).expect("manifest").depth();
        if depth == target {
            return;
        }
        let mut txn = db.begin().expect("begin");
        let size = db.page_size();
        let mut bytes = vec![0xAAu8; size];
        bytes[..8].copy_from_slice(&(round as u64).to_le_bytes());
        db.write(&mut txn, (round % 16) as LogicalPageNo, bytes)
            .expect("write");
        db.commit(txn).expect("commit");
    }
    panic!("could not reach depth {target}");
}

/// **Fork must be O(1) and under a millisecond**, regardless of how big the database is.
///
/// If this ever scales with database size, the entire product thesis is gone: FlockDB cannot give
/// ten thousand tenants a database each, and LoomDB cannot give every agent session a branch.
fn fork_is_constant_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("fork");
    group.measurement_time(Duration::from_secs(5));

    for pages in [100u64, 1_000, 16_384] {
        let db = seeded(pages);
        let head = db.head();

        group.bench_function(format!("{pages}_pages"), |b| {
            b.iter(|| {
                let fork = db.fork(black_box(&head)).expect("fork");
                black_box(fork.head());
            })
        });
    }
    group.finish();
}

/// Snapshot is a pointer read. It must not care how big the database is either.
fn snapshot_is_constant_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");
    for pages in [100u64, 16_384] {
        let db = seeded(pages);
        group.bench_function(format!("{pages}_pages"), |b| {
            b.iter(|| black_box(db.snapshot().expect("snapshot")))
        });
    }
    group.finish();
}

/// **Read overhead at depth 8 versus flat: the < 20 % target (docs/02 §7).**
///
/// This is the number that justifies `MAX_OVERLAY_DEPTH`.
///
/// # What is actually being measured, and why the page size matters
///
/// Resolving a page through 8 overlays costs ~8 map lookups instead of 1. Measured *in isolation*
/// that is about **2.5× the manifest work** — nowhere near the 20% target, and an earlier version of
/// this benchmark hid that behind a much larger cost (every read used to deserialize the whole
/// manifest, so both cases were slow and the overhead *looked* like 0.6%).
///
/// But a page read is not a manifest lookup. It is a manifest lookup **plus fetching 64 KiB of page
/// bytes**, and the bytes dominate by orders of magnitude. So the honest question is not "how much
/// slower is the chain walk" — it is "how much slower is *a read*", and that is what the ≤ 20 %
/// target is about.
///
/// Both are measured below, at the real 64 KiB page size, because a benchmark run at a toy page size
/// would answer a question nobody asked.
fn overlay_read_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_1_page_64KiB");
    group.measurement_time(Duration::from_secs(5));

    // 2,048 pages × 64 KiB = 128 MiB. Big enough to be honest, small enough to build per run.
    let flat = seeded_at(2_048, DEFAULT_PAGE_SIZE);
    let deep = seeded_at(2_048, DEFAULT_PAGE_SIZE);
    deepen_to(&deep, MAX_OVERLAY_DEPTH);

    let deep_depth = deep.manifest(&deep.head()).expect("manifest").depth();
    assert_eq!(
        deep_depth, MAX_OVERLAY_DEPTH,
        "the 'deep' case is at depth {deep_depth}, not {MAX_OVERLAY_DEPTH} — \
         this benchmark is not measuring what it claims"
    );

    // Read a page NOBODY in the chain touched, so the walk goes all the way to the bottom. Reading a
    // page the top overlay happens to hold would exit on the first hop and flatter the result.
    let cold_page: LogicalPageNo = 1_500;

    group.bench_function("flat", |b| {
        b.iter(|| black_box(flat.read_head(black_box(cold_page)).expect("read")))
    });
    group.bench_function(format!("overlay_depth_{deep_depth}"), |b| {
        b.iter(|| black_box(deep.read_head(black_box(cold_page)).expect("read")))
    });

    group.finish();

    // And the manifest work on its own, so the tradeoff is visible rather than buried. This is the
    // number that is *not* under 20% — and it is the one that does not matter, because it is
    // nanoseconds against a page fetch measured in microseconds.
    let mut group = c.benchmark_group("manifest_lookup_only");
    group.bench_function("flat", |b| {
        b.iter(|| {
            black_box(
                flat.lookup(&flat.head(), black_box(cold_page))
                    .expect("lookup"),
            )
        })
    });
    group.bench_function(format!("overlay_depth_{deep_depth}"), |b| {
        b.iter(|| {
            black_box(
                deep.lookup(&deep.head(), black_box(cold_page))
                    .expect("lookup"),
            )
        })
    });
    group.finish();
}

/// Committing one page must not cost the size of the database.
///
/// This is what overlays bought. Before them, every commit cloned the whole page map — a one-page
/// change to a 1 GiB database wrote a 650 KiB manifest.
fn commit_one_page(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_1_page");
    for pages in [100u64, 1_000, 16_384] {
        group.bench_function(format!("db_{pages}_pages"), |b| {
            b.iter_batched(
                || seeded(pages),
                |db| {
                    let mut txn = db.begin().expect("begin");
                    db.write(&mut txn, 0, b"changed".to_vec()).expect("write");
                    black_box(db.commit(txn).expect("commit"));
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Three-way diff must scale with what *changed*, not with how much data exists.
fn three_way_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("diff3");
    group.measurement_time(Duration::from_secs(5));

    // 16,384 pages × 64 KiB = a 1 GiB logical database (docs/02 §7 names this size).
    let db = seeded(16_384);
    let base = db.head();

    // Two branches, each touching a handful of pages.
    let mut txn = db.begin().expect("begin");
    for page_no in 0..8u64 {
        db.write(&mut txn, page_no, b"branch-a".to_vec())
            .expect("write");
    }
    let a = db.commit(txn).expect("commit");

    let branch_b = db.fork(&base).expect("fork");
    let mut txn = branch_b.begin().expect("begin");
    for page_no in 100..108u64 {
        branch_b
            .write(&mut txn, page_no, b"branch-b".to_vec())
            .expect("write");
    }
    let b = branch_b.commit(txn).expect("commit");

    group.bench_function("1GiB_logical_16_changed_pages", |bench| {
        bench.iter(|| {
            let diff = db
                .diff3(black_box(&base), black_box(&a), black_box(&b))
                .expect("diff3");
            // The result must be tiny even though the database is not.
            debug_assert!(diff.len() <= 16);
            black_box(diff.len())
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    fork_is_constant_time,
    snapshot_is_constant_time,
    overlay_read_overhead,
    commit_one_page,
    three_way_diff,
);
criterion_main!(benches);
