//! The `keyed-hash` build: page identity that an adversary cannot confirm.
//!
//! # What this feature is for
//!
//! By default `PageId = BLAKE3(plaintext)`. That gives free deduplication and safe caching, and it
//! leaks **membership**: an adversary who can see page ids and who *guesses* a page's contents can
//! hash the guess and confirm it. For public or single-tenant data that is a fine trade. For CUI
//! and classified pools it is not, and `keyed-hash` closes it — `PageId = BLAKE3_keyed(pool_key,
//! plaintext)`, so a guess is unconfirmable without the key and dedup cannot span pools.
//!
//! # Why it is a separate build, not a runtime switch
//!
//! Because a CUI deployment must not be able to be *configured* back into the weak mode by a tired
//! operator at 2am. With this feature compiled in, constructing an unkeyed store does not fail a
//! policy check — it fails at the door, every time, with no override. That is the entire point, and
//! it is why this build is mutually exclusive with the default one rather than additive to it.
//!
//! Which is also why this whole file only exists under the feature: in a default build, an unkeyed
//! store is perfectly legal, and asserting that it is refused would be asserting something false.

#![cfg(feature = "keyed-hash")]

use substrate_pager::{PageHasher, PageId, PageStore, Pager, PagerError, StoreConfig};

fn cui_config(pool_key: [u8; 32]) -> StoreConfig {
    StoreConfig {
        hasher: PageHasher::Keyed(pool_key),
        pool: "cui-secret".to_string(),
        ..Default::default()
    }
}

#[test]
fn an_unkeyed_store_cannot_be_created_in_this_build() {
    let err = Pager::in_memory(StoreConfig::default());
    assert!(
        matches!(err, Err(PagerError::UnkeyedStoreInKeyedBuild)),
        "a CUI build must refuse plaintext-confirmable page identity outright, \
         not merely discourage it"
    );
}

#[test]
fn a_keyed_store_works_exactly_like_a_normal_one() -> Result<(), PagerError> {
    let db = Pager::in_memory(cui_config([7; 32]))?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"classified".to_vec())?;
    let v1 = db.commit(txn)?;

    assert_eq!(db.read_head(0)?.as_bytes(), b"classified");

    // Forking, the whole reason substrate exists, still works.
    let fork = db.fork(&v1)?;
    let mut txn = fork.begin()?;
    fork.write(&mut txn, 0, b"a different classification".to_vec())?;
    fork.commit(txn)?;

    assert_eq!(db.read_head(0)?.as_bytes(), b"classified");
    Ok(())
}

#[test]
fn the_same_plaintext_in_two_pools_is_not_the_same_page() -> Result<(), PagerError> {
    // The property that makes this mode worth having. If these ids matched, an adversary holding
    // one pool could prove a page exists in the other simply by comparing hashes.
    let alpha = Pager::in_memory(cui_config([1; 32]))?;
    let bravo = Pager::in_memory(cui_config([2; 32]))?;

    let secret = b"TROOP MOVEMENT 0400 GRID 12345".to_vec();

    let mut txn = alpha.begin()?;
    let in_alpha = alpha.write(&mut txn, 0, secret.clone())?;
    alpha.commit(txn)?;

    let mut txn = bravo.begin()?;
    let in_bravo = bravo.write(&mut txn, 0, secret.clone())?;
    bravo.commit(txn)?;

    assert_ne!(
        in_alpha, in_bravo,
        "identical plaintext in two pools produced the same page id — cross-pool membership leak"
    );

    // ...and neither is confirmable by hashing the guess without the key.
    assert_ne!(in_alpha, PageId::of(&secret));
    assert_ne!(in_bravo, PageId::of(&secret));
    Ok(())
}
