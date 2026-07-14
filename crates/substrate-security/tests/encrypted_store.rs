//! An encrypted database, end to end, through the real engine.
//!
//! The claim being tested: **encryption is invisible above the CAS, and total below it.** Fork,
//! snapshot, rewind, and GC all work exactly as they do on a plaintext store — while the bytes on
//! disk are ciphertext and stay that way.

use std::sync::Arc;
use substrate_pager::{
    Cas, MemCas, MemManifestStore, PageHasher, PageId, PageStore, Pager, StoreConfig, MIN_PAGE_SIZE,
};
use substrate_security::{EncryptedCas, PoolMasterKey, Result};

fn config() -> StoreConfig {
    StoreConfig {
        page_size: MIN_PAGE_SIZE,
        pool: "cui-secret".to_string(),
        ..Default::default()
    }
}

/// Build a real `Pager` whose CAS encrypts.
fn encrypted_db(disk: Arc<MemCas>, master: &PoolMasterKey, database: &str) -> Result<Pager> {
    let cas = EncryptedCas::new(disk, master.derive_data_key(database), PageHasher::Unkeyed);
    Ok(Pager::from_parts(
        cas as Arc<dyn Cas>,
        Arc::new(MemManifestStore::new()),
        config(),
    )?)
}

/// The whole engine works, and the disk holds nothing readable.
#[test]
fn the_engine_works_normally_and_the_disk_holds_only_ciphertext() -> Result<()> {
    let disk = Arc::new(MemCas::new(PageHasher::Unkeyed));
    let master = PoolMasterKey::from_bytes([7; 32]);
    let db = encrypted_db(disk.clone(), &master, "acme-prod")?;

    let secret = b"SALARY OF EMPLOYEE 4471 IS 220000".to_vec();

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, secret.clone())?;
    db.write(&mut txn, 1, b"another secret".to_vec())?;
    let v1 = db.commit(txn)?;

    // Above the CAS: plaintext, exactly as always.
    assert_eq!(db.read_head(0)?.as_bytes(), secret.as_slice());

    // Below the CAS: ciphertext. The secret does not appear anywhere on the disk.
    let stored = disk.get_raw(PageId::of(&secret))?;
    assert_ne!(stored, secret, "the plaintext was written to disk");
    assert!(
        !stored
            .windows(secret.len())
            .any(|window| window == secret.as_slice()),
        "the plaintext appears verbatim inside the stored bytes"
    );

    // --- and every engine primitive still works ---

    // Fork.
    let fork = db.fork(&v1)?;
    let mut txn = fork.begin()?;
    fork.write(&mut txn, 0, b"changed on the fork".to_vec())?;
    fork.commit(txn)?;
    assert_eq!(
        db.read_head(0)?.as_bytes(),
        secret.as_slice(),
        "the fork leaked into the base"
    );
    assert_eq!(fork.read_head(0)?.as_bytes(), b"changed on the fork");

    // Snapshot immutability.
    assert_eq!(db.read(&v1, 0)?.as_bytes(), secret.as_slice());

    // GC.
    let stats = db.gc(&[v1, fork.head()])?;
    assert_eq!(stats.pages_swept, 0, "nothing here is garbage: {stats}");
    assert_eq!(db.read_head(0)?.as_bytes(), secret.as_slice());

    Ok(())
}

/// **Deduplication survives encryption.** This is why the plaintext is what gets hashed.
#[test]
fn identical_pages_still_deduplicate_when_encrypted() -> Result<()> {
    let disk = Arc::new(MemCas::new(PageHasher::Unkeyed));
    let master = PoolMasterKey::from_bytes([7; 32]);
    let db = encrypted_db(disk.clone(), &master, "acme-prod")?;

    // Write the same content to twenty different logical pages.
    let repeated = b"the same row, twenty times".to_vec();
    let mut txn = db.begin()?;
    for page_no in 0..20u64 {
        db.write(&mut txn, page_no, repeated.clone())?;
    }
    db.commit(txn)?;

    assert_eq!(
        disk.list()?.len(),
        1,
        "twenty identical pages became {} stored objects — deduplication is broken, which means \
         encryption was applied ABOVE content addressing and a fork is no longer free",
        disk.list()?.len()
    );
    Ok(())
}

/// A stolen disk is useless without the key. This is the threat the whole feature exists for.
#[test]
fn a_stolen_disk_without_the_key_yields_nothing() -> Result<()> {
    let disk = Arc::new(MemCas::new(PageHasher::Unkeyed));
    let master = PoolMasterKey::from_bytes([7; 32]);

    {
        let db = encrypted_db(disk.clone(), &master, "acme-prod")?;
        let mut txn = db.begin()?;
        db.write(&mut txn, 0, b"classified".to_vec())?;
        db.commit(txn)?;
    }

    // The attacker has the disk. They do not have the key.
    let attacker_key = PoolMasterKey::from_bytes([0; 32]);
    let stolen = encrypted_db(disk.clone(), &attacker_key, "acme-prod")?;

    // They can see that a page exists...
    let page_id = PageId::of(b"classified");
    assert!(disk.contains(page_id)?);

    // ...and they cannot read it. It reports as corruption, which is exactly right: from the
    // engine's point of view, bytes that will not authenticate are bytes that cannot be trusted.
    let err = stolen.read(&stolen.head(), 0);
    assert!(err.is_err(), "a stolen disk was readable without the key");
    Ok(())
}

/// One pool, two databases. Compromising one database's key does not hand over the other.
#[test]
fn the_key_hierarchy_contains_a_compromise() -> Result<()> {
    let disk = Arc::new(MemCas::new(PageHasher::Unkeyed));
    let master = PoolMasterKey::from_bytes([7; 32]);

    // ACME writes a secret.
    {
        let acme = encrypted_db(disk.clone(), &master, "acme-prod")?;
        let mut txn = acme.begin()?;
        acme.write(&mut txn, 0, b"acme's secret".to_vec())?;
        acme.commit(txn)?;
    }

    // Globex's data key is compromised. Globex shares the pool, the disk, and the master key's
    // *derivation*, but not ACME's data key — and a data key cannot be walked back to the master.
    let globex_key = master.derive_data_key("globex-prod");
    let globex_cas = EncryptedCas::new(disk.clone(), globex_key, PageHasher::Unkeyed);

    let page_id = PageId::of(b"acme's secret");
    assert!(
        globex_cas.get(page_id).is_err(),
        "one database's key decrypted another database's page — the hierarchy is not a hierarchy"
    );
    Ok(())
}

/// Tampering with ciphertext on disk is caught — and reported as **corruption**, so the scrubber and
/// the repair path work on an encrypted store exactly as they do on a plaintext one.
#[test]
fn tampered_ciphertext_reports_as_corruption_so_scrub_and_repair_still_work() -> Result<()> {
    let disk = Arc::new(MemCas::new(PageHasher::Unkeyed));
    let master = PoolMasterKey::from_bytes([7; 32]);
    let db = encrypted_db(disk.clone(), &master, "acme-prod")?;

    let mut txn = db.begin()?;
    db.write(&mut txn, 0, b"honest bytes".to_vec())?;
    let head = db.commit(txn)?;

    // Flip a bit in the ciphertext, behind the engine's back.
    let page_id = PageId::of(b"honest bytes");
    let mut sealed = disk.get_raw(page_id)?;
    sealed[4] ^= 0x01;
    disk.remove(page_id)?;
    disk.put_raw(page_id, &sealed)?;

    let err = db.read(&head, 0);
    assert!(
        err.as_ref().is_err_and(|e| e.is_corruption()),
        "tampered ciphertext must surface as CORRUPTION, so that the integrity scrubber finds it \
         and the repair path can re-fetch it. Got: {err:?}"
    );

    // And the scrubber does in fact find it, on an encrypted store, with no special handling.
    let report = db.scrub(&[head])?;
    assert!(!report.is_healthy());
    assert_eq!(report.corrupt, vec![page_id]);
    Ok(())
}
