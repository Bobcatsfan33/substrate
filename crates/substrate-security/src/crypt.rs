//! Page encryption at rest.
//!
//! # Where encryption sits, and the honest tradeoff
//!
//! ```text
//!   plaintext ──► BLAKE3 ──► PageId          (identity is computed on the PLAINTEXT)
//!       │
//!       └──► XChaCha20-Poly1305 ──► stored bytes   (storage holds only CIPHERTEXT)
//! ```
//!
//! **We hash the plaintext and encrypt for storage, and verify both on read.**
//!
//! This is the only arrangement that lets content addressing work at all. If the id were the hash of
//! the *ciphertext*, then two identical pages encrypted with different nonces would have different
//! ids — deduplication would collapse to nothing, a fork would stop being free, and every property in
//! docs/02 §3.1 would evaporate. The engine would still function; it would simply no longer be
//! interesting.
//!
//! ## What it costs, stated plainly
//!
//! An adversary who can observe `PageId`s and who can **guess** a page's plaintext can confirm the
//! guess by hashing it. Within a dedup scope this leaks **membership**: *"does any database in this
//! pool contain exactly this page?"*
//!
//! For public or single-tenant data that is an acceptable price. For **CUI and classified pools it is
//! not**, and there the `keyed-hash` build makes `PageId = BLAKE3_keyed(pool_key, plaintext)` — a
//! guess is then unconfirmable without the key, and dedup is confined to the pool, which docs/02 §9.1
//! already requires anyway. That mode is not optional in those deployments; it is a different build,
//! and an unkeyed store cannot be constructed in it at all.
//!
//! ## Convergent encryption, and why the nonce is derived
//!
//! The nonce is **derived from the page id**, not random:
//!
//! ```text
//! nonce = BLAKE3_keyed(data_key, "substrate-page-nonce" || page_id)[..24]
//! ```
//!
//! The instinct that this is dangerous is a good instinct, so here is why it is not. Nonce reuse
//! breaks a stream cipher when the **same key and nonce encrypt different plaintexts** — the
//! keystream cancels and you can XOR the ciphertexts. That cannot happen here: the nonce is a
//! function of the page id, the page id is a function of the plaintext, so *different plaintext means
//! a different nonce*. The only way to reuse a nonce is to encrypt the **same plaintext** again, which
//! produces the same ciphertext, which reveals exactly one thing: that the two pages are identical.
//! And content addressing already told you that.
//!
//! What we buy is that encryption is **deterministic and idempotent**, which is what a write-once,
//! content-addressed, deduplicating store requires. Random nonces would mean the same page encrypts
//! to different bytes each time, and a write-once CAS would have no way to know it already had it.
//!
//! XChaCha20's 192-bit nonce makes derivation safe with an enormous margin; the birthday bound on
//! 24 bytes is not a number anyone will reach.

use crate::error::{Result, SecurityError};
use crate::keys::DataKey;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use substrate_pager::PageId;

/// The domain separator for nonce derivation. Changing it re-encrypts the world.
const NONCE_CONTEXT: &str = "substrate-page-nonce-v1";

/// The domain separator baked into every page's AAD.
const AAD_CONTEXT: &[u8] = b"substrate-page-v1";

/// Derive this page's nonce. Deterministic — see the module docs for why that is safe.
fn nonce_for(key: &DataKey, page_id: PageId) -> XNonce {
    let mut hasher = blake3::Hasher::new_derive_key(NONCE_CONTEXT);
    hasher.update(key.expose());
    hasher.update(page_id.as_bytes());
    let full = hasher.finalize();
    let mut nonce = [0u8; 24];
    nonce.copy_from_slice(&full.as_bytes()[..24]);
    XNonce::from(nonce)
}

/// The additional authenticated data bound into every page.
///
/// Binding the **page id** into the AAD means a page cannot be silently relocated: ciphertext lifted
/// from page A and stored under page B's id will fail to authenticate. Without this, an attacker with
/// write access to the storage layer could shuffle valid, correctly-encrypted pages between
/// addresses and produce a database that decrypts perfectly and says something entirely different.
fn aad_for(page_id: PageId) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_CONTEXT.len() + 32);
    aad.extend_from_slice(AAD_CONTEXT);
    aad.extend_from_slice(page_id.as_bytes());
    aad
}

/// Encrypt a page's bytes for storage.
///
/// `page_id` must be the id of the **plaintext** — that is the whole design (see the module docs).
pub fn seal(key: &DataKey, page_id: PageId, plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.expose().into());
    let aad = aad_for(page_id);

    cipher
        .encrypt(
            &nonce_for(key, page_id),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::Encrypt)
}

/// Decrypt a page's bytes, and confirm they are the page they claim to be.
///
/// Two independent checks have to pass, and they catch different things:
///
/// 1. **The AEAD tag** — the ciphertext is authentic, was encrypted under this key, and was stored at
///    this page id. Catches tampering and relocation.
/// 2. **The plaintext hash** — the decrypted bytes really do hash to `page_id`. Catches a key that
///    decrypts to garbage, and any bug in this file.
///
/// The second is redundant given the first, in theory. We do it anyway, because "in theory" is doing
/// a great deal of work in that sentence and the cost is a BLAKE3 hash.
pub fn open(key: &DataKey, page_id: PageId, ciphertext: &[u8]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.expose().into());
    let aad = aad_for(page_id);

    let plaintext = cipher
        .decrypt(
            &nonce_for(key, page_id),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::Decrypt { page: page_id })?;

    // Belt and braces (see above).
    if PageId::of(&plaintext) != page_id {
        return Err(SecurityError::PlaintextMismatch { page: page_id });
    }
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::PoolMasterKey;

    fn key() -> DataKey {
        PoolMasterKey::from_bytes([7u8; 32]).derive_data_key("acme-prod")
    }

    #[test]
    fn round_trips() -> Result<()> {
        let key = key();
        let plaintext = b"the quick brown fox".to_vec();
        let id = PageId::of(&plaintext);

        let sealed = seal(&key, id, &plaintext)?;
        assert_ne!(sealed, plaintext, "the stored bytes must be ciphertext");
        assert_eq!(open(&key, id, &sealed)?, plaintext);
        Ok(())
    }

    #[test]
    fn encryption_is_deterministic_so_the_cas_can_deduplicate() -> Result<()> {
        // A write-once, content-addressed store needs the same page to encrypt to the same bytes,
        // every time. Random nonces would make every write a different object and dedup would die.
        let key = key();
        let plaintext = b"identical content".to_vec();
        let id = PageId::of(&plaintext);

        let first = seal(&key, id, &plaintext)?;
        for _ in 0..16 {
            assert_eq!(seal(&key, id, &plaintext)?, first);
        }
        Ok(())
    }

    #[test]
    fn different_plaintexts_get_different_nonces() -> Result<()> {
        // The property the whole convergent-nonce design rests on: nonce reuse across DIFFERENT
        // plaintexts is what breaks a stream cipher, and it cannot happen when the nonce is a
        // function of the plaintext's hash.
        let key = key();
        let a = b"plaintext one".to_vec();
        let b = b"plaintext two".to_vec();

        let nonce_a = nonce_for(&key, PageId::of(&a));
        let nonce_b = nonce_for(&key, PageId::of(&b));
        assert_ne!(nonce_a, nonce_b);
        Ok(())
    }

    #[test]
    fn the_wrong_key_cannot_decrypt() -> Result<()> {
        let plaintext = b"secret".to_vec();
        let id = PageId::of(&plaintext);
        let sealed = seal(&key(), id, &plaintext)?;

        let wrong = PoolMasterKey::from_bytes([9u8; 32]).derive_data_key("acme-prod");
        assert!(matches!(
            open(&wrong, id, &sealed),
            Err(SecurityError::Decrypt { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_different_database_in_the_same_pool_cannot_decrypt() -> Result<()> {
        // The key hierarchy earning its keep: one pool, two databases, and compromising one
        // database's data key does not hand over the other's.
        let master = PoolMasterKey::from_bytes([7u8; 32]);
        let plaintext = b"acme's secret".to_vec();
        let id = PageId::of(&plaintext);

        let sealed = seal(&master.derive_data_key("acme-prod"), id, &plaintext)?;

        let other_db = master.derive_data_key("globex-prod");
        assert!(matches!(
            open(&other_db, id, &sealed),
            Err(SecurityError::Decrypt { .. })
        ));
        Ok(())
    }

    #[test]
    fn a_page_cannot_be_relocated_to_another_address() -> Result<()> {
        // Without the page id in the AAD, an attacker with write access to storage could take a
        // valid, correctly-encrypted page and store it under a DIFFERENT id — producing a database
        // that decrypts perfectly and says something else entirely. This is the check that stops it.
        let key = key();
        let plaintext = b"page A's contents".to_vec();
        let real_id = PageId::of(&plaintext);
        let sealed = seal(&key, real_id, &plaintext)?;

        let other_id = PageId::of(b"page B's contents");
        assert!(
            matches!(
                open(&key, other_id, &sealed),
                Err(SecurityError::Decrypt { .. })
            ),
            "ciphertext was accepted at an address it was not written to"
        );
        Ok(())
    }

    #[test]
    fn a_single_flipped_bit_is_caught_by_the_aead_tag() -> Result<()> {
        let key = key();
        let plaintext = b"honest bytes".to_vec();
        let id = PageId::of(&plaintext);
        let sealed = seal(&key, id, &plaintext)?;

        for byte in 0..sealed.len() {
            let mut corrupted = sealed.clone();
            corrupted[byte] ^= 0x01;
            assert!(
                open(&key, id, &corrupted).is_err(),
                "a flipped bit at byte {byte} was not detected"
            );
        }
        Ok(())
    }
}
