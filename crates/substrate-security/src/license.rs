//! Offline licensing, and the clock.
//!
//! # The rule that outranks every other rule in this file
//!
//! > **Enforcement returns `Ok` | `Warning(days)` | `Degraded`. It NEVER stops a read or a write.**
//!
//! Not on expiry. Not on a corrupt license file. Not on a missing one. Not on a clock that has jumped
//! to 2031.
//!
//! This is not generosity, it is engineering. Our customers run in facilities that cannot phone home
//! for a renewal — that is the entire point of an offline license. If an expired license could stop a
//! database from serving reads in an air-gapped facility, then we have shipped a weapon that fires at
//! our own customer, at random, during an incident, while they have no way to disarm it.
//!
//! `Degraded` disables **fleet-plane administrative features**. It does not disable the database. A
//! customer whose license lapses gets a loud, unmissable, escalating nuisance — and their data keeps
//! working. That is the correct trade, and it is not negotiable in code review.
//!
//! # The clock
//!
//! Two kinds of time, and confusing them is how a storage engine gets broken by an operator running
//! `date -s`:
//!
//! - **Monotonic** — every internal decision. Timeouts, leases, backoff, idle-sleep. Cannot go
//!   backwards, so nothing internal can be broken by moving the wall clock.
//! - **Wall clock** — license checks *only*, and even then not directly: against a persisted
//!   **high-water mark that never moves backward**.
//!
//! The high-water mark is what makes a license enforceable at all without a network. Set the system
//! clock back five years and the license does not un-expire, because we remember the furthest forward
//! we have ever seen time go. An enclave whose clock legitimately drifts (and they do) is tolerated up
//! to **±30 days** (docs/02 §9.2) before we start complaining.

use crate::error::{Result, SecurityError};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Milliseconds in a day.
const DAY_MS: u64 = 86_400_000;

/// How far an air-gapped enclave's clock may legitimately drift before we complain (docs/02 §9.2).
pub const CLOCK_DRIFT_TOLERANCE_DAYS: u64 = 30;

/// What a license says.
///
/// Deliberately small. A license format that grows fields grows ways to be ambiguous, and this one
/// gets parsed in a facility where nobody can call support.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseClaims {
    /// Who it is for.
    pub licensee: String,
    /// What it turns on.
    pub features: Vec<String>,
    /// Not valid before this instant (ms since the Unix epoch).
    pub not_before: u64,
    /// Not valid after this instant.
    pub not_after: u64,
    /// How long after `not_after` we keep saying `Warning` instead of `Degraded`.
    ///
    /// Grace exists because the renewal has to physically travel to an air-gapped site, sometimes on
    /// a person, and that person can be delayed.
    pub grace_days: u64,
}

impl LicenseClaims {
    /// The canonical bytes that get signed. Deterministic, or a signature means nothing.
    fn signing_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|source| SecurityError::Codec {
            what: "license claims",
            source,
        })
    }
}

/// A signed license.
///
/// # Two signatures, on purpose
///
/// `ed25519` is signed today. `ml_dsa` is reserved for a post-quantum signature, and the format
/// carries the slot **now**, empty, so that adding it later is not a format break — an air-gapped
/// customer cannot be asked to take a new license format on short notice, and "we'll add a field
/// later" is how you end up with two incompatible license formats in the field.
///
/// A verifier that understands only Ed25519 ignores the second slot. A future verifier will require
/// both.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct License {
    /// The claims.
    pub claims: LicenseClaims,
    /// Ed25519 signature over the canonical claim bytes.
    pub ed25519: Vec<u8>,
    /// ML-DSA signature. Empty today; the slot exists so adding it is not a format break.
    #[serde(default)]
    pub ml_dsa: Vec<u8>,
}

impl License {
    /// Sign a set of claims. (Used by the licence issuer, and by tests.)
    pub fn sign(claims: LicenseClaims, key: &SigningKey) -> Result<License> {
        let signature = key.sign(&claims.signing_bytes()?);
        Ok(License {
            claims,
            ed25519: signature.to_bytes().to_vec(),
            ml_dsa: Vec::new(),
        })
    }

    /// Verify the signature against a public key.
    ///
    /// **Verification failing does not stop the database.** It produces a `Degraded` status, and the
    /// caller decides what that turns off — which, per docs/02 §9.2, is fleet-plane admin features and
    /// nothing else.
    pub fn verify(&self, public: &VerifyingKey) -> Result<()> {
        let bytes: [u8; 64] = self
            .ed25519
            .as_slice()
            .try_into()
            .map_err(|_| SecurityError::LicenseSignature)?;

        public
            .verify(
                &self.claims.signing_bytes()?,
                &Signature::from_bytes(&bytes),
            )
            .map_err(|_| SecurityError::LicenseSignature)
    }

    /// Serialize.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|source| SecurityError::Codec {
            what: "license",
            source,
        })
    }

    /// Parse.
    pub fn from_json(s: &str) -> Result<License> {
        serde_json::from_str(s).map_err(|source| SecurityError::Codec {
            what: "license",
            source,
        })
    }
}

/// What the licence engine says. **None of these stop a read or a write.**
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// Licensed, valid, nothing to say.
    Ok,
    /// Valid, but expiring. Say so, loudly, and keep saying it.
    Warning {
        /// Days until the licence (or its grace period) runs out.
        days_left: u64,
        /// Why we are warning.
        reason: String,
    },
    /// Not licensed: expired past grace, missing, malformed, or forged.
    ///
    /// **Fleet-plane administrative features are disabled. Reads and writes are not.** The caller
    /// decides what "administrative" means; it must not include serving data.
    Degraded {
        /// What an operator needs to be told.
        reason: String,
    },
}

impl Status {
    /// True if there is nothing to complain about.
    pub fn is_ok(&self) -> bool {
        matches!(self, Status::Ok)
    }

    /// True if admin features should be turned off.
    pub fn is_degraded(&self) -> bool {
        matches!(self, Status::Degraded { .. })
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Ok => write!(f, "licensed"),
            Status::Warning { days_left, reason } => {
                write!(f, "LICENCE WARNING ({days_left} days left): {reason}")
            }
            Status::Degraded { reason } => write!(
                f,
                "LICENCE DEGRADED: {reason}. Fleet administration is disabled. \
                 Reads and writes are UNAFFECTED and will remain so."
            ),
        }
    }
}

/// A wall clock that never goes backwards.
///
/// # Why this exists
///
/// An offline license has to be enforceable without a network, which means trusting the local clock,
/// which means an operator can defeat it by setting the clock back. The high-water mark is the answer:
/// we persist the furthest-forward time we have ever seen, and never accept anything earlier.
///
/// Set the clock back five years and the license does not un-expire.
///
/// It also tolerates the *legitimate* case, which is real and common: an air-gapped enclave's clock
/// genuinely drifts, and an operator genuinely does correct it. ±30 days is fine, silently.
#[derive(Debug)]
pub struct HighWaterClock {
    high_water_ms: AtomicU64,
}

impl HighWaterClock {
    /// Start from a persisted high-water mark (0 if this is the first ever run).
    pub fn new(persisted_high_water_ms: u64) -> Self {
        HighWaterClock {
            high_water_ms: AtomicU64::new(persisted_high_water_ms),
        }
    }

    /// Feed in the current wall clock, and get back the time we are willing to believe.
    ///
    /// Ratchets forward, never back.
    pub fn observe(&self, wall_clock_ms: u64) -> u64 {
        self.high_water_ms
            .fetch_max(wall_clock_ms, Ordering::SeqCst)
            .max(wall_clock_ms)
    }

    /// The high-water mark, to persist.
    pub fn high_water_ms(&self) -> u64 {
        self.high_water_ms.load(Ordering::SeqCst)
    }

    /// How far *behind* the high-water mark the given wall clock is.
    ///
    /// Small values are drift. Large values are somebody trying to un-expire a license, or a machine
    /// whose battery died — and we cannot tell the difference, so we do the same safe thing either
    /// way: believe the high-water mark, and say something.
    pub fn backwards_drift_days(&self, wall_clock_ms: u64) -> u64 {
        self.high_water_ms()
            .saturating_sub(wall_clock_ms)
            .saturating_div(DAY_MS)
    }
}

/// The license engine.
#[derive(Debug)]
pub struct Enforcement {
    public_key: VerifyingKey,
    clock: HighWaterClock,
}

impl Enforcement {
    /// Build an enforcement engine against a compiled-in public key.
    pub fn new(public_key: VerifyingKey, persisted_high_water_ms: u64) -> Self {
        Enforcement {
            public_key,
            clock: HighWaterClock::new(persisted_high_water_ms),
        }
    }

    /// The high-water mark, to persist.
    pub fn high_water_ms(&self) -> u64 {
        self.clock.high_water_ms()
    }

    /// Evaluate a license. **This function cannot return anything that stops a read or a write.**
    ///
    /// `license` is `None` when there is no license file at all — which is `Degraded`, not a crash and
    /// not a refusal to open the database.
    pub fn evaluate(&self, license: Option<&License>, wall_clock_ms: u64) -> Status {
        // The clock we are willing to believe: monotonic, ratcheting, immune to `date -s`.
        let now = self.clock.observe(wall_clock_ms);

        let Some(license) = license else {
            return Status::Degraded {
                reason: "no licence file found".to_string(),
            };
        };

        if self.verify_signature(license).is_err() {
            return Status::Degraded {
                reason: "licence signature is invalid — the file is corrupt, forged, or for a \
                         different product"
                    .to_string(),
            };
        }

        let claims = &license.claims;

        if now < claims.not_before {
            let days = (claims.not_before - now) / DAY_MS;
            return Status::Degraded {
                reason: format!("licence is not valid for another {days} day(s)"),
            };
        }

        let grace_ms = claims.grace_days.saturating_mul(DAY_MS);
        let hard_end = claims.not_after.saturating_add(grace_ms);

        // Expired, past grace.
        if now >= hard_end {
            let days = (now - claims.not_after) / DAY_MS;
            return Status::Degraded {
                reason: format!(
                    "licence for {:?} expired {days} day(s) ago and the grace period has ended",
                    claims.licensee
                ),
            };
        }

        // Expired, inside grace.
        if now >= claims.not_after {
            return Status::Warning {
                days_left: (hard_end - now) / DAY_MS,
                reason: format!(
                    "licence for {:?} has EXPIRED; running on grace",
                    claims.licensee
                ),
            };
        }

        // Valid, but the clock has moved backwards a suspicious distance.
        let drift = self.clock.backwards_drift_days(wall_clock_ms);
        if drift > CLOCK_DRIFT_TOLERANCE_DAYS {
            return Status::Warning {
                days_left: (claims.not_after - now) / DAY_MS,
                reason: format!(
                    "the system clock is {drift} day(s) behind the highest time this system has \
                     ever seen. Licence checks are using the high-water mark, not the system clock"
                ),
            };
        }

        // Valid, and expiring soon enough to mention.
        let days_left = (claims.not_after - now) / DAY_MS;
        if days_left <= CLOCK_DRIFT_TOLERANCE_DAYS {
            return Status::Warning {
                days_left,
                reason: format!("licence for {:?} expires soon", claims.licensee),
            };
        }

        Status::Ok
    }

    /// Whether a licensed feature is enabled. A `Degraded` licence enables nothing extra — but,
    /// again, it never disables the database.
    pub fn has_feature(&self, license: Option<&License>, feature: &str, now_ms: u64) -> bool {
        if self.evaluate(license, now_ms).is_degraded() {
            return false;
        }
        license.is_some_and(|l| l.claims.features.iter().any(|f| f == feature))
    }

    fn verify_signature(&self, license: &License) -> Result<()> {
        license.verify(&self.public_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    const T0: u64 = 1_700_000_000_000; // an ordinary Tuesday
    const YEAR: u64 = 365 * DAY_MS;

    fn issuer() -> SigningKey {
        SigningKey::from_bytes(&[42u8; 32])
    }

    fn engine() -> Enforcement {
        Enforcement::new(issuer().verifying_key(), 0)
    }

    fn licence(not_before: u64, not_after: u64, grace_days: u64) -> License {
        License::sign(
            LicenseClaims {
                licensee: "ACME Corp".to_string(),
                features: vec!["fleet".to_string(), "airgap".to_string()],
                not_before,
                not_after,
                grace_days,
            },
            &issuer(),
        )
        .expect("sign")
    }

    #[test]
    fn a_valid_licence_is_ok() {
        let l = licence(T0, T0 + YEAR, 30);
        assert_eq!(engine().evaluate(Some(&l), T0 + DAY_MS), Status::Ok);
    }

    #[test]
    fn an_expiring_licence_warns_but_never_stops() {
        let l = licence(T0, T0 + YEAR, 30);
        let status = engine().evaluate(Some(&l), T0 + YEAR - 5 * DAY_MS);

        match status {
            Status::Warning { days_left, .. } => assert_eq!(days_left, 5),
            other => panic!("expected a warning, got {other:?}"),
        }
    }

    #[test]
    fn an_expired_licence_inside_grace_warns() {
        let l = licence(T0, T0 + YEAR, 30);
        let status = engine().evaluate(Some(&l), T0 + YEAR + 10 * DAY_MS);

        match status {
            Status::Warning { days_left, reason } => {
                // 30 days of grace, 10 days elapsed since expiry.
                assert_eq!(days_left, 20);
                assert!(reason.contains("EXPIRED"));
            }
            other => panic!("expected a grace warning, got {other:?}"),
        }
    }

    #[test]
    fn an_expired_licence_past_grace_degrades_and_says_reads_are_unaffected() {
        let l = licence(T0, T0 + YEAR, 30);
        let status = engine().evaluate(Some(&l), T0 + YEAR + 60 * DAY_MS);

        assert!(status.is_degraded());
        // The message is part of the contract. An operator reading this at 3am must not think their
        // database is about to stop serving data, because it is not.
        assert!(status
            .to_string()
            .contains("Reads and writes are UNAFFECTED"));
    }

    #[test]
    fn a_missing_licence_degrades_it_does_not_explode() {
        let status = engine().evaluate(None, T0);
        assert!(status.is_degraded());
        assert!(status
            .to_string()
            .contains("Reads and writes are UNAFFECTED"));
    }

    #[test]
    fn a_forged_licence_is_degraded_not_accepted() {
        let attacker = SigningKey::from_bytes(&[99u8; 32]);
        let forged = License::sign(
            LicenseClaims {
                licensee: "ACME Corp".to_string(),
                features: vec!["fleet".to_string()],
                not_before: T0,
                not_after: T0 + 100 * YEAR, // a very optimistic licence
                grace_days: 0,
            },
            &attacker,
        )
        .expect("sign");

        assert!(engine().evaluate(Some(&forged), T0).is_degraded());
    }

    #[test]
    fn tampering_with_the_claims_invalidates_the_signature() {
        let mut l = licence(T0, T0 + DAY_MS, 0);
        l.claims.not_after = T0 + 100 * YEAR; // "just extend it a bit"

        assert!(engine().evaluate(Some(&l), T0).is_degraded());
    }

    // ---- the clock-jump scenarios from docs/02 §9.2 ----

    #[test]
    fn setting_the_clock_back_does_not_un_expire_a_licence() {
        // The attack this whole design exists to defeat.
        let l = licence(T0, T0 + YEAR, 0);
        let engine = engine();

        // Time passes; the licence expires.
        let expired = engine.evaluate(Some(&l), T0 + YEAR + DAY_MS);
        assert!(expired.is_degraded());

        // The operator sets the clock back a year.
        let after_tampering = engine.evaluate(Some(&l), T0);

        assert!(
            after_tampering.is_degraded(),
            "winding the clock back un-expired the licence — the high-water mark is not working"
        );
    }

    #[test]
    fn a_thirty_day_backwards_drift_is_tolerated_quietly() {
        // The legitimate case. Enclave clocks drift, and operators correct them. We must not scream
        // at somebody for fixing their clock.
        let l = licence(T0, T0 + YEAR, 30);
        let engine = engine();

        engine.evaluate(Some(&l), T0 + 100 * DAY_MS); // sets the high-water mark
        let status = engine.evaluate(Some(&l), T0 + 80 * DAY_MS); // 20 days back

        assert_eq!(status, Status::Ok, "20 days of drift is within tolerance");
    }

    #[test]
    fn a_large_backwards_jump_warns_but_still_does_not_stop_anything() {
        let l = licence(T0, T0 + YEAR, 30);
        let engine = engine();

        engine.evaluate(Some(&l), T0 + 200 * DAY_MS);
        let status = engine.evaluate(Some(&l), T0 + 10 * DAY_MS); // 190 days back

        match &status {
            Status::Warning { reason, .. } => {
                assert!(reason.contains("high-water mark"));
            }
            other => panic!("expected a clock warning, got {other:?}"),
        }
        assert!(
            !status.is_degraded(),
            "a clock jump must not degrade a valid licence"
        );
    }

    #[test]
    fn the_high_water_mark_only_ever_ratchets_forward() {
        let clock = HighWaterClock::new(0);
        assert_eq!(clock.observe(1_000), 1_000);
        assert_eq!(clock.observe(5_000), 5_000);
        assert_eq!(
            clock.observe(2_000),
            5_000,
            "time does not go backwards here"
        );
        assert_eq!(clock.observe(0), 5_000);
        assert_eq!(clock.high_water_ms(), 5_000);
    }

    #[test]
    fn a_licence_from_the_future_is_not_valid_yet() {
        let l = licence(T0 + 10 * DAY_MS, T0 + YEAR, 0);
        assert!(engine().evaluate(Some(&l), T0).is_degraded());
    }

    #[test]
    fn features_are_gated_by_the_licence_but_data_never_is() {
        let engine = engine();
        let l = licence(T0, T0 + YEAR, 0);

        assert!(engine.has_feature(Some(&l), "fleet", T0));
        assert!(!engine.has_feature(Some(&l), "a-feature-nobody-bought", T0));

        // Degraded: no features...
        assert!(!engine.has_feature(None, "fleet", T0));
        // ...but there is no API here that can stop a read, and that is on purpose.
    }

    #[test]
    fn round_trips_through_json() -> Result<()> {
        let l = licence(T0, T0 + YEAR, 30);
        let parsed = License::from_json(&l.to_json()?)?;
        assert_eq!(parsed, l);
        parsed.verify(&issuer().verifying_key())?;
        Ok(())
    }

    #[test]
    fn the_ml_dsa_slot_exists_so_adding_it_later_is_not_a_format_break() -> Result<()> {
        // An air-gapped customer cannot be asked to accept a new licence format on short notice.
        // The slot is in the format today, empty, so that a post-quantum signature can be added
        // without one.
        let l = licence(T0, T0 + YEAR, 0);
        assert!(l.ml_dsa.is_empty());

        // A licence written without the field at all still parses.
        let legacy = r#"{"claims":{"licensee":"ACME","features":[],"not_before":0,
                         "not_after":1,"grace_days":0},"ed25519":[]}"#;
        assert!(License::from_json(legacy).is_ok());
        Ok(())
    }
}
