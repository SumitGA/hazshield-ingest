//! Per-sensor token buckets: the flood shield.
//!
//! Threat model: a malfunctioning or compromised sensor declared at 1Hz
//! starts screaming at 100Hz. Without a limiter, one bad device eats the
//! warm channel, the writer, and the disk — a single-sensor DoS on the
//! whole plant's storage.
//!
//! Policy (the important part):
//!   - The limiter gates STORAGE, not SAFETY. Every reading is still
//!     threshold-evaluated; a real gas leak reported by a chattering
//!     sensor still alarms. Rate-limited readings are evaluated,
//!     counted, and NOT stored. The asymmetry, one more time.
//!   - Budget derives from the sensor's DECLARED sample_hz (registry):
//!     refill = 2x declared rate (grace for clock skew and batching),
//!     burst = 5 seconds of declared rate (batches arrive in clumps).
//!
//! Mechanics: classic token bucket, lazily refilled on access. One
//! DashMap entry per sensor (~3k entries, bounded by the registry — no
//! eviction needed). DashMap = sharded locking; two readings from
//! DIFFERENT sensors never contend, and same-sensor contention is the
//! thing being rate-limited anyway.

use dashmap::DashMap;
use std::time::Instant;
use uuid::Uuid;

struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct RateLimiter {
    buckets: DashMap<Uuid, Bucket>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self { buckets: DashMap::new() }
    }

    /// true = within budget (store it), false = over budget (evaluate
    /// only). `declared_hz` comes from the registry snapshot.
    pub fn allow(&self, sensor_id: Uuid, declared_hz: f32) -> bool {
        let rate = (declared_hz as f64 * 2.0).max(1.0);   // tokens/sec
        let burst = (declared_hz as f64 * 5.0).max(5.0);  // bucket cap
        let now = Instant::now();

        let mut b = self.buckets.entry(sensor_id).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * rate).min(burst);
        b.last = now;

        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
