//! Configuration: environment variables only, loaded once at startup.
//!
//! Design position: no config files, no hot-reload of *this* struct.
//! Anything that must change at runtime (sensor thresholds) lives in the
//! registry and arrives via Redis invalidation — infrastructure config
//! (ports, URLs, buffer sizes) changes only with a restart. Two config
//! planes, two lifetimes, no ambiguity about which is which.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Bind address for the HTTP listener.
    #[serde(default = "default_bind")]
    pub bind_addr: String,

    /// Postgres DSN — used from Session 2 (registry) onward.
    pub database_url: String,

    /// Redis URL — used from Session 2 (invalidation) onward.
    pub redis_url: String,

    /// Warm-lane channel capacity (readings). Bounded on purpose:
    /// this number IS our backpressure policy. Sessions 4+.
    #[serde(default = "default_channel_capacity")]
    pub warm_channel_capacity: usize,

    /// Max readings per ingest batch request.
    #[serde(default = "default_max_batch")]
    pub max_batch_size: usize,
}

fn default_bind() -> String { "127.0.0.1:8020".into() }
fn default_channel_capacity() -> usize { 50_000 }
fn default_max_batch() -> usize { 2_000 }

impl Config {
    /// Load from environment (HAZ_ prefix), reading .env first if present.
    /// Fails fast and loud: a gateway with half a config must not start.
    pub fn load() -> Result<Self, envy::Error> {
        dotenvy::dotenv().ok(); // absence of .env is fine (systemd sets env)
        envy::prefixed("HAZ_").from_env()
    }
}
