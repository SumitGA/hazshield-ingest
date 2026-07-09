//! Shared application state — the answer to the /uptime homework, grown up.
//!
//! Axum clones AppState for every handler invocation, hence #[derive(Clone)]
//! and hence every field must be cheap to clone: Instant is Copy, PgPool is
//! internally an Arc, SharedRegistry is an Arc. Cloning this struct costs
//! ~24 bytes of pointer copies. That is the whole trick.

use crate::{metrics::Metrics, registry::SharedRegistry, warm::StoredReading};
use sqlx::PgPool;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub started: Instant,
    pub pool: PgPool,
    pub registry: SharedRegistry,
    pub metrics: Metrics,
    pub max_batch_size: usize,
    /// Warm-lane sender. Dropping the LAST clone (router teardown on
    /// shutdown) is what closes the channel and triggers the drain.
    pub warm_tx: mpsc::Sender<StoredReading>,
    pub warm_capacity: usize,
    /// Round-robin counter for 1-in-N sampling in degraded mode.
    pub degrade_seq: Arc<AtomicU64>,
}