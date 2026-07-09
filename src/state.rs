//! Shared application state — the answer to the /uptime homework, grown up.
//!
//! Axum clones AppState for every handler invocation, hence #[derive(Clone)]
//! and hence every field must be cheap to clone: Instant is Copy, PgPool is
//! internally an Arc, SharedRegistry is an Arc. Cloning this struct costs
//! ~24 bytes of pointer copies. That is the whole trick.

use crate::registry::SharedRegistry;
use sqlx::PgPool;
use std::time::Instant;
#[derive(Clone)]
pub struct AppState {
    pub started: Instant,
    pub pool: PgPool,
    pub registry: SharedRegistry,
}