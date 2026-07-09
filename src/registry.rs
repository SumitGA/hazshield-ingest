//! The sensor registry: warm-up 03's ArcSwap pattern in its production seat.
//!
//! Lifecycle:
//!   1. At startup, `Registry::load` pulls every sensor from Postgres into
//!      an immutable snapshot.
//!   2. The hot path calls `state.registry.load()` — a wait-free atomic
//!      read. No locks, ever, on the ingest path.
//!   3. `spawn_invalidation_listener` subscribes to a Redis channel; any
//!      message means "config changed" -> rebuild from Postgres -> one
//!      atomic store(). In-flight requests keep their old snapshot.
//!
//! Note what the invalidation message does NOT carry: the new data. It's
//! a doorbell, not a delivery. Postgres stays the single source of truth,
//! and a lost pub/sub message costs one stale interval, not correctness
//! (a periodic refresh backstop arrives with the metrics work later).

use arc_swap::ArcSwap;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Everything the hot path needs to know about one sensor.
/// Copy-friendly and small: ~64 bytes * 3000 sensors ≈ 200KB total.
#[derive(Debug, Clone)]
pub struct SensorMeta {
    pub zone_id: Uuid,
    pub warn_threshold: f64,
    pub crit_threshold: f64,
    pub active: bool,
}

#[derive(Debug, Default)]
pub struct Registry {
    pub sensors: HashMap<Uuid, SensorMeta>,
    /// Monotonic generation counter — shows up in /readyz and logs so
    /// "which config is live" is always answerable.
    pub generation: u64,
}

impl Registry {
    pub async fn load(pool: &PgPool, generation: u64) -> Result<Self, sqlx::Error> {
        // One query, whole table. 3k rows is nothing; even 300k would be
        // fine at startup. Rebuild-then-swap beats incremental mutation.
        let rows: Vec<(Uuid, Uuid, f64, f64, String)> = sqlx::query_as(
            r#"SELECT sensor_id, zone_id, warn_threshold, crit_threshold,
                      status::text
               FROM sensor"#,
        )
        .fetch_all(pool)
        .await?;

        let sensors = rows
            .into_iter()
            .map(|(sensor_id, zone_id, warn_threshold, crit_threshold, status)| {
                (
                    sensor_id,
                    SensorMeta {
                        zone_id,
                        warn_threshold,
                        crit_threshold,
                        active: status == "active",
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        info!(count = sensors.len(), generation, "registry loaded");
        Ok(Self { sensors, generation })
    }
}

pub type SharedRegistry = Arc<ArcSwap<Registry>>;

/// Redis channel name — the FastAPI control plane publishes here when
/// thresholds/sensors change (Phase 3). Until then: redis-cli PUBLISH.
pub const INVALIDATION_CHANNEL: &str = "hazshield:registry:invalidate";

/// Background task: doorbell listener. Owns its own Redis connection.
pub fn spawn_invalidation_listener(
    redis_url: String,
    pool: PgPool,
    registry: SharedRegistry,
) {
    tokio::spawn(async move {
        loop {
            match listen(&redis_url, &pool, &registry).await {
                Ok(()) => warn!("invalidation stream ended; reconnecting"),
                Err(e) => error!(error = %e, "invalidation listener failed; retrying in 5s"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            // On reconnect we may have MISSED a doorbell — so reload once,
            // unconditionally. Cheap insurance against the gap.
            reload(&pool, &registry).await;
        }
    });
}

async fn listen(
    redis_url: &str,
    pool: &PgPool,
    registry: &SharedRegistry,
) -> anyhow::Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(INVALIDATION_CHANNEL).await?;
    info!(channel = INVALIDATION_CHANNEL, "invalidation listener subscribed");

    use futures_util::StreamExt;
    let mut stream = pubsub.on_message();
    while let Some(_msg) = stream.next().await {
        // Payload ignored on purpose: doorbell, not delivery.
        reload(pool, registry).await;
    }
    Ok(())
}

async fn reload(pool: &PgPool, registry: &SharedRegistry) {
    let next_gen = registry.load().generation + 1;
    match Registry::load(pool, next_gen).await {
        // THE swap: one atomic store. Readers mid-batch keep the old
        // Arc; the next .load() anywhere sees the new one. (Warm-up 03.)
        Ok(fresh) => registry.store(Arc::new(fresh)),
        // Reload failing must NOT take down the gateway — we keep
        // serving with the previous snapshot and say so loudly.
        Err(e) => error!(error = %e, "registry reload failed; keeping previous snapshot"),
    }
}