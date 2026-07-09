//! The warm lane: bounded channel -> batch writer -> binary COPY.
//!
//! Warm-up 05's batch-or-timeout loop grown up, with three production
//! concerns added:
//!
//!   1. BINARY COPY. Postgres's fastest ingestion path — one round-trip
//!      per flush, rows encoded in PG's binary wire format. We hand-
//!      encode it: it's just big-endian lengths and payloads, and
//!      knowing the format beats a dependency for 30 lines of encoding.
//!
//!   2. RETRY WITHOUT LOSS, BOUNDED. A failed flush keeps the buffer and
//!      retries with backoff (warm-up 05 challenge 3: clear only on
//!      success). But unbounded retry + full channel = memory growth, so
//!      past MAX_BUFFERED the OLDEST bulk rows are shed, counted loudly.
//!      Bulk telemetry is droppable by design; violations never ride
//!      this lane (Session 5 gives them their own).
//!
//!   3. DRAIN ON SHUTDOWN. Channel closes (all senders dropped) ->
//!      recv() yields None -> final flush -> exit. main awaits us with a
//!      deadline that fits inside systemd's TimeoutStopSec.

use crate::metrics::Metrics;
use crate::types::Quality;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use uuid::Uuid;

/// What the warm lane carries: the reading plus its storage-quality flag.
#[derive(Debug)]
pub struct StoredReading {
    pub ts: DateTime<Utc>,
    pub sensor_id: Uuid,
    pub value: f32,
    pub quality: Quality,
}

pub const MAX_BATCH_ROWS: usize = 1_000;
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// Retry-time cap: ~5 batches. Beyond this we shed oldest bulk rows.
pub const MAX_BUFFERED: usize = 5_000;

pub fn spawn_writer(
    pool: PgPool,
    mut rx: mpsc::Receiver<StoredReading>,
    metrics: Metrics,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf: Vec<StoredReading> = Vec::with_capacity(MAX_BATCH_ROWS);
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut backoff_ms: u64 = 0; // 0 = healthy

        loop {
            tokio::select! {
                // Drain the channel in preference to the timer under load.
                biased;

                maybe = rx.recv() => match maybe {
                    Some(r) => {
                        buf.push(r);
                        if buf.len() >= MAX_BATCH_ROWS && backoff_ms == 0 {
                            flush(&pool, &mut buf, &metrics, &mut backoff_ms, "size").await;
                        }
                        // Failure containment: shed oldest, keep newest.
                        if buf.len() > MAX_BUFFERED {
                            let shed = buf.len() - MAX_BUFFERED;
                            buf.drain(..shed);
                            metrics.warm_shed_total.inc_by(shed as u64);
                            warn!(shed, "warm buffer over cap during outage; shed oldest rows");
                        }
                    }
                    None => {
                        // Shutdown drain: senders gone, flush what remains.
                        info!(remaining = buf.len(), "warm lane draining");
                        flush(&pool, &mut buf, &metrics, &mut backoff_ms, "shutdown").await;
                        break;
                    }
                },
                _ = ticker.tick() => {
                    if backoff_ms > 0 {
                        // Postgres was down; ticker doubles as retry pacing.
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                    if !buf.is_empty() {
                        flush(&pool, &mut buf, &metrics, &mut backoff_ms, "timer").await;
                    }
                }
            }
            metrics.warm_buffered.set(buf.len() as i64);
        }
        info!("warm writer exited");
    })
}

async fn flush(
    pool: &PgPool,
    buf: &mut Vec<StoredReading>,
    metrics: &Metrics,
    backoff_ms: &mut u64,
    reason: &str,
) {
    if buf.is_empty() { return; }
    let timer = metrics.warm_flush_seconds.start_timer();

    match copy_rows(pool, buf).await {
        Ok(n) => {
            metrics.warm_flushed_total.inc_by(n);
            info!(rows = n, reason, "warm flush");
            buf.clear();               // ONLY on success (warm-up 05 ch.3)
            *backoff_ms = 0;
        }
        Err(e) => {
            *backoff_ms = (*backoff_ms * 2).clamp(250, 5_000);
            metrics.warm_flush_failures_total.inc();
            error!(error = %e, buffered = buf.len(), next_retry_ms = *backoff_ms,
                   "warm flush failed; buffer retained");
        }
    }
    timer.observe_duration();
}

/// One binary COPY round-trip. copy_in_raw lives on the CONNECTION in
/// sqlx 0.7, so we check one out of the pool for the duration.
async fn copy_rows(pool: &PgPool, rows: &[StoredReading]) -> Result<u64, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let mut copy = conn
        .copy_in_raw(
            "COPY sensor_readings (time, sensor_id, value, quality) \
             FROM STDIN WITH (FORMAT binary)",
        )
        .await?;
    copy.send(encode_binary(rows)).await?;
    copy.finish().await
}

/// Postgres binary COPY format, by hand. Everything is big-endian.
/// Layout: 19-byte header, then per row (i16 field-count, then per field
/// i32 byte-length + payload), then i16 -1 trailer.
fn encode_binary(rows: &[StoredReading]) -> Vec<u8> {
    // timestamptz payload = MICROSECONDS since 2000-01-01 (not 1970!).
    const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

    let mut b = Vec::with_capacity(19 + rows.len() * 50);
    b.extend_from_slice(b"PGCOPY\n\xff\r\n\0");   // magic
    b.extend_from_slice(&0i32.to_be_bytes());      // flags
    b.extend_from_slice(&0i32.to_be_bytes());      // header extension len

    for r in rows {
        b.extend_from_slice(&4i16.to_be_bytes());  // 4 fields

        let micros = r.ts.timestamp_micros() - PG_EPOCH_MICROS;
        b.extend_from_slice(&8i32.to_be_bytes());
        b.extend_from_slice(&micros.to_be_bytes());

        b.extend_from_slice(&16i32.to_be_bytes());
        b.extend_from_slice(r.sensor_id.as_bytes());

        b.extend_from_slice(&4i32.to_be_bytes());
        b.extend_from_slice(&r.value.to_be_bytes());

        b.extend_from_slice(&2i32.to_be_bytes());
        b.extend_from_slice(&(r.quality as i16).to_be_bytes());
    }
    b.extend_from_slice(&(-1i16).to_be_bytes());   // trailer
    b
}
