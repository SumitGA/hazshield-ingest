//! The hot lane: violations -> Redis Stream + pub/sub, with a local
//! spill file so "never dropped" survives Redis dying.
//!
//! Contrast with the warm lane, deliberately:
//!
//!   WARM (bulk):  try_send, shed under pressure, quality flags.
//!   HOT (alarms): send().await — if this lane saturates, we BLOCK the
//!                 HTTP caller. Backpressure is the correct behavior for
//!                 precious data: the sensor gateway retries, and slow
//!                 truth beats fast silence for safety events.
//!
//! Transport per violation, when Redis is up:
//!   1. XADD hazshield:violations MAXLEN ~100000  (durable; Phase 3's
//!      workers read this with a consumer group + XAUTOCLAIM)
//!   2. PUBLISH hazshield:violations:live          (fire-and-forget for
//!      the Phase 4 UI; lost messages cost a screen update, not truth)
//!
//! When Redis is down: append JSON lines to the spool file. A reconnect
//! loop probes every 2s; on recovery it REPLAYS the spill into the
//! stream FIRST (oldest violations first), then resumes live traffic.
//! The stream is at-least-once by design — replay after a partial
//! failure may duplicate; consumers dedupe on (sensor_id, ts).

use crate::metrics::Metrics;
use crate::types::Violation;
use redis::aio::MultiplexedConnection;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub const STREAM: &str = "hazshield:violations";
pub const LIVE_CHANNEL: &str = "hazshield:violations:live";
const STREAM_MAXLEN: usize = 100_000;

pub fn spawn_dispatcher(
    redis_url: String,
    spool_dir: PathBuf,
    mut rx: mpsc::Receiver<Violation>,
    metrics: Metrics,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        std::fs::create_dir_all(&spool_dir).ok();
        let spill_path = spool_dir.join("violations.spill");
        let client = match redis::Client::open(redis_url.as_str()) {
            Ok(c) => c,
            Err(e) => { error!(error = %e, "bad redis url; hot lane spilling only"); 
                        run_spill_only(&spill_path, &mut rx, &metrics).await; return; }
        };

        let mut conn: Option<MultiplexedConnection> = None;
        let mut probe = tokio::time::interval(std::time::Duration::from_secs(2));

        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(v) => {
                        dispatch(&mut conn, &spill_path, &v, &metrics).await;
                        metrics.hot_redis_up.set(conn.is_some() as i64);
                    }
                    None => {
                        // Shutdown drain: channel closed; nothing buffered
                        // here (we dispatch one-by-one), just exit.
                        info!("hot dispatcher exiting");
                        break;
                    }
                },
                _ = probe.tick(), if conn.is_none() => {
                    match client.get_multiplexed_async_connection().await {
                        Ok(mut c) => {
                            info!("redis reconnected; replaying spill");
                            if replay_spill(&mut c, &spill_path, &metrics).await {
                                conn = Some(c);
                                metrics.hot_redis_up.set(1);
                            }
                        }
                        Err(_) => { /* still down; keep spilling */ }
                    }
                }
            }
        }
    })
}

async fn dispatch(
    conn: &mut Option<MultiplexedConnection>,
    spill_path: &PathBuf,
    v: &Violation,
    metrics: &Metrics,
) {
    let payload = serde_json::to_string(v).expect("violation serializes");

    if let Some(c) = conn.as_mut() {
        match xadd_and_publish(c, &payload).await {
            Ok(()) => { metrics.hot_sent_total.inc(); return; }
            Err(e) => {
                warn!(error = %e, "redis send failed; hot lane entering spill mode");
                *conn = None;
                metrics.hot_redis_up.set(0);
            }
        }
    }
    spill(spill_path, &payload, metrics);
}

async fn xadd_and_publish(
    c: &mut MultiplexedConnection,
    payload: &str,
) -> redis::RedisResult<()> {
    // Durable leg first: if only one leg survives, it must be the stream.
    redis::cmd("XADD").arg(STREAM)
        .arg("MAXLEN").arg("~").arg(STREAM_MAXLEN)
        .arg("*").arg("v").arg(payload)
        .query_async::<_, ()>(c).await?;
    // Best-effort leg: UI liveness. Failure here is not a spill event.
    let _ : redis::RedisResult<i64> =
        redis::cmd("PUBLISH").arg(LIVE_CHANNEL).arg(payload).query_async(c).await;
    Ok(())
}

/// Append one JSON line. Sync I/O on the async runtime is acceptable
/// here: this is the RARE path (Redis down), writes are tiny appends,
/// and losing the violation to an .await cancellation would be worse.
fn spill(path: &PathBuf, payload: &str, metrics: &Metrics) {
    use std::io::Write;
    match std::fs::OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut f) => {
            if writeln!(f, "{payload}").is_ok() {
                metrics.hot_spilled_total.inc();
            } else {
                metrics.hot_lost_total.inc();
                error!("SPILL WRITE FAILED — violation lost; page someone");
            }
        }
        Err(e) => {
            metrics.hot_lost_total.inc();
            error!(error = %e, "SPILL OPEN FAILED — violation lost; page someone");
        }
    }
}

/// Replay the spill into the stream, oldest first. Returns true when the
/// file is fully drained (or absent). Partial failure rewrites the file
/// with only the unsent remainder — no line is ever replayed-and-kept.
async fn replay_spill(
    c: &mut MultiplexedConnection,
    path: &PathBuf,
    metrics: &Metrics,
) -> bool {
    let Ok(content) = std::fs::read_to_string(path) else { return true };
    let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    if lines.is_empty() { let _ = std::fs::remove_file(path); return true; }

    for (i, line) in lines.iter().enumerate() {
        if xadd_and_publish(c, line).await.is_err() {
            // Redis died mid-replay: keep exactly the unsent tail.
            let remainder = lines[i..].join("\n") + "\n";
            let _ = std::fs::write(path, remainder);
            warn!(replayed = i, remaining = lines.len() - i, "replay interrupted");
            metrics.hot_replayed_total.inc_by(i as u64);
            return false;
        }
    }
    metrics.hot_replayed_total.inc_by(lines.len() as u64);
    info!(replayed = lines.len(), "spill fully replayed");
    let _ = std::fs::remove_file(path);
    true
}

/// Degenerate mode: config had an unparseable Redis URL. Never drop.
async fn run_spill_only(path: &PathBuf, rx: &mut mpsc::Receiver<Violation>, m: &Metrics) {
    while let Some(v) = rx.recv().await {
        spill(path, &serde_json::to_string(&v).unwrap(), m);
    }
}
