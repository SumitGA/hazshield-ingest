//! The ingest endpoint: where readings meet thresholds.
//!
//! Hot-path anatomy, per batch:
//!   1. ONE registry snapshot load (wait-free) — every reading in the
//!      batch is judged against the SAME consistent config, even if a
//!      hot-swap lands mid-batch.
//!   2. Per reading: one HashMap lookup + two float compares. No locks,
//!      no allocation beyond the response counters, no I/O.
//!   3. Violations are detected here; TRANSPORT is Session 5 (hot lane),
//!      storage of accepted readings is Session 4 (warm lane). Today
//!      they're counted, logged, and dropped — scaffolding honestly
//!      labeled.
//!
//! Threshold direction: most sensors alarm HIGH (gas, temp, vibration),
//! but o2 and airflow alarm LOW — danger is *below* the line. Rather
//! than schema-tagging direction, we infer it: crit >= warn means a
//! high-alarm sensor, crit < warn means low-alarm. The seeder built the
//! data this way on purpose; one derived bool, zero extra columns.

use crate::warm::StoredReading;
use crate::{error::IngestError, state::AppState, types::*};
use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::atomic::Ordering;
use tracing::debug;

#[derive(Debug, Serialize, Default)]
pub struct IngestSummary {
    pub accepted: usize,
    pub rejected_unknown: usize,
    pub rejected_inactive: usize,
    pub violations_warn: usize,
    pub violations_critical: usize,
    /// Evaluated for safety but NOT stored: over the sensor's rate budget.
    pub rate_limited: usize,
    /// Rejected outright: non-finite value (inf/overflow) — untrustable.
    pub rejected_invalid: usize,    
    /// True if the gateway is currently sampling bulk storage (1-in-10).
    pub degraded: bool,
}

pub async fn ingest_batch(
    State(s): State<AppState>,
    Json(batch): Json<ReadingBatch>,
) -> Result<Json<IngestSummary>, IngestError> {
    let timer = s.metrics.handler_seconds.start_timer();

    if batch.readings.len() > s.max_batch_size {
        return Err(IngestError::BatchTooLarge {
            got: batch.readings.len(),
            max: s.max_batch_size,
        });
    }
    s.metrics.batch_size.observe(batch.readings.len() as f64);

    // One snapshot for the whole batch (warm-up 03, challenge 1).
    let reg = s.registry.load();

    let mut sum = IngestSummary::default();
    
    // Degradation watermark: below 20% channel headroom, storage drops
    // to 1-in-10 sampling. THRESHOLD EVALUATION IS NOT AFFECTED — every
    // reading below still runs the full safety check. Storage fidelity
    // bends; safety never does. This check is per-batch: cheap, and a
    // batch is our unit of consistency.
    let degraded = s.warm_tx.capacity() < s.warm_capacity / 5;
    sum.degraded = degraded;

    for r in &batch.readings {
        let Some(meta) = reg.sensors.get(&r.sensor_id) else {
            sum.rejected_unknown += 1;
            continue;
        };
        if !meta.active {
            sum.rejected_inactive += 1;
            continue;
        }
        // Physical sanity: JSON happily parses 1e39 into f32::INFINITY.
        // A non-finite value can't be evaluated OR stored — reject.
        if !r.value.is_finite() {
            sum.rejected_invalid += 1;
            continue;
        }
        sum.accepted += 1;

        // Rate limiting gates STORAGE only — evaluation always runs.
        let within_budget = s.limiter.allow(r.sensor_id, meta.sample_hz);
        if !within_budget {
            sum.rate_limited += 1;
        }

        // --- inline threshold evaluation: the 5ms promise starts here ---
        let v = r.value as f64;
        let high_alarm = meta.crit_threshold >= meta.warn_threshold;
        let severity = if high_alarm {
            if v >= meta.crit_threshold { Some(Severity::Critical) }
            else if v >= meta.warn_threshold { Some(Severity::Warn) }
            else { None }
        } else {
            if v <= meta.crit_threshold { Some(Severity::Critical) }
            else if v <= meta.warn_threshold { Some(Severity::Warn) }
            else { None }
        };

        if let Some(sev) = severity {
            match sev {
                Severity::Warn => sum.violations_warn += 1,
                Severity::Critical => sum.violations_critical += 1,
            }
            let violation = Violation {
                sensor_id: r.sensor_id,
                zone_id: meta.zone_id,
                severity: sev,
                value: r.value,
                threshold: if matches!(sev, Severity::Critical) {
                    meta.crit_threshold
                } else {
                    meta.warn_threshold
                },
                ts: r.ts,
            };
            debug!(sensor = %violation.sensor_id, zone = %violation.zone_id,
                   severity = ?violation.severity, value = violation.value,
                   "violation detected");
            // Hot lane: send().await — if the dispatcher is saturated we
            // BLOCK this request rather than drop an alarm. Err only if
            // the dispatcher is gone (shutdown race): spill via metrics
            // visibility is handled there; here we surface loudly.
            if s.hot_tx.send(violation).await.is_err() {
                s.metrics.hot_lost_total.inc();
                tracing::error!("hot lane closed while ingesting — violation lost");
            }
            
        }

        if !within_budget {
            continue;
        }

        // --- warm lane: hand the reading to the batch writer ---
        let (store, quality) = if degraded {
            let n = s.degrade_seq.fetch_add(1, Ordering::Relaxed);
            if n % 10 == 0 {
                (true, Quality::DegradedSampled)
            } else {
                s.metrics.warm_degraded_total.with_label_values(&["dropped"]).inc();
                (false, Quality::Good)
            }
        } else {
            (true, Quality::Good)
        };

        if store {
            if degraded {
                s.metrics.warm_degraded_total.with_label_values(&["kept"]).inc();
            }
            // try_send: the handler NEVER blocks on storage (warm-up 04).
            // Full despite the watermark = burst won the race; count it.
            if s.warm_tx.try_send(StoredReading {
                ts: r.ts, sensor_id: r.sensor_id, value: r.value, quality,
            }).is_err() {
                s.metrics.warm_degraded_total.with_label_values(&["dropped"]).inc();
            }
        }
    }

    s.metrics.readings.with_label_values(&["accepted"]).inc_by(sum.accepted as u64);
    s.metrics.readings.with_label_values(&["unknown_sensor"]).inc_by(sum.rejected_unknown as u64);
    s.metrics.readings.with_label_values(&["inactive_sensor"]).inc_by(sum.rejected_inactive as u64);
    s.metrics.violations.with_label_values(&["warn"]).inc_by(sum.violations_warn as u64);
    s.metrics.violations.with_label_values(&["critical"]).inc_by(sum.violations_critical as u64);

    timer.observe_duration();
    Ok(Json(sum))
}
