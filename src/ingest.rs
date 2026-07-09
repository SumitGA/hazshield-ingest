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

use crate::{error::IngestError, state::AppState, types::*};
use axum::{extract::State, Json};
use serde::Serialize;
use tracing::debug;

#[derive(Debug, Serialize, Default)]
pub struct IngestSummary {
    pub accepted: usize,
    pub rejected_unknown: usize,
    pub rejected_inactive: usize,
    pub violations_warn: usize,
    pub violations_critical: usize,
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

    for r in &batch.readings {
        let Some(meta) = reg.sensors.get(&r.sensor_id) else {
            sum.rejected_unknown += 1;
            continue;
        };
        if !meta.active {
            sum.rejected_inactive += 1;
            continue;
        }
        sum.accepted += 1;

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
            // Session 5: violation -> hot-lane channel (XADD + pub/sub).
            // Until then: visible in logs at debug, counted in metrics.
            debug!(sensor = %violation.sensor_id, zone = %violation.zone_id,
                   severity = ?violation.severity, value = violation.value,
                   "violation detected");
        }
        // Session 4: accepted reading -> warm-lane channel (batch writer).
    }

    s.metrics.readings.with_label_values(&["accepted"]).inc_by(sum.accepted as u64);
    s.metrics.readings.with_label_values(&["unknown_sensor"]).inc_by(sum.rejected_unknown as u64);
    s.metrics.readings.with_label_values(&["inactive_sensor"]).inc_by(sum.rejected_inactive as u64);
    s.metrics.violations.with_label_values(&["warn"]).inc_by(sum.violations_warn as u64);
    s.metrics.violations.with_label_values(&["critical"]).inc_by(sum.violations_critical as u64);

    timer.observe_duration();
    Ok(Json(sum))
}
