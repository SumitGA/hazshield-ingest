//! Prometheus metrics: the gateway's flight instruments.
//!
//! Philosophy (RED method): for the ingest endpoint we track Rate
//! (readings by result), Errors (rejections are a *result label*, not a
//! separate system), Duration (handler latency histogram). Plus the
//! domain metric that matters more than any of them: violations by
//! severity. Grafana reads these in Phase 5; the checkpoint run in
//! Session 7 is judged BY these numbers, so they exist from day one.

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    /// result = accepted | unknown_sensor | inactive_sensor
    pub readings: IntCounterVec,
    /// severity = warn | critical
    pub violations: IntCounterVec,
    pub batch_size: Histogram,
    pub handler_seconds: Histogram,
    // ---- warm lane ---
    pub warm_flushed_total: IntCounter,
    pub warm_flush_failures_total: IntCounter,
    pub warm_shed_total: IntCounter,
    pub warm_degraded_total: IntCounterVec,
    pub warm_buffered: IntGauge,
    pub warm_flush_seconds: Histogram,
    // --- hot lane ---
    pub hot_sent_total: IntCounter,
    pub hot_spilled_total: IntCounter,
    pub hot_replayed_total: IntCounter,
    pub hot_lost_total: IntCounter,
    pub hot_redis_up: IntGauge, 
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let readings = IntCounterVec::new(
            Opts::new("ingest_readings_total", "Readings by processing result"),
            &["result"],
        )
        .unwrap();
        let violations = IntCounterVec::new(
            Opts::new("ingest_violations_total", "Threshold violations by severity"),
            &["severity"],
        )
        .unwrap();
        let batch_size = Histogram::with_opts(
            HistogramOpts::new("ingest_batch_size", "Readings per batch")
                .buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0]),
        )
        .unwrap();
        let handler_seconds = Histogram::with_opts(
            HistogramOpts::new("ingest_handler_seconds", "Ingest handler latency")
                // p99 target is 10ms — buckets bracket it so the histogram
                // can actually RESOLVE the target, not just straddle it.
                .buckets(vec![0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1]),
        )
        .unwrap();

        let warm_flushed_total =
            IntCounter::new("warm_flushed_rows_total", "Rows written via COPY").unwrap();
        let warm_flush_failures_total =
            IntCounter::new("warm_flush_failures_total", "Failed COPY flushes").unwrap();
        let warm_shed_total =
            IntCounter::new("warm_shed_rows_total", "Bulk rows shed during outage").unwrap();
        let warm_degraded_total = IntCounterVec::new(
            Opts::new("warm_degraded_total", "Degraded-mode decisions"),
            &["action"], // kept | dropped
        )
        .unwrap();
        let warm_buffered =
            IntGauge::new("warm_buffered_rows", "Rows waiting in writer buffer").unwrap();
        let warm_flush_seconds = Histogram::with_opts(
            HistogramOpts::new("warm_flush_seconds", "COPY flush latency")
                .buckets(vec![0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25]),
        )
        .unwrap();
        registry.register(Box::new(warm_flushed_total.clone())).unwrap();
        registry.register(Box::new(warm_flush_failures_total.clone())).unwrap();
        registry.register(Box::new(warm_shed_total.clone())).unwrap();
        registry.register(Box::new(warm_degraded_total.clone())).unwrap();
        registry.register(Box::new(warm_buffered.clone())).unwrap();
        registry.register(Box::new(warm_flush_seconds.clone())).unwrap();

        let hot_sent_total =
            IntCounter::new("hot_violations_sent_total", "Violations XADDed to the stream").unwrap();
        let hot_spilled_total =
            IntCounter::new("hot_violations_spilled_total", "Violations written to spill file").unwrap();
        let hot_replayed_total =
            IntCounter::new("hot_violations_replayed_total", "Spilled violations replayed to stream").unwrap();
        let hot_lost_total =
            IntCounter::new("hot_violations_lost_total", "Violations LOST (spill unwritable) — must stay 0").unwrap();
        let hot_redis_up = IntGauge::new("hot_redis_up", "1 when the hot lane has Redis").unwrap();

        for c in [&hot_sent_total, &hot_spilled_total, &hot_replayed_total, &hot_lost_total] {
            registry.register(Box::new(c.clone())).unwrap();
        }
        registry.register(Box::new(hot_redis_up.clone())).unwrap();

        for c in [&readings, &violations] {
            registry.register(Box::new(c.clone())).unwrap();
        }
        registry.register(Box::new(batch_size.clone())).unwrap();
        registry.register(Box::new(handler_seconds.clone())).unwrap();

        Self { registry: Arc::new(registry), readings, violations, batch_size, handler_seconds,
            warm_flushed_total, warm_flush_failures_total, warm_shed_total, warm_degraded_total,
            warm_buffered, warm_flush_seconds, hot_sent_total, hot_spilled_total, hot_replayed_total, 
            hot_lost_total, hot_redis_up 
        }
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap_or_default()
    }
}
