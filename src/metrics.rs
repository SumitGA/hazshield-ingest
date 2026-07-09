//! Prometheus metrics: the gateway's flight instruments.
//!
//! Philosophy (RED method): for the ingest endpoint we track Rate
//! (readings by result), Errors (rejections are a *result label*, not a
//! separate system), Duration (handler latency histogram). Plus the
//! domain metric that matters more than any of them: violations by
//! severity. Grafana reads these in Phase 5; the checkpoint run in
//! Session 7 is judged BY these numbers, so they exist from day one.

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounterVec, Opts, Registry, TextEncoder,
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

        for c in [&readings, &violations] {
            registry.register(Box::new(c.clone())).unwrap();
        }
        registry.register(Box::new(batch_size.clone())).unwrap();
        registry.register(Box::new(handler_seconds.clone())).unwrap();

        Self { registry: Arc::new(registry), readings, violations, batch_size, handler_seconds }
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap_or_default()
    }
}
