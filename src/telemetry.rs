//! Structured logging. JSON to stdout; journald does storage & rotation.
//!
//! Convention worth adopting from line one: log EVENTS with FIELDS,
//! never interpolated prose. `info!(batch = 512, lag_ms = 3, "flushed")`
//! is queryable in Grafana/Loki; "flushed 512 rows in 3ms" is not.

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init() {
    // RUST_LOG controls verbosity per-module, e.g.:
    //   RUST_LOG=hazshield_ingest=debug,tower_http=info
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("hazshield_ingest=info,tower_http=warn"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json().with_current_span(true))
        .init();
}
