mod config;
mod routes;
mod telemetry;
mod types;
mod state;

use std::time::{Duration, Instant};
use tokio::signal;
use tracing::{info, warn};

fn main() -> anyhow::Result<()> {
    // Explicit runtime construction instead of #[tokio::main]: same thing,
    // but the knobs are visible and ours to tune (worker count on a 2-vCPU
    // VM, thread names for debugging).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("hazshield-worker")
        .enable_all()
        .build()?;

    runtime.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    telemetry::init();
    let cfg = config::Config::load()?;
    info!(bind = %cfg.bind_addr, "hazshield-ingest starting");

    let app_state = state::AppState {
        started: Instant::now(),
    };

    let app = routes::router(app_state);

    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    info!("listening");

    // Graceful shutdown is not polish — it's a core requirement.
    // From Session 4 there will be up to 50k readings buffered in
    // channels; SIGTERM must mean "drain, then exit", never "drop".
    // Session 1 wires the mechanism so the drain has somewhere to live.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // <- Session 4 adds: close ingest channel, await writer task's
    //    final flush, fsync spill file. The ORDER will matter.
    info!("drained and stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install ctrl-c handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => warn!("SIGINT received"),
        _ = terminate => warn!("SIGTERM received"),
    }

    info!(grace = ?Duration::from_secs(10), "beginning graceful shutdown");
}
