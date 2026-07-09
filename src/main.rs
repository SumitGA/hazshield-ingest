mod config;
mod routes;
mod telemetry;
mod types;
mod state;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal;
use tracing::{info, warn};

fn main() -> anyhow::Result<()> {
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

    // connect, load registry, arm the doorbell
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4) // gateway shares the DB with everyone; be polite
        .acquire_timeout(Duration::from_secs(5))
        .connect(&cfg.database_url)
        .await?;
    info!("postgres pool connected");   

    // Fail fast if the registry can't load at startup: a gateway that
    // can't validate sensors shouldn't accept traffic at all.
    let initial = registry::Registry::load(&pool, 1).await?;
    let shared: registry::SharedRegistry =
        Arc::new(arc_swap::ArcSwap::from_pointee(initial));

    registry::spawn_invalidation_listener(
        cfg.redis_url.clone(),
        pool.clone(),
        Arc::clone(&shared),
    );

    let app_state = state::AppState {
        started: Instant::now(),
        pool,
        registry: shared,
    };

    let app = routes::router(app_state);
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    info!("listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

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
