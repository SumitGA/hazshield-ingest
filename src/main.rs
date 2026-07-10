mod config;
mod error;
mod hot;
mod ingest;
mod limiter;
mod metrics;
mod registry;
mod routes;
mod state;
mod telemetry;
mod types;
mod warm;

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
    let metrics_handle = metrics::Metrics::new();

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

    let (warm_tx, warm_rx) = tokio::sync::mpsc::channel(cfg.warm_channel_capacity);
    let writer = warm::spawn_writer(pool.clone(), warm_rx, metrics_handle.clone());

    // Hot lane: sized for alarm storms (10k in flight), not steady state.
    let (hot_tx, hot_rx) = tokio::sync::mpsc::channel(10_000);
    let dispatcher = hot::spawn_dispatcher(
        cfg.redis_url.clone(),
        std::path::PathBuf::from(&cfg.spool_dir),
        hot_rx,
        metrics_handle.clone(),
    );

    let app_state = state::AppState {
        metrics: metrics_handle.clone(),
        max_batch_size: cfg.max_batch_size,
        warm_tx,
        warm_capacity: cfg.warm_channel_capacity,
        hot_tx,
        limiter: std::sync::Arc::new(limiter::RateLimiter::new()),
        degrade_seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

    // Serve returned: the router (and with it every warm_tx clone) is
    // dropped -> channel closes -> writer sees None -> final flush.
    // We await that drain with a deadline INSIDE systemd's 15s window.
    let drains = async { let _ = writer.await; let _ = dispatcher.await; };
    match tokio::time::timeout(Duration::from_secs(10), drains).await {
        Ok(_) => info!("drained and stopped"),
        Err(_) => warn!("drain deadline exceeded; exiting with rows possibly buffered"),
    }
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
