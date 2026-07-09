use crate::state::AppState;
use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use serde_json::{json, Value};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/ingest", post(crate::ingest::ingest_batch))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .route("/version", get(version))
        .route("/uptime", get(uptime))
        .with_state(state)
}

/// Liveness: "the process is up and the runtime schedules tasks".
/// Readiness (DB reachable, registry loaded) is a different question
/// and gets its own endpoint in Session 2.
async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(s): State<AppState>) -> (StatusCode, Json<Value>) {
    let reg = s.registry.load();
    let db_ok = sqlx::query("SELECT 1").execute(&s.pool).await.is_ok();

    let ready = db_ok && !reg.sensors.is_empty();
    let code = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (
        code, 
        Json(json!({
            "ready": ready,
            "db": db_ok,
            "registry_sensors": reg.sensors.len(),
            "registry_generation": reg.generation,
        }))
    )
}

async fn uptime(State(s): State<AppState>) -> Json<Value> {
    Json(json!({ "uptime_seconds": s.started.elapsed().as_secs()}))
}

async fn version() -> Json<Value> {
    Json(json!({
        "service": "hazshield-ingest",
        "version": env!("CARGO_PKG_VERSION"),
        "git_sha": env!("GIT_SHA"),
    }))
}

async fn metrics(State(s): State<AppState>) -> String {
    s.metrics.render()
}
