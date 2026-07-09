use crate::state::AppState;
use axum::{extract::State, routing::get, Json, Router};
use serde_json::{json, Value};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
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

async fn version() -> Json<Value> {
    Json(json!({
        "service": "hazshield-ingest",
        "version": env!("CARGO_PKG_VERSION"),
        "git_sha": env!("GIT_SHA"),
    }))
}

async fn uptime(State(s): State<AppState>) -> Json<Value> {
    let uptime = std::time::Instant::now()
        .duration_since(s.started)
        .as_secs();
    Json(json!({ "uptime": uptime }))
}