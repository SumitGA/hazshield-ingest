use axum::{routing::get, Json, Router};
use serde_json::{json, Value};

pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(version))
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
    }))
}
