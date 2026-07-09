//! Ingest error taxonomy — warm-up 06's doctrine in production.
//!
//! Design line: BATCH-level problems are errors (typed, mapped to HTTP
//! statuses here). READING-level problems are NOT errors — they're
//! counted outcomes in a 200 response. A batch of 500 readings with 3
//! unknown sensors ingests 497 and reports the 3; failing the whole
//! batch would punish 497 good readings for 3 bad ones, and machine
//! clients retrying whole batches on 4xx would re-send the good ones.

use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("batch of {got} exceeds max {max}")]
    BatchTooLarge { got: usize, max: usize },
}

impl IntoResponse for IngestError {
    fn into_response(self) -> Response {
        let status = match &self {
            IngestError::BatchTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
