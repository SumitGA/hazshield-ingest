//! Domain types — the vocabulary of the whole system.
//!
//! These mirror the Postgres schema deliberately. The enums are the same
//! enums, the field names are the same names. When the wire format, the
//! code, and the database share one vocabulary, an entire class of
//! translation bugs stops existing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One sensor reading as it arrives on the wire.
/// Kept intentionally narrow — this struct will exist in memory
/// ~50k times at peak, so every field earns its bytes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Reading {
    pub sensor_id: Uuid,
    /// Sensor's own timestamp (event time), NOT arrival time.
    /// The distinction matters the moment anything is delayed or replayed.
    pub ts: DateTime<Utc>,
    pub value: f32,
}

/// A batch as POSTed by collectors/simulator: the unit of ingestion.
#[derive(Debug, Deserialize)]
pub struct ReadingBatch {
    pub readings: Vec<Reading>,
}

/// Mirrors the `sensor_kind` Postgres enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorKind {
    GasCh4,
    GasCo,
    O2,
    Temp,
    Vibration,
    Pressure,
    Airflow,
    DustPm10,
    Seismic,
}

/// Severity of a threshold breach — mirrors `alarm_severity` in Postgres.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Warn,
    Critical,
}

/// A detected violation: the hot-lane event. Born in Session 3's
/// threshold evaluator, transported in Session 5.
#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub sensor_id: Uuid,
    pub zone_id: Uuid,
    pub severity: Severity,
    pub value: f32,
    pub threshold: f64,
    pub ts: DateTime<Utc>,
}

/// Quality flag persisted with each reading (mirrors smallint in schema).
#[derive(Debug, Clone, Copy)]
#[repr(i16)]
pub enum Quality {
    Good = 0,
    Suspect = 1,
    DegradedSampled = 2,
}
