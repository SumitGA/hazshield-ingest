//! Plant seeder: builds a realistic site in Postgres.
//!
//!   1 site -> 18 zones (with an adjacency graph shaped like a real plant:
//!   a spine of processing halls, branches to vent shafts and galleries)
//!   -> ~180 assets -> 3,000 sensors with kind-appropriate thresholds.
//!
//! Idempotent by nuke-and-rebuild inside ONE transaction: either the old
//! plant or the complete new plant is visible — never half a plant. Run it
//! as often as you like:   cargo run --release --bin seed
//!
//! Design note: thresholds are drawn per-sensor from a kind-specific range
//! rather than one constant, so the simulator can push a zone hot and get
//! a realistic STAGGER of alarms instead of 170 simultaneous ones.

use rand::prelude::*;
use sqlx::PgPool;
use uuid::Uuid;

const N_SENSORS: usize = 3000;

/// (kind, unit, warn range, crit multiplier over warn)
const KINDS: &[(&str, &str, std::ops::Range<f64>, f64)] = &[
    ("gas_ch4",   "%LEL", 10.0..20.0,  2.0),
    ("gas_co",    "ppm",  25.0..35.0,  1.8),
    ("o2",        "%vol", 19.0..19.5,  0.9), // low O2 alarms: crit BELOW warn
    ("temp",      "degC", 60.0..75.0,  1.4),
    ("vibration", "mm_s", 4.5..7.0,    1.6),
    ("pressure",  "kPa",  550.0..700.0,1.3),
    ("airflow",   "m3_s", 20.0..30.0,  0.7), // low-flow alarms
    ("dust_pm10", "ug_m3",120.0..180.0,1.5),
    ("seismic",   "mm_s", 2.0..4.0,    2.5),
];

const ZONES: &[(&str, &str)] = &[
    ("Crusher Hall A", "crusher_hall"), ("Crusher Hall B", "crusher_hall"),
    ("Conveyor Gallery 1", "conveyor_gallery"), ("Conveyor Gallery 2", "conveyor_gallery"),
    ("Conveyor Gallery 3", "conveyor_gallery"),
    ("Mill Floor", "processing"), ("Flotation Hall", "processing"),
    ("Thickener Deck", "processing"), ("Reagent Store", "storage"),
    ("Concentrate Shed", "storage"), ("Vent Shaft North", "vent_shaft"),
    ("Vent Shaft South", "vent_shaft"), ("Substation 1", "electrical"),
    ("Substation 2", "electrical"), ("Control Annex", "control"),
    ("Workshop", "maintenance"), ("Tailings Pump House", "pumping"),
    ("Water Treatment", "utilities"),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("HAZ_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("set HAZ_DATABASE_URL");
    let pool = PgPool::connect(&url).await?;
    let mut rng = StdRng::seed_from_u64(9843); // reproducible plants

    let mut tx = pool.begin().await?;

    // Nuke in FK order — all-or-nothing thanks to the transaction.
    for t in ["sensor", "asset", "zone_adjacency", "zone", "site"] {
        sqlx::query(&format!("DELETE FROM {t}")).execute(&mut *tx).await?;
    }

    let site_id: Uuid = sqlx::query_scalar(
        "INSERT INTO site (name) VALUES ($1) RETURNING site_id",
    )
    .bind("Pilbara Processing Facility")
    .fetch_one(&mut *tx)
    .await?;

    // Zones
    let mut zone_ids = Vec::with_capacity(ZONES.len());
    for (name, kind) in ZONES {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO zone (site_id, name, kind) VALUES ($1,$2,$3) RETURNING zone_id",
        )
        .bind(site_id).bind(name).bind(kind)
        .fetch_one(&mut *tx)
        .await?;
        zone_ids.push(id);
    }

    // Adjacency: a spine (each zone touches the next) + cross-links so the
    // AI's isolation planning has real graph structure to reason over.
    let mut edges: Vec<(Uuid, Uuid, &str)> = Vec::new();
    for w in zone_ids.windows(2) {
        edges.push((w[0], w[1], "passage"));
    }
    for &(a, b, kind) in &[(0usize, 5usize, "conveyor"), (1, 5, "conveyor"),
                            (5, 6, "conveyor"), (10, 0, "duct"), (10, 5, "duct"),
                            (11, 6, "duct"), (16, 17, "passage")] {
        edges.push((zone_ids[a], zone_ids[b], kind));
    }
    for (a, b, kind) in edges {
        // Store both directions: simpler queries beat clever storage.
        for (x, y) in [(a, b), (b, a)] {
            sqlx::query(
                "INSERT INTO zone_adjacency (zone_id, adjacent_id, link_kind)
                 VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
            )
            .bind(x).bind(y).bind(kind)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Assets: ~10 per zone, with isolation actions the AI will consume.
    let mut asset_ids: Vec<(Uuid, Uuid)> = Vec::new(); // (asset, zone)
    for &zone_id in &zone_ids {
        for i in 0..10 {
            let name = format!("asset-{}", i + 1);
            let kind = ["ball_mill", "conveyor_drive", "vent_fan", "pump",
                        "screen", "feeder"][i % 6];
            let actions = serde_json::json!([
                {"step": 1, "action": format!("stop {kind}"), "requires": []},
                {"step": 2, "action": "lockout-tagout", "requires": [1]},
            ]);
            let id: Uuid = sqlx::query_scalar(
                "INSERT INTO asset (zone_id, name, kind, isolation_actions)
                 VALUES ($1,$2,$3,$4) RETURNING asset_id",
            )
            .bind(zone_id).bind(&name).bind(kind).bind(&actions)
            .fetch_one(&mut *tx)
            .await?;
            asset_ids.push((id, zone_id));
        }
    }

    // Sensors: batched multi-row inserts, 500 at a time. UUIDv7 so the
    // PK is time-ordered — index locality for every table that FKs us.
    let mut inserted = 0usize;
    let mut batch: Vec<(Uuid, Uuid, Uuid, &str, &str, f32, f64, f64)> = Vec::new();
    for n in 0..N_SENSORS {
        let (asset_id, zone_id) = asset_ids[n % asset_ids.len()];
        let (kind, unit, warn_range, crit_mult) = {
            let k = &KINDS[n % KINDS.len()];
            (k.0, k.1, k.2.clone(), k.3)
        };
        let warn = rng.gen_range(warn_range);
        let crit = warn * crit_mult;
        let hz = if kind == "vibration" { 10.0 } else { 1.0 };
        batch.push((Uuid::now_v7(), asset_id, zone_id, kind, unit, hz, warn, crit));

        if batch.len() == 500 || n == N_SENSORS - 1 {
            let mut qb = sqlx::QueryBuilder::new(
                "INSERT INTO sensor (sensor_id, asset_id, zone_id, kind, unit,
                                     sample_hz, warn_threshold, crit_threshold) ",
            );
            qb.push_values(batch.drain(..), |mut b, s| {
                b.push_bind(s.0).push_bind(s.1).push_bind(s.2)
                 .push_bind(s.3).push_unseparated("::sensor_kind")
                 .push_bind(s.4).push_bind(s.5).push_bind(s.6).push_bind(s.7);
            });
            qb.build().execute(&mut *tx).await?;
            inserted += 500.min(N_SENSORS - inserted);
        }
    }

    tx.commit().await?;
    println!("seeded: 1 site, {} zones, {} assets, {} sensors",
             zone_ids.len(), asset_ids.len(), N_SENSORS);
    Ok(())
}
