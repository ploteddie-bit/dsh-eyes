//! Router HTTP principal : fusionne `/health`, `/ws` et `/stats`.
//!
//! Choix d'architecture (Task 3) : `Counters` est passé explicitement à
//! `router(counters)` et partagé via `axum::extract::State`. C'est plus
//! explicite qu'un `OnceLock` global, facilite les tests, et `Counters`
//! est `Clone` (intérieur `Arc`), donc le passage par valeur est gratuit.

use axum::{routing::get, Json, Router};
use serde::Serialize;

use crate::stats::{Counters, stats_router};

#[derive(Serialize)]
pub struct Health {
    ok: bool,
    version: &'static str,
}

pub async fn health() -> Json<Health> {
    Json(Health {
        ok: true,
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Router principal — fusionne `/health`, `/ws` et `/stats`.
///
/// Le `Counters` est partagé par `/ws` (incrément) et `/stats` (lecture).
pub fn router(counters: Counters) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(crate::ingest::ws_router(counters.clone()))
        .merge(stats_router(counters))
}
