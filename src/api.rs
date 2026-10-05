use axum::{routing::get, Json, Router};
use serde::Serialize;

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

/// Router principal — fusionne `/health` et `/ws` (ingestion Task 2).
pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(crate::ingest::ws_router())
}
