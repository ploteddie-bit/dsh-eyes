//! Router HTTP principal : fusionne `/health`, `/ws`, `/stats`, `/events`.
//!
//! Choix d'architecture (Task 3 → Task 4) :
//! - `VigilCtx` (cf. `state.rs`) agrège `Counters`, `Allowlist`,
//!   `SharedSession` et `AlertBuffer`. C'est plus simple que de propager
//!   4 `State<...>` distincts (axum 0.7 n'autorise qu'un `with_state`
//!   par `Router`). On l'attache au router racine via
//!   `.layer(Extension(ctx))` — tous les sous-routers y accèdent
//!   gratuitement sans avoir à le re-passer.
//! - Les sous-routers `ws_router()`, `stats_router()` ne posent plus
//!   de `with_state` ; ils lisent l'extension.

use axum::{
    extract::Extension,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;

use crate::state::VigilCtx;
use crate::vigil;

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

/// Nombre max d'alertes servies par `/events` (cf. brief Task 4).
const EVENTS_TAIL: usize = 50;

/// Handler `GET /events` — retourne les N dernières alertes en JSON.
///
/// Priorité de lecture : buffer en mémoire (`ctx.alerts.tail(N)`) — c'est la
/// source de vérité pour la session courante. Fallback `alerts.jsonl` sur
/// disque : si le buffer est vide (par ex. daemon fraîchement redémarré)
/// on relit l'historique persistant des alertes `critical`.
async fn events_handler(Extension(ctx): Extension<VigilCtx>) -> impl IntoResponse {
    let from_buffer = ctx.alerts.tail(EVENTS_TAIL).await;
    if !from_buffer.is_empty() {
        return (StatusCode::OK, Json(serde_json::json!(from_buffer))).into_response();
    }
    // Fallback disque : on lit alerts.jsonl (cf. brief : « 50 dernières
    // alertes »). `tail_jsonl` gère le cas « fichier absent ».
    let path = match vigil::alerts_path() {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("chemin alerts invalide: {e}"),
            )
                .into_response();
        }
    };
    match vigil::tail_jsonl(&path, EVENTS_TAIL) {
        Ok(values) => (StatusCode::OK, Json(values)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("lecture alerts: {e}"),
        )
            .into_response(),
    }
}

/// Router principal — fusionne `/health`, `/ws`, `/stats`, `/events`.
pub fn router(ctx: VigilCtx) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events", get(events_handler))
        .merge(crate::ingest::ws_router())
        .merge(crate::stats::stats_router())
        .layer(Extension(ctx))
}
