//! Compteurs par domaine et exposition HTTP via `GET /stats`.
//!
//! `Counters` est partagé entre le routeur WS (qui incrémente à chaque
//! événement valide) et le handler `/stats` (qui lit pour sérialiser).
//! L'intérieur est un `RwLock<HashMap<…>>` pour permettre des lectures
//! concurrentes ; les écritures sont courtes (un incrément) et peu
//! fréquentes (≈ fréquence d'événements WS), donc le coût du lock est
//! négligeable.
//!
//! Format JSON produit par `/stats` (contrat Task 3) :
//! ```json
//! {
//!   "domains": {
//!     "example.com": { "requests": 3, "bytes": 12345 },
//!     "other.org":   { "requests": 1, "bytes": 0 }
//!   },
//!   "total_requests": 4,
//!   "total_bytes":    12345
//! }
//! ```
//!
//! Task 4 : la récupération des compteurs passe par `Extension<VigilCtx>`
//! (cf. `state.rs`) — on n'utilise plus un `State<Counters>` direct, mais
//! l'API publique `Counters` reste inchangée.

use axum::{extract::Extension, http::StatusCode, response::IntoResponse, Json, Router};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::state::VigilCtx;

/// Compteurs agrégés pour un domaine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DomainCounters {
    pub requests: u64,
    pub bytes: u64,
}

/// Compteurs globaux, partagés par clonage de l'`Arc` intérieur.
/// `Counters` est elle-même `Clone` (clone = nouvel `Arc`, même map).
#[derive(Clone, Default)]
pub struct Counters {
    inner: Arc<RwLock<HashMap<String, DomainCounters>>>,
}

impl Counters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Incrémente le compteur d'un domaine. `bytes = None` ajoute 0.
    /// Domaines vides ignorés (sécurité).
    /// Méthode async, à utiliser depuis un handler axum.
    pub async fn record_async(&self, domain: &str, bytes: Option<u64>) {
        if domain.is_empty() {
            return;
        }
        let b = bytes.unwrap_or(0);
        let mut g = self.inner.write().await;
        let entry = g.entry(domain.to_string()).or_default();
        entry.requests = entry.requests.saturating_add(1);
        entry.bytes = entry.bytes.saturating_add(b);
    }

    /// Copie cohérente (sous verrou lecture) de l'état courant.
    /// Méthode async, à utiliser depuis un handler axum.
    pub async fn snapshot_async(&self) -> HashMap<String, DomainCounters> {
        self.inner.read().await.clone()
    }
}

/// Sérialisation du snapshot sous la forme attendue par le contrat.
#[derive(Serialize)]
struct StatsResponse {
    domains: HashMap<String, DomainCounters>,
    total_requests: u64,
    total_bytes: u64,
}

/// Handler `GET /stats`.
async fn stats_handler(Extension(ctx): Extension<VigilCtx>) -> impl IntoResponse {
    let snap = ctx.counters.snapshot_async().await;
    let total_requests: u64 = snap.values().map(|c| c.requests).sum();
    let total_bytes: u64 = snap.values().map(|c| c.bytes).sum();
    let body = StatsResponse {
        domains: snap,
        total_requests,
        total_bytes,
    };
    (StatusCode::OK, Json(body))
}

/// Sous-router `/stats`. Ne pose plus de `with_state` — l'état est fourni
/// par le router racine via `Extension<VigilCtx>` (cf. `api::router`).
pub fn stats_router() -> Router {
    Router::new().route("/stats", axum::routing::get(stats_handler))
}
