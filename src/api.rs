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
//!
//! v2 (Task 1) :
//! - `router_with_v2(ctx, config)` ajoute le sous-router `/v2` **monté
//!   derrière** `auth::require_token`. Les routes v1 (`/health`, `/ws`,
//!   `/stats`, `/events`) restent **inchangées et ouvertes**.
//! - Le token est passé au middleware via `Extension(AuthToken(...))`.
//! - `router(ctx)` reste intact pour la compat v1 (signature stable,
//!   12 tests verts à préserver).

use axum::{
    extract::Extension,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;

use crate::auth::{require_token, AuthToken};
use crate::config::Config;
use crate::hands::browser::{BrowserPool, BrowserStatus};
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

/// Handler `GET /v2/ping` — endpoint trivial pour tester l'auth.
async fn v2_ping() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

/// Handler `GET /v2/browser/status` — snapshot JSON-safe du pool navigateur.
///
/// Comportement :
/// - Appelle d'abord `ensure_running()` (idempotent : ne relance que si
///   crash détecté) — c'est le contrat du brief Task 2 « kill du process
///   Chrome simulé → ensure_running relance au prochain appel ».
/// - 200 + `{"running":true,"pages":N,"pid":P}` si Chrome tourne.
/// - 200 + `{"running":false}` (sans pages ni pid) si le pool n'a pas
///   pu lancer Chrome (erreur loggée mais on ne fait pas échouer le GET —
///   sinon un client qui poll le status en boucle pendant un crash
///   recevrait du 500 spam).
async fn v2_browser_status(Extension(pool): Extension<BrowserPool>) -> Json<BrowserStatus> {
    if let Err(e) = pool.ensure_running().await {
        // Log seulement : on ne fait pas échouer le GET. L'utilisateur
        // verra `running:false` et pourra diagnostiquer via les logs.
        tracing::warn!(error = %e, "ensure_running a échoué (status only)");
    }
    Json(pool.status().await)
}

/// Construit le sous-router `/v2` avec l'auth middleware appliqué.
/// Le token attendu est transporté par `Extension(AuthToken(...))` posé
/// sur ce sous-router (pas sur le router racine — ça reste confined à /v2).
/// Le `BrowserPool` est transporté par `Extension(BrowserPool)` (clone = partage).
///
/// Note : on utilise `nest("/v2", ...)` côté `router_with_v2`, donc ici
/// le sous-router déclare juste `/ping` et `/browser/status` — le préfixe
/// `/v2` est ajouté par le `nest` à la fusion.
fn v2_router(token: String, pool: BrowserPool) -> Router {
    Router::new()
        .route("/ping", get(v2_ping))
        .route("/browser/status", get(v2_browser_status))
        .route_layer(middleware::from_fn(require_token))
        .layer(Extension(AuthToken(token)))
        .layer(Extension(pool))
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
/// **Inchangé** (compat v1 stricte, 12 tests verts à préserver).
pub fn router(ctx: VigilCtx) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events", get(events_handler))
        .merge(crate::ingest::ws_router())
        .merge(crate::stats::stats_router())
        .layer(Extension(ctx))
}

/// Router v1 + sous-router `/v2` protégé par `auth::require_token`.
///
/// Le token est passé via `Extension(AuthToken)` au sous-router /v2 ; le
/// middleware le lit dans `request.extensions()`. Le `BrowserPool` est
/// passé via `Extension(BrowserPool)` au sous-router /v2 (handlers comme
/// `/v2/browser/status` le lisent). Aucune route v1 n'est affectée
/// (on utilise `nest` + `route_layer` qui confinent le middleware au
/// sous-arbre /v2).
///
/// **Changement Task 2** : la signature prend désormais un `BrowserPool`
/// en plus du `(ctx, config)`. Le test d'auth Task 1 a été mis à jour en
/// conséquence (cf. `tests/auth.rs`).
pub fn router_with_v2(ctx: VigilCtx, config: Config, pool: BrowserPool) -> Router {
    router(ctx).nest(
        "/v2",
        v2_router(config.api_token().to_string(), pool),
    )
}
