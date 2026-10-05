//! Router HTTP principal : fusionne `/health`, `/ws`, `/stats`, `/events`.
//!
//! ## Choix d'architecture (Task 3 → Task 4)
//!
//! - `VigilCtx` (cf. `state.rs`) agrège `Counters`, `Allowlist`,
//!   `SharedSession` et `AlertBuffer`. C'est plus simple que de propager
//!   4 `State<...>` distincts (axum 0.7 n'autorise qu'un `with_state`
//!   par `Router`). On l'attache au router racine via
//!   `.layer(Extension(ctx))` — tous les sous-routers y accèdent
//!   gratuitement sans avoir à le re-passer.
//! - Les sous-routers `ws_router()`, `stats_router()` ne posent plus
//!   de `with_state` ; ils lisent l'extension.
//!
//! ## v2 (Task 1 → REWORK Task 2)
//!
//! - `router_with_v2(ctx, config, channel)` ajoute le sous-router `/v2`
//!   **monté derrière** `auth::require_token`. Les routes v1 (`/health`,
//!   `/ws`, `/stats`, `/events`) restent **inchangées et ouvertes**.
//! - Le token est passé au middleware via `Extension(AuthToken(...))`.
//! - Le `ExtensionChannel` est passé via `Extension(ExtensionChannel)` à
//!   `/v2/cmd` (POST) — le handler pousse la commande dans le canal et
//!   attend la réponse de l'extension (modèle webbridge, REWORK Eddie
//!   2026-10-05).
//! - `router(ctx)` reste intact pour la compat v1 (signature stable,
//!   12 tests verts à préserver).

use axum::{
    extract::Extension,
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::{require_token, AuthToken};
use crate::config::Config;
use crate::hands::channel::{CommandError, ExtensionChannel};
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

/// Corps de la requête `POST /v2/cmd`. Désérialisé en `Value` côté
/// `params` pour rester agnostique du contenu (le démon ne valide pas
/// la forme des params — c'est l'affaire de l'extension).
#[derive(Debug, Deserialize)]
pub struct CmdRequest {
    /// Nom de la commande : `goto`, `click`, `read`, `fill`, `tabs_create`, …
    pub cmd: String,
    /// Paramètres de la commande (forme libre, validée par l'extension).
    #[serde(default)]
    pub params: Value,
}

/// Réponse uniforme de `POST /v2/cmd`.
#[derive(Debug, Serialize)]
pub struct CmdResponse {
    pub ok: bool,
    /// Résultat de la commande (forme libre, fournie par l'extension).
    /// `None` en cas d'erreur.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// Message d'erreur (timeout, pas d'extension, extension a échoué).
    /// `None` en cas de succès.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handler `POST /v2/cmd` — modèle webbridge (REWORK).
///
/// Flux :
/// 1. Désérialise `{cmd, params}`.
/// 2. Pousse la commande dans le `ExtensionChannel` (qui attend la
///    réponse de l'extension avec timeout 30 s).
/// 3. Traduit le résultat :
///    - `Ok(Response)` → 200 + `{ok:true, result:...}`.
///    - `Err(NoExtension)` → 504 Gateway Timeout (pas d'extension).
///    - `Err(Timeout(_))` → 504 Gateway Timeout.
///    - `Err(ChannelClosed)` → 504 Gateway Timeout.
///    - `Err(ExtensionError(msg))` → 502 Bad Gateway (extension a
///      renvoyé `ok:false`).
async fn v2_cmd(
    Extension(channel): Extension<ExtensionChannel>,
    Json(req): Json<CmdRequest>,
) -> impl IntoResponse {
    match channel.send_command(req.cmd, req.params).await {
        Ok(resp) => {
            // resp.ok est toujours true ici (Channel mappe false → Err).
            let body = CmdResponse {
                ok: true,
                result: resp.result,
                error: None,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(CommandError::NoExtension) => {
            tracing::warn!("/v2/cmd refusé : aucune extension connectée");
            let body = CmdResponse {
                ok: false,
                result: None,
                error: Some("no_extension_connected".into()),
            };
            (StatusCode::GATEWAY_TIMEOUT, Json(body)).into_response()
        }
        Err(CommandError::Timeout(d)) => {
            tracing::warn!(timeout = ?d, "/v2/cmd : timeout en attendant l'extension");
            let body = CmdResponse {
                ok: false,
                result: None,
                error: Some(format!("extension_timeout_{}s", d.as_secs())),
            };
            (StatusCode::GATEWAY_TIMEOUT, Json(body)).into_response()
        }
        Err(CommandError::ChannelClosed) => {
            tracing::warn!("/v2/cmd : canal oneshot fermé (extension déconnectée)");
            let body = CmdResponse {
                ok: false,
                result: None,
                error: Some("extension_disconnected".into()),
            };
            (StatusCode::GATEWAY_TIMEOUT, Json(body)).into_response()
        }
        Err(CommandError::ExtensionError(msg)) => {
            tracing::warn!(error = %msg, "/v2/cmd : l'extension a renvoyé ok=false");
            let body = CmdResponse {
                ok: false,
                result: None,
                error: Some(msg),
            };
            (StatusCode::BAD_GATEWAY, Json(body)).into_response()
        }
    }
}

/// Construit le sous-router `/v2` avec l'auth middleware appliqué.
fn v2_router(token: String, channel: ExtensionChannel) -> Router {
    Router::new()
        .route("/ping", get(v2_ping))
        .route("/cmd", post(v2_cmd))
        .route_layer(middleware::from_fn(require_token))
        .layer(Extension(AuthToken(token)))
        .layer(Extension(channel))
}

/// Handler `GET /events` — retourne les N dernières alertes en JSON.
async fn events_handler(Extension(ctx): Extension<VigilCtx>) -> impl IntoResponse {
    const EVENTS_TAIL: usize = 50;
    let from_buffer = ctx.alerts.tail(EVENTS_TAIL).await;
    if !from_buffer.is_empty() {
        return (StatusCode::OK, Json(serde_json::json!(from_buffer))).into_response();
    }
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
///
/// **REWORK 2026-10-05** : la signature prend désormais un
/// `ExtensionChannel` (même si on n'utilise pas /v2) car `/ws` le
/// référence pour identifier l'extension bidirectionnelle. En v1 pur
/// (sans REWORK), un `ExtensionChannel::new()` vide suffit — il ne
/// sera jamais interrogé.
pub fn router(ctx: VigilCtx, channel: ExtensionChannel) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events", get(events_handler))
        .merge(crate::ingest::ws_router())
        .merge(crate::stats::stats_router())
        .layer(Extension(channel))
        .layer(Extension(ctx))
}

/// Router v1 + sous-router `/v2` protégé par `auth::require_token`.
///
/// Le sous-router /v2 expose `/ping` (trivial) et `/cmd` (POST, modèle
/// webbridge). Le `channel` est posé à la fois sur le routeur racine
/// (pour `/ws`) et passé à `v2_router` (pour `/v2/cmd`).
pub fn router_with_v2(
    ctx: VigilCtx,
    config: Config,
    channel: ExtensionChannel,
) -> Router {
    router(ctx, channel.clone()).nest(
        "/v2",
        v2_router(config.api_token().to_string(), channel),
    )
}
