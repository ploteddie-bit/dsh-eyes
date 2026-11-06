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
//! ## v2 (Task 1 → REWORK Task 2 → Task 3)
//!
//! - `router_with_v2(ctx, config, channel)` ajoute le sous-router `/v2`
//!   **monté derrière** `auth::require_token`. Les routes v1 (`/health`,
//!   `/ws`, `/stats`, `/events`) restent **inchangées et ouvertes**.
//! - Le token est passé au middleware via `Extension(AuthToken(...))`.
//! - Le `ExtensionChannel` est passé via `Extension(ExtensionChannel)` à
//!   `/v2/cmd` (POST, modèle webbridge brut) et à `/v2/open` +
//!   `/v2/read` (Task 3, endpoints de commodité qui enveloppent `/v2/cmd`).
//! - `router(ctx)` reste intact pour la compat v1 (signature stable).
//!
//! ## Task 3 — endpoints de commodité
//!
//! `POST /v2/open {"url"}` et `POST /v2/read {"tab_id"}` factorisent
//! l'envoi de commandes au canal via la fonction privée [`send_cmd`]
//! (commune à `v2_cmd`, `v2_open`, `v2_read`). Ça évite de dupliquer
//! le `match` sur `CommandError` (5 variantes) à chaque handler. Le
//! mapping d'erreur est unique, documenté en tête de `send_cmd`.

use axum::{
    extract::rejection::JsonRejection,
    extract::{Extension, Query, Request},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt as _;

use crate::auth::{require_token, AuthToken};
use crate::config::{Config, SolverConfig};
use crate::hands::channel::{CommandError, ExtensionChannel};
use crate::state::VigilCtx;
use crate::vigil;

/// Commandes ajoutées pour la parité WebBridge (2026-11-06). Le démon
/// refuse de les envoyer à une extension qui ne les annonce pas dans son
/// `hello` : l'opérateur obtient `command_not_supported` au lieu d'un
/// `commande inconnue` opaque.
const PARITY_COMMANDS: &[&str] = &[
    "snapshot",
    "tabs_list",
    "tab_attach",
    "tab_close",
    "session_close",
    "send_keys",
    "network",
    "pdf",
    "upload",
    "cdp",
];

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

/// Handler `GET /v2/info` — version du démon et état du handshake
/// extension (connectée, version, capacités). Sert au diagnostic de
/// déploiement et au handshake de version démon ↔ extension.
async fn v2_info(Extension(channel): Extension<ExtensionChannel>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "extension": {
            "connected": channel.is_connected().await,
            "hello": channel.hello().await,
        }
    }))
}

/// Handler `GET /v2/metrics` — volumes, issues et latence des commandes
/// envoyées à l'extension, globalement et par commande.
async fn v2_metrics(Extension(channel): Extension<ExtensionChannel>) -> Json<serde_json::Value> {
    Json(channel.metrics_snapshot())
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

/// Erreur de parsing du `Json` body — on l'utilise comme
/// `IntoResponse` pour renvoyer 400 (au lieu du 422 par défaut d'axum)
/// et un message uniforme `{"ok":false,"error":"invalid_body"}`.
/// On garde le détail dans `tracing::debug!` (pas en log warning :
/// c'est du bruit côté serveur si un client mal-formé appelle en boucle).
#[derive(Debug)]
struct JsonBodyError(String);

impl JsonBodyError {
    fn from_rejection(e: JsonRejection) -> Self {
        Self(e.body_text())
    }
}

impl IntoResponse for JsonBodyError {
    fn into_response(self) -> Response {
        tracing::debug!(error = %self.0, "rejet Json body");
        error_response(StatusCode::BAD_REQUEST, "invalid_body")
    }
}

/// Extractor wrapper : `Json<T>` avec gestion custom de la rejection.
/// Pattern recommandé par axum 0.7 (cf. docs.rs/axum/extract).
///
/// Usage : `body: ValidatedJson<MyRequest>` au lieu de `Json<MyRequest>`.
/// Côté handler on déballe via `ValidatedJson(Json(req)) = body`.
struct ValidatedJson<T>(Json<T>);

#[axum::async_trait]
impl<T, S> axum::extract::FromRequest<S> for ValidatedJson<T>
where
    T: serde::de::DeserializeOwned + 'static,
    S: Send + Sync,
    Json<T>: axum::extract::FromRequest<S, Rejection = JsonRejection>,
{
    type Rejection = JsonBodyError;

    async fn from_request(
        req: Request,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(j) => Ok(ValidatedJson(j)),
            Err(e) => Err(JsonBodyError::from_rejection(e)),
        }
    }
}

/// Envoie une commande via le canal et traduit uniformément les erreurs
/// en `Response` HTTP. **Point d'entrée unique** pour tous les handlers
/// qui parlent à l'extension (`/v2/cmd`, `/v2/open`, `/v2/read`).
///
/// Mapping d'erreur (cf. tests/cmd.rs pour la matrice exacte) :
/// - `Ok(Response)` → `Ok(resp.result)` (le caller fait son formatage).
/// - `Err(NoExtension)` → 504 `no_extension_connected`.
/// - `Err(Timeout(d))` → 504 `extension_timeout_Ns`.
/// - `Err(ChannelClosed)` → 504 `extension_disconnected`.
/// - `Err(ExtensionError(msg))` → 502 + `msg` propagé tel quel.
///
/// Le `Result<Value, Response>` permet au caller d'utiliser `?` ou un
/// simple `match` pour composer sa réponse. Pas d'`unwrap`/panic : tous
/// les chemins d'erreur renvoient une `Response` HTTP valide.
///
/// `pub(crate)` : exposé à [`crate::solver`] pour partager la couche
/// de transport vers l'extension entre les handlers API et le solveur.
#[allow(clippy::result_large_err)] // Err = Response HTTP prête à renvoyer ; chemin froid.
pub(crate) async fn send_cmd(
    channel: &ExtensionChannel,
    cmd: impl Into<String>,
    params: Value,
) -> Result<Value, Response> {
    let cmd = cmd.into();
    if !channel.check_rate_limit() {
        tracing::warn!("send_cmd : rate limit atteint (120/min, rafale 60)");
        return Err(error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited"));
    }
    // Refus explicite d'une commande que l'extension n'a pas annoncée au
    // `hello` : sans cette barrière, l'extension répondrait
    // « commande inconnue: X » (502 opaque). On ne contrôle que les
    // commandes de parité — les commandes historiques restent tolérantes
    // pour une extension plus ancienne que le démon.
    if PARITY_COMMANDS.contains(&cmd.as_str()) {
        if let Some(hello) = channel.hello().await {
            if !hello.capabilities.is_empty() && !hello.capabilities.iter().any(|c| c == &cmd) {
                tracing::warn!(cmd = %cmd, "send_cmd : commande non annoncée par l'extension");
                return Err(error_response(
                    StatusCode::BAD_GATEWAY,
                    format!("command_not_supported: {cmd}"),
                ));
            }
        }
    }
    match channel.send_command(cmd, params).await {
        Ok(resp) => {
            // resp.ok est toujours true ici (Channel mappe false → Err).
            // resp.result est `Option<Value>` selon l'extension — on
            // normalise en `Value::Null` si None (jamais observé mais
            // on reste safe).
            Ok(resp.result.unwrap_or(Value::Null))
        }
        Err(CommandError::NoExtension) => {
            tracing::warn!("send_cmd : aucune extension connectée");
            Err(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "no_extension_connected",
            ))
        }
        Err(CommandError::Timeout(d)) => {
            tracing::warn!(timeout = ?d, "send_cmd : timeout en attendant l'extension");
            Err(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                format!("extension_timeout_{}s", d.as_secs()),
            ))
        }
        Err(CommandError::ChannelClosed) => {
            tracing::warn!("send_cmd : canal oneshot fermé (extension déconnectée)");
            Err(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "extension_disconnected",
            ))
        }
        Err(CommandError::ExtensionError(msg)) => {
            // Sélecteur introuvable → 404 (ressource ciblée inexistante),
            // wait_timeout → 504 (cohérent avec les autres timeouts du
            // canal), les autres erreurs extension restent en 502.
            if msg.starts_with("sélecteur introuvable") {
                return Err(error_response(StatusCode::NOT_FOUND, msg));
            }
            if msg.starts_with("wait_timeout:") {
                return Err(error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "extension_wait_timeout",
                ));
            }
            tracing::warn!(error = %msg, "send_cmd : l'extension a renvoyé ok=false");
            Err(error_response(StatusCode::BAD_GATEWAY, msg))
        }
    }
}

/// Helper : construit une `Response` JSON `{ok:false, error}` avec le
/// statut donné. Utilisé par [`send_cmd`] et par les handlers qui
/// retournent des erreurs 4xx (400 input invalide, 403 forbidden).
///
/// `pub(crate)` : exposé à [`crate::solver`] pour partager le format
/// d'erreur entre les handlers API et le solveur.
pub(crate) fn error_response(status: StatusCode, error: impl Into<String>) -> Response {
    let body = CmdResponse {
        ok: false,
        result: None,
        error: Some(error.into()),
    };
    (status, Json(body)).into_response()
}

/// Chemin du journal des actions — capturé UNE SEULE FOIS (OnceLock
/// process-wide). Exposé pub pour que les tests lisent EXACTEMENT le
/// même dossier que le démon, sans dépendre de l'env à l'instant T.
pub fn actions_jsonl_path() -> PathBuf {
    static HOME: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    let home = HOME.get_or_init(|| {
        std::env::var_os("DSH_EYES_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::env::var_os("HOME")
                    .map(|h| std::path::PathBuf::from(h).join(".dsh-eyes"))
                    .unwrap_or_else(|| std::path::PathBuf::from(".dsh-eyes"))
            })
    });
    home.join("actions.jsonl")
}

/// Masque les champs sensibles d'un détail d'action avant journalisation.
/// Les clés sont volontairement larges : `value` (fill), `code` (eval),
/// jetons et images base64. Le but est de garder une trace exploitable
/// sans conserver de secret ni de gros payload.
fn redact_params(mut value: Value) -> Value {
    const SENSITIVE: [&str; 8] = [
        "value",
        "password",
        "token",
        "code",
        "text",
        "image_b64",
        "data_url",
        "b64",
    ];
    if let Value::Object(map) = &mut value {
        for key in SENSITIVE {
            if map.contains_key(key) {
                map.insert(key.to_string(), json!("[redacted]"));
            }
        }
    }
    value
}

/// Journalise une action v2 dans `actions.jsonl` (Task 5 — traçabilité
/// « toute action est loggée »). Fire-and-forget : l'échec d'écriture est
/// loggé mais ne fait jamais échouer l'action métier.
///
/// Format : `{"ts":<ms>,"action":"open|goto|click|fill|wait|cmd|solve","detail":{…}}`
pub(crate) fn log_action(action: &str, detail: Value) {
    // Masque les valeurs sensibles AVANT toute sérialisation : le
    // journal d'actions ne doit jamais contenir un mot de passe, un
    // code eval ou une image base64.
    let detail = redact_params(detail);
    // Chemin capturé UNE SEULE FOIS au premier log (OnceLock process-wide) :
    // les tests qui modifient DSH_EYES_HOME en parallèle ne font plus
    // varier le dossier entre le moment de l'appel et l'écriture async.
    let path = actions_jsonl_path();
    let home = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let line = serde_json::json!({
        "ts": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        "action": action,
        "detail": detail,
    });
    tokio::spawn(async move {
        let _ = tokio::fs::create_dir_all(&home).await;
        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(mut f) => {
                let mut buf = match serde_json::to_vec(&line) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                buf.push(b'\n');
                if let Err(e) = f.write_all(&buf).await {
                    tracing::warn!(error = %e, "log_action : écriture actions.jsonl échouée");
                }
            }
            Err(e) => tracing::warn!(error = %e, "log_action : ouverture actions.jsonl échouée"),
        }
    });
}

/// Handler `POST /v2/cmd` — modèle webbridge (REWORK).
///
/// Flux :
/// 1. Désérialise `{cmd, params}`.
/// 2. Délègue à [`send_cmd`] (mapping d'erreur uniforme).
/// 3. Wrap le `Ok` dans `{ok:true, result:...}`.
async fn v2_cmd(
    Extension(channel): Extension<ExtensionChannel>,
    Json(req): Json<CmdRequest>,
) -> Response {
    // Garde-fou SSRF centralisé : toute commande qui porte une URL
    // passe par le même validateur que /v2/open et /v2/goto. Sans cela,
    // `{"cmd":"tabs_create","params":{"url":"http://127.0.0.1/…"}}`
    // contournait complètement `is_forbidden_host`.
    if matches!(req.cmd.as_str(), "goto" | "tabs_create" | "open") {
        if let Some(url) = req.params.get("url").and_then(|v| v.as_str()) {
            if let Err(resp) = validate_target_url(url).await {
                return resp;
            }
        }
    }
    log_action(&req.cmd, req.params.clone());
    match send_cmd(&channel, req.cmd, req.params).await {
        Ok(result) => {
            let body = CmdResponse {
                ok: true,
                result: Some(result),
                error: None,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(resp) => resp,
    }
}

// ─── Act batch : idempotence + annulation coopérative ────────────────────

/// Une étape d'un batch `act`.
#[derive(Debug, Deserialize)]
pub struct ActStep {
    pub cmd: String,
    #[serde(default)]
    pub params: Value,
}

/// Corps de `POST /v2/act`.
#[derive(Debug, Deserialize)]
pub struct ActRequest {
    pub steps: Vec<ActStep>,
    #[serde(default)]
    pub action_id: Option<String>,
}

/// Corps de `POST /v2/act/cancel`.
#[derive(Debug, Deserialize)]
pub struct ActCancelRequest {
    pub action_id: String,
}

/// Handler `POST /v2/act` — exécute une séquence d'étapes, dans l'ordre,
/// avec un `action_id` optionnel. Le résultat est mémorisé 5 min : un
/// second appel avec le même `action_id` renvoie le cache sans rejouer
/// les étapes. L'annulation est coopérative entre deux étapes via
/// `POST /v2/act/cancel`.
async fn v2_act(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<ActRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    if req.steps.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "empty_steps");
    }
    if req.steps.len() > 50 {
        return error_response(StatusCode::BAD_REQUEST, "too_many_steps");
    }
    let action_id = req.action_id.clone();

    if let Some(id) = action_id.as_deref() {
        if let Some(cached) = channel.cached_action(id) {
            return (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "action_id": id,
                    "cached": true,
                    "results": cached,
                })),
            )
                .into_response();
        }
        // Un nouveau batch avec le même id repart d'un état propre.
        channel.clear_action_cancel(id);
    }

    let mut results: Vec<Value> = Vec::new();
    let mut last_tab_id: Option<i64> = None;
    for (idx, step) in req.steps.iter().enumerate() {
        if let Some(id) = action_id.as_deref() {
            if channel.is_action_cancelled(id) {
                channel.clear_action_cancel(id);
                return (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "ok": false,
                        "action_id": id,
                        "error": "cancelled",
                        "step": idx,
                        "results": results,
                    })),
                )
                    .into_response();
            }
        }
        if matches!(step.cmd.as_str(), "goto" | "tabs_create" | "open") {
            if let Some(url) = step.params.get("url").and_then(|v| v.as_str()) {
                if let Err(resp) = validate_target_url(url).await {
                    return resp;
                }
            }
        }
        // Propagation du tab_id : après un tabs_create/goto, les étapes
        // suivantes sans tab_id explicite ciblent l'onglet créé. C'est
        // le comportement attendu d'un batch séquentiel.
        let mut params = if step.params.is_object() {
            step.params.clone()
        } else {
            json!({})
        };
        if !matches!(step.cmd.as_str(), "tabs_create" | "goto" | "open")
            && params.get("tab_id").is_none()
        {
            if let Some(id) = last_tab_id {
                params["tab_id"] = json!(id);
            }
        }
        log_action(
            "act",
            json!({
                "action_id": action_id.as_deref(),
                "step": idx,
                "cmd": step.cmd.as_str(),
            }),
        );
        match send_cmd(&channel, step.cmd.clone(), params).await {
            Ok(result) => {
                if let Some(id) = result.get("tab_id").and_then(|v| v.as_i64()) {
                    last_tab_id = Some(id);
                }
                results.push(json!({
                    "cmd": step.cmd.as_str(),
                    "ok": true,
                    "result": result,
                }));
            }
            Err(resp) => return resp,
        }
    }

    let results_value = Value::Array(results);
    if let Some(id) = action_id.as_deref() {
        channel.cache_action(id, results_value.clone());
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "action_id": action_id,
            "cached": false,
            "results": results_value,
        })),
    )
        .into_response()
}

/// Handler `POST /v2/act/cancel` — pose le drapeau d'annulation d'un
/// batch. Le handler `act` le vérifie avant chaque étape.
async fn v2_act_cancel(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<ActCancelRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let id = req.action_id.trim();
    if id.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "missing_action_id");
    }
    channel.cancel_action(id);
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "action_id": id,
            "cancelled": true,
        })),
    )
        .into_response()
}

// ─── Task 3 : endpoints de commodité ──────────────────────────────────────

/// Corps de `POST /v2/open` : une URL à ouvrir dans un nouvel onglet.
#[derive(Debug, Deserialize)]
pub struct OpenRequest {
    /// URL absolue (schéma http ou https). Les hôtes interdits
    /// (localhost, 127.0.0.0/8, RFC1918) sont rejetés en 403.
    pub url: Option<String>,
}

/// Réponse de `POST /v2/open` : `{ok:true, result:{tab_id:N}}` en cas
/// de succès, `{ok:false, error:"..."}` sinon.
#[derive(Debug, Serialize)]
pub struct OpenResponse {
    pub ok: bool,
    /// Résultat de la commande `tabs_create` : `{tab_id, url?}` selon
    /// ce que renvoie l'extension. `None` en cas d'erreur.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handler `POST /v2/open {"url"}` — enveloppement de `tabs_create`.
///
/// 1. Valide que l'URL est présente et que l'hôte n'est pas interdit
///    (réutilise `vigil::is_forbidden_host`).
/// 2. Délègue à [`send_cmd`] avec la commande `tabs_create`.
/// 3. Retourne `{ok:true, result:{tab_id:N}}` ou une erreur déjà
///    formatée par `send_cmd`.
async fn v2_open(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<OpenRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    // 1) Validation locale : URL présente + hôte non interdit.
    let url = match req.url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(u) => u,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_url"),
    };
    if let Err(resp) = validate_target_url(url).await {
        return resp;
    }

    // 2) Envoi via le canal. Paramètres = {"url": "<url>"}. On inclut
    // `active: true` pour que l'onglet soit visible côté extension
    // (chrome.tabs.create defaults à active=true de toute façon, mais
    // c'est plus lisible).
    let params = json!({ "url": url, "active": true });
    log_action("open", params.clone());
    match send_cmd(&channel, "tabs_create", params).await {
        Ok(result) => {
            let body = OpenResponse {
                ok: true,
                result: Some(result),
                error: None,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/read` : un `tab_id` (entier) à lire, et depuis le
/// 2026-11-06 un `frame` optionnel (identifiant `#fN` renvoyé par
/// `snapshot`, ou sélecteur CSS d'iframe) et un `start` de continuation.
#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    /// Identifiant entier de l'onglet à lire (renvoyé par `/v2/open`
    /// ou par `POST /v2/cmd` avec cmd=`tabs_create`).
    pub tab_id: Option<i64>,
    /// Iframe cible (`#fN` fourni par `snapshot.frames`, ou sélecteur CSS).
    #[serde(default)]
    pub frame: Option<String>,
    /// Offset de continuation : reprendre la lecture de `read_page` là où
    /// le précédent appel s'est arrêté (`next_start`).
    #[serde(default)]
    pub start: Option<usize>,
}

/// Réponse de `POST /v2/read` : `{ok:true, text:"…"}` en cas de succès.
/// `complete`/`next_start`/`frame` n'apparaissent que lorsque l'extension
/// les fournit (lecture d'iframe ou continuation).
#[derive(Debug, Serialize)]
pub struct ReadResponse {
    pub ok: bool,
    /// Texte tronqué (200 Ko côté extension par défaut). `None` si erreur.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_start: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handler `POST /v2/read {"tab_id":N}` — enveloppement de `read`.
///
/// 1. Valide la présence de `tab_id`.
/// 2. Délègue à [`send_cmd`] avec la commande `read {tab_id}`.
/// 3. Extrait `result.text` du retour et l'enveloppe dans `{ok:true, text}`.
///
/// Note : la troncature à 200 Ko est faite **côté extension** (cf.
/// `cmdRead` dans `background.js`). Le démon ne tronque pas une 2ᵉ fois.
async fn v2_read(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<ReadRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    // 1) tab_id est requis. On accepte les entiers 0 et négatifs
    // (chrome.tabs autorise des IDs signés en théorie), mais on
    // vérifie la présence (`Option<i64>`). On ne valide pas le type
    // non-entier : c'est l'affaire de `JsonRejection` → `invalid_body`.
    let tab_id = match req.tab_id {
        Some(n) => n,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_tab_id"),
    };
    let mut params = json!({ "tab_id": tab_id });
    if let Some(frame) = req.frame.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        params["frame"] = json!(frame);
    }
    if let Some(start) = req.start {
        params["start"] = json!(start);
    }
    log_action("read", params.clone());
    match send_cmd(&channel, "read", params).await {
        Ok(result) => {
            // L'extension renvoie `{text:"…"}` (forme historique) ou, avec
            // un `frame`/`start`, `{text, complete, next_start, frame}`.
            // On remonte ces champs optionnels sans changer la forme de
            // base attendue par les clients existants.
            let text = result
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match text {
                Some(t) => {
                    let body = ReadResponse {
                        ok: true,
                        text: Some(t),
                        complete: result.get("complete").and_then(|v| v.as_bool()),
                        next_start: result
                            .get("next_start")
                            .and_then(|v| v.as_u64())
                            .map(|n| n as usize),
                        frame: result
                            .get("frame")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        error: None,
                    };
                    (StatusCode::OK, Json(body)).into_response()
                }
                None => error_response(
                    StatusCode::BAD_GATEWAY,
                    "extension_returned_unexpected_shape",
                ),
            }
        }
        Err(resp) => resp,
    }
}

// ── Task 4 : actions goto / click / fill / wait ─────────────────────────
// Endpoints de commodité qui enveloppent `send_cmd` (modèle webbridge),
// mêmes motifs que `v2_open`/`v2_read`.

/// Corps de `POST /v2/goto {"url", "tab_id"?}`.
#[derive(Debug, Deserialize)]
pub struct GotoRequest {
    pub url: Option<String>,
    /// Onglet à naviguer. Omis : comportement historique (onglet de travail
    /// de la session / création). Fourni depuis le 2026-11-06 pour que la
    /// navigation ciblée fonctionne (parité WebBridge, `navigate` sur un
    /// onglet existant).
    #[serde(default)]
    pub tab_id: Option<i64>,
}

/// Handler `POST /v2/goto` — navigue vers `url`, dans `tab_id` quand il est
/// fourni, sinon dans l'onglet de travail de la session (comportement
/// historique, identique à `/v2/open`). Garde-fou d'hôte identique.
async fn v2_goto(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<GotoRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let url = match req.url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(u) => u,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_url"),
    };
    if let Err(resp) = validate_target_url(url).await {
        return resp;
    }
    let mut params = json!({ "url": url, "active": true });
    if let Some(tab_id) = req.tab_id {
        params["tab_id"] = json!(tab_id);
    }
    log_action("goto", params.clone());
    match send_cmd(&channel, "goto", params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result" : result }))).into_response(),
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/click {"tab_id"?,"selector"}`.
#[derive(Debug, Deserialize)]
pub struct ClickRequest {
    pub tab_id: Option<i64>,
    pub selector: Option<String>,
}

async fn v2_click(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<ClickRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let selector = match req.selector.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_selector"),
    };
    let params = json!({ "tab_id": req.tab_id, "selector": selector });
    log_action("click", params.clone());
    match send_cmd(&channel, "click", params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result": result }))).into_response(),
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/fill {"tab_id"?,"selector","value"}`.
#[derive(Debug, Deserialize)]
pub struct FillRequest {
    pub tab_id: Option<i64>,
    pub selector: Option<String>,
    pub value: Option<String>,
}

async fn v2_fill(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<FillRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let selector = match req.selector.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_selector"),
    };
    let value = req.value.unwrap_or_default();
    let params = json!({ "tab_id": req.tab_id, "selector": selector, "value": value });
    log_action("fill", params.clone());
    match send_cmd(&channel, "fill", params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result": result }))).into_response(),
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/wait {"tab_id"?,"selector","timeout_ms"?}` (plafond 10 s).
#[derive(Debug, Deserialize)]
pub struct WaitRequest {
    pub tab_id: Option<i64>,
    pub selector: Option<String>,
    pub timeout_ms: Option<u64>,
}

async fn v2_wait(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<WaitRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let selector = match req.selector.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_selector"),
    };
    // Plafond 10 s : jamais au-delà, même si le client demande plus.
    let timeout_ms = req.timeout_ms.unwrap_or(5000).min(10_000);
    let params = json!({ "tab_id": req.tab_id, "selector": selector, "timeout_ms": timeout_ms });
    log_action("wait", params.clone());
    match send_cmd(&channel, "wait", params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result": result }))).into_response(),
        Err(resp) => resp,
    }
}
// ── Task 6 : screenshot + détection captcha ────────────────────────────
//
// Deux endpoints de commodité qui enveloppent `send_cmd` (modèle webbridge),
// même motif que `v2_goto`/`v2_click`. La commande `screenshot` renvoie un
// dataURL `data:image/png;base64,…` ; on extrait la partie base64 pour
// renvoyer `{ok:true, b64:"…"}`. La commande `captcha_detect` renvoie
// `{detected, type, selector, image_b64?}` qu'on remonte tel quel.
//
// v1 simple : le `image_b64` du captcha = capture plein écran (pas de crop).
// Le crop viendra plus tard (off-screen canvas côté extension).

/// Corps de `POST /v2/page/screenshot {"tab_id"?,"format"?}`.
/// `format` est `"png"` (défaut) ou `"jpeg"`. `tab_id` est optionnel :
/// l'extension prend l'onglet actif si absent.
#[derive(Debug, Deserialize)]
pub struct ScreenshotRequest {
    pub tab_id: Option<i64>,
    #[serde(default)]
    pub format: Option<String>,
    /// Sélecteur CSS de l'élément à capturer (2026-11-06, parité WebBridge).
    #[serde(default)]
    pub selector: Option<String>,
    /// Capture de la page entière (assemblage `captureBeyondViewport`,
    /// plafonné à 20 000 px côté extension).
    #[serde(default)]
    pub full_page: Option<bool>,
    /// Chemin où le démon écrit la capture (l'extension n'a pas accès au
    /// disque). Quand il est fourni, `b64` n'est pas renvoyé.
    #[serde(default)]
    pub path: Option<String>,
}

/// Réponse de `POST /v2/page/screenshot` : `{ok:true, format, b64}` ou
/// `{ok:true, format, path, size_bytes}` quand un `path` était demandé.
#[derive(Debug, Serialize)]
pub struct ScreenshotResponse {
    pub ok: bool,
    /// Format effectivement utilisé (`png` ou `jpeg`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Chaîne base64 (sans le préfixe `data:image/...;base64,`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b64: Option<String>,
    /// Chemin du fichier écrit par le démon (si `path` demandé).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Taille du fichier écrit, en octets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handler `POST /v2/page/screenshot` — délègue à `send_cmd` avec la
/// commande `screenshot` puis extrait `data_url` en `b64` (on strippe
/// le préfixe MIME côté serveur pour faciliter la consommation
/// client).
async fn v2_page_screenshot(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<ScreenshotRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    // Validation légère : si `format` est fourni, on vérifie qu'il vaut
    // "png" ou "jpeg". Les autres formes sont rejetées en 400 pour
    // éviter qu'une faute de frappe silencieuse ne renvoie un format
    // inattendu.
    let format = match req.format.as_deref().map(str::trim) {
        None | Some("") | Some("png") | Some("jpeg") => req.format.clone(),
        Some(other) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid_format: '{other}' (attendu 'png' ou 'jpeg')"),
            );
        }
    };
    let mut params = json!({ "tab_id": req.tab_id, "format": format });
    if let Some(selector) = req.selector.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        params["selector"] = json!(selector);
    }
    if let Some(full_page) = req.full_page {
        params["full_page"] = json!(full_page);
    }
    log_action("page_screenshot", params.clone());
    match send_cmd(&channel, "screenshot", params).await {
        Ok(result) => {
            // L'extension renvoie `{format, data_url}`. On extrait la
            // partie base64 du dataURL. Si la forme est inattendue (pas
            // un string, pas de virgule), on tombe sur une 502 dédiée
            // — jamais d'unwrap.
            let data_url = result
                .get("data_url")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let fmt = result
                .get("format")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match data_url {
                Some(d) => {
                    let b64 = match d.split_once(',') {
                        Some((_prefix, payload)) => payload.to_string(),
                        None => d, // pas de virgule : on renvoie brut
                    };
                    // `path` : le démon écrit le fichier (l'extension ne
                    // peut pas accéder au disque) puis ne renvoie pas le
                    // base64 pour ne pas gonfler la réponse.
                    if let Some(path) = req.path.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                        let bytes = match b64_decode(&b64) {
                            Some(bytes) => bytes,
                            None => {
                                return error_response(
                                    StatusCode::BAD_GATEWAY,
                                    "invalid_base64_from_extension",
                                );
                            }
                        };
                        if let Err(e) = tokio::fs::write(path, &bytes).await {
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("write_failed: {e}"),
                            );
                        }
                        let body = ScreenshotResponse {
                            ok: true,
                            format: fmt.or_else(|| Some("png".to_string())),
                            b64: None,
                            path: Some(path.to_string()),
                            size_bytes: Some(bytes.len()),
                            error: None,
                        };
                        return (StatusCode::OK, Json(body)).into_response();
                    }
                    let body = ScreenshotResponse {
                        ok: true,
                        format: fmt.or_else(|| Some("png".to_string())),
                        b64: Some(b64),
                        path: None,
                        size_bytes: None,
                        error: None,
                    };
                    (StatusCode::OK, Json(body)).into_response()
                }
                None => error_response(
                    StatusCode::BAD_GATEWAY,
                    "extension_returned_unexpected_shape",
                ),
            }
        }
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/page/captcha {"tab_id"?}` — pas de validation
/// spécifique, `tab_id` est résolu côté extension.
#[derive(Debug, Deserialize)]
pub struct CaptchaRequest {
    pub tab_id: Option<i64>,
}

/// Réponse de `POST /v2/page/captcha` : `{ok:true, detected, type,
/// selector?, image_b64?}`. On sérialise en JSON libre (forme dict
/// passée telle quelle) — le format exact est imposé par l'extension
/// (cf. `cmdCaptchaDetect`).
#[derive(Debug, Serialize)]
pub struct CaptchaResponse {
    pub ok: bool,
    /// Résultat de la détection (forme dict). `None` en cas d'erreur.
    /// On laisse `serde_json` aplatir le dict dans la réponse pour
    /// rester agnostique du schéma exact.
    #[serde(flatten)]
    pub result: serde_json::Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handler `POST /v2/page/captcha` — délègue à `send_cmd` avec la
/// commande `captcha_detect`. La réponse de l'extension
/// (`{detected, type, selector?, image_b64?}`) est fusionnée dans le
/// `CaptchaResponse` via `flatten`.
///
/// **Task 7** : factorisé autour de [`crate::solver::detect_captcha`]
/// pour partager la logique avec `POST /v2/page/{id}/solve`. La
/// sérialisation reste en `flatten` pour préserver la compatibilité
/// de la réponse publique.
async fn v2_page_captcha(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<CaptchaRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let params = json!({ "tab_id": req.tab_id });
    log_action("page_captcha", params.clone());
    match crate::solver::detect_captcha(&channel, req.tab_id).await {
        crate::solver::DetectOutcome::Info(info) => {
            // On convertit la `CaptchaInfo` en `serde_json::Map` pour
            // préserver la forme publique `{detected, type, selector,
            // image_b64}`. Les champs `None` ne sont pas inclus grâce
            // au `skip_serializing_if`.
            let value = serde_json::to_value(&info).unwrap_or(Value::Object(Default::default()));
            let map = match value {
                Value::Object(m) => m,
                _ => {
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "extension_returned_unexpected_shape",
                    );
                }
            };
            let body = CaptchaResponse {
                ok: true,
                result: map,
                error: None,
            };
            (StatusCode::OK, Json(body)).into_response()
        }
        crate::solver::DetectOutcome::ErrorResponse(resp) => resp,
    }
}

// ── Task « persistance » : groupe d'onglets nommé ──────────────────────

/// Corps de `POST /v2/page/group {"tab_id"?,"title","color"?}`.
#[derive(Debug, Deserialize)]
pub struct TabGroupRequest {
    pub tab_id: Option<i64>,
    pub title: Option<String>,
    pub color: Option<String>,
}

/// Handler `POST /v2/page/group` — place un onglet dans un groupe nommé
/// (créé à la volée). Délègue à la commande extension `tab_group`.
async fn v2_page_group(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<TabGroupRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let title = match req.title.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(t) => t,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_title"),
    };
    let mut params = json!({ "tab_id": req.tab_id, "title": title });
    if let Some(c) = req.color.as_deref() {
        params["color"] = json!(c);
    }
    log_action("tab_group", params.clone());
    match send_cmd(&channel, "tab_group", params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result": result }))).into_response(),
        Err(resp) => resp,
    }
}

/// Extrait le host d'une URL `http://host[:port]/…` ou
/// `https://host[:port]/…`. Retourne `None` si l'URL est mal formée.
///
/// Refuse les userinfo (`http://user@host/`), les schémas autres que
/// http/https et les autorités vides. Les crochets IPv6 sont décodés.
fn extract_host(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    let authority = rest.split(['/', '?', '#']).next()?;
    // `@` = userinfo (http://user@host/). `\` = séparateur selon le
    // parser WHATWG de Chrome : `http://127.0.0.1\evil.com/` devient
    // `http://127.0.0.1/evil.com/`. On refuse tout backslash dans
    // l'autorité pour ne pas laisser passer une cible locale déguisée.
    if authority.is_empty() || authority.contains('@') || authority.contains('\\') {
        return None;
    }
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        let end = stripped.find(']')?;
        &stripped[..end]
    } else {
        authority.split(':').next()?
    };
    if host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
}

/// Valide une URL de navigation avant de l'envoyer à l'extension.
///
/// Deux niveaux :
/// 1. `is_forbidden_host` sur le host littéral (localhost, IP privées,
///    formes numériques déguisées).
/// 2. Résolution DNS (timeout 3 s) : toute adresse résolue loopback /
///    privée / link-local / ULA est refusée. Couvre `127.0.0.1.nip.io`.
///
/// Un échec de résolution (DNS indisponible) n'est pas bloquant : la
/// navigation échouera d'elle-même, et bloquer casserait les domaines
/// légitimes dont le DNS est lent. La limite est documentée.
#[allow(clippy::result_large_err)] // Err = Response HTTP 400/403 ; chemin froid.
async fn validate_target_url(url: &str) -> Result<(), Response> {
    let host = extract_host(url)
        .ok_or_else(|| error_response(StatusCode::BAD_REQUEST, "invalid_url"))?;
    if vigil::is_forbidden_host(&host) {
        tracing::warn!(host = %host, "cible interdite (hôte)");
        return Err(error_response(StatusCode::FORBIDDEN, "forbidden_target"));
    }
    let lookup = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::lookup_host((host.as_str(), 0)),
    )
    .await;
    if let Ok(Ok(addrs)) = lookup {
        for addr in addrs {
            if vigil::is_forbidden_ip(&addr.ip()) {
                tracing::warn!(host = %host, ip = %addr.ip(), "cible interdite (DNS)");
                return Err(error_response(StatusCode::FORBIDDEN, "forbidden_target"));
            }
        }
    }
    Ok(())
}

// ── Parité WebBridge (2026-11-06) ───────────────────────────────────────
// Routes ajoutées pour combler les écarts fonctionnels identifiés par
// l'audit : snapshot d'accessibilité, cycle de vie des onglets, clavier
// natif, capture réseau, export PDF, upload, passe-plat CDP.
// Contrat : `docs/protocol-parity-2026-11-06.md`.

/// Décode du base64 standard (avec ou sans padding). Évite d'ajouter une
/// dépendance pour deux usages (capture et PDF).
fn b64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;
    for raw in input.bytes() {
        if raw == b'=' || raw.is_ascii_whitespace() {
            continue;
        }
        let v = val(raw)?;
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Envoie une commande et renvoie `{ok:true, result}` tel que l'extension
/// l'a produit (routes « passe-plat » de la parité WebBridge).
async fn forward(
    channel: &ExtensionChannel,
    cmd: &str,
    params: Value,
    action: &str,
) -> Response {
    log_action(action, params.clone());
    match send_cmd(channel, cmd, params).await {
        Ok(result) => (StatusCode::OK, Json(json!({ "ok": true, "result": result }))).into_response(),
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/snapshot`.
#[derive(Debug, Deserialize)]
pub struct SnapshotRequest {
    pub tab_id: Option<i64>,
    #[serde(default)]
    pub max_chars: Option<usize>,
    /// Référence `@eN` d'un nœud : renvoie son sous-arbre.
    #[serde(default, rename = "ref")]
    pub node: Option<String>,
}

/// Handler `POST /v2/snapshot` — arbre d'accessibilité + refs `@eN`.
async fn v2_snapshot(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<SnapshotRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let mut params = json!({ "tab_id": req.tab_id });
    if let Some(max_chars) = req.max_chars {
        params["max_chars"] = json!(max_chars);
    }
    if let Some(node) = req.node.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        params["ref"] = json!(node);
    }
    forward(&channel, "snapshot", params, "snapshot").await
}

/// Corps de `POST /v2/tabs`.
#[derive(Debug, Deserialize)]
pub struct TabsRequest {
    /// `list` | `attach` | `close` | `close_session`
    pub op: String,
    #[serde(default)]
    pub tab_id: Option<i64>,
    /// Session (groupe d'onglets) pour `close_session`.
    #[serde(default)]
    pub session: Option<String>,
    /// `list` sur toutes les fenêtres.
    #[serde(default)]
    pub all_windows: Option<bool>,
}

/// Handler `POST /v2/tabs` — cycle de vie des onglets.
async fn v2_tabs(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<TabsRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let op = req.op.trim().to_ascii_lowercase();
    let (cmd, params) = match op.as_str() {
        "list" => (
            "tabs_list",
            json!({ "all_windows": req.all_windows.unwrap_or(false) }),
        ),
        "attach" => match req.tab_id {
            Some(tab_id) => ("tab_attach", json!({ "tab_id": tab_id })),
            None => return error_response(StatusCode::BAD_REQUEST, "missing_tab_id"),
        },
        "close" => match req.tab_id {
            Some(tab_id) => ("tab_close", json!({ "tab_id": tab_id })),
            None => return error_response(StatusCode::BAD_REQUEST, "missing_tab_id"),
        },
        "close_session" => match req.session.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(session) => ("session_close", json!({ "session": session })),
            None => return error_response(StatusCode::BAD_REQUEST, "missing_session"),
        },
        other => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid_op: '{other}' (attendu list, attach, close ou close_session)"),
            );
        }
    };
    forward(&channel, cmd, params, "tabs").await
}

/// Corps de `POST /v2/send_keys`.
#[derive(Debug, Deserialize)]
pub struct SendKeysRequest {
    pub tab_id: Option<i64>,
    pub keys: Option<String>,
    #[serde(default)]
    pub focus_emulation: Option<bool>,
}

/// Handler `POST /v2/send_keys` — clavier natif (raccourcis inclus).
async fn v2_send_keys(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<SendKeysRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let keys = match req.keys.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(keys) => keys,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_keys"),
    };
    let mut params = json!({ "tab_id": req.tab_id, "keys": keys });
    if let Some(focus) = req.focus_emulation {
        params["focus_emulation"] = json!(focus);
    }
    forward(&channel, "send_keys", params, "send_keys").await
}

/// Corps de `POST /v2/network`.
#[derive(Debug, Deserialize)]
pub struct NetworkRequest {
    /// `start` | `stop` | `list` | `detail`
    pub op: String,
    pub tab_id: Option<i64>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
}

/// Handler `POST /v2/network` — capture réseau par onglet.
async fn v2_network(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<NetworkRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let op = req.op.trim().to_ascii_lowercase();
    if !matches!(op.as_str(), "start" | "stop" | "list" | "detail") {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!("invalid_op: '{}' (attendu start, stop, list ou detail)", req.op),
        );
    }
    if op == "detail" && req.request_id.as_deref().map(str::trim).unwrap_or("").is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "missing_request_id");
    }
    let mut params = json!({ "op": op, "tab_id": req.tab_id });
    if let Some(filter) = req.filter.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        params["filter"] = json!(filter);
    }
    if let Some(request_id) = req.request_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        params["request_id"] = json!(request_id);
    }
    forward(&channel, "network", params, "network").await
}

/// Corps de `POST /v2/pdf`.
#[derive(Debug, Deserialize)]
pub struct PdfRequest {
    pub tab_id: Option<i64>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub paper_format: Option<String>,
    #[serde(default)]
    pub landscape: Option<bool>,
    #[serde(default)]
    pub scale: Option<f64>,
    #[serde(default)]
    pub print_background: Option<bool>,
}

/// Handler `POST /v2/pdf` — export PDF de la page courante.
async fn v2_pdf(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<PdfRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    if let Some(scale) = req.scale {
        if !(0.1..=2.0).contains(&scale) {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid_scale: {scale} (attendu 0.1 à 2.0)"),
            );
        }
    }
    let mut params = json!({ "tab_id": req.tab_id });
    for (key, value) in [
        ("paper_format", req.paper_format.as_ref().map(|v| json!(v))),
        ("landscape", req.landscape.map(|v| json!(v))),
        ("scale", req.scale.map(|v| json!(v))),
        ("print_background", req.print_background.map(|v| json!(v))),
    ] {
        if let Some(value) = value {
            params[key] = value;
        }
    }
    match send_cmd(&channel, "pdf", params).await {
        Ok(result) => {
            let title = result.get("page_title").and_then(|v| v.as_str()).map(str::to_string);
            let b64 = match result.get("b64").and_then(|v| v.as_str()) {
                Some(b64) => b64.to_string(),
                None => {
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "extension_returned_unexpected_shape",
                    );
                }
            };
            if let Some(path) = req.path.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                let bytes = match b64_decode(&b64) {
                    Some(bytes) => bytes,
                    None => {
                        return error_response(
                            StatusCode::BAD_GATEWAY,
                            "invalid_base64_from_extension",
                        );
                    }
                };
                if let Err(e) = tokio::fs::write(path, &bytes).await {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("write_failed: {e}"),
                    );
                }
                let size = bytes.len();
                return (StatusCode::OK, Json(json!({
                    "ok": true,
                    "path": path,
                    "size_bytes": size,
                    "page_title": title,
                })))
                .into_response();
            }
            (StatusCode::OK, Json(json!({
                "ok": true,
                "b64": b64,
                "page_title": title,
            })))
            .into_response()
        }
        Err(resp) => resp,
    }
}

/// Corps de `POST /v2/upload`.
#[derive(Debug, Deserialize)]
pub struct UploadRequest {
    pub tab_id: Option<i64>,
    pub selector: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
}

/// Handler `POST /v2/upload` — dépose des fichiers dans un input file.
async fn v2_upload(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<UploadRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let selector = match req.selector.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(selector) => selector,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_selector"),
    };
    if req.files.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "missing_files");
    }
    let params = json!({ "tab_id": req.tab_id, "selector": selector, "files": req.files });
    forward(&channel, "upload", params, "upload").await
}

/// Corps de `POST /v2/cdp`.
#[derive(Debug, Deserialize)]
pub struct CdpRequest {
    pub tab_id: Option<i64>,
    pub method: Option<String>,
    #[serde(default)]
    pub params: Value,
}

/// Handler `POST /v2/cdp` — passe-plat Chrome DevTools Protocol.
async fn v2_cdp(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<CdpRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let method = match req.method.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(method) => method,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_method"),
    };
    let params = json!({ "tab_id": req.tab_id, "method": method, "params": req.params });
    forward(&channel, "cdp", params, "cdp").await
}

/// Construit le sous-router `/v2` avec l'auth middleware appliqué.
///
/// **Lot B — D2 (2026-10-07)** : la `SolverConfig` (URLs Ollama +
/// modèle vision) est passée en paramètre puis injectée via
/// `Extension(SolverConfig)` pour que `solve_handler` la lise sans
/// avoir à recharger `Config` à chaque requête. Cf. [`SolverConfig`].
fn v2_router(
    token: String,
    solver_config: SolverConfig,
    channel: ExtensionChannel,
    record: crate::record::RecordState,
) -> Router {
    Router::new()
        .route("/ping", get(v2_ping))
        .route("/info", get(v2_info))
        .route("/metrics", get(v2_metrics))
        .route("/cmd", post(v2_cmd))
        .route("/act", post(v2_act))
        .route("/act/cancel", post(v2_act_cancel))
        .route("/open", post(v2_open))
        .route("/read", post(v2_read))
        // Parité WebBridge (2026-11-06).
        .route("/snapshot", post(v2_snapshot))
        .route("/tabs", post(v2_tabs))
        .route("/send_keys", post(v2_send_keys))
        .route("/network", post(v2_network))
        .route("/pdf", post(v2_pdf))
        .route("/upload", post(v2_upload))
        .route("/cdp", post(v2_cdp))
        .route("/goto", post(v2_goto))
        .route("/click", post(v2_click))
        .route("/fill", post(v2_fill))
        .route("/wait", post(v2_wait))
        .route("/record/start", post(crate::record::start))
        .route("/record/stop", post(crate::record::stop))
        .route("/record/status", get(crate::record::status))
        .route("/page/screenshot", post(v2_page_screenshot))
        .route("/page/captcha", post(v2_page_captcha))
        .route("/page/group", post(v2_page_group))
        // Task 7 : solveur de captcha. Le brief spécifiait
        // `/v2/page/{id}/solve` mais le `{id}` n'est pas utilisé (le
        // `tab_id` du body est prioritaire). En axum 0.7 + matchit,
        // `/page/{id}/solve` peut être capricieux quand des routes
        // statiques voisines (`/page/captcha`, `/page/screenshot`)
        // existent — on observe des 404 sur certaines versions de
        // matchit. On utilise donc une route statique `/page/solve` qui
        // matche toujours, et on documente la deviation dans le rapport.
        .route(
            "/page/solve",
            post(crate::solver::solve_handler),
        )
        .route("/vision/locate", post(crate::solver::locate_handler))
        .route("/atspi/read", post(crate::atspi::lire_handler))
        .route_layer(middleware::from_fn(require_token))
        .layer(Extension(AuthToken(token)))
        .layer(Extension(solver_config))
        .layer(Extension(channel))
        .layer(Extension(record))
}

/// Query string de `/events` : `?limit=N` (défaut 50, plafonné à 200).
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub limit: Option<usize>,
}

/// Handler `GET /events` — retourne les N dernières alertes en JSON.
async fn events_handler(
    Extension(ctx): Extension<VigilCtx>,
    Query(q): Query<EventsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let from_buffer = ctx.alerts.tail(limit).await;
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
    match vigil::tail_jsonl(&path, limit) {
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
fn router_with_auth(ctx: VigilCtx, channel: ExtensionChannel, auth_token: String) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/events", get(events_handler))
        .merge(crate::ingest::ws_router())
        .merge(crate::stats::stats_router())
        .layer(Extension(AuthToken(auth_token)))
        .layer(Extension(channel))
        .layer(Extension(ctx))
}

/// Routeur v1 sans auth WS (mode test / intégration v1). Le binaire de
/// production utilise `router_with_v2`, qui injecte le token réel.
#[allow(dead_code)]
pub fn router(ctx: VigilCtx, channel: ExtensionChannel) -> Router {
    router_with_auth(ctx, channel, String::new())
}

/// Router v1 + sous-router `/v2` protégé par `auth::require_token`.
///
/// Le sous-router /v2 expose `/ping` (trivial), `/cmd` (POST, modèle
/// webbridge brut), `/open` (POST, ouvre un onglet avec garde-fou
/// localhost/RFC1918) et `/read` (POST, lit le innerText d'un onglet).
/// Le `channel` est posé à la fois sur le routeur racine (pour `/ws`)
/// et passé à `v2_router` (pour les routes `/v2/*`).
pub fn router_with_v2(
    ctx: VigilCtx,
    config: Config,
    channel: ExtensionChannel,
) -> Router {
    let token = config.api_token().to_string();
    router_with_auth(ctx, channel.clone(), token.clone()).nest(
        "/v2",
        v2_router(
            token,
            // **Lot B — D2 (2026-10-07)** : on extrait le sous-ensemble
            // solveur (URLs + modèle) une seule fois à la construction
            // du router, puis `v2_router` l'injecte via `Extension`
            // pour que `solve_handler` n'ait plus à recharger la config.
            config.solver_config(),
            channel,
            crate::record::RecordState::default(),
        ),
    )
}

#[cfg(test)]
mod redaction_tests {
    use super::redact_params;
    use serde_json::json;

    #[test]
    fn redact_masque_value_text_code_et_images() {
        let out = redact_params(json!({
            "value": "secret",
            "text": "mot de passe",
            "code": "fetch('x')",
            "image_b64": "AAAA",
            "selector": "#ok",
            "tab_id": 1
        }));
        assert_eq!(out["value"], "[redacted]");
        assert_eq!(out["text"], "[redacted]");
        assert_eq!(out["code"], "[redacted]");
        assert_eq!(out["image_b64"], "[redacted]");
        assert_eq!(out["selector"], "#ok");
        assert_eq!(out["tab_id"], 1);
    }

    /// Le décodage base64 sert à écrire les captures et les PDF demandés
    /// avec un `path` (parité WebBridge 2026-11-06).
    #[test]
    fn base64_decoder_accepts_padding_and_whitespace() {
        use super::b64_decode;
        assert_eq!(b64_decode("aGVsbG8=").expect("valide"), b"hello");
        assert_eq!(b64_decode("aGVs\nbG8=").expect("valide"), b"hello");
        assert!(b64_decode("!!").is_none());
        assert_eq!(b64_decode("").expect("vide"), Vec::<u8>::new());
    }

    /// Les commandes de parité sont bien celles que la barrière de
    /// capacités contrôle (sinon elle ne servirait à rien).
    #[test]
    fn parity_command_list_covers_the_new_routes() {
        use super::PARITY_COMMANDS;
        for cmd in [
            "snapshot",
            "tabs_list",
            "tab_attach",
            "tab_close",
            "session_close",
            "send_keys",
            "network",
            "pdf",
            "upload",
            "cdp",
        ] {
            assert!(PARITY_COMMANDS.contains(&cmd), "commande absente: {cmd}");
        }
    }
}