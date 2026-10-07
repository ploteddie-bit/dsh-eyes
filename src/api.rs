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
    extract::{Extension, Request},
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
pub(crate) async fn send_cmd(
    channel: &ExtensionChannel,
    cmd: impl Into<String>,
    params: Value,
) -> Result<Value, Response> {
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

/// Journalise une action v2 dans `actions.jsonl` (Task 5 — traçabilité
/// « toute action est loggée »). Fire-and-forget : l'échec d'écriture est
/// loggé mais ne fait jamais échouer l'action métier.
///
/// Format : `{"ts":<ms>,"action":"open|goto|click|fill|wait|cmd|solve","detail":{…}}`
pub(crate) fn log_action(action: &str, detail: Value) {
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
    let host = match extract_host(url) {
        Some(h) => h,
        None => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_url");
        }
    };
    if vigil::is_forbidden_host(&host) {
        tracing::warn!(host = %host, "/v2/open : hôte interdit");
        return error_response(StatusCode::FORBIDDEN, "forbidden_target");
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

/// Corps de `POST /v2/read` : un `tab_id` (entier) à lire.
#[derive(Debug, Deserialize)]
pub struct ReadRequest {
    /// Identifiant entier de l'onglet à lire (renvoyé par `/v2/open`
    /// ou par `POST /v2/cmd` avec cmd=`tabs_create`).
    pub tab_id: Option<i64>,
}

/// Réponse de `POST /v2/read` : `{ok:true, text:"…"}` en cas de succès.
#[derive(Debug, Serialize)]
pub struct ReadResponse {
    pub ok: bool,
    /// Texte tronqué (200 Ko côté extension par défaut). `None` si erreur.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
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
    let params = json!({ "tab_id": tab_id });
    log_action("read", params.clone());
    match send_cmd(&channel, "read", params).await {
        Ok(result) => {
            // L'extension renvoie `{text: "..."}`. Si la forme est
            // inattendue (panique JSON côté extension), on tombe sur
            // `null` et on retourne une 502.
            let text = result
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match text {
                Some(t) => {
                    let body = ReadResponse {
                        ok: true,
                        text: Some(t),
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

/// Corps de `POST /v2/goto {"url"}`.
#[derive(Debug, Deserialize)]
pub struct GotoRequest {
    pub url: Option<String>,
}

/// Handler `POST /v2/goto` — même sémantique que `/v2/open` (la commande
/// extension `goto` fait `chrome.tabs.create`) ; garde-fou identique.
async fn v2_goto(
    Extension(channel): Extension<ExtensionChannel>,
    body: ValidatedJson<GotoRequest>,
) -> Response {
    let ValidatedJson(Json(req)) = body;
    let url = match req.url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(u) => u,
        None => return error_response(StatusCode::BAD_REQUEST, "missing_url"),
    };
    let host = match extract_host(url) {
        Some(h) => h,
        None => return error_response(StatusCode::BAD_REQUEST, "invalid_url"),
    };
    if vigil::is_forbidden_host(&host) {
        tracing::warn!(host = %host, "/v2/goto : hôte interdit");
        return error_response(StatusCode::FORBIDDEN, "forbidden_target");
    }
    let params = json!({ "url": url, "active": true });
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
}

/// Réponse de `POST /v2/page/screenshot` : `{ok:true, format, b64}`.
#[derive(Debug, Serialize)]
pub struct ScreenshotResponse {
    pub ok: bool,
    /// Format effectivement utilisé (`png` ou `jpeg`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Chaîne base64 (sans le préfixe `data:image/...;base64,`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b64: Option<String>,
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
    let params = json!({ "tab_id": req.tab_id, "format": format });
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
                    let body = ScreenshotResponse {
                        ok: true,
                        format: fmt.or_else(|| Some("png".to_string())),
                        b64: Some(b64),
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

/// Extrait le host (schéma + autorité) d'une URL `http://host[:port]/…`
/// ou `https://host[:port]/…`. Retourne `None` si l'URL est mal formée.
///
/// On évite la dépendance à `url::Url` (légère mais optionnelle) — un
/// split sur `://` puis sur `/` suffit pour les formes canoniques.
/// C'est ce que fait aussi l'extension (`new URL(...)`) pour `webRequest`.
fn extract_host(url: &str) -> Option<String> {
    // Schéma http ou https uniquement (pas de file://, ws://, javascript:).
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    let authority = rest.split('/').next()?;
    let host = authority.split(':').next()?;
    if host.is_empty() {
        return None;
    }
    // Décode les crochets IPv6 : `http://[::1]/` → `::1`.
    let host = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .map(|s| s.to_string())
        .unwrap_or_else(|| host.to_string());
    Some(host)
}

/// Construit le sous-router `/v2` avec l'auth middleware appliqué.
///
/// **Lot B — D2 (2026-10-07)** : la `SolverConfig` (URLs Ollama +
/// modèle vision) est passée en paramètre puis injectée via
/// `Extension(SolverConfig)` pour que `solve_handler` la lise sans
/// avoir à recharger `Config` à chaque requête. Cf. [`SolverConfig`].
fn v2_router(token: String, solver_config: SolverConfig, channel: ExtensionChannel) -> Router {
    Router::new()
        .route("/ping", get(v2_ping))
        .route("/cmd", post(v2_cmd))
        .route("/open", post(v2_open))
        .route("/read", post(v2_read))
        .route("/goto", post(v2_goto))
        .route("/click", post(v2_click))
        .route("/fill", post(v2_fill))
        .route("/wait", post(v2_wait))
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
        .route_layer(middleware::from_fn(require_token))
        .layer(Extension(AuthToken(token)))
        .layer(Extension(solver_config))
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
    router(ctx, channel.clone()).nest(
        "/v2",
        v2_router(
            config.api_token().to_string(),
            // **Lot B — D2 (2026-10-07)** : on extrait le sous-ensemble
            // solveur (URLs + modèle) une seule fois à la construction
            // du router, puis `v2_router` l'injecte via `Extension`
            // pour que `solve_handler` n'ait plus à recharger la config.
            config.solver_config(),
            channel,
        ),
    )
}