//! Solveur de captcha hiérarchisé (Task 7).
//!
//! ## Architecture
//!
//! Trois niveaux, cascade automatique : Ollama tier 1 → Ollama tier 2 →
//! fallback humain. Le tier 1 et le tier 2 sont deux instances Ollama
//! distinctes configurées via `config.toml` (champs `ollama_tier1_url` et
//! `ollama_tier2_url`). Le modèle vision est partagé (`vision_model`,
//! défaut `qwen2.5-vl`).
//!
//! ## Pourquoi deux tiers
//!
//! Eddie veut une résilience face aux pannes silencieuses d'Ollama
//! (réseau down, instance qui crash, modèle pas encore chargé en RAM).
//! Le tier 1 est le chemin nominal (ia-general sur `10.0.0.223`) ; si
//! il échoue (timeout, 5xx, JSON cassé), on tente le tier 2 (serveur-ia
//! sur `10.0.0.245`). Si les deux échouent, on remonte
//! `{"status":"human_required"}` — le client sait qu'il doit prendre la
//! main.
//!
//! ## Pourquoi pas de tier "local"
//!
//! Pas de fallback GPU local (llama.cpp, etc.) dans cette itération. Le
//! brief mentionne "ia-general → serveur-ia → humain". Un tier local
//! serait une itération future (Eddie tranchera).
//!
//! ## Limites assumées
//!
//! - **v3 invisible** : pas de résolution locale possible (score de
//!   confiance basé sur le comportement de navigation, pas une image).
//!   On remonte `{"status":"unsupported","type":"recaptcha_v3_invisible"}`
//!   pour signaler explicitement que le démon refuse de tenter.
//! - **Application des solutions grille/slider** : on remonte la solution
//!   brute (`"applied":false`) mais on ne l'applique pas. L'application
//!   ciblée (clic sur tuile, drag de slider) nécessitera des commandes
//!   extension dédiées qui n'existent pas encore (Task future).
//! - **Application "text"** : on applique la solution via `fill` + on
//!   tente un `click` sur un bouton submit voisin si détectable.
//!   Heuristique minimaliste v1 ; amélioration future si besoin.

use axum::{
    extract::rejection::JsonRejection,
    extract::{Extension, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::{error_response, send_cmd};
use crate::config::SolverConfig;
use crate::hands::channel::ExtensionChannel;

/// Représentation normalisée du résultat de détection captcha (handler
/// `POST /v2/page/captcha` ET solveur). Forme alignée avec ce que
/// renvoie `background.js::cmdCaptchaDetect`.
///
/// Champs :
/// - `detected` : `false` si rien.
/// - `type` : `"none"|"recaptcha_v2_invisible"|"recaptcha_v2_image"|
///   "recaptcha_v3_invisible"|"hcaptcha"|"text"|"slider"`.
/// - `selector` : sélecteur CSS de l'élément captcha (None si rien).
/// - `image_b64` : capture viewport (sans préfixe MIME) pour les types
///   "image" uniquement. None pour les autres.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptchaInfo {
    #[serde(default)]
    pub detected: bool,
    #[serde(default = "default_type_none")]
    pub r#type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_b64: Option<String>,
}

fn default_type_none() -> String {
    "none".to_string()
}

impl CaptchaInfo {
    /// Convertit la `Value` brute renvoyée par `send_cmd` (résultat de
    /// la commande `captcha_detect` côté extension) en `CaptchaInfo`.
    /// `detected=false` est garanti si la forme est inattendue (jamais
    /// de panic, jamais d'`unwrap`).
    pub fn from_extension_value(v: &Value) -> Self {
        let detected = v
            .get("detected")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        let r#type = v
            .get("type")
            .and_then(|x| x.as_str())
            .unwrap_or("none")
            .to_string();
        let selector = v
            .get("selector")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        let image_b64 = v
            .get("image_b64")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        Self {
            detected,
            r#type,
            selector,
            image_b64,
        }
    }
}

/// Issue d'une tentative de détection. Soit on a un `CaptchaInfo`, soit
/// une `Response` d'erreur déjà formée (504 pas d'extension, 502
/// extension a échoué, 502 forme inattendue).
///
/// Variante d'`enum` au lieu de `Result<CaptchaInfo, Response>` pour
/// pouvoir ajouter d'autres états dans une itération future sans
/// casser les callers (`Err` est sémantiquement ambigu ici — c'est
/// "je n'ai pas pu faire la détection", pas "j'ai détecté mais c'est
/// un échec").
pub enum DetectOutcome {
    Info(CaptchaInfo),
    ErrorResponse(Response),
}

/// Corps de `POST /v2/page/solve` (et `POST /v2/page/{id}/solve`).
/// Le `tab_id` du body est prioritaire sur le `{id}` du path (qui est
/// conservé pour symétrie d'API avec `screenshot` et `captcha`, mais
/// ignoré côté serveur).
#[derive(Debug, Deserialize)]
pub struct SolveRequest {
    pub tab_id: Option<i64>,
}

/// Réponse unifiée de `/v2/page/solve`. Tous les champs sont présents ;
/// `None` signifie "pas applicable à ce statut".
///
/// Statuts possibles :
/// - `"solved"` — un solveur Ollama a renvoyé une réponse (tier 1 ou 2).
/// - `"none"` — aucun captcha détecté, rien à faire.
/// - `"unsupported"` — type détecté hors capacité locale (v3 invisible).
/// - `"human_required"` — les deux tiers Ollama ont échoué.
#[derive(Debug, Serialize)]
pub struct SolveResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,
    pub applied: bool,
    /// `1` ou `2` selon le tier qui a réussi ; `null` sinon.
    pub tier_used: Option<u8>,
}

/// Détecte un captcha via la commande extension `captcha_detect`.
///
/// **Fonction partagée** entre `POST /v2/page/captcha` (handler qui
/// renvoie la détection brute) et `POST /v2/page/{id}/solve` (solveur
/// qui orchestre la résolution). L'extension est la source de vérité
/// de la détection (heuristiques DOM côté content-script).
pub async fn detect_captcha(
    channel: &ExtensionChannel,
    tab_id: Option<i64>,
) -> DetectOutcome {
    let params = json!({ "tab_id": tab_id });
    let value = match send_cmd(channel, "captcha_detect", params).await {
        Ok(v) => v,
        Err(resp) => return DetectOutcome::ErrorResponse(resp),
    };
    // send_cmd retourne Ok(Value) déjà déballée. Si jamais la forme
    // est inattendue, on retourne un `none` — pas une 502 (la détection
    // elle-même n'est pas en erreur, on n'a juste pas pu la lire).
    DetectOutcome::Info(CaptchaInfo::from_extension_value(&value))
}

/// Client HTTP vers Ollama. Deux URLs (tier 1 + tier 2) + un `Client`
/// reqwest configuré avec le timeout [`OLLAMA_TIMEOUT`]. Le client est
/// construit une fois par appel `/solve` (pas de cache ; un
/// `reqwest::Client` est lui-même un pool réutilisable mais le coût de
/// reconstruction est négligeable à l'usage).
pub struct OllamaClient {
    tier1_url: String,
    tier2_url: String,
    model: String,
    http: reqwest::Client,
}

/// Délai d'attente pour la réponse d'un Ollama (par tier). 60 s
/// conformément au brief — au-delà on tente le tier suivant.
pub const OLLAMA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Erreur émise par [`OllamaClient::solve`] après que les deux tiers
/// ont échoué. On garde le détail des deux erreurs pour le log.
#[derive(Debug)]
pub struct OllamaError {
    pub tier1_err: String,
    pub tier2_err: String,
}

impl OllamaClient {
    /// Construit un client Ollama à partir de la config solveur
    /// ([`SolverConfig`]) injectée via `Extension` axum. Pas de
    /// validation des URLs ici — un format incorrect sera détecté au
    /// premier appel HTTP (erreur réseau reportée par reqwest).
    ///
    /// **Lot B — D2 (2026-10-07)** : on n'utilise plus `&Config` (qui
    /// contient aussi le `api_token` qu'on ne veut pas dupliquer dans
    /// le handler). La [`SolverConfig`] est le sous-ensemble strict
    /// nécessaire au solveur.
    pub fn new(solver: &SolverConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(OLLAMA_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            tier1_url: solver.tier1.clone(),
            tier2_url: solver.tier2.clone(),
            model: solver.model.clone(),
            http,
        }
    }

    /// Variante de test : permet d'injecter un timeout différent (utile
    /// pour les tests d'intégration qui ne veulent pas attendre 60 s
    /// même si la performance est rapide).
    #[allow(dead_code)]
    pub fn new_with_timeout(solver: &SolverConfig, timeout: std::time::Duration) -> Self {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            tier1_url: solver.tier1.clone(),
            tier2_url: solver.tier2.clone(),
            model: solver.model.clone(),
            http,
        }
    }

    /// Tente le solveur tier 1 ; en cas d'échec, tente tier 2.
    /// Retourne `(answer, tier_used)` en succès, `OllamaError` sinon.
    ///
    /// "Échec" = timeout réseau, statut HTTP ≥ 400, JSON invalide, ou
    /// réponse Ollama sans `response` exploitable.
    pub async fn solve(&self, image_b64: &str) -> Result<(String, u8), OllamaError> {
        let prompt = "This image contains a CAPTCHA. \
            Answer with ONLY the solution: if it is text, \
            give the characters exactly; if it is an image grid \
            (select tiles), reply as JSON array of tile indexes \
            starting at 1, row-major. No other words.";
        let tier1_err = match self.try_tier(&self.tier1_url, image_b64, prompt).await {
            Ok(answer) => return Ok((answer, 1)),
            Err(e) => e,
        };
        let tier2_err = match self.try_tier(&self.tier2_url, image_b64, prompt).await {
            Ok(answer) => return Ok((answer, 2)),
            Err(e) => e,
        };
        Err(OllamaError { tier1_err, tier2_err })
    }

    /// Tente un tier unique. Retourne `Ok(answer)` ou `Err(msg)`.
    /// Privé : seul [`OllamaClient::solve`] l'appelle.
    async fn try_tier(&self, base_url: &str, image_b64: &str, prompt: &str) -> Result<String, String> {
        // Endpoint Ollama /api/generate. La forme exacte du JSON est
        // imposée par l'API Ollama : `model`, `prompt`, `images: [b64]`,
        // `stream: false`. On retire tout préfixe MIME potentiel du
        // b64 (l'extension peut envoyer la forme brute ou avec préfixe).
        let url = format!("{}/api/generate", base_url.trim_end_matches('/'));
        let body = json!({
            "model": self.model,
            "prompt": prompt,
            "images": [strip_data_url_prefix(image_b64)],
            "stream": false,
        });
        let resp = match self.http.post(&url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("HTTP error: {e}"));
            }
        };
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(format!("HTTP status {status}"));
        }
        let value: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => return Err(format!("JSON parse error: {e}")),
        };
        // Ollama répond `{"response":"…", "done":true, …}`. On extrait
        // `response` (string). Si manquant ou pas string → erreur.
        match value.get("response").and_then(|v| v.as_str()) {
            Some(s) => {
                let s = s.trim().to_string();
                if s.is_empty() {
                    Err("empty response field".to_string())
                } else {
                    Ok(s)
                }
            }
            None => Err("missing 'response' field".to_string()),
        }
    }
}

/// Strippe le préfixe MIME `data:image/...;base64,` d'une chaîne base64
/// si présent. Ollama attend du base64 brut.
fn strip_data_url_prefix(s: &str) -> &str {
    s.split_once(',')
        .map(|(_, payload)| payload)
        .unwrap_or(s)
}

/// Tente d'appliquer la solution text via `fill` + `click` sur un bouton
/// submit voisin. Renvoie `true` si l'application a réussi.
///
/// Heuristique v1 : on applique `fill` sur le sélecteur du captcha si
/// il pointe vers un input, puis on cherche un bouton submit
/// générique (`button[type="submit"], input[type="submit"]`) et on
/// clique. Pas de garantie de succès — l'extension peut refuser
/// (sélecteur hors DOM). On remonte `false` au caller, qui mettra
/// `"applied":false` dans la réponse.
async fn try_apply_text_solution(
    channel: &ExtensionChannel,
    tab_id: Option<i64>,
    selector: &str,
    answer: &str,
) -> bool {
    // 1. fill sur le sélecteur du captcha.
    let fill_params = json!({
        "tab_id": tab_id,
        "selector": selector,
        "value": answer,
    });
    let fill_ok = send_cmd(channel, "fill", fill_params).await.is_ok();
    if !fill_ok {
        return false;
    }
    // 2. click sur un submit voisin. On tente quelques sélecteurs
    //    courants ; le premier qui répond est le bon.
    for submit_sel in ["button[type=\"submit\"]", "input[type=\"submit\"]", "button"] {
        let click_params = json!({
            "tab_id": tab_id,
            "selector": submit_sel,
        });
        if send_cmd(channel, "click", click_params).await.is_ok() {
            return true;
        }
    }
    // fill a réussi mais aucun submit trouvé : on a appliqué le texte
    // sans soumettre. On retourne true quand même (l'utilisateur peut
    // soumettre lui-même), mais avec une nuance — `applied` sera
    // false si fill a échoué.
    fill_ok
}

/// Handler `POST /v2/page/{id}/solve` — résout un captcha détecté via
/// la cascade tier 1 → tier 2 → humain.
///
/// Étapes :
/// 1. Détection via [`detect_captcha`].
/// 2. Selon le type :
///    - `none` → `{"status":"none"}`.
///    - `recaptcha_v3_invisible` → `{"status":"unsupported","type":…}`.
///    - type "image" sans `image_b64` → erreur (extension a détecté
///      mais n'a pas pu capturer le viewport).
///    - autre → solveur Ollama avec fallback tier 2.
/// 3. Si type `text` et solve OK, applique via `fill`+`click`.
/// 4. Réponse unifiée [`SolveResponse`].
pub async fn solve_handler(
    Extension(channel): Extension<ExtensionChannel>,
    Extension(solver_config): Extension<SolverConfig>,
    body: Result<Json<SolveRequest>, JsonRejection>,
) -> Response {
    // On accepte les deux formes : `Json<SolveRequest>` ou rejet
    // (pour gérer le cas body invalide — `?` ne marche pas avec
    // JsonRejection qui n'est pas Error).
    let req = match body {
        Ok(Json(r)) => r,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_body");
        }
    };
    let tab_id = req.tab_id;
    crate::api::log_action("solve", serde_json::json!({ "tab_id": tab_id }));

    // 1. Détection.
    let info = match detect_captcha(&channel, tab_id).await {
        DetectOutcome::Info(i) => i,
        DetectOutcome::ErrorResponse(resp) => return resp,
    };

    // 2. Switch sur le type détecté.
    if !info.detected || info.r#type == "none" {
        return (
            StatusCode::OK,
            Json(SolveResponse {
                status: "none".to_string(),
                r#type: Some(info.r#type),
                answer: None,
                applied: false,
                tier_used: None,
            }),
        )
            .into_response();
    }

    if info.r#type == "recaptcha_v3_invisible" {
        return (
            StatusCode::OK,
            Json(SolveResponse {
                status: "unsupported".to_string(),
                r#type: Some(info.r#type),
                answer: None,
                applied: false,
                tier_used: None,
            }),
        )
            .into_response();
    }

    // Types image / text / slider : on a besoin d'un image_b64.
    // Un type détecté sans image exploitable (ex. recaptcha_v2_invisible)
    // = statut `unsupported` honnête (pas une erreur 502) : le solveur
    // local ne peut rien en faire.
    let image_b64 = match &info.image_b64 {
        Some(b) if !b.is_empty() => b.clone(),
        _ => {
            return (
                StatusCode::OK,
                Json(SolveResponse {
                    status: "unsupported".to_string(),
                    r#type: Some(info.r#type.clone()),
                    answer: None,
                    applied: false,
                    tier_used: None,
                }),
            )
                .into_response();
        }
    };

    // 3. Solveur Ollama. La config (URLs + modèle) est injectée
    // via `Extension(SolverConfig)` depuis `router_with_v2` — pas de
    // lecture disque par requête, plus de `spawn_blocking` sur le
    // chemin chaud.
    //
    // **Lot B — D2 (2026-10-07)** : on supprime le
    // `tokio::task::spawn_blocking(Config::load)` qui était coûteux
    // (I/O synchrone) et serialisait inutilement les requêtes
    // concurrentes sur le même pool blocking. Le client HTTP
    // `reqwest` interne est lui-même un pool réutilisable, donc
    // aucune perte de performance.
    let client = OllamaClient::new(&solver_config);
    let (answer, tier) = match client.solve(&image_b64).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(tier1_err = %e.tier1_err, tier2_err = %e.tier2_err,
                "/solve : solveurs Ollama indisponibles, fallback humain");
            return (
                StatusCode::OK,
                Json(SolveResponse {
                    status: "human_required".to_string(),
                    r#type: Some(info.r#type.clone()),
                    answer: None,
                    applied: false,
                    tier_used: None,
                }),
            )
                .into_response();
        }
    };

    // 4. Application si type text.
    let applied = if info.r#type == "text" {
        if let Some(sel) = info.selector.as_deref() {
            try_apply_text_solution(&channel, tab_id, sel, &answer).await
        } else {
            false
        }
    } else {
        false
    };

    (
        StatusCode::OK,
        Json(SolveResponse {
            status: "solved".to_string(),
            r#type: Some(info.r#type.clone()),
            answer: Some(answer),
            applied,
            tier_used: Some(tier),
        }),
    )
        .into_response()
}

// ─── Tests unitaires ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captcha_info_from_extension_value_handles_minimal_none() {
        let v = json!({"detected": false, "type": "none"});
        let info = CaptchaInfo::from_extension_value(&v);
        assert!(!info.detected);
        assert_eq!(info.r#type, "none");
        assert!(info.selector.is_none());
        assert!(info.image_b64.is_none());
    }

    #[test]
    fn captcha_info_from_extension_value_handles_recaptcha_image() {
        let v = json!({
            "detected": true,
            "type": "recaptcha_v2_image",
            "selector": "iframe[src*=\"recaptcha\"]",
            "image_b64": "ABCD"
        });
        let info = CaptchaInfo::from_extension_value(&v);
        assert!(info.detected);
        assert_eq!(info.r#type, "recaptcha_v2_image");
        assert_eq!(info.selector.as_deref(), Some("iframe[src*=\"recaptcha\"]"));
        assert_eq!(info.image_b64.as_deref(), Some("ABCD"));
    }

    #[test]
    fn captcha_info_from_extension_value_tolerates_missing_fields() {
        let v = json!({});
        let info = CaptchaInfo::from_extension_value(&v);
        assert!(!info.detected);
        assert_eq!(info.r#type, "none");
        assert!(info.selector.is_none());
        assert!(info.image_b64.is_none());
    }

    #[test]
    fn strip_data_url_prefix_handles_data_url() {
        assert_eq!(
            strip_data_url_prefix("data:image/png;base64,ABCDE"),
            "ABCDE"
        );
    }

    #[test]
    fn strip_data_url_prefix_handles_bare_b64() {
        assert_eq!(strip_data_url_prefix("ABCDE"), "ABCDE");
    }

    #[test]
    fn strip_data_url_prefix_handles_empty() {
        assert_eq!(strip_data_url_prefix(""), "");
    }
}

// ─── Bloc `FromRequest` pour gérer les rejections JSON ──────────────────
//
// Le brief impose zéro `unwrap`/`expect` async et zéro panic. On
// duplique le pattern `ValidatedJson` de `api.rs` ici plutôt que
// d'exposer l'instance (l'API publique de `solver` reste minimale).
#[derive(Debug)]
struct JsonBodyError(String);

impl JsonBodyError {
    fn from_rejection(e: JsonRejection) -> Self {
        Self(e.body_text())
    }
}

impl IntoResponse for JsonBodyError {
    fn into_response(self) -> Response {
        tracing::debug!(error = %self.0, "rejet Json body (solve)");
        error_response(StatusCode::BAD_REQUEST, "invalid_body")
    }
}

#[allow(dead_code)]
struct ValidatedJson<T>(Json<T>);

#[axum::async_trait]
#[allow(dead_code)]
impl<T, S> axum::extract::FromRequest<S> for ValidatedJson<T>
where
    T: serde::de::DeserializeOwned + 'static,
    S: Send + Sync,
    Json<T>: axum::extract::FromRequest<S, Rejection = JsonRejection>,
{
    type Rejection = JsonBodyError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(j) => Ok(ValidatedJson(j)),
            Err(e) => Err(JsonBodyError::from_rejection(e)),
        }
    }
}