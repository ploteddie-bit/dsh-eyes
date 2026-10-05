//! Ingestion WebSocket des événements de navigation + persistance JSONL
//! + vigil (règles d'alerte) + persistance des alertes critical.
//!
//! Route exposée : `GET /ws`.
//!
//! Contrat (cf. brief Task 2 + Task 4) :
//! - Message JSON valide `{ts, domain, url, kind, bytes?}` → append d'une ligne
//!   JSONL dans `${DSH_EYES_HOME}/events.jsonl` (défaut `~/.dsh-eyes/events.jsonl`)
//!   puis réponse WS `{"ok":true}`.
//! - Vigil : sur chaque événement valide, `vigil::check` est appelé.
//!   - Si une alerte `critical` est émise, elle est aussi persistée dans
//!     `${DSH_EYES_HOME}/alerts.jsonl` (une ligne JSONL par alerte).
//!   - Si une alerte est émise (peu importe le niveau), elle est mise en
//!     mémoire dans la `SharedSession` (consultable via `GET /events`).
//! - Message invalide (JSON cassé, champ manquant, champ supplémentaire,
//!   type mauvais) → réponse WS `{"ok":false,"error":"…"}`. Jamais de panic,
//!   jamais de crash de connexion.
//!
//! Règle dev-rust : zéro `unwrap()/expect()` sur le chemin async/réseau —
//! `?` et `Result`. (La sérialisation d'un `Ack` constant est documentée
//! « impossible par construction ».)

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Extension,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt as _;

use crate::state::VigilCtx;
use crate::vigil::{self, Alert, AlertLevel};

/// Format d'un événement de navigation reçu sur WS.
/// `#[serde(deny_unknown_fields)]` garantit que les clients envoyant des champs
/// inattendus sont rejetés (réponse `{"ok":false,"error":"unknown field …"}`).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NavEvent {
    pub ts: u64,
    pub domain: String,
    pub url: String,
    pub kind: String,
    pub bytes: Option<u64>,
}

/// Réponse WS du serveur.
#[derive(Serialize)]
struct Ack {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Ack {
    fn ok() -> Message {
        // Sérialisation d'une structure constante : l'échec est impossible par
        // construction, on retombe sur un littéral plutôt que de paniquer.
        let s = serde_json::to_string(&Ack {
            ok: true,
            error: None,
        })
        .unwrap_or_else(|_| r#"{"ok":true}"#.to_string());
        Message::Text(s)
    }
    fn err(msg: impl Into<String>) -> Message {
        let inner = Ack {
            ok: false,
            error: Some(msg.into()),
        };
        let s = serde_json::to_string(&inner)
            .unwrap_or_else(|_| r#"{"ok":false}"#.to_string());
        Message::Text(s)
    }
}

/// Handler axum : upgrade la requête HTTP en WebSocket et délègue à [`handle_socket`].
/// Récupère le `VigilCtx` partagé via `Extension` (câblé par `api::router`).
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(ctx): Extension<VigilCtx>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, ctx))
}

/// Tampon circulaire thread-safe des dernières alertes émises.
///
/// Mêmes justifications que `Counters` : `Arc<RwLock<VecDeque<...>>>` —
/// lecture concurrente, écriture courte. Borné à [`MAX_IN_MEMORY_ALERTS`]
/// entrées (FIFO) pour éviter une croissance non maîtrisée. Les alertes
/// `critical` sont par ailleurs persistées dans `alerts.jsonl` (durée
/// arbitraire, c'est l'historique).
#[derive(Clone)]
pub struct AlertBuffer {
    inner: std::sync::Arc<tokio::sync::RwLock<std::collections::VecDeque<Alert>>>,
}

/// Nombre max d'alertes gardées en mémoire (sert `/events`).
/// 200 = 4× la fenêtre « 50 dernières » demandée par `/events` ; marge
/// confortable pour absorber un burst avant que la fenêtre ne se vide.
const MAX_IN_MEMORY_ALERTS: usize = 200;

impl AlertBuffer {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::VecDeque::new(),
            )),
        }
    }

    /// Pousse une alerte. Si le buffer est plein, on retire la plus ancienne.
    pub async fn push(&self, alert: Alert) {
        let mut g = self.inner.write().await;
        if g.len() == MAX_IN_MEMORY_ALERTS {
            g.pop_front();
        }
        g.push_back(alert);
    }

    /// Copie (sous lock) des N dernières alertes.
    pub async fn tail(&self, n: usize) -> Vec<Alert> {
        let g = self.inner.read().await;
        let take = n.min(g.len());
        g.iter().rev().take(take).cloned().collect::<Vec<_>>()
    }
}

impl Default for AlertBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Boucle principale côté serveur : lit les messages texte, parse, persiste,
/// applique le vigil, répond.
async fn handle_socket(socket: WebSocket, ctx: VigilCtx) {
    let (mut sink, mut stream) = socket.split();
    let events_path = match events_path() {
        Ok(p) => p,
        Err(e) => {
            let _ = sink.send(Ack::err(format!("chemin persistance invalide: {e}"))).await;
            let _ = sink.close().await;
            return;
        }
    };
    let alerts_path = match vigil::alerts_path() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "alertes désactivées : chemin invalide");
            // On continue sans persistance d'alertes (le buffer en mémoire
            // reste fonctionnel pour /events le temps de la session).
            PathBuf::from("/dev/null/alerts.jsonl")
        }
    };

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "erreur WS côté serveur, fermeture");
                let _ = sink.send(Ack::err(format!("transport: {e}"))).await;
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                let raw = text.to_string();
                match serde_json::from_str::<NavEvent>(&raw) {
                    Ok(evt) => {
                        // 1) Vigil : alerte éventuelle.
                        // Note : le check est async (Mutex sur SessionState)
                        // mais ne fait pas d'I/O. On l'appelle en premier pour
                        // que les alertes soient dans le buffer AVANT l'ack
                        // (sinon un client rapide pourrait voir `ok:true` et
                        // `/events` vide).
                        if let Some(alert) =
                            vigil::check(&evt, &ctx.allowlist, &ctx.session).await
                        {
                            // Buffer en mémoire (toujours).
                            ctx.alerts.push(alert.clone()).await;
                            // Persistance disque : uniquement `critical` (cf. brief).
                            if alert.level == AlertLevel::Critical {
                                if let Err(e) = append_alert(&alerts_path, &alert).await {
                                    tracing::warn!(error = %e, "échec persistance alerte critical");
                                }
                            }
                        }

                        // 2) Incrémente les compteurs (sémantique Task 3 :
                        // événement syntaxiquement valide → accepté).
                        ctx.counters.record_async(&evt.domain, evt.bytes).await;

                        // 3) Persiste puis acquitte.
                        match append_event(&events_path, &evt).await {
                            Ok(()) => {
                                if sink.send(Ack::ok()).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "échec persistance JSONL");
                                if sink
                                    .send(Ack::err(format!("io: {e}")))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let err = format!("parse: {e}");
                        if sink.send(Ack::err(err)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Message::Binary(_) => {
                if sink
                    .send(Ack::err("binary non supporté"))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Message::Ping(p) => {
                if sink.send(Message::Pong(p)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(reason) => {
                let _ = sink.send(Message::Close(reason)).await;
                break;
            }
        }
    }
}

/// Calcule le chemin de `events.jsonl` à partir de `DSH_EYES_HOME`
/// (défaut `~/.dsh-eyes`). Crée le dossier si besoin.
fn events_path() -> std::io::Result<PathBuf> {
    let home = match std::env::var_os("DSH_EYES_HOME") {
        Some(v) => PathBuf::from(v),
        None => default_home()?,
    };
    std::fs::create_dir_all(&home)?;
    Ok(home.join("events.jsonl"))
}

/// Défaut `~/.dsh-eyes` (résolution via `HOME` ou `USERPROFILE`).
fn default_home() -> std::io::Result<PathBuf> {
    if let Some(h) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(h).join(".dsh-eyes"));
    }
    if let Some(h) = std::env::var_os("USERPROFILE") {
        return Ok(PathBuf::from(h).join(".dsh-eyes"));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "ni HOME ni USERPROFILE défini",
    ))
}

/// Append d'un événement en JSONL (une ligne, `\n` final).
async fn append_event(path: &std::path::Path, evt: &NavEvent) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let mut buf = serde_json::to_vec(evt)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    buf.push(b'\n');
    file.write_all(&buf).await?;
    file.flush().await?;
    Ok(())
}

/// Append d'une alerte en JSONL (même format qu'`events.jsonl`).
async fn append_alert(path: &std::path::Path, alert: &Alert) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let mut buf = serde_json::to_vec(alert)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    buf.push(b'\n');
    file.write_all(&buf).await?;
    file.flush().await?;
    Ok(())
}

/// Sous-router WS — utile aux tests d'intégration pour isoler `/ws`.
/// Ne pose plus de `with_state` — l'état est fourni par le router racine
/// via `Extension<VigilCtx>`.
pub fn ws_router() -> Router {
    Router::new().route("/ws", get(ws_handler))
}
