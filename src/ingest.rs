//! Ingestion WebSocket des événements de navigation + persistance JSONL
//! + vigil (règles d'alerte) + persistance des alertes critical.
//!
//! Route exposée : `GET /ws`.
//!
//! ## Contrat (REWORK 2026-10-05 — modèle webbridge)
//!
//! Le canal WS est **bidirectionnel** :
//!
//! 1. **Extension → démon** : messages JSON de deux formes possibles :
//!    - **Événement de navigation** (legacy) : `{ts, domain, url, kind, bytes?}`.
//!      Persisté dans `${DSH_EYES_HOME}/events.jsonl`. Réponse
//!      `{"ok":true}` ou `{"ok":false,"error":"…"}`.
//!    - **Réponse à une commande** : `{cmd_id, ok, result?, error?}`.
//!      Dispatché vers le `ExtensionChannel` (cf. `hands::channel`) pour
//!      réveiller le `oneshot` de la requête correspondante. Pas
//!      d'ack direct.
//!
//! 2. **Démon → extension** : messages JSON `{cmd_id, cmd, params}`.
//!    L'extension les exécute (chrome.tabs, chrome.scripting) et
//!    répond au format « Réponse à une commande » ci-dessus.
//!
//! L'extension s'auto-enregistre : dès qu'une WS s'ouvre, on crée un
//! sink bidirectionnel (mpsc + task d'envoi) et on appelle
//! `ExtensionChannel::register(handle)`. À la fermeture, on appelle
//! `unregister(handle)` (avec vérification d'identité — cf. channel).
//!
//! ## Règles dev-rust
//!
//! Zéro `unwrap()/expect()` sur le chemin async/réseau — `?` et
//! `Result`. La sérialisation d'un `Ack` constant est documentée
//! « impossible par construction ».

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
use tokio::sync::mpsc;

use crate::hands::channel::{ExtensionChannel, Response as ExtResponse, SinkHandle};
use crate::state::VigilCtx;
use crate::vigil::{self, Alert, AlertLevel};

/// Format d'un événement de navigation reçu sur WS.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct NavEvent {
    pub ts: u64,
    pub domain: String,
    pub url: String,
    pub kind: String,
    pub bytes: Option<u64>,
}

/// Réponse WS du serveur (pour les événements uniquement — les
/// réponses aux commandes passent par `ExtensionChannel`).
#[derive(Serialize)]
struct Ack {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Ack {
    fn ok() -> String {
        // Sérialisation d'un Ack constant : échec impossible par
        // construction ; fallback sur un littéral si jamais.
        serde_json::to_string(&Ack { ok: true, error: None })
            .unwrap_or_else(|_| r#"{"ok":true}"#.to_string())
    }
    fn err(msg: impl Into<String>) -> String {
        let inner = Ack {
            ok: false,
            error: Some(msg.into()),
        };
        serde_json::to_string(&inner)
            .unwrap_or_else(|_| r#"{"ok":false}"#.to_string())
    }
}

/// Message sortant pump-able par la task d'écriture WS. Encapsule
/// les textes JSON (Ack legacy, Command) et les pongs binaires.
#[derive(Debug, Clone)]
enum Outbound {
    Text(String),
    Pong(Vec<u8>),
}

/// Handler axum : upgrade la requête HTTP en WebSocket et délègue à [`handle_socket`].
/// Récupère l'état partagé (vigil + canal extension) via `Extension`.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(ctx): Extension<VigilCtx>,
    Extension(channel): Extension<ExtensionChannel>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, ctx, channel))
}

/// Tampon circulaire thread-safe des dernières alertes émises.
#[derive(Clone)]
pub struct AlertBuffer {
    inner: std::sync::Arc<tokio::sync::RwLock<std::collections::VecDeque<Alert>>>,
}

/// Nombre max d'alertes gardées en mémoire (sert `/events`).
const MAX_IN_MEMORY_ALERTS: usize = 200;

impl AlertBuffer {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::VecDeque::new(),
            )),
        }
    }

    pub async fn push(&self, alert: Alert) {
        let mut g = self.inner.write().await;
        if g.len() == MAX_IN_MEMORY_ALERTS {
            g.pop_front();
        }
        g.push_back(alert);
    }

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

/// Boucle principale côté serveur :
/// 1. Crée un sink bidirectionnel et l'enregistre dans `channel`.
/// 2. Spawn une task de pump qui lit le `rx` du sink et pousse sur
///    le writer WS (les messages sortants — y compris les Ack legacy
///    et les pongs — passent par un mpsc local).
/// 3. Boucle de lecture des messages WS :
///    - `Response` → dispatch vers `channel` (commande traitée).
///    - `NavEvent` (legacy) → vigil + persistance + ack.
///    - autres → ack erreur ou pong.
///
/// Le `SinkHandle` (handler de `channel`) pousse des `String` JSON
/// (les `Command`). Ces strings sont mergées avec le flux local
/// (Acks + Pongs) dans un seul mpsc `Outbound` consommé par la task
/// pump. C'est plus simple que de partager le `ws_sink` entre la
/// boucle de lecture et la route `/v2/cmd` — un seul écrivain, le
/// pump.
async fn handle_socket(socket: WebSocket, ctx: VigilCtx, channel: ExtensionChannel) {
    let (mut ws_sink, mut ws_stream) = socket.split();

    // Canal local pour la task pump — tous les messages sortants
    // (commandes vers l'extension, Acks legacy, Pongs) y transitent.
    let (local_tx, mut local_rx) = mpsc::unbounded_channel::<Outbound>();
    // Clone pour le bridge (sinon la closure move le consomme).
    let local_tx_for_bridge = local_tx.clone();

    // 1) Sink bidirectionnel : la route v2/cmd pousse du texte sur
    //    la WS via ce bridge — on merge la sortie dans `local_rx`.
    let (ext_tx, mut ext_rx) = mpsc::unbounded_channel::<String>();
    let handle = SinkHandle::new(ext_tx);
    channel.register(handle.clone()).await;

    // Bridge ext → local (convertit les `String` JSON en `Outbound::Text`).
    let bridge_task = tokio::spawn(async move {
        while let Some(s) = ext_rx.recv().await {
            if local_tx_for_bridge.send(Outbound::Text(s)).is_err() {
                break;
            }
        }
    });

    // 2) Task de pump : local_rx → ws_sink.
    let pump_task = tokio::spawn(async move {
        while let Some(msg) = local_rx.recv().await {
            let send_result = match msg {
                Outbound::Text(s) => ws_sink.send(Message::Text(s)).await,
                Outbound::Pong(p) => ws_sink.send(Message::Pong(p)).await,
            };
            if send_result.is_err() {
                // Le client a fermé — on arrête le pump.
                break;
            }
        }
    });

    // Helper local : pousse un message sortant (textuel) sur le
    // pump. Retourne `false` si la WS est fermée.
    let send_text = |local_tx: &mpsc::UnboundedSender<Outbound>, s: String| -> bool {
        local_tx.send(Outbound::Text(s)).is_ok()
    };

    // 3) Boucle de lecture.
    let events_path = match events_path() {
        Ok(p) => p,
        Err(e) => {
            let _ = send_text(&local_tx, Ack::err(format!("chemin persistance invalide: {e}")));
            channel.unregister(&handle).await;
            drop(local_tx);
            let _ = pump_task.await;
            let _ = bridge_task.await;
            return;
        }
    };
    let alerts_path = match vigil::alerts_path() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "alertes désactivées : chemin invalide");
            PathBuf::from("/dev/null/alerts.jsonl")
        }
    };

    while let Some(msg) = ws_stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(error = %e, "WS fermée par le client");
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                let raw = text.to_string();

                // Tenter d'abord comme une réponse à une commande
                // (présence de `cmd_id` + `ok`). Si ça match, dispatcher
                // vers le canal. Sinon, tenter comme un événement de
                // navigation.
                if let Ok(resp) = serde_json::from_str::<ExtResponse>(&raw) {
                    if !channel.dispatch_response(resp).await {
                        tracing::debug!("réponse orpheline reçue et ignorée");
                    }
                    continue;
                }

                // Legacy : événement de navigation.
                match serde_json::from_str::<NavEvent>(&raw) {
                    Ok(evt) => {
                        if let Some(alert) =
                            vigil::check(&evt, &ctx.allowlist, &ctx.session).await
                        {
                            ctx.alerts.push(alert.clone()).await;
                            if alert.level == AlertLevel::Critical {
                                if let Err(e) = append_alert(&alerts_path, &alert).await {
                                    tracing::warn!(error = %e, "échec persistance alerte critical");
                                }
                            }
                        }
                        ctx.counters.record_async(&evt.domain, evt.bytes).await;
                        match append_event(&events_path, &evt).await {
                            Ok(()) => {
                                if !send_text(&local_tx, Ack::ok()) {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "échec persistance JSONL");
                                if !send_text(&local_tx, Ack::err(format!("io: {e}"))) {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let err = format!("parse: {e}");
                        if !send_text(&local_tx, Ack::err(err)) {
                            break;
                        }
                    }
                }
            }
            Message::Binary(_) => {
                if !send_text(&local_tx, Ack::err("binary non supporté")) {
                    break;
                }
            }
            Message::Ping(p) => {
                if local_tx.send(Outbound::Pong(p)).is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    // Fermeture : on désenregistre le sink (purge les requêtes en
    // attente) et on ferme les deux tasks (bridge + pump) en droppant
    // les senders.
    channel.unregister(&handle).await;
    drop(local_tx);
    let _ = pump_task.await;
    let _ = bridge_task.await;
    // `handle` est droppé en sortie de scope.
    let _ = handle;
}

/// Calcule le chemin de `events.jsonl` à partir de `DSH_EYES_HOME`.
fn events_path() -> std::io::Result<PathBuf> {
    let home = match std::env::var_os("DSH_EYES_HOME") {
        Some(v) => PathBuf::from(v),
        None => default_home()?,
    };
    std::fs::create_dir_all(&home)?;
    Ok(home.join("events.jsonl"))
}

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
pub fn ws_router() -> Router {
    Router::new().route("/ws", get(ws_handler))
}
