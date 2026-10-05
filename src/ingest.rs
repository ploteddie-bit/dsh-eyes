//! Ingestion WebSocket des événements de navigation + persistance JSONL.
//!
//! Route exposée : `GET /ws`.
//!
//! Contrat (cf. brief Task 2) :
//! - Message JSON valide `{ts, domain, url, kind, bytes?}` → append d'une ligne JSONL
//!   dans `${DSH_EYES_HOME}/events.jsonl` (défaut `~/.dsh-eyes/events.jsonl`)
//!   puis réponse WS `{"ok":true}`.
//! - Message invalide (JSON cassé, champ manquant, champ supplémentaire, type mauvais) →
//!   réponse WS `{"ok":false,"error":"…"}`. Jamais de panic, jamais de crash de connexion.
//!
//! Règle dev-rust : zéro `unwrap()/expect()` sur le chemin async/réseau — `?` et `Result`.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt as _;

use crate::stats::Counters;

/// Format d'un événement de navigation reçu sur WS.
/// `#[serde(deny_unknown_fields)]` garantit que les clients envoyant des champs
/// inattendus sont rejetés (réponse `{"ok":false,"error":"unknown field …"}`).
#[derive(Debug, Deserialize, Serialize)]
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
/// Récupère le `Counters` partagé via `State` (câblé par `ws_router`).
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(counters): State<Counters>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, counters))
}

/// Boucle principale côté serveur : lit les messages texte, parse, persiste, répond.
/// `counters` est cloné dans la closure d'upgrade : pas besoin de le re-cloner par message.
async fn handle_socket(socket: WebSocket, counters: Counters) {
    let (mut sink, mut stream) = socket.split();
    let path = match events_path() {
        Ok(p) => p,
        Err(e) => {
            // Si on ne peut même pas calculer le chemin, on ne peut rien faire d'utile.
            // On tente un dernier message d'erreur puis on ferme proprement.
            let _ = sink.send(Ack::err(format!("chemin persistance invalide: {e}"))).await;
            let _ = sink.close().await;
            return;
        }
    };

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                // Erreur de transport WS — on notifie puis on quitte la boucle.
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
                        // Incrémente les compteurs *avant* la persistance :
                        // l'événement est syntaxiquement valide, donc « accepté ».
                        // (cf. brief Task 3)
                        counters.record_async(&evt.domain, evt.bytes).await;
                        // Persiste puis acquitte.
                        match append_event(&path, &evt).await {
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
                        // Message invalide — on répond {"ok":false,"error":"…"} sans fermer.
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
                // tokio-tungstenite répond automatiquement aux pings,
                // mais on peut aussi renvoyer un Pong explicite si on veut.
                if sink.send(Message::Pong(p)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {
                // No-op : client confirme un ping.
            }
            Message::Close(reason) => {
                // Acquitte la fermeture puis quitte.
                let _ = sink.send(Message::Close(reason)).await;
                break;
            }
        }
    }

    // Sink drop : la connexion se ferme.
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

/// Sous-router WS — utile aux tests d'intégration pour isoler `/ws`.
/// Le `Counters` est partagé via `State` (cf. choix d'architecture api.rs).
pub fn ws_router(counters: Counters) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(counters)
}