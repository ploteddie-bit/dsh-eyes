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
        Extension, Query,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;

use crate::auth::{ct_eq, AuthToken};
use crate::hands::channel::{ExtensionChannel, HelloInfo, Response as ExtResponse, SinkHandle};
use crate::state::VigilCtx;
use crate::vigil::{self, Alert, AlertLevel};

/// Query string de `/ws` : le token d'API est passé en paramètre
/// (`?token=...`) car l'API WebSocket des navigateurs ne permet pas
/// d'envoyer un header `Authorization` personnalisé.
#[derive(Debug, Deserialize)]
pub struct WsQuery {
    pub token: Option<String>,
}

/// Enveloppe du handshake envoyé par l'extension à l'ouverture :
/// `{"hello":{"client":"…","version":"…","capabilities":[…]}}`.
#[derive(Debug, Deserialize)]
struct HelloEnvelope {
    hello: HelloInfo,
}

/// Keep-alive applicatif : `{"ping": <ts>}` → réponse `{"pong": <ts>}`.
#[derive(Debug, Deserialize)]
struct PingEnvelope {
    ping: u64,
}

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

/// Seuil de flush en nombre de lignes accumulées dans le
/// `BufWriter` interne. Au-delà, un `flush()` est forcé.
const EVENT_WRITER_FLUSH_LINES: usize = 10;

/// Idle maximum toléré avant qu'un `flush()` idle soit déclenché.
const EVENT_WRITER_FLUSH_IDLE: Duration = Duration::from_millis(500);

/// Écrivain JSONL bufferisé — dédie une tâche tokio à l'écriture
/// des [`NavEvent`] dans un fichier append-unique ouvert une seule
/// fois pour toute la durée de la connexion WS.
///
/// # Stratégie (D1 — Lot A 2026-10-07)
///
/// - Canal `mpsc::unbounded` : `append` est fire-and-forget (jamais
///   d'échec bloquant pour l'appelant — un canal saturé est un
///   *bug* investigué, pas un cas attendu).
/// - Tâche écrivain propriétaire d'un `BufWriter<File>` ouvert en
///   append. Flush forcé toutes les [`EVENT_WRITER_FLUSH_LINES`]
///   lignes OU après [`EVENT_WRITER_FLUSH_IDLE`] d'idle (via
///   `tokio::time::timeout` autour de `recv`).
/// - À la fermeture du canal (sender droppée) : flush final + exit
///   propre. `handle_socket` attend la `JoinHandle` avant de
///   retourner, garantissant 0 événement perdu.
pub struct EventWriter {
    tx: mpsc::UnboundedSender<NavEvent>,
}

impl EventWriter {
    /// Démarre la tâche écrivain. Retourne le handle fire-and-forget
    /// et la `JoinHandle` de la tâche — le caller doit `await` la
    /// `JoinHandle` après `drop` du writer pour garantir le flush
    /// final.
    pub fn start(path: PathBuf) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::unbounded_channel::<NavEvent>();
        let join = tokio::spawn(writer_loop(path, rx));
        (Self { tx }, join)
    }

    /// Fire-and-forget : ne bloque jamais l'appelant. Si le canal
    /// est saturé (jamais attendu en pratique) ou fermé (writer
    /// terminé), l'événement est silencieusement perdu.
    pub fn append(&self, evt: NavEvent) {
        // `send` ne bloque pas (unbounded). Une erreur indique un
        // canal fermé : la tâche écrivain s'est arrêtée
        // (probablement erreur d'ouverture au démarrage). On log
        // au niveau debug, pas warn, car c'est attendu en cas
        // d'erreur d'I/O au boot.
        if self.tx.send(evt).is_err() {
            tracing::debug!("EventWriter: canal fermé, événement perdu");
        }
    }
}

async fn writer_loop(path: PathBuf, mut rx: mpsc::UnboundedReceiver<NavEvent>) {
    let file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, path = %path.display(), "EventWriter: ouverture impossible");
            // Drain silencieux des events reçus pour libérer le canal
            // sans bloquer les appelants. On retourne ensuite.
            while rx.recv().await.is_some() {}
            return;
        }
    };
    let mut bw = tokio::io::BufWriter::with_capacity(8 * 1024, file);

    let mut since_flush: usize = 0;
    loop {
        match tokio::time::timeout(EVENT_WRITER_FLUSH_IDLE, rx.recv()).await {
            Ok(Some(evt)) => {
                let mut line = match serde_json::to_vec(&evt) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "EventWriter: sérialisation échouée");
                        continue;
                    }
                };
                line.push(b'\n');
                if let Err(e) = bw.write_all(&line).await {
                    tracing::warn!(error = %e, "EventWriter: write_all échoué");
                    continue;
                }
                since_flush += 1;
                if since_flush >= EVENT_WRITER_FLUSH_LINES {
                    if let Err(e) = bw.flush().await {
                        tracing::warn!(error = %e, "EventWriter: flush périodique échoué");
                    }
                    since_flush = 0;
                }
            }
            Ok(None) => {
                // Canal fermé : flush final + sortie.
                if let Err(e) = bw.flush().await {
                    tracing::warn!(error = %e, "EventWriter: flush final échoué");
                }
                break;
            }
            Err(_timeout) => {
                // Idle 500 ms atteint : flush si quelque chose est
                // en attente.
                if since_flush > 0 {
                    if let Err(e) = bw.flush().await {
                        tracing::warn!(error = %e, "EventWriter: flush idle échoué");
                    }
                    since_flush = 0;
                }
            }
        }
    }
    // `bw` (BufWriter) est droppé ici, ce qui ferme le fichier
    // proprement (flush résiduel + close).
}

/// Handler axum : upgrade la requête HTTP en WebSocket et délègue à [`handle_socket`].
/// Récupère l'état partagé (vigil + canal extension) via `Extension`.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    Extension(ctx): Extension<VigilCtx>,
    Extension(channel): Extension<ExtensionChannel>,
    Extension(auth): Extension<AuthToken>,
) -> Response {
    // Auth par token en query string. Un token vide désactive la
    // vérification (tests v1) ; en production `router_with_v2` injecte
    // toujours le token réel.
    if !auth.0.is_empty() {
        let presented = q.token.as_deref().unwrap_or("");
        if !ct_eq(auth.0.as_bytes(), presented.as_bytes()) {
            tracing::warn!("/ws : token absent ou invalide");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
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
            drop(handle);
            let _ = tokio::time::timeout(Duration::from_secs(2), bridge_task).await;
            drop(local_tx);
            let _ = tokio::time::timeout(Duration::from_secs(2), pump_task).await;
            return;
        }
    };
    // EventWriter : bufferise l'écriture JSONL (D1 — Lot A 2026-10-07).
    // Tâche écrivain propriétaire du fichier ouvert en append + un
    // BufWriter. `append` est fire-and-forget. Le JoinHandle est
    // attendu à la sortie de `handle_socket` pour garantir le
    // flush final (sender droppée → canal fermé → writer fait
    // `Ok(None)` → flush final + exit).
    let (event_writer, writer_task) = EventWriter::start(events_path);
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

                // Handshake de version/capacités envoyé par l'extension
                // à l'ouverture de la WS.
                if let Ok(HelloEnvelope { hello }) = serde_json::from_str::<HelloEnvelope>(&raw) {
                    tracing::info!(
                        client = %hello.client,
                        version = %hello.version,
                        capabilities = ?hello.capabilities,
                        "handshake extension reçu"
                    );
                    channel.set_hello(hello).await;
                    if !send_text(&local_tx, Ack::ok()) {
                        break;
                    }
                    continue;
                }

                // Keep-alive applicatif : réponse immédiate, sans passer
                // par le parse NavEvent (sinon le démon logguait une
                // erreur "parse" toutes les 25 s).
                if let Ok(PingEnvelope { ping }) = serde_json::from_str::<PingEnvelope>(&raw) {
                    let pong = serde_json::json!({ "pong": ping }).to_string();
                    if !send_text(&local_tx, pong) {
                        break;
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
                        // D1 — Lot A : `append` est fire-and-forget.
                        // On ack tout de suite à l'extension (le
                        // contrat historique) ; l'écriture réelle
                        // est différée jusqu'au flush suivant
                        // (10 lignes ou 500 ms idle) ou jusqu'au
                        // flush final à la fermeture de la WS.
                        event_writer.append(evt);
                        if !send_text(&local_tx, Ack::ok()) {
                            break;
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
    // attente), on ferme les deux tasks (bridge + pump) en droppant
    // les senders, et on attend la fin de la tâche écrivain
    // (EventWriter) pour garantir le flush final (0 event perdu).
    channel.unregister(&handle).await;
    // Fermer d'abord le sender de l'extension : le bridge attend
    // `ext_rx`, qui ne se ferme qu'au drop du dernier `SinkHandle`.
    // Sans ce drop, le bridge ne termine jamais et la socket reste
    // en CLOSE-WAIT (fuite observée le 21/10).
    drop(handle);
    let _ = tokio::time::timeout(Duration::from_secs(2), bridge_task).await;
    drop(local_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pump_task).await;
    // `event_writer` est droppé implicitement à la sortie de scope
    // (sender fermée → task écrivain voit `Ok(None)` → flush final).
    // `writer_task` est joinée après le drop.
    drop(event_writer);
    if let Err(e) = writer_task.await {
        tracing::warn!(error = %e, "EventWriter: join final a échoué");
    }
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

/// Rotation de démarrage des journaux JSONL. Un fichier qui dépasse
/// 50 Mo est renommé en `.1`, les archives existantes glissent
/// (`.1`→`.2`, `.2`→`.3`) et la plus ancienne est supprimée.
/// Appelée une fois dans `main` avant l'ouverture des writers.
pub fn rotate_logs_on_startup() {
    const MAX_LOG_BYTES: u64 = 50 * 1024 * 1024;
    const KEEP: usize = 3;
    let home = match default_home() {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "rotation des logs ignorée : home introuvable");
            return;
        }
    };
    for name in ["events.jsonl", "alerts.jsonl", "actions.jsonl"] {
        let path = home.join(name);
        let too_big = std::fs::metadata(&path)
            .map(|m| m.len() > MAX_LOG_BYTES)
            .unwrap_or(false);
        if !too_big {
            continue;
        }
        let oldest = path.with_extension(format!("jsonl.{KEEP}"));
        let _ = std::fs::remove_file(&oldest);
        for i in (1..KEEP).rev() {
            let from = path.with_extension(format!("jsonl.{i}"));
            let to = path.with_extension(format!("jsonl.{}", i + 1));
            if from.exists() {
                let _ = std::fs::rename(&from, &to);
            }
        }
        let archive = path.with_extension("jsonl.1");
        if let Err(e) = std::fs::rename(&path, &archive) {
            tracing::warn!(error = %e, path = %path.display(), "rotation du log échouée");
        } else {
            tracing::info!(path = %path.display(), archive = %archive.display(), "log rotaté (dépassement 50 Mo)");
        }
    }
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
