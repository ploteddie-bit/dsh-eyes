//! Canal bidirectionnel démon ↔ extension Chrome (modèle WebBridge).
//!
//! ## Pourquoi (REWORK de l'opérateur — modèle webbridge)
//!
//! L'extension DSH Eyes, déjà chargée dans le navigateur de l'utilisateur
//! (manifest MV3, service worker `background.js`), pousse des événements
//! de navigation sur `ws://127.0.0.1:8445/ws` (cf. `ingest.rs`). La nouvelle
//! architecture (REWORK 2026-10-05) la rend **bidirectionnelle** : le démon
//! lui envoie des **commandes** (`goto`, `click`, `read`, `fill`,
//! `tabs_create`, …) et l'extension les exécute via `chrome.tabs` et
//! `chrome.scripting.executeScript`, puis renvoie le résultat.
//!
//! La connexion WS existante (`/ws`) est réutilisée : l'extension
//! s'identifie par un message `hello` (cf. `ingest::handle_socket`) et
//! son sink est enregistré dans [`ExtensionChannel`]. Le routeur HTTP v2
//! lit ce canal pour pousser des commandes.
//!
//! ## Concurrence
//!
//! - Plusieurs extensions peuvent se connecter (sur des onglets différents
//!   par ex.). Politique : **la dernière enregistrée gagne**. Les
//!   précédentes voient leur sink remplacé et leurs requêtes en attente
//!   échouent avec `error: "extension_disconnected"`. C'est documenté en
//!   section « Concurrence » plus bas.
//! - Quand la connexion WS se ferme, `ingest::handle_socket` appelle
//!   [`ExtensionChannel::unregister`] pour purger les requêtes en attente.
//!
//! ## Sérialisation
//!
//! - Messages sortants (démon → extension) : JSON
//!   `{"cmd_id": N, "cmd": "goto"|…, "params": {...}}`. Le `cmd_id` est
//!   un `u64` monotonement croissant par connexion (rallocation à 0 si
//!   wraparound, peu probable).
//! - Messages entrants de **commande** (extension → démon) : JSON
//!   `{"cmd_id": N, "ok": bool, "result"?: any, "error"?: string}`.
//!   Le `cmd_id` est utilisé pour router la réponse vers le `oneshot`
//!   de la requête correspondante.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Commande envoyée à l'extension. Sérialisée en JSON avant push sur WS.
/// Forme JSON : `{"cmd_id":N,"cmd":"goto","params":{...}}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub cmd_id: u64,
    pub cmd: String,
    pub params: Value,
}

impl Command {
    /// Sérialise la commande en string JSON prête à être poussée sur
    /// la WS. Échec de sérialisation : ne devrait pas se produire (les
    /// champs sont triviaux) — fallback sur une forme minimale.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| r#"{"cmd_id":0,"cmd":"error","params":{}}"#.to_string())
    }
}

/// Réponse reçue de l'extension. Sérialisée en JSON, reçue via WS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub cmd_id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// Helper : réponse d'erreur uniforme (sert à purger les requêtes
    /// en attente quand l'extension se déconnecte).
    pub fn err(cmd_id: u64, msg: impl Into<String>) -> Self {
        Self {
            cmd_id,
            ok: false,
            result: None,
            error: Some(msg.into()),
        }
    }
}

/// Sink d'écriture vers l'extension — c'est le sender du split WS
/// (`axum::extract::ws::WebSocket` côté serveur). On l'enveloppe en
/// `mpsc::UnboundedSender<String>` où la string est le JSON à pousser
/// tel quel sur la WS. Le canal est agnostique : la forme du JSON est
/// laissée aux callers (Command sortante, Ack sur événement, etc.).
///
/// **Note identité** : `UnboundedSender` ne fournit pas `same_ref`. On
/// stocke donc un `Arc<UnboundedSender<String>>` en interne pour avoir
/// une adresse stable. Les callers peuvent ainsi comparer leur sender
/// (via `Arc::ptr_eq`) à celui enregistré et décider de le remplacer.
/// Informations de handshake envoyées par l'extension à l'ouverture
/// de la WS (`{"hello": {...}}`). Sert à `/v2/info` et au diagnostic
/// de version démon ↔ extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloInfo {
    pub client: String,
    pub version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

pub type ExtensionSink = mpsc::UnboundedSender<String>;

/// Wrapper identité-stable autour d'un `ExtensionSink`. Les clones
/// partagent la même `Arc`, donc `Arc::ptr_eq` est utilisable.
#[derive(Clone)]
pub struct SinkHandle(pub Arc<ExtensionSink>);

impl SinkHandle {
    pub fn new(sink: ExtensionSink) -> Self {
        Self(Arc::new(sink))
    }

    /// Compare deux handles par identité (`Arc::ptr_eq`).
    pub fn same_handle(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Pousse une string JSON sur le sink sous-jacent. Retourne `false`
    /// si le receiver a été droppé (extension déconnectée).
    pub fn send(&self, s: String) -> bool {
        self.0.send(s).is_ok()
    }
}

/// Compteurs par commande (atomics : pas de verrou sur le chemin chaud).
#[derive(Default)]
struct CommandStats {
    total: AtomicU64,
    ok: AtomicU64,
    error: AtomicU64,
    timeout: AtomicU64,
    total_ms: AtomicU64,
}

/// Issue d'une commande pour les métriques.
#[derive(Clone, Copy)]
enum Outcome {
    Ok,
    Error,
    Timeout,
    NoExtension,
}

/// Métriques process-wide du canal extension : volumes, issues, latence.
#[derive(Default)]
struct Metrics {
    total: AtomicU64,
    ok: AtomicU64,
    error: AtomicU64,
    timeout: AtomicU64,
    no_extension: AtomicU64,
    in_flight: AtomicU64,
    total_ms: AtomicU64,
    per_command: StdMutex<HashMap<String, Arc<CommandStats>>>,
}

impl Metrics {
    fn start(&self, cmd: &str) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.stats_for(cmd).total.fetch_add(1, Ordering::Relaxed);
    }

    fn finish(&self, cmd: &str, outcome: Outcome, elapsed: Duration) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let ms = elapsed.as_millis() as u64;
        self.total_ms.fetch_add(ms, Ordering::Relaxed);
        let stats = self.stats_for(cmd);
        stats.total_ms.fetch_add(ms, Ordering::Relaxed);
        match outcome {
            Outcome::Ok => {
                self.ok.fetch_add(1, Ordering::Relaxed);
                stats.ok.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::Error => {
                self.error.fetch_add(1, Ordering::Relaxed);
                stats.error.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::Timeout => {
                self.timeout.fetch_add(1, Ordering::Relaxed);
                stats.timeout.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::NoExtension => {
                self.no_extension.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn stats_for(&self, cmd: &str) -> Arc<CommandStats> {
        let mut map = self.per_command.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(cmd.to_string())
            .or_insert_with(|| Arc::new(CommandStats::default()))
            .clone()
    }

    fn snapshot(&self) -> Value {
        let mut per_command = serde_json::Map::new();
        let map = self.per_command.lock().unwrap_or_else(|e| e.into_inner());
        for (cmd, s) in map.iter() {
            per_command.insert(
                cmd.clone(),
                serde_json::json!({
                    "total": s.total.load(Ordering::Relaxed),
                    "ok": s.ok.load(Ordering::Relaxed),
                    "error": s.error.load(Ordering::Relaxed),
                    "timeout": s.timeout.load(Ordering::Relaxed),
                    "total_ms": s.total_ms.load(Ordering::Relaxed),
                }),
            );
        }
        serde_json::json!({
            "ok": true,
            "commands": {
                "total": self.total.load(Ordering::Relaxed),
                "ok": self.ok.load(Ordering::Relaxed),
                "error": self.error.load(Ordering::Relaxed),
                "timeout": self.timeout.load(Ordering::Relaxed),
                "no_extension": self.no_extension.load(Ordering::Relaxed),
                "in_flight": self.in_flight.load(Ordering::Relaxed),
                "total_ms": self.total_ms.load(Ordering::Relaxed),
                "per_command": per_command,
            }
        })
    }
}

/// Débit maximal de commandes API (token bucket global).
const RATE_PER_SEC: f64 = 2.0; // 120 commandes/min
const RATE_BURST: f64 = 60.0;  // rafale initiale

struct RateState {
    tokens: f64,
    last: Instant,
}

impl Default for RateState {
    fn default() -> Self {
        Self {
            tokens: RATE_BURST,
            last: Instant::now(),
        }
    }
}

/// État partagé entre la boucle WS (`ingest::handle_socket`) et le
/// routeur HTTP v2 (`api::v2_cmd`).
///
/// Architecture :
/// - `sink` : `Mutex<Option<ExtensionSink>>` — None = aucune extension
///   connectée. Remplacé à chaque `register()`. La dernière connexion
///   gagne (cf. section « Concurrence »).
/// - `pending` : `Mutex<HashMap<u64, oneshot::Sender<Response>>>` — les
///   requêtes en attente de leur réponse. Vidée par `unregister()` à la
///   déconnexion (chaque oneshot reçoit une erreur).
/// - `next_id` : `AtomicU64` — compteur monotonement croissant des
///   `cmd_id` sortants (per-canal ; wraparound à 2^64, négligeable).
#[derive(Clone)]
pub struct ExtensionChannel {
    inner: Arc<Inner>,
}

struct Inner {
    sink: Mutex<Option<SinkHandle>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Response>>>,
    next_id: AtomicU64,
    /// Dernier `hello` reçu de l'extension (version, client, capacités).
    hello: Mutex<Option<HelloInfo>>,
    /// Verrous par onglet : deux commandes portant le même `tab_id`
    /// explicite ne se percutent pas côté démon (la sérialisation
    /// existait déjà côté extension, elle est désormais aussi dans le
    /// démon). Les commandes sans `tab_id` restent concurrentes.
    tab_locks: Mutex<HashMap<i64, Arc<Mutex<()>>>>,
    /// Compteurs d'observabilité (volumes, issues, latence).
    metrics: Metrics,
    /// Cache des résultats de batches `act` par `action_id` (TTL 5 min).
    action_cache: StdMutex<HashMap<String, (Value, Instant)>>,
    /// Actions dont l'annulation a été demandée.
    cancelled_actions: StdMutex<HashSet<String>>,
    /// Token bucket global des commandes API.
    rate: StdMutex<RateState>,
}

/// Timeout par défaut d'attente de réponse d'une commande.
pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Résultat de [`ExtensionChannel::send_command`].
pub type CommandResult = Result<Response, CommandError>;

/// Erreurs émises par [`ExtensionChannel::send_command`].
#[derive(Debug)]
pub enum CommandError {
    NoExtension,
    Timeout(Duration),
    ChannelClosed,
    /// L'extension a renvoyé `ok:false` ; on remonte son `error` tel quel.
    ExtensionError(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoExtension => write!(f, "aucune extension connectée"),
            Self::Timeout(d) => write!(f, "timeout après {d:?} en attendant la réponse de l'extension"),
            Self::ChannelClosed => write!(f, "canal oneshot fermé (extension déconnectée)"),
            Self::ExtensionError(m) => write!(f, "extension a renvoyé ok=false: {m}"),
        }
    }
}

impl std::error::Error for CommandError {}

impl ExtensionChannel {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                sink: Mutex::new(None),
                pending: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                hello: Mutex::new(None),
                tab_locks: Mutex::new(HashMap::new()),
                metrics: Metrics::default(),
                action_cache: StdMutex::new(HashMap::new()),
                cancelled_actions: StdMutex::new(HashSet::new()),
                rate: StdMutex::new(RateState::default()),
            }),
        }
    }

    /// Enregistre le sink d'écriture de l'extension. Remplace un éventuel
    /// sink précédent (la dernière connexion gagne). Vidée d'office à
    /// chaque appel : les requêtes en attente du précédent sink sont
    /// marquées en erreur (les oneshots reçoivent `err`).
    ///
    /// Prend un `SinkHandle` par valeur (pas un `ExtensionSink` brut)
    /// pour préserver l'identité — ainsi `unregister(handle)` peut
    /// comparer par `Arc::ptr_eq` et ne purger que les requêtes du
    /// bon sink.
    pub async fn register(&self, handle: SinkHandle) {
        // Purge des requêtes en attente de l'ancien sink.
        Self::fail_all_pending(&self.inner.pending, "extension_disconnected").await;
        // Un nouveau sink n'a pas encore envoyé son hello : on efface
        // celui de la connexion précédente pour que `/v2/info` ne
        // présente pas une version périmée.
        *self.inner.hello.lock().await = None;
        let mut g = self.inner.sink.lock().await;
        *g = Some(handle);
    }

    /// Désenregistre le sink (appelé par `ingest::handle_socket` quand
    /// la WS se ferme). Vidée des requêtes en attente. `handle` est le
    /// `SinkHandle` de la connexion qui se ferme ; on ne dépose que si
    /// c'est bien ce handle qui est enregistré (sinon une autre
    /// connexion l'a déjà remplacé).
    pub async fn unregister(&self, handle: &SinkHandle) {
        let mut g = self.inner.sink.lock().await;
        if g.as_ref().is_some_and(|s| s.same_handle(handle)) {
            *g = None;
            drop(g);
            *self.inner.hello.lock().await = None;
            Self::fail_all_pending(&self.inner.pending, "extension_disconnected").await;
        }
    }

    /// Indique si une extension est actuellement enregistrée.
    #[allow(dead_code)] // API publique — utilisée par les tests, pas le binaire
    pub async fn is_connected(&self) -> bool {
        self.inner.sink.lock().await.is_some()
    }

    /// Enregistre le dernier `hello` reçu de l'extension.
    pub async fn set_hello(&self, hello: HelloInfo) {
        *self.inner.hello.lock().await = Some(hello);
    }

    /// Retourne le dernier `hello` reçu, s'il y en a un.
    pub async fn hello(&self) -> Option<HelloInfo> {
        self.inner.hello.lock().await.clone()
    }

    /// Snapshot JSON des métriques du canal (volumes, issues, latence
    /// globale et par commande).
    pub fn metrics_snapshot(&self) -> Value {
        self.inner.metrics.snapshot()
    }

    /// Token bucket global des commandes API : 120/min, rafale 60.
    /// Retourne `false` si le quota est épuisé (l'appelant renvoie 429).
    pub fn check_rate_limit(&self) -> bool {
        let mut r = self.inner.rate.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed = now.duration_since(r.last).as_secs_f64();
        r.last = now;
        r.tokens = (r.tokens + elapsed * RATE_PER_SEC).min(RATE_BURST);
        if r.tokens >= 1.0 {
            r.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Retourne le résultat d'un batch `act` déjà exécuté (TTL 5 min).
    /// Le cache ne contient que les succès : une erreur ou un timeout
    /// n'est pas mémorisé, pour permettre un retry après incident
    /// transitoire.
    pub fn cached_action(&self, id: &str) -> Option<Value> {
        let mut cache = self.inner.action_cache.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        cache.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(300));
        cache.get(id).map(|(v, _)| v.clone())
    }

    /// Mémorise le résultat d'un batch `act` réussi.
    pub fn cache_action(&self, id: &str, value: Value) {
        let mut cache = self.inner.action_cache.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        cache.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(300));
        if cache.len() >= 1000 {
            cache.clear();
        }
        cache.insert(id.to_string(), (value, now));
    }

    /// Demande l'annulation d'un batch `act`. L'annulation est
    /// coopérative : le handler vérifie le drapeau entre deux étapes.
    pub fn cancel_action(&self, id: &str) {
        let mut set = self.inner.cancelled_actions.lock().unwrap_or_else(|e| e.into_inner());
        if set.len() >= 1000 {
            set.clear();
        }
        set.insert(id.to_string());
    }

    pub fn is_action_cancelled(&self, id: &str) -> bool {
        self.inner
            .cancelled_actions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(id)
    }

    pub fn clear_action_cancel(&self, id: &str) {
        self.inner
            .cancelled_actions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
    }

    /// Retourne le verrou associé à un `tab_id` explicite. La map grandit
    /// d'une entrée par onglet réellement utilisé ; le nombre d'onglets
    /// est borné par l'usage. Les commandes sans `tab_id` n'utilisent
    /// pas ce verrou et restent concurrentes.
    async fn tab_lock(&self, tab_id: i64) -> Arc<Mutex<()>> {
        let mut locks = self.inner.tab_locks.lock().await;
        locks
            .entry(tab_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Pousse une commande vers l'extension et attend la réponse, avec
    /// timeout [`DEFAULT_COMMAND_TIMEOUT`].
    ///
    /// Flux :
    /// 1. Alloue un `cmd_id` (atomique).
    /// 2. Crée un `oneshot`, l'enregistre dans `pending`.
    /// 3. Pousse la `Command` dans le sink. Si le push échoue
    ///    (déconnexion entre l'enregistrement et le send), on retire
    ///    le oneshot et on remonte `NoExtension`.
    /// 4. Attend le oneshot avec `tokio::time::timeout`.
    /// 5. Sur réception : si `ok=false` on remappe en `ExtensionError`,
    ///    sinon on retourne la `Response` telle quelle.
    pub async fn send_command(
        &self,
        cmd: impl Into<String>,
        params: Value,
    ) -> CommandResult {
        self.send_command_with_timeout(cmd, params, DEFAULT_COMMAND_TIMEOUT).await
    }

    /// Variante avec timeout configurable (utile aux tests).
    pub async fn send_command_with_timeout(
        &self,
        cmd: impl Into<String>,
        params: Value,
        timeout: Duration,
    ) -> CommandResult {
        let cmd = cmd.into();
        self.inner.metrics.start(&cmd);
        let started = Instant::now();
        let result = self.send_command_locked(cmd.clone(), params, timeout).await;
        let outcome = match &result {
            Ok(_) => Outcome::Ok,
            Err(CommandError::NoExtension) => Outcome::NoExtension,
            Err(CommandError::Timeout(_)) => Outcome::Timeout,
            Err(CommandError::ChannelClosed | CommandError::ExtensionError(_)) => Outcome::Error,
        };
        self.inner.metrics.finish(&cmd, outcome, started.elapsed());
        result
    }

    async fn send_command_locked(
        &self,
        cmd: String,
        params: Value,
        timeout: Duration,
    ) -> CommandResult {
        // Sérialisation par onglet quand `tab_id` est explicite. Le
        // verrou est tenu pendant tout l'aller-retour (push + attente).
        let tab_lock = match params.get("tab_id").and_then(|v| v.as_i64()) {
            Some(id) => Some(self.tab_lock(id).await),
            None => None,
        };
        let _tab_guard = match &tab_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        let cmd_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let command = Command { cmd_id, cmd, params };

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().await;
            pending.insert(cmd_id, tx);
        }

        // Push sur le sink. Si le sink n'existe plus (déconnexion
        // entre `is_connected` et ici), on retire le oneshot et on
        // remonte l'erreur.
        let push_ok = {
            let g = self.inner.sink.lock().await;
            match g.as_ref() {
                Some(s) => s.send(command.to_json()),
                None => false,
            }
        };

        if !push_ok {
            // On retire le oneshot pour ne pas le laisser pendouiller.
            let mut pending = self.inner.pending.lock().await;
            pending.remove(&cmd_id);
            return Err(CommandError::NoExtension);
        }

        // Attente réponse ou timeout.
        match tokio::time::timeout(timeout, rx).await {
            Err(_) => {
                // Timeout : on retire le oneshot et on remonte l'erreur.
                let mut pending = self.inner.pending.lock().await;
                pending.remove(&cmd_id);
                Err(CommandError::Timeout(timeout))
            }
            Ok(Err(_)) => {
                // Le sender a été droppé (via `fail_all_pending` /
                // `unregister`). On retire au cas où.
                let mut pending = self.inner.pending.lock().await;
                pending.remove(&cmd_id);
                Err(CommandError::ChannelClosed)
            }
            Ok(Ok(resp)) => {
                if resp.ok {
                    Ok(resp)
                } else {
                    // ok:false sans message : erreur de protocole extension,
                    // pas une erreur métier — nommée explicitement plutôt
                    // que masquée par un fallback générique.
                    match resp.error {
                        Some(err) => Err(CommandError::ExtensionError(err)),
                        None => Err(CommandError::ExtensionError(
                            "extension_protocol_error: ok=false sans message".into(),
                        )),
                    }
                }
            }
        }
    }

    /// Dispatche une réponse reçue de l'extension vers le oneshot
    /// correspondant. Retourne `true` si un oneshot a été trouvé (donc
    /// routé), `false` sinon (réponse orpheline — log).
    pub async fn dispatch_response(&self, resp: Response) -> bool {
        let mut pending = self.inner.pending.lock().await;
        if let Some(tx) = pending.remove(&resp.cmd_id) {
            // Si le receiver a été droppé (timeout entre-temps), `send`
            // renvoie Err. Pas critique : on a déjà nettoyé la map.
            let _ = tx.send(resp);
            true
        } else {
            false
        }
    }

    /// Purge toutes les requêtes en attente en leur envoyant une erreur.
    /// Chaque oneshot reçoit `err(cmd_id, msg)`.
    async fn fail_all_pending(
        pending: &Mutex<HashMap<u64, oneshot::Sender<Response>>>,
        msg: &str,
    ) {
        let mut g = pending.lock().await;
        let drained: Vec<(u64, oneshot::Sender<Response>)> = g.drain().collect();
        drop(g);
        for (cmd_id, tx) in drained {
            let _ = tx.send(Response::err(cmd_id, msg));
        }
    }
}

impl Default for ExtensionChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AO};
    use tokio::time::sleep;

    /// Helper : crée un `ExtensionSink` + un handle pour récupérer les
    /// `String` JSON poussées par le canal (simulation d'extension).
    fn make_sink() -> (ExtensionSink, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, rx)
    }

    /// Parse une string JSON en `serde_json::Value` pour les assertions.
    fn parse(s: &str) -> Value {
        serde_json::from_str(s).unwrap_or_else(|e| panic!("json invalide: {e} dans {s}"))
    }

    #[tokio::test]
    async fn neuf_pas_connecte_send_command_retourne_no_extension() {
        let ch = ExtensionChannel::new();
        assert!(!ch.is_connected().await);
        let err = ch
            .send_command("goto", serde_json::json!({"url": "https://example.com"}))
            .await
            .unwrap_err();
        assert!(matches!(err, CommandError::NoExtension));
    }

    #[tokio::test]
    async fn register_puis_send_puis_dispatch_repond_ok() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        let handle = SinkHandle::new(sink);
        ch.register(handle).await;
        assert!(ch.is_connected().await);

        // On lance send_command en tâche de fond (elle va bloquer sur
        // le oneshot).
        let ch2 = ch.clone();
        let handle = tokio::spawn(async move {
            ch2.send_command("goto", serde_json::json!({"url": "https://example.com"})).await
        });

        // Le sink doit recevoir le JSON de la Command.
        let env_raw = sink_rx.recv().await.expect("sink doit avoir reçu la cmd");
        let env = parse(&env_raw);
        assert_eq!(env["cmd"], "goto");
        assert_eq!(env["params"]["url"], "https://example.com");
        let cmd_id = env["cmd_id"].as_u64().expect("cmd_id manquant");

        // On dispatche la « vraie » réponse (ok=true, result=...).
        ch.dispatch_response(Response {
            cmd_id,
            ok: true,
            result: Some(serde_json::json!({"page_id": "p1"})),
            error: None,
        })
        .await;

        // send_command doit retourner Ok(Response) avec result={"page_id":"p1"}.
        let resp = handle.await.expect("join").expect("Ok");
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap()["page_id"], "p1");
    }

    #[tokio::test]
    async fn dispatch_response_orpheline_ne_pan_pas() {
        let ch = ExtensionChannel::new();
        // Aucune commande en attente, on dispatche une réponse au hasard.
        let routed = ch
            .dispatch_response(Response {
                cmd_id: 9999,
                ok: true,
                result: None,
                error: None,
            })
            .await;
        assert!(!routed, "aucun oneshot en attente, doit retourner false");
    }

    #[tokio::test]
    async fn timeout_si_extension_ne_repond_pas() {
        let ch = ExtensionChannel::new();
        let (sink, _sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let ch2 = ch.clone();
        let res = ch2
            .send_command_with_timeout(
                "read",
                serde_json::json!({}),
                Duration::from_millis(50),
            )
            .await;
        assert!(matches!(res, Err(CommandError::Timeout(_))));
    }

    #[tokio::test]
    async fn unregister_purge_les_pending_avec_erreur() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        let handle = SinkHandle::new(sink);
        ch.register(handle.clone()).await;

        // Lance deux send_command ; on ne dispatche aucune réponse.
        let ch2 = ch.clone();
        let h1 = tokio::spawn(async move {
            ch2.send_command_with_timeout("a", serde_json::json!({}), Duration::from_secs(2)).await
        });
        // Récupère la cmd_id du 1er push pour vérifier le 2e incrémente.
        let env1_raw = sink_rx.recv().await.expect("cmd 1 pushed");
        let id1 = parse(&env1_raw)["cmd_id"].as_u64().unwrap();

        let ch3 = ch.clone();
        let h2 = tokio::spawn(async move {
            ch3.send_command_with_timeout("b", serde_json::json!({}), Duration::from_secs(2)).await
        });
        let env2_raw = sink_rx.recv().await.expect("cmd 2 pushed");
        let id2 = parse(&env2_raw)["cmd_id"].as_u64().unwrap();

        // Maintenant on désenregistre.
        ch.unregister(&handle).await;
        assert!(!ch.is_connected().await);

        // Les deux send_command doivent avoir reçu l'erreur.
        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();
        assert!(matches!(r1, Err(CommandError::ExtensionError(_))));
        assert!(matches!(r2, Err(CommandError::ExtensionError(_))));
        // Les cmd_id doivent être distincts (0 puis 1).
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn register_remplace_ancien_sink_et_purge_pending() {
        let ch = ExtensionChannel::new();
        let (sink1, mut rx1) = make_sink();
        ch.register(SinkHandle::new(sink1)).await;

        // Commande en attente sur sink1.
        let ch2 = ch.clone();
        let h1 = tokio::spawn(async move {
            ch2.send_command_with_timeout("a", serde_json::json!({}), Duration::from_secs(2)).await
        });
        // sink1 reçoit la commande.
        let _env1 = rx1.recv().await.expect("sink1 reçoit cmd");

        // sink2 prend la place. Le pending de sink1 doit être purgé.
        let (sink2, _rx2) = make_sink();
        ch.register(SinkHandle::new(sink2)).await;
        let r1 = h1.await.unwrap();
        assert!(matches!(r1, Err(CommandError::ExtensionError(_))));
    }

    #[tokio::test]
    async fn unregister_ne_tue_pas_les_pending_si_sink_a_change() {
        // Si on déconnecte sink1 mais que sink2 a pris sa place, on ne
        // doit pas purger les requêtes de sink2.
        let ch = ExtensionChannel::new();
        let (sink1, mut _rx1) = make_sink();
        let h1_handle = SinkHandle::new(sink1);
        ch.register(h1_handle.clone()).await;
        let (sink2, mut _rx2) = make_sink();
        let h2_handle = SinkHandle::new(sink2);
        ch.register(h2_handle.clone()).await; // sink2 remplace sink1

        // Lance une commande (sur sink2).
        let ch2 = ch.clone();
        let h = tokio::spawn(async move {
            ch2.send_command_with_timeout("a", serde_json::json!({}), Duration::from_secs(2)).await
        });
        // sink2 reçoit la commande.
        let env_raw = _rx2.recv().await.expect("sink2 reçoit cmd");
        let cmd_id = parse(&env_raw)["cmd_id"].as_u64().unwrap();

        // Déconnexion de sink1 (qui n'est plus enregistré) → no-op.
        ch.unregister(&h1_handle).await;

        // sink2 répond → succès.
        ch.dispatch_response(Response {
            cmd_id,
            ok: true,
            result: Some(serde_json::json!({"ok": true})),
            error: None,
        })
        .await;
        let r = h.await.unwrap();
        assert!(r.is_ok());

        // Et unregister du bon sink purge bien.
        ch.unregister(&h2_handle).await;
        assert!(!ch.is_connected().await);
    }

    #[tokio::test]
    async fn extension_renvoie_erreur_remappee_en_extension_error() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let ch2 = ch.clone();
        let h = tokio::spawn(async move {
            ch2.send_command("click", serde_json::json!({"selector": "#x"})).await
        });
        let env_raw = sink_rx.recv().await.expect("cmd pushed");
        let cmd_id = parse(&env_raw)["cmd_id"].as_u64().unwrap();
        ch.dispatch_response(Response {
            cmd_id,
            ok: false,
            result: None,
            error: Some("element introuvable".into()),
        })
        .await;
        let r = h.await.unwrap();
        match r {
            Err(CommandError::ExtensionError(msg)) => assert_eq!(msg, "element introuvable"),
            other => panic!("attendu ExtensionError, reçu {other:?}"),
        }
    }

    #[tokio::test]
    async fn compteur_cmd_id_monotone() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;
        let mut ids = Vec::new();
        for i in 0..5 {
            let ch2 = ch.clone();
            let h = tokio::spawn(async move {
                ch2.send_command_with_timeout("noop", serde_json::json!(i), Duration::from_millis(50)).await
            });
            let env_raw = sink_rx.recv().await.expect("pushed");
            let cmd_id = parse(&env_raw)["cmd_id"].as_u64().unwrap();
            ids.push(cmd_id);
            ch.dispatch_response(Response {
                cmd_id,
                ok: true,
                result: Some(serde_json::json!({})),
                error: None,
            })
            .await;
            let _ = h.await.unwrap();
        }
        // Strictement croissant.
        for w in ids.windows(2) {
            assert!(w[0] < w[1], "ids pas croissants: {ids:?}");
        }
    }

    // Smoke test : 100 push parallèles + 100 dispatches, pas de panic,
    // pas de fuite. On vérifie juste que tous les push sont routés
    // (via un compteur atomique sur le sink).
    #[tokio::test]
    async fn charge_push_dispatch_100_requetes() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let counter = Arc::new(AtomicUsize::new(0));
        // Drainer les push pour les compter (sans les traiter).
        let c2 = counter.clone();
        let drainer = tokio::spawn(async move {
            while let Some(env) = sink_rx.recv().await {
                c2.fetch_add(1, AO::SeqCst);
                // Dispatch auto-ok.
                let _ = env; // env sera routé manuellement plus bas
            }
        });

        // Lance 100 requêtes.
        let mut handles = Vec::new();
        for _ in 0..100 {
            let ch2 = ch.clone();
            handles.push(tokio::spawn(async move {
                ch2.send_command_with_timeout("noop", serde_json::json!({}), Duration::from_millis(200)).await
            }));
        }
        // Laisse un tick pour que tous les push soient faits.
        sleep(Duration::from_millis(20)).await;
        // Vérifie qu'on a bien 100 push.
        assert_eq!(counter.load(AO::SeqCst), 100);
        // Pas de réponse dispatchée → tous doivent timeout (ou être
        // purgés par unregister qu'on n'a pas fait). On abort le drainer
        // et on unregister pour cleanup.
        drainer.abort();
        // Récupère le sink actuel via unregister factice (clone nécessaire).
        // En pratique on drop le sink : `unregister` prend &ExtensionSink.
        // On a stocké `sink` dans la closure du register — pour le test,
        // on n'en a plus la référence ici, donc on ne peut pas unregister.
        // Mais les handles en timeout seront nettoyés par le runtime.
        for h in handles {
            let _ = h.await; // ignore Err(Timeout)
        }
    }

    #[tokio::test]
    async fn commandes_meme_onglet_serialisees() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let ch1 = ch.clone();
        let h1 = tokio::spawn(async move {
            ch1.send_command_with_timeout(
                "read",
                serde_json::json!({"tab_id": 7}),
                Duration::from_secs(2),
            )
            .await
        });
        let ch2 = ch.clone();
        let h2 = tokio::spawn(async move {
            ch2.send_command_with_timeout(
                "read",
                serde_json::json!({"tab_id": 7}),
                Duration::from_secs(2),
            )
            .await
        });

        // La première commande part ; la seconde est retenue par le verrou.
        let env1 = sink_rx.recv().await.expect("cmd 1");
        let id1 = parse(&env1)["cmd_id"].as_u64().expect("cmd_id 1");
        assert!(
            tokio::time::timeout(Duration::from_millis(80), sink_rx.recv())
                .await
                .is_err(),
            "la seconde commande du même onglet ne doit pas partir avant la réponse"
        );

        ch.dispatch_response(Response {
            cmd_id: id1,
            ok: true,
            result: Some(serde_json::json!({"text": "ok"})),
            error: None,
        })
        .await;
        let _ = h1.await.expect("join 1").expect("réponse 1");

        let env2 = sink_rx.recv().await.expect("cmd 2");
        let id2 = parse(&env2)["cmd_id"].as_u64().expect("cmd_id 2");
        ch.dispatch_response(Response {
            cmd_id: id2,
            ok: true,
            result: Some(serde_json::json!({"text": "ok"})),
            error: None,
        })
        .await;
        let _ = h2.await.expect("join 2").expect("réponse 2");
    }

    #[tokio::test]
    async fn commandes_onglets_differents_concurrentes() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let ch1 = ch.clone();
        let h1 = tokio::spawn(async move {
            ch1.send_command_with_timeout(
                "read",
                serde_json::json!({"tab_id": 1}),
                Duration::from_secs(2),
            )
            .await
        });
        let ch2 = ch.clone();
        let h2 = tokio::spawn(async move {
            ch2.send_command_with_timeout(
                "read",
                serde_json::json!({"tab_id": 2}),
                Duration::from_secs(2),
            )
            .await
        });

        // Les deux onglets sont différents : les deux commandes partent
        // sans attendre la réponse de l'autre.
        let env1 = sink_rx.recv().await.expect("cmd tab 1");
        let env2 = sink_rx.recv().await.expect("cmd tab 2");
        let id1 = parse(&env1)["cmd_id"].as_u64().expect("cmd_id 1");
        let id2 = parse(&env2)["cmd_id"].as_u64().expect("cmd_id 2");
        ch.dispatch_response(Response {
            cmd_id: id1,
            ok: true,
            result: Some(serde_json::json!({})),
            error: None,
        })
        .await;
        ch.dispatch_response(Response {
            cmd_id: id2,
            ok: true,
            result: Some(serde_json::json!({})),
            error: None,
        })
        .await;
        let _ = h1.await.expect("join 1").expect("réponse 1");
        let _ = h2.await.expect("join 2").expect("réponse 2");
    }

    #[tokio::test]
    async fn metrics_comptent_ok_et_timeout() {
        let ch = ExtensionChannel::new();
        let (sink, mut sink_rx) = make_sink();
        ch.register(SinkHandle::new(sink)).await;

        let ch2 = ch.clone();
        let h = tokio::spawn(async move {
            ch2.send_command_with_timeout(
                "goto",
                serde_json::json!({"tab_id": 1}),
                Duration::from_secs(2),
            )
            .await
        });
        let env = sink_rx.recv().await.expect("cmd goto");
        let id = parse(&env)["cmd_id"].as_u64().expect("cmd_id");
        ch.dispatch_response(Response {
            cmd_id: id,
            ok: true,
            result: Some(serde_json::json!({})),
            error: None,
        })
        .await;
        let _ = h.await.expect("join").expect("réponse");

        let _ = ch
            .send_command_with_timeout("read", serde_json::json!({}), Duration::from_millis(30))
            .await;

        let snap = ch.metrics_snapshot();
        assert_eq!(snap["commands"]["total"], 2);
        assert_eq!(snap["commands"]["ok"], 1);
        assert_eq!(snap["commands"]["timeout"], 1);
        assert_eq!(snap["commands"]["in_flight"], 0);
        assert_eq!(snap["commands"]["per_command"]["goto"]["ok"], 1);
        assert_eq!(snap["commands"]["per_command"]["read"]["timeout"], 1);
    }

    #[test]
    fn cache_et_annulation_actions() {
        let ch = ExtensionChannel::new();
        assert!(ch.cached_action("a1").is_none());
        ch.cache_action("a1", serde_json::json!({"results": [1, 2]}));
        assert_eq!(ch.cached_action("a1").unwrap()["results"][1], 2);
        assert!(!ch.is_action_cancelled("a1"));
        ch.cancel_action("a1");
        assert!(ch.is_action_cancelled("a1"));
        ch.clear_action_cancel("a1");
        assert!(!ch.is_action_cancelled("a1"));
    }

    #[test]
    fn rate_limit_burst_et_refill() {
        let ch = ExtensionChannel::new();
        for _ in 0..(RATE_BURST as usize) {
            assert!(ch.check_rate_limit());
        }
        assert!(!ch.check_rate_limit());
        std::thread::sleep(Duration::from_millis(600));
        assert!(ch.check_rate_limit());
    }
}
