//! Vigil — moteur de règles d'alerte sur les `NavEvent` (Task 4).
//!
//! Trois règles v1 :
//! 1. `localhost_access` (critical) : `127.0.0.0/8`, `::1`, et le nom `localhost`.
//! 2. `private_network` (warning) : `10.0.0.0/8`, `192.168.0.0/16`, `172.16.0.0/12`.
//! 3. `new_domain` (info) : première occurrence d'un domaine dans la session
//!    qui n'est pas dans l'allowlist (`~/.dsh-eyes/allowlist.json`).
//!
//! Architecture :
//! - `vigil::check(&NavEvent, &Allowlist, &SessionState) -> Option<Alert>`
//!   est **pur** (pas d'I/O disque) — les dépendances externes sont injectées.
//!   L'état de session est un `Arc<Mutex<HashSet<String>>>` pour permettre
//!   le partage entre `handle_socket` (qui mute) et `/events` (qui lit).
//! - L'allowlist est chargée au démarrage (lazy) et passée aux checks.
//!   Depuis Lot C (D4), elle supporte le **hot-reload par mtime** : voir
//!   `Allowlist::reload_if_changed` et `Allowlist::spawn_reload_task`.
//! - Alertes sérialisées `{"level":"...","rule":"...","detail":...}`.
//! - Les alertes `critical` sont aussi écrites dans
//!   `${DSH_EYES_HOME}/alerts.jsonl` par `ingest::handle_socket` (côté I/O).
//!
//! Choix de parsing d'IP : `std::net::IpAddr::from_str` — pas de dépendance
//! lourde. Les plages RFC1918/loopback sont testées sur les octets (`octets()`
//! pour v4, comparaison `Segment` pour v6 — voir `is_loopback_v6`).

use serde::Serialize;
use std::{
    collections::HashSet,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;

use crate::ingest::NavEvent;

/// Sévérité d'une alerte. Sérialisée en snake_case minuscule dans le JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertLevel {
    Critical,
    Warning,
    Info,
}

/// Règle qui a déclenché l'alerte. Sérialisée en snake_case dans le JSON
/// (`"localhost_access"`, `"private_network"`, `"new_domain"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertRule {
    LocalhostAccess,
    PrivateNetwork,
    NewDomain,
}

/// Alerte émise par le vigil. Le `detail` est un texte court (le `domain`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Alert {
    pub level: AlertLevel,
    pub rule: AlertRule,
    pub detail: String,
}

/// Allowlist des domaines connus (jamais signalés `new_domain`).
///
/// Wrapper `Clone + Default` autour d'un `Arc<Mutex<AllowlistCore>>` :
/// - `Clone` partage le même état interne (mêmes domaines, même mtime).
/// - `Default` produit une allowlist vide **sans chemin** (utilisée par les
///   tests et par les fallbacks `Self::empty()`).
/// - Les lectures (`contains`) prennent un lock court — la contention est
///   nulle en pratique (les checks sont <µs, pas d'`await` dans le lock).
///
/// **Lot C (D4) — hot-reload par mtime** : au chargement (`load_or_default`),
/// on mémorise le `mtime` du fichier + son chemin. `reload_if_changed` peut
/// ensuite comparer le mtime courant au mtime mémorisé :
/// - Fichier modifié (mtime ultérieur) → recharger (re-parse + remplace).
/// - Fichier apparu (mtime mémorisé = None, fichier présent) → charger.
/// - Fichier disparu (mtime mémorisé = Some, fichier absent) → vider.
/// - Erreur de lecture → `tracing::warn!` + état inchangé (jamais de panic).
/// - Pas de modification (mtime identique) → no-op silencieux.
#[derive(Debug, Clone, Default)]
pub struct Allowlist {
    inner: Arc<StdMutex<AllowlistCore>>,
}

/// État interne de l'`Allowlist`. Sous un seul `std::sync::Mutex` parce que
/// la section critique est minuscule (un `HashSet::contains` ou un clone
/// d'un petit set) — pas la peine d'introduire un `RwLock` ou de séparer
/// en deux `Arc`.
#[derive(Debug, Default)]
struct AllowlistCore {
    domains: HashSet<String>,
    /// `mtime` du fichier observée lors du dernier chargement. `None` si
    /// aucun fichier n'a encore été lu (allowlist vide au démarrage,
    /// fichier absent ou `from_vec` synthétique).
    last_known_mtime: Option<SystemTime>,
    /// Chemin résolu du fichier `allowlist.json`. `None` pour les
    /// `Allowlist` synthétiques (`empty`, `from_vec`) qui n'ont pas de
    /// backing store — `reload_if_changed` est alors un no-op.
    path: Option<PathBuf>,
}

impl Allowlist {
    /// Allowlist vide (tous les domaines sont « nouveaux »).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construit une allowlist à partir d'un `Vec<String>`.
    /// Utilisé par les tests et par le chargement initial. Le `path` est
    /// `None` : ces allowlists ne sont **pas** hot-reloadables (no-op).
    pub fn from_vec(domains: Vec<String>) -> Self {
        let core = AllowlistCore {
            domains: domains.into_iter().collect(),
            last_known_mtime: None,
            path: None,
        };
        Self {
            inner: Arc::new(Mutex::new(core)),
        }
    }

    /// Charge l'allowlist depuis `~/.dsh-eyes/allowlist.json` et **mémorise
    /// le mtime du fichier** pour le hot-reload ultérieur.
    ///
    /// Format attendu : `{"domains": ["github.com", "wikipedia.org", ...]}`.
    /// Fichier absent → allowlist vide (sémantique : aucun domaine connu,
    /// tout passe par `new_domain`). Fichier présent mais malformé → allowlist
    /// vide + `tracing::warn!` (dégradation gracieuse, on n'arrête pas le daemon).
    pub fn load_or_default() -> Self {
        let path = match allowlist_path() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "impossible de calculer le chemin d'allowlist");
                return Self::empty();
            }
        };
        if !path.exists() {
            // Fichier absent : on garde le `path` en mémoire pour que
            // `reload_if_changed` puisse détecter l'apparition ultérieure
            // du fichier et le charger.
            return Self::from_path(path, None, HashSet::new());
        }
        let mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .ok();
        match std::fs::read_to_string(&path) {
            Ok(raw) => match serde_json::from_str::<AllowlistFile>(&raw) {
                Ok(f) => Self::from_path(path, mtime, f.domains.into_iter().collect()),
                Err(e) => {
                    tracing::warn!(error = %e, path = %path.display(),
                        "allowlist.json malformé ; allowlist vide appliquée");
                    Self::from_path(path, None, HashSet::new())
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "lecture allowlist.json impossible ; allowlist vide appliquée");
                Self::from_path(path, None, HashSet::new())
            }
        }
    }

    /// Helper interne : construit une `Allowlist` à partir d'un chemin,
    /// d'un mtime optionnel et d'un set de domaines déjà parsé.
    fn from_path(path: PathBuf, mtime: Option<SystemTime>, domains: HashSet<String>) -> Self {
        Self {
            inner: Arc::new(StdMutex::new(AllowlistCore {
                domains,
                last_known_mtime: mtime,
                path: Some(path),
            })),
        }
    }

    /// `true` si `domain` est dans l'allowlist.
    pub fn contains(&self, domain: &str) -> bool {
        // Lock court : `HashSet::contains` est O(1) et ne fait pas d'I/O.
        // On n'utilise jamais `await` sous le lock — la contention est nulle.
        let core = self.inner.lock().expect("Allowlist mutex poisoned");
        core.domains.contains(domain)
    }

    /// Recharge l'allowlist si le fichier sur disque a changé depuis le
    /// dernier chargement.
    ///
    /// **Lot C (D4) — hot-reload par mtime.** Compare le `mtime` courant du
    /// fichier `allowlist.json` au `mtime` mémorisé. Règles :
    /// - Aucun `path` mémorisé (`from_vec` / `empty`) → no-op silencieux.
    /// - Fichier présent et `mtime` strictement plus récent → recharger.
    /// - Fichier absent alors qu'on en avait un → vider (dégradation
    ///   gracieuse, comme `load_or_default`).
    /// - Fichier présent alors qu'on n'en avait pas (mtime mémorisé `None`) →
    ///   charger (cas du démarrage où le fichier n'existait pas encore).
    /// - `mtime` identique → no-op silencieux.
    /// - Erreur de lecture (I/O, JSON malformé) → `tracing::warn!` et on
    ///   **garde l'état actuel** (jamais de panic, jamais d'écrasement par
    ///   du vide silencieux).
    ///
    /// `async` par cohérence avec le reste du code (la tâche de reload
    /// tourne dans un `tokio::spawn`).
    pub async fn reload_if_changed(&self) {
        // On capture le path + le mtime mémorisé sous un premier lock court,
        // puis on effectue l'I/O **hors lock** (le lock n'est pas async, on
        // ne veut pas le tenir pendant un read disque). C'est le pattern
        // « lock-check-act » classique.
        let (path_opt, known_mtime) = {
            let core = self.inner.lock().expect("Allowlist mutex poisoned");
            (core.path.clone(), core.last_known_mtime)
        };
        let Some(path) = path_opt else {
            // Allowlist synthétique (from_vec / empty) — pas de fichier.
            return;
        };
        if !path.exists() {
            // Fichier disparu : si on avait quelque chose en mémoire, on
            // vide. Sinon, no-op.
            if known_mtime.is_some() {
                let mut core = self.inner.lock().expect("Allowlist mutex poisoned");
                if core.path.as_deref() == Some(path.as_path()) {
                    core.domains.clear();
                    core.last_known_mtime = None;
                }
                tracing::info!(path = %path.display(),
                    "allowlist.json disparu — allowlist vidée (hot-reload)");
            }
            return;
        }
        // Fichier présent : on lit le mtime. Si on n'arrive pas à le lire,
        // on ne touche à rien (warning + état inchangé).
        let current_mtime = match std::fs::metadata(&path).and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "hot-reload: mtime illisible ; état conservé");
                return;
            }
        };
        // Pas de changement détecté → no-op silencieux.
        if Some(current_mtime) == known_mtime {
            return;
        }
        // Changement : on relit le contenu. Toute erreur → warn + état
        // conservé (l'ancien set reste valide pour le vigil).
        let raw = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "hot-reload: lecture impossible ; état conservé");
                return;
            }
        };
        let parsed: AllowlistFile = match serde_json::from_str(&raw) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "hot-reload: JSON malformé ; état conservé");
                return;
            }
        };
        let new_domains: HashSet<String> = parsed.domains.into_iter().collect();
        let mut core = self.inner.lock().expect("Allowlist mutex poisoned");
        // Re-vérification sous lock : un autre reload a pu nous devancer.
        if Some(current_mtime) == core.last_known_mtime {
            return;
        }
        core.domains = new_domains;
        core.last_known_mtime = Some(current_mtime);
        tracing::info!(path = %path.display(),
            domains = core.domains.len(),
            "allowlist rechargée (hot-reload par mtime)");
    }

    /// Lance une tâche tokio de fond qui appelle `reload_if_changed` toutes
    /// les `interval` (par défaut 25 s). La tâche est « fire-and-forget » :
    /// elle ne retourne pas de `JoinHandle` (le daemon n'en a pas besoin,
    /// et cela évite au contrôleur en `main.rs` d'avoir à la gérer).
    ///
    /// **Note pour le câblage** (hors périmètre Lot C) : `main.rs` doit
    /// appeler une seule ligne :
    /// ```ignore
    /// dsh_eyes::vigil::Allowlist::spawn_reload_task(ctx.allowlist.clone());
    /// ```
    /// après la construction du `VigilCtx`. Le contrôleur du v2.2 câblera
    /// cette ligne lors de l'intégration finale (Lot E ou post-vague).
    ///
    /// Pourquoi `25 s` : c'est l'intervalle de l'alarme keep-alive côté
    /// extension (cf. `chore(extension): version 0.2.1`). On aligne le
    /// tick de reload sur la même granularité pour rester sous le seuil
    /// de perception humaine (< 30 s) sans surcharger le disque.
    pub fn spawn_reload_task(self) {
        const RELOAD_INTERVAL: Duration = Duration::from_secs(25);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(RELOAD_INTERVAL);
            // Le premier tick est immédiat (`MissedTickBehavior::default`
            // en tokio 1.x) — on le consomme pour ne pas recharger deux
            // fois dans la même seconde que `load_or_default`.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                self.reload_if_changed().await;
            }
        });
    }
}

/// Format JSON de `~/.dsh-eyes/allowlist.json`.
#[derive(serde::Deserialize)]
struct AllowlistFile {
    domains: Vec<String>,
}

/// État de session : ensemble des domaines déjà vus (toutes alertes confondues).
///
/// Stocké dans un `Arc<Mutex<...>>` pour pouvoir être partagé entre
/// `handle_socket` (qui appelle `check` puis enregistre le domaine) et un
/// futur endpoint `/state` (lecture seule). Choix `tokio::sync::Mutex` pour
/// cohérence avec `Counters` (Task 3) et parce qu'on est toujours en code
/// async.
#[derive(Debug, Default)]
pub struct SessionState {
    seen: HashSet<String>,
}

impl SessionState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enregistre `domain` comme vu et retourne `true` si c'était la 1ʳᵉ
    /// occurrence. Utilisé en interne par `check` ; exposé pour les tests.
    pub fn mark_seen(&mut self, domain: &str) -> bool {
        self.seen.insert(domain.to_string())
    }
}

/// Wrapper partageable pour `SessionState` — même pattern que `Counters`.
pub type SharedSession = Arc<Mutex<SessionState>>;

/// Coche d'évaluations de règles. Une seule alerte par événement est émise
/// (la plus sévère gagne : critical > warning > info). Si aucune règle ne
/// matche, `check` retourne `None` ET marque le domaine comme vu
/// (sinon la 2ᵉ occurrence déclencherait `new_domain`).
///
/// Ordre d'évaluation :
/// 1. `localhost_access` (critical) — matche les IP loopback.
/// 2. `private_network` (warning) — matche les plages RFC1918.
/// 3. `new_domain` (info) — matche les domaines pas encore vus et hors allowlist.
///
/// Cette fonction est **async** (par cohérence avec le reste du code) mais
/// ne fait pas d'I/O. Les `await` sont triviaux sur `Mutex<HashSet<...>>`.
pub async fn check(event: &NavEvent, allow: &Allowlist, session: &SharedSession) -> Option<Alert> {
    let domain = event.domain.as_str();

    // 1) Localhost (critical)
    if let Some(alert) = check_localhost(domain) {
        return Some(alert);
    }
    // 2) Réseau privé (warning)
    if let Some(alert) = check_private(domain) {
        return Some(alert);
    }
    // 3) Nouveau domaine (info) — sauf si déjà dans l'allowlist.
    if !allow.contains(domain) {
        let mut state = session.lock().await;
        if state.mark_seen(domain) {
            return Some(Alert {
                level: AlertLevel::Info,
                rule: AlertRule::NewDomain,
                detail: domain.to_string(),
            });
        }
    } else {
        // Domaine allowlisté : on le marque comme vu quand même pour qu'une
        // future édition de l'allowlist (sans redémarrage) ne le re-signale
        // pas rétroactivement comme nouveau. Cf. note dans `Allowlist::load_or_default`.
        let mut state = session.lock().await;
        let _ = state.mark_seen(domain);
    }
    None
}

/// Règle `localhost_access` : `localhost`, `127.0.0.0/8`, `::1`.
fn check_localhost(domain: &str) -> Option<Alert> {
    if domain.eq_ignore_ascii_case("localhost") {
        return Some(Alert {
            level: AlertLevel::Critical,
            rule: AlertRule::LocalhostAccess,
            detail: domain.to_string(),
        });
    }
    if let Ok(ip) = domain.parse::<IpAddr>() {
        if is_loopback(&ip) {
            return Some(Alert {
                level: AlertLevel::Critical,
                rule: AlertRule::LocalhostAccess,
                detail: domain.to_string(),
            });
        }
    }
    None
}

/// Règle `private_network` : `10.0.0.0/8`, `192.168.0.0/16`, `172.16.0.0/12`.
fn check_private(domain: &str) -> Option<Alert> {
    if let Ok(ip) = domain.parse::<IpAddr>() {
        if is_private_rfc1918(&ip) {
            return Some(Alert {
                level: AlertLevel::Warning,
                rule: AlertRule::PrivateNetwork,
                detail: domain.to_string(),
            });
        }
    }
    None
}

/// `true` si l'IP est dans une plage loopback (`127.0.0.0/8` ou `::1`).
fn is_loopback(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.octets()[0] == 127,
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}

/// `true` si l'IP est dans une plage RFC1918 (10/8, 192.168/16, 172.16/12).
/// Pour IPv6 on ne signale pas (ULA fc00::/7 = `is_unique_local()`, mais
/// l'usage loopback est déjà couvert par `is_loopback`).
fn is_private_rfc1918(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            o[0] == 10
                || (o[0] == 172 && (16..=31).contains(&o[1]))
                || (o[0] == 192 && o[1] == 168)
        }
        IpAddr::V6(_) => false,
    }
}

/// Garde-fou partagé (Task 3, REWORK webbridge) : dit si un hôte / URL
/// pointe vers une cible interdite (loopback, RFC1918, nom `localhost`).
///
/// Utilisé par `POST /v2/open` pour refuser en 403 les ouvertures vers
/// les ressources locales du poste (l'extension DSH Eyes observe déjà
/// ce trafic via `chrome.webRequest`, pas besoin de l'orchestrer depuis
/// l'API). Mutualise `is_loopback` et `is_private_rfc1918` qui étaient
/// jusque-là privées au module `vigil`.
///
/// Sémantique :
/// - Le nom `localhost` (insensible à la casse) → interdit.
/// - Une IP parseable en `127.0.0.0/8` ou `::1` → interdite.
/// - Une IP parseable en RFC1918 (10/8, 192.168/16, 172.16/12) → interdite.
/// - Tout autre chaîne (domaine public, IP publique, IPv6 non-loopback) → autorisé.
///
/// Note : on **ne fait pas** de résolution DNS ici (impossible sans
/// bloquer). Un attaquant peut donc inscrire `127.0.0.1.nip.io` —
/// c'est le rôle du vigil de notifier `new_domain`. Cette fonction ne
/// vise que les cibles manifestement privées.
pub fn is_forbidden_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // Normalise IPv4-mapped IPv6 (::ffff:127.0.0.1 → 127.0.0.1) avant le
    // parse, sinon la forme mappée échappe aux règles loopback/RFC1918.
    let normalized = host
        .strip_prefix("::ffff:")
        .map(|s| s.to_string())
        .unwrap_or_else(|| host.to_string());
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return is_loopback(&ip) || is_private_rfc1918(&ip);
    }
    false
}

/// Calcule le chemin de `allowlist.json` (même logique que `events.jsonl` —
/// `DSH_EYES_HOME` ou `~/.dsh-eyes/`). Ne crée pas le dossier.
fn allowlist_path() -> std::io::Result<std::path::PathBuf> {
    let home = match std::env::var_os("DSH_EYES_HOME") {
        Some(v) => std::path::PathBuf::from(v),
        None => default_home()?,
    };
    Ok(home.join("allowlist.json"))
}

/// Symétrique de `ingest::default_home` — duplication volontaire pour ne
/// pas exposer la fonction interne de `ingest.rs` (boundary de module).
fn default_home() -> std::io::Result<std::path::PathBuf> {
    if let Some(h) = std::env::var_os("HOME") {
        return Ok(std::path::PathBuf::from(h).join(".dsh-eyes"));
    }
    if let Some(h) = std::env::var_os("USERPROFILE") {
        return Ok(std::path::PathBuf::from(h).join(".dsh-eyes"));
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "ni HOME ni USERPROFILE défini",
    ))
}

/// Helper exposé pour mutualiser le calcul du dossier d'accueil avec
/// `ingest::events_path` (cf. brief : « même logique DSH_EYES_HOME »).
/// Utilisé par `ingest.rs` pour `alerts.jsonl` (l'historique) et par
/// `api.rs` pour `/events` (les 50 dernières).
pub fn dsh_eyes_home() -> std::io::Result<std::path::PathBuf> {
    let home = match std::env::var_os("DSH_EYES_HOME") {
        Some(v) => std::path::PathBuf::from(v),
        None => default_home()?,
    };
    std::fs::create_dir_all(&home)?;
    Ok(home)
}

/// Helper public pour récupérer le chemin du fichier `alerts.jsonl` —
/// mutualisé avec `ingest::events_path` (qui reste maître pour `events.jsonl`).
pub fn alerts_path() -> std::io::Result<std::path::PathBuf> {
    Ok(dsh_eyes_home()?.join("alerts.jsonl"))
}

/// Lit les N dernières lignes d'un fichier (utilisé par `/events` pour
/// servir les 50 dernières alertes). Retourne un `Vec<serde_json::Value>`
/// des lignes JSONL valides (les lignes malformées sont ignorées).
pub fn tail_jsonl(path: &Path, n: usize) -> std::io::Result<Vec<serde_json::Value>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out: Vec<serde_json::Value> = raw
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect();
    // Garde les N dernières.
    if out.len() > n {
        let drop = out.len() - n;
        out.drain(..drop);
    }
    Ok(out)
}
