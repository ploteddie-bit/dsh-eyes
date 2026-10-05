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
//! - L'allowlist est chargée une fois au démarrage (lazy) et passée aux
//!   checks. Édition à la main → redémarrage du daemon pour prendre effet.
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
    path::Path,
    sync::Arc,
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
/// `Arc<HashSet<String>>` intérieur : clone = partage du même set, comparaison
/// O(1), pas d'I/O. `Allowlist::from_vec` permet aux tests d'injecter un
/// contenu arbitraire sans toucher au disque.
#[derive(Debug, Clone, Default)]
pub struct Allowlist {
    inner: Arc<HashSet<String>>,
}

impl Allowlist {
    /// Allowlist vide (tous les domaines sont « nouveaux »).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construit une allowlist à partir d'un `Vec<String>`.
    /// Utilisé par les tests et par le chargement initial.
    pub fn from_vec(domains: Vec<String>) -> Self {
        Self {
            inner: Arc::new(domains.into_iter().collect()),
        }
    }

    /// Charge l'allowlist depuis `~/.dsh-eyes/allowlist.json`.
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
            return Self::empty();
        }
        match std::fs::read_to_string(&path) {
            Ok(raw) => match serde_json::from_str::<AllowlistFile>(&raw) {
                Ok(f) => Self::from_vec(f.domains),
                Err(e) => {
                    tracing::warn!(error = %e, path = %path.display(),
                        "allowlist.json malformé ; allowlist vide appliquée");
                    Self::empty()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "lecture allowlist.json impossible ; allowlist vide appliquée");
                Self::empty()
            }
        }
    }

    /// `true` si `domain` est dans l'allowlist.
    pub fn contains(&self, domain: &str) -> bool {
        self.inner.contains(domain)
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
