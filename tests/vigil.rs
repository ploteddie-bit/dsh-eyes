//! Tests unitaires purs de `dsh_eyes::vigil` (Task 4).
//!
//! Contrat (extrait du brief Task 4) :
//! 1. `localhost_access` (critical) : `domain` = `127.0.0.1`, `localhost`,
//!    toute IP de `127.0.0.0/8`, `::1`.
//! 2. `private_network` (warning) : `10.0.0.0/8`, `192.168.0.0/16`, `172.16.0.0/12`.
//! 3. `new_domain` (info) : première occurrence d'un domaine dans la session
//!    (les domaines connus sont listés dans `~/.dsh-eyes/allowlist.json`,
//!    éditable à la main, chargée lazy).
//!
//! Approche test-first : ce test doit échouer tant que `src/vigil.rs` n'est
//! pas implémenté. Aucune dépendance réseau / disque pour rester déterministe :
//! chaque test utilise un `Allowlist::empty()` ou un `Allowlist` ad-hoc.
//!
//! Note : `vigil::check` est *pur* (pas d'I/O, pas d'état global) — l'état
//! de session est passé en paramètre `SessionState` (Mutex<HashSet<String>>)
//! pour que les tests soient totalement isolés et parallélisables.

use dsh_eyes::ingest::NavEvent;
use dsh_eyes::vigil::{Alert, AlertLevel, AlertRule, Allowlist, SessionState};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Construit un `NavEvent` minimal pour les tests.
fn evt(domain: &str) -> NavEvent {
    NavEvent {
        ts: 1,
        domain: domain.to_string(),
        url: format!("https://{domain}/"),
        kind: "request".to_string(),
        bytes: None,
    }
}

/// 1) `localhost_access` (critical) sur `127.0.0.1`.
#[tokio::test]
async fn vigil_detecte_localhost_v4() {
    let allow = Allowlist::empty();
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("127.0.0.1"), &allow, &session).await;
    let alert = alert.expect("127.0.0.1 doit produire une alerte");
    assert_eq!(alert.level, AlertLevel::Critical);
    assert_eq!(alert.rule, AlertRule::LocalhostAccess);
}

/// 2) `localhost_access` (critical) sur `::1` (IPv6 loopback).
#[tokio::test]
async fn vigil_detecte_localhost_v6() {
    let allow = Allowlist::empty();
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("::1"), &allow, &session).await;
    let alert = alert.expect("::1 doit produire une alerte");
    assert_eq!(alert.level, AlertLevel::Critical);
    assert_eq!(alert.rule, AlertRule::LocalhostAccess);
}

/// 3) `private_network` (warning) sur `10.0.0.x` (RFC1918).
#[tokio::test]
async fn vigil_detecte_private_network() {
    let allow = Allowlist::empty();
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("10.0.0.5"), &allow, &session).await;
    let alert = alert.expect("10.0.0.5 doit produire une alerte");
    assert_eq!(alert.level, AlertLevel::Warning);
    assert_eq!(alert.rule, AlertRule::PrivateNetwork);
}

/// 4) Domaine allowlisté (ex. `github.com`) : aucune alerte, et **pas** de
/// marquage `new_domain` (l'allowlist fait foi).
#[tokio::test]
async fn vigil_allowlist_pas_d_alerte() {
    let allow = Allowlist::from_vec(vec!["github.com".to_string()]);
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("github.com"), &allow, &session).await;
    assert!(alert.is_none(), "github.com est dans l'allowlist : None ; got={alert:?}");
}

/// 5) Domaine neuf (pas dans l'allowlist, 1ère occurrence en session) :
/// `new_domain` au niveau `info`.
#[tokio::test]
async fn vigil_detecte_nouveau_domaine() {
    let allow = Allowlist::empty();
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("fresh.example.org"), &allow, &session).await;
    let alert = alert.expect("nouveau domaine doit produire new_domain");
    assert_eq!(alert.level, AlertLevel::Info);
    assert_eq!(alert.rule, AlertRule::NewDomain);
}

/// 6) Domaine récurrent (vu plus d'une fois en session) : aucune alerte
/// (la première occurrence a déjà été signalée comme `new_domain`).
#[tokio::test]
async fn vigil_domaine_recurrent_pas_d_alerte() {
    let allow = Allowlist::empty();
    let session = Arc::new(Mutex::new(SessionState::new()));
    // Premier passage : déclenche new_domain.
    let _ = dsh_eyes::vigil::check(&evt("seen.example.com"), &allow, &session).await;
    // Second passage : plus rien.
    let alert = dsh_eyes::vigil::check(&evt("seen.example.com"), &allow, &session).await;
    assert!(alert.is_none(), "deuxième occurrence : pas d'alerte ; got={alert:?}");
}

/// Bonus — la sérialisation JSON de l'`Alert` respecte le contrat :
/// `{"level":"...","rule":"...","detail":...}`.
#[tokio::test]
async fn vigil_alert_serialization_shape() {
    let alert = Alert {
        level: AlertLevel::Critical,
        rule: AlertRule::LocalhostAccess,
        detail: "127.0.0.1".to_string(),
    };
    let v: serde_json::Value = serde_json::to_value(&alert).expect("serialize Alert");
    assert_eq!(v["level"], "critical");
    assert_eq!(v["rule"], "localhost_access");
    assert_eq!(v["detail"], "127.0.0.1");
}
