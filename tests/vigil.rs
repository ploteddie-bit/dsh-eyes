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

// Mutex global pour sérialiser les tests qui partagent `DSH_EYES_HOME`
// (variable d'environnement du process). Cf. `tests/common/mod.rs` — on
// importe le module par `path` (pas `mod common { include! }` qui ne
// compile pas à cause d'un `use` orphelin dans le mod parent).
#[path = "common/mod.rs"]
mod common;
use common::env_guard;

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

// =====================================================================
// Lot C — D4 : hot-reload de l'allowlist par mtime
//
// Les 2 tests suivants valident le contrat de
// `Allowlist::reload_if_changed` (cf. `src/vigil.rs`) :
// 1. Édition du fichier `allowlist.json` (mtime + nouveau domaine) →
//    rechargement automatique : la nouvelle entrée n'est plus considérée
//    comme « nouveau domaine » par `vigil::check` après `reload_if_changed`.
// 2. Fichier supprimé entre deux ticks → `reload_if_changed` vide
//    l'allowlist sans paniquer (dégradation gracieuse, jamais de panic).
//
// Ces tests partagent `DSH_EYES_HOME` (variable d'environnement du
// process) — l'`env_guard` les sérialise pour éviter qu'un test concurrent
// ne modifie le même répertoire temporaire.
// =====================================================================

/// Prépare un répertoire temporaire sous `/tmp/dsh-eyes-allowlist-<label>`
/// et l'exporte comme `DSH_EYES_HOME`. Le `MutexGuard` retourné par
/// `env_guard()` est rendu pour que l'appelant le maintienne jusqu'à la
/// fin du test (les tests `reload_*` sont donc mutuellement exclusifs —
/// c'est l'`env_guard` global partagé avec `endpoints.rs`).
fn setup_temp_home(label: &str) -> (std::path::PathBuf, std::sync::MutexGuard<'static, ()>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    // On prend le `env_guard` **AVANT** toute opération mutable sur
    // l'env du process — sinon 2 tests parallèles qui appellent
    // `set_var` sans tenir le guard pourraient se chevaucher.
    let guard = env_guard();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let dir = std::path::PathBuf::from(format!(
        "/tmp/dsh-eyes-allowlist-{label}-{pid}-{n}"
    ));
    // Nettoyage préventif (un test précédent peut avoir planté).
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir temp DSH_EYES_HOME");
    // `set_var` est `unsafe` depuis Rust 1.74 pour signaler la race
    // théorique ; ici on l'isole par le `MutexGuard` de `env_guard`.
    unsafe { std::env::set_var("DSH_EYES_HOME", &dir) };
    (dir, guard)
}

/// Écrit un allowlist JSON d'un ou plusieurs domaines dans
/// `<home>/allowlist.json`.
fn write_allowlist(home: &std::path::Path, domains: &[&str]) {
    let payload = serde_json::json!({ "domains": domains });
    std::fs::write(
        home.join("allowlist.json"),
        serde_json::to_string_pretty(&payload).expect("serialize allowlist"),
    )
    .expect("write allowlist.json");
}

/// Test 1 — modification du fichier (mtime + nouveau domaine) :
/// après `reload_if_changed`, le nouveau domaine n'est plus signalé
/// `new_domain` par `vigil::check`. C'est le scénario « édition manuelle
/// prise en compte sans restart ».
// `allow(await_holding_lock)` : le `MutexGuard` de `env_guard()` est tenu
// pendant les `.await` du test, mais il sert exclusivement à sérialiser
// les tests qui partagent `DSH_EYES_HOME` — il n'est pas un garde de
// données async. Le relâcher entre chaque await compliquerait le test
// sans bénéfice (les autres tests ne pourraient pas entrer en course
// tant que le répertoire temp n'est pas nettoyé par le test driver).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn vigil_reload_if_changed_prend_en_compte_les_editions() {
    let (home, _guard) = setup_temp_home("edit");

    // 1) Premier chargement : allowlist contient `github.com` uniquement.
    write_allowlist(&home, &["github.com"]);
    let allow = Allowlist::load_or_default();
    assert!(
        allow.contains("github.com"),
        "github.com doit être chargé au démarrage"
    );
    assert!(
        !allow.contains("fresh.example.org"),
        "fresh.example.org n'est pas encore dans l'allowlist"
    );

    // 2) Avant reload : `fresh.example.org` est « nouveau ».
    let session = Arc::new(Mutex::new(SessionState::new()));
    let alert = dsh_eyes::vigil::check(&evt("fresh.example.org"), &allow, &session).await;
    assert!(
        matches!(
            alert.as_ref().map(|a| (a.level, a.rule)),
            Some((AlertLevel::Info, AlertRule::NewDomain))
        ),
        "avant reload, fresh.example.org doit déclencher new_domain ; got={alert:?}"
    );

    // 3) Édition du fichier (nouvelle entrée + garantit un mtime différent
    //    : la résolution mtime est la seconde sur certains FS — on attend
    //    1,1 s pour être robuste sur ext4/NTFS/FAT).
    std::thread::sleep(std::time::Duration::from_millis(1100));
    write_allowlist(&home, &["github.com", "fresh.example.org"]);

    // 4) Hot-reload : le clone du `Allowlist` (Arc intérieur) partage
    //    l'état ; `reload_if_changed` mute l'Arc partagé en place.
    allow.reload_if_changed().await;

    // 5) Après reload : `fresh.example.org` n'est plus « nouveau » —
    //    aucune alerte `new_domain`.
    let session2 = Arc::new(Mutex::new(SessionState::new()));
    let alert2 = dsh_eyes::vigil::check(&evt("fresh.example.org"), &allow, &session2).await;
    assert!(
        alert2.is_none(),
        "après reload, fresh.example.org est allowlisté : pas d'alerte ; got={alert2:?}"
    );
}

/// Test 2 — fichier supprimé entre deux ticks :
/// `reload_if_changed` vide l'allowlist sans paniquer.
// Cf. note sur `await_holding_lock` du test 1 ci-dessus — même raison :
// `env_guard()` n'est qu'un sérialiseur, pas un garde de données async.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn vigil_reload_if_changed_fichier_absent_allowlist_vide() {
    let (home, _guard) = setup_temp_home("absent");

    // 1) Premier chargement avec un fichier existant.
    write_allowlist(&home, &["github.com"]);
    let allow = Allowlist::load_or_default();
    assert!(allow.contains("github.com"));

    // 2) Suppression du fichier (simule « l'opérateur a effacé l'allowlist »).
    std::fs::remove_file(home.join("allowlist.json"))
        .expect("rm allowlist.json");

    // 3) Hot-reload : doit dégrader gracieusement vers vide, sans panic.
    allow.reload_if_changed().await;

    // 4) L'allowlist est désormais vide.
    assert!(
        !allow.contains("github.com"),
        "après suppression du fichier, github.com ne doit plus être dans l'allowlist"
    );
}
