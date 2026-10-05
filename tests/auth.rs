//! Test d'intégration de l'auth v2 (Task 1).
//!
//! Contrat (extrait du brief Task 1) :
//! 1. Démarre l'application complète sur un port éphémère, avec un
//!    `DSH_EYES_HOME` temporaire (pour ne pas générer/écraser le token
//!    réel de l'utilisateur).
//! 2. `GET /v2/ping` SANS `Authorization` → 401.
//! 3. `GET /v2/ping` AVEC `Authorization: Bearer <token lu dans
//!    $DSH_EYES_HOME/token>` → 200, body = `{"ok":true}`.
//! 4. `GET /v2/ping` avec un mauvais token → 401.
//!
//! Réutilise `common::env_guard()` pour sérialiser l'accès à DSH_EYES_HOME
//! (env partagé du process de test — garde de Task 4).

use dsh_eyes::config::Config;
use dsh_eyes::hands::channel::ExtensionChannel;
use dsh_eyes::state::VigilCtx;
use dsh_eyes::stats::Counters;
use dsh_eyes::vigil::Allowlist;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;

/// Le sous-test 401-sans-token et 401-mauvais-token partagent la même
/// fixture : on les exécute ensemble pour éviter de relancer le serveur.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn auth_v2_ping_refuse_sans_token_puis_accepte_avec_token() {
    // Sérialise l'accès à DSH_EYES_HOME.
    let _guard = common::env_guard();

    // Dossier temporaire pour la persistance du token v2.
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    // Charge la config : doit avoir généré un token dans `tmp/token`.
    let cfg = Config::load().expect("config load");
    let token = cfg.api_token().to_string();
    assert!(!token.is_empty(), "token généré vide");
    let token_path = tmp.join("token");
    assert!(token_path.exists(), "fichier token absent après load");
    // Le fichier token doit être en mode 600 (UNIX only — le test skip sinon).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&token_path)
            .expect("metadata token")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "token chmod != 600 (mode={mode:o})");
    }

    // Monte l'application via la nouvelle route v2.
    let ctx = VigilCtx::for_tests(Counters::new(), Allowlist::empty());
    let channel = ExtensionChannel::new();
    let app = dsh_eyes::api::router_with_v2(ctx, cfg, channel);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    // 1) Sans token → 401.
    let status = fetch_status(addr, "/v2/ping", None).await;
    assert_eq!(
        status, 401,
        "/v2/ping sans Authorization doit retourner 401 (reçu {status})"
    );

    // 2) Mauvais token → 401.
    let status = fetch_status(addr, "/v2/ping", Some("Bearer not-the-right-token")).await;
    assert_eq!(
        status, 401,
        "/v2/ping avec un mauvais token doit retourner 401 (reçu {status})"
    );

    // 3) Bon token → 200, body = {"ok":true}.
    let (status, body) = fetch_full(addr, "/v2/ping", Some(&format!("Bearer {token}"))).await;
    assert_eq!(status, 200, "/v2/ping avec bon token : 200 attendu, reçu {status}");
    let v: Value = serde_json::from_str(&body).expect("JSON /v2/ping");
    assert_eq!(v["ok"], true, "body inattendu : {body}");

    // 4) /health reste ouvert même avec le montage /v2 → 200 sans token.
    let status = fetch_status(addr, "/health", None).await;
    assert_eq!(
        status, 200,
        "/health doit rester ouvert sans auth (reçu {status})"
    );

    server.abort();
}

/// Crée un répertoire temporaire sous `target/tests-tmp/`. Nettoyé par
/// `cargo clean`. Évite de polluer `/tmp` (règle Aegis 2026-09-28).
fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "auth-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

/// GET un path et retourne uniquement le status code.
async fn fetch_status(
    addr: std::net::SocketAddr,
    path: &str,
    auth: Option<&str>,
) -> u16 {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect TCP");
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n"
    );
    if let Some(b) = auth {
        req.push_str(&format!("Authorization: {b}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8(buf).expect("utf8");
    parse_status(&raw)
}

/// GET un path et retourne (status, body).
async fn fetch_full(
    addr: std::net::SocketAddr,
    path: &str,
    auth: Option<&str>,
) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect TCP");
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n"
    );
    if let Some(b) = auth {
        req.push_str(&format!("Authorization: {b}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8(buf).expect("utf8");
    let status = parse_status(&raw);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

fn parse_status(raw: &str) -> u16 {
    let line = raw.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let _ = parts.next(); // "HTTP/1.1"
    parts.next().and_then(|s| s.parse().ok()).unwrap_or(0)
}
