//! Test d'intégration de la route `GET /events` (Task 4).
//!
//! Contrat (extrait du brief Task 4) :
//! 1. Démarre l'application complète sur un port éphémère, avec un
//!    `VigilCtx::for_tests` (allowlist vide) et un `DSH_EYES_HOME` temporaire
//!    pour ne pas polluer `~/.dsh-eyes/alerts.jsonl`.
//! 2. Envoie 3 événements WS :
//!    - 127.0.0.1 → alerte critical (localhost_access), persistée disque
//!    - 10.0.0.5  → alerte warning (private_network), non persistée disque
//!    - example.com (1ère fois) → alerte info (new_domain), non persistée disque
//! 3. GET /events et vérifie la présence des 3 alertes avec les bons niveaux.

use dsh_eyes::hands::channel::ExtensionChannel;
use dsh_eyes::state::VigilCtx;
use dsh_eyes::stats::Counters;
use dsh_eyes::vigil::Allowlist;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

mod common;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
// Le MutexGuard couvre tout le test (qui contient des `await` partout) : c'est
// volontaire, on veut sérialiser tout le test pour ne pas marcher sur DSH_EYES_HOME.
#[allow(clippy::await_holding_lock)]
async fn events_reflete_les_alertes_ws() {
    // Sérialise l'accès à DSH_EYES_HOME (env partagé du process de test).
    let _guard = common::env_guard();

    // Dossier temporaire pour la persistance d'alertes.
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    let ctx = VigilCtx::for_tests(Counters::new(), Allowlist::empty());
    let channel = ExtensionChannel::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = dsh_eyes::api::router(ctx, channel);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    // 3 événements WS : localhost / private / new domain.
    let url = format!("ws://{}/ws", addr);
    let (stream, _) =
        tokio_tungstenite::connect_async(&url).await.expect("connect WS");
    let (mut ws_tx, mut ws_rx) = stream.split();

    let cases = [
        (1u64, "127.0.0.1", "critical", "localhost_access"),
        (2u64, "10.0.0.5", "warning", "private_network"),
        (3u64, "example.com", "info", "new_domain"),
    ];
    for (ts, domain, _level, _rule) in cases {
        let evt = json!({
            "ts": ts,
            "domain": domain,
            "url": format!("https://{domain}/"),
            "kind": "request",
        });
        ws_tx.send(Message::Text(evt.to_string())).await.expect("send");
        let reply = tokio::time::timeout(TEST_TIMEOUT, ws_rx.next())
            .await
            .expect("réponse dans le délai")
            .expect("stream non vide")
            .expect("message WS");
        match reply {
            Message::Text(t) => assert_eq!(t.to_string(), r#"{"ok":true}"#),
            other => panic!("réponse inattendue: {other:?}"),
        }
    }
    let _ = ws_tx.send(Message::Close(None)).await;

    // Petit délai pour que les push async dans l'AlertBuffer soient visibles.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // GET /events
    let body = fetch_events(addr).await;
    let v: Value = serde_json::from_str(&body).expect("JSON /events");
    let arr = v.as_array().expect("réponse = array JSON");
    assert_eq!(
        arr.len(),
        3,
        "/events doit retourner 3 alertes (localhost + private + new_domain) ; body={body}"
    );

    // Les alertes sont retournées en ordre inverse d'insertion (tail).
    // Index 0 = plus récente = new_domain, index 2 = plus ancienne = localhost.
    // On vérifie la présence et le contenu sans dépendre de l'ordre.
    let by_rule: std::collections::HashMap<String, &Value> = arr
        .iter()
        .map(|a| (a["rule"].as_str().unwrap().to_string(), a))
        .collect();
    assert_eq!(by_rule["localhost_access"]["level"], "critical");
    assert_eq!(by_rule["localhost_access"]["detail"], "127.0.0.1");
    assert_eq!(by_rule["private_network"]["level"], "warning");
    assert_eq!(by_rule["private_network"]["detail"], "10.0.0.5");
    assert_eq!(by_rule["new_domain"]["level"], "info");
    assert_eq!(by_rule["new_domain"]["detail"], "example.com");

    // Vérifie que l'alerte critical a été persistée sur disque dans alerts.jsonl.
    let alerts_jsonl = tmp.join("alerts.jsonl");
    let mut attempts = 0u8;
    let body = loop {
        attempts += 1;
        if let Ok(b) = std::fs::read_to_string(&alerts_jsonl) {
            if !b.is_empty() {
                break b;
            }
        }
        if attempts > 50 {
            panic!(
                "alerts.jsonl absent ou vide après {} tentatives (chemin={})",
                attempts,
                alerts_jsonl.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let line: Value = serde_json::from_str(body.trim()).expect("JSONL valide");
    assert_eq!(line["level"], "critical");
    assert_eq!(line["rule"], "localhost_access");
    assert_eq!(line["detail"], "127.0.0.1");

    server.abort();
}

/// Crée un répertoire temporaire sous `target/tests-tmp/`. Nettoyé par
/// `cargo clean`. Évite de polluer `/tmp` (règle Aegis 2026-09-28).
fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "events-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

/// GET /events en HTTP/1.1 brut.
async fn fetch_events(addr: std::net::SocketAddr) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect TCP");
    let req = format!(
        "GET /events HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8(buf).expect("utf8");
    let Some(idx) = raw.find("\r\n\r\n") else {
        panic!("réponse HTTP sans CRLF CRLF : {raw:?}");
    };
    raw[idx + 4..].to_string()
}
