//! Test d'intégration de la route WebSocket `GET /ws` (Task 2).
//!
//! Contrat (extrait du brief Task 2) :
//! 1. Démarre l'application complète (`api::router()` après ajout de la route WS)
//!    sur un TcpListener bindé sur un port éphémère (port 0).
//! 2. Se comporte comme un client WS `tokio-tungstenite` (`connect_async`).
//! 3. Envoie un événement JSON valide.
//! 4. Attend la réponse `{"ok":true}` du serveur.
//! 5. Vérifie que `${DSH_EYES_HOME}/events.jsonl` contient une ligne avec le bon `domain`.
//!
//! Implémentation test-first : ce test doit échouer tant que `src/ingest.rs`
//! et la modif de `src/api.rs` ne sont pas faites.

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// Petit timeout global pour éviter qu'un bug ne fasse tourner le test à l'infini.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn ws_accepte_un_evenement_valide() {
    // Dossier temporaire pour la persistance. On positionne la variable d'env AVANT
    // de binder le listener, pour que `ingest::ws_handler` lise la bonne valeur.
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    // Bind sur port éphémère (0).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    // Construit le router complet (avec /health et /ws) et spawn le serveur.
    let app = dsh_eyes::api::router();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    // Connexion client WS via tokio-tungstenite. URLs ws:// (pas wss) — port éphémère.
    let url = format!("ws://{}/ws", addr);
    let (stream, _response) =
        tokio_tungstenite::connect_async(&url).await.expect("connect_async WS");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // Envoie un événement valide.
    let evt = json!({
        "ts": 1u64,
        "domain": "example.com",
        "url": "https://example.com/",
        "kind": "request",
        "bytes": 42u64,
    });
    ws_tx
        .send(Message::Text(evt.to_string()))
        .await
        .expect("send text");

    // Attend la réponse {"ok":true}.
    let reply_msg = tokio::time::timeout(TEST_TIMEOUT, ws_rx.next())
        .await
        .expect("réponse dans le délai")
        .expect("stream non vide")
        .expect("message WS");
    let reply_text = match reply_msg {
        Message::Text(t) => t.to_string(),
        other => panic!("réponse inattendue: {other:?}"),
    };
    assert_eq!(reply_text, r#"{"ok":true}"#);

    // Ferme proprement.
    let _ = ws_tx.send(Message::Close(None)).await;

    // Vérifie que la ligne JSONL a été écrite.
    let jsonl_path = tmp.join("events.jsonl");
    // L'écriture est async (OpenOptions + write_all), on laisse un petit délai.
    let mut attempts = 0u8;
    let body = loop {
        attempts += 1;
        if let Ok(b) = std::fs::read_to_string(&jsonl_path) {
            if !b.is_empty() {
                break b;
            }
        }
        if attempts > 50 {
            panic!(
                "events.jsonl absent ou vide après {} tentatives (chemin={})",
                attempts,
                jsonl_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let parsed: serde_json::Value =
        serde_json::from_str(body.trim()).expect("ligne JSONL valide");
    assert_eq!(parsed["domain"], "example.com");
    assert_eq!(parsed["ts"], 1u64);

    // Stoppe le serveur.
    server.abort();
}

/// Crée un répertoire temporaire sous le dossier `target/`, qui est
/// nettoyé par cargo lors d'un `cargo clean`. Suffisant pour ce test :
/// on n'a pas besoin d'un vrai tmp externe.
fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "ws-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}