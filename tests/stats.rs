//! Test d'intégration de la route `GET /stats` (Task 3).
//!
//! Contrat (extrait du brief Task 3) :
//! 1. Démarre l'application avec un `Counters` partagé, sur un port éphémère.
//! 2. Envoie 3 événements valides via WS (2 domaines, mix avec/sans `bytes`).
//! 3. Appelle `GET /stats` et vérifie la forme JSON exacte :
//!    `{"domains": {"<domaine>": {"requests": N, "bytes": B}}, ...}`
//! 4. Vérifie que les totaux agrègent correctement.
//!
//! Note : on injecte directement `Counters` (pas via `DSH_EYES_HOME`) pour
//! éviter le conflit avec `tests/ingest.rs` qui pose cette variable d'env.
//! L'ordre des tests `cargo test` n'étant pas garanti, cette approche est
//! robuste.

use dsh_eyes::hands::channel::ExtensionChannel;
use dsh_eyes::state::VigilCtx;
use dsh_eyes::stats::{Counters, DomainCounters};
use dsh_eyes::vigil::Allowlist;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn stats_reflete_les_evenements_ws() {
    // Compteur partagé, cloné dans le router ET dans la vérif HTTP.
    let counters = Counters::new();

    // Bind éphémère.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    // Router complet, câblé avec le contexte agrégé. Task 4 : `api::router`
    // prend un `VigilCtx` ; on injecte directement les compteurs via
    // `for_tests` (allowlist vide, session vide). REWORK 2026-10-05 :
    // `api::router` prend aussi un `ExtensionChannel`.
    let app = dsh_eyes::api::router(
        VigilCtx::for_tests(counters.clone(), Allowlist::empty()),
        ExtensionChannel::new(),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    // --- Burst WS : 3 événements, 2 domaines -------------------------------
    let url = format!("ws://{}/ws", addr);
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect_async WS");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // 1) example.com, request, 100 bytes
    send_event(
        &mut ws_tx,
        &mut ws_rx,
        json!({"ts": 1, "domain": "example.com", "url": "https://example.com/", "kind": "request", "bytes": 100}),
    )
    .await;
    // 2) example.com, response, 250 bytes
    send_event(
        &mut ws_tx,
        &mut ws_rx,
        json!({"ts": 2, "domain": "example.com", "url": "https://example.com/a", "kind": "response", "bytes": 250}),
    )
    .await;
    // 3) other.org, request, sans bytes (doit compter 0)
    send_event(
        &mut ws_tx,
        &mut ws_rx,
        json!({"ts": 3, "domain": "other.org", "url": "https://other.org/", "kind": "request"}),
    )
    .await;

    // Ferme la WS.
    let _ = ws_tx.send(Message::Close(None)).await;

    // --- GET /stats via hyper ----------------------------------------------
    // Petit délai pour que l'incrément asynchrone soit visible côté HTTP
    // (le `write()` du RwLock est synchrone, mais on lit ensuite en HTTP).
    tokio::time::sleep(Duration::from_millis(50)).await;

    let body = fetch_stats(addr).await;

    // Vérification exacte de la forme JSON.
    let v: serde_json::Value = serde_json::from_str(&body).expect("JSON /stats valide");

    // example.com : 2 requêtes, 350 bytes
    assert_eq!(
        v["domains"]["example.com"],
        json!({"requests": 2, "bytes": 350}),
        "example.com doit valoir {{requests:2, bytes:350}} ; body={body}"
    );
    // other.org : 1 requête, 0 bytes (champ absent → 0)
    assert_eq!(
        v["domains"]["other.org"],
        json!({"requests": 1, "bytes": 0}),
        "other.org doit valoir {{requests:1, bytes:0}} ; body={body}"
    );
    // totaux
    assert_eq!(v["total_requests"], json!(3), "total_requests=3 ; body={body}");
    assert_eq!(v["total_bytes"], json!(350), "total_bytes=350 ; body={body}");

    server.abort();
}

#[tokio::test]
async fn stats_snapshot_vide_quand_rien_ne_passe() {
    // Pas de WS : /stats doit renvoyer 0 partout, sans planter.
    let counters = Counters::new();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let app = dsh_eyes::api::router(
        VigilCtx::for_tests(counters.clone(), Allowlist::empty()),
        ExtensionChannel::new(),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    let body = fetch_stats(addr).await;
    let v: serde_json::Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(v["domains"], json!({}), "domains vide ; body={body}");
    assert_eq!(v["total_requests"], json!(0));
    assert_eq!(v["total_bytes"], json!(0));

    server.abort();
}

#[tokio::test]
async fn counters_record_agregge_correctement() {
    // Test unitaire direct : on n'utilise pas le réseau du tout.
    let counters = Counters::new();
    counters.record_async("a.test", Some(10)).await;
    counters.record_async("a.test", Some(15)).await;
    counters.record_async("b.test", None).await;
    counters.record_async("b.test", Some(100)).await;

    let snap: std::collections::HashMap<String, DomainCounters> =
        counters.snapshot_async().await;
    assert_eq!(
        snap.get("a.test"),
        Some(&DomainCounters {
            requests: 2,
            bytes: 25
        })
    );
    assert_eq!(
        snap.get("b.test"),
        Some(&DomainCounters {
            requests: 2,
            bytes: 100
        })
    );
}

/// Envoie un événement et attend `{"ok":true}`.
async fn send_event(
    ws_tx: &mut futures_util::stream::SplitSink<
        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    ws_rx: &mut futures_util::stream::SplitStream<
        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    payload: serde_json::Value,
) {
    ws_tx
        .send(Message::Text(payload.to_string()))
        .await
        .expect("send text");
    let reply = tokio::time::timeout(TEST_TIMEOUT, ws_rx.next())
        .await
        .expect("réponse dans le délai")
        .expect("stream non vide")
        .expect("message WS");
    let reply_text = match reply {
        Message::Text(t) => t.to_string(),
        other => panic!("réponse inattendue: {other:?}"),
    };
    assert_eq!(reply_text, r#"{"ok":true}"#, "événement valide doit être acquitté");
}

/// GET /stats en HTTP/1.1 brut (on évite d'ajouter un client HTTP en dep).
async fn fetch_stats(addr: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect TCP");
    let req = format!(
        "GET /stats HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8(buf).expect("utf8");
    // Sépare headers / body
    let Some(idx) = raw.find("\r\n\r\n") else {
        panic!("réponse HTTP sans CRLF CRLF : {raw:?}");
    };
    raw[idx + 4..].to_string()
}
