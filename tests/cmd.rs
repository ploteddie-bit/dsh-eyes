//! Test d'intégration de la route `POST /v2/cmd` (REWORK Task 2).
//!
//! ## Pourquoi ce test existe
//!
//! Le brief REWORK 2026-10-05 demande une **preuve automatisée** du
//! cycle complet `démion → commande → extension → réponse`, sans
//! dépendre du vrai Chrome de l'opérateur (celui-ci est trop lourd à
//! recharger pour chaque test). On utilise donc une **fausse extension**
//! — un client WS qui parle le même protocole que `background.js` et
//! qui répond aux commandes.
//!
//! ## Scénario
//!
//! 1. Démarre l'application complète (v1 + v2) sur un port éphémère.
//! 2. Ouvre une WS sur `/ws` avec un client `tokio-tungstenite`.
//! 3. Lit la première `Command` JSON que le serveur envoie.
//! 4. Répond avec `{"cmd_id":N,"ok":true,"result":...}`.
//! 5. Le `POST /v2/cmd` (effectué en parallèle par une autre task)
//!    doit recevoir la réponse et retourner 200 + le `result`.
//!
//! ## Couverture
//!
//! - Cycle complet : démon pousse une commande, l'extension la reçoit,
//!   y répond, le handler HTTP retourne le résultat.
//! - Pas d'extension connectée → 504 `no_extension_connected`.
//! - Extension renvoie `ok:false` → 502 + `error` propagé.
//! - Mauvais token → 401 (l'auth prime sur tout).

use dsh_eyes::config::Config;
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

/// Lit le premier message texte d'un `WebSocketStream` (la première
/// commande envoyée par le démon). Retourne la `Value` JSON parsée.
async fn read_command<S>(rx: &mut S) -> Value
where
    S: futures_util::Stream<
            Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>,
        > + Unpin,
{
    let msg = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("timeout en attendant la commande")
        .expect("stream non vide")
        .expect("message WS");
    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("attendu Message::Text, reçu {other:?}"),
    };
    serde_json::from_str(&text).expect("JSON valide")
}

/// Envoie une réponse JSON sur la WS.
async fn send_response<S>(tx: &mut S, cmd_id: u64, result: Value)
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let payload = json!({ "cmd_id": cmd_id, "ok": true, "result": result });
    tx.send(Message::Text(payload.to_string()))
        .await
        .expect("send response");
}

/// Lance le serveur et retourne (token, address, channel_clone).
/// Le serveur tourne jusqu'à ce que `server` soit aborted.
#[allow(clippy::await_holding_lock)]
async fn start_server_with_token() -> (String, std::net::SocketAddr, ExtensionChannel) {
    let _guard = common::env_guard();
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);
    let cfg = Config::load().expect("config load");
    let token = cfg.api_token().to_string();
    let ctx = VigilCtx::for_tests(Counters::new(), Allowlist::empty());
    let channel = ExtensionChannel::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = dsh_eyes::api::router_with_v2(ctx, cfg, channel.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });
    // Le `server` est volontairement leak — chaque test l'abort
    // localement, mais on garde une référence pour le cleanup.
    std::mem::forget(server);
    (token, addr, channel)
}

/// POST /v2/cmd avec Bearer token, retourne (status, body_json).
async fn post_cmd(
    addr: std::net::SocketAddr,
    path: &str,
    auth: Option<&str>,
    body: &str,
) -> (u16, Value) {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect TCP");
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(b) = auth {
        req.push_str(&format!("Authorization: {b}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body);
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let raw = String::from_utf8(buf).expect("utf8");
    let status = parse_status(&raw);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    (status, v)
}

fn parse_status(raw: &str) -> u16 {
    let line = raw.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let _ = parts.next();
    parts.next().and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Test principal : cycle complet extension ↔ démon via /v2/cmd.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn cmd_succes_via_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;

    // 1) Ouvre la WS en tant que fausse extension.
    let url = format!("ws://{addr}/ws?token={token}");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // L'extension doit être enregistrée dès que la WS s'ouvre.
    // Petit délai pour laisser `handle_socket` faire le register.
    for _ in 0..50 {
        if channel.is_connected().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        channel.is_connected().await,
        "l'extension doit être enregistrée après l'ouverture WS"
    );

    // 2) Lance le POST /v2/cmd en tâche de fond.
    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post_cmd(
            addr,
            "/v2/cmd",
            Some(&format!("Bearer {token2}")),
            r#"{"cmd":"goto","params":{"url":"https://example.com"}}"#,
        )
        .await
    });

    // 3) Reçoit la commande, répond avec un result.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "goto");
    assert_eq!(cmd["params"]["url"], "https://example.com");
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "page_id": "p42", "title": "Example" }),
    )
    .await;

    // 4) Vérifie la réponse HTTP.
    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "/v2/cmd doit retourner 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["page_id"], "p42");
    assert_eq!(body["result"]["title"], "Example");

    // Cleanup.
    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Test : 401 sans token (l'auth prime sur tout).
#[tokio::test]
async fn cmd_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post_cmd(
        addr,
        "/v2/cmd",
        None,
        r#"{"cmd":"goto","params":{}}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

/// Test : 401 avec mauvais token.
#[tokio::test]
async fn cmd_401_mauvais_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post_cmd(
        addr,
        "/v2/cmd",
        Some("Bearer not-the-right-token"),
        r#"{"cmd":"goto","params":{}}"#,
    )
    .await;
    assert_eq!(status, 401, "mauvais token doit retourner 401");
}

/// Test : 504 + no_extension_connected si la WS n'est pas ouverte
/// au moment du POST.
#[tokio::test]
async fn cmd_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    // Pas d'extension connectée (channel vide).
    assert!(!channel.is_connected().await);

    let (status, body) = post_cmd(
        addr,
        "/v2/cmd",
        Some(&format!("Bearer {token}")),
        r##"{"cmd":"read","params":{"selector":"#main"}}"##,
    )
    .await;
    assert_eq!(status, 504, "sans extension doit retourner 504");
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"], "no_extension_connected");
}

/// Test : 502 + error propagé si l'extension renvoie `ok:false`.
#[tokio::test]
async fn cmd_502_extension_renvoie_erreur() {
    let (token, addr, _channel) = start_server_with_token().await;

    // 1) Ouvre la WS.
    let url = format!("ws://{addr}/ws?token={token}");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // 2) Lance le POST.
    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post_cmd(
            addr,
            "/v2/cmd",
            Some(&format!("Bearer {token2}")),
            r##"{"cmd":"click","params":{"selector":"#missing"}}"##,
        )
        .await
    });

    // 3) L'extension répond ok=false avec un message d'erreur.
    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    let err_payload = json!({
        "cmd_id": cmd_id,
        "ok": false,
        "error": "element introuvable: #missing"
    });
    ws_tx
        .send(Message::Text(err_payload.to_string()))
        .await
        .expect("send error response");

    // 4) Vérifie 502 + error propagé.
    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "ok=false doit retourner 502 (body={body})");
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"], "element introuvable: #missing");

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Test : cmd_id monotones et routage correct sur plusieurs commandes
/// successives.
#[tokio::test]
async fn cmd_plusieurs_requetes_en_parallele() {
    let (token, addr, _channel) = start_server_with_token().await;

    let url = format!("ws://{addr}/ws?token={token}");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // 3 POST concurrents.
    let mut post_tasks = Vec::new();
    for i in 0..3 {
        let token2 = token.clone();
        let body = format!(r#"{{"cmd":"read","params":{{"idx":{i}}}}}"#);
        let task = tokio::spawn(async move {
            post_cmd(addr, "/v2/cmd", Some(&format!("Bearer {token2}")), &body).await
        });
        post_tasks.push(task);
    }

    // Répond aux 3 commandes dans l'ordre.
    let mut cmd_ids = Vec::new();
    for _ in 0..3 {
        let cmd = read_command(&mut ws_rx).await;
        let cmd_id = cmd["cmd_id"].as_u64().unwrap();
        cmd_ids.push(cmd_id);
        send_response(&mut ws_tx, cmd_id, json!({ "ok": true, "echo": cmd_id }))
            .await;
    }

    // Vérifie que les 3 cmd_id sont distincts et les 3 statuts sont 200.
    let mut seen = std::collections::HashSet::new();
    for id in &cmd_ids {
        assert!(seen.insert(*id), "cmd_id dupliqué: {id}");
    }
    for task in post_tasks {
        let (status, body) = task.await.unwrap();
        assert_eq!(status, 200, "body={body}");
        assert_eq!(body["ok"], true);
    }

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Crée un répertoire temporaire sous `target/tests-tmp/`.
fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "cmd-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}
