//! Tests d'intégration des endpoints de commodité `POST /v2/open` et
//! `POST /v2/read` (Task 3, REWORK 2026-10-05 — modèle webbridge).
//!
//! ## Pourquoi ce test existe
//!
//! Le parent (Eddie) a fixé l'intention de la Task 3 : donner aux
//! inférences les **2 fondamentaux « ouvrir une page » et « lire le
//! contenu »**. On ne duplique pas la logique du canal — on
//! enveloppe `POST /v2/cmd` (déjà couvert par `tests/cmd.rs`) avec deux
//! endpoints métier qui valident l'URL d'entrée.
//!
//! ## Stratégie
//!
//! On réutilise **exactement** le harnais de `tests/cmd.rs` : on lance
//! le serveur avec une fausse extension WS qui répond aux commandes
//! `tabs_create` (réponse `{"tab_id":N}`) et `read` (réponse
//! `{"text":"…"}`). Ça évite tout couplage avec un vrai Chrome et
//! garantit que les routes `/v2/open` et `/v2/read` poussent la bonne
//! commande dans le bon format.
//!
//! ## Couverture
//!
//! - 401 sans token (l'auth prime sur tout)
//! - 401 mauvais token
//! - 403 sur URL localhost
//! - 403 sur IP privée (RFC1918)
//! - 403 sur IP loopback v4
//! - 200 + `{"tab_id":N}` quand la fausse extension répond à `tabs_create`
//! - 200 + `{"text":"…"}` quand la fausse extension répond à `read`
//! - 504 quand aucune extension n'est connectée
//! - 502 quand l'extension renvoie `ok:false`

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
/// commande envoyée par le démon). Voir `tests/cmd.rs` pour la version
/// canonique — dupliquée ici pour garder les tests isolés.
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

/// Envoie une réponse JSON sur la WS — voir `tests/cmd.rs`.
async fn send_response<S>(tx: &mut S, cmd_id: u64, result: Value)
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let payload = json!({ "cmd_id": cmd_id, "ok": true, "result": result });
    tx.send(Message::Text(payload.to_string()))
        .await
        .expect("send response");
}

/// Lance le serveur (harnais identique à `tests/cmd.rs`).
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
    std::mem::forget(server);
    (token, addr, channel)
}

/// POST HTTP/1.1 minimal sur TCP (même helper que `tests/cmd.rs`).
async fn post(
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

/// Ouvre la fausse extension et attend son enregistrement côté serveur.
async fn open_fake_extension(
    addr: std::net::SocketAddr,
    channel: &ExtensionChannel,
) -> (
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
) {
    let url = format!("ws://{addr}/ws");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");
    let (tx, rx) = stream.split();
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
    (rx, tx)
}

/// Crée un répertoire temporaire sous `target/tests-tmp/` (idem cmd.rs).
fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "endpoints-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

// ─── Tests /v2/open ─────────────────────────────────────────────────────

#[tokio::test]
async fn open_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/open",
        None,
        r#"{"url":"https://example.com"}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

#[tokio::test]
async fn open_401_mauvais_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/open",
        Some("Bearer not-the-right-token"),
        r#"{"url":"https://example.com"}"#,
    )
    .await;
    assert_eq!(status, 401, "mauvais token doit retourner 401");
}

#[tokio::test]
async fn open_403_localhost() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://localhost:8080/"}"#,
    )
    .await;
    assert_eq!(status, 403, "localhost doit retourner 403 (body={body})");
    assert_eq!(body["error"], "forbidden_target");
}

#[tokio::test]
async fn open_403_ip_loopback_v4() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://127.0.0.1:8445/ws"}"#,
    )
    .await;
    assert_eq!(status, 403, "127.0.0.1 doit retourner 403 (body={body})");
    assert_eq!(body["error"], "forbidden_target");
}

#[tokio::test]
async fn open_403_ip_privee_rfc1918() {
    let (token, addr, _channel) = start_server_with_token().await;
    // 10.0.0.0/8
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://10.0.0.5/"}"#,
    )
    .await;
    assert_eq!(status, 403, "10.0.0.5 doit retourner 403 (body={body})");
    assert_eq!(body["error"], "forbidden_target");

    // 192.168.0.0/16
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://192.168.1.1/"}"#,
    )
    .await;
    assert_eq!(status, 403, "192.168.1.1 doit retourner 403 (body={body})");

    // 172.16.0.0/12
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://172.16.0.1/"}"#,
    )
    .await;
    assert_eq!(status, 403, "172.16.0.1 doit retourner 403 (body={body})");
}

#[tokio::test]
async fn open_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/open",
            Some(&format!("Bearer {token2}")),
            r#"{"url":"https://example.com"}"#,
        )
        .await
    });

    // La fausse extension doit recevoir une commande `tabs_create` avec
    // l'URL passée.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "tabs_create", "doit pousser tabs_create");
    assert_eq!(cmd["params"]["url"], "https://example.com");
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    send_response(&mut ws_tx, cmd_id, json!({ "tab_id": 42 })).await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "/v2/open doit retourner 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["tab_id"], 42);

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn open_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{"url":"https://example.com"}"#,
    )
    .await;
    assert_eq!(status, 504, "sans extension doit retourner 504");
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"], "no_extension_connected");
}

#[tokio::test]
async fn open_502_extension_renvoie_erreur() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/open",
            Some(&format!("Bearer {token2}")),
            r#"{"url":"https://example.com"}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    let err_payload = json!({
        "cmd_id": cmd_id,
        "ok": false,
        "error": "chrome.tabs.create a échoué: ERR_INVALID_URL"
    });
    ws_tx
        .send(Message::Text(err_payload.to_string()))
        .await
        .expect("send error response");

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "ok=false doit retourner 502 (body={body})");
    assert_eq!(body["ok"], false);
    assert_eq!(
        body["error"],
        "chrome.tabs.create a échoué: ERR_INVALID_URL"
    );

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn open_400_url_manquante() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/open",
        Some(&format!("Bearer {token}")),
        r#"{}"#,
    )
    .await;
    assert_eq!(status, 400, "url manquante → 400 (body={body})");
    assert_eq!(body["error"], "missing_url");
}

// ─── Tests /v2/read ─────────────────────────────────────────────────────

#[tokio::test]
async fn read_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/read",
        None,
        r#"{"tab_id":42}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

#[tokio::test]
async fn read_400_tab_id_manquant() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/read",
        Some(&format!("Bearer {token}")),
        r#"{}"#,
    )
    .await;
    assert_eq!(status, 400, "tab_id manquant → 400 (body={body})");
    assert_eq!(body["error"], "missing_tab_id");
}

#[tokio::test]
async fn read_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/read",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    // La fausse extension reçoit une commande `read` avec le bon tab_id.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "read", "doit pousser read");
    assert_eq!(cmd["params"]["tab_id"], 42);
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "text": "hello dsh\n[tronqué à 200 Ko]" }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "/v2/read doit retourner 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["text"], "hello dsh\n[tronqué à 200 Ko]");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn read_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/read",
        Some(&format!("Bearer {token}")),
        r#"{"tab_id":42}"#,
    )
    .await;
    assert_eq!(status, 504, "sans extension doit retourner 504");
    assert_eq!(body["error"], "no_extension_connected");
}

#[tokio::test]
async fn read_502_extension_renvoie_erreur() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/read",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    let err_payload = json!({
        "cmd_id": cmd_id,
        "ok": false,
        "error": "No tab with id: 42"
    });
    ws_tx
        .send(Message::Text(err_payload.to_string()))
        .await
        .expect("send error response");

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502);
    assert_eq!(body["error"], "No tab with id: 42");

    let _ = ws_tx.send(Message::Close(None)).await;
}
/// Task 5 — toute action v2 est journalisée dans `actions.jsonl`.
#[tokio::test]
async fn open_journalise_dans_actions_jsonl() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/open",
            Some(&format!("Bearer {token2}")),
            r#"{"url":"https://example.com"}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");
    send_response(&mut ws_tx, cmd_id, json!({ "tab_id": 7 })).await;

    let (status, _body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200);

    // Le démon doit avoir journalisé l'action (log_action → actions.jsonl
    // dans $DSH_EYES_HOME, posé par le harnais).
    let home = std::env::var("DSH_EYES_HOME").expect("DSH_EYES_HOME");
    let path = std::path::Path::new(&home).join("actions.jsonl");
    let mut attempts = 0u8;
    let body = loop {
        attempts += 1;
        if let Ok(b) = std::fs::read_to_string(&path) {
            if !b.is_empty() {
                break b;
            }
        }
        assert!(attempts <= 50, "actions.jsonl absent après 50 tentatives");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    // Le fichier peut contenir des actions d'autres tests tournant en
    // parallèle (read_*, etc.) — on cherche UNE ligne action=="open".
    let found_open = body
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .any(|v| v["action"] == "open");
    assert!(
        found_open,
        "aucune ligne action=open dans actions.jsonl (body={body})"
    );

    let _ = ws_tx.send(Message::Close(None)).await;
}
