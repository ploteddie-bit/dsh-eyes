//! Tests d'intégration des endpoints `POST /v2/page/screenshot` et
//! `POST /v2/page/captcha` (Task 6 — modèle webbridge).
//!
//! ## Pourquoi ce test existe
//!
//! Le parent (l'opérateur) a fixé l'intention de la Task 6 : ajouter deux
//! endpoints qui permettent à une inférence de (a) capturer le viewport
//! d'un onglet et (b) détecter la présence d'un captcha sur la page.
//! Comme pour les Tasks 3-4-5, on **enveloppe** `POST /v2/cmd` (déjà
//! couvert par `tests/cmd.rs`) avec des endpoints de commodité qui
//! valident les champs et mappent les erreurs.
//!
//! ## Stratégie
//!
//! Harnais repris à l'identique de `tests/endpoints.rs` / `tests/actions.rs`
//! : on lance le serveur avec une fausse extension WS qui répond aux
//! commandes `screenshot` et `captcha_detect`. Pour `screenshot`, on
//! répond avec un dataURL factice (`"data:image/png;base64,FAKE…"`).
//! Pour `captcha_detect`, on répond avec les formes de la spec : aucun
//! captcha, recaptcha iframe, etc.
//!
//! ## Mapping d'erreur (cf. brief Task 6)
//!
//! - Format invalide (≠ "png"/"jpeg") → **400** `invalid_format: '…'`.
//! - `ok=false` extension → **502** + msg propagé.
//! - Timeout canal 30 s → **504** `extension_timeout_30s`.
//! - Pas d'extension connectée → **504** `no_extension_connected`.
//!
//! ## Couverture
//!
//! ### /v2/page/screenshot (5 tests)
//! - 401 sans token
//! - 400 format invalide
//! - 200 avec fausse extension (dataURL → b64 extrait, préfixe MIME strippé)
//! - 504 sans extension
//! - 502 erreur extension (dataURL absent)
//!
//! ### /v2/page/captcha (4 tests)
//! - 401 sans token
//! - 200 aucun captcha détecté
//! - 200 recaptcha_v2_image détecté (avec image_b64 factice)
//! - 504 sans extension

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

// ── Helpers (repris de `tests/endpoints.rs` / `tests/actions.rs`) ─────

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

async fn send_response<S>(tx: &mut S, cmd_id: u64, result: Value)
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let payload = json!({ "cmd_id": cmd_id, "ok": true, "result": result });
    tx.send(Message::Text(payload.to_string()))
        .await
        .expect("send response");
}

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

fn tempdir_in_tests_target() -> std::path::PathBuf {
    let base = std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "captcha-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

// ─── Tests /v2/page/screenshot ──────────────────────────────────────────

#[tokio::test]
async fn screenshot_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/page/screenshot",
        None,
        r#"{"tab_id":42}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

#[tokio::test]
async fn screenshot_400_format_invalide() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/page/screenshot",
        Some(&format!("Bearer {token}")),
        r#"{"format":"gif"}"#,
    )
    .await;
    assert_eq!(status, 400, "format invalide → 400 (body={body})");
    assert!(
        body["error"].as_str().unwrap_or("").starts_with("invalid_format"),
        "le message doit commencer par 'invalid_format', reçu: {}",
        body["error"]
    );
}

#[tokio::test]
async fn screenshot_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/screenshot",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    // La fausse extension reçoit la commande `screenshot` avec tab_id.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "screenshot", "doit pousser screenshot");
    assert_eq!(cmd["params"]["tab_id"], 42);
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    // Réponse : dataURL factice. Le serveur doit extraire la partie
    // après la virgule.
    let fake_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "format": "png",
            "data_url": format!("data:image/png;base64,{fake_b64}"),
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "/v2/page/screenshot doit retourner 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["format"], "png");
    assert_eq!(body["b64"], fake_b64, "le serveur doit stripper le préfixe MIME");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn screenshot_200_sans_format() {
    // Pas de `format` dans la requête : le serveur doit accepter (défaut
    // côté extension = "png") et le `b64` doit être extrait.
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/screenshot",
            Some(&format!("Bearer {token2}")),
            r#"{}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "screenshot");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "format": "png",
            "data_url": "data:image/png;base64,ABCD",
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["b64"], "ABCD", "préfixe MIME strippé");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn screenshot_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/page/screenshot",
        Some(&format!("Bearer {token}")),
        r#"{}"#,
    )
    .await;
    assert_eq!(status, 504, "sans extension doit retourner 504");
    assert_eq!(body["error"], "no_extension_connected");
}

#[tokio::test]
async fn screenshot_502_extension_renvoie_erreur() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/screenshot",
            Some(&format!("Bearer {token2}")),
            r#"{}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    let err_payload = json!({
        "cmd_id": cmd_id,
        "ok": false,
        "error": "captureVisibleTab a échoué: tab inactif"
    });
    ws_tx
        .send(Message::Text(err_payload.to_string()))
        .await
        .expect("send error response");

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "ok=false → 502");
    assert_eq!(body["error"], "captureVisibleTab a échoué: tab inactif");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn screenshot_502_data_url_manquant() {
    // L'extension renvoie ok=true mais sans data_url (forme inattendue).
    // On s'attend à une 502 dédiée, pas une 200 avec b64=null.
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/screenshot",
            Some(&format!("Bearer {token2}")),
            r#"{}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(&mut ws_tx, cmd_id, json!({ "format": "png" })).await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "data_url manquant → 502");
    assert_eq!(body["error"], "extension_returned_unexpected_shape");

    let _ = ws_tx.send(Message::Close(None)).await;
}

// ─── Tests /v2/page/captcha ─────────────────────────────────────────────

#[tokio::test]
async fn captcha_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/page/captcha",
        None,
        r#"{"tab_id":42}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

#[tokio::test]
async fn captcha_200_aucun_detecte() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/captcha",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "captcha_detect", "doit pousser captcha_detect");
    assert_eq!(cmd["params"]["tab_id"], 42);
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    // Réponse : pas de captcha.
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "detected": false, "type": "none" }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["detected"], false);
    assert_eq!(body["type"], "none");
    // Pas de selector ni image_b64 quand rien n'est détecté.
    assert!(body.get("selector").is_none() || body["selector"].is_null());
    assert!(body.get("image_b64").is_none() || body["image_b64"].is_null());

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn captcha_200_recaptcha_v2_image_detecte() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/captcha",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "captcha_detect");
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    // Réponse : reCAPTCHA v2 image détecté + image_b64 (capture viewport).
    let fake_b64 = "iVBORw0KGgoAAAA==";
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "detected": true,
            "type": "recaptcha_v2_image",
            "selector": "iframe[src*=\"recaptcha\"]",
            "image_b64": fake_b64
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["detected"], true);
    assert_eq!(body["type"], "recaptcha_v2_image");
    assert_eq!(body["selector"], "iframe[src*=\"recaptcha\"]");
    assert_eq!(body["image_b64"], fake_b64);

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn captcha_200_recaptcha_v2_invisible() {
    // Type différent : reCAPTCHA invisible (pas d'image_b64 attendu).
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/captcha",
            Some(&format!("Bearer {token2}")),
            r#"{}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "detected": true,
            "type": "recaptcha_v2_invisible",
            "selector": "iframe[src*=\"recaptcha\"]"
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["type"], "recaptcha_v2_invisible");
    assert_eq!(body["detected"], true);
    // Pas d'image_b64 en mode invisible.
    assert!(body.get("image_b64").is_none() || body["image_b64"].is_null());

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn captcha_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/page/captcha",
        Some(&format!("Bearer {token}")),
        r#"{}"#,
    )
    .await;
    assert_eq!(status, 504, "sans extension doit retourner 504");
    assert_eq!(body["error"], "no_extension_connected");
}

#[tokio::test]
async fn captcha_502_extension_renvoie_erreur() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/captcha",
            Some(&format!("Bearer {token2}")),
            r#"{}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    let err_payload = json!({
        "cmd_id": cmd_id,
        "ok": false,
        "error": "executeScript a échoué: No tab with id: 42"
    });
    ws_tx
        .send(Message::Text(err_payload.to_string()))
        .await
        .expect("send error response");

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "ok=false → 502");
    assert_eq!(body["error"], "executeScript a échoué: No tab with id: 42");

    let _ = ws_tx.send(Message::Close(None)).await;
}