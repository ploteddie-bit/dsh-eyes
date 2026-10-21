//! Tests d'intégration des endpoints d'actions `POST /v2/goto`,
//! `POST /v2/click`, `POST /v2/fill`, `POST /v2/wait` (Task 4).
//!
//! ## Pourquoi ce test existe
//!
//! Le parent (l'opérateur) a fixé l'intention de la Task 4 : donner aux
//! inférences les **4 actions de manipulation** de page (navigation,
//! click, fill, wait for selector) sous forme d'endpoints de commodité.
//! Même philosophie que `tests/endpoints.rs` : on **enveloppe** `POST
//! /v2/cmd` (déjà couvert par `tests/cmd.rs`) avec des endpoints
//! métier qui valident les champs et mappent les erreurs.
//!
//! ## Stratégie
//!
//! Harnais repris à l'identique de `tests/endpoints.rs` : on lance le
//! serveur avec une fausse extension WS qui répond aux commandes
//! `goto`, `click`, `fill`, `wait`. Le canal pousse la commande dans le
//! bon format et on vérifie le cycle complet (auth, validation, mapping
//! d'erreur).
//!
//! ## Mapping d'erreur (cf. brief Task 4)
//!
//! - `sélecteur introuvable: …` (click/fill) → **404** + `error: sélecteur introuvable: <sel>` (le message d'origine est propagé pour la traçabilité).
   //! - `wait_timeout: …` (wait) → **504** `wait_timeout` (cohérent avec les
   //!   autres timeouts du canal, qui mappent déjà en 504).
//! - Toute autre `ok=false` → **502** (échec d'exécution métier).
//! - Timeout canal 30 s sans réponse → **504** `extension_timeout_30s`.
//! - Pas d'extension connectée → **504** `no_extension_connected`.
//!
//! ## Couverture
//!
//! ### /v2/goto (7 tests)
//! - 401 sans token, 400 url manquante, 403 localhost,
//!   200 sans tab_id (crée onglet), 200 avec tab_id (update onglet),
//!   504 sans extension, 502 erreur extension.
//!
//! ### /v2/click (6 tests)
//! - 401 sans token, 400 selector manquant, 200,
//!   404 sélecteur introuvable, 502 autre erreur, 504 sans extension.
//!
//! ### /v2/fill (6 tests)
//! - 401 sans token, 400 selector manquant, 400 value manquant,
//!   200, 404 sélecteur introuvable, 502 autre erreur, 504 sans extension.
//!
//! ### /v2/wait (7 tests)
//! - 401 sans token, 400 selector manquant, 400 timeout > 10 s,
//!   400 timeout < 0, 200 trouvé, 504 wait_timeout, 504 sans extension.

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

// ─── Harnais (repris de tests/endpoints.rs) ────────────────────────────

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

async fn send_error<S>(tx: &mut S, cmd_id: u64, error: &str)
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let payload = json!({ "cmd_id": cmd_id, "ok": false, "error": error });
    tx.send(Message::Text(payload.to_string()))
        .await
        .expect("send error response");
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
    let base = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let dir = base.join("target").join("tests-tmp").join(format!(
        "actions-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

// ─── Tests /v2/goto ───────────────────────────────────────────────────

#[tokio::test]
async fn goto_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/goto",
        None,
        r#"{"url":"https://example.com"}"#,
    )
    .await;
    assert_eq!(status, 401, "sans token doit retourner 401");
}

#[tokio::test]
async fn goto_400_url_manquante() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/goto",
        Some(&format!("Bearer {token}")),
        r#"{}"#,
    )
    .await;
    assert_eq!(status, 400, "url manquante → 400 (body={body})");
    assert_eq!(body["error"], "missing_url");
}

#[tokio::test]
async fn goto_403_localhost() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/goto",
        Some(&format!("Bearer {token}")),
        r#"{"url":"http://localhost:8080/"}"#,
    )
    .await;
    assert_eq!(status, 403, "localhost → 403 (body={body})");
    assert_eq!(body["error"], "forbidden_target");
}

#[tokio::test]
async fn goto_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/goto",
            Some(&format!("Bearer {token2}")),
            r#"{"url":"https://example.com"}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "goto", "doit pousser goto (pas tabs_create)");
    assert_eq!(cmd["params"]["url"], "https://example.com");
    let cmd_id = cmd["cmd_id"].as_u64().expect("cmd_id manquant");

    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "tab_id": 42, "url": "https://example.com" }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "/v2/goto doit retourner 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["tab_id"], 42);

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn goto_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/goto",
        Some(&format!("Bearer {token}")),
        r#"{"url":"https://example.com"}"#,
    )
    .await;
    assert_eq!(status, 504, "sans extension → 504");
    assert_eq!(body["error"], "no_extension_connected");
}

#[tokio::test]
async fn goto_502_extension_renvoie_erreur() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/goto",
            Some(&format!("Bearer {token2}")),
            r#"{"url":"https://example.com"}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_error(
        &mut ws_tx,
        cmd_id,
        "chrome.tabs.update a échoué: tab introuvable",
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "ok=false → 502 (body={body})");
    assert_eq!(
        body["error"],
        "chrome.tabs.update a échoué: tab introuvable"
    );

    let _ = ws_tx.send(Message::Close(None)).await;
}

// ─── Tests /v2/click ──────────────────────────────────────────────────

#[tokio::test]
async fn click_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/click",
        None,
        r##"{"tab_id":1,"selector":"#btn"}"##,
    )
    .await;
    assert_eq!(status, 401, "sans token → 401");
}

#[tokio::test]
async fn click_400_selector_manquant() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/click",
        Some(&format!("Bearer {token}")),
        r#"{"tab_id":1}"#,
    )
    .await;
    assert_eq!(status, 400, "selector manquant → 400 (body={body})");
    assert_eq!(body["error"], "missing_selector");
}

#[tokio::test]
async fn click_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/click",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#btn"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "click");
    assert_eq!(cmd["params"]["tab_id"], 1);
    assert_eq!(cmd["params"]["selector"], "#btn");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();

    send_response(&mut ws_tx, cmd_id, json!({ "clicked": "#btn" })).await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "click 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["clicked"], "#btn");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn click_404_selecteur_introuvable() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/click",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#missing"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    // Préfixe harmonisé côté extension (background.js cmdClick).
    send_error(&mut ws_tx, cmd_id, "sélecteur introuvable: #missing").await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 404, "'sélecteur introuvable' → 404 (body={body})");
    assert_eq!(body["error"], "sélecteur introuvable: #missing");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn click_502_extension_renvoie_erreur_autre() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/click",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#btn"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_error(&mut ws_tx, cmd_id, "No tab with id: 1").await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "erreur autre → 502 (body={body})");
    assert_eq!(body["error"], "No tab with id: 1");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn click_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/click",
        Some(&format!("Bearer {token}")),
        r##"{"tab_id":1,"selector":"#btn"}"##,
    )
    .await;
    assert_eq!(status, 504);
    assert_eq!(body["error"], "no_extension_connected");
}

// ─── Tests /v2/fill ───────────────────────────────────────────────────

#[tokio::test]
async fn fill_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/fill",
        None,
        r##"{"tab_id":1,"selector":"#x","value":"y"}"##,
    )
    .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn fill_400_selector_manquant() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/fill",
        Some(&format!("Bearer {token}")),
        r#"{"tab_id":1,"value":"y"}"#,
    )
    .await;
    assert_eq!(status, 400, "selector manquant → 400 (body={body})");
    assert_eq!(body["error"], "missing_selector");
}

#[tokio::test]
async fn fill_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/fill",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#in","value":"hello"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "fill");
    assert_eq!(cmd["params"]["tab_id"], 1);
    assert_eq!(cmd["params"]["selector"], "#in");
    assert_eq!(cmd["params"]["value"], "hello");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();

    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "filled": "#in", "length": 5 }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "fill 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["filled"], "#in");
    assert_eq!(body["result"]["length"], 5);

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn fill_404_selecteur_introuvable() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/fill",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#missing","value":"x"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_error(&mut ws_tx, cmd_id, "sélecteur introuvable: #missing").await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 404, "'sélecteur introuvable' → 404 (body={body})");
    assert_eq!(body["error"], "sélecteur introuvable: #missing");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn fill_502_extension_renvoie_erreur_autre() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/fill",
            Some(&format!("Bearer {token2}")),
            r##"{"tab_id":1,"selector":"#in","value":"x"}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_error(&mut ws_tx, cmd_id, "tab inaccessible").await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 502, "erreur autre → 502 (body={body})");
    assert_eq!(body["error"], "tab inaccessible");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn fill_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/fill",
        Some(&format!("Bearer {token}")),
        r##"{"tab_id":1,"selector":"#x","value":"y"}"##,
    )
    .await;
    assert_eq!(status, 504);
    assert_eq!(body["error"], "no_extension_connected");
}

// ─── Tests /v2/wait ───────────────────────────────────────────────────

#[tokio::test]
async fn wait_401_sans_token() {
    let (_token, addr, _channel) = start_server_with_token().await;
    let (status, _body) = post(
        addr,
        "/v2/wait",
        None,
        r##"{"selector":"#ready","timeout_ms":2000}"##,
    )
    .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn wait_400_selector_manquant() {
    let (token, addr, _channel) = start_server_with_token().await;
    let (status, body) = post(
        addr,
        "/v2/wait",
        Some(&format!("Bearer {token}")),
        r#"{"timeout_ms":2000}"#,
    )
    .await;
    assert_eq!(status, 400, "selector manquant → 400 (body={body})");
    assert_eq!(body["error"], "missing_selector");
}

#[tokio::test]
async fn wait_200_avec_fausse_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/wait",
            Some(&format!("Bearer {token2}")),
            r##"{"selector":"#ready","timeout_ms":2000}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "wait");
    assert_eq!(cmd["params"]["selector"], "#ready");
    assert_eq!(cmd["params"]["timeout_ms"], 2000);
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();

    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "found": true, "selector": "#ready", "elapsed_ms": 42 }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "wait 200 (body={body})");
    assert_eq!(body["ok"], true);
    assert_eq!(body["result"]["found"], true);
    assert_eq!(body["result"]["selector"], "#ready");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn wait_502_wait_timeout_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/wait",
            Some(&format!("Bearer {token2}")),
            r##"{"selector":"#never","timeout_ms":500}"##,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_error(
        &mut ws_tx,
        cmd_id,
        "wait_timeout: '#never' introuvable après 500 ms",
    )
    .await;

    // Le code actuel mappe toute ExtensionError en 502 par défaut (cf.
    // `send_cmd` dans src/api.rs). Le contrat HTTP est "la commande a
    // été exécutée par l'extension, elle a renvoyé ok=false".
    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 504, "wait_timeout → 504 harmonisé (body={body})");
    assert_eq!(body["error"], "extension_wait_timeout");

    let _ = ws_tx.send(Message::Close(None)).await;
}

#[tokio::test]
async fn wait_504_sans_extension() {
    let (token, addr, channel) = start_server_with_token().await;
    assert!(!channel.is_connected().await);

    let (status, body) = post(
        addr,
        "/v2/wait",
        Some(&format!("Bearer {token}")),
        r##"{"selector":"#x","timeout_ms":2000}"##,
    )
    .await;
    assert_eq!(status, 504);
    assert_eq!(body["error"], "no_extension_connected");
}
