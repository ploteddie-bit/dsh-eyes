//! Tests d'intégration du solveur de captcha (Task 7).
//!
//! ## Architecture des tests
//!
//! On mocke Ollama avec un mini-serveur `axum` démarré sur un port
//! éphémère. Chaque test configure le comportement attendu :
//!
//! - `tier1_returns_solution` : tier 1 répond `{"response":"ABCD"}`.
//!   Le solveur retourne `solved tier=1`.
//! - `tier1_fails_then_tier2` : tier 1 renvoie 500, tier 2 répond OK.
//!   Le solveur retourne `solved tier=2`.
//! - `both_tiers_fail_returns_human_required` : les deux tiers
//!   renvoient 500. Le solveur retourne `human_required`.
//! - `unsupported_type_returns_unsupported` : l'extension détecte
//!   `recaptcha_v3_invisible`. Le solveur retourne `unsupported`
//!   sans appeler Ollama.
//!
//! ## Harnais
//!
//! Repris à l'identique de `tests/captcha.rs` :
//!
//! - `start_server_with_token`
//! - `open_fake_extension`
//! - helpers WS pour piloter la fausse extension.
//!
//! Pour Ollama, on ajoute `start_mock_ollama` qui binde un axum-router
//! sur un port libre et retourne son URL.
//!
//! ## Pourquoi Config::for_tests_with_ollama
//!
//! `Config::load()` lit `config.toml` sur disque — inacceptable pour
//! des tests où on veut injecter des URLs dynamiques. Le constructeur
//! `for_tests_with_ollama` (cfg test-util, donc visible aux tests
//! d'intégration) permet de pointer tier 1 / tier 2 vers les mocks
//! sans toucher au filesystem.

use axum::{response::IntoResponse as _, routing::post as axum_post, Json, Router};
use dsh_eyes::config::Config;
use dsh_eyes::hands::channel::ExtensionChannel;
use dsh_eyes::solver::OllamaClient;
use dsh_eyes::state::VigilCtx;
use dsh_eyes::stats::Counters;
use dsh_eyes::vigil::Allowlist;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

mod common;

// Sérialisation des tests solver entre eux : ils partagent
// `DSH_EYES_HOME` (variable d'env process-globale), `config.toml`
// (lu par Config::load dans le solve_handler) et le port éphémère.
// Sans sérialisation, deux tests en parallèle se marchent dessus
// (symptôme : tier1 reçu avec la réponse d'un autre test, ou 401).
//
// On utilise un tokio::Mutex statique : les tests `#[tokio::test]`
// sont async, donc un std::sync::Mutex ne bloque pas entre awaits.
// Le premier test qui entre prend le lock, les autres attendent.
async fn solver_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

// ─── Mock Ollama ─────────────────────────────────────────────────────────

/// État partagé du mock Ollama : permet de changer le comportement en
/// cours de test (par ex. tier 1 OK pour le 1er appel puis KO pour le
/// 2e test). Tous les compteurs sont `Atomic*` pour être accessibles
/// depuis le handler `async`.
#[derive(Clone)]
struct MockOllama {
    /// Comportement du tier 1 : "ok" | "error" | "timeout" | "empty" | "missing".
    tier1_behavior: Arc<std::sync::Mutex<String>>,
    /// Comportement du tier 2 : "ok" | "error" | "timeout" | "empty" | "missing".
    tier2_behavior: Arc<std::sync::Mutex<String>>,
    /// Réponse renvoyée par le tier 1 en cas de "ok".
    tier1_response: Arc<std::sync::Mutex<String>>,
    /// Réponse renvoyée par le tier 2 en cas de "ok".
    tier2_response: Arc<std::sync::Mutex<String>>,
    /// Compteur d'appels reçus par tier.
    tier1_calls: Arc<AtomicU16>,
    tier2_calls: Arc<AtomicU16>,
}

impl MockOllama {
    fn new(tier1_behavior: &str, tier2_behavior: &str, response: &str) -> Self {
        // Constructeur de compat : la même réponse pour les deux tiers.
        // Pour avoir des réponses différentes par tier, utilisez
        // `new_split`.
        Self::new_split(tier1_behavior, tier2_behavior, response, response)
    }

    fn new_split(
        tier1_behavior: &str,
        tier2_behavior: &str,
        tier1_response: &str,
        tier2_response: &str,
    ) -> Self {
        Self {
            tier1_behavior: Arc::new(std::sync::Mutex::new(tier1_behavior.to_string())),
            tier2_behavior: Arc::new(std::sync::Mutex::new(tier2_behavior.to_string())),
            tier1_response: Arc::new(std::sync::Mutex::new(tier1_response.to_string())),
            tier2_response: Arc::new(std::sync::Mutex::new(tier2_response.to_string())),
            tier1_calls: Arc::new(AtomicU16::new(0)),
            tier2_calls: Arc::new(AtomicU16::new(0)),
        }
    }

    #[allow(dead_code)]
    fn set_tier1_behavior(&self, b: &str) {
        *self.tier1_behavior.lock().unwrap() = b.to_string();
    }

    #[allow(dead_code)]
    fn set_tier2_behavior(&self, b: &str) {
        *self.tier2_behavior.lock().unwrap() = b.to_string();
    }

    fn tier1_calls(&self) -> u16 {
        self.tier1_calls.load(Ordering::SeqCst)
    }
    fn tier2_calls(&self) -> u16 {
        self.tier2_calls.load(Ordering::SeqCst)
    }
}

/// Démarre un mock Ollama qui répond aux requêtes `/api/generate` selon
/// `state`. Le serveur tourne jusqu'à abort du `JoinHandle`. Retourne
/// `(url_tier1, url_tier2, mock, server_handle)`. On binde deux
/// instances séparées (une par tier) — c'est ce que le code de prod
/// attend : deux URLs distinctes en `ollama_tier1_url` et
/// `ollama_tier2_url`.
async fn start_mock_ollama(state: MockOllama) -> (String, String, MockOllama, tokio::task::JoinHandle<()>) {
    // Tier 1.
    let s1 = state.clone();
    let listener1 = TcpListener::bind("127.0.0.1:0").await.expect("bind tier1");
    let addr1 = listener1.local_addr().expect("addr1");
    let app1 = Router::new().route(
        "/api/generate",
        axum_post(move |Json(_body): Json<Value>| {
            let s = s1.clone();
            async move { handle_generate(s, "tier1").await }
        }),
    );
    let server1 = tokio::spawn(async move {
        axum::serve(listener1, app1).await.expect("serve tier1");
    });

    // Tier 2.
    let s2 = state.clone();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.expect("bind tier2");
    let addr2 = listener2.local_addr().expect("addr2");
    let app2 = Router::new().route(
        "/api/generate",
        axum_post(move |Json(_body): Json<Value>| {
            let s = s2.clone();
            async move { handle_generate(s, "tier2").await }
        }),
    );
    let server2 = tokio::spawn(async move {
        axum::serve(listener2, app2).await.expect("serve tier2");
    });

    // Le retour `server_handle` est un `JoinHandle` unifié qui permet
    // au test d'attendre / cleanup. On fusionne les deux handles par
    // un `JoinSet` minimaliste (Box::pin ne suffit pas, on utilise
    // `tokio::spawn` qui retourne un JoinHandle).
    let merged = tokio::spawn(async move {
        let _ = tokio::join!(server1, server2);
    });

    let url1 = format!("http://{addr1}");
    let url2 = format!("http://{addr2}");
    (url1, url2, state, merged)
}

/// Handler unique pour les deux tiers. Lit le comportement dans
/// `state`, incrémente le compteur, et renvoie la réponse HTTP
/// appropriée.
async fn handle_generate(state: MockOllama, tier: &str) -> axum::response::Response {
    let (behavior, calls, response) = if tier == "tier1" {
        (state.tier1_behavior.clone(), &state.tier1_calls, state.tier1_response.clone())
    } else {
        (state.tier2_behavior.clone(), &state.tier2_calls, state.tier2_response.clone())
    };
    calls.fetch_add(1, Ordering::SeqCst);
    let behavior_str = behavior.lock().unwrap().clone();
    match behavior_str.as_str() {
        "ok" => {
            let resp = response.lock().unwrap().clone();
            axum::Json(json!({ "response": resp, "done": true })).into_response()
        }
        "empty" => axum::Json(json!({ "response": "", "done": true })).into_response(),
        "missing" => axum::Json(json!({ "done": true })).into_response(), // pas de "response"
        _ => {
            // "error" (ou autre) : on renvoie un statut 500.
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "mock error").into_response()
        }
    }
}

// ─── Harnais serveur + fausse extension (repris de tests/captcha.rs) ──

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
async fn start_server_with_urls(
    tier1_url: String,
    tier2_url: String,
) -> (String, SocketAddr, ExtensionChannel, tokio::task::JoinHandle<()>) {
    let _guard = common::env_guard();
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    // Écrit un `config.toml` dans le home de test AVANT le démarrage
    // du serveur. `Config::load()` lit ce fichier dans le solve_handler
    // pour récupérer les URLs Ollama — sans ça, le handler utilise les
    // défauts prod (10.0.0.223) et ne parle jamais à nos mocks.
    let toml = format!(
        r#"ollama_tier1_url = "{tier1_url}"
ollama_tier2_url = "{tier2_url}"
vision_model = "qwen2.5-vl"
"#
    );
    std::fs::write(tmp.join("config.toml"), toml).expect("write config.toml");
    // Écrit aussi le fichier `token` pour que `Config::load` l'utilise
    // au lieu d'en générer un nouveau. Sinon le serveur a un token
    // aléatoire et le test envoie un token connu → 401.
    let token = "a".repeat(64);
    std::fs::write(tmp.join("token"), &token).expect("write token");

    // Token en dur pour les tests : 64 chars hex comme en prod.
    // Écrit ci-dessus dans le fichier `token`. `Config::load` le
    // lira et le réutilisera. Sans ça, le serveur génère un token
    // aléatoire → Bearer client = 401.
    let token = "a".repeat(64);
    // Charge la config qui pointe vers les mocks.
    let cfg = Config::load().expect("Config::load");
    let ctx = VigilCtx::for_tests(Counters::new(), Allowlist::empty());
    let channel = ExtensionChannel::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = dsh_eyes::api::router_with_v2(ctx, cfg, channel.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });
    (token, addr, channel, server)
}

async fn post(
    addr: SocketAddr,
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
    addr: SocketAddr,
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
        "solver-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create tests-tmp dir");
    dir
}

// ─── Tests ───────────────────────────────────────────────────────────────

/// Tier 1 OK → solved tier=1.
#[tokio::test]
async fn solve_tier1_success() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "ABCD");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    // Lance POST /v2/page/solve en tâche de fond.
    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    // L'extension reçoit captcha_detect.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "captcha_detect", "premier appel = détection");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "detected": true,
            "type": "recaptcha_v2_image",
            "selector": "iframe[src*=\"recaptcha\"]",
            "image_b64": "FAKE_BASE64_OF_CAPTCHA"
        }),
    )
    .await;

    // Le solveur appelle Ollama tier 1 directement (HTTP), pas
    // l'extension. Donc la WS ne reçoit plus rien pour ce test.
    // On attend simplement la réponse HTTP.

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "solved");
    assert_eq!(body["type"], "recaptcha_v2_image");
    assert_eq!(body["answer"], "ABCD");
    assert_eq!(body["tier_used"], 1);
    // Tier 1 doit avoir été appelé exactement 1 fois. Tier 2 jamais.
    assert_eq!(mock.tier1_calls(), 1, "tier 1 doit avoir été appelé");
    assert_eq!(mock.tier2_calls(), 0, "tier 2 ne doit pas être appelé");

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Tier 1 KO (500) → fallback tier 2 OK → solved tier=2.
#[tokio::test]
async fn solve_tier1_fails_then_tier2() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("error", "ok", "WXYZ");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
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
            "type": "hcaptcha",
            "selector": "iframe[src*=\"hcaptcha\"]",
            "image_b64": "FAKE"
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "solved");
    assert_eq!(body["type"], "hcaptcha");
    assert_eq!(body["answer"], "WXYZ");
    assert_eq!(body["tier_used"], 2);
    assert_eq!(mock.tier1_calls(), 1);
    assert_eq!(mock.tier2_calls(), 1);

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Les deux tiers KO → human_required.
#[tokio::test]
async fn solve_both_tiers_fail_returns_human_required() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("error", "error", "");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
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
            "type": "slider",
            "selector": ".slider",
            "image_b64": "FAKE"
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "human_required");
    assert_eq!(body["type"], "slider");
    assert!(body.get("answer").is_none() || body["answer"].is_null());
    assert_eq!(body["tier_used"], Value::Null);
    // applied=false pour slider (pas d'application auto).
    assert_eq!(body["applied"], false);
    assert_eq!(mock.tier1_calls(), 1);
    assert_eq!(mock.tier2_calls(), 1);

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Type v3 invisible → unsupported sans appeler Ollama.
#[tokio::test]
async fn solve_v3_invisible_returns_unsupported() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "should_not_be_called");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
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
            "type": "recaptcha_v3_invisible",
            "selector": "iframe[src*=\"recaptcha\"]"
            // Pas d'image_b64 : normal pour v3 invisible.
        }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "unsupported");
    assert_eq!(body["type"], "recaptcha_v3_invisible");
    assert_eq!(mock.tier1_calls(), 0, "Ollama ne doit pas être appelé");
    assert_eq!(mock.tier2_calls(), 0);

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Aucun captcha détecté → none.
#[tokio::test]
async fn solve_no_captcha_returns_none() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    let cmd = read_command(&mut ws_rx).await;
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({ "detected": false, "type": "none" }),
    )
    .await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "none");
    assert_eq!(body["type"], "none");
    assert_eq!(mock.tier1_calls(), 0);
    assert_eq!(mock.tier2_calls(), 0);

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// 401 sans token (l'auth prime sur tout).
#[tokio::test]
async fn solve_401_sans_token() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (_token, addr, _channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (status, _body) = post(
        addr,
        "/v2/page/solve",
        None,
        r#"{"tab_id":42}"#,
    )
    .await;
    assert_eq!(status, 401);
}

/// Type "text" résolu → applied=true après fill + click.
#[tokio::test]
async fn solve_text_applied_via_fill_and_click() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "captcha_answer");
    let (url1, url2, _mock_state, _server_ollama) = start_mock_ollama(mock.clone()).await;
    let (token, addr, channel, _server_dsh) =
        start_server_with_urls(url1, url2).await;
    let (mut ws_rx, mut ws_tx) = open_fake_extension(addr, &channel).await;

    let token2 = token.clone();
    let post_task = tokio::spawn(async move {
        post(
            addr,
            "/v2/page/solve",
            Some(&format!("Bearer {token2}")),
            r#"{"tab_id":42}"#,
        )
        .await
    });

    // 1) Extension reçoit captcha_detect.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "captcha_detect");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(
        &mut ws_tx,
        cmd_id,
        json!({
            "detected": true,
            "type": "text",
            "selector": "input[name*=\"captcha\"]",
            "image_b64": "FAKE_TEXT_IMAGE"
        }),
    )
    .await;

    // 2) Le solveur résout via Ollama, puis appelle `fill` sur
    // l'extension, puis `click` sur un submit.
    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "fill", "post-solve doit appeler fill");
    assert_eq!(cmd["params"]["selector"], "input[name*=\"captcha\"]");
    assert_eq!(cmd["params"]["value"], "captcha_answer");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(&mut ws_tx, cmd_id, json!({ "ok": true })).await;

    let cmd = read_command(&mut ws_rx).await;
    assert_eq!(cmd["cmd"], "click", "post-fill doit tenter click submit");
    let cmd_id = cmd["cmd_id"].as_u64().unwrap();
    send_response(&mut ws_tx, cmd_id, json!({ "ok": true })).await;

    let (status, body) = post_task.await.expect("post_task join");
    assert_eq!(status, 200, "body={body}");
    assert_eq!(body["status"], "solved");
    assert_eq!(body["answer"], "captcha_answer");
    assert_eq!(body["applied"], true);

    let _ = ws_tx.send(Message::Close(None)).await;
}

/// Test unitaire direct sur OllamaClient sans démarrer le serveur du
/// démon : on binde un mock Ollama et on appelle `solve()` directement.
/// Vérifie que la cascade tier1 → tier2 fonctionne en isolation.
#[tokio::test]
async fn ollama_client_solve_cascade() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("ok", "ok", "DIRECT_TEST_ANSWER");
    let (url1, url2, _state, _server) = start_mock_ollama(mock.clone()).await;
    let cfg = Config::for_tests_with_ollama(url1, url2);
    let solver_cfg = cfg.solver_config();
    let client = OllamaClient::new_with_timeout(&solver_cfg, Duration::from_secs(5));
    let (answer, tier) = client.solve("IMAGE_B64_FAKE").await.expect("solve OK");
    assert_eq!(answer, "DIRECT_TEST_ANSWER");
    assert_eq!(tier, 1, "tier 1 doit réussir en premier");
    assert_eq!(mock.tier1_calls(), 1);
    assert_eq!(mock.tier2_calls(), 0);
}

/// Test unitaire : OllamaClient en cascade tier 1 KO → tier 2 OK.
#[tokio::test]
async fn ollama_client_cascade_tier1_fail() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("error", "ok", "TIER2_ANSWER");
    let (url1, url2, _state, _server) = start_mock_ollama(mock.clone()).await;
    let cfg = Config::for_tests_with_ollama(url1, url2);
    let solver_cfg = cfg.solver_config();
    let client = OllamaClient::new_with_timeout(&solver_cfg, Duration::from_secs(5));
    let (answer, tier) = client.solve("IMAGE").await.expect("solve OK après fallback");
    assert_eq!(answer, "TIER2_ANSWER");
    assert_eq!(tier, 2);
    assert_eq!(mock.tier1_calls(), 1);
    assert_eq!(mock.tier2_calls(), 1);
}

/// Test unitaire : OllamaClient les deux KO → erreur.
#[tokio::test]
async fn ollama_client_both_tiers_fail() {
    let _g = solver_lock().await;
    let mock = MockOllama::new("error", "error", "");
    let (url1, url2, _state, _server) = start_mock_ollama(mock.clone()).await;
    let cfg = Config::for_tests_with_ollama(url1, url2);
    let solver_cfg = cfg.solver_config();
    let client = OllamaClient::new_with_timeout(&solver_cfg, Duration::from_secs(5));
    let err = client.solve("IMAGE").await.expect_err("doit échouer");
    assert!(
        err.tier1_err.contains("HTTP status 500") || err.tier1_err.contains("HTTP error"),
        "tier1_err doit mentionner l'erreur HTTP, reçu: {}",
        err.tier1_err
    );
    assert!(
        err.tier2_err.contains("HTTP status 500") || err.tier2_err.contains("HTTP error"),
        "tier2_err doit mentionner l'erreur HTTP, reçu: {}",
        err.tier2_err
    );
    assert_eq!(mock.tier1_calls(), 1);
    assert_eq!(mock.tier2_calls(), 1);
}

/// Test unitaire : réponse Ollama vide → erreur sur ce tier, fallback
/// tier 2. Tier 2 renvoie une réponse valide → solved.
#[tokio::test]
async fn ollama_client_empty_response_is_error() {
    let _g = solver_lock().await;
    // Tier 1 : réponse vide (donc erreur "empty response field").
    // Tier 2 : réponse valide → solved.
    let mock = MockOllama::new_split("empty", "ok", "", "TIER2_VALID");
    let (url1, url2, _state, _server) = start_mock_ollama(mock.clone()).await;
    let cfg = Config::for_tests_with_ollama(url1, url2);
    let solver_cfg = cfg.solver_config();
    let client = OllamaClient::new_with_timeout(&solver_cfg, Duration::from_secs(5));
    let (answer, tier) = client.solve("IMAGE").await.expect("doit tomber sur tier 2");
    assert_eq!(answer, "TIER2_VALID");
    assert_eq!(tier, 2);
}