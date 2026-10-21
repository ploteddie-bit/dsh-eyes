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

use dsh_eyes::hands::channel::ExtensionChannel;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

mod common;

/// Petit timeout global pour éviter qu'un bug ne fasse tourner le test à l'infini.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
// Le MutexGuard couvre tout le test (qui contient des `await` partout) : c'est
// volontaire, on veut sérialiser tout le test pour ne pas marcher sur DSH_EYES_HOME.
#[allow(clippy::await_holding_lock)]
async fn ws_accepte_un_evenement_valide() {
    // Sérialise l'accès à DSH_EYES_HOME (env partagé du process de test).
    let _guard = common::env_guard();

    // Dossier temporaire pour la persistance. On positionne la variable d'env AVANT
    // de binder le listener, pour que `ingest::ws_handler` lise la bonne valeur.
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    // Bind sur port éphémère (0).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    // Construit le router complet (avec /health, /ws, /stats, /events) et
    // spawn le serveur. Task 4 : `api::router` prend un `VigilCtx` agrégé.
    // On n'a pas besoin d'allowlist / session pour ce test — valeurs par défaut.
    // REWORK 2026-10-05 : `api::router` prend aussi un `ExtensionChannel`
    // (extension bidirectionnelle) — on en passe un vide.
    let app = dsh_eyes::api::router(
        dsh_eyes::state::VigilCtx::new(),
        ExtensionChannel::new(),
    );
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

/// Lot A — D1 : bufferisation de `append_event` (EventWriter).
///
/// Test discriminant en deux phases :
/// 1. **Bufferisation observable** : envoyer 5 événements (< seuil
///    de 10 lignes, < 500 ms d'idle) puis attendre 200 ms. Le
///    fichier DOIT être VIDE : la bufferisation est en cours et
///    aucun flush (ni périodique 10 lignes, ni idle 500 ms) n'a
///    été déclenché. Avec l'ancienne impl (OpenOptions + flush
///    par event), les 5 lignes seraient déjà sur disque.
/// 2. **Garantie de persistance** : fermer la WS, attendre le
///    flush final propre (drop sender → canal fermé → writer
///    termine avec flush), puis vérifier que les 5 événements
///    sont bien tous présents dans le fichier.
///
/// Avec l'EventWriter (Lot A), les deux phases passent. Avec
/// l'ancienne impl, la phase 1 échoue (fichier non vide).
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn ws_bufferise_5_evenements_observables_et_persistes() {
    let _guard = common::env_guard();

    // Dossier temporaire pour la persistance — isolé du test 1
    // (env_guard ci-dessus assure la sérialisation).
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0");
    let addr = listener.local_addr().expect("local_addr");

    let app = dsh_eyes::api::router(
        dsh_eyes::state::VigilCtx::new(),
        ExtensionChannel::new(),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    let url = format!("ws://{}/ws", addr);
    let (stream, _response) =
        tokio_tungstenite::connect_async(&url).await.expect("connect_async WS");
    let (mut ws_tx, mut ws_rx) = stream.split();

    // Envoie 5 événements (< 10 lignes, < 500 ms). On consomme les
    // 5 acks pour garantir qu'ils ont atteint la `event_writer`
    // (sinon l'ack partirait avant que l'event soit dans le mpsc).
    const N: usize = 5;
    for i in 0..N {
        let evt = json!({
            "ts": i as u64,
            "domain": format!("example{}.com", i),
            "url": format!("https://example{}.com/", i),
            "kind": "request",
            "bytes": i as u64,
        });
        ws_tx
            .send(Message::Text(evt.to_string()))
            .await
            .expect("send text");
    }
    for i in 0..N {
        let reply_msg = tokio::time::timeout(TEST_TIMEOUT, ws_rx.next())
            .await
            .unwrap_or_else(|_| panic!("ack {i} timeout"))
            .expect("stream non vide")
            .expect("message WS");
        match reply_msg {
            Message::Text(t) => assert_eq!(t.to_string(), r#"{"ok":true}"#, "ack {i}"),
            other => panic!("réponse inattendue ack {i}: {other:?}"),
        }
    }

    // Phase 1 — bufferisation observable : 200 ms < 500 ms (idle
    // non déclenché) et N=5 < 10 (seuil périodique non déclenché).
    // Le fichier DOIT être vide : la bufferisation est en cours.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let jsonl_path = tmp.join("events.jsonl");
    let body_phase1 = std::fs::read_to_string(&jsonl_path).unwrap_or_default();
    let count_phase1 = body_phase1.lines().filter(|l| !l.is_empty()).count();
    assert_eq!(
        count_phase1, 0,
        "EventWriter doit bufferiser (attendu 0 ligne à t+200ms, \
         observé {count_phase1}). L'implémentation flush probablement \
         à chaque event."
    );

    // Ferme proprement la WS → le serveur sort de la boucle, drop
    // la sender de l'EventWriter, la task écrivain fait son flush
    // final puis exit.
    let _ = ws_tx.send(Message::Close(None)).await;

    // Attente : WS fermée → drop sender → canal fermé côté écrivain
    // → flush final. Marge 1,5 s (500 ms flush + cycle + marge OS).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Phase 2 — garantie de persistance : tous les events sont
    // là après la fermeture (0 event perdu).
    let body = std::fs::read_to_string(&jsonl_path).expect("lecture events.jsonl");
    let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        lines.len(),
        N,
        "attendu {N} lignes après flush final, lu {} :\n{}",
        lines.len(),
        body
    );

    // Vérifie le contenu (chaque ligne a un domain unique).
    let mut domains = std::collections::HashSet::new();
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).expect("JSONL valide");
        let d = v["domain"].as_str().expect("domain string").to_string();
        domains.insert(d);
    }
    assert_eq!(domains.len(), N, "tous les domains doivent être uniques");

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

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn ws_ping_repond_pong() {
    let _guard = common::env_guard();
    let tmp = tempdir_in_tests_target();
    std::env::set_var("DSH_EYES_HOME", &tmp);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = dsh_eyes::api::router(
        dsh_eyes::state::VigilCtx::new(),
        ExtensionChannel::new(),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum serve");
    });

    let url = format!("ws://{}/ws", addr);
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("connect_async WS");
    let (mut ws_tx, mut ws_rx) = stream.split();

    ws_tx
        .send(Message::Text(r#"{"ping":123}"#.to_string()))
        .await
        .expect("send ping");

    let reply = tokio::time::timeout(TEST_TIMEOUT, ws_rx.next())
        .await
        .expect("réponse dans le délai")
        .expect("stream non vide")
        .expect("message WS");
    let text = match reply {
        Message::Text(t) => t.to_string(),
        other => panic!("réponse inattendue: {other:?}"),
    };
    let v: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert_eq!(v["pong"], 123);

    let _ = ws_tx.send(Message::Close(None)).await;
    server.abort();
}