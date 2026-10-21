//! Enregistrement d'écran piloté par le démon (v0.6.1).
//!
//! Le démon tourne sous WSL ; la capture doit être faite par un processus
//! Windows (mss). On lance `rec_ecran_cli.py` via l'interop WSL, on garde
//! le `Child` et on arrête par fichier sentinelle — l'enregistrement peut
//! donc durer plusieurs minutes sans garder de socket ouverte.

use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

/// Python du venv dédié. Chemin **WSL** : le démon est un processus Linux,
/// l'interop traduit l'exécution vers Windows. Les arguments, eux, restent
/// des chemins Windows compris par ce Python.
const DEFAULT_PYTHON: &str =
    "/mnt/c/Users/eddie/Documents/Mon Enregistreur/venv-rec/Scripts/python.exe";
/// CLI déployé côté Windows (argument, donc chemin Windows).
const DEFAULT_SCRIPT: &str = r"C:\Users\eddie\Documents\Mon Enregistreur\rec_ecran_cli.py";

#[derive(Clone, Default)]
pub struct RecordState {
    inner: Arc<Mutex<Option<Session>>>,
}

struct Session {
    child: Child,
    stop_path: PathBuf,
    out_path: String,
    started: Instant,
}

#[derive(Deserialize, Default)]
pub struct StartReq {
    /// Chemin Windows du MP4. Défaut : `C:\Users\eddie\Videos\dsh-record-<ts>.mp4`.
    pub out: Option<String>,
    pub fps: Option<u32>,
    pub resolution: Option<String>,
    pub duration: Option<f64>,
    pub monitor: Option<i32>,
    pub cursor: Option<bool>,
    pub python: Option<String>,
    pub script: Option<String>,
}

fn jeton() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}-{}", now, std::process::id())
}

/// `C:\Users\...` → `/mnt/c/Users/...` (le démon est sous WSL).
fn win_to_wsl(p: &str) -> String {
    let s = p.replace('\\', "/");
    match s.strip_prefix("C:/") {
        Some(rest) => format!("/mnt/c/{rest}"),
        None => s,
    }
}

fn default_out() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!(r"C:\Users\eddie\Videos\dsh-record-{now}.mp4")
}

fn erreur(code: StatusCode, msg: impl Into<String>) -> Response {
    (
        code,
        Json(json!({"ok": false, "error": msg.into()})),
    )
        .into_response()
}

/// `POST /v2/record/start` — lance un enregistrement.
pub async fn start(
    Extension(state): Extension<RecordState>,
    Json(req): Json<StartReq>,
) -> Response {
    let mut guard = state.inner.lock().await;
    if guard.is_some() {
        return erreur(StatusCode::CONFLICT, "enregistrement déjà en cours");
    }

    let out = req.out.unwrap_or_else(default_out);
    let stop_win = format!(
        r"C:\Users\eddie\AppData\Local\Temp\dsh-record-stop-{}.flag",
        jeton()
    );
    let stop_wsl = win_to_wsl(&stop_win);
    let _ = std::fs::remove_file(&stop_wsl);

    let python = req.python.unwrap_or_else(|| DEFAULT_PYTHON.to_string());
    let script = req.script.unwrap_or_else(|| DEFAULT_SCRIPT.to_string());

    let mut cmd = Command::new(&python);
    cmd.arg(&script)
        .arg("--out")
        .arg(&out)
        .arg("--fps")
        .arg(req.fps.unwrap_or(20).to_string())
        .arg("--resolution")
        .arg(
            req.resolution
                .unwrap_or_else(|| "720p".to_string()),
        )
        .arg("--stop-file")
        .arg(&stop_win);
    if let Some(m) = req.monitor {
        cmd.arg("--monitor").arg(m.to_string());
    }
    if req.cursor.unwrap_or(false) {
        cmd.arg("--cursor");
    }
    if let Some(d) = req.duration {
        cmd.arg("--duration").arg(d.to_string());
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    match cmd.spawn() {
        Ok(child) => {
            *guard = Some(Session {
                child,
                stop_path: PathBuf::from(stop_wsl),
                out_path: out.clone(),
                started: Instant::now(),
            });
            (
                StatusCode::OK,
                Json(json!({"ok": true, "out": out, "stop_file": stop_win})),
            )
                .into_response()
        }
        Err(e) => erreur(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("lancement impossible: {e}"),
        ),
    }
}

/// `POST /v2/record/stop` — arrête et renvoie le résultat JSON du CLI.
pub async fn stop(Extension(state): Extension<RecordState>) -> Response {
    let session = { state.inner.lock().await.take() };
    let Some(session) = session else {
        return erreur(StatusCode::NOT_FOUND, "aucun enregistrement en cours");
    };

    let _ = tokio::fs::write(&session.stop_path, b"stop").await;
    let elapsed = session.started.elapsed().as_secs_f64();
    let out_path = session.out_path.clone();

    match session.child.wait_with_output().await {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let dernier = stdout.lines().last().unwrap_or("").trim().to_string();
            let parsed: serde_json::Value =
                serde_json::from_str(&dernier).unwrap_or(serde_json::Value::Null);
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let _ = tokio::fs::remove_file(&session.stop_path).await;
            (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "out": out_path,
                    "elapsed_s": elapsed,
                    "result": parsed,
                    "stderr": stderr,
                })),
            )
                .into_response()
        }
        Err(e) => erreur(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("attente du processus impossible: {e}"),
        ),
    }
}

/// `GET /v2/record/status` — état courant.
pub async fn status(Extension(state): Extension<RecordState>) -> Response {
    let guard = state.inner.lock().await;
    match guard.as_ref() {
        Some(s) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "recording": true,
                "out": s.out_path,
                "elapsed_s": s.started.elapsed().as_secs_f64(),
            })),
        )
            .into_response(),
        None => (
            StatusCode::OK,
            Json(json!({"ok": true, "recording": false})),
        )
            .into_response(),
    }
}
