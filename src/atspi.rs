use axum::{
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

use crate::api::{error_response, log_action};

const DELAI_ATSPI: Duration = Duration::from_secs(5);
const TAILLE_SORTIE_MAX: usize = 4 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub struct LireRequest {
    pub application: String,
    pub texte: Option<String>,
    pub limite: Option<usize>,
}

struct ErreurAtspi(StatusCode, &'static str);

impl ErreurAtspi {
    fn reponse(self) -> Response {
        error_response(self.0, self.1)
    }
}

fn chemin_lecteur() -> Result<PathBuf, String> {
    if let Some(chemin) = std::env::var_os("DSH_EYES_ATSPI_BIN") {
        return Ok(PathBuf::from(chemin));
    }
    let executable = std::env::current_exe().map_err(|e| e.to_string())?;
    let parent = executable.parent().ok_or("Parent du démon introuvable")?;
    Ok(parent.join("atspi-lire"))
}

fn valider(req: &LireRequest) -> Result<(String, Option<String>, usize), ErreurAtspi> {
    let application = req.application.trim();
    if application.is_empty()
        || application.chars().count() > 200
        || req.texte.as_ref().is_some_and(|texte| texte.chars().count() > 200)
    {
        return Err(ErreurAtspi(StatusCode::BAD_REQUEST, "invalid_atspi_filter"));
    }
    let limite = req.limite.unwrap_or(200);
    if limite == 0 || limite > 500 {
        return Err(ErreurAtspi(StatusCode::BAD_REQUEST, "invalid_atspi_limit"));
    }
    Ok((
        application.to_string(),
        req.texte.clone(),
        limite,
    ))
}

async fn executer_lecteur(
    application: &str,
    texte: Option<&str>,
    limite: usize,
) -> Result<Value, ErreurAtspi> {
    let chemin = chemin_lecteur().map_err(|_| {
        ErreurAtspi(StatusCode::INTERNAL_SERVER_ERROR, "atspi_reader_path")
    })?;
    let mut commande = Command::new(chemin);
    commande
        .arg("--application")
        .arg(application)
        .arg("--limite")
        .arg(limite.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(texte) = texte {
        commande.arg("--texte").arg(texte);
    }
    let sortie = match timeout(DELAI_ATSPI, commande.output()).await {
        Ok(Ok(sortie)) => sortie,
        Ok(Err(erreur)) => {
            tracing::warn!(%erreur, "impossible de lancer le lecteur AT-SPI");
            return Err(ErreurAtspi(
                StatusCode::BAD_GATEWAY,
                "atspi_reader_unavailable",
            ));
        }
        Err(_) => {
            tracing::warn!("lecteur AT-SPI expiré");
            return Err(ErreurAtspi(StatusCode::GATEWAY_TIMEOUT, "atspi_timeout"));
        }
    };
    if !sortie.status.success() {
        tracing::warn!(status = %sortie.status, "échec du lecteur AT-SPI");
        return Err(ErreurAtspi(StatusCode::BAD_GATEWAY, "atspi_reader_failed"));
    }
    if sortie.stdout.len() > TAILLE_SORTIE_MAX {
        tracing::warn!(taille = sortie.stdout.len(), "sortie AT-SPI trop volumineuse");
        return Err(ErreurAtspi(
            StatusCode::BAD_GATEWAY,
            "atspi_output_too_large",
        ));
    }
    serde_json::from_slice(&sortie.stdout)
        .map_err(|_| ErreurAtspi(StatusCode::BAD_GATEWAY, "atspi_invalid_output"))
}

pub async fn lire_handler(body: Result<Json<LireRequest>, JsonRejection>) -> Response {
    let req = match body {
        Ok(Json(req)) => req,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_body"),
    };
    let (application, texte, limite) = match valider(&req) {
        Ok(params) => params,
        Err(erreur) => return erreur.reponse(),
    };
    log_action(
        "atspi_read",
        json!({"application": application, "limite": limite}),
    );
    match executer_lecteur(&application, texte.as_deref(), limite).await {
        Ok(resultat) => (StatusCode::OK, Json(json!({"ok": true, "result": resultat}))).into_response(),
        Err(erreur) => erreur.reponse(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requete(application: &str, limite: Option<usize>) -> LireRequest {
        LireRequest {
            application: application.into(),
            texte: None,
            limite,
        }
    }

    #[test]
    fn validation_refuse_application_vide_et_limites_invalides() {
        assert!(valider(&requete("  ", None)).is_err());
        assert!(valider(&requete("agent", Some(0))).is_err());
        assert!(valider(&requete("agent", Some(501))).is_err());
        assert!(valider(&requete("agent", Some(20))).is_ok());
    }
}
