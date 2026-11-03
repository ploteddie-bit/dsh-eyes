mod api;
mod atspi;
mod auth;
mod config;
mod hands;
mod ingest;
mod record;
mod solver;
mod state;
mod stats;
mod vigil;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    // Rotation de démarrage : un JSONL > 50 Mo est archivé avant que
    // les writers ne l'ouvrent en append.
    ingest::rotate_logs_on_startup();
    let ctx = state::VigilCtx::new();
    // Hot-reload allowlist (Lot C — D4) : la tâche re-vérifie le mtime de
    // allowlist.json toutes les 25 s — les éditions manuelles sont prises
    // en compte sans restart du démon.
    vigil::Allowlist::spawn_reload_task(ctx.allowlist.clone());
    let config = config::Config::load()?;
    tracing::info!(
        ollama_tier1 = %config.ollama_tier1_url,
        ollama_tier2 = %config.ollama_tier2_url,
        "config v2 chargée (token en extension, jamais loggé)"
    );

    // Canal de communication bidirectionnel avec l'extension Chrome
    // (REWORK 2026-10-05 — modèle webbridge, voir hands::channel).
    // Pas de préchauffage : la connexion WS de l'extension est paresseuse
    // et le canal la découvre à `register()`.
    let channel = hands::channel::ExtensionChannel::new();

    let app = api::router_with_v2(ctx, config, channel);
    let port = std::env::var("DSH_EYES_PORT")
        .ok()
        .map(|valeur| {
            valeur
                .parse::<u16>()
                .map_err(|e| anyhow::anyhow!("DSH_EYES_PORT invalide : {e}"))
                .and_then(|port| {
                    if port < 1024 {
                        Err(anyhow::anyhow!("DSH_EYES_PORT doit être entre 1024 et 65535"))
                    } else {
                        Ok(port)
                    }
                })
        })
        .transpose()?
        .unwrap_or(8445);
    let adresse = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&adresse).await?;
    tracing::info!(%adresse, "dsh-eyes écoute sur le loopback");
    axum::serve(listener, app).await?;
    Ok(())
}
