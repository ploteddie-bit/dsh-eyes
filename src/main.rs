mod api;
mod auth;
mod config;
mod hands;
mod ingest;
mod solver;
mod state;
mod stats;
mod vigil;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let ctx = state::VigilCtx::new();
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8445").await?;
    tracing::info!("dsh-eyes écoute sur 127.0.0.1:8445");
    axum::serve(listener, app).await?;
    Ok(())
}
