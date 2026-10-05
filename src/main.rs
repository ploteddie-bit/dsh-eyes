mod api;
mod auth;
mod config;
mod ingest;
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
        chrome_headless = config.chrome_headless,
        "config v2 chargée (token en extension, jamais loggé)"
    );
    let app = api::router_with_v2(ctx, config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8445").await?;
    tracing::info!("dsh-eyes écoute sur 127.0.0.1:8445");
    axum::serve(listener, app).await?;
    Ok(())
}
