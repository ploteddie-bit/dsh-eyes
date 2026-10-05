mod api;
mod auth;
mod config;
mod hands;
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

    // Pool navigateur (Task 2). On lance Chrome au boot par défaut (le
    // délai de spawn ~2-3 s est acceptable au démarrage) pour que
    // `/v2/browser/status` soit `running=true` dès le premier curl.
    // On tolère l'échec : si Chrome n'est pas dispo (machine de dev
    // minimale), le daemon démarre quand même et le pool essaiera de
    // relancer à chaque `ensure_running` ultérieur.
    let pool = hands::browser::BrowserPool::new(&config);
    if let Err(e) = pool.ensure_running().await {
        tracing::warn!(error = %e, "échec du préchauffage Chrome au boot");
    }

    let app = api::router_with_v2(ctx, config, pool.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8445").await?;
    tracing::info!("dsh-eyes écoute sur 127.0.0.1:8445");
    axum::serve(listener, app).await?;
    // Shutdown propre : si on a été interrompu (Ctrl-C), on tente de
    // tuer Chrome. best-effort.
    pool.shutdown().await;
    Ok(())
}
