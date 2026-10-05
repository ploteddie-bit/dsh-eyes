mod api;
mod ingest;
mod state;
mod stats;
mod vigil;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let ctx = state::VigilCtx::new();
    let app = api::router(ctx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8445").await?;
    tracing::info!("dsh-eyes écoute sur 127.0.0.1:8445");
    axum::serve(listener, app).await?;
    Ok(())
}
