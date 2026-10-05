mod api;
mod ingest;
mod stats;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let counters = stats::Counters::new();
    let app = api::router(counters);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8445").await?;
    tracing::info!("dsh-eyes écoute sur 127.0.0.1:8445");
    axum::serve(listener, app).await?;
    Ok(())
}
