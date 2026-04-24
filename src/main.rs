use std::sync::Arc;

mod config;
mod db;
mod jobs;
mod models;
mod parse;
mod quality;
mod s3;
mod stations;
mod transcribe;
mod trim;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if present (dev convenience)
    let _ = dotenvy::dotenv();

    // Initialize structured logging
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "wxstream=info,warn".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    tracing::info!("WxStream starting up");

    // Load configuration
    let cfg = config::load().map_err(|e| {
        tracing::error!("Configuration error: {}", e);
        e
    })?;

    tracing::info!(
        "Config loaded | DB: {} | S3: s3://{} | Model: {}",
        cfg.mongodb.database,
        cfg.s3.bucket,
        cfg.openai.model,
    );

    // Load station registry
    let stations = stations::load(&cfg.stations.json_path)?;
    let stations = Arc::new(stations);

    // Connect to MongoDB
    let db = db::Db::connect(&cfg.mongodb).await?;
    db.ensure_indexes().await?;
    let db = Arc::new(db);

    let cfg = Arc::new(cfg);

    tracing::info!("All systems connected — launching pipeline jobs");

    // Spawn all four job workers as independent concurrent tasks.
    // Each runs forever in its own loop, polling MongoDB for work.
    let transcribe_handle = tokio::spawn(jobs::transcribe_job::run(
        cfg.clone(),
        db.clone(),
        stations.clone(),
    ));

    let parse_handle = tokio::spawn(jobs::parse_job::run(
        cfg.clone(),
        db.clone(),
        stations.clone(),
    ));

    let trim_handle = tokio::spawn(jobs::trim_job::run(
        cfg.clone(),
        db.clone(),
        stations.clone(),
    ));

    let quality_handle = tokio::spawn(jobs::quality_job::run(
        cfg.clone(),
        db.clone(),
    ));

    tracing::info!("Pipeline running | Ctrl+C to stop");

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown signal received");
        }
        result = transcribe_handle => {
            tracing::error!("TranscribeJob exited unexpectedly: {:?}", result);
        }
        result = parse_handle => {
            tracing::error!("ParseJob exited unexpectedly: {:?}", result);
        }
        result = trim_handle => {
            tracing::error!("TrimJob exited unexpectedly: {:?}", result);
        }
        result = quality_handle => {
            tracing::error!("QualityJob exited unexpectedly: {:?}", result);
        }
    }

    tracing::info!("WxStream stopped");
    Ok(())
}
