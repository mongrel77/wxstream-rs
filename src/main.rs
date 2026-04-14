use std::sync::Arc;

mod config;
mod db;
mod jobs;
mod models;
mod parse;
mod quality;
mod s3;
mod transcribe;
mod trim;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "wxstream=info,warn".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    tracing::info!("WxStream starting up");

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

    let db = db::Db::connect(&cfg.mongodb).await?;
    db.ensure_indexes().await?;
    let db = Arc::new(db);

    // Load sites from MongoDB sites collection
    let sites = db.load_sites().await?;
    let sites = Arc::new(sites);

    let cfg = Arc::new(cfg);

    tracing::info!("All systems connected — launching pipeline jobs");

    // Scanner: finds new raw recordings and creates transcribe jobs
    let scanner_handle = tokio::spawn(jobs::scanner_job::run(
        cfg.clone(),
        db.clone(),
    ));

    // Transcribe: picks up transcribe jobs, calls Whisper
    let transcribe_handle = tokio::spawn(jobs::transcribe_job::run(
        cfg.clone(),
        db.clone(),
        sites.clone(),
    ));

    // Parse: picks up parse jobs, extracts weather data
    let parse_handle = tokio::spawn(jobs::parse_job::run(
        cfg.clone(),
        db.clone(),
        sites.clone(),
    ));

    // Trim: picks up trim jobs, cuts audio and uploads to S3
    let trim_handle = tokio::spawn(jobs::trim_job::run(
        cfg.clone(),
        db.clone(),
        sites.clone(),
    ));

    // Quality: picks up quality jobs, runs Claude agent
    let quality_handle = tokio::spawn(jobs::quality_job::run(
        cfg.clone(),
        db.clone(),
    ));

    tracing::info!("Pipeline running | Ctrl+C to stop");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutdown signal received");
        }
        result = scanner_handle => {
            tracing::error!("ScannerJob exited unexpectedly: {:?}", result);
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
