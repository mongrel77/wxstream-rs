use std::sync::Arc;
use clap::{Parser, ValueEnum};

mod config;
mod db;
mod jobs;
mod models;
mod parse;
mod quality;
mod s3;
mod transcribe;
mod trim;

#[derive(Debug, Clone, ValueEnum)]
enum Service {
    /// Scans for new raw recordings and creates transcribe jobs
    Scanner,
    /// Transcribes audio using Whisper API
    Transcribe,
    /// Parses transcriptions into weather data
    Parse,
    /// Trims audio to single broadcast loop
    Trim,
    /// Runs quality agent on parsed data
    Quality,
    /// Runs all services in a single process (default, dev mode)
    All,
}

#[derive(Parser, Debug)]
#[command(name = "wxstream", about = "WxStream audio processing pipeline")]
struct Args {
    /// Which service to run
    #[arg(short, long, default_value = "all")]
    service: Service,

    /// Number of concurrent workers (overrides config)
    #[arg(short, long)]
    workers: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let args = Args::parse();

    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "wxstream=info,warn".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    let service_name = format!("{:?}", args.service).to_lowercase();
    tracing::info!("WxStream starting | service={} workers={:?}",
        service_name, args.workers);

    let mut cfg = config::load().map_err(|e| {
        tracing::error!("Configuration error: {}", e);
        e
    })?;

    // Override worker counts from CLI if provided
    if let Some(w) = args.workers {
        match args.service {
            Service::Transcribe => cfg.jobs.transcribe_concurrency = w,
            Service::Parse      => cfg.jobs.parse_concurrency      = w,
            Service::Trim       => cfg.jobs.trim_concurrency       = w,
            Service::Quality    => cfg.jobs.quality_concurrency    = w,
            _ => {}
        }
    }

    tracing::info!("Config loaded | DB: {} | S3: s3://{}",
        cfg.mongodb.database, cfg.s3.bucket);

    let db = db::Db::connect(&cfg.mongodb).await?;
    db.ensure_indexes().await?;
    let db = Arc::new(db);

    // Load sites from MongoDB — needed by transcribe, parse, trim
    let sites = match args.service {
        Service::Scanner | Service::Quality => Arc::new(std::collections::HashMap::new()),
        _ => {
            let s = db.load_sites().await?;
            Arc::new(s)
        }
    };

    let cfg = Arc::new(cfg);

    tracing::info!("Connected | launching {}", service_name);

    match args.service {
        Service::Scanner => {
            tracing::info!("ScannerService started");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = jobs::scanner_job::run(cfg.clone(), db.clone()) => {}
            }
        }

        Service::Transcribe => {
            tracing::info!("TranscribeService started (workers={})",
                cfg.jobs.transcribe_concurrency);
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = jobs::transcribe_job::run(cfg.clone(), db.clone(), sites.clone()) => {}
            }
        }

        Service::Parse => {
            tracing::info!("ParseService started (workers={})",
                cfg.jobs.parse_concurrency);
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = jobs::parse_job::run(cfg.clone(), db.clone(), sites.clone()) => {}
            }
        }

        Service::Trim => {
            tracing::info!("TrimService started (workers={})",
                cfg.jobs.trim_concurrency);
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = jobs::trim_job::run(cfg.clone(), db.clone(), sites.clone()) => {}
            }
        }

        Service::Quality => {
            tracing::info!("QualityService started (workers={})",
                cfg.jobs.quality_concurrency);
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = jobs::quality_job::run(cfg.clone(), db.clone()) => {}
            }
        }

        Service::All => {
            tracing::info!("All services started (dev mode)");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Shutdown signal received");
                }
                _ = jobs::scanner_job::run(cfg.clone(), db.clone()) => {
                    tracing::error!("ScannerJob exited unexpectedly");
                }
                _ = jobs::transcribe_job::run(cfg.clone(), db.clone(), sites.clone()) => {
                    tracing::error!("TranscribeJob exited unexpectedly");
                }
                _ = jobs::parse_job::run(cfg.clone(), db.clone(), sites.clone()) => {
                    tracing::error!("ParseJob exited unexpectedly");
                }
                _ = jobs::trim_job::run(cfg.clone(), db.clone(), sites.clone()) => {
                    tracing::error!("TrimJob exited unexpectedly");
                }
                _ = jobs::quality_job::run(cfg.clone(), db.clone()) => {
                    tracing::error!("QualityJob exited unexpectedly");
                }
            }
        }
    }

    tracing::info!("WxStream stopped");
    Ok(())
}
