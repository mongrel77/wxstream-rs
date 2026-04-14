use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    quality,
};

/// Continuously polls for "quality_pending" records and runs the
/// Claude quality agent to validate parsed weather data.
pub async fn run(cfg: Arc<Config>, db: Arc<Db>) {
    let poll_interval = Duration::from_secs(cfg.jobs.quality_poll_interval_s);
    let concurrency   = cfg.jobs.quality_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("QualityJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_for_quality().await {
                Ok(Some(record)) => {
                    claimed += 1;
                    let cfg = cfg.clone();
                    let db  = db.clone();
                    let sem = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit    = sem.acquire_owned().await.unwrap();
                        let id         = record.id.unwrap();
                        let station_id = record.station_id.clone();

                        let parsed = match &record.parsed {
                            Some(p) => p.clone(),
                            None => {
                                tracing::warn!("[{}] Quality check: no parsed data", station_id);
                                let _ = db.mark_quality_result(
                                    id,
                                    &crate::models::QualityDoc {
                                        notes: Some("No parsed data available".into()),
                                        human_reviewed: false,
                                        ..Default::default()
                                    },
                                    crate::models::QualityStatus::NeedsReview,
                                ).await;
                                return;
                            }
                        };

                        tracing::info!("[{}] Running quality check", station_id);

                        match quality::run_quality_check(
                            &cfg.anthropic,
                            &station_id,
                            &record.transcription,
                            &parsed,
                        ).await {
                            Ok((quality_doc, status)) => {
                                let needs_review = status == crate::models::QualityStatus::NeedsReview;
                                let confidence   = quality_doc.confidence.unwrap_or(0.0);

                                tracing::info!(
                                    "[{}] Quality check: confidence={:.2} needs_review={} flagged={:?}",
                                    station_id,
                                    confidence,
                                    needs_review,
                                    quality_doc.flagged_fields,
                                );

                                if let Err(e) = db.mark_quality_result(id, &quality_doc, status).await {
                                    tracing::error!("[{}] Failed to write quality result: {}", station_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Quality agent failed: {}", station_id, e);
                                let _ = db.mark_quality_result(
                                    id,
                                    &crate::models::QualityDoc {
                                        notes: Some(format!("Agent error: {}", e)),
                                        human_reviewed: false,
                                        ..Default::default()
                                    },
                                    crate::models::QualityStatus::Failed,
                                ).await;
                            }
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("QualityJob claim error: {}", e);
                    break;
                }
            }
        }

        sleep(poll_interval).await;
    }
}
