use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::{QualityResult, QualityStatus},
    quality,
};

pub async fn run(cfg: Arc<Config>, db: Arc<Db>) {
    let poll_interval = Duration::from_secs(cfg.jobs.quality_poll_interval_s);
    let concurrency   = cfg.jobs.quality_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("QualityJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_quality_job().await {
                Ok(Some((job, metar, tx))) => {
                    claimed += 1;
                    let cfg = cfg.clone();
                    let db  = db.clone();
                    let sem = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit  = sem.acquire_owned().await.unwrap();
                        let job_id   = job.id.unwrap();
                        let metar_id = metar.id.unwrap();
                        let site_id  = job.site_id.clone();

                        tracing::info!("[{}] Running quality check", site_id);

                        match quality::run_quality_check(
                            &cfg.anthropic,
                            &site_id,
                            &tx.raw_transcript,
                            tx.cleaned_transcript.as_deref(),
                            &metar,
                        ).await {
                            Ok((quality_result, status)) => {
                                tracing::info!(
                                    "[{}] Quality: confidence={:.2} needs_review={} flagged={:?}",
                                    site_id,
                                    quality_result.confidence.unwrap_or(0.0),
                                    status == QualityStatus::NeedsReview,
                                    quality_result.flagged_fields,
                                );
                                if let Err(e) = db.complete_quality_job(
                                    job_id, metar_id, &quality_result, status,
                                ).await {
                                    tracing::error!("[{}] Failed to save quality result: {}", site_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Quality agent failed: {}", site_id, e);
                                let _ = db.complete_quality_job(
                                    job_id,
                                    metar_id,
                                    &QualityResult {
                                        notes: Some(format!("Agent error: {}", e)),
                                        human_reviewed: false,
                                        ..Default::default()
                                    },
                                    QualityStatus::Failed,
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
