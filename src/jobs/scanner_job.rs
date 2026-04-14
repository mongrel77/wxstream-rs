use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::{JobStage, ProcessingJob},
};

/// Continuously scans audio_recordings for type "raw" entries
/// that don't yet have a transcribe job, and creates one.
pub async fn run(cfg: Arc<Config>, db: Arc<Db>) {
    let poll_interval = Duration::from_secs(cfg.jobs.transcribe_poll_interval_s);

    tracing::info!("ScannerJob started (poll={}s)", poll_interval.as_secs());

    loop {
        match db.find_unqueued_raw_recordings().await {
            Ok(recordings) => {
                if !recordings.is_empty() {
                    tracing::info!("[scanner] Found {} unqueued raw recording(s)", recordings.len());
                }
                for recording in recordings {
                    let recording_id = match recording.id {
                        Some(id) => id,
                        None => {
                            tracing::warn!("[scanner] Recording has no _id, skipping");
                            continue;
                        }
                    };

                    let job = ProcessingJob::new(
                        recording_id,
                        recording.site_id.clone(),
                        JobStage::Transcribe,
                    );

                    match db.create_job(&job).await {
                        Ok(_) => tracing::info!(
                            "[scanner] Created transcribe job for {} ({})",
                            recording.site_id,
                            recording_id,
                        ),
                        Err(e) => tracing::error!(
                            "[scanner] Failed to create job for {}: {}",
                            recording.site_id, e
                        ),
                    }
                }
            }
            Err(e) => tracing::error!("[scanner] Query failed: {}", e),
        }

        sleep(poll_interval).await;
    }
}
