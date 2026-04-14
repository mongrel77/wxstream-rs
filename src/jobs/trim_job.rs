use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::Station,
    s3,
    trim,
};

/// Continuously polls for "trim_pending" records, downloads raw audio,
/// trims to single broadcast loop, uploads trimmed audio to S3.
pub async fn run(
    cfg:      Arc<Config>,
    db:       Arc<Db>,
    stations: Arc<std::collections::HashMap<String, Station>>,
) {
    let poll_interval = Duration::from_secs(cfg.jobs.trim_poll_interval_s);
    let concurrency   = cfg.jobs.trim_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("TrimJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_for_trim().await {
                Ok(Some(record)) => {
                    claimed += 1;
                    let cfg      = cfg.clone();
                    let db       = db.clone();
                    let stations = stations.clone();
                    let sem      = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit    = sem.acquire_owned().await.unwrap();
                        let id         = record.id.unwrap();
                        let station_id = record.station_id.clone();

                        tracing::info!("[{}] Trimming audio", station_id);

                        let s3_client = match s3::build_client(&cfg.s3).await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!("[{}] S3 client error: {}", station_id, e);
                                let _ = db.mark_trim_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        // Download raw audio
                        let raw_tmp = match tempfile::Builder::new()
                            .suffix(".mp3")
                            .tempfile()
                        {
                            Ok(f) => f,
                            Err(e) => {
                                let _ = db.mark_trim_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        if let Err(e) = s3::download(
                            &s3_client,
                            &cfg.s3.bucket,
                            &record.raw_s3_key,
                            raw_tmp.path(),
                        ).await {
                            tracing::error!("[{}] S3 download failed: {}", station_id, e);
                            let _ = db.mark_trim_failed(id, &e.to_string()).await;
                            return;
                        }

                        // Prepare output temp file
                        let out_tmp = match tempfile::Builder::new()
                            .suffix(".mp3")
                            .tempfile()
                        {
                            Ok(f) => f,
                            Err(e) => {
                                let _ = db.mark_trim_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        // Get word timestamps and obs time from the record
                        let words   = &record.transcription.word_timestamps;
                        let obs_time = record.parsed
                            .as_ref()
                            .and_then(|p| p.selected_loop_time.as_deref());

                        // Get station first word for fallback
                        let station_first_word: Option<String> = stations.get(station_id.as_str())
                            .map(|s| s.location.split_whitespace().next().unwrap_or("").to_string())
                            .filter(|s| !s.is_empty());

                        // Perform trim
                        let result = trim::trim_audio(
                            raw_tmp.path(),
                            out_tmp.path(),
                            if words.is_empty() { None } else { Some(words) },
                            obs_time,
                            station_first_word.as_deref(),
                            &cfg.trim,
                        ).await;

                        match result {
                            Ok(trim_result) => {
                                tracing::info!(
                                    "[{}] Trimmed: {:.1}s via {}",
                                    station_id,
                                    trim_result.duration_s,
                                    trim_result.method
                                );

                                // Derive output filename from raw key
                                let raw_filename = s3::filename_from_key(&record.raw_s3_key);
                                let trimmed_key  = cfg.s3.trimmed_key(&station_id, raw_filename);

                                // Upload trimmed audio to S3
                                let content_type = s3::content_type_for(out_tmp.path());
                                if let Err(e) = s3::upload(
                                    &s3_client,
                                    &cfg.s3.bucket,
                                    &trimmed_key,
                                    out_tmp.path(),
                                    content_type,
                                ).await {
                                    tracing::error!("[{}] S3 upload failed: {}", station_id, e);
                                    let _ = db.mark_trim_failed(id, &e.to_string()).await;
                                    return;
                                }

                                if let Err(e) = db.mark_trim_completed(id, &trimmed_key).await {
                                    tracing::error!("[{}] DB update failed: {}", station_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Trim failed: {}", station_id, e);
                                let _ = db.mark_trim_failed(id, &e.to_string()).await;
                            }
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("TrimJob claim error: {}", e);
                    break;
                }
            }
        }

        sleep(poll_interval).await;
    }
}
