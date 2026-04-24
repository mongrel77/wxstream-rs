use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    s3,
    transcribe::{self, build_transcription_prompt, generic_prompt},
    trim::strip_silence,
    models::Station,
};

/// Continuously polls MongoDB for "not_processed" audio records,
/// downloads from S3, transcribes with Whisper, writes results back.
pub async fn run(cfg: Arc<Config>, db: Arc<Db>, stations: Arc<std::collections::HashMap<String, Station>>) {
    let poll_interval = Duration::from_secs(cfg.jobs.transcribe_poll_interval_s);
    let concurrency   = cfg.jobs.transcribe_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("TranscribeJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        // Claim up to `concurrency` records in one poll cycle
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_for_transcription().await {
                Ok(Some(record)) => {
                    claimed += 1;
                    let cfg      = cfg.clone();
                    let db       = db.clone();
                    let stations = stations.clone();
                    let sem      = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.unwrap();
                        let id       = record.id.unwrap();
                        let station_id = &record.station_id;

                        tracing::info!("[{}] Transcribing {}", station_id, record.raw_s3_key);

                        // Build station-specific Whisper prompt
                        let prompt = stations.get(station_id.as_str())
                            .map(|s| build_transcription_prompt(s))
                            .unwrap_or_else(generic_prompt);

                        // Download from S3 to temp file
                        let tmp = match tempfile::NamedTempFile::new() {
                            Ok(f) => f,
                            Err(e) => {
                                tracing::error!("[{}] Failed to create temp file: {}", station_id, e);
                                let _ = db.mark_transcription_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        let s3_client = match s3::build_client(&cfg.s3).await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!("[{}] S3 client error: {}", station_id, e);
                                let _ = db.mark_transcription_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        if let Err(e) = s3::download(
                            &s3_client,
                            &cfg.s3.bucket,
                            &record.raw_s3_key,
                            tmp.path(),
                        ).await {
                            tracing::error!("[{}] S3 download failed: {}", station_id, e);
                            let _ = db.mark_transcription_failed(id, &e.to_string()).await;
                            return;
                        }

                        // Strip trailing silence before transcription
                        let stripped_tmp = match tempfile::NamedTempFile::new() {
                            Ok(f) => f,
                            Err(e) => {
                                tracing::error!("[{}] Failed to create stripped temp file: {}", station_id, e);
                                let _ = db.mark_transcription_failed(id, &e.to_string()).await;
                                return;
                            }
                        };

                        let audio_path = match strip_silence(
                            tmp.path(),
                            stripped_tmp.path(),
                            cfg.silence_strip.threshold_db,
                            cfg.silence_strip.min_silence_s,
                        ).await {
                            Ok(_) => stripped_tmp.path().to_path_buf(),
                            Err(e) => {
                                tracing::warn!("[{}] Silence strip failed ({}), using raw audio", station_id, e);
                                tmp.path().to_path_buf()
                            }
                        };

                        // Transcribe
                        match transcribe::transcribe(&cfg.openai, &audio_path, &prompt).await {
                            Ok(tx_doc) => {
                                tracing::info!(
                                    "[{}] Transcribed: {} words, source={}",
                                    station_id,
                                    tx_doc.word_timestamps.len(),
                                    tx_doc.timestamp_source.as_deref().unwrap_or("unknown")
                                );
                                if let Err(e) = db.mark_transcribed(id, &tx_doc).await {
                                    tracing::error!("[{}] DB write failed: {}", station_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Whisper failed: {}", station_id, e);
                                let _ = db.mark_transcription_failed(id, &e.to_string()).await;
                            }
                        }
                    });
                }
                Ok(None) => break, // No more work in this poll cycle
                Err(e) => {
                    tracing::error!("TranscribeJob claim error: {}", e);
                    break;
                }
            }
        }

        sleep(poll_interval).await;
    }
}
