use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::{Site, Transcription},
    s3,
    transcribe::{self, build_transcription_prompt, generic_prompt},
    trim::strip_silence,
};

pub async fn run(
    cfg:   Arc<Config>,
    db:    Arc<Db>,
    sites: Arc<std::collections::HashMap<String, Site>>,
) {
    let poll_interval = Duration::from_secs(cfg.jobs.transcribe_poll_interval_s);
    let concurrency   = cfg.jobs.transcribe_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("TranscribeJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_transcribe_job().await {
                Ok(Some((job, recording))) => {
                    claimed += 1;
                    let cfg   = cfg.clone();
                    let db    = db.clone();
                    let sites = sites.clone();
                    let sem   = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit    = sem.acquire_owned().await.unwrap();
                        let job_id     = job.id.unwrap();
                        let rec_id     = recording.id.unwrap();
                        let site_id    = recording.site_id.clone();

                        tracing::info!("[{}] Transcribing {}", site_id, recording.object_key);

                        // Build station-specific Whisper prompt
                        let prompt = sites.get(site_id.as_str())
                            .map(|s| build_transcription_prompt(s))
                            .unwrap_or_else(generic_prompt);

                        let s3_client = match s3::build_client(&cfg.s3).await {
                            Ok(c) => c,
                            Err(e) => {
                                let _ = db.fail_job(job_id, &e.to_string()).await;
                                return;
                            }
                        };

                        // Download raw audio from S3
                        let raw_tmp = match tempfile::Builder::new().suffix(".wav").tempfile() {
                            Ok(f) => f,
                            Err(e) => { let _ = db.fail_job(job_id, &e.to_string()).await; return; }
                        };

                        if let Err(e) = s3::download(
                            &s3_client,
                            &recording.bucket,
                            &recording.object_key,
                            raw_tmp.path(),
                        ).await {
                            tracing::error!("[{}] S3 download failed: {}", site_id, e);
                            let _ = db.fail_job(job_id, &e.to_string()).await;
                            return;
                        }

                        // Strip trailing silence
                        let stripped_tmp = match tempfile::Builder::new().suffix(".wav").tempfile() {
                            Ok(f) => f,
                            Err(e) => { let _ = db.fail_job(job_id, &e.to_string()).await; return; }
                        };

                        let audio_path = match strip_silence(
                            raw_tmp.path(),
                            stripped_tmp.path(),
                            cfg.silence_strip.threshold_db,
                            cfg.silence_strip.min_silence_s,
                        ).await {
                            Ok(_)  => stripped_tmp.path().to_path_buf(),
                            Err(e) => {
                                tracing::warn!("[{}] Silence strip failed ({}), using raw", site_id, e);
                                raw_tmp.path().to_path_buf()
                            }
                        };

                        // Transcribe with Whisper
                        match transcribe::transcribe(&cfg.openai, &audio_path, &prompt).await {
                            Ok(tx_doc) => {
                                tracing::info!(
                                    "[{}] Transcribed: {} words, source={}",
                                    site_id,
                                    tx_doc.word_timestamps.len(),
                                    tx_doc.timestamp_source.as_deref().unwrap_or("unknown"),
                                );

                                let tx = Transcription {
                                    id:                  None,
                                    audio_recording_id:  rec_id,
                                    site_id:             site_id.clone(),
                                    raw_transcript:      tx_doc.raw_transcript.unwrap_or_default(),
                                    word_timestamps:     tx_doc.word_timestamps,
                                    segment_timestamps:  tx_doc.segment_timestamps,
                                    timestamp_source:    tx_doc.timestamp_source.unwrap_or_else(|| "word".into()),
                                    cleaned_transcript:  tx_doc.cleaned_transcript,
                                    hallucination_chars: tx_doc.hallucination_chars,
                                    created_at:          chrono::Utc::now(),
                                };

                                if let Err(e) = db.complete_transcribe_job(
                                    job_id, rec_id, &site_id, &tx,
                                ).await {
                                    tracing::error!("[{}] Failed to complete transcribe job: {}", site_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Whisper failed: {}", site_id, e);
                                let _ = db.fail_job(job_id, &e.to_string()).await;
                            }
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("TranscribeJob claim error: {}", e);
                    break;
                }
            }
        }
        sleep(poll_interval).await;
    }
}
