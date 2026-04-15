use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::Site,
    s3,
    trim,
};

pub async fn run(
    cfg:   Arc<Config>,
    db:    Arc<Db>,
    sites: Arc<std::collections::HashMap<String, Site>>,
) {
    let poll_interval = Duration::from_secs(cfg.jobs.trim_poll_interval_s);
    let concurrency   = cfg.jobs.trim_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("TrimJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_trim_job().await {
                Ok(Some((job, recording, tx, selected_loop_time))) => {
                    claimed += 1;
                    let cfg   = cfg.clone();
                    let db    = db.clone();
                    let sites = sites.clone();
                    let sem   = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit  = sem.acquire_owned().await.unwrap();
                        let job_id   = job.id.unwrap();
                        let rec_id   = recording.id.unwrap();
                        let site_id  = recording.site_id.clone();

                        tracing::info!("[{}] Trimming audio", site_id);

                        let s3_client = match s3::build_client(&cfg.s3).await {
                            Ok(c) => c,
                            Err(e) => { let _ = db.fail_job(job_id, &e.to_string()).await; return; }
                        };

                        // Download raw audio
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

                        let out_tmp = match tempfile::Builder::new().suffix(".wav").tempfile() {
                            Ok(f) => f,
                            Err(e) => { let _ = db.fail_job(job_id, &e.to_string()).await; return; }
                        };

                        let words = &tx.word_timestamps;
                        let obs_time = selected_loop_time.as_deref();

                        let station_first_word: Option<String> = sites.get(site_id.as_str())
                            .and_then(|s| s.loc_name.split_whitespace().next().map(|w| w.to_string()));

                        let result = trim::trim_audio(
                            raw_tmp.path(),
                            out_tmp.path(),
                            if words.is_empty() { None } else { Some(words.as_slice()) },
                            obs_time,
                            station_first_word.as_deref(),
                            &cfg.trim,
                        ).await;

                        match result {
                            Ok(trim_result) => {
                                tracing::info!(
                                    "[{}] Trimmed: {:.1}s via {}",
                                    site_id, trim_result.duration_s, trim_result.method,
                                );

                                // Derive trimmed S3 key from raw key
                                // e.g. KAIZ/2026/04/14/raw/164649622.wav
                                //   -> KAIZ/2026/04/14/trimmed/164649622.wav
                                let trimmed_key = recording.object_key.replace("/raw/", "/trimmed/");

                                let content_type = s3::content_type_for(out_tmp.path());
                                if let Err(e) = s3::upload(
                                    &s3_client,
                                    &recording.bucket,
                                    &trimmed_key,
                                    out_tmp.path(),
                                    content_type,
                                ).await {
                                    tracing::error!("[{}] S3 upload failed: {}", site_id, e);
                                    let _ = db.fail_job(job_id, &e.to_string()).await;
                                    return;
                                }

                                if let Err(e) = db.complete_trim_job(
                                    job_id,
                                    rec_id,
                                    &recording.bucket,
                                    &trimmed_key,
                                ).await {
                                    tracing::error!("[{}] DB update failed: {}", site_id, e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Trim failed: {}", site_id, e);
                                let _ = db.fail_job(job_id, &e.to_string()).await;
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
