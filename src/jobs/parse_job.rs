use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::{JobStage, MetarEntry, ProcessingJob, QualityStatus, Site, SkyCondition, WindEntry},
    parse::{self, ParseInput},
};

pub async fn run(
    cfg:   Arc<Config>,
    db:    Arc<Db>,
    sites: Arc<std::collections::HashMap<String, Site>>,
) {
    let poll_interval = Duration::from_secs(cfg.jobs.parse_poll_interval_s);
    let concurrency   = cfg.jobs.parse_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("ParseJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_parse_job().await {
                Ok(Some((job, tx))) => {
                    claimed += 1;
                    let _cfg  = cfg.clone();
                    let db    = db.clone();
                    let sites = sites.clone();
                    let sem   = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit  = sem.acquire_owned().await.unwrap();
                        let job_id   = job.id.unwrap();
                        let tx_id    = tx.id.unwrap();
                        let rec_id   = job.audio_recording_id;
                        let site_id  = job.site_id.clone();

                        tracing::info!("[{}] Parsing transcript", site_id);

                        let raw_transcript = tx.cleaned_transcript
                            .as_deref()
                            .filter(|s| !s.is_empty())
                            .unwrap_or(&tx.raw_transcript)
                            .to_string();

                        tracing::debug!(
                            "[{}] Transcript len={} raw_len={} cleaned_len={}",
                            site_id,
                            raw_transcript.len(),
                            tx.raw_transcript.len(),
                            tx.cleaned_transcript.as_deref().unwrap_or("").len()
                        );

                        let (location, station_type) = sites.get(site_id.as_str())
                            .map(|s| (s.loc_name.clone(), s.site_type.clone()))
                            .unwrap_or_else(|| (site_id.clone(), "AWOS".to_string()));

                        let input = ParseInput {
                            raw_transcript: &raw_transcript,
                            station_id:     &site_id,
                            location:       &location,
                            station_type:   &station_type,
                            recorded_at:    tx.created_at,
                        };

                        let parsed = parse::parse(&input);

                        tracing::info!(
                            "[{}] Parsed: time={} vis={} alt={}",
                            site_id,
                            parsed.time.as_deref().unwrap_or("N/A"),
                            parsed.visibility_sm.as_deref().unwrap_or("N/A"),
                            parsed.altimeter_inhg.as_deref().unwrap_or("N/A"),
                        );

                        let wind = parsed.wind.map(|w| WindEntry {
                            direction: w.direction,
                            speed_kt:  w.speed_kt,
                            gust_kt:   w.gust_kt,
                            variable:  w.variable,
                            calm:      w.calm,
                            raw:       w.raw,
                            metar:     w.metar,
                        });

                        let sky: Vec<SkyCondition> = parsed.sky.into_iter()
                            .map(|s| SkyCondition { coverage: s.coverage, height_ft: s.height_ft })
                            .collect();

                        let now = chrono::Utc::now();
                        let metar = MetarEntry {
                            id:                  None,
                            audio_recording_id:  rec_id,
                            transcription_id:    tx_id,
                            site_id:             site_id.clone(),
                            observed_at:         tx.created_at,
                            time:                parsed.time,
                            wind,
                            visibility_sm:       parsed.visibility_sm,
                            sky,
                            temperature_c:       parsed.temperature_c,
                            dewpoint_c:          parsed.dewpoint_c,
                            altimeter_inhg:      parsed.altimeter_inhg,
                            density_altitude_ft: parsed.density_altitude_ft,
                            phenomena:           parsed.phenomena,
                            remarks:             parsed.remarks,
                            metar:               parsed.metar,
                            selected_loop_time:  parsed.selected_loop_time,
                            local_info:          parsed.local_info,
                            quality_status:      QualityStatus::Pending,
                            quality:             None,
                            validation_warnings: parsed.validation_warnings,
                            audit_warnings:      parsed.audit_warnings,
                            created_at:          now,
                            updated_at:          now,
                        };

                        if let Err(e) = db.complete_parse_job(job_id, &metar).await {
                            tracing::error!("[{}] Failed to complete parse job: {}", site_id, e);
                            let _ = db.fail_job(job_id, &e.to_string()).await;
                            return;
                        }

                        // Only create a quality job if there are warnings
                        let needs_quality = !metar.audit_warnings.is_empty()
                            || !metar.validation_warnings.is_empty();

                        if needs_quality {
                            let quality_job = ProcessingJob::new(
                                rec_id,
                                site_id.clone(),
                                JobStage::Quality,
                            );
                            if let Err(e) = db.create_job(&quality_job).await {
                                tracing::warn!("[{}] Failed to create quality job: {}", site_id, e);
                            } else {
                                tracing::info!(
                                    "[{}] Quality job created ({} audit + {} validation warnings)",
                                    site_id,
                                    metar.audit_warnings.len(),
                                    metar.validation_warnings.len(),
                                );
                            }
                        } else {
                            tracing::info!("[{}] No warnings - skipping quality job", site_id);
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("ParseJob claim error: {}", e);
                    break;
                }
            }
        }
        sleep(poll_interval).await;
    }
}
