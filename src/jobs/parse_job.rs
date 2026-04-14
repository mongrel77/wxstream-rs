use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::{
    config::Config,
    db::Db,
    models::{Station, WeatherObservation},
    parse::{self, ParseInput},
};

/// Continuously polls for "transcribed" records, parses weather data,
/// writes WeatherObservation, signals trim and quality jobs.
pub async fn run(
    cfg:      Arc<Config>,
    db:       Arc<Db>,
    stations: Arc<std::collections::HashMap<String, Station>>,
) {
    let poll_interval = Duration::from_secs(cfg.jobs.parse_poll_interval_s);
    let concurrency   = cfg.jobs.parse_concurrency;
    let semaphore     = Arc::new(tokio::sync::Semaphore::new(concurrency));

    tracing::info!("ParseJob started (concurrency={}, poll={}s)", concurrency, poll_interval.as_secs());

    loop {
        let mut claimed = 0;
        while claimed < concurrency {
            match db.claim_for_parsing().await {
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

                        tracing::info!("[{}] Parsing transcript", station_id);

                        let raw_transcript = match &record.transcription.cleaned_transcript {
                            Some(t) => t.clone(),
                            None => match &record.transcription.raw_transcript {
                                Some(t) => t.clone(),
                                None => {
                                    let _ = db.mark_parse_failed(id, "No transcript text available").await;
                                    return;
                                }
                            }
                        };

                        // Get station metadata
                        let (location, station_type) = stations.get(station_id.as_str())
                            .map(|s| (s.location.clone(), s.stn_type.clone()))
                            .unwrap_or_else(|| (station_id.clone(), "AWOS".to_string()));

                        let input = ParseInput {
                            raw_transcript: &raw_transcript,
                            station_id:     &station_id,
                            location:       &location,
                            station_type:   &station_type,
                            recorded_at:    record.recorded_at,
                        };

                        let parsed = parse::parse(&input);

                        tracing::info!(
                            "[{}] Parsed: time={} wind={} vis={} alt={}",
                            station_id,
                            parsed.time.as_deref().unwrap_or("N/A"),
                            parsed.wind.as_ref().and_then(|w| w.raw.as_deref()).unwrap_or("N/A"),
                            parsed.visibility_sm.as_deref().unwrap_or("N/A"),
                            parsed.altimeter_inhg.as_deref().unwrap_or("N/A"),
                        );

                        // Write parsed data back to audio_records + signal downstream
                        if let Err(e) = db.mark_parsed(id, &parsed).await {
                            tracing::error!("[{}] Failed to write parsed data: {}", station_id, e);
                            return;
                        }

                        // Create initial WeatherObservation document
                        let mut updated_record = record.clone();
                        updated_record.parsed = Some(parsed);

                        let obs = WeatherObservation::from_audio_record(&updated_record, id);
                        match db.upsert_weather_observation(&obs).await {
                            Ok(obs_id) => {
                                tracing::info!("[{}] WeatherObservation upserted: {}", station_id, obs_id);
                            }
                            Err(e) => {
                                tracing::error!("[{}] Failed to upsert WeatherObservation: {}", station_id, e);
                            }
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
