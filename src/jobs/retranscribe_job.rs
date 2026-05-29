/// Retranscribe worker.
///
/// Triggered when the parse job detects a hallucinated transcript with null
/// key fields.  The worker:
///   1. Downloads the raw audio from S3
///   2. Runs ffmpeg silencedetect to find silence boundaries
///   3. Splits the audio into chunks at silence gaps (≥ MIN_SILENCE_S seconds)
///   4. Uploads each chunk to S3 under SITE/chunks/SITE_timestamp_chunkN.mp3
///   5. Creates a transcribe ProcessingJob for each chunk, tagged with a
///      shared chunk_group_id and chunk_index / chunk_total fields
///   6. Marks the retranscribe job done
///
/// The regular transcribe worker handles each chunk normally.  After all
/// chunks in a group are transcribed, a separate aggregation pass in this
/// worker concatenates their transcripts and creates a new parse job.

use std::{path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use bson::oid::ObjectId;
use tokio::{process::Command, time::{sleep, Duration}};

use crate::{
    config::Config,
    db::Db,
    models::{JobStage, ProcessingJob},
    s3,
};

const MIN_SILENCE_S:   f64 = 1.5;
const SILENCE_DB:      f64 = -35.0;
const POLL_INTERVAL_S: u64 = 15;

pub async fn run(cfg: Arc<Config>, db: Arc<Db>) {
    let poll = Duration::from_secs(POLL_INTERVAL_S);
    tracing::info!("RetranscribeJob started (poll={}s)", POLL_INTERVAL_S);

    loop {
        // ── 1. Process pending retranscribe jobs ──────────────────────────
        loop {
            match db.claim_retranscribe_job().await {
                Ok(Some((job, recording))) => {
                    let cfg = cfg.clone();
                    let db  = db.clone();
                    tokio::spawn(async move {
                        let job_id  = job.id.unwrap();
                        let site_id = recording.site_id.clone();
                        tracing::info!("[{}] Retranscribing {}", site_id, recording.object_key);

                        if let Err(e) = process_retranscribe(&cfg, &db, &recording).await {
                            tracing::error!("[{}] Retranscribe failed: {}", site_id, e);
                            let _ = db.fail_job(job_id, &e.to_string()).await;
                        } else {
                            let _ = db.complete_retranscribe_job(job_id).await;
                            tracing::info!("[{}] Retranscribe job complete", site_id);
                        }
                    });
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("RetranscribeJob claim error: {}", e);
                    break;
                }
            }
        }

        // ── 2. Check for completed chunk groups ───────────────────────────
        if let Err(e) = finalize_completed_groups(&db).await {
            tracing::warn!("finalize_completed_groups error: {}", e);
        }

        sleep(poll).await;
    }
}

// ---------------------------------------------------------------------------
// Core retranscribe logic
// ---------------------------------------------------------------------------

async fn process_retranscribe(
    cfg:       &Config,
    db:        &Db,
    recording: &crate::models::AudioRecording,
) -> Result<()> {
    let site_id  = &recording.site_id;
    let _rec_id  = recording.id.unwrap();

    // Download raw audio
    let s3_client = s3::build_client(&cfg.s3).await?;
    let raw_tmp = tempfile::Builder::new()
        .suffix(".mp3")
        .tempfile()
        .context("tempfile")?;

    s3::download(&s3_client, &recording.bucket, &recording.object_key, raw_tmp.path()).await
        .context("S3 download")?;

    tracing::debug!("[{}] Downloaded {} bytes", site_id,
        std::fs::metadata(raw_tmp.path())?.len());

    // Detect silence boundaries
    let boundaries = detect_silence_boundaries(raw_tmp.path()).await?;
    tracing::info!("[{}] Found {} silence boundaries", site_id, boundaries.len());

    if boundaries.is_empty() {
        bail!("No silence boundaries found — cannot split audio");
    }

    // Split into chunks at silence midpoints
    let chunks = split_at_boundaries(raw_tmp.path(), &boundaries).await?;
    tracing::info!("[{}] Split into {} chunks", site_id, chunks.len());

    if chunks.is_empty() {
        bail!("No audio chunks produced");
    }

    // Generate a shared group ID for all chunks in this retranscribe batch
    let group_id   = ObjectId::new();
    let chunk_total = chunks.len() as u32;

    // Upload chunks and create transcribe jobs
    let timestamp = chrono::Utc::now().timestamp();

    for (idx, chunk_path) in chunks.iter().enumerate() {
        let object_key = format!(
            "{}/chunks/{}_{}_chunk{}.mp3",
            site_id, site_id, timestamp, idx
        );

        s3::upload(&s3_client, &recording.bucket, &object_key, chunk_path.path(), "audio/mpeg").await
            .with_context(|| format!("S3 upload chunk {}", idx))?;

        tracing::debug!("[{}] Uploaded chunk {} -> {}", site_id, idx, object_key);

        // Create a new audio_recording for this chunk
        let chunk_rec = crate::models::AudioRecording {
            id:                      None,
            site_id:                 site_id.clone(),
            recorded:                recording.recorded.clone(),
            rec_type:                "chunk".into(),
            bucket:                  recording.bucket.clone(),
            object_key:              object_key.clone(),
            created_at:              Some(bson::Bson::DateTime(bson::DateTime::now())),
            updated_at:              Some(bson::Bson::DateTime(bson::DateTime::now())),
            retranscribe_attempted:  false,
        };
        let chunk_rec_id = db.insert_audio_recording(&chunk_rec).await?;

        // Create transcribe job with chunk metadata
        let mut tx_job = ProcessingJob::new(
            chunk_rec_id,
            site_id.clone(),
            JobStage::Transcribe,
        );
        tx_job.chunk_group_id = Some(group_id);
        tx_job.chunk_index    = Some(idx as u32);
        tx_job.chunk_total    = Some(chunk_total);

        db.create_job(&tx_job).await
            .with_context(|| format!("create transcribe job for chunk {}", idx))?;
    }

    tracing::info!(
        "[{}] Created {} chunk transcribe jobs (group {})",
        site_id, chunk_total, group_id
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Silence detection via ffmpeg
// ---------------------------------------------------------------------------

async fn detect_silence_boundaries(audio_path: &Path) -> Result<Vec<(f64, f64)>> {
    // ffmpeg silencedetect outputs lines like:
    //   silence_start: 12.345
    //   silence_end: 14.567 | silence_duration: 2.222
    let output = Command::new("ffmpeg")
        .args([
            "-i", audio_path.to_str().unwrap(),
            "-af", &format!("silencedetect=noise={}dB:duration={}", SILENCE_DB, MIN_SILENCE_S),
            "-f", "null", "-",
        ])
        .output()
        .await
        .context("ffmpeg silencedetect")?;

    // ffmpeg writes filter output to stderr
    let stderr = String::from_utf8_lossy(&output.stderr);

    let mut starts: Vec<f64> = Vec::new();
    let mut ends:   Vec<f64> = Vec::new();

    for line in stderr.lines() {
        if let Some(s) = line.strip_prefix("  silence_start: ")
            .or_else(|| line.split("silence_start: ").nth(1)) {
            if let Ok(t) = s.trim().parse::<f64>() {
                starts.push(t);
            }
        }
        if let Some(s) = line.split("silence_end: ").nth(1) {
            let end_str = s.split('|').next().unwrap_or(s).trim();
            if let Ok(t) = end_str.parse::<f64>() {
                ends.push(t);
            }
        }
    }

    let pairs: Vec<(f64, f64)> = starts.into_iter().zip(ends).collect();
    Ok(pairs)
}

// ---------------------------------------------------------------------------
// Audio splitting via ffmpeg
// ---------------------------------------------------------------------------

async fn split_at_boundaries(
    audio_path: &Path,
    boundaries: &[(f64, f64)],
) -> Result<Vec<tempfile::NamedTempFile>> {
    // Split at the midpoint of each silence gap
    let mut cut_points: Vec<f64> = boundaries
        .iter()
        .map(|(start, end)| (start + end) / 2.0)
        .collect();

    // Get total duration
    let duration = get_audio_duration(audio_path).await?;
    cut_points.insert(0, 0.0);
    cut_points.push(duration);

    let mut chunks = Vec::new();

    for window in cut_points.windows(2) {
        let start = window[0];
        let end   = window[1];
        let len   = end - start;

        // Skip very short segments (< 3 seconds — not a full broadcast)
        if len < 3.0 { continue; }

        let tmp = tempfile::Builder::new()
            .suffix(".mp3")
            .tempfile()
            .context("tempfile for chunk")?;

        let status = Command::new("ffmpeg")
            .args([
                "-ss", &start.to_string(),
                "-t",  &len.to_string(),
                "-i",  audio_path.to_str().unwrap(),
                "-c",  "copy",
                "-y",
                tmp.path().to_str().unwrap(),
            ])
            .status()
            .await
            .context("ffmpeg split")?;

        if !status.success() {
            tracing::warn!("ffmpeg split failed for segment {:.1}-{:.1}s", start, end);
            continue;
        }

        chunks.push(tmp);
    }

    Ok(chunks)
}

async fn get_audio_duration(audio_path: &Path) -> Result<f64> {
    let output = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            audio_path.to_str().unwrap(),
        ])
        .output()
        .await
        .context("ffprobe duration")?;

    let s = String::from_utf8_lossy(&output.stdout);
    s.trim().parse::<f64>().context("parse duration")
}

// ---------------------------------------------------------------------------
// Chunk group finalization — concatenate transcripts and create parse job
// ---------------------------------------------------------------------------

async fn finalize_completed_groups(db: &Db) -> Result<()> {
    let groups = db.find_completed_chunk_groups().await?;

    for (group_id, rec_id, site_id, transcripts) in groups {
        tracing::info!("[{}] Finalizing chunk group {}", site_id, group_id);

        // Concatenate transcripts in chunk order
        let combined = transcripts.join("\n");

        // Insert a combined transcription document
        let tx = crate::models::Transcription {
            id:                  None,
            audio_recording_id:  rec_id,
            site_id:             site_id.clone(),
            raw_transcript:      combined,
            word_timestamps:     vec![],
            segment_timestamps:  vec![],
            timestamp_source:    "chunk_concat".into(),
            cleaned_transcript:  None,
            hallucination_chars: None,
            created_at:          chrono::Utc::now(),
        };

        let tx_id = db.insert_transcription(&tx).await?;

        // Create a parse job for the combined transcript
        let parse_job = ProcessingJob::new(rec_id, site_id.clone(), JobStage::Parse);
        db.create_job(&parse_job).await?;

        // Mark the group as finalized so we don't process it again
        db.mark_chunk_group_finalized(group_id, tx_id).await?;

        tracing::info!("[{}] Chunk group {} finalized -> parse job created", site_id, group_id);
    }

    Ok(())
}
