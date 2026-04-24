pub mod timestamp;

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::process::Command;

use crate::config::TrimConfig;
use crate::models::WordTimestamp;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub struct TrimResult {
    pub output_path: PathBuf,
    pub duration_s:  f64,
    pub method:      TrimMethod,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrimMethod {
    Timestamp,
    Energy,
}

impl std::fmt::Display for TrimMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimMethod::Timestamp => write!(f, "timestamp"),
            TrimMethod::Energy    => write!(f, "energy"),
        }
    }
}

/// Trim a raw audio file to a single clean broadcast loop.
/// Tries timestamp-based trim first; falls back to energy-based.
pub async fn trim_audio(
    input_path:         &Path,
    output_path:        &Path,
    words:              Option<&[WordTimestamp]>,
    obs_time:           Option<&str>,
    station_first_word: Option<&str>,
    cfg:                &TrimConfig,
) -> Result<TrimResult> {
    // Try timestamp-based first
    if let Some(words) = words {
        if !words.is_empty() {
            match trim_by_timestamps(input_path, output_path, words, obs_time, station_first_word, cfg).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::warn!("Timestamp trim failed: {} — trying energy fallback", e);
                }
            }
        }
    }

    // Energy-based fallback
    trim_by_energy(input_path, output_path, cfg).await
}

// ---------------------------------------------------------------------------
// Timestamp-based trim
// ---------------------------------------------------------------------------

async fn trim_by_timestamps(
    input_path:         &Path,
    output_path:        &Path,
    words:              &[WordTimestamp],
    obs_time:           Option<&str>,
    station_first_word: Option<&str>,
    cfg:                &TrimConfig,
) -> Result<TrimResult> {
    let (start_sec, mut end_sec) = timestamp::find_loop_from_timestamps(
        words,
        obs_time,
        station_first_word,
        cfg,
    )
    .ok_or_else(|| anyhow::anyhow!("Could not identify a complete loop in timestamps"))?;

    // Remove trailing silence from the selected region
    if let Ok(tail) = trailing_silence_end(input_path, start_sec, end_sec, cfg.trailing_db).await {
        end_sec = start_sec + tail;
    }

    let duration = end_sec - start_sec;
    if duration < cfg.min_loop_s {
        let orig = get_duration(input_path).await.unwrap_or(0.0);
        if orig >= cfg.min_loop_s {
            anyhow::bail!("Trimmed duration too short ({:.1}s)", duration);
        }
    }

    ffmpeg_trim(input_path, output_path, start_sec, end_sec).await?;
    let actual_dur = get_duration(output_path).await.unwrap_or(duration);

    Ok(TrimResult {
        output_path: output_path.to_path_buf(),
        duration_s:  actual_dur,
        method:      TrimMethod::Timestamp,
    })
}

// ---------------------------------------------------------------------------
// Energy-based fallback — mirrors trim_by_energy() from audio_trim.py
// ---------------------------------------------------------------------------

async fn trim_by_energy(
    input_path:  &Path,
    output_path: &Path,
    cfg:         &TrimConfig,
) -> Result<TrimResult> {
    // Decode to raw PCM via ffmpeg, then analyse in process
    let pcm = decode_to_pcm(input_path, 22050).await?;
    let sr   = 22050usize;
    let frame_s = 0.05f64;
    let hop_s   = 0.02f64;
    let frame = (sr as f64 * frame_s) as usize;
    let hop   = (sr as f64 * hop_s)   as usize;

    // Compute RMS per frame
    let rms: Vec<f32> = (0..pcm.len().saturating_sub(frame))
        .step_by(hop)
        .map(|i| {
            let slice = &pcm[i..i + frame];
            let mean_sq: f32 = slice.iter().map(|s| s * s).sum::<f32>() / frame as f32;
            mean_sq.sqrt()
        })
        .collect();

    if rms.is_empty() {
        anyhow::bail!("No audio frames found");
    }

    // Threshold at 20th percentile * 1.5
    let mut sorted_rms = rms.clone();
    sorted_rms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p20 = sorted_rms[(sorted_rms.len() as f64 * 0.20) as usize];
    let threshold = p20 * 1.5;

    let silent: Vec<bool> = rms.iter().map(|r| *r < threshold).collect();

    // Find silence boundaries (gap > 0.7s)
    let mut starts: Vec<f64> = Vec::new();
    let mut silence_start: Option<usize> = None;

    for (i, &is_silent) in silent.iter().enumerate() {
        if is_silent && silence_start.is_none() {
            silence_start = Some(i);
        } else if !is_silent {
            if let Some(ss) = silence_start {
                let gap_s = (i - ss) as f64 * hop_s;
                if gap_s > 0.7 {
                    starts.push(i as f64 * hop_s);
                }
                silence_start = None;
            }
        }
    }

    if starts.len() < 2 {
        anyhow::bail!("Insufficient silence boundaries detected");
    }

    // Pick best pair
    let (mut start_sec, end_sec) = if starts.len() >= 3
        && cfg.min_loop_s < starts[2] - starts[0]
        && starts[2] - starts[0] < cfg.max_loop_s
    {
        (starts[0], starts[2])
    } else {
        (starts[0], starts[1])
    };

    start_sec = (start_sec - 0.04).max(0.0);

    // Trailing silence removal
    let tail = {
        let s0 = (start_sec * sr as f64) as usize;
        let s1 = (end_sec   * sr as f64) as usize;
        let region = &pcm[s0.min(pcm.len())..s1.min(pcm.len())];
        trailing_silence_from_samples(region, sr, cfg.trailing_db)
    };
    let end_sec = start_sec + tail;

    let duration = end_sec - start_sec;
    if duration < cfg.min_loop_s {
        let orig = get_duration(input_path).await.unwrap_or(0.0);
        if orig >= cfg.min_loop_s {
            anyhow::bail!("Trimmed duration too short ({:.1}s)", duration);
        }
    }

    ffmpeg_trim(input_path, output_path, start_sec, end_sec).await?;
    let actual_dur = get_duration(output_path).await.unwrap_or(duration);

    Ok(TrimResult {
        output_path: output_path.to_path_buf(),
        duration_s:  actual_dur,
        method:      TrimMethod::Energy,
    })
}

// ---------------------------------------------------------------------------
// ffmpeg helpers
// ---------------------------------------------------------------------------

/// Decode audio to f32 mono PCM at the given sample rate.
async fn decode_to_pcm(path: &Path, sr: u32) -> Result<Vec<f32>> {
    let output = Command::new("ffmpeg")
        .args([
            "-v", "error",
            "-i", path.to_str().unwrap_or(""),
            "-ac", "1",
            "-ar", &sr.to_string(),
            "-f", "s16le",
            "-",
        ])
        .output()
        .await
        .context("ffmpeg decode failed")?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffmpeg decode error: {}", err);
    }

    let raw: Vec<i16> = output.stdout
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();

    Ok(raw.iter().map(|&s| s as f32 / 32768.0).collect())
}

/// Cut audio with ffmpeg stream copy (no re-encode).
async fn ffmpeg_trim(input: &Path, output: &Path, start: f64, end: f64) -> Result<()> {
    let status = Command::new("ffmpeg")
        .args([
            "-v", "error", "-y",
            "-ss", &format!("{:.3}", start),
            "-t",  &format!("{:.3}", end - start),
            "-i",  input.to_str().unwrap_or(""),
            "-c",  "copy",
            output.to_str().unwrap_or(""),
        ])
        .status()
        .await
        .context("ffmpeg trim failed")?;

    if !status.success() {
        anyhow::bail!("ffmpeg trim exited with non-zero status");
    }
    Ok(())
}

/// Get duration of an audio file via ffprobe.
pub async fn get_duration(path: &Path) -> Result<f64> {
    let output = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path.to_str().unwrap_or(""),
        ])
        .output()
        .await?;

    let s = String::from_utf8_lossy(&output.stdout);
    s.trim().parse::<f64>().context("Failed to parse duration")
}

/// Strip trailing silence via ffmpeg silenceremove filter.
async fn trailing_silence_end(
    path:        &Path,
    start_sec:   f64,
    end_sec:     f64,
    threshold_db: f64,
) -> Result<f64> {
    // Decode region to PCM
    let output = Command::new("ffmpeg")
        .args([
            "-v", "error",
            "-ss", &format!("{:.3}", start_sec),
            "-t",  &format!("{:.3}", end_sec - start_sec),
            "-i",  path.to_str().unwrap_or(""),
            "-ac", "1",
            "-ar", "22050",
            "-f",  "s16le",
            "-",
        ])
        .output()
        .await?;

    let raw: Vec<i16> = output.stdout
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();

    let samples: Vec<f32> = raw.iter().map(|&s| s as f32 / 32768.0).collect();
    Ok(trailing_silence_from_samples(&samples, 22050, threshold_db))
}

/// Find the point where speech ends (trailing silence begins).
/// Mirrors find_trailing_silence_end() from audio_trim.py.
fn trailing_silence_from_samples(samples: &[f32], sr: usize, threshold_db: f64) -> f64 {
    let threshold = 10f64.powf(threshold_db / 20.0) as f32;
    let chunk = (sr as f64 * 0.1) as usize;
    if chunk == 0 { return samples.len() as f64 / sr as f64; }

    let mut end = samples.len();
    while end > chunk {
        let slice = &samples[end - chunk..end];
        let rms = (slice.iter().map(|s| s * s).sum::<f32>() / chunk as f32).sqrt();
        if rms > threshold {
            break;
        }
        end -= chunk;
    }
    end as f64 / sr as f64
}

/// Strip trailing silence from a raw audio file using ffmpeg silenceremove.
/// Mirrors strip_file() from silence_strip.py.
pub async fn strip_silence(
    input_path:   &Path,
    output_path:  &Path,
    threshold_db: f64,
    min_silence_s: f64,
) -> Result<()> {
    let filter = format!(
        "silenceremove=stop_periods=-1:stop_duration={}:stop_threshold={}dB",
        min_silence_s, threshold_db
    );

    let status = Command::new("ffmpeg")
        .args([
            "-v", "error", "-y",
            "-i", input_path.to_str().unwrap_or(""),
            "-af", &filter,
            "-c:a", "libmp3lame", "-q:a", "2",
            output_path.to_str().unwrap_or(""),
        ])
        .status()
        .await
        .context("ffmpeg silence strip failed")?;

    if !status.success() {
        anyhow::bail!("ffmpeg silence strip exited with non-zero status");
    }

    // Sanity check: output must be at least 5KB
    let size = tokio::fs::metadata(output_path).await?.len();
    if size < 5 * 1024 {
        anyhow::bail!("Output too small after silence strip ({} bytes)", size);
    }

    Ok(())
}
