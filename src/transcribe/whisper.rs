use anyhow::{bail, Context, Result};
use reqwest::multipart;
use serde::Deserialize;
use std::path::Path;

use crate::{
    config::OpenAiConfig,
    models::{SegmentTimestamp, TranscriptionDoc, WordTimestamp},
};

const WHISPER_URL: &str = "https://api.openai.com/v1/audio/transcriptions";
const MIN_ADVANCE_S: f64 = 0.05;
const FREEZE_RUN: usize = 8;

// ---------------------------------------------------------------------------
// Whisper API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WhisperResponse {
    text:      String,
    words:     Option<Vec<WhisperWord>>,
    segments:  Option<Vec<WhisperSegment>>,
}

#[derive(Debug, Deserialize)]
struct WhisperWord {
    word:  String,
    start: f64,
    end:   f64,
}

#[derive(Debug, Deserialize)]
struct WhisperSegment {
    text:  String,
    start: f64,
    end:   f64,
}

// ---------------------------------------------------------------------------
// Public transcription entry point
// ---------------------------------------------------------------------------

/// Transcribe an audio file using the OpenAI Whisper API.
/// Applies hallucination stripping and timestamp freeze detection,
/// mirroring the logic in wxstream_pipeline.py.
pub async fn transcribe(
    cfg: &OpenAiConfig,
    audio_path: &Path,
    prompt: &str,
) -> Result<TranscriptionDoc> {
    let audio_bytes = tokio::fs::read(audio_path)
        .await
        .with_context(|| format!("Failed to read audio file: {}", audio_path.display()))?;

    let filename = audio_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("audio.mp3")
        .to_string();

    let content_type = if filename.ends_with(".wav") {
        "audio/wav"
    } else {
        "audio/mpeg"
    };

    let file_part = multipart::Part::bytes(audio_bytes)
        .file_name(filename)
        .mime_str(content_type)?;

    let form = multipart::Form::new()
        .part("file", file_part)
        .text("model", cfg.model.clone())
        .text("language", "en")
        .text("temperature", cfg.temperature.to_string())
        .text("response_format", "verbose_json")
        .text("timestamp_granularities[]", "word")
        .text("timestamp_granularities[]", "segment")
        .text("prompt", prompt.to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post(WHISPER_URL)
        .bearer_auth(&cfg.api_key)
        .multipart(form)
        .send()
        .await
        .context("Whisper API request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body   = resp.text().await.unwrap_or_default();
        bail!("Whisper API error {}: {}", status, body);
    }

    let whisper: WhisperResponse = resp
        .json()
        .await
        .context("Failed to parse Whisper response")?;

    process_whisper_response(whisper)
}

// ---------------------------------------------------------------------------
// Response processing — mirrors wxstream_pipeline.py logic exactly
// ---------------------------------------------------------------------------

fn process_whisper_response(whisper: WhisperResponse) -> Result<TranscriptionDoc> {
    let raw_text   = whisper.text.trim().to_string();
    let word_count = raw_text.split_whitespace().count();

    // Convert raw word timestamps
    let raw_word_timestamps: Vec<WordTimestamp> = whisper
        .words
        .unwrap_or_default()
        .into_iter()
        .map(|w| WordTimestamp {
            word:  w.word,
            start: round3(w.start),
            end:   round3(w.end),
        })
        .collect();

    // Convert segment timestamps
    let segment_timestamps: Vec<SegmentTimestamp> = whisper
        .segments
        .unwrap_or_default()
        .into_iter()
        .map(|s| SegmentTimestamp {
            text:  s.text.trim().to_string(),
            start: round3(s.start),
            end:   round3(s.end),
        })
        .collect();

    // Detect word timestamp collapse — fall back to segment timestamps
    let timestamp_collapsed =
        raw_word_timestamps.len() < usize::max(10, word_count / 10);

    let (word_timestamps, timestamp_source) = if timestamp_collapsed {
        tracing::warn!(
            "Word timestamp collapse: {} timestamps for {} words — using segment fallback",
            raw_word_timestamps.len(),
            word_count
        );

        // Distribute segment timestamps evenly across tokens
        let mut expanded: Vec<WordTimestamp> = Vec::new();
        for seg in &segment_timestamps {
            let seg_words: Vec<&str> = seg.text.split_whitespace().collect();
            if seg_words.is_empty() {
                continue;
            }
            let duration = seg.end - seg.start;
            let step     = duration / seg_words.len() as f64;
            for (k, w) in seg_words.iter().enumerate() {
                expanded.push(WordTimestamp {
                    word:  w.to_string(),
                    start: round3(seg.start + k as f64 * step),
                    end:   round3(seg.start + (k + 1) as f64 * step),
                });
            }
        }
        (expanded, "segment".to_string())
    } else {
        (raw_word_timestamps.clone(), "word".to_string())
    };

    // Apply hallucination stripping
    let (cleaned_text, cleaned_words, was_cleaned, removed_chars) =
        strip_hallucinations(&raw_text, &word_timestamps);

    // Apply timestamp freeze detection
    let freeze_idx = find_timestamp_freeze_point(&cleaned_words);
    let final_words = if freeze_idx < cleaned_words.len() {
        tracing::warn!(
            "Timestamp freeze at word {} — truncating {} words",
            freeze_idx,
            cleaned_words.len() - freeze_idx
        );
        cleaned_words[..freeze_idx].to_vec()
    } else {
        cleaned_words
    };

    if was_cleaned {
        tracing::info!(
            "Hallucination stripped: {} chars removed",
            removed_chars
        );
    }

    Ok(TranscriptionDoc {
        raw_transcript:      Some(raw_text),
        cleaned_transcript:  Some(cleaned_text),
        hallucination_chars: Some(removed_chars as i32),
        word_timestamps:     final_words,
        segment_timestamps,
        timestamp_source:    Some(timestamp_source),
    })
}

// ---------------------------------------------------------------------------
// Hallucination detection — mirrors strip_hallucinations() in pipeline
// ---------------------------------------------------------------------------

fn strip_hallucinations(
    text: &str,
    words: &[WordTimestamp],
) -> (String, Vec<WordTimestamp>, bool, usize) {
    let original_len  = text.len();
    let mut token_list: Vec<&str> = text.split_whitespace().collect();

    let min_phrase_words = 4usize;
    let min_repeats      = 3usize;

    if token_list.len() < min_phrase_words * min_repeats {
        return (text.to_string(), words.to_vec(), false, 0);
    }

    let max_phrase = usize::min(token_list.len() / min_repeats, 60);
    let mut i = 0;

    'outer: loop {
        if i >= token_list.len() {
            break;
        }
        for phrase_len in (min_phrase_words..=max_phrase).rev() {
            if i + phrase_len > token_list.len() {
                continue;
            }
            let phrase = &token_list[i..i + phrase_len];
            let mut count = 1usize;
            let mut j = i + phrase_len;
            while j + phrase_len <= token_list.len()
                && token_list[j..j + phrase_len] == *phrase
            {
                count += 1;
                j     += phrase_len;
            }
            if count >= min_repeats {
                token_list.drain(i + phrase_len..j);
                continue 'outer;
            }
        }
        i += 1;
    }

    let cleaned_text  = token_list.join(" ");
    let was_cleaned   = cleaned_text != text;
    let removed_chars = original_len.saturating_sub(cleaned_text.len());

    (cleaned_text, words.to_vec(), was_cleaned, removed_chars)
}

/// Find the index where Whisper's word alignment freezes.
/// Mirrors _find_timestamp_freeze_point() in wxstream_pipeline.py.
fn find_timestamp_freeze_point(words: &[WordTimestamp]) -> usize {
    if words.is_empty() {
        return 0;
    }
    let mut frozen_since: Option<usize> = None;
    for i in 1..words.len() {
        if words[i].start - words[i - 1].start < MIN_ADVANCE_S {
            if frozen_since.is_none() {
                frozen_since = Some(i - 1);
            }
            if i - frozen_since.unwrap() >= FREEZE_RUN - 1 {
                return frozen_since.unwrap();
            }
        } else {
            frozen_since = None;
        }
    }
    words.len()
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}
