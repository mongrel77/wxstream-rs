use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    config::AnthropicConfig,
    models::{ParsedDoc, QualityDoc, QualityStatus, TranscriptionDoc},
};

// ---------------------------------------------------------------------------
// Claude API types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ClaudeRequest {
    model:      String,
    max_tokens: u32,
    system:     String,
    messages:   Vec<ClaudeMessage>,
}

#[derive(Serialize)]
struct ClaudeMessage {
    role:    String,
    content: String,
}

#[derive(Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContent>,
}

#[derive(Deserialize)]
struct ClaudeContent {
    #[serde(rename = "type")]
    content_type: String,
    text:         Option<String>,
}

// ---------------------------------------------------------------------------
// Quality agent output schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct QualityAgentResult {
    confidence:     f64,
    flagged_fields: Vec<String>,
    notes:          String,
    corrections:    Option<serde_json::Value>,
    needs_review:   bool,
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT: &str = r#"You are a quality control agent for AWOS (Automated Weather Observing System) and ASOS (Automated Surface Observing System) weather data parsed from audio transcriptions.

Your job is to review parsed weather data alongside the raw transcript and:
1. Identify any fields marked as "N/A" that could be extracted with more careful reading
2. Flag implausible values (e.g. temperature of 85°C, altimeter of 50.00)
3. Validate that METAR format is correct
4. Check that values are internally consistent (e.g. dewpoint should not exceed temperature)

Valid ranges for reference:
- Wind direction: 000-360 degrees
- Wind speed: 0-100 knots (gusts up to 120)
- Visibility: 0-10 SM (can be >10 with prefix)
- Sky height: 100-25000 ft
- Temperature: -60°C to +50°C
- Dewpoint: -80°C to +35°C (always <= temperature)
- Altimeter: 28.00-31.00 inHg

You must respond ONLY with a valid JSON object. No preamble, no markdown, no explanation outside the JSON.

Required JSON schema:
{
  "confidence": <float 0.0-1.0, overall confidence in parsed data quality>,
  "flagged_fields": [<list of field names with issues, e.g. "visibility", "altimeter">],
  "notes": "<brief explanation of findings>",
  "corrections": <null or object with field -> corrected_value for high-confidence corrections>,
  "needs_review": <true if human should review, false if data looks good>
}

Flag for review (needs_review: true) when:
- confidence < 0.7
- Any field has an implausible value
- Dewpoint exceeds temperature
- METAR format appears malformed
- Multiple N/A fields that should have values"#;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the Claude quality agent on a parsed record.
/// Returns the quality doc and the recommended status.
pub async fn run_quality_check(
    cfg:           &AnthropicConfig,
    station_id:    &str,
    transcription: &TranscriptionDoc,
    parsed:        &ParsedDoc,
) -> Result<(QualityDoc, QualityStatus)> {
    let user_content = build_user_prompt(station_id, transcription, parsed);

    let request = ClaudeRequest {
        model:      cfg.model.clone(),
        max_tokens: cfg.max_tokens,
        system:     SYSTEM_PROMPT.to_string(),
        messages:   vec![ClaudeMessage {
            role:    "user".to_string(),
            content: user_content,
        }],
    };

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &cfg.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
        .context("Claude API request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body   = resp.text().await.unwrap_or_default();
        bail!("Claude API error {}: {}", status, body);
    }

    let claude_resp: ClaudeResponse = resp
        .json()
        .await
        .context("Failed to parse Claude response")?;

    let text = claude_resp
        .content
        .iter()
        .filter(|c| c.content_type == "text")
        .filter_map(|c| c.text.as_ref())
        .cloned()
        .collect::<Vec<_>>()
        .join("");

    // Strip markdown fences if present
    let clean = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let result: QualityAgentResult = serde_json::from_str(clean)
        .with_context(|| format!("Failed to parse quality agent JSON: {}", clean))?;

    let status = if result.needs_review {
        QualityStatus::NeedsReview
    } else {
        QualityStatus::Validated
    };

    let quality_doc = QualityDoc {
        reviewed_at:    Some(chrono::Utc::now()),
        model:          Some(cfg.model.clone()),
        confidence:     Some(result.confidence),
        flagged_fields: result.flagged_fields,
        notes:          Some(result.notes),
        corrections:    result.corrections,
        human_reviewed: false,
        human_notes:    None,
    };

    Ok((quality_doc, status))
}

// ---------------------------------------------------------------------------
// Prompt builder
// ---------------------------------------------------------------------------

fn build_user_prompt(
    station_id:    &str,
    transcription: &TranscriptionDoc,
    parsed:        &ParsedDoc,
) -> String {
    let transcript_text = transcription
        .cleaned_transcript
        .as_deref()
        .or(transcription.raw_transcript.as_deref())
        .unwrap_or("(not available)");

    let parsed_json = serde_json::to_string_pretty(parsed)
        .unwrap_or_else(|_| "(serialization error)".to_string());

    format!(
        "Station: {station_id}\n\n\
        RAW TRANSCRIPT:\n{transcript}\n\n\
        PARSED DATA:\n{parsed}\n\n\
        Please review the parsed data for accuracy and completeness.",
        station_id = station_id,
        transcript = transcript_text,
        parsed     = parsed_json,
    )
}
