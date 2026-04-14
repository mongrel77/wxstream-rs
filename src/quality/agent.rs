use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    config::AnthropicConfig,
    models::{MetarEntry, QualityResult, QualityStatus},
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

#[derive(Debug, Deserialize)]
struct AgentResult {
    confidence:     f64,
    flagged_fields: Vec<String>,
    notes:          String,
    corrections:    Option<serde_json::Value>,
    needs_review:   bool,
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT: &str = r#"You are a quality control agent for AWOS/ASOS weather data parsed from audio transcriptions.

Review the parsed weather data alongside the raw transcript and:
1. Identify fields marked N/A that could be extracted with more careful reading
2. Flag implausible values (e.g. temperature of 85C, altimeter of 50.00)
3. Validate METAR format correctness
4. Check internal consistency (dewpoint must not exceed temperature)

Valid ranges:
- Wind direction: 000-360 degrees
- Wind speed: 0-100 knots (gusts up to 120)
- Visibility: 0-10 SM (can be >10 with > prefix)
- Sky height: 100-25000 ft
- Temperature: -60C to +50C
- Dewpoint: -80C to +35C (always <= temperature)
- Altimeter: 28.00-31.00 inHg

Respond ONLY with valid JSON, no preamble or markdown:
{
  "confidence": <float 0.0-1.0>,
  "flagged_fields": [<field names with issues>],
  "notes": "<brief explanation>",
  "corrections": null or {<field>: <corrected_value>},
  "needs_review": <true|false>
}

Flag for review when: confidence < 0.7, implausible values, dewpoint > temperature, malformed METAR, or multiple N/A fields that should have values."#;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub async fn run_quality_check(
    cfg:                &AnthropicConfig,
    site_id:            &str,
    raw_transcript:     &str,
    cleaned_transcript: Option<&str>,
    metar:              &MetarEntry,
) -> Result<(QualityResult, QualityStatus)> {
    let user_content = build_user_prompt(site_id, raw_transcript, cleaned_transcript, metar);

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

    let text = claude_resp.content.iter()
        .filter(|c| c.content_type == "text")
        .filter_map(|c| c.text.as_ref())
        .cloned()
        .collect::<Vec<_>>()
        .join("");

    let clean = text.trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();

    let result: AgentResult = serde_json::from_str(clean)
        .with_context(|| format!("Failed to parse quality agent JSON: {}", clean))?;

    let status = if result.needs_review {
        QualityStatus::NeedsReview
    } else {
        QualityStatus::Validated
    };

    let quality_result = QualityResult {
        reviewed_at:    Some(chrono::Utc::now()),
        model:          Some(cfg.model.clone()),
        confidence:     Some(result.confidence),
        flagged_fields: result.flagged_fields,
        notes:          Some(result.notes),
        corrections:    result.corrections,
        human_reviewed: false,
        human_notes:    None,
    };

    Ok((quality_result, status))
}

// ---------------------------------------------------------------------------
// Prompt builder
// ---------------------------------------------------------------------------

fn build_user_prompt(
    site_id:            &str,
    raw_transcript:     &str,
    cleaned_transcript: Option<&str>,
    metar:              &MetarEntry,
) -> String {
    let transcript_text = cleaned_transcript.unwrap_or(raw_transcript);
    let metar_json = serde_json::to_string_pretty(metar)
        .unwrap_or_else(|_| "(serialization error)".to_string());

    format!(
        "Site: {site_id}\n\nTRANSCRIPT:\n{transcript}\n\nPARSED METAR DATA:\n{parsed}\n\nPlease review for accuracy.",
        site_id    = site_id,
        transcript = transcript_text,
        parsed     = metar_json,
    )
}
