use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    config::OpenAiConfig,
    models::{MetarEntry, QualityResult, QualityStatus},
};

// ---------------------------------------------------------------------------
// OpenAI API types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct OpenAiRequest {
    model:       String,
    max_tokens:  u32,
    messages:    Vec<OpenAiMessage>,
    temperature: f32,
}

#[derive(Serialize)]
struct OpenAiMessage {
    role:    String,
    content: String,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: OpenAiChoiceMessage,
}

#[derive(Deserialize)]
struct OpenAiChoiceMessage {
    content: String,
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

const SYSTEM_PROMPT: &str = "You are a quality control agent for AWOS/ASOS aviation weather data. \
You will be given a parsed METAR record and the raw transcript it was parsed from. \
Your job is to answer ONLY the specific checklist questions below. \
Do not perform any checks not listed. Do not comment on anything not listed. \
Do not flag anything not listed. Answer each question strictly based on evidence \
in the transcript.\n\n\
CHECKLIST — answer each item:\n\
1. WIND_NULL: Is wind null/N/A AND the transcript contains a readable wind value (direction and speed)? \
   Note: variable wind (VRB) with null direction is correct. Calm wind is correct. \
   Only flag if a specific direction+speed is spoken but both are null.\n\
2. VISIBILITY_NULL: Is visibility null/N/A AND the transcript contains a readable visibility value? \
   Note: >10 SM is correct for more than one zero/ten. Missing sensor (transcript says visibility missing) is correct as null.\n\
3. SKY_NULL: Is sky null/N/A AND the transcript mentions a sky coverage word (few/scattered/broken/overcast/ceiling)? \
   Note: sky condition missing in transcript means null is correct.\n\
4. TEMP_NULL: Is temperature null/N/A AND the transcript contains a spoken temperature value (not the word missing)?\n\
5. DEWPOINT_NULL: Is dewpoint null/N/A AND the transcript contains a spoken dewpoint value (not the word missing)?\n\
6. ALTIMETER_NULL: Is altimeter null/N/A AND the transcript contains a spoken altimeter value?\n\
7. WIND_IMPLAUSIBLE: Is wind speed greater than 100 knots OR direction outside 0-360 degrees? \
   Only flag actual parsed numeric values, not null.\n\
8. TEMP_IMPLAUSIBLE: Is temperature outside -60C to +50C? Only flag actual parsed numeric values.\n\
9. ALTIMETER_IMPLAUSIBLE: Is altimeter outside 27.50-32.00 inHg? Only flag actual parsed numeric values.\n\
10. PHENOMENA_MISSED: Does the transcript explicitly mention a weather phenomenon \
   (rain, snow, fog, mist, haze, thunderstorm, drizzle, ice) that is completely absent from the phenomena list? \
   Only flag if the word is clearly present as a weather report, not in a sensor-missing context. \
   Intensity prefixes (-RA, +TS, -BR) count as the phenomenon being present.\n\n\
For each flagged item provide the corrected value ONLY if you can extract it directly and unambiguously \
from the transcript text. Do not guess or infer values not explicitly spoken.\n\n\
Respond ONLY with valid JSON, no preamble or markdown:\n\
{\n\
  \"confidence\": <float 0.0-1.0>,\n\
  \"flagged_fields\": [<list of field names with issues, empty if none>],\n\
  \"notes\": \"<one sentence summary of issues found, or 'No issues found'>\",\n\
  \"corrections\": null or {<field>: <corrected_value>},\n\
  \"needs_review\": <true if any checklist item fired, false otherwise>\n\
}";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub async fn run_quality_check(
    cfg:                &OpenAiConfig,
    site_id:            &str,
    raw_transcript:     &str,
    cleaned_transcript: Option<&str>,
    metar:              &MetarEntry,
) -> Result<(QualityResult, QualityStatus)> {
    let user_content = build_user_prompt(site_id, raw_transcript, cleaned_transcript, metar);

    let request = OpenAiRequest {
        model:       "gpt-4o-mini".to_string(),
        max_tokens:  1024,
        temperature: 0.0,
        messages:    vec![
            OpenAiMessage {
                role:    "system".to_string(),
                content: SYSTEM_PROMPT.to_string(),
            },
            OpenAiMessage {
                role:    "user".to_string(),
                content: user_content,
            },
        ],
    };

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(&cfg.api_key)
        .json(&request)
        .send()
        .await
        .context("OpenAI API request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body   = resp.text().await.unwrap_or_default();
        bail!("OpenAI API error {}: {}", status, body);
    }

    let openai_resp: OpenAiResponse = resp
        .json()
        .await
        .context("Failed to parse OpenAI response")?;

    let text = openai_resp.choices
        .first()
        .map(|c| c.message.content.clone())
        .unwrap_or_default();

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
        model:          Some("gpt-4o-mini".to_string()),
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
        "Site: {site_id}\n\nTRANSCRIPT:\n{transcript}\n\nPARSED METAR DATA:\n{parsed}\n\nAnswer the checklist.",
        site_id    = site_id,
        transcript = transcript_text,
        parsed     = metar_json,
    )
}
