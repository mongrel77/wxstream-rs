#![allow(dead_code)]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use bson::oid::ObjectId;

// ---------------------------------------------------------------------------
// Processing job stages and statuses
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStage {
    Transcribe,
    Parse,
    Trim,
    Quality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    NotStarted,
    Pending,
    Done,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityStatus {
    Pending,
    Processing,
    Validated,
    NeedsReview,
    Failed,
}

// ---------------------------------------------------------------------------
// Site (from sites collection, _id is station string e.g. "KAIZ")
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Site {
    #[serde(rename = "_id")]
    pub id:                       String,
    pub loc_name:                 String,
    pub county:                   Option<String>,
    pub state:                    Option<String>,
    pub frequency:                Option<f64>,
    pub phone:                    Option<String>,
    #[serde(rename = "type")]
    pub site_type:                String,
    pub silence_threshold_ms:     Option<u32>,
    pub rms_silence_threshold_db: Option<f64>,
    pub created_at:               Option<bson::DateTime>,
    pub updated_at:               Option<bson::DateTime>,
}

// ---------------------------------------------------------------------------
// S3 audio location subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AudioLocation {
    pub bucket:     String,
    pub object_key: String,
}

// ---------------------------------------------------------------------------
// AudioRecording — audio_recordings collection
// Remote sites insert with type "raw". We append type "trimmed" after trim.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioRecording {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id:            Option<ObjectId>,

    pub site_id:       String,
    pub recorded:      bson::DateTime,

    #[serde(rename = "type")]
    pub rec_type:      String,   // "raw"

    pub bucket:        String,
    pub object_key:    String,

    pub created_at:    Option<bson::DateTime>,
    pub updated_at:    Option<bson::DateTime>,
}

// ---------------------------------------------------------------------------
// ProcessingJob — processing_jobs collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingJob {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id:                 Option<ObjectId>,

    pub audio_recording_id: ObjectId,
    pub site_id:            String,
    pub stage:              JobStage,
    pub status:             JobStatus,
    pub error:              Option<String>,

    pub created_at:         bson::DateTime,
    pub updated_at:         bson::DateTime,
}

impl ProcessingJob {
    pub fn new(audio_recording_id: ObjectId, site_id: String, stage: JobStage) -> Self {
        let now = bson::DateTime::now();
        Self {
            id: None,
            audio_recording_id,
            site_id,
            stage,
            status: JobStatus::NotStarted,
            error:  None,
            created_at: now,
            updated_at: now,
        }
    }
}

// ---------------------------------------------------------------------------
// Word and segment timestamps
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WordTimestamp {
    pub word:  String,
    pub start: f64,
    pub end:   f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentTimestamp {
    pub text:  String,
    pub start: f64,
    pub end:   f64,
}

// ---------------------------------------------------------------------------
// Transcription — transcriptions collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transcription {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id:                    Option<ObjectId>,

    pub audio_recording_id:    ObjectId,
    pub site_id:               String,

    pub raw_transcript:        String,
    pub word_timestamps:       Vec<WordTimestamp>,
    pub segment_timestamps:    Vec<SegmentTimestamp>,
    pub timestamp_source:      String,

    pub cleaned_transcript:    Option<String>,
    pub hallucination_chars:   Option<i32>,

    pub created_at:            DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Wind subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindEntry {
    pub direction:  Option<String>,
    pub speed_kt:   Option<String>,
    pub gust_kt:    Option<String>,
    pub variable:   Option<bool>,
    pub calm:       Option<bool>,
    pub raw:        Option<String>,
    pub metar:      Option<String>,
}

// ---------------------------------------------------------------------------
// Sky condition subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SkyCondition {
    pub coverage:  String,
    pub height_ft: Option<u32>,
}

// ---------------------------------------------------------------------------
// MetarEntry — metar_entries collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetarEntry {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id:                  Option<ObjectId>,

    pub audio_recording_id:  ObjectId,
    pub transcription_id:    ObjectId,
    pub site_id:             String,
    pub observed_at:         DateTime<Utc>,

    pub time:                Option<String>,
    pub wind:                Option<WindEntry>,
    pub visibility_sm:       Option<String>,
    pub sky:                 Vec<SkyCondition>,
    pub temperature_c:       Option<String>,
    pub dewpoint_c:          Option<String>,
    pub altimeter_inhg:      Option<String>,
    pub density_altitude_ft: Option<String>,
    pub phenomena:           Vec<String>,
    pub remarks:             Option<String>,
    pub metar:               Option<String>,
    pub selected_loop_time:  Option<String>,

    pub quality_status:      QualityStatus,
    pub quality:             Option<QualityResult>,

    pub created_at:          DateTime<Utc>,
    pub updated_at:          DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Quality result subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QualityResult {
    pub reviewed_at:    Option<DateTime<Utc>>,
    pub model:          Option<String>,
    pub confidence:     Option<f64>,
    pub flagged_fields: Vec<String>,
    pub notes:          Option<String>,
    pub corrections:    Option<serde_json::Value>,
    pub human_reviewed: bool,
    pub human_notes:    Option<String>,
}
