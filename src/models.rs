use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Status enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawStatus {
    NotProcessed,
    Processing,
    Transcribed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrimStatus {
    Pending,
    Processing,
    Completed,
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
// Word timestamp (matches Whisper verbose_json output)
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
// Transcription subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TranscriptionDoc {
    pub raw_transcript:       Option<String>,
    pub cleaned_transcript:   Option<String>,
    pub hallucination_chars:  Option<i32>,
    pub word_timestamps:      Vec<WordTimestamp>,
    pub segment_timestamps:   Vec<SegmentTimestamp>,
    /// "word" | "segment" | "none"
    pub timestamp_source:     Option<String>,
}

// ---------------------------------------------------------------------------
// Parsed weather subdocument (mirrors parse_transcripts.py output)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WindDoc {
    pub direction:  Option<String>,
    pub speed_kt:   Option<String>,
    pub gust_kt:    Option<String>,
    pub variable:   Option<bool>,
    pub calm:       Option<bool>,
    pub raw:        Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SkyConditionDoc {
    pub coverage:  String,   // FEW | SCT | BKN | OVC | CLR | SKC
    pub height_ft: Option<u32>,
    pub raw:       Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ParsedDoc {
    pub selected_loop_time: Option<String>,
    pub time:               Option<String>,
    pub wind:               Option<WindDoc>,
    pub visibility_sm:      Option<String>,
    pub sky:                Vec<SkyConditionDoc>,
    pub temperature_c:      Option<String>,
    pub dewpoint_c:         Option<String>,
    pub altimeter_inhg:     Option<String>,
    pub density_altitude_ft: Option<String>,
    pub remarks:            Option<String>,
    pub phenomena:          Vec<String>,
    pub metar:              Option<String>,
}

// ---------------------------------------------------------------------------
// Quality review subdocument
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QualityDoc {
    pub reviewed_at:    Option<DateTime<Utc>>,
    pub model:          Option<String>,
    pub confidence:     Option<f64>,
    /// Fields flagged as uncertain by the quality agent
    pub flagged_fields: Vec<String>,
    /// Agent's notes / reasoning
    pub notes:          Option<String>,
    /// Suggested corrections (field -> corrected value)
    pub corrections:    Option<serde_json::Value>,
    /// Whether a human has reviewed this record
    pub human_reviewed: bool,
    pub human_notes:    Option<String>,
}

// ---------------------------------------------------------------------------
// AudioRecord — primary pipeline tracking document
// Collection: wxstream.audio_records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioRecord {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<bson::oid::ObjectId>,

    pub station_id:    String,
    pub recorded_at:   DateTime<Utc>,

    // S3 locations
    pub raw_s3_key:     String,
    pub trimmed_s3_key: Option<String>,

    // Pipeline status flags
    pub raw_status:     RawStatus,
    pub trim_status:    TrimStatus,
    pub quality_status: QualityStatus,

    // Subdocuments populated by each job
    pub transcription:  TranscriptionDoc,
    pub parsed:         Option<ParsedDoc>,
    pub quality:        Option<QualityDoc>,

    // Audit
    pub created_at:     DateTime<Utc>,
    pub updated_at:     DateTime<Utc>,
    pub error:          Option<String>,
}

impl AudioRecord {
    pub fn new(station_id: String, raw_s3_key: String, recorded_at: DateTime<Utc>) -> Self {
        let now = Utc::now();
        Self {
            id:             None,
            station_id,
            recorded_at,
            raw_s3_key,
            trimmed_s3_key: None,
            raw_status:     RawStatus::NotProcessed,
            trim_status:    TrimStatus::Pending,
            quality_status: QualityStatus::Pending,
            transcription:  TranscriptionDoc::default(),
            parsed:         None,
            quality:        None,
            created_at:     now,
            updated_at:     now,
            error:          None,
        }
    }
}

// ---------------------------------------------------------------------------
// WeatherObservation — clean final output document
// Collection: wxstream.weather_observations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherObservation {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<bson::oid::ObjectId>,

    /// Reference back to the AudioRecord that produced this observation
    pub audio_record_id: bson::oid::ObjectId,

    pub station_id:   String,
    pub observed_at:  DateTime<Utc>,

    // Parsed weather fields
    pub wind:             Option<WindDoc>,
    pub visibility_sm:    Option<String>,
    pub sky:              Vec<SkyConditionDoc>,
    pub temperature_c:    Option<String>,
    pub dewpoint_c:       Option<String>,
    pub altimeter_inhg:   Option<String>,
    pub density_altitude_ft: Option<String>,
    pub phenomena:        Vec<String>,
    pub remarks:          Option<String>,
    pub metar:            Option<String>,

    // S3 location of trimmed audio
    pub trimmed_s3_key:   Option<String>,

    // Quality
    pub quality_status:   QualityStatus,
    pub quality_notes:    Option<String>,

    pub created_at:  DateTime<Utc>,
    pub updated_at:  DateTime<Utc>,
}

impl WeatherObservation {
    pub fn from_audio_record(
        record: &AudioRecord,
        record_id: bson::oid::ObjectId,
    ) -> Self {
        let now = Utc::now();
        let parsed = record.parsed.clone().unwrap_or_default();
        Self {
            id:               None,
            audio_record_id:  record_id,
            station_id:       record.station_id.clone(),
            observed_at:      record.recorded_at,
            wind:             parsed.wind,
            visibility_sm:    parsed.visibility_sm,
            sky:              parsed.sky,
            temperature_c:    parsed.temperature_c,
            dewpoint_c:       parsed.dewpoint_c,
            altimeter_inhg:   parsed.altimeter_inhg,
            density_altitude_ft: parsed.density_altitude_ft,
            phenomena:        parsed.phenomena,
            remarks:          parsed.remarks,
            metar:            parsed.metar,
            trimmed_s3_key:   record.trimmed_s3_key.clone(),
            quality_status:   record.quality_status.clone(),
            quality_notes:    None,
            created_at:       now,
            updated_at:       now,
        }
    }
}

// ---------------------------------------------------------------------------
// Station metadata (loaded from JSON file)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id:       String,
    pub location: String,
    #[serde(rename = "type")]
    pub stn_type: String,
    pub phone:    Option<String>,
    pub lat:      Option<f64>,
    pub lon:      Option<f64>,
    pub elev_ft:  Option<f64>,
}
