use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub mongodb:    MongoConfig,
    pub s3:         S3Config,
    pub openai:     OpenAiConfig,
    pub anthropic:  AnthropicConfig,
    pub jobs:       JobsConfig,
    pub trim:       TrimConfig,
    pub silence_strip: SilenceStripConfig,
    pub stations:   StationsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MongoConfig {
    pub uri:                              String,
    pub database:                         String,
    pub audio_records_collection:         String,
    pub weather_observations_collection:  String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub region:      String,
    pub bucket:      String,
    pub raw_prefix:  String,
    pub trim_prefix: String,
}

impl S3Config {
    /// S3 key for a raw audio chunk: recordings/KAIZ/raw/filename.wav
    pub fn raw_key(&self, station_id: &str, filename: &str) -> String {
        format!("{}/{}/raw/{}", self.raw_prefix, station_id, filename)
    }

    /// S3 key for a trimmed audio chunk: recordings/KAIZ/trimmed/filename.wav
    pub fn trimmed_key(&self, station_id: &str, filename: &str) -> String {
        format!("{}/{}/trimmed/{}", self.trim_prefix, station_id, filename)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct OpenAiConfig {
    pub api_key:     String,
    pub model:       String,
    pub temperature: f32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AnthropicConfig {
    pub api_key:    String,
    pub model:      String,
    pub max_tokens: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JobsConfig {
    pub transcribe_poll_interval_s: u64,
    pub parse_poll_interval_s:      u64,
    pub trim_poll_interval_s:       u64,
    pub quality_poll_interval_s:    u64,

    pub transcribe_concurrency:     usize,
    pub parse_concurrency:          usize,
    pub trim_concurrency:           usize,
    pub quality_concurrency:        usize,

    pub min_audio_duration_s:       f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TrimConfig {
    pub preroll_s:    f64,
    pub trailing_db:  f64,
    pub min_loop_s:   f64,
    pub max_loop_s:   f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SilenceStripConfig {
    pub threshold_db:  f64,
    pub min_silence_s: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StationsConfig {
    pub json_path: PathBuf,
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

pub fn load() -> Result<Config> {
    // Allow an explicit config file path via env, otherwise look for config.toml
    // in the working directory.
    let config_path = std::env::var("WXSTREAM_CONFIG")
        .unwrap_or_else(|_| "config.toml".to_string());

    let cfg = config::Config::builder()
        .add_source(config::File::with_name(&config_path).required(false))
        // Environment variables with prefix WXSTREAM__ override file values.
        // Double underscore separates sections: WXSTREAM__OPENAI__API_KEY
        .add_source(
            config::Environment::with_prefix("WXSTREAM")
                .separator("__")
                .try_parsing(true),
        )
        // Also accept bare OPENAI_API_KEY and ANTHROPIC_API_KEY for convenience.
        .set_override_option(
            "openai.api_key",
            std::env::var("OPENAI_API_KEY").ok(),
        )?
        .set_override_option(
            "anthropic.api_key",
            std::env::var("ANTHROPIC_API_KEY").ok(),
        )?
        .set_override_option(
            "mongodb.uri",
            std::env::var("MONGO_URI")
                .ok()
                .or_else(|| {
                    // Reconstruct URI from password env var (matches existing Python pattern)
                    std::env::var("MONGO_DB_PASSWORD").ok().map(|pw| {
                        format!(
                            "mongodb+srv://remote:{}@prod.vew9qwt.mongodb.net/?appName=prod",
                            pw
                        )
                    })
                }),
        )?
        .build()
        .context("Failed to build configuration")?;

    cfg.try_deserialize::<Config>()
        .context("Failed to deserialize configuration")
}
