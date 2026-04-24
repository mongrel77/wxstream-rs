use anyhow::{Context, Result};
use bson::{doc, oid::ObjectId, DateTime as BsonDateTime};
use chrono::Utc;
use mongodb::{
    options::{ClientOptions, FindOneAndUpdateOptions, ReturnDocument},
    Client, Collection, Database,
};

use crate::{
    config::MongoConfig,
    models::{AudioRecord, QualityStatus, WeatherObservation},
};

// ---------------------------------------------------------------------------
// Handle — wraps the MongoDB client and exposes typed collection accessors
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Db {
    pub database: Database,
    cfg: MongoConfig,
}

impl Db {
    pub async fn connect(cfg: &MongoConfig) -> Result<Self> {
        let opts = ClientOptions::parse(&cfg.uri)
            .await
            .context("Failed to parse MongoDB URI")?;

        let client = Client::with_options(opts)
            .context("Failed to create MongoDB client")?;

        // Ping to verify connectivity
        client
            .database("admin")
            .run_command(doc! { "ping": 1 }, None)
            .await
            .context("MongoDB ping failed — check URI and network")?;

        tracing::info!("Connected to MongoDB Atlas");

        let database = client.database(&cfg.database);
        Ok(Self { database, cfg: cfg.clone() })
    }

    pub fn audio_records(&self) -> Collection<AudioRecord> {
        self.database.collection(&self.cfg.audio_records_collection)
    }

    pub fn weather_observations(&self) -> Collection<WeatherObservation> {
        self.database
            .collection(&self.cfg.weather_observations_collection)
    }

    // -----------------------------------------------------------------------
    // Index setup — call once at startup
    // -----------------------------------------------------------------------

    pub async fn ensure_indexes(&self) -> Result<()> {
        use mongodb::IndexModel;
        use mongodb::options::IndexOptions;

        let ar = self.audio_records();

        // Compound index for job polling — most critical for performance
        ar.create_index(
            IndexModel::builder()
                .keys(doc! { "raw_status": 1, "created_at": 1 })
                .options(IndexOptions::builder().name("raw_status_created".to_string()).build())
                .build(),
            None,
        )
        .await?;

        ar.create_index(
            IndexModel::builder()
                .keys(doc! { "trim_status": 1, "updated_at": 1 })
                .options(IndexOptions::builder().name("trim_status_updated".to_string()).build())
                .build(),
            None,
        )
        .await?;

        ar.create_index(
            IndexModel::builder()
                .keys(doc! { "quality_status": 1, "updated_at": 1 })
                .options(IndexOptions::builder().name("quality_status_updated".to_string()).build())
                .build(),
            None,
        )
        .await?;

        ar.create_index(
            IndexModel::builder()
                .keys(doc! { "station_id": 1, "recorded_at": -1 })
                .options(IndexOptions::builder().name("station_recorded".to_string()).build())
                .build(),
            None,
        )
        .await?;

        // Unique index on raw_s3_key — prevents duplicate processing
        ar.create_index(
            IndexModel::builder()
                .keys(doc! { "raw_s3_key": 1 })
                .options(
                    IndexOptions::builder()
                        .unique(true)
                        .name("raw_s3_key_unique".to_string())
                        .build(),
                )
                .build(),
            None,
        )
        .await?;

        let wo = self.weather_observations();
        wo.create_index(
            IndexModel::builder()
                .keys(doc! { "station_id": 1, "observed_at": -1 })
                .options(IndexOptions::builder().name("station_observed".to_string()).build())
                .build(),
            None,
        )
        .await?;

        wo.create_index(
            IndexModel::builder()
                .keys(doc! { "quality_status": 1 })
                .options(IndexOptions::builder().name("quality_status".to_string()).build())
                .build(),
            None,
        )
        .await?;

        tracing::info!("MongoDB indexes ensured");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Transcribe job helpers
    // -----------------------------------------------------------------------

    /// Atomically claim one "not_processed" record for transcription.
    /// Uses findOneAndUpdate to prevent two workers claiming the same record.
    pub async fn claim_for_transcription(&self) -> Result<Option<AudioRecord>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "created_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let result = self
            .audio_records()
            .find_one_and_update(
                doc! { "raw_status": "not_processed" },
                doc! {
                    "$set": {
                        "raw_status": "processing",
                        "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                opts,
            )
            .await
            .context("claim_for_transcription failed")?;

        Ok(result)
    }

    /// Mark a record as successfully transcribed.
    pub async fn mark_transcribed(
        &self,
        id: ObjectId,
        transcription: &crate::models::TranscriptionDoc,
    ) -> Result<()> {
        let tx_bson = bson::to_bson(transcription)?;
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "raw_status":    "transcribed",
                        "transcription": tx_bson,
                        "updated_at":    BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                        "error":         bson::Bson::Null,
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    /// Mark a record as failed during transcription.
    pub async fn mark_transcription_failed(&self, id: ObjectId, error: &str) -> Result<()> {
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "raw_status": "failed",
                        "error":      error,
                        "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Parse job helpers
    // -----------------------------------------------------------------------

    /// Atomically claim one "transcribed" record for parsing.
    pub async fn claim_for_parsing(&self) -> Result<Option<AudioRecord>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "updated_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let result = self
            .audio_records()
            .find_one_and_update(
                doc! { "raw_status": "transcribed" },
                doc! {
                    "$set": {
                        "raw_status": "parsing",
                        "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                opts,
            )
            .await
            .context("claim_for_parsing failed")?;

        Ok(result)
    }

    /// Write parsed weather data and signal downstream jobs.
    pub async fn mark_parsed(
        &self,
        id: ObjectId,
        parsed: &crate::models::ParsedDoc,
    ) -> Result<()> {
        let parsed_bson = bson::to_bson(parsed)?;
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "raw_status":     "parsed",
                        "parsed":          parsed_bson,
                        "trim_status":    "pending",
                        "quality_status": "pending",
                        "updated_at":     BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                        "error":          bson::Bson::Null,
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn mark_parse_failed(&self, id: ObjectId, error: &str) -> Result<()> {
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "raw_status": "parse_failed",
                        "error":      error,
                        "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Trim job helpers
    // -----------------------------------------------------------------------

    /// Atomically claim one "pending" trim record.
    pub async fn claim_for_trim(&self) -> Result<Option<AudioRecord>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "updated_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let result = self
            .audio_records()
            .find_one_and_update(
                doc! { "trim_status": "pending", "raw_status": "parsed" },
                doc! {
                    "$set": {
                        "trim_status": "processing",
                        "updated_at":  BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                opts,
            )
            .await
            .context("claim_for_trim failed")?;

        Ok(result)
    }

    pub async fn mark_trim_completed(
        &self,
        id: ObjectId,
        trimmed_s3_key: &str,
    ) -> Result<()> {
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "trim_status":    "completed",
                        "trimmed_s3_key": trimmed_s3_key,
                        "updated_at":     BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn mark_trim_failed(&self, id: ObjectId, error: &str) -> Result<()> {
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "trim_status": "failed",
                        "error":       error,
                        "updated_at":  BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Quality job helpers
    // -----------------------------------------------------------------------

    /// Atomically claim one "pending" quality record.
    pub async fn claim_for_quality(&self) -> Result<Option<AudioRecord>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "updated_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let result = self
            .audio_records()
            .find_one_and_update(
                doc! { "quality_status": "pending", "raw_status": "parsed" },
                doc! {
                    "$set": {
                        "quality_status": "processing",
                        "updated_at":     BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                opts,
            )
            .await
            .context("claim_for_quality failed")?;

        Ok(result)
    }

    pub async fn mark_quality_result(
        &self,
        id: ObjectId,
        quality: &crate::models::QualityDoc,
        status: QualityStatus,
    ) -> Result<()> {
        let quality_bson = bson::to_bson(quality)?;
        let status_bson  = bson::to_bson(&status)?;
        self.audio_records()
            .update_one(
                doc! { "_id": id },
                doc! {
                    "$set": {
                        "quality_status": status_bson,
                        "quality":        quality_bson,
                        "updated_at":     BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                    }
                },
                None,
            )
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Weather observations
    // -----------------------------------------------------------------------

    /// Upsert a WeatherObservation keyed on audio_record_id.
    pub async fn upsert_weather_observation(&self, obs: &WeatherObservation) -> Result<ObjectId> {
        use mongodb::options::FindOneAndUpdateOptions;

        let obs_bson = bson::to_document(obs)?;
        let opts = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();

        let result = self
            .weather_observations()
            .find_one_and_update(
                doc! { "audio_record_id": obs.audio_record_id },
                doc! { "$set": obs_bson },
                opts,
            )
            .await?
            .context("upsert_weather_observation returned None")?;

        Ok(result.id.unwrap_or_else(ObjectId::new))
    }
}
