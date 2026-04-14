use anyhow::{Context, Result};
use bson::{doc, oid::ObjectId, DateTime as BsonDateTime};
use chrono::Utc;
use futures::TryStreamExt;
use mongodb::{
    options::{ClientOptions, FindOneAndUpdateOptions, IndexOptions, ReturnDocument},
    Client, Collection, Database, IndexModel,
};

use crate::{
    config::MongoConfig,
    models::{
        AudioRecording, JobStage, JobStatus, MetarEntry,
        ProcessingJob, QualityResult, QualityStatus, Site, Transcription,
    },
};

// ---------------------------------------------------------------------------
// Db handle
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Db {
    pub database: Database,
}

impl Db {
    pub async fn connect(cfg: &MongoConfig) -> Result<Self> {
        let opts = ClientOptions::parse(&cfg.uri)
            .await
            .context("Failed to parse MongoDB URI")?;

        let client = Client::with_options(opts)
            .context("Failed to create MongoDB client")?;

        client
            .database("admin")
            .run_command(doc! { "ping": 1 }, None)
            .await
            .context("MongoDB ping failed")?;

        tracing::info!("Connected to MongoDB Atlas (db: {})", cfg.database);
        Ok(Self { database: client.database(&cfg.database) })
    }

    // -----------------------------------------------------------------------
    // Collection accessors
    // -----------------------------------------------------------------------

    pub fn audio_recordings(&self) -> Collection<AudioRecording> {
        self.database.collection("audio_recordings")
    }

    pub fn processing_jobs(&self) -> Collection<ProcessingJob> {
        self.database.collection("processing_jobs")
    }

    pub fn transcriptions(&self) -> Collection<Transcription> {
        self.database.collection("transcriptions")
    }

    pub fn metar_entries(&self) -> Collection<MetarEntry> {
        self.database.collection("metar_entries")
    }

    pub fn sites(&self) -> Collection<Site> {
        self.database.collection("sites")
    }

    // -----------------------------------------------------------------------
    // Index setup
    // -----------------------------------------------------------------------

    pub async fn ensure_indexes(&self) -> Result<()> {
        // audio_recordings — scanner polls for type "raw"
        self.audio_recordings().create_index(
            IndexModel::builder()
                .keys(doc! { "type": 1, "created_at": 1 })
                .options(IndexOptions::builder().name("type_created".to_string()).build())
                .build(),
            None,
        ).await?;

        // processing_jobs — job workers poll by stage + status
        self.processing_jobs().create_index(
            IndexModel::builder()
                .keys(doc! { "stage": 1, "status": 1, "created_at": 1 })
                .options(IndexOptions::builder().name("stage_status_created".to_string()).build())
                .build(),
            None,
        ).await?;

        // Unique index — prevent duplicate jobs for same recording + stage
        self.processing_jobs().create_index(
            IndexModel::builder()
                .keys(doc! { "audio_recording_id": 1, "stage": 1 })
                .options(
                    IndexOptions::builder()
                        .unique(true)
                        .name("recording_stage_unique".to_string())
                        .build(),
                )
                .build(),
            None,
        ).await?;

        self.transcriptions().create_index(
            IndexModel::builder()
                .keys(doc! { "audio_recording_id": 1 })
                .options(IndexOptions::builder().name("audio_recording_id".to_string()).build())
                .build(),
            None,
        ).await?;

        self.metar_entries().create_index(
            IndexModel::builder()
                .keys(doc! { "audio_recording_id": 1 })
                .options(IndexOptions::builder().name("audio_recording_id".to_string()).build())
                .build(),
            None,
        ).await?;

        self.metar_entries().create_index(
            IndexModel::builder()
                .keys(doc! { "site_id": 1, "observed_at": -1 })
                .options(IndexOptions::builder().name("site_observed".to_string()).build())
                .build(),
            None,
        ).await?;

        tracing::info!("MongoDB indexes ensured");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Site loading
    // -----------------------------------------------------------------------

    pub async fn load_sites(&self) -> Result<std::collections::HashMap<String, Site>> {
        let mut cursor = self.sites().find(None, None).await?;
        let mut map    = std::collections::HashMap::new();
        while let Some(site) = cursor.try_next().await? {
            map.insert(site.id.clone(), site);
        }
        tracing::info!("Loaded {} sites from database", map.len());
        Ok(map)
    }

    // -----------------------------------------------------------------------
    // Scanner — finds unqueued raw recordings and creates transcribe jobs
    // -----------------------------------------------------------------------

    /// Find raw audio_recordings that don't yet have a transcribe job.
    pub async fn find_unqueued_raw_recordings(&self) -> Result<Vec<AudioRecording>> {
        // Get all audio_recording_ids that already have a transcribe job
        let existing: Vec<ObjectId> = self.processing_jobs()
            .find(doc! { "stage": "transcribe" }, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|j| j.audio_recording_id)
            .collect();

        // Find raw recordings not in that list
        let filter = if existing.is_empty() {
            doc! { "type": "raw" }
        } else {
            doc! { "type": "raw", "_id": { "$nin": existing } }
        };

        let recordings = self.audio_recordings()
            .find(filter, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(recordings)
    }

    /// Insert a processing job. Ignores duplicate key errors (job already exists).
    pub async fn create_job(&self, job: &ProcessingJob) -> Result<()> {
        match self.processing_jobs().insert_one(job, None).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Ignore duplicate key error (unique index on recording_id + stage)
                if e.to_string().contains("11000") {
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Transcribe job
    // -----------------------------------------------------------------------

    pub async fn claim_transcribe_job(&self) -> Result<Option<(ProcessingJob, AudioRecording)>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "created_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let job = self.processing_jobs()
            .find_one_and_update(
                doc! { "stage": "transcribe", "status": "not_started" },
                doc! { "$set": {
                    "status":     "pending",
                    "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                }},
                opts,
            )
            .await
            .context("claim_transcribe_job failed")?;

        let job = match job { Some(j) => j, None => return Ok(None) };

        let recording = self.audio_recordings()
            .find_one(doc! { "_id": job.audio_recording_id }, None)
            .await?
            .context("Audio recording not found for transcribe job")?;

        Ok(Some((job, recording)))
    }

    pub async fn complete_transcribe_job(
        &self,
        job_id:       ObjectId,
        recording_id: ObjectId,
        site_id:      &str,
        tx:           &Transcription,
    ) -> Result<()> {
        // Insert transcription
        self.transcriptions().insert_one(tx, None).await?;

        let now = BsonDateTime::from_millis(Utc::now().timestamp_millis());

        // Mark transcribe job done
        self.processing_jobs().update_one(
            doc! { "_id": job_id },
            doc! { "$set": { "status": "done", "updated_at": now.clone() }},
            None,
        ).await?;

        // Create parse job
        let parse_job = ProcessingJob::new(recording_id, site_id.to_string(), JobStage::Parse);
        self.create_job(&parse_job).await?;

        // Create trim job
        let trim_job = ProcessingJob::new(recording_id, site_id.to_string(), JobStage::Trim);
        self.create_job(&trim_job).await?;

        tracing::info!("[{}] Transcribe job done — parse + trim jobs created", site_id);
        Ok(())
    }

    pub async fn fail_job(&self, job_id: ObjectId, error: &str) -> Result<()> {
        self.processing_jobs().update_one(
            doc! { "_id": job_id },
            doc! { "$set": {
                "status":     "failed",
                "error":      error,
                "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
            }},
            None,
        ).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Parse job
    // -----------------------------------------------------------------------

    pub async fn claim_parse_job(&self) -> Result<Option<(ProcessingJob, Transcription)>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "created_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let job = self.processing_jobs()
            .find_one_and_update(
                doc! { "stage": "parse", "status": "not_started" },
                doc! { "$set": {
                    "status":     "pending",
                    "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                }},
                opts,
            )
            .await
            .context("claim_parse_job failed")?;

        let job = match job { Some(j) => j, None => return Ok(None) };

        let tx = self.transcriptions()
            .find_one(doc! { "audio_recording_id": job.audio_recording_id }, None)
            .await?
            .context("Transcription not found for parse job")?;

        Ok(Some((job, tx)))
    }

    pub async fn complete_parse_job(
        &self,
        job_id: ObjectId,
        metar:  &MetarEntry,
    ) -> Result<()> {
        self.metar_entries().insert_one(metar, None).await?;

        self.processing_jobs().update_one(
            doc! { "_id": job_id },
            doc! { "$set": {
                "status":     "done",
                "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
            }},
            None,
        ).await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Trim job
    // -----------------------------------------------------------------------

    pub async fn claim_trim_job(
        &self,
    ) -> Result<Option<(ProcessingJob, AudioRecording, Transcription, Option<String>)>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "created_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let job = self.processing_jobs()
            .find_one_and_update(
                doc! { "stage": "trim", "status": "not_started" },
                doc! { "$set": {
                    "status":     "pending",
                    "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                }},
                opts,
            )
            .await
            .context("claim_trim_job failed")?;

        let job = match job { Some(j) => j, None => return Ok(None) };

        let recording = self.audio_recordings()
            .find_one(doc! { "_id": job.audio_recording_id }, None)
            .await?
            .context("Audio recording not found for trim job")?;

        let tx = self.transcriptions()
            .find_one(doc! { "audio_recording_id": job.audio_recording_id }, None)
            .await?
            .context("Transcription not found for trim job")?;

        // Get selected_loop_time from metar entry if available
        let selected_loop_time = self.metar_entries()
            .find_one(doc! { "audio_recording_id": job.audio_recording_id }, None)
            .await?
            .and_then(|m| m.selected_loop_time);

        Ok(Some((job, recording, tx, selected_loop_time)))
    }

    pub async fn complete_trim_job(
        &self,
        job_id:         ObjectId,
        recording_id:   ObjectId,
        trimmed_bucket: &str,
        trimmed_key:    &str,
    ) -> Result<()> {
        // Append a "trimmed" entry to audio_recordings
        // We insert a new document with type "trimmed" referencing the original
        let now = BsonDateTime::from_millis(Utc::now().timestamp_millis());

        self.database.collection::<bson::Document>("audio_recordings")
            .update_one(
                doc! { "_id": recording_id },
                doc! { "$push": {
                    "trimmed": {
                        "bucket":     trimmed_bucket,
                        "object_key": trimmed_key,
                        "created_at": now.clone(),
                    }
                }},
                None,
            )
            .await?;

        // Mark trim job done
        self.processing_jobs().update_one(
            doc! { "_id": job_id },
            doc! { "$set": {
                "status":     "done",
                "updated_at": now,
            }},
            None,
        ).await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Quality job
    // -----------------------------------------------------------------------

    pub async fn claim_quality_job(&self) -> Result<Option<(ProcessingJob, MetarEntry, Transcription)>> {
        let opts = FindOneAndUpdateOptions::builder()
            .sort(doc! { "created_at": 1 })
            .return_document(ReturnDocument::After)
            .build();

        let job = self.processing_jobs()
            .find_one_and_update(
                doc! { "stage": "quality", "status": "not_started" },
                doc! { "$set": {
                    "status":     "pending",
                    "updated_at": BsonDateTime::from_millis(Utc::now().timestamp_millis()),
                }},
                opts,
            )
            .await
            .context("claim_quality_job failed")?;

        let job = match job { Some(j) => j, None => return Ok(None) };

        let metar = self.metar_entries()
            .find_one(doc! { "audio_recording_id": job.audio_recording_id }, None)
            .await?
            .context("MetarEntry not found for quality job")?;

        let tx = self.transcriptions()
            .find_one(doc! { "audio_recording_id": job.audio_recording_id }, None)
            .await?
            .context("Transcription not found for quality job")?;

        Ok(Some((job, metar, tx)))
    }

    pub async fn complete_quality_job(
        &self,
        job_id:   ObjectId,
        metar_id: ObjectId,
        quality:  &QualityResult,
        status:   QualityStatus,
    ) -> Result<()> {
        let quality_bson = bson::to_bson(quality)?;
        let status_bson  = bson::to_bson(&status)?;
        let now = BsonDateTime::from_millis(Utc::now().timestamp_millis());

        self.metar_entries().update_one(
            doc! { "_id": metar_id },
            doc! { "$set": {
                "quality_status": status_bson,
                "quality":        quality_bson,
                "updated_at":     now.clone(),
            }},
            None,
        ).await?;

        self.processing_jobs().update_one(
            doc! { "_id": job_id },
            doc! { "$set": { "status": "done", "updated_at": now }},
            None,
        ).await?;

        Ok(())
    }
}
