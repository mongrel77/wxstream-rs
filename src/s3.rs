use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::config::S3Config;

// ---------------------------------------------------------------------------
// S3 client builder
// ---------------------------------------------------------------------------

pub async fn build_client(cfg: &S3Config) -> Result<S3Client> {
    let region = aws_sdk_s3::config::Region::new(cfg.region.clone());
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    Ok(S3Client::new(&config))
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// Download an S3 object to a local temp file.
/// Returns the path to the downloaded file.
pub async fn download(
    client: &S3Client,
    bucket: &str,
    key: &str,
    dest: &Path,
) -> Result<()> {
    tracing::debug!("S3 download s3://{}/{} -> {}", bucket, key, dest.display());

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("S3 GetObject failed for s3://{}/{}", bucket, key))?;

    let body = resp.body.collect().await?.into_bytes();

    let mut file = File::create(dest)
        .await
        .with_context(|| format!("Failed to create local file: {}", dest.display()))?;

    file.write_all(&body)
        .await
        .context("Failed to write downloaded audio to disk")?;

    tracing::debug!(
        "Downloaded {} bytes from s3://{}/{}",
        body.len(),
        bucket,
        key
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/// Upload a local file to S3.
pub async fn upload(
    client: &S3Client,
    bucket: &str,
    key: &str,
    src: &Path,
    content_type: &str,
) -> Result<()> {
    tracing::debug!("S3 upload {} -> s3://{}/{}", src.display(), bucket, key);

    let body = tokio::fs::read(src)
        .await
        .with_context(|| format!("Failed to read local file for upload: {}", src.display()))?;

    let byte_stream = aws_sdk_s3::primitives::ByteStream::from(body);

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type(content_type)
        .body(byte_stream)
        .send()
        .await
        .with_context(|| format!("S3 PutObject failed for s3://{}/{}", bucket, key))?;

    tracing::info!("Uploaded to s3://{}/{}", bucket, key);
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive the content type from a file extension.
pub fn content_type_for(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .as_deref()
    {
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("m4a") => "audio/mp4",
        _ => "application/octet-stream",
    }
}

/// Extract just the filename component from an S3 key.
pub fn filename_from_key(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}
