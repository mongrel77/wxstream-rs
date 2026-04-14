use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use crate::models::Station;

pub fn load(path: &Path) -> Result<HashMap<String, Station>> {
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read stations file: {}", path.display()))?;

    let stations: Vec<Station> = serde_json::from_str(&data)
        .context("Failed to parse stations JSON")?;

    let count = stations.len();
    let map: HashMap<String, Station> = stations
        .into_iter()
        .map(|s| (s.id.clone(), s))
        .collect();

    tracing::info!("Loaded {} stations from {}", count, path.display());
    Ok(map)
}
