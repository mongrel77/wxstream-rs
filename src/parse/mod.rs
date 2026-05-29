pub mod altimeter;
pub mod audit;
pub mod local_info;
pub mod normalize;
pub mod sky;
pub mod temperature;
pub mod validate;
pub mod visibility;
pub mod wind;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;

use altimeter::{extract_altimeter, extract_phenomena, extract_remarks};
use local_info::extract_local_info;
use normalize::{normalize, strip_preamble, truncate_digit_storm};
use sky::extract_sky;
use temperature::extract_temp_dp;
use visibility::extract_visibility;
use wind::extract_wind;

// ---------------------------------------------------------------------------
// ParseInput / ParsedWeather - local types, decoupled from MongoDB models
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct ParseInput<'a> {
    pub raw_transcript: &'a str,
    pub station_id:     &'a str,
    pub location:       &'a str,
    pub station_type:   &'a str,
    pub recorded_at:    DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedWind {
    pub direction: Option<String>,
    pub speed_kt:  Option<String>,
    pub gust_kt:   Option<String>,
    pub variable:  Option<bool>,
    pub calm:      Option<bool>,
    pub raw:       Option<String>,
    pub metar:     Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedSky {
    pub coverage:  String,
    pub height_ft: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedWeather {
    pub selected_loop_time:  Option<String>,
    pub time:                Option<String>,
    pub wind:                Option<ParsedWind>,
    pub visibility_sm:       Option<String>,
    pub sky:                 Vec<ParsedSky>,
    pub temperature_c:       Option<String>,
    pub dewpoint_c:          Option<String>,
    pub altimeter_inhg:      Option<String>,
    pub density_altitude_ft: Option<String>,
    pub remarks:             Option<String>,
    pub phenomena:           Vec<String>,
    pub metar:               Option<String>,
    pub local_info:          Option<String>,
    pub validation_warnings: Vec<String>,
    pub audit_warnings:      Vec<String>,
}

// ---------------------------------------------------------------------------
// Majority-vote helpers
// ---------------------------------------------------------------------------

/// Split norm_full into per-loop segments and return them.
/// Returns an empty Vec if fewer than 2 loops are found.
fn loop_segments(norm_full: &str) -> Vec<String> {
    static LOOP_PAT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)automated\s+weather\s+observation[.\s,]+\d{4}[.\s,]*zulu[.\s,]*").unwrap()
    });
    let matches: Vec<_> = LOOP_PAT.find_iter(norm_full).collect();
    if matches.len() < 2 {
        return vec![];
    }
    let mut segs = Vec::new();
    for (i, m) in matches.iter().enumerate() {
        let end = if i + 1 < matches.len() { matches[i + 1].start() } else { norm_full.len() };
        segs.push(norm_full[m.start()..end].to_string());
    }
    segs
}

/// Majority vote for sky across loops.
/// Returns the most common (metar, display) pair when it is a strict majority
/// (> half of valid votes).  Falls back to current_metar/display unchanged.
fn majority_vote_sky(
    norm_full: &str,
    current_metar: &str,
    current_display: &str,
) -> (String, String) {
    let segs = loop_segments(norm_full);
    if segs.is_empty() {
        return (current_metar.to_string(), current_display.to_string());
    }
    let mut counts: std::collections::HashMap<(String, String), usize> = std::collections::HashMap::new();
    let invalid = ["N/A", "M", "Missing", ""];
    for seg in &segs {
        let r = extract_sky(seg);
        if !invalid.contains(&r.metar.as_str()) {
            *counts.entry((r.metar, r.display)).or_insert(0) += 1;
        }
    }
    if counts.is_empty() {
        return (current_metar.to_string(), current_display.to_string());
    }
    let total: usize = counts.values().sum();
    let ((best_metar, best_disp), best_count) = counts.into_iter().max_by_key(|(_, c)| *c).unwrap();
    if best_count * 2 > total && (best_metar != current_metar || best_disp != current_display) {
        (best_metar, best_disp)
    } else {
        (current_metar.to_string(), current_display.to_string())
    }
}

/// Majority vote for temp/dp across loops.
/// Only overrides when one value appears in a strict majority of loops.
fn majority_vote_temp(
    norm_full: &str,
    current_display: &str,
    current_metar: &str,
) -> (String, String) {
    let segs = loop_segments(norm_full);
    if segs.is_empty() {
        return (current_display.to_string(), current_metar.to_string());
    }
    let mut counts: std::collections::HashMap<(String, String), usize> = std::collections::HashMap::new();
    for seg in &segs {
        let r = extract_temp_dp(seg);
        if r.display != "N/A" && r.display != "Missing" && !is_temp_implausible(&r.display) {
            *counts.entry((r.display, r.metar)).or_insert(0) += 1;
        }
    }
    if counts.is_empty() {
        return (current_display.to_string(), current_metar.to_string());
    }
    let total: usize = counts.values().sum();
    let ((best_disp, best_metar), best_count) = counts.into_iter().max_by_key(|(_, c)| *c).unwrap();
    if best_count * 2 > total && (best_disp != current_display || best_metar != current_metar) {
        (best_disp, best_metar)
    } else {
        (current_display.to_string(), current_metar.to_string())
    }
}

// ---------------------------------------------------------------------------
// Main parse function
// ---------------------------------------------------------------------------

pub fn parse(input: &ParseInput) -> ParsedWeather {
    let norm_full = normalize(input.raw_transcript);
    let norm_full = truncate_digit_storm(&norm_full, 8);
    let (norm, selected_loop_time) = strip_preamble(&norm_full);
    let rec_day = input.recorded_at.format("%d").to_string();

    // Time
    let time_str = extract_time(&norm);

    // Wind
    let mut wind_result = extract_wind(&norm, &norm_full);
    if wind_result.display == "N/A" {
        let full = extract_wind(&norm_full, &norm_full);
        if full.display != "N/A" && full.display != "Missing" {
            wind_result = full;
        }
    }

    // Visibility
    let mut vis = extract_visibility(&norm);
    if is_vis_invalid(&vis) {
        let full_vis = extract_visibility(&norm_full);
        if !is_vis_invalid(&full_vis) { vis = full_vis; }
    }

    // Sky
    let mut sky_result = extract_sky(&norm);
    if sky_result.metar == "N/A" || sky_result.metar == "CLR" {
        let full_sky = extract_sky(&norm_full);
        let upgrade = sky_result.metar == "N/A"
            || (full_sky.metar != "N/A" && full_sky.metar != "CLR")
            || (full_sky.display.contains('(') && !sky_result.display.contains('('));
        if upgrade { sky_result = full_sky; }
    }
    // Majority vote: correct outlier last loops (e.g. KFWB temp 17->18 pattern
    // applied to sky - rare but guards against coverage flips in the final loop).
    // Strict majority required (> half votes) so genuine 2-loop updates are preserved.
    if sky_result.metar != "N/A" && sky_result.metar != "M" && sky_result.metar != "Missing" {
        let (voted_metar, voted_disp) =
            majority_vote_sky(&norm_full, &sky_result.metar, &sky_result.display);
        sky_result.metar   = voted_metar;
        sky_result.display = voted_disp;
    }

    // Temperature / Dewpoint
    let mut temp_result = extract_temp_dp(&norm);
    // Fall back to norm_full if temp is implausible OR if dewpoint is missing
    // (last loop may have temp but not dewpoint due to Whisper truncation)
    // Only fall back if dewpoint is N/A (parse failure) — not if sensor is explicitly Missing
    let temp_missing_dp = temp_result.display.ends_with("/ N/A");
    if is_temp_implausible(&temp_result.display) || temp_missing_dp {
        let full_temp = extract_temp_dp(&norm_full);
        if !is_temp_implausible(&full_temp.display) && !full_temp.display.ends_with("/ N/A") {
            temp_result = full_temp;
        }
    }
    // Majority vote: correct outlier last loops (e.g. KFWB: 2 loops 17°C, last loop 18°C).
    // Strict majority required so genuine station updates between loops are preserved.
    if temp_result.display != "N/A" && temp_result.display != "Missing" {
        let (voted_disp, voted_metar) =
            majority_vote_temp(&norm_full, &temp_result.display, &temp_result.metar);
        temp_result.display = voted_disp;
        temp_result.metar   = voted_metar;
    }

    // Altimeter
    let mut alt_result = extract_altimeter(&norm);
    if alt_result.display == "N/A" { alt_result = extract_altimeter(&norm_full); }

    // Remarks
    let mut remarks = extract_remarks(&norm);
    if remarks.is_empty() || remarks == "AO2" {
        let full_remarks = extract_remarks(&norm_full);
        if !full_remarks.is_empty() && full_remarks != "AO2" { remarks = full_remarks; }
    }

    // Phenomena
    let phenomena = extract_phenomena(&norm);

    // METAR
    let vis_metar = vis.replace(" SM", "SM").replace('>', "");
    let wx_metar  = phenomena.iter().map(|p| p.code.as_str()).collect::<Vec<_>>().join(" ");
    // Sky "M" (sensor missing) and "N/A" must not be emitted literally into the METAR
    // string — a bare "M" adjacent to the temp field produces e.g. "M28/21" which
    // looks like a negative temperature. Omit the sky group when sensor has no data.
    let sky_metar_field = match sky_result.metar.as_str() {
        "M" | "Missing" | "N/A" => String::new(),
        other => other.to_string(),
    };
    let mut metar_parts: Vec<String> = vec![
        format!("METAR {} {}{}Z AUTO", input.station_id, rec_day, time_str.trim_end_matches('Z')),
    ];
    // Only emit fields that have real values — never write N/A or Missing into the METAR string
    if wind_result.metar != "N/A" && wind_result.metar != "Missing" && wind_result.metar != "MIS" {
        metar_parts.push(wind_result.metar.clone());
    }
    if !vis_metar.is_empty() && vis_metar != "N/A" && vis_metar != "Missing" {
        metar_parts.push(vis_metar.clone());
    }
    if !wx_metar.is_empty() { metar_parts.push(wx_metar.clone()); }
    if !sky_metar_field.is_empty() { metar_parts.push(sky_metar_field.clone()); }
    if temp_result.metar != "N/A" && temp_result.metar != "Missing" {
        metar_parts.push(temp_result.metar.clone());
    }
    if alt_result.metar != "N/A" && alt_result.metar != "Missing" {
        metar_parts.push(alt_result.metar.clone());
    }
    metar_parts.push("RMK AO2".to_string());
    let metar_str = metar_parts.join(" ");

    // Extract density altitude from the selected loop's remarks first,
    // then fall back to norm_full in case it was truncated in the selected loop.
    let density_altitude = extract_density_altitude(&remarks).or_else(|| {
        let full_remarks = extract_remarks(&norm_full);
        extract_density_altitude(&full_remarks)
    });
    let wind_parsed      = build_wind(&wind_result);
    let sky_parsed       = build_sky(&sky_result);
    let local_info       = extract_local_info(input.raw_transcript);

    let mut result = ParsedWeather {
        selected_loop_time,
        time:                Some(time_str),
        wind:                Some(wind_parsed),
        visibility_sm:       Some(vis),
        sky:                 sky_parsed,
        temperature_c:       extract_temp_value(&temp_result.display, 0),
        dewpoint_c:          extract_temp_value(&temp_result.display, 1),
        altimeter_inhg:      Some(alt_result.display),
        density_altitude_ft: density_altitude,
        remarks:             Some(remarks),
        phenomena:           phenomena.into_iter().map(|p| p.code).collect(),
        metar:               Some(metar_str),
        local_info,
        validation_warnings: Vec::new(),
        audit_warnings:      Vec::new(),
    };

    // Run sanity validation — clears implausible field values in place
    let validation = validate::validate(&mut result);
    if !validation.is_clean() {
        tracing::warn!(
            "Validation warnings for {}: {:?}",
            input.station_id,
            validation.warnings
        );
    }
    result.validation_warnings = validation.warnings;

    // Run transcript audit — checks for keywords that should have produced values
    let audit_warnings = audit::audit(&norm, &norm_full, &result);
    if !audit_warnings.is_empty() {
        tracing::warn!(
            "Audit warnings for {}: {:?}",
            input.station_id,
            audit_warnings
        );
    }
    result.audit_warnings = audit_warnings;

    result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_time(text: &str) -> String {
    static TIME_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d{4})[,.\s]*[Zz]ulu").unwrap());
    let matches: Vec<_> = TIME_RE.captures_iter(text).collect();
    if let Some(last) = matches.last() {
        format!("{}Z", &last[1])
    } else {
        "N/A".into()
    }
}

fn is_vis_invalid(vis: &str) -> bool {
    if vis == "N/A" || vis == "Missing" { return true; }
    if let Some(m) = Regex::new(r">?([\d.]+)").unwrap().captures(vis) {
        if let Ok(n) = m[1].parse::<f64>() {
            // 0 SM means the parse failed (trailing period -> Rust parse -> 0.0)
            if n == 0.0 { return true; }
            return n > 10.0 && !vis.starts_with('>');
        }
    }
    false
}

fn is_temp_implausible(disp: &str) -> bool {
    if disp == "N/A" { return true; }
    if disp == "Missing" { return false; } // Explicitly missing sensor — not implausible
    let vals: Vec<f64> = Regex::new(r"-?[\d.]+").unwrap()
        .find_iter(disp)
        .filter_map(|m| m.as_str().parse().ok())
        .collect();
    vals.iter().any(|v| v.abs() > 60.0)
}

fn build_wind(result: &wind::WindResult) -> ParsedWind {
    if result.display == "N/A"     { return ParsedWind { raw: Some("N/A".into()),     ..Default::default() }; }
    if result.display == "Missing" { return ParsedWind { raw: Some("Missing".into()), ..Default::default() }; }
    if result.display == "Calm"    { return ParsedWind { calm: Some(true), raw: Some("Calm".into()), metar: Some("00000KT".into()), ..Default::default() }; }

    // Parse structured fields from the METAR string (pure ASCII) rather than the
    // display string which may contain a degree symbol that gets corrupted in
    // transit (UTF-8 Â° issue). METAR format: DDDSSGGGKTor DDDSSKT[nnnVnnn]
    static METAR_DIR_SPD: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(\d{3})(\d{2})(?:G(\d{2}))?KT").unwrap());
    static METAR_VRB:     Lazy<Regex> = Lazy::new(|| Regex::new(r"^VRB(\d{2})(?:G(\d{2}))?KT").unwrap());
    static METAR_VAR:     Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d{3})V(\d{3})").unwrap());

    if let Some(caps) = METAR_VRB.captures(&result.metar) {
        let spd  = Some(caps[1].to_string());
        let gust = caps.get(2).map(|m| m.as_str().to_string());
        return ParsedWind { variable: Some(true), speed_kt: spd, gust_kt: gust, raw: Some(result.display.clone()), metar: Some(result.metar.clone()), ..Default::default() };
    }

    if let Some(caps) = METAR_DIR_SPD.captures(&result.metar) {
        let direction = Some(caps[1].to_string());
        let speed     = Some(caps[2].to_string());
        let gust      = caps.get(3).map(|m| m.as_str().to_string());
        let variable  = if METAR_VAR.is_match(&result.metar) { Some(true) } else { None };
        return ParsedWind { direction, speed_kt: speed, gust_kt: gust, variable, raw: Some(result.display.clone()), metar: Some(result.metar.clone()), ..Default::default() };
    }

    // Fallback: METAR didn't match expected format
    ParsedWind { raw: Some(result.display.clone()), metar: Some(result.metar.clone()), ..Default::default() }
}

fn build_sky(result: &sky::SkyResult) -> Vec<ParsedSky> {
    if result.metar == "N/A" || result.metar == "M" {
        return vec![ParsedSky { coverage: result.metar.clone(), height_ft: None }];
    }
    if result.metar == "CLR" || result.metar == "SKC" {
        return vec![ParsedSky { coverage: result.metar.clone(), height_ft: None }];
    }
    Regex::new(r"(FEW|SCT|BKN|OVC|VV)(\d{3})").unwrap()
        .captures_iter(&result.metar)
        .map(|c| ParsedSky {
            coverage:  c[1].to_string(),
            height_ft: c[2].parse::<u32>().ok().map(|h| h * 100),
        })
        .collect()
}

fn extract_temp_value(disp: &str, index: usize) -> Option<String> {
    if disp == "N/A" { return None; }
    // Preserve "Missing" so downstream can distinguish sensor-missing from parse failure
    if disp == "Missing" { return Some("Missing".to_string()); }
    let parts: Vec<&str> = disp.split(" / ").collect();
    // Individual parts may also be N/A or Missing (e.g. "17°C / N/A")
    parts.get(index).and_then(|s| {
        let s = s.trim_end_matches("°C");
        if s == "N/A" { None }
        else if s == "Missing" { Some("Missing".to_string()) }
        else { Some(s.to_string()) }
    })
}

fn extract_density_altitude(remarks: &str) -> Option<String> {
    static DA: Lazy<Regex> = Lazy::new(|| Regex::new(r"Density Alt (-?\d[\d,]*) ft").unwrap());
    DA.captures(remarks).and_then(|c| c.get(1)).map(|m| m.as_str().replace(',', ""))
}
