pub mod altimeter;
pub mod normalize;
pub mod sky;
pub mod temperature;
pub mod visibility;
pub mod wind;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;

use altimeter::{extract_altimeter, extract_phenomena, extract_remarks};
use normalize::{normalize, strip_preamble, truncate_digit_storm};
use sky::extract_sky;
use temperature::extract_temp_dp;
use visibility::extract_visibility;
use wind::extract_wind;

// ---------------------------------------------------------------------------
// ParseInput / ParsedWeather — local types, decoupled from MongoDB models
// ---------------------------------------------------------------------------

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
}

// ---------------------------------------------------------------------------
// Main parse function
// ---------------------------------------------------------------------------

pub fn parse(input: &ParseInput) -> ParsedWeather {
    let raw = truncate_digit_storm(input.raw_transcript, 8);
    let norm_full = normalize(&raw);
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

    // Temperature / Dewpoint
    let mut temp_result = extract_temp_dp(&norm);
    if is_temp_implausible(&temp_result.display) {
        let full_temp = extract_temp_dp(&norm_full);
        if !is_temp_implausible(&full_temp.display) { temp_result = full_temp; }
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
    let metar_str = format!(
        "METAR {} {}{}Z AUTO {} {} {}{}{} {} RMK AO2",
        input.station_id,
        rec_day,
        time_str.trim_end_matches('Z'),
        wind_result.metar,
        vis_metar,
        if wx_metar.is_empty() { String::new() } else { format!("{} ", wx_metar) },
        sky_result.metar,
        temp_result.metar,
        alt_result.metar,
    );

    let density_altitude = extract_density_altitude(&remarks);
    let wind_parsed      = build_wind(&wind_result);
    let sky_parsed       = build_sky(&sky_result);

    ParsedWeather {
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
    }
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
            return n > 10.0 && !vis.starts_with('>');
        }
    }
    false
}

fn is_temp_implausible(disp: &str) -> bool {
    if disp == "N/A" { return true; }
    let vals: Vec<f64> = Regex::new(r"-?[\d.]+").unwrap()
        .find_iter(disp)
        .filter_map(|m| m.as_str().parse().ok())
        .collect();
    vals.iter().any(|v| v.abs() > 60.0)
}

fn build_wind(result: &wind::WindResult) -> ParsedWind {
    static DIR_SPD: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d{3})° at (\d+) kts").unwrap());
    static GUST:    Lazy<Regex> = Lazy::new(|| Regex::new(r"gusts (\d+) kts").unwrap());
    static VAR_SPD: Lazy<Regex> = Lazy::new(|| Regex::new(r"Variable at (\d+)").unwrap());

    if result.display == "N/A"     { return ParsedWind { raw: Some("N/A".into()),     ..Default::default() }; }
    if result.display == "Missing" { return ParsedWind { raw: Some("Missing".into()), ..Default::default() }; }
    if result.display == "Calm"    { return ParsedWind { calm: Some(true), raw: Some("Calm".into()), ..Default::default() }; }

    if result.display.starts_with("Variable at") {
        let spd = VAR_SPD.captures(&result.display).and_then(|c| c.get(1)).map(|m| m.as_str().to_string());
        return ParsedWind { variable: Some(true), speed_kt: spd, raw: Some(result.display.clone()), metar: Some(result.metar.clone()), ..Default::default() };
    }

    let direction = DIR_SPD.captures(&result.display).and_then(|c| c.get(1)).map(|m| m.as_str().to_string());
    let speed     = DIR_SPD.captures(&result.display).and_then(|c| c.get(2)).map(|m| m.as_str().to_string());
    let gust      = GUST.captures(&result.display).and_then(|c| c.get(1)).map(|m| m.as_str().to_string());

    ParsedWind { direction, speed_kt: speed, gust_kt: gust, raw: Some(result.display.clone()), metar: Some(result.metar.clone()), ..Default::default() }
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
    if disp == "N/A" || disp == "Missing" { return None; }
    let parts: Vec<&str> = disp.split(" / ").collect();
    parts.get(index).map(|s| s.trim_end_matches("°C").to_string())
}

fn extract_density_altitude(remarks: &str) -> Option<String> {
    static DA: Lazy<Regex> = Lazy::new(|| Regex::new(r"Density Alt (-?\d[\d,]*) ft").unwrap());
    DA.captures(remarks).and_then(|c| c.get(1)).map(|m| m.as_str().replace(',', ""))
}
