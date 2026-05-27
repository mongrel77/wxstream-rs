use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;

fn format_thousands(n: u32) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[derive(Debug, Clone)]
pub struct SkyResult {
    pub metar: String,
    pub display: String,
}

fn priority(code: &str) -> u8 {
    match code {
        "OVC" => 4,
        "BKN" => 3,
        "SCT" => 2,
        "FEW" => 1,
        "VV"  => 5,
        _ => 0,
    }
}

fn add_layer(seen_alts: &mut HashMap<u32, (String, String)>, alt: u32, cover: &str) {
    if cover != "VV" && alt < 100 && alt != 0 { return; }
    if alt > 99_900 { return; }

    let metar = format!("{}{:03}", cover, alt / 100);
    let disp  = format!("{} {} ft", cover, format_thousands(alt));
    let existing = seen_alts.get(&alt);
    if existing.is_none() || priority(cover) > priority(&existing.unwrap().0) {
        seen_alts.insert(alt, (metar, disp));
    }
}

fn coverage_code(tok: &str) -> Option<&'static str> {
    match tok {
        "few" => Some("FEW"),
        "scattered" => Some("SCT"),
        "broken" => Some("BKN"),
        "overcast" => Some("OVC"),
        _ => None,
    }
}

fn is_field_boundary(tok: &str) -> bool {
    matches!(
        tok,
        "temperature" | "temp" | "dewpoint" | "dew" | "altimeter" | "remarks" |
        "weather" | "wind" | "visibility" | "automated" | "observation" | "zulu"
    )
}

fn parse_alt(tok: &str) -> Option<u32> {
    if tok.is_empty() || !tok.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let alt = tok.parse::<u32>().ok()?;
    if (100..=99_900).contains(&alt) { Some(alt) } else { None }
}

fn next_non_filler(tokens: &[String], mut idx: usize) -> Option<usize> {
    while idx < tokens.len() {
        match tokens[idx].as_str() {
            "feet" | "foot" | "ft" | "cloud" | "clouds" => idx += 1,
            _ => return Some(idx),
        }
    }
    None
}

fn coverage_following_alt(tokens: &[String], cover_idx: usize, allow_ceiling: bool) -> Option<usize> {
    let mut idx = cover_idx + 1;

    // Common forms: "broken at 5500", "few clouds at 600".
    while idx < tokens.len() {
        match tokens[idx].as_str() {
            "at" | "feet" | "foot" | "ft" | "cloud" | "clouds" => idx += 1,
            "ceiling" if allow_ceiling => idx += 1,
            _ => break,
        }
    }

    if idx < tokens.len() && !is_field_boundary(&tokens[idx]) {
        return parse_alt(&tokens[idx]).map(|_| idx);
    }
    None
}

/// Decide whether a coverage-before-altitude phrase should claim the altitude.
///
/// This is the key ambiguity resolver:
/// - "scattered 2800 broken 5500" keeps SCT028 because "broken" has its own 5500.
/// - "broken 5500 overcast temperature" does NOT keep BKN055 because "overcast"
///   has no altitude of its own, so 5500 belongs to OVC.
fn direct_coverage_claims_alt(tokens: &[String], _cover_idx: usize, alt_idx: usize) -> bool {
    let Some(next_idx) = next_non_filler(tokens, alt_idx + 1) else {
        return true;
    };

    if coverage_code(&tokens[next_idx]).is_some() {
        // If the following coverage has its own following altitude, keep the
        // current direct association. Otherwise the altitude belongs to that
        // following unvalued coverage word.
        return coverage_following_alt(tokens, next_idx, true).is_some();
    }

    true
}

#[allow(dead_code)]
fn previous_coverage_claims_alt(tokens: &[String], alt_idx: usize) -> bool {
    if alt_idx == 0 {
        return false;
    }
    let prev_idx = alt_idx - 1;
    if coverage_code(&tokens[prev_idx]).is_some() {
        return direct_coverage_claims_alt(tokens, prev_idx, alt_idx);
    }
    false
}

fn detect_sky_format(tokens: &[String]) -> &'static str {
    let field_bounds = ["temperature","temp","dewpoint","dew","altimeter","remarks",
                        "weather","wind","visibility","automated","observation","zulu",
                        "sky","condition"];
    for (i, tok) in tokens.iter().enumerate() {
        if field_bounds.contains(&tok.as_str()) { continue; }
        if coverage_code(tok).is_some() { return "cov_before"; }
        if parse_alt(tok).is_some() {
            // Only alt_before if followed by a coverage word
            if let Some(nxt) = next_non_filler(tokens, i + 1) {
                if coverage_code(&tokens[nxt]).is_some() { return "alt_before"; }
            }
        }
    }
    "cov_before"
}

fn scan_sky_layers(text: &str, seen_alts: &mut HashMap<u32, (String, String)>) {
    static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)[a-z]+|\d+").unwrap());
    let tokens: Vec<String> = TOKEN_RE
        .find_iter(text)
        .map(|m| m.as_str().to_ascii_lowercase())
        .collect();

    let fmt = detect_sky_format(&tokens);

    if fmt == "alt_before" {
        // Each altitude is followed by its coverage word.
        // "2800 Scattered ceiling 4900 Broken 5500 Overcast" -> SCT028 BKN049 OVC055
        for i in 0..tokens.len() {
            let Some(alt) = parse_alt(&tokens[i]) else { continue };
            let Some(next_idx) = next_non_filler(&tokens, i + 1) else { continue };
            if let Some(code) = coverage_code(&tokens[next_idx]) {
                add_layer(seen_alts, alt, code);
            }
        }
    } else {
        // Each coverage word claims its following altitude.
        // For coverage words with no following alt (e.g. "broken 5500 overcast temperature"),
        // claim the preceding altitude instead.
        for i in 0..tokens.len() {
            let Some(code) = coverage_code(&tokens[i]) else { continue };
            let alt_idx = coverage_following_alt(&tokens, i, false);
            if let Some(alt_idx) = alt_idx {
                let Some(alt) = parse_alt(&tokens[alt_idx]) else { continue };
                if direct_coverage_claims_alt(&tokens, i, alt_idx) {
                    add_layer(seen_alts, alt, code);
                }
            } else {
                // No following alt — claim the immediately preceding altitude
                let mut prev = i as isize - 1;
                while prev >= 0 && ["feet","foot","ft","cloud","clouds"]
                    .contains(&tokens[prev as usize].as_str()) { prev -= 1; }
                if prev >= 0 {
                    if let Some(prev_alt) = parse_alt(&tokens[prev as usize]) {
                        add_layer(seen_alts, prev_alt, code);
                    }
                }
            }
        }
    }
    // Vertical visibility remains a specific phrase, not part of ordinary
    // coverage-word scanning.
    static VV: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)vertical\s+visibility[\s.,]+(\d[\d,]+)").unwrap()
    });
    for m in VV.captures_iter(text) {
        let alt_clean: String = m[1].chars().filter(|c| c.is_ascii_digit()).collect();
        if let Ok(alt) = alt_clean.parse::<u32>() {
            add_layer(seen_alts, alt, "VV");
        }
    }
}

/// Extract sky conditions. Handles both AWOS orderings:
///   - altitude-before-coverage: "2800 Scattered ceiling 4900 Broken 5500 Overcast"
///   - coverage-before-altitude: "Scattered 2800 Broken 5000 Overcast 5500"
pub fn extract_sky(text: &str) -> SkyResult {
    let mut seen_alts: HashMap<u32, (String, String)> = HashMap::new();

    // Whisper-split ceiling: 'Ceiling 1000 9. Hundred.' -> 1900 ft.
    static SPLIT_CEILING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling\s+(\d+)\s+(\d)\s*[.,]\s*hundred").unwrap()
    });
    for m in SPLIT_CEILING.captures_iter(text) {
        let base: u32 = m[1].parse().unwrap_or(0);
        let extra: u32 = m[2].parse().unwrap_or(0);
        let alt = base + extra * 100;
        add_layer(&mut seen_alts, alt, "BKN");
    }

    scan_sky_layers(text, &mut seen_alts);

    // Build output if any layers found.
    if !seen_alts.is_empty() {
        let mut layers: Vec<(u32, (String, String))> = seen_alts.drain().collect();
        layers.sort_by_key(|(k, _)| *k);
        let metar_codes = layers.iter().map(|(_, (m, _))| m.as_str()).collect::<Vec<_>>().join(" ");
        let disp_parts  = layers.iter().map(|(_, (_, d))| d.as_str()).collect::<Vec<_>>().join(" / ");
        return SkyResult { metar: metar_codes, display: disp_parts };
    }

    // CLR / SKC
    static CLR_BELOW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b(?:sky\s+condition[\s,.]+clear|clear)[\s,.]+below[\s,.]+(\d[\d\s,.]*\d|\d)").unwrap()
    });
    if let Some(m) = CLR_BELOW.captures(text) {
        let alt: String = m[1].chars().filter(|c| c.is_ascii_digit()).collect();
        if let Ok(a) = alt.parse::<u32>() {
            if a <= 12_000 {
                return SkyResult {
                    metar:   "CLR".into(),
                    display: format!("CLR (below {} ft)", format_thousands(a)),
                };
            }
        }
    }
    static CLR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b(sky\s+condition[\s,]+clear|clear[\s,]+below|clr\b|skc\b)").unwrap()
    });
    if CLR.is_match(text) {
        return SkyResult { metar: "CLR".into(), display: "CLR".into() };
    }

    // Missing sensor
    static MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\b(?:sky\s+condition|ceiling)[\s.,]+missing\b").unwrap()
    });
    if MISSING.is_match(text) {
        return SkyResult { metar: "M".into(), display: "Missing".into() };
    }

    SkyResult { metar: "N/A".into(), display: "N/A".into() }
}
