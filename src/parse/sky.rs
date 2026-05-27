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

/// Extract sky conditions. Mirrors extract_sky() from parse_transcripts.py.
pub fn extract_sky(text: &str) -> SkyResult {
    let mut seen_alts: HashMap<u32, (String, String)> = HashMap::new();
    // Track altitudes claimed by direct "coverage altitude" patterns (e.g. "few 1000").
    // ALT_* reverse patterns ("1000 scattered") must not override these — the direct
    // pattern is more reliable since it unambiguously associates coverage with altitude.
    let mut direct_alts: std::collections::HashSet<u32> = std::collections::HashSet::new();

    let priority = |code: &str| -> u8 {
        match &code[..3] {
            "OVC" => 4, "BKN" => 3, "SCT" => 2, "FEW" => 1, _ => 0,
        }
    };

    let mut add_layer = |alt_str: &str, cover: &str, direct: bool| {
        let alt_clean: String = alt_str.chars().filter(|c| c.is_ascii_digit()).collect();
        if let Ok(alt) = alt_clean.parse::<u32>() {
            if cover != "VV" && alt < 100 && alt != 0 { return; }
            if alt > 99900 { return; }
            // Skip reverse-pattern matches for altitudes already claimed by direct patterns
            if !direct && direct_alts.contains(&alt) { return; }
            let metar = format!("{}{:03}", cover, alt / 100);
            let disp  = format!("{} {} ft", cover, format_thousands(alt));
            let existing = seen_alts.get(&alt);
            if existing.is_none() || priority(cover) > priority(&existing.unwrap().0) {
                seen_alts.insert(alt, (metar, disp));
                if direct { direct_alts.insert(alt); }
            }
        }
    };

    // Whisper-split ceiling: 'Ceiling 1000 9. Hundred.' -> 1900 ft
    static SPLIT_CEILING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling\s+(\d+)\s+(\d)\s*[.,]\s*hundred").unwrap()
    });
    for m in SPLIT_CEILING.captures_iter(text) {
        let base: u32 = m[1].parse().unwrap_or(0);
        let extra: u32 = m[2].parse().unwrap_or(0);
        let alt = base + extra * 100;
        add_layer(&alt.to_string(), "BKN", true);
    }

    // Overcast patterns
    static OVC_CEILING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling[\s.,]+(\d{3}[\d,]*)[\s.,]+overcast").unwrap()
    });
    for m in OVC_CEILING.captures_iter(text) { add_layer(&m[1], "OVC", true); }

    static OVC_AT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(?:sky\s+condition\s+)?overcast[\s.,]+(?:at\s+)?(\d[\d,]+)").unwrap()
    });
    for m in OVC_AT.captures_iter(text) { add_layer(&m[1], "OVC", true); }

    // Broken patterns
    static BKN_CEILING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling[\s.,]+(\d{3}[\d,]*)[\s.,]+broken").unwrap()
    });
    for m in BKN_CEILING.captures_iter(text) { add_layer(&m[1], "BKN", true); }

    static BKN_AFTER: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(\d{3}[\d,]*)[\s.,]+broken").unwrap()
    });
    for m in BKN_AFTER.captures_iter(text) { add_layer(&m[1], "BKN", true); }

    static BKN_AT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)broken[\s.,]+(?:at[\s.,]+)?(\d{3}[\d,]*)").unwrap()
    });
    for m in BKN_AT.captures_iter(text) { add_layer(&m[1], "BKN", true); }

    // Scattered patterns
    static SCT_CEILING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling[\s.,]+(\d{3}[\d,]*)[\s.,]+scattered").unwrap()
    });
    for m in SCT_CEILING.captures_iter(text) { add_layer(&m[1], "SCT", true); }

    static SCT_AT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)scattered[\s.,]+(?:at\s+)?(\d{3}[\d,]*)").unwrap()
    });
    for m in SCT_AT.captures_iter(text) { add_layer(&m[1], "SCT", true); }

    static ALT_SCT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(\d{3}[\d,]*)(?:[ \t,]+|\.)scattered").unwrap()
    });
    for m in ALT_SCT.captures_iter(text) { add_layer(&m[1], "SCT", false); }

    // Reverse overcast: "5500 Overcast" (altitude then coverage)
    static ALT_OVC: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(\d{3}[\d,]*)(?:[ \t,]+|\.)overcast").unwrap()
    });
    for m in ALT_OVC.captures_iter(text) { add_layer(&m[1], "OVC", false); }

    // Few patterns
    // "few clouds at 600" — the word "clouds" is optional between "few" and the altitude
    static FEW_AT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)few[\s.,]+(?:clouds?\s+)?(?:at\s+)?(\d{3}[\d,]*)").unwrap()
    });
    for m in FEW_AT.captures_iter(text) { add_layer(&m[1], "FEW", true); }

    static ALT_FEW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(\d{3}[\d,]*)(?:[ \t,]+|\.)few").unwrap()
    });
    for m in ALT_FEW.captures_iter(text) { add_layer(&m[1], "FEW", false); }

    // Vertical visibility
    static VV: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)vertical\s+visibility[\s.,]+(\d[\d,]+)").unwrap()
    });
    for m in VV.captures_iter(text) { add_layer(&m[1], "VV", true); }

    // Build output if any layers found
    if !seen_alts.is_empty() {
        let mut layers: Vec<(u32, (String, String))> = seen_alts.into_iter().collect();
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
            if a <= 12000 {
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
