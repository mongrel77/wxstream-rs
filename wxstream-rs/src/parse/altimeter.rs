use once_cell::sync::Lazy;
use regex::Regex;

fn format_thousands(n: u32) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { result.push(','); }
        result.push(c);
    }
    result.chars().rev().collect()
}

// ---------------------------------------------------------------------------
// Altimeter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct AltResult {
    pub display: String,
    pub metar:   String,
}

/// Mirrors extract_altimeter() from parse_transcripts.py.
pub fn extract_altimeter(text: &str) -> AltResult {
    // Special case: "2, 9er, 9er, 9er"
    static ALT_2999: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)altimeter\s+2\s*,\s*9").unwrap()
    });
    if ALT_2999.is_match(text) {
        return AltResult { display: "29.99 inHg".into(), metar: "A2999".into() };
    }

    static MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)altimeter[\s.,]+(?:missing|information[\s.,]+not[\s.,]+available)\b").unwrap()
    });
    if MISSING.is_match(text) {
        return AltResult { display: "Missing".into(), metar: "AMIS".into() };
    }

    static ALT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)altimeter[\s.,]+(\d+(?:\.\d+)?)").unwrap()
    });

    for caps in ALT_RE.captures_iter(text) {
        let raw = caps[1].replace('.', "");
        let mut val = raw.clone();

        // Truncate to 4 digits if starts with 2 or 3
        if val.len() > 4 && (val.starts_with('2') || val.starts_with('3')) {
            val = val[..4].to_string();
        }

        if val.len() == 4 && (val.starts_with('2') || val.starts_with('3')) {
            let display = format!("{}{}.{}{} inHg",
                &val[..1], &val[1..2], &val[2..3], &val[3..4]);
            let metar = format!("A{}", val);
            return AltResult { display, metar };
        }
    }

    AltResult { display: "N/A".into(), metar: "N/A".into() }
}

// ---------------------------------------------------------------------------
// Remarks
// ---------------------------------------------------------------------------

/// Mirrors extract_remarks() from parse_transcripts.py.
pub fn extract_remarks(text: &str) -> String {
    let mut remarks: Vec<String> = Vec::new();

    // Density altitude
    static DENSITY_ALT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)density[\s.,]+alt(?:itude)?[\s.,]+(minus[\s.,]+)?(\d[\d,]+)").unwrap()
    });
    if let Some(m) = DENSITY_ALT.captures(text) {
        let sign = if m.get(1).is_some() { "-" } else { "" };
        let alt: String = m[2].chars().filter(|c| c.is_ascii_digit()).collect();
        if let Ok(a) = alt.parse::<u32>() {
            remarks.push(format!("Density Alt {}{} ft", sign, format_thousands(a)));
        }
    }

    // TSNO
    static TSNO: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)thunderstorm.*?(?:information\s+)?not\s+available").unwrap()
    });
    if TSNO.is_match(text) {
        remarks.push("TSNO".into());
    }

    // Lightning missing
    static LTG_MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)lightning\s+missing").unwrap()
    });
    if LTG_MISSING.is_match(text) {
        remarks.push("Lightning sensor missing".into());
    }

    // Lightning observed with direction
    static LTG: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)lightning[\s.,]+(?:distance[\s.,]+|distant[\s.,]+|observed[\s.,]+)?(.*?)(?:\.|$|temperature|dewpoint|altimeter|remarks|density)").unwrap()
    });
    static LTG_SENSOR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)lightning\s+(?:missing|sensor|information)").unwrap()
    });
    if !LTG_SENSOR.is_match(text) {
        if let Some(m) = LTG.captures(text) {
            let raw_dir = m[1].trim().trim_end_matches(|c| c == '.' || c == ',' || c == ' ');
            if !raw_dir.is_empty() {
                let dir_map = [
                    ("north", "N"), ("south", "S"), ("east", "E"), ("west", "W"),
                    ("northeast", "NE"), ("northwest", "NW"),
                    ("southeast", "SE"), ("southwest", "SW"),
                ];
                static SPLIT: Lazy<Regex> = Lazy::new(|| {
                    Regex::new(r"[\s,]+(?:through|and|to)[\s,]+|[\s,]+").unwrap()
                });
                let parts: Vec<&str> = SPLIT.split(raw_dir).collect();
                let dirs: Vec<&str> = parts.iter().map(|p| {
                    dir_map.iter().find(|(w, _)| p.to_lowercase() == *w)
                        .map(|(_, a)| *a)
                        .unwrap_or(p)
                }).collect();
                let dir_str = dirs.join("-");
                remarks.push(format!("Lightning {}", dir_str));
            } else {
                remarks.push("Lightning observed".into());
            }
        }
    }

    // Ceiling variable between X and Y
    static CIG_VAR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)ceiling\s+variable\s+between\s+(\d+)\s+and\s+(\d+)").unwrap()
    });
    if let Some(m) = CIG_VAR.captures(text) {
        let lo: u32 = m[1].parse().unwrap_or(0);
        let hi: u32 = m[2].parse().unwrap_or(0);
        remarks.push(format!("CIG variable {}-{} ft", format_thousands(lo), format_thousands(hi)));
    }

    if remarks.is_empty() {
        "AO2".into()
    } else {
        remarks.join(", ")
    }
}

// ---------------------------------------------------------------------------
// Phenomena
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Phenomenon {
    pub display: String,
    pub code:    String,
}

const PHENOMENA: &[(&str, &str, &str)] = &[
    ("Thunderstorm",     "TS",   r"(?i)\bthunderstorm\b(?![\s.,]+information)"),
    ("Freezing Rain",    "FZRA", r"(?i)\bfreezing\s+rain\b"),
    ("Freezing Drizzle", "FZDZ", r"(?i)\bfreezing\s+drizzle\b"),
    ("Freezing Fog",     "FZFG", r"(?i)\bfreezing\s+fog\b"),
    ("Rain",             "RA",   r"(?i)\brain\b"),
    ("Drizzle",          "DZ",   r"(?i)\bdrizzle\b"),
    ("Snow",             "SN",   r"(?i)\bsnow\b(?!\s+grains)"),
    ("Snow Grains",      "SG",   r"(?i)\bsnow\s+grains\b"),
    ("Ice Pellets",      "PL",   r"(?i)\bice\s+pellets\b"),
    ("Ice Crystals",     "IC",   r"(?i)\bice\s+crystals\b"),
    ("Hail",             "GR",   r"(?i)\bhail\b"),
    ("Small Hail",       "GS",   r"(?i)\bsmall\s+hail\b"),
    ("Fog",              "FG",   r"(?i)\bfog\b"),
    ("Mist",             "BR",   r"(?i)\bmist\b"),
    ("Haze",             "HZ",   r"(?i)\bhaze\b"),
    ("Unknown Precip",   "UP",   r"(?i)\bunknown\s+precipitation\b"),
    ("Squall",           "SQ",   r"(?i)\bsquall\b"),
    ("Funnel Cloud",     "FC",   r"(?i)\bfunnel\s+cloud\b"),
    ("Tornado",          "FC+",  r"(?i)\btornado\b|\bwaterspout\b"),
    ("Volcanic Ash",     "VA",   r"(?i)\bvolcanic\s+ash\b"),
    ("Blowing Snow",     "BLSN", r"(?i)\bblowing\s+snow\b"),
    ("Blowing Dust",     "BLDU", r"(?i)\bblowing\s+dust\b"),
    ("Blowing Sand",     "BLSA", r"(?i)\bblowing\s+sand\b"),
    ("Smoke",            "FU",   r"(?i)\bsmoke\b"),
    ("Dust",             "DU",   r"(?i)\bdust\b(?!\s+\d)"),
    ("Sand",             "SA",   r"(?i)\bsand\b"),
    ("Dust/Sand Storm",  "SS",   r"(?i)\b(?:dust|sand)\s+storm\b"),
];

const SUPPRESS_IF_PARENT: &[(&str, &[&str])] = &[
    ("FG",  &["FZFG"]),
    ("RA",  &["FZRA"]),
    ("DZ",  &["FZDZ"]),
    ("SN",  &["BLSN"]),
    ("GR",  &["GS"]),
];

/// Mirrors extract_phenomena() from parse_transcripts.py.
pub fn extract_phenomena(text: &str) -> Vec<Phenomenon> {
    let text_lower = text.to_lowercase();
    let mut found_codes: Vec<String> = Vec::new();
    let mut found: Vec<Phenomenon> = Vec::new();

    for (display, code, pattern) in PHENOMENA {
        if let Ok(re) = Regex::new(pattern) {
            if let Some(m) = re.find(&text_lower) {
                // Check intensity prefix
                let pre_start = m.start().saturating_sub(20);
                let pre = &text_lower[pre_start..m.start()];

                let (intensity, disp_prefix) = if Regex::new(r"\bheavy\b").unwrap().is_match(pre) {
                    ("+", "Heavy ")
                } else if Regex::new(r"\blight\b").unwrap().is_match(pre) {
                    ("-", "Light ")
                } else {
                    ("", "")
                };

                let full_code = format!("{}{}", intensity, code);
                let full_disp = format!("{}{}", disp_prefix, display);
                found_codes.push(code.to_string());
                found.push(Phenomenon { display: full_disp, code: full_code });
            }
        }
    }

    // Suppress less-specific codes
    found.into_iter().filter(|p| {
        let base_code = p.code.trim_start_matches(|c| c == '+' || c == '-');
        !SUPPRESS_IF_PARENT.iter().any(|(code, parents)| {
            *code == base_code && parents.iter().any(|parent| found_codes.contains(&parent.to_string()))
        })
    }).collect()
}
