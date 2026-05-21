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

    // Lightning observed with direction.
    // Handles two transcript patterns:
    //   "lightning distant northeast"   — direction inline
    //   "lightning. Distant northeast." — direction sentence after a period
    // Captures up to 60 chars after "lightning" (crossing sentence boundaries)
    // then filters to only recognised compass-direction words.
    static LTG_SENSOR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)lightning\s+(?:missing|sensor|information)").unwrap()
    });
    static AFTER_LTG: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)lightning[\s.,]+(.{0,80})").unwrap()
    });
    static LTG_SPLIT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"[\s,]+(?:through|and|to)[\s,]+|[\s,]+").unwrap()
    });
    if !LTG_SENSOR.is_match(text) {
        let dir_map = [
            ("north", "N"), ("south", "S"), ("east", "E"), ("west", "W"),
            ("northeast", "NE"), ("northwest", "NW"),
            ("southeast", "SE"), ("southwest", "SW"),
            ("distant", ""), ("distance", ""), ("observed", ""), // skip qualifiers
        ];
        if let Some(caps) = AFTER_LTG.captures(text) {
            let after = caps[1].trim();
            // Stop at keywords that signal the next field
            let stop = ["temperature", "dewpoint", "altimeter", "remarks", "density", "visibility"];
            let after = stop.iter().fold(after.to_string(), |s, kw| {
                if let Some(pos) = s.to_lowercase().find(kw) { s[..pos].to_string() } else { s }
            });
            let dir_words: Vec<&str> = LTG_SPLIT.split(after.trim())
                .filter(|p| {
                    let lc = p.to_lowercase();
                    dir_map.iter().any(|(w, abbr)| lc == *w && !abbr.is_empty())
                })
                .collect();
            if !dir_words.is_empty() {
                let dir_str = dir_words.iter().map(|p| {
                    dir_map.iter().find(|(w, _)| p.to_lowercase() == *w)
                        .map(|(_, a)| *a).unwrap_or(p)
                }).collect::<Vec<_>>().join("-");
                remarks.push(format!("Lightning {}", dir_str));
            } else if AFTER_LTG.is_match(text) {
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
#[allow(dead_code)]
pub struct Phenomenon {
    pub display: String,
    pub code:    String,
}

const PHENOMENA: &[(&str, &str, &str)] = &[
    // Thunderstorm: exclude "thunderstorm information not available" - matched by TSNO separately
    ("Thunderstorm",     "TS",   r"(?i)\bthunderstorm(?:\s+(?:and|with|in|near|overhead)\b|\s*[,.]|\s*$)"),
    ("Freezing Rain",    "FZRA", r"(?i)\bfreezing\s+rain\b"),
    ("Freezing Drizzle", "FZDZ", r"(?i)\bfreezing\s+drizzle\b"),
    ("Freezing Fog",     "FZFG", r"(?i)\bfreezing\s+fog\b"),
    ("Rain",             "RA",   r"(?i)\brain\b"),
    ("Drizzle",          "DZ",   r"(?i)\bdrizzle\b"),
    ("Snow Grains",      "SG",   r"(?i)\bsnow\s+grains\b"),
    ("Snow",             "SN",   r"(?i)\bsnow\b"),
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
    ("Dust",             "DU",   r"(?i)\bdust\b"),
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

/// Extract the phenomena section of the transcript — the slice between the
/// altimeter reading and the remarks/density keyword.  Phenomena are broadcast
/// in this window, so Whisper transcription errors ("missed"/"miss" for "mist",
/// "haze" mis-heard as "days", etc.) are only corrected inside this window to
/// avoid corrupting other fields.
fn phenomena_window(text: &str) -> String {
    let tl = text.to_lowercase();

    // Find start: just after the altimeter value  e.g. "altimeter 2991"
    let start = {
        static ALT_END: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
            regex::Regex::new(r"(?i)altimeter[\s.,]+\d{4}").unwrap()
        });
        ALT_END.find(&tl).map(|m| m.end()).unwrap_or(0)
    };

    // Find end: start of remarks / density / next anchor
    let end = {
        static RMK_START: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
            regex::Regex::new(r"(?i)(remarks|density\s+alt|automated\s+weather)").unwrap()
        });
        RMK_START.find(&tl[start..])
            .map(|m| start + m.start())
            .unwrap_or(tl.len())
    };

    let window = &text[start..end];

    // Whisper substitution corrections — only applied within this window
    // Each entry: (pattern, replacement)
    let corrections: &[(&str, &str)] = &[
        // "mist" commonly transcribed as "missed" or "miss"
        (r"(?i)missed", "mist"),
        (r"(?i)miss",   "mist"),
        // "haze" occasionally heard as "days" or "hays"
        (r"(?i)hays",   "haze"),
        // "drizzle" sometimes heard as "Bristol" or "gristle"
        // (add more as discovered from real transcripts)
    ];

    let mut w = window.to_string();
    for (pat, rep) in corrections {
        if let Ok(re) = regex::Regex::new(pat) {
            w = re.replace_all(&w, *rep).to_string();
        }
    }
    w
}

/// Mirrors extract_phenomena() from parse_transcripts.py.
pub fn extract_phenomena(text: &str) -> Vec<Phenomenon> {
    // Apply Whisper corrections to the full text first, then also run the
    // phenomena window for additional context. "Miss"/"missed" for "mist" can
    // appear anywhere in the broadcast (e.g. between visibility and sky condition),
    // not just in the dedicated phenomena slot between altimeter and remarks.
    let corrections: &[(&str, &str)] = &[
        (r"(?i)\bmissed\b", "mist"),
        (r"(?i)\bmiss\b",   "mist"),
        (r"(?i)\bhays\b",   "haze"),
    ];
    let mut corrected_text = text.to_string();
    for (pat, rep) in corrections {
        if let Ok(re) = regex::Regex::new(pat) {
            corrected_text = re.replace_all(&corrected_text, *rep).to_string();
        }
    }
    let window = phenomena_window(&corrected_text);
    let combined = format!("{} {}", corrected_text, window);
    let text_lower = combined.to_lowercase();
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
