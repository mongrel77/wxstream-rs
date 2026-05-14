use once_cell::sync::Lazy;
use regex::Regex;

/// Extract supplemental local information broadcast after the weather observation.
/// Mirrors extract_local_info() from parse_transcripts.py exactly.
/// Operates on the raw (un-normalized) transcript.
pub fn extract_local_info(raw_text: &str) -> Option<String> {
    static ANCHOR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)[Aa]utomated\s+[Ww]eather\s+[Oo]bservation[.\s,]+\d{4}[.\s,]*[Zz]ulu[.\s,]*")
            .unwrap()
    });

    // Split on anchor pattern to get broadcast bodies
    let matches: Vec<_> = ANCHOR.find_iter(raw_text).collect();
    if matches.is_empty() {
        return None;
    }

    // Build segments between anchors (the content after each anchor)
    let mut bodies: Vec<&str> = Vec::new();
    for (i, m) in matches.iter().enumerate() {
        let end = if i + 1 < matches.len() {
            matches[i + 1].start()
        } else {
            raw_text.len()
        };
        bodies.push(&raw_text[m.end()..end]);
    }

    if bodies.is_empty() {
        return None;
    }

    // Use second-to-last body - local info repeats with each cycle
    let body = if bodies.len() >= 2 {
        bodies[bodies.len() - 2]
    } else {
        bodies[bodies.len() - 1]
    };

    // Local trigger patterns - same as Python version
    let local_triggers: &[&str] = &[
        r"(?i)tower\s+(?:is\s+)?(?:of\s+)?(?:hours|operation)",
        r"(?i)common\s+traffic\s+advis",
        r"(?i)pilot.operated",
        r"(?i)pilot\s+operated",
        r"(?i)approach\s+control",
        r"(?i)avgas",
        r"(?i)self-serve",
        r"(?i)full\s+service\s+100",
        r"(?i)call\s+before\s+landing",
        r"(?i)\d{3}-\d{3}-\d{4}",
        r"(?i)contact\s+\w+.*\s+(?:approach|control|center)",
        r"(?i)IFR\s+clearance",
        r"(?i)for\s+additional\s+information",
        r"(?i)frequency\s+(?:is\s+)?\d",
        r"(?i)on\s+frequency\s+\d",
        r"(?i)\d{3,4}\s+local\s+time",
        r"(?i)frequency\s+for\s+(?:automated|weather)",
        r"(?i)jet\s+[Aa]",
        r"(?i)fuel\s+available",
        r"(?i)\d{3}\.\d",   // radio frequency like 123.4
    ];

    // Find start of local info section - walk back to nearest sentence boundary
    let combined_pattern = local_triggers.join("|");
    let combined_re = match Regex::new(&combined_pattern) {
        Ok(r) => r,
        Err(_) => return None,
    };

    let trigger_start = combined_re.find(body)?.start();

    // Walk back to nearest period
    let section_start = body[..trigger_start]
        .rfind('.')
        .map(|i| i + 1)
        .unwrap_or(0);

    let body = &body[section_start..];

    // Split into sentences
    static SENT_SPLIT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\.\s+(?=[A-Z0-9])").unwrap()
    });

    let sentences: Vec<&str> = SENT_SPLIT.split(body).collect();

    // Airport name pattern - exclude bare airport name lines
    static AIRPORT_PAT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)^[A-Z][\w\s.\-]+(?:Airport|Field|Center|Airpark)\s*[,\.]*\s*$").unwrap()
    });

    // Anchor pattern to stop collecting at next broadcast
    static NEXT_ANCHOR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)automated\s+weather\s+observation[.\s,]+\d{4}").unwrap()
    });

    let mut local_sentences: Vec<&str> = Vec::new();
    let mut in_local = false;

    for sent in &sentences {
        if combined_re.is_match(sent) {
            in_local = true;
        }
        if in_local {
            if NEXT_ANCHOR.is_match(sent) {
                break;
            }
            let stripped = sent.trim();
            if !stripped.is_empty() && !AIRPORT_PAT.is_match(stripped) {
                local_sentences.push(stripped);
            }
        }
    }

    if local_sentences.is_empty() {
        return None;
    }

    // Join sentences
    let mut result = local_sentences.join(". ");
    result = result.trim_end_matches(|c| c == '.' || c == ',' || c == ' ').to_string();

    // Strip trailing bare airport name
    static TRAILING_AIRPORT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\.\s+[A-Z][\w\s.\-]+(?:Airport|Field|Center|Airpark)\s*\.?\s*$").unwrap()
    });
    result = TRAILING_AIRPORT.replace(&result, "").to_string();

    // Strip leading altimeter fragment
    static LEADING_ALT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)^[Aa]ltimeter[\s.,]+\d{4}(\d*[\s.,]+)").unwrap()
    });
    result = LEADING_ALT.replace(&result, "$1").trim().to_string();

    if result.is_empty() {
        None
    } else {
        if !result.ends_with('.') {
            result.push('.');
        }
        Some(result)
    }
}
