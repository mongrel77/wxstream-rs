use once_cell::sync::Lazy;
use regex::Regex;

/// Convert spoken/formatted digits to plain numeric strings.
/// Mirrors normalize() from parse_transcripts.py exactly.
pub fn normalize(text: &str) -> String {
    let mut t = text.to_string();

    // Pre-split: separate visibility value from adjacent sky altitude
    static VIS_PRESPLIT: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(visibility[\s.,]+)((?:\w+\s+)*?)(\w+\s+)((?:thousand|hundred)\b)").unwrap()
    });
    t = VIS_PRESPLIT.replace_all(&t, "$1$2. $3$4").to_string();

    // Collapse hyphen-separated single digits: 1-4-5-2 -> 1452
    // Use \b instead of lookbehind/lookahead (not supported in Rust regex)
    static HYPHEN_DIGITS: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\b(\d)(?:-(\d))+\b").unwrap()
    });
    t = HYPHEN_DIGITS.replace_all(&t, |caps: &regex::Captures| {
        caps[0].replace('-', "")
    }).to_string();

    // Insert space at fused AWOS keyword boundaries
    static ZULU_FUSE: Lazy<Regex> = Lazy::new(|| Regex::new(r"([Zz]ulu)([A-Za-z])").unwrap());
    t = ZULU_FUSE.replace_all(&t, "$1 $2").to_string();

    for kw in &["Wind", "Visibility", "Sky", "Temperature", "Dewpoint", "Altimeter", "Remarks"] {
        let pat = format!(r"([a-z])({})\b", kw);
        let re  = Regex::new(&pat).unwrap();
        t = re.replace_all(&t, "$1 $2").to_string();
    }

    // niner / nineer -> 9
    static NINER: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bninee?r\b").unwrap());
    t = NINER.replace_all(&t, "9").to_string();

    static NINER_DIGIT: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d)er\b").unwrap());
    t = NINER_DIGIT.replace_all(&t, "$1").to_string();

    // Spoken tens
    static TWENTY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\btwenty\b").unwrap());
    static THIRTY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bthirty\b").unwrap());
    t = TWENTY.replace_all(&t, "20").to_string();
    t = THIRTY.replace_all(&t, "30").to_string();

    // Spoken word digits
    let word_digits = [
        (r"(?i)\bone\b",   "1"), (r"(?i)\btwo\b",   "2"), (r"(?i)\bthree\b", "3"),
        (r"(?i)\bfour\b",  "4"), (r"(?i)\bfive\b",  "5"), (r"(?i)\bsix\b",   "6"),
        (r"(?i)\bseven\b", "7"), (r"(?i)\beight\b", "8"), (r"(?i)\bnine\b",  "9"),
        (r"(?i)\bzero\b",  "0"),
    ];
    for (pat, digit) in &word_digits {
        let re = Regex::new(pat).unwrap();
        t = re.replace_all(&t, *digit).to_string();
    }

    // Space-digit collapse: "3 0" -> "30"
    static SPACE_DIGITS: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d)(?: (\d))+\b").unwrap());
    t = SPACE_DIGITS.replace_all(&t, |caps: &regex::Captures| {
        caps[0].replace(' ', "")
    }).to_string();

    // Period-separated digits: '3. 0. 2. 5.' -> '3025.'
    for _ in 0..6 {
        static PERIOD_DIGITS: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d)\.\s*(\d)").unwrap());
        let t2 = PERIOD_DIGITS.replace_all(&t, "$1$2").to_string();
        if t2 == t { break; }
        t = t2;
    }

    // Comma-collapse patterns (no magnitude following)
    let no_mag = r"(?!\s*,?\s*(?:thousand|hundred))";
    for pat in &[
        format!(r"\b(\d{{1,3}}),\s*(\d),\s*(\d),\s*(\d)\b{}", no_mag),
        format!(r"\b(\d{{1,3}}),\s*(\d),\s*(\d)\b{}", no_mag),
        format!(r"\b(\d{{1,3}}),\s*(\d)\b{}", no_mag),
    ] {
        if let Ok(re) = Regex::new(pat) {
            t = re.replace_all(&t, |caps: &regex::Captures| {
                caps.iter().skip(1).flatten().map(|m| m.as_str()).collect::<String>()
            }).to_string();
        }
    }

    // Two-digit comma collapse: "29, 96" -> "2996"
    let pat2d = format!(r"\b(\d{{1,2}}),\s*(\d{{2}})\b{}", no_mag);
    if let Ok(re) = Regex::new(&pat2d) {
        t = re.replace_all(&t, "$1$2").to_string();
    }

    // Compound tens: "20-1" -> "21", "20 1" -> "21"
    static COMPOUND_TENS_DASH: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(2[0-9])-([1-9])\b").unwrap());
    t = COMPOUND_TENS_DASH.replace_all(&t, |caps: &regex::Captures| {
        let a: i32 = caps[1].parse().unwrap_or(0);
        let b: i32 = caps[2].parse().unwrap_or(0);
        (a + b).to_string()
    }).to_string();

    static COMPOUND_TENS_SPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(2[0-9]) ([1-9])\b").unwrap());
    t = COMPOUND_TENS_SPACE.replace_all(&t, |caps: &regex::Captures| {
        let a: i32 = caps[1].parse().unwrap_or(0);
        let b: i32 = caps[2].parse().unwrap_or(0);
        (a + b).to_string()
    }).to_string();

    // Convert spoken magnitudes
    static THOUSAND: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)(\d+),?\s*thousand").unwrap());
    t = THOUSAND.replace_all(&t, |caps: &regex::Captures| {
        let n: i64 = caps[1].parse().unwrap_or(0);
        (n * 1000).to_string()
    }).to_string();

    static HUNDRED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)(\d+),?\s*hundred").unwrap());
    t = HUNDRED.replace_all(&t, |caps: &regex::Captures| {
        let n: i64 = caps[1].parse().unwrap_or(0);
        (n * 100).to_string()
    }).to_string();

    // Combine adjacent thousand+hundred: "1000 100" -> "1100"
    static THOU_HUND: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d+)000 (\d+)00\b").unwrap());
    t = THOU_HUND.replace_all(&t, |caps: &regex::Captures| {
        let a: i64 = caps[1].parse().unwrap_or(0);
        let b: i64 = caps[2].parse().unwrap_or(0);
        (a * 1000 + b * 100).to_string()
    }).to_string();

    // Thousands-separator variants
    static THOU_SEP_COMMA: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d{1,2}),\s*000\b").unwrap());
    t = THOU_SEP_COMMA.replace_all(&t, "${1}000").to_string();

    static THOU_SEP_DASH: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d{1,2})-000\b").unwrap());
    t = THOU_SEP_DASH.replace_all(&t, "${1}000").to_string();

    // Spoken decimal point: '128 point 45' -> '128.45'
    static DECIMAL2: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d)\s+[Pp]oint\s+(\d)").unwrap());
    t = DECIMAL2.replace_all(&t, "$1.$2").to_string();

    // Zero-pad 3-digit times before 'local time': '700 local time' -> '0700 local time'
    static LOCAL_TIME: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b(\d{3})\b(\s+local\s+time)").unwrap());
    t = LOCAL_TIME.replace_all(&t, |caps: &regex::Captures| {
        format!("0{}{}", &caps[1], &caps[2])
    }).to_string();

    t
}

/// Truncate digit-storm hallucinations — runs of comma-separated spoken digits.
/// Mirrors _truncate_digit_storm() from parse_transcripts.py.
pub fn truncate_digit_storm(text: &str, min_run: usize) -> String {
    let digit_word = "(?:zero|one|two|three|four|five|six|seven|eight|niner|nine)";
    let sep        = "[, ]+";
    let pattern    = format!(
        "(?i)(?:{}{}){{{},}}{}",
        digit_word, sep, min_run, digit_word
    );
    if let Ok(re) = Regex::new(&pattern) {
        if let Some(m) = re.find(text) {
            return text[..m.start()].trim_end_matches(|c| c == ' ' || c == ',' || c == '.').to_string();
        }
    }
    text.to_string()
}

/// Strip preamble — find the last complete broadcast loop.
/// Returns (segment_text, obs_time_4digit).
/// Mirrors strip_preamble() from parse_transcripts.py.
pub fn strip_preamble(text: &str) -> (String, Option<String>) {
    static PATTERN: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)[Aa]utomated weather observation[.\s,]+(\d{4})[.\s,]*[Zz]ulu(?:[Ww]eather)?").unwrap()
    });

    let matches: Vec<_> = PATTERN.find_iter(text).collect();
    if matches.is_empty() {
        return (text.to_string(), None);
    }

    // Collect obs times
    let captures: Vec<_> = PATTERN.captures_iter(text).collect();

    // First pass: prefer loops with both altimeter and visibility
    for (i, m) in matches.iter().enumerate().rev() {
        let segment = &text[m.start()..];
        let has_alt = Regex::new(r"(?i)\baltimeter\b").unwrap().is_match(segment);
        let has_vis = Regex::new(r"(?i)\bvisibility\b").unwrap().is_match(segment);
        if has_alt && has_vis {
            let obs_time = captures.get(i)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string());
            return (segment.to_string(), obs_time);
        }
    }

    // Second pass: altimeter-only
    for (i, m) in matches.iter().enumerate().rev() {
        let segment = &text[m.start()..];
        let has_alt = Regex::new(r"(?i)\baltimeter\b").unwrap().is_match(segment);
        if has_alt {
            let obs_time = captures.get(i)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string());
            return (segment.to_string(), obs_time);
        }
    }

    let last = matches.last().unwrap();
    let obs_time = captures.last()
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string());
    (text[last.start()..].to_string(), obs_time)
}
