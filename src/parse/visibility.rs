// visibility.rs — mirrors extract_visibility() from parse_transcripts.py

use once_cell::sync::Lazy;
use regex::Regex;

pub fn extract_visibility(text: &str) -> String {
    // Strip commas used as separators
    let text = text.replace(',', " ");

    // Fractional visibility conversions
    static FRAC_PATTERNS: &[(&str, &str)] = &[
        (r"(?i)\b(\d+)\s+and\s+(?:one\s+half|1\s+half|half)\b",          "HALF"),
        (r"(?i)\b(\d+)\s+and\s+(?:three\s+quarters?|3\s+quarters?)\b",   "THREE_Q"),
        (r"(?i)\b(\d+)\s+and\s+(?:one\s+quarter|1\s+quarter|quarter)\b", "ONE_Q"),
        (r"(?i)\b(?:three|3)\s+quarters?\b",                              "0.75"),
        (r"(?i)\b(?:one|1)\s+half\b",                                     "0.5"),
        (r"(?i)\b(?:one|1)\s+quarter\b",                                  "0.25"),
    ];

    let mut t = text.clone();

    // Apply whole+fraction patterns
    if let Ok(re) = Regex::new(r"(?i)\b(\d+)\s+and\s+(?:one\s+half|1\s+half|half)\b") {
        t = re.replace_all(&t, |caps: &regex::Captures| {
            let n: f64 = caps[1].parse().unwrap_or(0.0);
            format!("{}", n + 0.5)
        }).to_string();
    }
    if let Ok(re) = Regex::new(r"(?i)\b(\d+)\s+and\s+(?:three\s+quarters?|3\s+quarters?)\b") {
        t = re.replace_all(&t, |caps: &regex::Captures| {
            let n: f64 = caps[1].parse().unwrap_or(0.0);
            format!("{}", n + 0.75)
        }).to_string();
    }
    if let Ok(re) = Regex::new(r"(?i)\b(\d+)\s+and\s+(?:one\s+quarter|1\s+quarter|quarter)\b") {
        t = re.replace_all(&t, |caps: &regex::Captures| {
            let n: f64 = caps[1].parse().unwrap_or(0.0);
            format!("{}", n + 0.25)
        }).to_string();
    }
    // Bare fractions
    static THREE_Q: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\b(?:three|3)\s+quarters?\b").unwrap());
    t = THREE_Q.replace_all(&t, "0.75").to_string();
    static ONE_HALF: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\b(?:one|1)\s+half\b").unwrap());
    t = ONE_HALF.replace_all(&t, "0.5").to_string();
    static ONE_Q: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\b(?:one|1)\s+quarter\b").unwrap());
    t = ONE_Q.replace_all(&t, "0.25").to_string();

    // Find visibility value
    static VIS_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)visibility[\s.,]+(more[\s.,]+than[\s.,]+)?(\d+\.?\d*)").unwrap()
    });

    for caps in VIS_RE.captures_iter(&t) {
        let prefix  = if caps.get(1).is_some() { ">" } else { "" };
        let val_str = &caps[2];
        let val: f64 = val_str.parse().unwrap_or(0.0);

        if val > 9999.0 { continue; }

        if val > 10.0 && prefix.is_empty() {
            let digits = val_str.to_string();
            if digits.len() >= 2 {
                if let Ok(first_two) = digits[..2].parse::<u32>() {
                    if first_two == 10 { return "10 SM".into(); }
                }
            }
            if let Some(first_char) = digits.chars().next() {
                if let Some(fd) = first_char.to_digit(10) {
                    if (1..=9).contains(&fd) {
                        return format!("{} SM", fd);
                    }
                }
            }
            continue;
        }

        let frac_map = [(0.25, "1/4"), (0.5, "1/2"), (0.75, "3/4")];
        for (fv, fs) in &frac_map {
            if (val - fv).abs() < 0.001 {
                return format!("{}{} SM", prefix, fs);
            }
        }

        if (val - val.floor()).abs() < 0.001 {
            return format!("{}{} SM", prefix, val as i64);
        } else {
            return format!("{}{} SM", prefix, val);
        }
    }

    // Missing
    static MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)visibility[\s.,]+(?:missing|information[\s.,]+not[\s.,]+available)\b").unwrap()
    });
    if MISSING.is_match(&t) {
        return "Missing".into();
    }

    "N/A".into()
}
