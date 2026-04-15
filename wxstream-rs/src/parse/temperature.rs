use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug, Clone, Default)]
pub struct TempResult {
    pub display: String,
    pub metar:   String,
}

fn fix_decimal(v: &str) -> String {
    let bare = v.trim_start_matches('-');
    if bare.len() == 2 && bare.ends_with('0') {
        if let Ok(n) = bare.parse::<f64>() {
            if n > 35.0 {
                return format!("{}{}.{}", if v.starts_with('-') { "-" } else { "" },
                    &bare[..1], &bare[1..]);
            }
        }
    }
    v.to_string()
}

fn format_temp_metar(val: f64) -> String {
    if val < 0.0 {
        format!("M{:02}", val.abs() as i32)
    } else {
        format!("{:02}", val as i32)
    }
}

/// Extract temperature and dewpoint. Mirrors extract_temp_dp() from parse_transcripts.py.
pub fn extract_temp_dp(text: &str) -> TempResult {
    let sep  = r"[\s,.]+";
    let tval = r"(minus[\s,.]+)?([.\d]+)";
    let cel  = r"(?:[\s,.]+[Cc]el[sc]ius)?";

    for dp_kw in &[r"dew[\s,.]*point", r"dewpoint"] {
        let pat = format!(
            r"(?i)temp(?:erature)?{sep}{tval}{cel}{sep}{dp_kw}{sep}{tval}",
            sep  = sep,
            tval = tval,
            cel  = cel,
            dp_kw = dp_kw
        );
        if let Ok(re) = Regex::new(&pat) {
            if let Some(caps) = re.captures(text) {
                let t_neg = caps.get(1).is_some();
                let t_val = &caps[2];
                let d_neg = caps.get(3).is_some();
                let d_val = &caps[4];

                let t_raw = format!("{}{}", if t_neg { "-" } else { "" }, t_val.trim_end_matches('.'));
                let d_raw = format!("{}{}", if d_neg { "-" } else { "" }, d_val.trim_end_matches('.'));

                let t_fixed = fix_decimal(&t_raw);
                let d_fixed = fix_decimal(&d_raw);

                let tf: f64 = t_fixed.parse().unwrap_or(0.0);
                let df: f64 = d_fixed.parse().unwrap_or(0.0);

                let t_metar = format_temp_metar(tf);
                let d_metar = format_temp_metar(df);

                return TempResult {
                    display: format!("{}°C / {}°C", t_fixed, d_fixed),
                    metar:   format!("{}/{}", t_metar, d_metar),
                };
            }
        }
    }

    // Temperature without dewpoint
    let pat_t = format!(r"(?i)temp(?:erature)?{sep}{tval}{cel}", sep = sep, tval = tval);
    if let Ok(re) = Regex::new(&pat_t) {
        if let Some(caps) = re.captures(text) {
            let t_neg = caps.get(1).is_some();
            let t_val = &caps[2];
            let t_raw = format!("{}{}", if t_neg { "-" } else { "" }, t_val.trim_end_matches('.'));
            let t_fixed = fix_decimal(&t_raw);
            let tf: f64 = t_fixed.parse().unwrap_or(0.0);
            let t_metar = format_temp_metar(tf);
            return TempResult {
                display: format!("{}°C / N/A", t_fixed),
                metar:   format!("{}/", t_metar),
            };
        }
    }

    // Missing
    static MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)temperature[\s.,]+(?:missing|information[\s.,]+not[\s.,]+available)\b").unwrap()
    });
    if MISSING.is_match(text) {
        return TempResult { display: "Missing".into(), metar: "MIS/MIS".into() };
    }

    TempResult { display: "N/A".into(), metar: "N/A".into() }
}
