use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug, Clone, Default)]
pub struct WindResult {
    pub display: String,
    pub metar:   String,
}

/// Extract wind from normalized transcript text.
/// Mirrors extract_wind() from parse_transcripts.py.
pub fn extract_wind(text: &str, full_text: &str) -> WindResult {
    let ft = if full_text.is_empty() { text } else { full_text };

    // Missing
    static MISSING: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bwind[\s.,]+(?:missing|information[\s.,]+not[\s.,]+available)\b").unwrap()
    });
    if MISSING.is_match(text) {
        return WindResult { display: "Missing".into(), metar: "MIS".into() };
    }

    // Calm
    static CALM: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bwind[\s.,]+calm\b").unwrap()
    });
    if CALM.is_match(text) {
        return WindResult { display: "Calm".into(), metar: "00000KT".into() };
    }

    // Variable with speed: 'wind variable at N'
    static VAR_SPD: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bwind[\s.,]+variable[\s.,]+(?:at[\s.,]+)?(\d+)").unwrap()
    });
    if let Some(m) = VAR_SPD.captures(text) {
        let spd = m[1].parse::<u32>().unwrap_or(0);
        return WindResult {
            display: format!("Variable at {} kts", spd),
            metar:   format!("VRB{:02}KT", spd),
        };
    }

    // Directional wind: 'wind DDD at NNN'
    static DIR_WIND: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)\bwind[\s.,]+(\d{1,3})[\s.,]+(?:at[\s.,]+)?(\d+)").unwrap()
    });
    if let Some(m) = DIR_WIND.captures(text) {
        let dir = m[1].parse::<u32>().unwrap_or(0);
        let spd = m[2].parse::<u32>().unwrap_or(0);
        let dir_s = format!("{:03}", dir);
        let spd_s = format!("{:02}", spd);

        // Check for gusts
        let gust_pat = format!(
            r"(?i)\bwind[\s.,]+{}[\s.,]+(?:at[\s.,]+)?\d+[\s.,]+gusts?[\s.,]+(\d+)",
            dir_s
        );
        let gust_part;
        let gust_metar;
        if let Ok(re) = Regex::new(&gust_pat) {
            if let Some(gm) = re.captures(ft) {
                let g = gm[1].parse::<u32>().unwrap_or(0);
                gust_part  = format!(", gusts {} kts", g);
                gust_metar = format!("G{:02}", g);
            } else {
                // Peak gust fallback
                static PEAK_GUST: Lazy<Regex> = Lazy::new(|| {
                    Regex::new(r"(?i)\bpeak[\s.,]+gust(?:s)?[\s.,]+(\d+)").unwrap()
                });
                if let Some(pgm) = PEAK_GUST.captures(ft) {
                    let g = pgm[1].parse::<u32>().unwrap_or(0);
                    gust_part  = format!(", gusts {} kts", g);
                    gust_metar = format!("G{:02}", g);
                } else {
                    gust_part  = String::new();
                    gust_metar = String::new();
                }
            }
        } else {
            gust_part  = String::new();
            gust_metar = String::new();
        }

        // Variable wind range: 'variable between X and Y'
        // Exclude matches preceded by 'visibility' or 'ceiling' — those are
        // visibility/ceiling variability remarks, not wind direction variability.
        // Additionally guard that lo/hi are valid compass bearings (0-360).
        static VAR_RANGE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)variable[\s,]+between[\s,]+(\d+)[\s,]+and[\s,]+(\d+)").unwrap()
        });
        let mut var_part  = String::new();
        let mut var_metar = String::new();
        for cap in VAR_RANGE.captures_iter(ft) {
            let m = VAR_RANGE.find(ft).unwrap();
            let before = &ft[..m.start()];
            let before_ctx = &before[before.len().saturating_sub(30)..];
            let before_lc = before_ctx.to_lowercase();
            if before_lc.contains("visibility") || before_lc.contains("ceiling") {
                continue;
            }
            let lo = cap[1].parse::<u32>().unwrap_or(999);
            let hi = cap[2].parse::<u32>().unwrap_or(999);
            // Wind variable headings are compass bearings (0-360); reject altitude values
            if lo > 360 || hi > 360 {
                continue;
            }
            var_part  = format!(", variable {:03}-{:03}", lo, hi);
            var_metar = format!(" {:03}V{:03}", lo, hi);
            break;
        }

        return WindResult {
            display: format!("{}° at {} kts{}{}", dir, spd, gust_part, var_part),
            metar:   format!("{}{}{}{}", dir_s, spd_s, gust_metar, var_metar),
        };
    }

    WindResult { display: "N/A".into(), metar: "N/A".into() }
}
