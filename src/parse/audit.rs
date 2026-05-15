/// Post-parse transcript audit.
/// Checks the normalized transcript for keywords that should have produced
/// parsed values, and flags cases where the parser likely missed something.
///
/// Returns a list of audit warning strings. Empty = clean.

use once_cell::sync::Lazy;
use regex::Regex;

use crate::parse::ParsedWeather;

pub fn audit(transcript: &str, parsed: &ParsedWeather) -> Vec<String> {
    let t = transcript.to_lowercase();
    let mut warnings: Vec<String> = Vec::new();

    // -----------------------------------------------------------------------
    // Time
    // -----------------------------------------------------------------------
    static ZULU: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b\d{4}\s*zulu\b").unwrap());
    if ZULU.is_match(&t) && parsed.time.as_deref().unwrap_or("N/A") == "N/A" {
        warnings.push("time: transcript contains a zulu time but none was parsed".into());
    }

    // -----------------------------------------------------------------------
    // Wind
    // -----------------------------------------------------------------------
    let wind_na = parsed.wind.as_ref()
        .map(|w| w.raw.as_deref().unwrap_or("N/A") == "N/A")
        .unwrap_or(true);

    static WIND_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bwind\b").unwrap()
    });
    if wind_na && WIND_KW.is_match(&t) {
        warnings.push("wind: transcript contains 'wind' but no wind was parsed".into());
    }

    // Variable wind
    static VARIABLE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bvariable\b").unwrap()
    });
    if VARIABLE.is_match(&t) {
        let is_variable = parsed.wind.as_ref()
            .and_then(|w| w.variable)
            .unwrap_or(false);
        if !is_variable {
            warnings.push("wind: transcript contains 'variable' but wind not marked variable".into());
        }
    }

    // Gusts
    static GUST: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bgust").unwrap()
    });
    if GUST.is_match(&t) {
        let has_gust = parsed.wind.as_ref()
            .and_then(|w| w.gust_kt.as_ref())
            .is_some();
        if !has_gust {
            warnings.push("wind_gust: transcript mentions gusts but no gust speed was parsed".into());
        }
    }

    // Calm
    static CALM: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bwind\s+calm\b|\bcalm\s+wind\b").unwrap()
    });
    if CALM.is_match(&t) {
        let is_calm = parsed.wind.as_ref()
            .and_then(|w| w.calm)
            .unwrap_or(false);
        if !is_calm {
            warnings.push("wind: transcript says calm but wind not marked calm".into());
        }
    }

    // -----------------------------------------------------------------------
    // Visibility
    // -----------------------------------------------------------------------
    static VIS_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bvisibility\b").unwrap()
    });
    if VIS_KW.is_match(&t) {
        let vis_na = parsed.visibility_sm.as_deref().unwrap_or("N/A") == "N/A";
        if vis_na {
            warnings.push("visibility: transcript contains 'visibility' but none was parsed".into());
        }
    }

    // -----------------------------------------------------------------------
    // Sky / Ceiling
    // -----------------------------------------------------------------------
    static SKY_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\b(ceiling|overcast|broken|scattered|few|clear below|sky condition)\b").unwrap()
    });
    let sky_na = parsed.sky.is_empty()
        || (parsed.sky.len() == 1 && parsed.sky[0].coverage == "N/A");

    if SKY_KW.is_match(&t) && sky_na {
        warnings.push("sky: transcript mentions sky conditions but none were parsed".into());
    }

    // Ceiling specifically
    static CEILING_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bceiling\b").unwrap()
    });
    if CEILING_KW.is_match(&t) && sky_na {
        warnings.push("sky: transcript mentions ceiling but no ceiling was parsed".into());
    }

    // -----------------------------------------------------------------------
    // Temperature
    // -----------------------------------------------------------------------
    static TEMP_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\btemperature\b").unwrap()
    });
    if TEMP_KW.is_match(&t) && parsed.temperature_c.is_none() {
        warnings.push("temperature: transcript mentions temperature but none was parsed".into());
    }

    // Dewpoint
    static DEWP_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bdewpoint\b|\bdew\s+point\b").unwrap()
    });
    if DEWP_KW.is_match(&t) && parsed.dewpoint_c.is_none() {
        warnings.push("dewpoint: transcript mentions dewpoint but none was parsed".into());
    }

    // -----------------------------------------------------------------------
    // Altimeter
    // -----------------------------------------------------------------------
    static ALT_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\baltimeter\b").unwrap()
    });
    if ALT_KW.is_match(&t) {
        let alt_na = parsed.altimeter_inhg.as_deref().unwrap_or("N/A") == "N/A";
        if alt_na {
            warnings.push("altimeter: transcript mentions altimeter but none was parsed".into());
        }
    }

    // -----------------------------------------------------------------------
    // Phenomena
    // -----------------------------------------------------------------------
    static RAIN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\brain\b").unwrap());
    static SNOW: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bsnow\b").unwrap());
    static FOG:  Lazy<Regex> = Lazy::new(|| Regex::new(r"\bfog\b").unwrap());
    static HAZE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bhaze\b").unwrap());
    static MIST: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bmist\b").unwrap());
    static THDR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bthunderstorm\b").unwrap()
    });
    static THDR_INFO: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bthunderstorm\s+information\s+not\s+available\b|\btsno\b").unwrap()
    });
    static DRIZZLE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bdrizzle\b").unwrap());
    static HAIL:    Lazy<Regex> = Lazy::new(|| Regex::new(r"\bhail\b").unwrap());
    static FREEZING: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bfreezing\b").unwrap());

    let phenomena_codes: Vec<&str> = parsed.phenomena.iter().map(|s| s.as_str()).collect();

    let check_phenomenon = |re: &Regex, codes: &[&str], label: &str, warnings: &mut Vec<String>| {
        if re.is_match(&t) && !codes.iter().any(|c| c.contains(label)) {
            warnings.push(format!(
                "phenomena: transcript mentions {} but it was not captured in phenomena",
                label
            ));
        }
    };

    if RAIN.is_match(&t) && !phenomena_codes.iter().any(|c| c.contains("RA") || c.contains("FZRA")) {
        warnings.push("phenomena: transcript mentions rain but RA not in phenomena".into());
    }
    if SNOW.is_match(&t) && !phenomena_codes.iter().any(|c| c.contains("SN") || c.contains("SG") || c.contains("BLSN")) {
        warnings.push("phenomena: transcript mentions snow but SN not in phenomena".into());
    }
    if FOG.is_match(&t) && !phenomena_codes.iter().any(|c| c.contains("FG") || c.contains("FZFG")) {
        warnings.push("phenomena: transcript mentions fog but FG not in phenomena".into());
    }
    if HAZE.is_match(&t) && !phenomena_codes.iter().any(|c| *c == "HZ") {
        warnings.push("phenomena: transcript mentions haze but HZ not in phenomena".into());
    }
    if MIST.is_match(&t) && !phenomena_codes.iter().any(|c| *c == "BR") {
        warnings.push("phenomena: transcript mentions mist but BR not in phenomena".into());
    }
    if THDR.is_match(&t) && !THDR_INFO.is_match(&t)
        && !phenomena_codes.iter().any(|c| c.contains("TS"))
    {
        warnings.push("phenomena: transcript mentions thunderstorm but TS not in phenomena".into());
    }
    if DRIZZLE.is_match(&t) && !phenomena_codes.iter().any(|c| c.contains("DZ")) {
        warnings.push("phenomena: transcript mentions drizzle but DZ not in phenomena".into());
    }
    if HAIL.is_match(&t) && !phenomena_codes.iter().any(|c| c.contains("GR") || c.contains("GS")) {
        warnings.push("phenomena: transcript mentions hail but GR not in phenomena".into());
    }
    if FREEZING.is_match(&t)
        && !phenomena_codes.iter().any(|c| c.contains("FZ"))
    {
        warnings.push("phenomena: transcript mentions freezing but no FZ phenomenon captured".into());
    }

    // -----------------------------------------------------------------------
    // Density altitude
    // -----------------------------------------------------------------------
    static DA_KW: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\bdensity\s+alt").unwrap()
    });
    if DA_KW.is_match(&t) && parsed.density_altitude_ft.is_none() {
        warnings.push("density_altitude: transcript mentions density altitude but none was parsed".into());
    }

    warnings
}
