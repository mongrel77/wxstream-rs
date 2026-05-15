/// Sanity validation for parsed AWOS/ASOS weather fields.
/// Returns a list of warning strings for any fields that failed validation.
/// Invalid fields are set to None/N/A by the caller based on the warnings.

use crate::parse::{ParsedSky, ParsedWind, ParsedWeather};

// ---------------------------------------------------------------------------
// Valid ranges
// ---------------------------------------------------------------------------

const WIND_DIR_MIN:  f64 = 0.0;
const WIND_DIR_MAX:  f64 = 360.0;
const WIND_SPD_MAX:  f64 = 100.0;
const WIND_GUST_MAX: f64 = 150.0;
const VIS_MAX:       f64 = 10.0;
const SKY_HT_MIN:    f64 = 100.0;
const SKY_HT_MAX:    f64 = 25000.0;
const TEMP_MIN:      f64 = -60.0;
const TEMP_MAX:      f64 = 50.0;
const DEWP_MIN:      f64 = -80.0;
const DEWP_MAX:      f64 = 35.0;
const ALT_MIN:       f64 = 27.50;
const ALT_MAX:       f64 = 32.00;
const DENSITY_ALT_MIN: f64 = -2000.0;
const DENSITY_ALT_MAX: f64 = 15000.0;

// ---------------------------------------------------------------------------
// Validation result
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ValidationResult {
    pub warnings: Vec<String>,
    /// Fields that should be cleared (set to N/A / None)
    pub clear_fields: Vec<String>,
}

impl ValidationResult {
    pub fn is_clean(&self) -> bool {
        self.warnings.is_empty()
    }

    fn warn(&mut self, field: &str, msg: &str, clear: bool) {
        self.warnings.push(format!("{}: {}", field, msg));
        if clear {
            self.clear_fields.push(field.to_string());
        }
    }
}

// ---------------------------------------------------------------------------
// Main validation entry point
// ---------------------------------------------------------------------------

pub fn validate(parsed: &mut ParsedWeather) -> ValidationResult {
    let mut result = ValidationResult::default();

    validate_wind(&mut parsed.wind, &mut result);
    validate_visibility(&mut parsed.visibility_sm, &mut result);
    validate_sky(&mut parsed.sky, &mut result);
    validate_temperature(
        &mut parsed.temperature_c,
        &mut parsed.dewpoint_c,
        &mut result,
    );
    validate_altimeter(&mut parsed.altimeter_inhg, &mut result);
    validate_density_altitude(&mut parsed.density_altitude_ft, &mut result);

    result
}

// ---------------------------------------------------------------------------
// Wind
// ---------------------------------------------------------------------------

fn validate_wind(wind: &mut Option<ParsedWind>, result: &mut ValidationResult) {
    let w = match wind.as_mut() {
        Some(w) => w,
        None => return,
    };

    if w.raw.as_deref() == Some("N/A") || w.calm == Some(true) || w.variable == Some(true) {
        return;
    }

    // Direction
    if let Some(dir_str) = &w.direction {
        if let Ok(dir) = dir_str.parse::<f64>() {
            if dir < WIND_DIR_MIN || dir > WIND_DIR_MAX {
                result.warn(
                    "wind_direction",
                    &format!("{} is outside 0-360deg", dir),
                    true,
                );
                w.direction = None;
                w.metar = None;
            }
        }
    }

    // Speed
    if let Some(spd_str) = &w.speed_kt {
        if let Ok(spd) = spd_str.parse::<f64>() {
            if spd > WIND_SPD_MAX {
                result.warn(
                    "wind_speed",
                    &format!("{} kt exceeds max of {} kt", spd, WIND_SPD_MAX),
                    true,
                );
                w.speed_kt = None;
                w.metar = None;
            }
        }
    }

    // Gust
    if let Some(gust_str) = &w.gust_kt {
        if let Ok(gust) = gust_str.parse::<f64>() {
            if gust > WIND_GUST_MAX {
                result.warn(
                    "wind_gust",
                    &format!("{} kt exceeds max gust of {} kt", gust, WIND_GUST_MAX),
                    true,
                );
                w.gust_kt = None;
                w.metar = None;
            } else if let Some(spd_str) = &w.speed_kt {
                if let Ok(spd) = spd_str.parse::<f64>() {
                    if gust <= spd {
                        result.warn(
                            "wind_gust",
                            &format!("gust {} kt must exceed speed {} kt", gust, spd),
                            true,
                        );
                        w.gust_kt = None;
                        w.metar = None;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

fn validate_visibility(vis: &mut Option<String>, result: &mut ValidationResult) {
    let v = match vis.as_ref() {
        Some(v) => v.clone(),
        None => return,
    };

    if v == "N/A" || v == "Missing" || v.starts_with('>') {
        return;
    }

    // Extract numeric value from "X SM" or "X/Y SM"
    let num_str = v.replace(" SM", "").replace(" sm", "");
    let val = parse_fraction_or_float(&num_str);

    if let Some(val) = val {
        if val < 0.0 || val > VIS_MAX + 0.01 {
            result.warn(
                "visibility",
                &format!("{} SM is outside 0-{} SM range", val, VIS_MAX),
                true,
            );
            *vis = Some("N/A".to_string());
        }
    }
}

fn parse_fraction_or_float(s: &str) -> Option<f64> {
    let s = s.trim();
    if let Ok(v) = s.parse::<f64>() {
        return Some(v);
    }
    // Handle fractions like "1/4", "1/2", "3/4"
    if let Some(slash) = s.find('/') {
        let num: f64 = s[..slash].trim().parse().ok()?;
        let den: f64 = s[slash+1..].trim().parse().ok()?;
        if den != 0.0 {
            return Some(num / den);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Sky
// ---------------------------------------------------------------------------

fn validate_sky(sky: &mut Vec<ParsedSky>, result: &mut ValidationResult) {
    let mut to_clear: Vec<usize> = Vec::new();

    for (i, layer) in sky.iter().enumerate() {
        if layer.coverage == "CLR" || layer.coverage == "SKC" || layer.coverage == "N/A" {
            continue;
        }
        if let Some(ht) = layer.height_ft {
            let ht = ht as f64;
            if ht < SKY_HT_MIN || ht > SKY_HT_MAX {
                result.warn(
                    "sky",
                    &format!(
                        "{} at {} ft is outside {}-{} ft range",
                        layer.coverage, ht, SKY_HT_MIN, SKY_HT_MAX
                    ),
                    false,
                );
                to_clear.push(i);
            }
        }
    }

    // Remove invalid layers
    for i in to_clear.into_iter().rev() {
        sky.remove(i);
    }

    if sky.is_empty() {
        sky.push(ParsedSky { coverage: "N/A".to_string(), height_ft: None });
    }
}

// ---------------------------------------------------------------------------
// Temperature and Dewpoint
// ---------------------------------------------------------------------------

fn validate_temperature(
    temp: &mut Option<String>,
    dewp: &mut Option<String>,
    result: &mut ValidationResult,
) {
    let t_val = temp.as_ref().and_then(|s| {
        if s == "N/A" { None } else { s.parse::<f64>().ok() }
    });
    let d_val = dewp.as_ref().and_then(|s| {
        if s == "N/A" { None } else { s.parse::<f64>().ok() }
    });

    if let Some(t) = t_val {
        if t < TEMP_MIN || t > TEMP_MAX {
            result.warn(
                "temperature",
                &format!("{}degC is outside {}-{}degC range", t, TEMP_MIN, TEMP_MAX),
                true,
            );
            *temp = None;
        }
    }

    if let Some(d) = d_val {
        if d < DEWP_MIN || d > DEWP_MAX {
            result.warn(
                "dewpoint",
                &format!("{}degC is outside {}-{}degC range", d, DEWP_MIN, DEWP_MAX),
                true,
            );
            *dewp = None;
        }
    }

    // Dewpoint must not exceed temperature
    if let (Some(t), Some(d)) = (t_val, d_val) {
        if d > t + 0.5 {
            // Allow small floating point tolerance
            result.warn(
                "dewpoint",
                &format!("dewpoint {}degC exceeds temperature {}degC", d, t),
                true,
            );
            *dewp = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Altimeter
// ---------------------------------------------------------------------------

fn validate_altimeter(alt: &mut Option<String>, result: &mut ValidationResult) {
    let a = match alt.as_ref() {
        Some(a) => a.clone(),
        None => return,
    };

    if a == "N/A" || a == "Missing" {
        return;
    }

    // Extract numeric value from "29.92 inHg"
    let num_str = a.replace(" inHg", "").replace(" inhg", "");
    if let Ok(val) = num_str.trim().parse::<f64>() {
        if val < ALT_MIN || val > ALT_MAX {
            result.warn(
                "altimeter",
                &format!("{} inHg is outside {:.2}-{:.2} inHg range", val, ALT_MIN, ALT_MAX),
                true,
            );
            *alt = Some("N/A".to_string());
        }
    }
}

// ---------------------------------------------------------------------------
// Density altitude
// ---------------------------------------------------------------------------

fn validate_density_altitude(da: &mut Option<String>, result: &mut ValidationResult) {
    let d = match da.as_ref() {
        Some(d) => d.clone(),
        None => return,
    };

    // Strip commas
    let num_str = d.replace(',', "");
    if let Ok(val) = num_str.trim().parse::<f64>() {
        if val < DENSITY_ALT_MIN || val > DENSITY_ALT_MAX {
            result.warn(
                "density_altitude",
                &format!(
                    "{} ft is outside {}-{} ft range",
                    val, DENSITY_ALT_MIN, DENSITY_ALT_MAX
                ),
                true,
            );
            *da = None;
        }
    }
}
