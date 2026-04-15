use crate::config::TrimConfig;
use crate::models::WordTimestamp;

const TRIGGER: [&str; 3] = ["automated", "weather", "observation"];
const FILTER_BEFORE: [&str; 4] = ["for", "the", "is", "an"];
const STATION_NAME_MAX_WORDS: usize = 8;

pub static INVALID_STATION_WORDS: &[&str] = &[
    "density", "altitude", "remarks", "remark", "temperature", "dewpoint",
    "dew", "altimeter", "visibility", "wind", "sky", "ceiling", "scattered",
    "broken", "overcast", "clear", "few", "haze", "fog", "snow", "rain",
    "peak", "gust", "gusts", "knots", "celcius", "celsius", "inches",
    "mercury", "automated", "weather", "observation", "zulu",
    "zero", "one", "two", "three", "four", "five",
    "six", "seven", "eight", "nine", "niner",
    "hundred", "thousand",
    "thunderstorm", "thunderstorms", "information", "available",
    "distant", "lightning", "present", "vicinity",
    "precipitation", "drizzle", "mist", "unknown", "airborne", "missing",
    "distance", "through", "east",
];

const DIRECTION_WORDS: &[&str] = &[
    "north", "south", "east", "west",
    "northeast", "northwest", "southeast", "southwest",
];

fn word_clean(w: &str) -> String {
    w.to_lowercase()
        .trim_matches(|c| c == '.' || c == ',' || c == ';')
        .to_string()
}

/// Decode the 4-digit obs time spoken after an anchor.
/// Mirrors _anchor_obs_time() from audio_trim.py.
fn anchor_obs_time(words: &[WordTimestamp], anchor_idx: usize) -> Option<String> {
    let digit_map = [
        ("zero", "0"), ("one", "1"), ("two", "2"), ("three", "3"), ("four", "4"),
        ("five", "5"), ("six", "6"), ("seven", "7"), ("eight", "8"),
        ("nine", "9"), ("niner", "9"),
    ];

    let end = (anchor_idx + 12).min(words.len());
    let mut digits: Vec<&str> = Vec::new();

    for w in &words[anchor_idx + 3..end] {
        let wc = word_clean(&w.word);
        if wc == "zulu" || wc == "z" {
            break;
        }
        if let Some((_, d)) = digit_map.iter().find(|(k, _)| *k == wc) {
            digits.push(d);
        }
    }

    if digits.len() >= 4 {
        Some(digits[..4].join(""))
    } else {
        None
    }
}

/// Find the start of the station name that precedes an anchor.
/// Mirrors _station_name_start() from audio_trim.py.
fn station_name_start(
    words: &[WordTimestamp],
    trigger_idx: usize,
    prev_anchor_idx: Option<usize>,
) -> (usize, bool) {
    let min_idx = prev_anchor_idx.map(|i| i + 3).unwrap_or(0);

    // Primary: content-based — walk backward from anchor
    let mut last_invalid = min_idx.saturating_sub(1);
    let start_k = if trigger_idx > 0 { trigger_idx - 1 } else { 0 };

    for k in (min_idx..=start_k).rev() {
        if INVALID_STATION_WORDS.contains(&word_clean(&words[k].word).as_str()) {
            last_invalid = k;
            break;
        }
    }

    let mut station_start = last_invalid + 1;

    // Cap to max station name length
    if trigger_idx > station_start && trigger_idx - station_start > STATION_NAME_MAX_WORDS {
        station_start = trigger_idx - STATION_NAME_MAX_WORDS;
    }

    // Skip direction words that are remark tail words
    let mut just_skipped_direction = false;
    while station_start < trigger_idx {
        let w = word_clean(&words[station_start].word);
        let prev_w = if station_start > 0 {
            word_clean(&words[station_start - 1].word)
        } else {
            String::new()
        };

        let is_remark_direction = DIRECTION_WORDS.contains(&w.as_str())
            && (prev_w == "distant" || station_start <= 1 || just_skipped_direction);

        let is_remark_connector =
            (w == "and" || w == "or" || w == "through") && just_skipped_direction;

        if is_remark_direction || is_remark_connector {
            just_skipped_direction = true;
            station_start += 1;
        } else {
            just_skipped_direction = false;
            break;
        }
    }

    if station_start < trigger_idx {
        let start_word = word_clean(&words[station_start].word);
        if !INVALID_STATION_WORDS.contains(&start_word.as_str()) {
            return (station_start, true);
        }
    }

    // Fallback: gap-based (>0.5s silence)
    for k in (min_idx + 1..trigger_idx).rev() {
        if words[k].start - words[k - 1].end > 0.5 {
            let w = word_clean(&words[k].word);
            if !INVALID_STATION_WORDS.contains(&w.as_str()) {
                return (k, true);
            }
        }
    }

    // Last resort: position 0
    if min_idx == 0 {
        let first = word_clean(&words[0].word);
        if first != "automated" && first != "weather" && first != "observation"
            && !INVALID_STATION_WORDS.contains(&first.as_str())
        {
            return (0, true);
        }
    }

    (min_idx, false)
}

fn has_clean_start(
    words: &[WordTimestamp],
    sw: usize,
    anchor_idx: usize,
) -> bool {
    let sw_t = words[sw].start;
    let a_t  = words[anchor_idx].start;
    sw_t < a_t - 0.5
}

/// Find one complete broadcast loop from word-level timestamps.
/// Mirrors find_loop_from_timestamps() from audio_trim.py.
pub fn find_loop_from_timestamps(
    words: &[WordTimestamp],
    obs_time: Option<&str>,
    station_first_word: Option<&str>,
    cfg: &TrimConfig,
) -> Option<(f64, f64)> {
    if words.is_empty() {
        return None;
    }

    // Find all valid broadcast anchors
    let mut anchors: Vec<usize> = Vec::new();
    let mut i = 0;
    while i + TRIGGER.len() <= words.len() {
        let matches = TRIGGER.iter().enumerate().all(|(j, t)| {
            word_clean(&words[i + j].word) == *t
        });
        if matches {
            let pre = if i > 0 { word_clean(&words[i - 1].word) } else { String::new() };
            if !FILTER_BEFORE.contains(&pre.as_str()) {
                anchors.push(i);
            }
            i += TRIGGER.len();
        } else {
            i += 1;
        }
    }

    // Single anchor fallback
    if anchors.len() < 2 {
        if anchors.len() == 1 {
            let (sw, valid) = station_name_start(words, anchors[0], None);
            let start_sec = if valid {
                (words[sw].start - cfg.preroll_s).max(0.0)
            } else {
                (words[anchors[0]].start - cfg.preroll_s).max(0.0)
            };
            let end_sec = words.last().unwrap().end;
            if cfg.min_loop_s < end_sec - start_sec && end_sec - start_sec < cfg.max_loop_s {
                return Some((start_sec, end_sec));
            }
        }

        // Zero-anchor: use station first word repetition
        if let Some(sfw) = station_first_word {
            let sfw_lower = sfw.to_lowercase().trim_matches('.').to_string();
            let name_hits: Vec<usize> = words.iter().enumerate()
                .filter(|(_, w)| word_clean(&w.word) == sfw_lower)
                .map(|(i, _)| i)
                .collect();

            for j in 0..name_hits.len().saturating_sub(1) {
                let dur = words[name_hits[j + 1]].start - words[name_hits[j]].start;
                if cfg.min_loop_s < dur && dur < cfg.max_loop_s {
                    let start_sec = (words[name_hits[j]].start - cfg.preroll_s).max(0.0);
                    let end_sec   = words[name_hits[j + 1]].start;
                    return Some((start_sec, end_sec));
                }
            }
        }

        return None;
    }

    // Build pairs from adjacent anchors
    let mut pairs: Vec<(usize, usize, bool, f64)> = Vec::new(); // (k, sw, valid, dur)
    for k in 0..anchors.len() - 1 {
        let prev = if k > 0 { Some(anchors[k - 1]) } else { None };
        let (sw, valid) = station_name_start(words, anchors[k], prev);
        let pair_start_t = if valid { words[sw].start } else { words[anchors[k]].start };
        let dur = words[anchors[k + 1]].start - pair_start_t;
        if cfg.min_loop_s < dur && dur < cfg.max_loop_s {
            pairs.push((k, sw, valid, dur));
        }
    }

    if pairs.is_empty() {
        return None;
    }

    // Select best pair
    let mut best: Option<(usize, usize, bool, f64)> = None;

    if let Some(ot) = obs_time {
        let mut clean_match: Option<(usize, usize, bool, f64)> = None;
        let mut any_match:   Option<(usize, usize, bool, f64)> = None;

        for &(k, sw, valid, dur) in &pairs {
            if anchor_obs_time(words, anchors[k]).as_deref() == Some(ot) {
                if any_match.is_none() {
                    any_match = Some((k, sw, valid, dur));
                }
                if has_clean_start(words, sw, anchors[k]) && clean_match.is_none() {
                    clean_match = Some((k, sw, valid, dur));
                }
            }
        }
        best = clean_match.or(any_match);
    }

    // Fall back to median duration
    if best.is_none() {
        let durs: Vec<f64> = pairs.iter().map(|p| p.3).collect();
        let median = {
            let mut sorted = durs.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            sorted[sorted.len() / 2]
        };
        best = pairs.iter()
            .min_by(|a, b| {
                (a.3 - median).abs().partial_cmp(&(b.3 - median).abs()).unwrap()
            })
            .copied();
    }

    let (best_k, mut best_sw, best_valid, _) = best?;

    // If selected pair starts near t=0, prefer next valid pair
    if best_valid && words[best_sw].start < 0.3 && pairs.len() > 1 {
        if let Some(alt) = pairs.iter().find(|p| p.0 != best_k) {
            let (ak, asw, av, _) = *alt;
            let _ = (ak, av); // suppress unused warnings
            best_sw = asw;
        }
    }

    let start_sec = if best_valid {
        (words[best_sw].start - cfg.preroll_s).max(0.0)
    } else {
        (words[anchors[best_k]].start - cfg.preroll_s).max(0.0)
    };

    // Trim end boundary
    let end_sec = if best_k + 1 == anchors.len() - 1 && best_k >= 1 {
        words.last().unwrap().end
    } else {
        let (ew, ew_valid) = station_name_start(words, anchors[best_k + 1], Some(anchors[best_k]));
        if ew_valid && words[ew].start > words[anchors[best_k]].start {
            words[ew].start
        } else {
            words[anchors[best_k + 1]].start
        }
    };

    // FIX: if end lands on last word and an earlier complete loop exists, prefer it
    let last_word_end = words.last().unwrap().end;
    let mut final_start = start_sec;
    let mut final_end   = end_sec;

    if (end_sec - last_word_end).abs() < 0.5 && best_k >= 1 {
        for &(pk, psw, pv, _) in &pairs {
            if pk >= best_k { continue; }
            let (ew2, ewv2) = station_name_start(words, anchors[pk + 1], Some(anchors[pk]));
            let pair_end = if ewv2 { words[ew2].start } else { words[anchors[pk + 1]].start };
            if (pair_end - last_word_end).abs() > 1.0 {
                final_start = if pv {
                    (words[psw].start - cfg.preroll_s).max(0.0)
                } else {
                    (words[anchors[pk]].start - cfg.preroll_s).max(0.0)
                };
                final_end = pair_end;
                break;
            }
        }
    }

    if final_end - final_start < cfg.min_loop_s {
        return None;
    }

    // Guard: reject if station start word is an AWOS content word
    let sw_word = word_clean(&words[best_sw].word);
    if INVALID_STATION_WORDS.contains(&sw_word.as_str())
        && sw_word != "automated"
        && sw_word != "weather"
        && sw_word != "observation"
    {
        return None;
    }

    Some((final_start, final_end))
}
