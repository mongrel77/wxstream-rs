use crate::models::Station;

/// Build a station-specific prompt that primes Whisper with the exact
/// identifier and location name it should expect to hear in the audio.
/// Mirrors build_transcription_prompt() from transcribe.py exactly.
pub fn build_transcription_prompt(station: &Station) -> String {
    format!(
        "{id} {location} automated weather observation. \
        One four three niner zulu.\
        Wind: two seven zero at one five knots. Peak Gusts two three\
        Visibility: one zero. Visibility: two and one half.\
        Haze. Light Rain.\
        Sky condition: clear below one two thousand.\
        Sky condition: scattered niner hundred. Broken one thousand niner hundred. Overcast one one thousand.\
        Temperature: two two celcius. Dewpoint: one eight celcius.\
        Altimeter: two niner niner two inches of mercury.\
        Niner, two, niner, three, zero, one, zero, two, four, zero.\
        Knots, ceiling, dewpoint, altimeter, overcast, \
        broken, scattered, few, clear, calm, visibility, variable. \
        Remarks: Density Altitude one thousand one hundred.\
        Thunderstorm information not available.",
        id       = station.id,
        location = station.location,
    )
}

/// Fallback prompt when station metadata is unavailable.
pub fn generic_prompt() -> String {
    "Automated weather observation. \
    Wind: two seven zero at one five knots. \
    Visibility: one zero. \
    Sky condition: clear below one two thousand. \
    Temperature: two two celcius. Dewpoint: one eight celcius. \
    Altimeter: two niner niner two inches of mercury. \
    Knots, ceiling, dewpoint, altimeter, overcast, \
    broken, scattered, few, clear, calm, visibility, variable."
        .to_string()
}
