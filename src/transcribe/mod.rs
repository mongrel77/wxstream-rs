pub mod prompt;
pub mod whisper;

pub use prompt::{build_transcription_prompt, generic_prompt};
pub use whisper::transcribe;
