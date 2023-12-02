use std::error::Error;

// Adds convenience for Result for brevity
pub type R<T> = Result<T, Box<dyn Error>>;
