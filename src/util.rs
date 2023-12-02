use std::error::Error;

// Define the type alias here
pub type R<T> = Result<T, Box<dyn Error>>;
