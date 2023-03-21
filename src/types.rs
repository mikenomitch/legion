use std::error;
// Change the alias to `Box<error::Error>`.
pub type Res<T> = std::result::Result<T, Box<dyn error::Error>>;
