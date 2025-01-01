use std::{error::Error, fmt};

#[allow(unused_imports)]
#[macro_use]
extern crate bodkin_derive;

#[doc(hidden)]
pub use bodkin_derive::*;

#[derive(Debug)]
pub enum BodkinError {
    ArrowError(arrow::error::ArrowError),
    MacroError(String),
}

impl BodkinError {
    pub fn new(message: String) -> Self {
        BodkinError::MacroError(message)
    }
}

impl Error for BodkinError {}

impl fmt::Display for BodkinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BodkinError::ArrowError(err) => write!(f, "ArrowError: {}", err),
            BodkinError::MacroError(message) => write!(f, "MacroError: {}", message),
        }
    }
}

impl From<arrow::error::ArrowError> for BodkinError {
    fn from(err: arrow::error::ArrowError) -> Self {
        BodkinError::ArrowError(err)
    }
}

/// `Result<T, BodkinError>`
///
/// A specialized `Result` type to be used by the code generated by the `ArrowIntegration` derive macro.
pub type Result<T, E = BodkinError> = core::result::Result<T, E>;
