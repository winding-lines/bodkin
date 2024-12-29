use std::{error::Error, fmt};

#[allow(unused_imports)]
#[macro_use]
extern crate bodkin_derive;

#[doc(hidden)]
pub use bodkin_derive::*;

#[derive(Debug)]
pub struct BodkinError {
    message: String,
}

impl BodkinError {
    pub fn new(message: String) -> Self {
        BodkinError { message }
    }
}

impl Error for BodkinError {}

impl fmt::Display for BodkinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BodkinError{}", self.message)
    }
}
