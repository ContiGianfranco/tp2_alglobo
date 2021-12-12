use serde::Deserialize;

/// A struct made to represent each CSV entry
#[derive(Deserialize, Copy, Clone)]
pub struct Payment {
    pub line: usize,
    pub bank: i32,
    pub airline: i32,
    pub hotel: i32,
}
