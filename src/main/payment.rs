use serde::Deserialize;

/// A struct made to represent each CSV entry
#[derive(Deserialize)]
pub struct Payment {
    pub line: usize,
    pub bank_amount: usize,
    pub aero_amount: usize,
    pub hotel_amount: usize,
}
