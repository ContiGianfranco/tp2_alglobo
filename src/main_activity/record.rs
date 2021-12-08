use serde::Deserialize;

#[derive(Deserialize, Copy, Clone)]
pub struct Record{
    pub hotel: i32,
    pub airline: i32,
    pub bank: i32,
}
