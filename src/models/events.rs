
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub timestamp: i64,     // unix time
    pub asset_binary: u8,   // 0 or 1
    pub side: u8,           // 0=bid 1=ask
    pub level: u32,         // 0=best, 1=second best, etc
    pub price_bps: i16,     // usdc price in basis points =  usdc price * 10000 (so mapping 0.0-1.0 to 0-10000)
    pub size: f64,          // number of contracts for 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChange {
    pub timestamp: i64,
    pub asset_binary: u8,
    pub price_bps: i16,     // usdc price in basis points =  usdc price * 10000 (so mapping 0.0-1.0 to 0-10000)
    pub size: f64,
    pub side: u8, //0=BUY 1=SELL
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub timestamp: i64,
    pub asset_binary: u8, 
    pub size: f64,
    pub side: u8,               //0=BUY 1=SELL
    pub free_rate_bps: i16,     // fee rate in basis points %
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
     Buy = 0,
     Sell = 1,
}

impl Side {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BUY" => Some(Side::Buy),
            "SELL" => Some(Side::Sell),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid = 0,
    Ask = 1,
}

impl BookSide {
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}
