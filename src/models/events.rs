
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub timestamp: i64,     // unix time
    pub asset_binary: u8,   // 0 or 1
    pub side: u8,           // 0=bid 1=ask
    pub price_bps: i16,     // usdc price in basis points =  usdc price * 10000 (so mapping 0.0-1.0 to 0-10000)
    pub size: f32,          // number of contracts for 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub timestamp: i64,         // unix time
    pub asset_binary: u8,       //
    pub transaction_hash: u128, //
    pub price_bps: i16,         // prce in basis points (price*10_000) as i16
    pub size: f64,              // 
    pub side: u8,               // 0=BUY 1=SELL
    pub fee_rate_bps: i16,      // fee rate in basis points %
}

impl Trade {

    // just store the first 128B of the hash (largest native primitive is u128 and i dont want to
    // make another type for this)
    pub fn parse_hash_hex(hash_str: &str) -> u128 {
        let hash_hex = hash_str.strip_prefix("0x").unwrap_or(hash_str);
        u128::from_str_radix(&hash_hex[..32], 16).unwrap_or(0)
    }
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
