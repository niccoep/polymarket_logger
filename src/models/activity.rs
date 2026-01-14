use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserActivity {
    pub timestamp: i64, // TODO human readable format (time crate?)
    #[serde(rename = "proxyWallet")]
    pub proxy_wallet: String,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub size: f64,
    #[serde(rename = "usdcSize")]
    pub usdc_size: f64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    pub price: f64,
    pub asset: String,
    pub side: String,
    #[serde(rename = "outcomeIndex")]
    pub outcome_index: u32,
    pub title: String,
    #[serde(rename = "eventSlug")]
    pub event_slug: String,
    pub outcome: String,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserActivityRecord {
    pub timestamp: i64,
    pub proxy_wallet: String, // TODO is a string of hex values, can be compressed by storing as uint?
    pub condition_id: String, // TODO is a string of hex values, can be compressed by storing as uint?
    pub type_: u8,
    pub size: f64,
    pub usdc_size: f64,
    pub transaction_hash: String, // TODO uint storage of hash
    pub price: f64,
    pub asset: String,
    pub side: u8,
    pub outcome_index: u32,
    pub title: String,
    pub event_slug: String,
    pub outcome: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityType {
    Trade = 0,
}

impl ActivityType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "TRADE" => Some(ActivityType::Trade),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}


impl UserActivity {
    pub fn to_record(self) -> Option<UserActivityRecord> {
    let side: u8 = match self.side.as_str() {
        "BUY" => 0,
        "SELL" => 1,
        _ => 255, // for now 255 will be unknown type
    };

    Some(UserActivityRecord {
        timestamp: self.timestamp,
        proxy_wallet: self.proxy_wallet, //can be compressed by storing as uint?
        condition_id: self.condition_id, //can be compressed by storing as uint?
        type_: ActivityType::from_str(&self.type_)?,
        size: self.size,
        usdc_size: self.usdc_size,
        transaction_hash: self.transaction_hash, // TODO uint storage of hash
        price: self.price,
        asset: self.asset,
        side: side_u8,
        outcome_index: self.outcome_index,
        title: self.title,
        event_slug: self.event_slug,
        outcome: self.outcome,
    })

    }
}
