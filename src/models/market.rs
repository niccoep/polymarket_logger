use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    pub start_timestamp: i64,
    pub market_id: String,
    pub slug: String,
    pub active: bool,
    pub closed: bool,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: [String; 2],
    pub outcomes: [String; 2],
    #[serde(rename = "acceptingOrdersTimestamp")]
    pub accepting_orders_timestamp: String,
    #[serde(rename = "acceptingOrders")]
    pub accepting_orders: bool,
}

// Metadata stored in the header of each market's parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetadata {
    pub start_timestamp: i64,
    pub market_id: String,
    pub slug: String,
    pub asset_id_mapping: [String; 2], // maps 0/1 -> full asset_id
    pub outcome_label_mapping: [String; 2], // maps 0/1 -> outcome_label
}

impl MarketMetadata {
    pub fn from_market_info(info: MarketInfo) -> Self {
        MarketMetadata {
            start_timestamp: info.start_timestamp,
            market_id: info.market_id,
            slug: info.slug,
            asset_id_mapping: info.clob_token_ids, // maps 0/1 -> full asset_id
            outcome_label_mapping: info.outcomes, // maps 0/1 -> outcome_label
        }
    }
}
