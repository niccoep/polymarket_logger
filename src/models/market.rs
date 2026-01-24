use serde::{Deserialize, Serialize};
use serde::de::Deserializer;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    pub id: String,
    pub slug: String,
    //pub active: bool,
    //pub closed: bool,
    #[serde(rename = "clobTokenIds", deserialize_with = "deserialize_string_vec")]
    pub clob_token_ids: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_vec")]
    pub outcomes: Vec<String>,
    #[serde(rename = "acceptingOrdersTimestamp")]
    pub accepting_orders_timestamp: String,
    #[serde(rename = "acceptingOrders")]
    pub accepting_orders: bool,
}

fn deserialize_string_vec<'de, D, T> (deserializer: D) -> Result<Vec<T>, D::Error> 
where 
    D: Deserializer<'de>,
    T: for<'a> Deserialize<'a>,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

// Metadata stored in the header of each market's parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMetadata {
    //pub start_timestamp: i64,
    pub market_id: String,
    pub slug: String,
    pub asset_id_mapping: Vec<String>, // maps 0/1 -> full asset_id
    pub outcome_label_mapping: Vec<String>, // maps 0/1 -> outcome_label
}

impl MarketMetadata {
    pub fn from_market_info(info: MarketInfo) -> Self {
        MarketMetadata {
            //start_timestamp: info.start_timestamp,
            market_id: info.id,
            slug: info.slug,
            asset_id_mapping: info.clob_token_ids, // maps 0/1 -> full asset_id
            outcome_label_mapping: info.outcomes, // maps 0/1 -> outcome_label
        }
    }
}
