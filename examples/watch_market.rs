use logger::api::{gamma::GammaClient, websocket::{WebSocketClient, MarketEvent}};
use logger::models::events::Trade;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let slug = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "No market provided".to_string());

    println!("Fetching marking info for slug: {}", slug);

    let gamma_client = GammaClient::new();
    let market_info = gamma_client.get_market(&slug).await?;

    println!("Connecting to WebSocket for market: {}", market_info.slug);
    println!("Asset IDs: {:?}", market_info.clob_token_ids);

    let mut asset_binary_map: HashMap<String, u8> = HashMap::new();
    for (idx, asset_id) in market_info.clob_token_ids.iter().enumerate() {
        if idx < 2 {
            asset_binary_map.insert(asset_id.clone(), idx as u8);
        }
    }

    let mut ws_client = WebSocketClient::new(market_info.clob_token_ids.clone());
    ws_client.connect().await?;

    println!("Connected, listening for events...\n");

    loop {
        match ws_client.next_event(&asset_binary_map).await? {
            Some(event) => match event {
                MarketEvent::Book((bids,asks)) => {
                    //println!("Book update:");
                    let mut bids_0 = Vec::new();
                    let mut asks_0 = Vec::new();
                    let mut bids_1 = Vec::new();
                    let mut asks_1 = Vec::new();

                    for level in levels {
                         match (level.asset_binary, level.side) {
                             (0, 0) => bids_0.push(level),
                             (0, 1) => asks_0.push(level),
                             (1, 0) => bids_1.push(level),
                             (1, 1) => asks_1.push(level),
                             _ => {}
                         }
                    }
                    // if we have a any bids or asks 
                    println!("Asset 0 (▲):");
                    println!("\tBids: {:?}", bids_0
                        .iter()
                        .take(3)
                        .map(|l| (l.price_bps as f64 / 10_000.0, l.size, l.side))
                        .collect::<Vec<_>>()
                    );
                    println!("\tAsks: {:?}", asks_0
                        .iter()
                        .take(3)
                        .map(|l| (l.price_bps as f64 / 10_000.0, l.size, l.side))
                        .collect::<Vec<_>>()
                    );
                    println!("Asset 1 (▼):");
                    println!("\tBids: {:?}", bids_1
                        .iter()
                        .take(3)
                        .map(|l| (l.price_bps as f64 / 10_000.0, l.size, l.side))
                        .collect::<Vec<_>>()
                    );
                    println!("\tAsks: {:?}", asks_1
                        .iter()
                        .take(3)
                        .map(|l| (l.price_bps as f64 / 10_000.0, l.size, l.side))
                        .collect::<Vec<_>>()
                    );
                    println!();
                }

                MarketEvent::PriceChange(changes) => {
                    //println!("Price change:");
                    //for change in changes {
                    //    let symbol = if change.asset_binary == 0 { "▲" } else { "▼" };
                    //    let side = if change.side == 0 { "BUY " } else { "SELL" };
                    //    println!("\t{} {} ${} {}", symbol, side, (change.price_bps as f64 / 10_000.0), change.size);
                    //}
                    //println!();
                }
                MarketEvent::Trade(trade) => {
                    let symbol = if trade.asset_binary == 0 { "▲" } else { "▼" };
                    let side = if trade.side == 0 { "BUY " } else { "SELL" };
                    println!("\t{} {:<4} ${:<5} {:<8.2} {:08x}", symbol, side, (trade.price_bps as f64 / 10_000.0), trade.size, (trade.transaction_hash >> 96) as u32);
                }
            }
            None =>  {
            }
        }
    }
}
