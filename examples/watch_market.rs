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
            Some(events) => 
                for event in events {
                    match event {
                        //pub timestamp: i64,     // unix time
                        //pub asset_binary: u8,   // 0 or 1
                        //pub side: u8,           // 0=bid 1=ask
                        //pub price_bps: i16,     // usdc price in basis points =  usdc price * 10000 (so mapping 0.0-1.0 to 0-10000)
                        //pub size: f32,          // number of contracts for 
                        MarketEvent::Book(book_snapshot) => {
                            //for book_level in book_snapshot {
                            //    match book_level.asset_binary {
                            //        0 => {
                            //            println!("▲");
                            //        }
                            //        1 => {
                            //            println!("▼");
                            //        }
                            //        _ => {}
                            //    }
                            //    //println!("\t: {:?}", book_level);
                            //    println!("\tbook update");
                            //}
                            println!("\tbook update {}", book_snapshot[0].timestamp);
                            println!();
                        }

                    MarketEvent::PriceChange(changes) => {
                        //println!("Price change:");
                        for change in changes {
                            println!("{:?}", change);
                        }
                        println!();
                    }
                    MarketEvent::Trade(trade) => {
                        let symbol = if trade.asset_binary == 0 { "▲" } else { "▼" };
                        let side = if trade.side == 0 { "BUY " } else { "SELL" };
                        println!("\t{} {:<4} ${:<5} {:<8.2} {:08x} {}", symbol, side, (trade.price_bps as f64 / 10_000.0), trade.size, (trade.transaction_hash >> 96) as u32, trade.timestamp);
                    }
                }
            }
            None =>  {
            }
        }
    }
}
