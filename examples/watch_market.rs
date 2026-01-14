use logger::api::{gamma::GammaClient, websocket::WebsocketClient};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let slug = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "idk_test".to_string());

    println!("Fetching marking infor for slug: {}", slug);

    let gamma_cluent = GammaClient::new();
    let market_info = gamma_client.get_market(&slug).await?;
}
