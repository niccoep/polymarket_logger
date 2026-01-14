use crate::error::{PolyError, Result};
use crate::models::events::{BookLevel, BookSide, PriceChange, Side, Trade};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

const WSS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Serialize)]
struct Subscription {
    asset_ids: Vec<String>,
    r#type: String,
}

#[derive(Debug, Deserialize)]
struct RawPriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    best_bid: String,
    best_ask: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    price_changes: Vec<RawPriceChange>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct RawBookLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct BookEvent {
    asset_id: String,
    market: String,
    bids: Vec<RawBookLevel>,
    asks: Vec<RawBookLevel>,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct TradeEvent {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    fee_rate_bps: String,
    timestamp: String,
}

#[derive(Debug)]
pub enum MarketEvent {
    Book(Vec<BookLevel>),
    PriceChange(Vec<PriceChange>),
    Trade(Trade),
}

pub struct WebSocketClient {
    asset_ids: Vec<String>,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,

}

impl WebSocketClient {
    pub fn new(asset_ids: Vec<String>) -> Self {
        Self {
            asset_ids,
            ws_stream: None,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let (ws_stream, _) = connect_async(WSS_URL)
            .await
            .map_err(|e| PolyError::WebsocketError(e.to_string()))?;

        self.ws_stream = Some(ws_stream);

        self.subscribe().await?;

        Ok(())
    }

    async fn subscribe(&mut self) -> Result<()> {
        let subscription = Subscription {
            asset_ids: self.asset_ids.clone(),
            r#type: "market".to_string(),
        };

        let msg = serde_json::to_string(&subscription)?;

        if let Some(ws) = &mut self.ws_stream {
            ws.send(Message::Text(msg))
            .await
            .map_err(|e| PolyError::WebsocketError(e.to_string()))?;
        }
    }
}
