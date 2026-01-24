use crate::error::{PolyError, Result};
use crate::models::events::{BookLevel, BookSide, PriceChange, Side, Trade};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde::de::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream };

const WSS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Serialize)]
struct Subscription {
    assets_ids: Vec<String>,
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
    timestamp: String,
    asset_id: String,
    transaction_hash: String,
    price: String,
    size: String,
    side: String,
    fee_rate_bps: String,
}

#[derive(Debug)]
pub enum MarketEvent {
    Book( Vec<( Vec<BookLevel>, Vec<BookLevel> )> ),
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
            assets_ids: self.asset_ids.clone(),
            r#type: "market".to_string(),
        };

        let msg = serde_json::to_string(&subscription)?;

        if let Some(ws) = &mut self.ws_stream {
            ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| PolyError::WebsocketError(e.to_string()))?;
        }

        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        if let Some(ws) = &mut self.ws_stream {
            ws.send(Message::Text("PING".into()))
                .await
                .map_err(|e| PolyError::WebsocketError(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn next_event(&mut self, asset_binary_map: &std::collections::HashMap<String, u8>) -> Result<Option<MarketEvent>> {
        if let Some(ws) = &mut self.ws_stream {
            if let Some(msg_result) = ws.next().await {
                let msg = msg_result.map_err(|e| PolyError::WebsocketError(e.to_string()))?;
                
                match msg {
                    Message::Text(text) => {
                        if text == "PONG" {
                            self.send_ping().await?;
                            return Ok(None);
                        }
                        return self.parse_message(&text, asset_binary_map);

                    }
                    Message::Close(_) => {
                        return Err(PolyError::WebsocketError("Connection closed".to_string()));
                    }
                    _ => return Ok(None),
                }
            }
        }

        Ok(None)
    }

    fn parse_message(&self, text: &str, asset_binary_map: &std::collections::HashMap<String, u8>) -> Result<Option<MarketEvent>> {
        let value: serde_json::Value = serde_json::from_str(text)?;

        match &value {
            serde_json::Value::Null => println!("Null"),
            serde_json::Value::Bool(b) => println!("Bool: {}", b),
            serde_json::Value::Number(n) => println!("Number: {}", n),
            serde_json::Value::String(s) => println!("String: {}", s),
            serde_json::Value::Array(a) => println!("Array: {:?}", a),
            serde_json::Value::Object(o) => println!("Object: {:?}", o),
        }

        let values_to_process = match value.as_array() {
            Some(array) => {
                println!("array found");
                if array.is_empty() { return Ok(None) };
                array.as_slice()
            }
            None => std::slice::from_ref(&value)
        };

        let event_type = values_to_process.get(0)
            .and_then(|v| v.get("event_type"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| PolyError::JsonError(serde_json::Error::custom("Missing event_type")))?;

        match event_type {
            "book" => {
                let mut all_levels: Vec<(Vec<BookLevel>, Vec<BookLevel>)> = Vec::new();
                for val in values_to_process {
                    println!("{}", values_to_process.len());
                    let event: BookEvent = serde_json::from_value(val.clone())?;
                    let book_levels = self.parse_book_event(event, asset_binary_map)?;
                    all_levels.push(book_levels);
                }
                Ok(Some(MarketEvent::Book(all_levels)))
            }
            "price_change" => {
                let event: PriceChangeEvent = serde_json::from_value(value)?;
                let price_changes = self.parse_price_change_event(event, asset_binary_map)?;
                Ok(Some(MarketEvent::PriceChange(price_changes)))
            }
            "last_trade_price" => {
                let event: TradeEvent = serde_json::from_value(value)?;
                let trade = self.parse_trade_event(event, asset_binary_map)?;
                Ok(Some(MarketEvent::Trade(trade)))
            }
            _ => Ok(None),
        }
    }

    fn parse_book_event(&self, event: BookEvent, asset_binary_map: &std::collections::HashMap<String, u8>) -> Result<(Vec<BookLevel>,Vec<BookLevel>)> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| PolyError::JsonError(serde_json::Error::custom("Invalid timestamp")))?;
        let asset_binary = *asset_binary_map.get(&event.asset_id)
            .ok_or(PolyError::InvalidAssetId("Invalid asset_id for book event".to_string()))?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // parse bid
        for (level, bid) in event.bids.iter().enumerate() {
            bids.push(BookLevel {
                timestamp,
                asset_binary,
                side: BookSide::Bid.to_u8(),
                level: level as u32,
                price_bps: (bid.price.parse::<f64>().unwrap_or(0.0) * 10_000.0) as i16,
                size: bid.size.parse().unwrap_or(0.0),
            });
        }

        // parse ask
        for (level, ask) in event.asks.iter().enumerate() {
            asks.push(BookLevel {
                timestamp,
                asset_binary,
                side: BookSide::Ask.to_u8(),
                level: level as u32,
                price_bps: (ask.price.parse::<f64>().unwrap_or(-1.0) * 10_000.0) as i16,
                size: ask.size.parse().unwrap_or(-1.0),
            });
        }

        Ok((bids,asks))
    }

    fn parse_price_change_event(&self, event: PriceChangeEvent, asset_binary_map: &std::collections::HashMap<String, u8>) -> Result<Vec<PriceChange>> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| PolyError::JsonError(serde_json::Error::custom("Invalid timestamp")))?;
    
        let mut changes = Vec::new();

        for change in event.price_changes {
            let asset_binary = *asset_binary_map.get(&change.asset_id)
                .ok_or(PolyError::InvalidAssetId("Invalid asset_id for price change envent".to_string()))?;

            let side = Side::from_str(&change.side)
                .ok_or_else(|| PolyError::JsonError(serde_json::Error::custom("Invalid Side")))?;

            changes.push(PriceChange {
                timestamp,
                asset_binary,
                price_bps: (change.price.parse::<f64>().unwrap_or(-1.0) * 10_000.0) as i16,
                size: change.size.parse().unwrap_or(-1.0),
                side: side.to_u8(),
            });
        }
        Ok(changes)
    }

    fn parse_trade_event(&self, event: TradeEvent, asset_binary_map: &std::collections::HashMap<String, u8>) -> Result<Trade> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| PolyError::JsonError(serde_json::Error::custom("Invalid timestamp")))?;

        let asset_binary = *asset_binary_map.get(&event.asset_id)
            .ok_or(PolyError::InvalidAssetId("Invalid asset_id for trade event".to_string()))?;

        let side = Side::from_str(&event.side)
            .ok_or_else(|| PolyError::JsonError(serde_json::Error::custom("Invalid side")))?;

        let transaction_hash = Trade::parse_hash_hex(&event.transaction_hash);

        Ok(Trade {
            timestamp,
            asset_binary,
            transaction_hash,
            price_bps: (event.price.parse().unwrap_or(-1.0) * 10_000.0) as i16,
            size: event.size.parse().unwrap_or(-1.0),
            side: side.to_u8(),
            fee_rate_bps: event.fee_rate_bps.parse().unwrap_or(-1),
        })
    }
}
