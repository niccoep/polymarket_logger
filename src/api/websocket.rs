use crate::error::{LoggerError, Result};
use crate::models::events::{BookLevel, BookSide, Side, Trade};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde::de::Error;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration, Instant, interval, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream };

use std::any::type_name;

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
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    timestamp: String,
    price_changes: Vec<RawPriceChange>,
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
    hash: String,
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
    Book(Vec<BookLevel>),
    PriceChange(Vec<BookLevel>),
    Trade(Trade),
}

pub struct WebSocketClient {
    asset_ids: Vec<String>, 
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    reconnect_attempts: u32,
    max_reconnect_attempts: u32,

}

impl WebSocketClient {
    pub fn new(asset_ids: Vec<String>) -> Self {
        Self {
            asset_ids,
            ws_stream: None,
            reconnect_attempts: 0,
            max_reconnect_attempts: 10,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let (ws_stream, _) = connect_async(WSS_URL)
            .await
            .map_err(|e| LoggerError::WebsocketError(e.to_string()))?;

        self.ws_stream = Some(ws_stream);
        self.subscribe().await?;
        self.reconnect_attempts = 0;

        println!("Websocket connected successfully: {}", WSS_URL);

        Ok(())
    }

    pub async fn connect_with_retry(&mut self) -> Result<()> {
        loop {
            match self.connect().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    self.reconnect_attempts += 1;

                    if self.reconnect_attempts >= self.max_reconnect_attempts {
                        return Err(LoggerError::WebsocketError(
                            format!("Max reconnect attemps ({}) exceeded", self.max_reconnect_attempts)
                        ));
                    }

                    let backoff_secs = std::cmp::min(2u64.pow(self.reconnect_attempts), 60);
                    eprintln!(
                        "Connection failed (attempt {}/{}): {}. Retrying in {}s...",
                        self.reconnect_attempts,
                        self.max_reconnect_attempts,
                        e,
                        backoff_secs
                    );

                    sleep(Duration::from_secs(backoff_secs)).await;
                }
            }
        }
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
            .map_err(|e| LoggerError::WebsocketError(e.to_string()))?;
        }

        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        if let Some(ws) = &mut self.ws_stream {
            ws.send(Message::Text("PING".into()))
                .await
                .map_err(|e| LoggerError::WebsocketError(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn next_event(&mut self, asset_ids: &Vec<String>) -> Result<Option<Vec<MarketEvent>>> {
        if let Some(ws) = &mut self.ws_stream {
            if let Some(msg_result) = ws.next().await {
                let msg = msg_result.map_err(|e| LoggerError::WebsocketError(e.to_string()))?;
                
                match msg {
                    Message::Text(text) => {
                        if text == "PONG" {
                            self.send_ping().await?;
                            return Ok(None);
                        }
                        return Ok(Some(self.parse_message(&text, asset_ids)?));

                    }
                    Message::Close(_) => {
                        return Err(LoggerError::WebsocketError("Connection closed".to_string()));
                    }
                    _ => return Ok(None),
                }
            }
        }

        Ok(None)
    }

    fn parse_message(&self, text: &str, asset_ids: &Vec<String>) -> Result<Vec<MarketEvent>> {
        let value: serde_json::Value = serde_json::from_str(text)?;
        //println!("{}", text);
        //println!();

        //match &value {
        //    serde_json::Value::Null => println!("Null"),
        //    serde_json::Value::Bool(b) => println!("Bool: {}", b),
        //    serde_json::Value::Number(n) => println!("Number: {}", n),
        //    serde_json::Value::String(s) => println!("String: {}", s),
        //    serde_json::Value::Array(a) => println!("Array: {:?}", a),
        //    serde_json::Value::Object(o) => println!("Object: {:?}", o),
        //}
        let objects = match value {
            serde_json::Value::Object(_) => vec![value],
            serde_json::Value::Array(arr) => arr,
            _ => vec![],
        };

        let mut events = Vec::new();
        
        // for all order book updates and price changes, i.e. anything that changes something on
        // the order book, i want an entry for each price level
        for val in objects {
            let event = match val.get("event_type").and_then(|v| v.as_str()) {
                Some("book") => {
                    let event: BookEvent = serde_json::from_value(val.clone())?;
                    let book = self.parse_book_event(event, asset_ids)?;
                    Some(MarketEvent::Book(book))
                }
                Some("price_change") => {
                    let event: PriceChangeEvent = serde_json::from_value(val)?;
                    let price_changes = self.parse_price_change_event(event, asset_ids)?;
                    Some(MarketEvent::PriceChange(price_changes))
                }
                Some("last_trade_price") => {
                    let event: TradeEvent = serde_json::from_value(val)?;
                    let trade = self.parse_trade_event(event, asset_ids)?;
                    Some(MarketEvent::Trade(trade))
                }
                _ => None,
            };

            if let Some(e) = event {
                events.push(e);
            }
        }
         
        Ok(events)
    }

    fn parse_book_event(&self, event: BookEvent, asset_ids: &Vec<String>) -> Result<Vec<BookLevel>> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| LoggerError::JsonError(
                    serde_json::Error::custom(
                        format!("Invalid timestamp for {} timestamp: {:?}", type_name::<BookEvent>(), event.timestamp)
                    )
            ))?;

        let asset_binary = asset_ids.iter().position(|x| x == &event.asset_id)
            .ok_or(LoggerError::InvalidAssetId("Invalid asset_id for book event".to_string()))? as u8;
 
        let mut book = Vec::new();

        // parse bid
        for bid in event.bids {
            book.push(BookLevel {
                timestamp,
                asset_binary,
                side: BookSide::Bid.to_u8(),
                price_bps: (bid.price.parse::<f64>().unwrap_or(0.0) * 10_000.0) as i16,
                size: bid.size.parse().unwrap_or(0.0),
            });
        }

        // parse ask
        for  ask in event.asks {
            book.push(BookLevel {
                timestamp,
                asset_binary,
                side: BookSide::Ask.to_u8(),
                price_bps: (ask.price.parse::<f64>().unwrap_or(-1.0) * 10_000.0) as i16,
                size: ask.size.parse().unwrap_or(-1.0),
            });
        }

        Ok(book)
    }

    fn parse_price_change_event(&self, event: PriceChangeEvent, asset_ids: &Vec<String>) -> Result<Vec<BookLevel>> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| LoggerError::JsonError(
                    serde_json::Error::custom(
                        format!("Invalid timestamp for {} timestamp: {:?}", type_name::<PriceChangeEvent>(), event)
                    )
            ))?;
    
        let mut changes = Vec::new();

        for change in event.price_changes {
            let asset_binary = asset_ids.iter().position(|x| x == &change.asset_id)
                .ok_or(LoggerError::InvalidAssetId("Invalid asset_id for price change envent".to_string()))? as u8;

            let side = Side::from_str(&change.side)
                .ok_or_else(|| LoggerError::JsonError(serde_json::Error::custom("Invalid Side")))?;

            changes.push(BookLevel {
                timestamp,
                asset_binary,
                price_bps: (change.price.parse::<f64>().unwrap_or(-1.0) * 10_000.0) as i16,
                size: change.size.parse().unwrap_or(-1.0),
                side: side.to_u8(),
            });
        }
        Ok(changes)
    }

    fn parse_trade_event(&self, event: TradeEvent, asset_ids: &Vec<String>) -> Result<Trade> {
        let timestamp = event.timestamp.parse::<i64>()
            .map_err(|_| LoggerError::JsonError(serde_json::Error::custom("Invalid timestamp")))?;

        let asset_binary = asset_ids.iter().position(|x| x == &event.asset_id)
            .ok_or(LoggerError::InvalidAssetId("Invalid asset_id for trade event".to_string()))? as u8;

        let side = Side::from_str(&event.side)
            .ok_or_else(|| LoggerError::JsonError(serde_json::Error::custom("Invalid side")))?;

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
