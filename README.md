# polymarket-logger

A Rust workspace for streaming, logging, and backtesting real-time order book data from [Polymarket](https://polymarket.com) prediction markets, focused on crypto price-prediction markets (BTC, ETH, SOL, XRP).

**Crates:**
- `logger` — streams the Polymarket CLOB WebSocket feed and writes order book data to Parquet files
- `backtest` — loads those Parquet files and replays the order book so you can run strategies over the recorded data

## Overview

The logger subscribes to Polymarket's CLOB WebSocket feed and records every order book snapshot and price change event as structured rows in compressed Parquet files. It is designed to run continuously, automatically discovering the current active market for each coin/interval combination and rolling to the next one when a market expires.

**What gets logged:**

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | `i64` | Unix timestamp (ms) |
| `asset_id` | `u8` | `0` = YES token, `1` = NO token |
| `side` | `u8` | `0` = bid, `1` = ask |
| `price_bps` | `i16` | Price in basis points (e.g. `6500` = $0.65) |
| `size` | `f32` | Number of contracts |

**Output structure:**

```
market_data/
  btc/
    15min/<unix_start_ts>/
      <session_uuid>_<datetime>.parquet
    hour/<unix_start_ts>/
      ...
    4hour/<unix_start_ts>/
      ...
  eth/...
  sol/...
  xrp/...
```

## Usage

### Run the logger

```bash
cargo run -p logger
```

This spawns one scheduler per coin × interval combination (12 total). Each scheduler:
1. Immediately starts logging the currently-active market
2. Waits until the next aligned interval boundary
3. Rolls over to the new market and repeats indefinitely

Sessions auto-reconnect on WebSocket errors (exponential backoff, up to 10 retries).

### Watch a single market live (example)

Stream events for any market to stdout by slug:

```bash
cargo run --example watch_market -- bitcoin-up-or-down-january-27-12am-et
```

This prints order book snapshots, price changes, and trades as they arrive:

```
Fetching market info for slug: bitcoin-up-or-down-january-27-12am-et
Connecting to WebSocket for market: bitcoin-up-or-down-january-27-12am-et
Asset IDs: ["0xabc...", "0xdef..."]
Connected, listening for events...

    book update 1706313600
▲ BUY  $0.65  120.50   1a2b3c4d 1706313612
BookLevel { timestamp: 1706313614, asset_binary: 0, side: 1, price_bps: 6500, size: 50.0 }
```

### Use as a library

```rust
use logger::api::gamma::GammaClient;
use logger::api::websocket::{WebSocketClient, MarketEvent};
use std::path::PathBuf;
use tokio::time::Duration;

// Look up a market by slug
let client = GammaClient::new();
let market = client.get_market("bitcoin-up-or-down-january-27-12am-et").await?;

// Stream events from it
let mut ws = WebSocketClient::new(market.clob_token_ids.clone());
ws.connect_with_retry().await?;

loop {
    if let Some(events) = ws.next_event(&market.clob_token_ids).await? {
        for event in events {
            match event {
                MarketEvent::Book(levels) => { /* full order book snapshot */ }
                MarketEvent::PriceChange(levels) => { /* incremental updates */ }
                MarketEvent::Trade(trade) => { /* filled trade */ }
            }
        }
    }
}
```

### Run the full session manager programmatically

```rust
use logger::storage::SessionManager;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let manager = SessionManager::new(PathBuf::from("./market_data"));
    manager.run().await.unwrap();
}
```

## Backtesting

The `backtest` crate loads the logged Parquet files and replays the order book tick-by-tick. Your strategy receives a callback at each timestamp with the full reconstructed book state for both the YES and NO tokens.

### Run the backtester

```bash
cargo run -p backtest
```

By default this globs `./market_data/btc/*/**/*.parquet` and runs files in parallel using Rayon.

### Write a strategy

Implement the `BookProcessor` trait:

```rust
use backtest::{BookProcessor, reconstruction::OrderBook};

struct MyStrategy {
    trades: Vec<(i64, f32)>,
}

impl BookProcessor for MyStrategy {
    type Output = Vec<(i64, f32)>;

    fn new() -> Self { Self { trades: vec![] } }

    fn process(&mut self, timestamp: i64, books: &[OrderBook; 2]) {
        // books[0] = YES token, books[1] = NO token
        // books[i].bids / books[i].asks are BTreeMap<i16, f32> (price_bps → size)
        if let Some((&best_bid_bps, _)) = books[0].bids.iter().next_back() {
            self.trades.push((timestamp, best_bid_bps as f32 / 10_000.0));
        }
    }

    fn finalize(self) -> Self::Output { self.trades }
}
```

Then replay a file:

```rust
use backtest::reconstruction::BookReplay;

let replay = BookReplay::from_parquet(&path)?;
let mut strategy = MyStrategy::new();
replay.replay(|ts, books| strategy.process(ts, books));
let results = strategy.finalize();
```

## Reading the Parquet files

The files are ZSTD-compressed Parquet (level 4). Any Parquet-compatible tool works — for example with Python/pandas:

```python
import pandas as pd

df = pd.read_parquet("market_data/btc/15min/1706313600/session_uuid_20240127_120000.parquet")
df["price"] = df["price_bps"] / 10_000          # convert bps → USDC price
df["side_label"] = df["side"].map({0: "bid", 1: "ask"})
print(df.head())
```

## Dependencies

**logger:** `tokio`, `tokio-tungstenite`, `arrow`, `parquet`, `reqwest`, `chrono` / `chrono-tz`

**backtest:** `polars` (lazy parquet reader), `rayon` (parallel file processing), `glob`
