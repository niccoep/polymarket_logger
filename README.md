# polymarket-logger

A Rust tool for streaming and logging real-time order book data from [Polymarket](https://polymarket.com) prediction markets to Parquet files, focused on crypto price-prediction markets (BTC, ETH, SOL, XRP).

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
cargo run
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

- `tokio` — async runtime
- `tokio-tungstenite` — WebSocket client
- `arrow` / `parquet` — columnar storage
- `reqwest` — Gamma REST API calls
- `chrono` / `chrono-tz` — market slug generation (ET timezone)
