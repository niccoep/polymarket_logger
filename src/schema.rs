// Book level (one row per price level)
struct BookLevel {
    timestamp: i64,
    asset_binary: u8,    // 0 or 1
    side: u8,            // 0=bid, 1=ask (or enum)
    level: u32,          // 0=best, 1=second best, etc
    price: f64,
    size: f64,
}

// Price change
struct PriceChange {
    timestamp: i64,
    asset_binary: u8,
    price: f64,
    size: f64,
    side: u8,            // 0=BUY, 1=SELL
}

// Trade
struct Trade {
    timestamp: i64,
    asset_binary: u8,
    price: f64,
    size: f64,
    side: u8,            // 0=BUY, 1=SELL
    fee_rate_bps: f64,
}
struct UserActivity {
    timestamp: i64,
    proxy_wallet: String,
    condition_id: String,
    type_: u8,               // enum: TRADE, etc
    size: f64,
    usdc_size: f64,
    transaction_hash: String,
    price: f64,
    asset: String,
    side: u8,                // 0=BUY, 1=SELL
    outcome_index: u32,
    title: String,
    event_slug: String,
    outcome: String,
}
