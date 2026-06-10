
use std::collections::BTreeMap;


#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<i16, f32>,
    pub asks: BTreeMap<i16, f32>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}

//struct BookSnapshot {
//    timestamp: i64,
//    up: OrderBook,
//    down: OrderBook,
//}
//
