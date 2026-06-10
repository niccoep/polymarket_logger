use polars::prelude::*;
use super::orderbook::OrderBook; 
use crate::Result;
use std::path::PathBuf;

pub struct BookReplay {
    timestamps: Vec<i64>,
    asset_ids: Vec<u8>,
    sides: Vec<u8>,
    prices: Vec<i16>,
    sizes: Vec<f32>,
}

impl BookReplay {
    pub fn from_parquet(parquet_path: &PathBuf) -> Result<Self> {
        let df = LazyFrame::scan_parquet(parquet_path, Default::default())?
            //.sort("timestamp", Default::default()) // don't think i need this as timestamps should be sorted (i guess not 100% guaranteed so might need it but have to check)
            .collect()?;

        let timestamps: Vec<i64> = df.column("timestamp")?.i64()?.to_vec().into_iter().map(|x| x.unwrap()).collect();
        let asset_ids: Vec<u8> = df.column("asset_id")?.u8()?.to_vec().into_iter().map(|x| x.unwrap()).collect();
        let sides: Vec<u8> = df.column("side")?.u8()?.to_vec().into_iter().map(|x| x.unwrap()).collect();
        let prices: Vec<i16> = df.column("price_bps")?.i16()?.to_vec().into_iter().map(|x| x.unwrap()).collect();
        let sizes: Vec<f32> =  df.column("size")?.f32()?.to_vec().into_iter().map(|x| x.unwrap()).collect();

        Ok(Self {
            timestamps,
            asset_ids,
            sides,
            prices,
            sizes,
        })
    }

    pub fn replay<F>(&self, mut strategy: F)
    where
        F: FnMut(i64, &[OrderBook; 2]) -> (),
    {
        let mut books = [OrderBook::new(), OrderBook::new()];
        let mut cur_ts: Option<i64> = None;

        for i in 0..self.timestamps.len() {
            let timestamp = self.timestamps[i];
            let asset_id = self.asset_ids[i];
            let side = self.sides[i];
            let price = self.prices[i];
            let size = self.sizes[i];

            // Process all updates for current timestamp, then call strategy when timestamp changes
            if cur_ts.is_some() && cur_ts != Some(timestamp) {
                strategy(cur_ts.unwrap(), &books);
            }
            cur_ts = Some(timestamp);

            let book = if side == 0 {
                &mut books[asset_id as usize].bids
            } else {
                &mut books[asset_id as usize].asks
            };

            if size == 0.0 {
                book.remove(&price);
            } else {
                book.insert(price, size);
            }
        }


        if let Some(ts) = cur_ts {
            strategy(ts, &books);
        }
    }
}
