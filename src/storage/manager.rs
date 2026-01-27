
use std::path::PathBuf;
use chrono::{DateTime, Utc, Timelike, Datelike};
use chrono_tz::America::New_York;
use tokio::time::{Duration, interval_at, Instant};
use tokio::task::JoinHandle;
use crate::storage::session::Session;
use crate::api::gamma::GammaClient;
use crate::error::Result;


#[derive(Debug, Clone)]
pub enum MarketInterval {
    FifteenMin,
    Hour,
    FourHour,
}

impl MarketInterval {
    pub fn all() -> Vec<MarketInterval> {
        vec![
            MarketInterval::FifteenMin,
            MarketInterval::Hour,
            MarketInterval::FourHour,
        ]
    }

    fn duration(&self) -> Duration {
        match self {
            MarketInterval::FifteenMin => Duration::from_mins(16),
            MarketInterval::Hour => Duration::from_hours(1),
            MarketInterval::FourHour => Duration::from_hours(4),
        }
    }

    fn interval_mins(&self) -> i64 {
        match self {
            MarketInterval::FifteenMin => 15,
            MarketInterval::Hour => 60,
            MarketInterval::FourHour => 240,
        }
    }
    fn interval_secs(&self) -> i64 {
        match self {
            MarketInterval::FifteenMin => 15 * 60,
            MarketInterval::Hour => 60 * 60,
            MarketInterval::FourHour => 4 * 60 * 60,
        }
    }
}

#[derive(Debug,Clone)]
pub enum Coin {
    Bitcoin,
    Ethereum,
    Solana,
    XRP,
}

impl Coin {
    pub fn all() -> Vec<Coin> {
        vec![
            Coin::Bitcoin,
            Coin::Ethereum,
            Coin::Solana,
            Coin::XRP,
        ]
    }

    pub fn as_str_long(&self) -> &str {
        match self {
            Coin::Bitcoin => "bitcoin",
            Coin::Ethereum => "ethereum",
            Coin::Solana => "solana",
            Coin::XRP => "xrp",
        }
    }
    pub fn as_str_short(&self) -> &str {
        match self {
            Coin::Bitcoin => "btc",
            Coin::Ethereum => "eth",
            Coin::Solana => "sol",
            Coin::XRP => "xrp",
        }
    }
}

pub struct SessionManager {
    gamma_client: GammaClient,
    base_dir: PathBuf,
}


impl SessionManager {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            gamma_client: GammaClient::new(),
            base_dir,
        }
    }

    fn generate_slug(coin: &Coin, interval: &MarketInterval, start_time: DateTime<Utc>) -> String {
        match interval {
            MarketInterval::FifteenMin => {
                format!("{}-updown-15m-{}",coin.as_str_short(), start_time.timestamp())
            }   
            MarketInterval::Hour => {
                // ex:
                // bitcoin-up-or-down-january-27-12am-et
                let dt_et = start_time.with_timezone(&New_York);
                let month = dt_et.format("%B").to_string().to_lowercase();
                let day = dt_et.format("%d").to_string();
                let hour = dt_et.format("%I").to_string().trim_start_matches('0').to_string();
                let ampm = dt_et.format("%p").to_string().to_lowercase();

                format!("{}-up-or-down-{}-{}-{}{}-et", coin.as_str_long(), month, day, hour, ampm)
            }
            MarketInterval::FourHour => {
                // ex:
                // sol-updown-4h-1769490000
                
                // for some reason polymarker is using 1 hour ahead (i.e. 12am is 1am) idk why
                let adjusted_time = start_time + chrono::Duration::hours(1);
                format!("{}-updown-4h-{}",coin.as_str_short(), adjusted_time.timestamp())
            }
        }
    }

    async fn spawn_session(
        &self, 
        coin: &Coin, 
        interval: &MarketInterval, 
        start_time: DateTime<Utc>
    ) -> Result<JoinHandle<Result<()>>> {
        let slug = Self::generate_slug(coin, interval, start_time);
        let dt_et = start_time.with_timezone(&New_York);

        println!(
            "Spawning session for: {} (ET: {})",
            slug,
            dt_et.format("%Y-%m-%d %H:%M:%S %Z"),
        );

        let market_info = self.gamma_client.get_market(&slug).await?;

        let output_dir = self.base_dir
            .join(coin.as_str_short())
            .join(match interval {
                MarketInterval::FifteenMin => "15min",
                MarketInterval::Hour => "hour",
                MarketInterval::FourHour => "4hour",
            })
        .join(start_time.timestamp().to_string());

        std::fs::create_dir_all(&output_dir)?;
        
        let session_duration = interval.duration();

        let handle = tokio::spawn(async move {
            let session = Session::new(
                market_info.clob_token_ids,
                output_dir,
                session_duration,
            );
            session.run().await?;
            Ok(())
        });

        Ok(handle)
    }

    fn next_aligned_time(interval: &MarketInterval) -> DateTime<Utc> {
        let now = Utc::now();
        let now_ts = now.timestamp();
        let interval_secs = interval.interval_secs();

        let next_ts = ((now_ts / interval_secs) + 1) * interval_secs;
        DateTime::from_timestamp(next_ts, 0).unwrap()
    }

    async fn run_scheduler_for_coin(&self, coin: Coin, interval: MarketInterval) -> Result<()> {
        let next_time = Self::next_aligned_time(&interval);
        let now = Utc::now();
        let now_ts = now.timestamp();
        let interval_secs = interval.interval_secs();

        let cur_aligned_ts = (now_ts / interval_secs) * interval_secs;
        let cur_aligned_time = DateTime::from_timestamp(cur_aligned_ts, 0).unwrap();

        let next_aligned_time = Self::next_aligned_time(&interval); 
        let time_until_next = (next_aligned_time.timestamp() - now_ts) as u64;

        let cur_time_et = cur_aligned_time.with_timezone(&New_York);
        let next_time_et = next_time.with_timezone(&New_York);

        println!(
            "Starting {} {:?} - spawning current market (started at {}), next at {} in {} seconds",
            coin.as_str_short(),
            interval,
            cur_time_et.format("%Y-%m-%d %H:%M:%S %Z"),
            next_time_et.format("%Y-%m-%d %H:%M:%S %Z"),
            time_until_next
        );

        // Spawn session for current market immediately
        match self.spawn_session(&coin, &interval, cur_aligned_time).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to spawn current session for {} {:?}: {:?}", coin.as_str_short(), interval, e);
            }
        }


        let start_instant = Instant::now() + Duration::from_secs(time_until_next);
        let mut ticker = interval_at(
            start_instant,
            interval.duration(),
        );

        loop {
            ticker.tick().await;
            let cur_time = Utc::now();

            let tick_ts = cur_time.timestamp();
            let aligned_ts = (tick_ts / interval_secs) * interval_secs;
            let aligned_time = DateTime::from_timestamp(aligned_ts, 0).unwrap();
            match self.spawn_session(&coin, &interval, aligned_time).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Failed to spawn session for {} {:?} {:?}", coin.as_str_short(), interval, e)
                }
            }
        }
    }

    pub async fn run(self) -> Result<()> {

        let mut handles = vec![];

        for coin in Coin::all() {
            for interval in MarketInterval::all() {
                let manager = SessionManager {
                    gamma_client: GammaClient::new(),
                    base_dir: self.base_dir.clone(),
                };

                let interval = interval.clone();
                let coin = coin.clone();

                let handle = tokio::spawn(async move {
                    manager.run_scheduler_for_coin(coin, interval).await
                });

                handles.push(handle);
            }
        }

        for handle in handles {
            let _  = handle.await;
        }

        Ok(())
    }
}
