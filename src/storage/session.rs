
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::time::{Duration, sleep, Instant};
use tokio::select;
use uuid::Uuid;

use crate::error::Result;
use crate::api::websocket::{WebSocketClient, MarketEvent};
use crate::storage::ParquetWriter;

#[derive(Debug)]
pub struct SessionStats {
    pub session_id: Uuid,
    pub duration: Duration,
    pub events_processed: u64,
    pub reconnects: u32,
}

pub struct Session {
    id: Uuid,
    asset_ids: Vec<String>,
    asset_binary_map: HashMap<String, u8>,
    output_dir: PathBuf,
    duration: Duration,
    max_retries: u32,
}

impl Session {
    pub async fn run(self) -> Result<SessionStats> {
        let mut retry_count = 0;

        loop {
            match self.run_once().await {
                Ok(stats) => return Ok(stats),
                Err(e) => {
                    retry_count += 1;

                    if retry_count >= self.max_retries {
                        eprintln!("Session {} failed after {} retries: {}", self.id, retry_count, e);
                        return Err(e);
                    }

                    eprintln!("Session {} failed (retry {}/{}): {}", 
                        self.id, retry_count, self.max_retries, e);

                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn run_once(&self) -> Result<SessionStats> {
        let session_start = Instant::now();
        let shutdown_timer = sleep(self.duration);
        tokio::pin!(shutdown_timer);

        let mut ws_client = WebSocketClient::new(self.asset_ids.clone());
        ws_client.connect_with_retry().await?;

        let filename = format!("session_{}_{}.parquet",
            self.id,
            chrono::Utc::now().format("%Y%m%d_%H%M%S"));
        let file_path = self.output_dir.join(filename);
        let mut writer = ParquetWriter::new(file_path, 1000)?;
        
        println!("Session {} started, will run for {:?}", self.id, self.duration);
        let mut events_processed = 0u64;
        let mut reconnect_count = 0u32;
        
        let result: Result<()> = loop {
            select! {
                event_result = ws_client.next_event(&self.asset_binary_map) => {
                    match event_result {
                        Ok(Some(events)) => {
                            for event in events {
                                match event {
                                    MarketEvent::Book(levels) |
                                    MarketEvent::PriceChange(levels) => {
                                        for level in levels {
                                            writer.add_row(level)?;
                                            events_processed += 1;
                                        }
                                    }
                                    MarketEvent::Trade(_trade) => {
                                        //TODO need to add trade logger to parquet
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            continue;
                        }
                        Err(e) => {
                            eprintln!("Session {} Websocket error: {}", self.id, e);

                            reconnect_count += 1;
                            match ws_client.connect_with_retry().await {
                                Ok(_) => {
                                    println!("Session {} reconnected successfully", self.id);
                                    continue;
                                }
                                Err(e) => {
                                    break Err(e);
                                }
                            }
                        }
                    }
                }
                _ = &mut shutdown_timer => {
                    println!("Session {} completed normaly after {:?}",
                        self.id, session_start.elapsed());
                    break Ok(());
                }
            }
        };

        println!("Session {} flushing and closing writer", self.id);

        if let Err(e) = writer.flush() {
            eprintln!("Session {} flush error: {}", self.id, e);
        }

        if let Err(e) = writer.close() {
            eprintln!("Session {} close error: {}", self.id, e);
        }

        result?;

        Ok(SessionStats{
            session_id: self.id,
            duration: session_start.elapsed(),
            events_processed,
            reconnects: reconnect_count,
        })
    }
}
