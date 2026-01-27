
use std::path::PathBuf;
use logger::storage::SessionManager;
use logger::error::Result;

#[tokio::main]
async fn main() -> Result<()>{
    let base_dir = PathBuf::from("./market_data");
    let manager = SessionManager::new(base_dir);

    manager.run().await?;

    Ok(())
}
