use crate::error::{PolyError, Result};
use crate::models::market::MarketInfo;
use reqwest::Client;

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

pub struct GammaClient {
    client: Client,
    base_url: String,
}

impl GammaClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            base_url: GAMMA_API_BASE.to_string(),
        }
    }

    pub async fn get_market(&self, slug: &str) -> Result<MarketInfo> {
        let url = format!("{}/markets/slug/{}", self.base_url, slug);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(PolyError::MarketNotFound(slug.to_string()));
        }

        let market: MarketInfo = response.json::<MarketInfo>().await?;
        Ok(market)
    }
}

impl Default for GammaClient {
    fn default() -> Self {
        Self::new()
    }
}

//#[cfg(test)]
//mod tests {
//    use super::*;
//
//    #[tokio::test]
//    async fn test_get_mnarket() {
//        let client = GammaClient::new();
//        let result = client.get_market("bitcoin-up-or-down-january-12-10am-et").await;
//        assert!(result.is_ok());
//    }
//}
