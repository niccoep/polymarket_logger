use crate::error::Result;
use crate::models::UserActivity;
use reqwest::Client;
use serde::{Deserialize, Serialize};

const DATA_API_URL: &str = "https://data-api.polymarket.com";

