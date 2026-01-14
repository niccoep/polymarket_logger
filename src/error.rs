use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum PolyError {
    HttpError(reqwest::Error),
    WebsocketError(String),
    JsonError(serde_json::Error),

    MarketNotFound(String),
    InvalidAssetId(String),
    IoError(std::io::Error),
    ParquetError(String),
    ConfigError(String),
}

impl fmt::Display for PolyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PolyError::HttpError(e) => 
                write!(f, "Http Error: {}", e),
            PolyError::WebsocketError(e) => 
                write!(f, "Websocket Error: {}", e),
            PolyError::JsonError(e) => 
                write!(f, "Json Error: {}", e),
            PolyError::MarketNotFound(m) => 
                write!(f, "Market not found: {}", m),
            PolyError::InvalidAssetId(id) => 
                write!(f, "InvalidAssetId: {}", id),
            PolyError::IoError(e) => 
                write!(f, "Io Error: {}", e),
            PolyError::ParquetError(e) => 
                write!(f, "Parquet Error: {}", e),
            PolyError::ConfigError(e) => 
                write!(f, "Config Error: {}", e),
        }
    }
}

impl Error for PolyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PolyError::HttpError(e) => Some(e),
            PolyError::JsonError(e) => Some(e),
            PolyError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for PolyError {
    fn from(err: reqwest::Error) -> Self {
        PolyError::HttpError(err)
    }
}

impl From<serde_json::Error> for PolyError {
    fn from(err: serde_json::Error) -> Self {
        PolyError::JsonError(err)
    }
}

impl From<std::io::Error> for PolyError {
    fn from(err: std::io::Error) -> Self {
        PolyError::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, PolyError>;
