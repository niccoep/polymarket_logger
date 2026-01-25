use std::fmt;
use std::error::Error;
use parquet::errors::ParquetError;
use arrow::error::ArrowError;

#[derive(Debug)]
pub enum LoggerError {
    // Polymarket
    HttpError(reqwest::Error),
    WebsocketError(String),
    JsonError(serde_json::Error),
    MarketNotFound(String),
    InvalidAssetId(String),
    IoError(std::io::Error),
    ParquetError(String),
    ConfigError(String),

    // parquet
    Parquet(ParquetError),
    Arrow(ArrowError),
    Config(String),
    Session(String),

}

impl fmt::Display for LoggerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LoggerError::HttpError(e) => 
                write!(f, "Http Error: {}", e),
            LoggerError::WebsocketError(e) => 
                write!(f, "Websocket Error: {}", e),
            LoggerError::JsonError(e) => 
                write!(f, "Json Error: {}", e),
            LoggerError::MarketNotFound(m) => 
                write!(f, "Market not found: {}", m),
            LoggerError::InvalidAssetId(id) => 
                write!(f, "InvalidAssetId: {}", id),
            LoggerError::IoError(e) => 
                write!(f, "Io Error: {}", e),
            LoggerError::ParquetError(e) => 
                write!(f, "Parquet Error: {}", e),
            LoggerError::ConfigError(e) => 
                write!(f, "Config Error: {}", e),
            LoggerError::Parquet(e) => 
                write!(f, "Parquet Error: {}", e),
            LoggerError::Arrow(e) => 
                write!(f, "Arrow Error: {}", e),
            LoggerError::Config(e) => 
                write!(f, "Parquet Config Error: {}", e),
            LoggerError::Session(e) => 
                write!(f, "Session Error: {}", e),
        }
    }
}

impl Error for LoggerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LoggerError::HttpError(e) => Some(e),
            LoggerError::JsonError(e) => Some(e),
            LoggerError::IoError(e) => Some(e),
            LoggerError::Parquet(e) => Some(e),
            LoggerError::Arrow(e) => Some(e),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for LoggerError {
    fn from(err: reqwest::Error) -> Self {
        LoggerError::HttpError(err)
    }
}

impl From<serde_json::Error> for LoggerError {
    fn from(err: serde_json::Error) -> Self {
        LoggerError::JsonError(err)
    }
}

impl From<std::io::Error> for LoggerError {
    fn from(err: std::io::Error) -> Self {
        LoggerError::IoError(err)
    }
}

impl From<ParquetError> for LoggerError {
    fn from(err: ParquetError) -> Self {
        LoggerError::Parquet(err)
    }
}

impl From<ArrowError> for LoggerError {
    fn from(err: ArrowError) -> Self {
        LoggerError::Arrow(err)
    }
}

pub type Result<T> = std::result::Result<T, LoggerError>;
