
#[derive(Debug)]
pub enum ReplayError {
    Polars(polars::error::PolarsError),
    Glob(glob::PatternError),
    IO(std::io::Error),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ReplayError::Polars(e) => write!(f, "Polars error: {}", e),
            ReplayError::Glob(e) => write!(f, "Glob error: {}", e),
            ReplayError::IO(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for ReplayError {}

impl std::convert::From<polars::error::PolarsError> for ReplayError {
    fn from(err: polars::error::PolarsError) -> Self {
        ReplayError::Polars(err)
    }
}

impl std::convert::From<glob::PatternError> for ReplayError {
    fn from(err: glob::PatternError) -> Self {
        ReplayError::Glob(err)
    }
}

impl std::convert::From<std::io::Error> for ReplayError {
    fn from(err: std::io::Error) -> Self {
        ReplayError::IO(err)
    }
}

pub type Result<T> = std::result::Result<T,ReplayError>;
