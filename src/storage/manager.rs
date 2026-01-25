
use std::path::PathBuf;
use std::collections::HashMap;
use crate::storage::session::{SessionStats};
use crate::error::Result;
use uuid::Uuid;

pub struct SessionManager {
    output_dir: PathBuf,
    active_sessions: HashMap<Uuid, tokio::task::JoinHandle<Result<SessionStats>>>,
}
