// Document I/O - read/write markdown with YAML front matter

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A loaded document with implicit fields and typed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document<T> {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub data: T,
    pub content: Option<String>,
}
