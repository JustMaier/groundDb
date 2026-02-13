use thiserror::Error;

#[derive(Error, Debug)]
pub enum GroundDbError {
    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Document not found: {collection}/{id}")]
    NotFound { collection: String, id: String },

    #[error("Path conflict: {path}")]
    PathConflict { path: String },

    #[error("Referential integrity violation: {0}")]
    ReferentialIntegrity(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, GroundDbError>;
