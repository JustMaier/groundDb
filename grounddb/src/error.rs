use thiserror::Error;

#[derive(Error, Debug)]
pub enum GroundDbError {
    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Document not found: {collection}/{id}")]
    NotFound { collection: String, id: String },

    #[error("Collection not found: {name}")]
    CollectionNotFound { name: String },

    #[error("View not found: {name}")]
    ViewNotFound { name: String },

    #[error("Field validation failed for {collection}.{field}: {message}")]
    FieldValidation {
        collection: String,
        field: String,
        message: String,
    },

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_display_includes_collection_and_id() {
        let err = GroundDbError::NotFound {
            collection: "users".into(),
            id: "u_42".into(),
        };
        assert_eq!(err.to_string(), "Document not found: users/u_42");
    }

    #[test]
    fn collection_not_found_display_carries_name() {
        let err = GroundDbError::CollectionNotFound {
            name: "ghosts".into(),
        };
        assert_eq!(err.to_string(), "Collection not found: ghosts");
    }

    #[test]
    fn view_not_found_display_carries_name() {
        let err = GroundDbError::ViewNotFound {
            name: "by_status".into(),
        };
        assert_eq!(err.to_string(), "View not found: by_status");
    }

    #[test]
    fn field_validation_display_includes_context() {
        let err = GroundDbError::FieldValidation {
            collection: "users".into(),
            field: "email".into(),
            message: "must contain @".into(),
        };
        assert_eq!(
            err.to_string(),
            "Field validation failed for users.email: must contain @"
        );
    }

    #[test]
    fn typed_variants_allow_programmatic_introspection() {
        let err = GroundDbError::CollectionNotFound {
            name: "x".into(),
        };
        match err {
            GroundDbError::CollectionNotFound { name } => assert_eq!(name, "x"),
            _ => panic!("wrong variant"),
        }
    }
}
