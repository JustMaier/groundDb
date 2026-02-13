use crate::error::Result;
use super::types::SchemaDefinition;
use std::path::Path;

/// Parse a schema.yaml file into a SchemaDefinition
pub fn parse_schema(path: &Path) -> Result<SchemaDefinition> {
    let content = std::fs::read_to_string(path)?;
    parse_schema_str(&content)
}

/// Parse a schema YAML string into a SchemaDefinition
pub fn parse_schema_str(content: &str) -> Result<SchemaDefinition> {
    let schema: SchemaDefinition = serde_yaml::from_str(content)?;
    Ok(schema)
}
