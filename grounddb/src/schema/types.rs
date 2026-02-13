use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level schema definition parsed from schema.yaml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    #[serde(default)]
    pub types: HashMap<String, HashMap<String, FieldDefinition>>,
    #[serde(default)]
    pub collections: HashMap<String, CollectionDefinition>,
    #[serde(default)]
    pub views: HashMap<String, ViewDefinition>,
}

/// Definition of a single collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionDefinition {
    pub path: String,
    #[serde(default)]
    pub fields: HashMap<String, FieldDefinition>,
    #[serde(default)]
    pub content: bool,
    #[serde(default)]
    pub additional_properties: bool,
    #[serde(default)]
    pub strict: bool,
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub on_delete: Option<OnDeletePolicy>,
    #[serde(default)]
    pub id: Option<IdConfig>,
    /// JSONL record definitions (for multi-record files)
    #[serde(default)]
    pub records: Option<RecordDefinition>,
}

/// Configuration for document ID generation and conflict handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdConfig {
    #[serde(default)]
    pub auto: Option<AutoIdStrategy>,
    #[serde(default)]
    pub on_conflict: Option<OnConflict>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoIdStrategy {
    Ulid,
    Uuid,
    Nanoid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    Error,
    Suffix,
}

/// Definition of a single field in a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(rename = "enum", default)]
    pub enum_values: Option<Vec<String>>,
    #[serde(default)]
    pub default: Option<serde_yaml::Value>,
    #[serde(default)]
    pub target: Option<RefTarget>,
    #[serde(default)]
    pub items: Option<ItemType>,
    #[serde(default)]
    pub on_delete: Option<OnDeletePolicy>,
}

/// Field type enumeration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Date,
    Datetime,
    List,
    Object,
    Ref,
    /// Custom/reusable type name (defined in `types:` section)
    #[serde(untagged)]
    Custom(std::string::String),
}

/// Reference target - single collection or multiple (for polymorphic refs)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RefTarget {
    Single(String),
    Multiple(Vec<String>),
}

/// Item type for lists - either a simple type name or a full field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ItemType {
    Simple(String),
    Complex(Box<FieldDefinition>),
}

/// On-delete referential integrity policy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnDeletePolicy {
    Error,
    Cascade,
    Nullify,
    Archive,
}

/// View definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub query: String,
    #[serde(rename = "type", default)]
    pub view_type: Option<ViewType>,
    #[serde(default)]
    pub materialize: bool,
    #[serde(default)]
    pub buffer: Option<String>,
    #[serde(default)]
    pub params: Option<HashMap<String, ParamDefinition>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewType {
    View,
    Query,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamDefinition {
    #[serde(rename = "type")]
    pub param_type: String,
}

/// JSONL record definition for multi-record file collections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordDefinition {
    /// Base fields shared across all record variants
    pub base: HashMap<String, FieldDefinition>,
    /// Discriminator field name
    pub by: String,
    /// Per-variant field definitions
    pub variants: HashMap<String, RecordVariant>,
}

/// A single variant of a JSONL record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordVariant {
    #[serde(default)]
    pub fields: HashMap<String, FieldDefinition>,
}

impl SchemaDefinition {
    /// Check whether a field type name refers to a reusable type
    pub fn is_custom_type(&self, name: &str) -> bool {
        self.types.contains_key(name)
    }

    /// Get the fields for a reusable type, if it exists
    pub fn get_custom_type(&self, name: &str) -> Option<&HashMap<String, FieldDefinition>> {
        self.types.get(name)
    }
}

impl CollectionDefinition {
    /// Returns the file extension from the path template
    pub fn file_extension(&self) -> &str {
        if self.path.ends_with(".json") {
            "json"
        } else if self.path.ends_with(".jsonl") {
            "jsonl"
        } else {
            "md"
        }
    }

    /// Returns the on_conflict policy, defaulting to Error
    pub fn on_conflict(&self) -> OnConflict {
        self.id
            .as_ref()
            .and_then(|id| id.on_conflict.clone())
            .unwrap_or(OnConflict::Error)
    }

    /// Returns the auto-id strategy, if configured
    pub fn auto_id(&self) -> Option<&AutoIdStrategy> {
        self.id.as_ref().and_then(|id| id.auto.as_ref())
    }
}

impl FieldDefinition {
    /// Returns the effective on_delete policy for a ref field,
    /// falling back to the provided collection-level default
    pub fn effective_on_delete(&self, collection_default: Option<&OnDeletePolicy>) -> OnDeletePolicy {
        self.on_delete
            .clone()
            .or_else(|| collection_default.cloned())
            .unwrap_or(OnDeletePolicy::Error)
    }
}

impl RefTarget {
    /// Returns the list of target collection names
    pub fn targets(&self) -> Vec<&str> {
        match self {
            RefTarget::Single(s) => vec![s.as_str()],
            RefTarget::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}
