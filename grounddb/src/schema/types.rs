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
}

/// Configuration for document ID generation and conflict handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdConfig {
    pub auto: Option<AutoIdStrategy>,
    pub on_conflict: Option<OnConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoIdStrategy {
    Ulid,
    Uuid,
    Nanoid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(untagged)]
    Custom(std::string::String),
}

/// Reference target - single collection or multiple
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RefTarget {
    Single(String),
    Multiple(Vec<String>),
}

/// Item type for lists
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ItemType {
    Simple(String),
    Complex(Box<FieldDefinition>),
}

/// On-delete referential integrity policy
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
