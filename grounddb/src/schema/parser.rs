use crate::error::{GroundDbError, Result};
use super::types::*;
use std::path::Path;

/// Parse a schema.yaml file into a SchemaDefinition
pub fn parse_schema(path: &Path) -> Result<SchemaDefinition> {
    let content = std::fs::read_to_string(path)?;
    parse_schema_str(&content)
}

/// Parse a schema YAML string into a SchemaDefinition
pub fn parse_schema_str(content: &str) -> Result<SchemaDefinition> {
    let schema: SchemaDefinition = serde_yaml::from_str(content)
        .map_err(|e| GroundDbError::Schema(format!("Failed to parse schema YAML: {e}")))?;
    validate_schema(&schema)?;
    Ok(schema)
}

/// Validate the schema for semantic correctness after parsing
fn validate_schema(schema: &SchemaDefinition) -> Result<()> {
    let mut errors: Vec<String> = Vec::new();

    for (name, collection) in &schema.collections {
        validate_collection(schema, name, collection, &mut errors);
    }

    for (name, view) in &schema.views {
        validate_view(name, view, &mut errors);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(GroundDbError::Schema(format!(
            "Schema validation failed:\n  - {}",
            errors.join("\n  - ")
        )))
    }
}

fn validate_collection(
    schema: &SchemaDefinition,
    name: &str,
    collection: &CollectionDefinition,
    errors: &mut Vec<String>,
) {
    // Path template must not be empty
    if collection.path.is_empty() {
        errors.push(format!("Collection '{name}': path template is empty"));
    }

    // Validate each field definition
    for (field_name, field) in &collection.fields {
        validate_field(schema, name, field_name, field, errors);
    }

    // If strict is set, additional_properties should typically be false
    // (but this is a warning, not a hard error -- the user might know what they're doing)
}

fn validate_field(
    schema: &SchemaDefinition,
    collection_name: &str,
    field_name: &str,
    field: &FieldDefinition,
    errors: &mut Vec<String>,
) {
    let ctx = format!("Collection '{collection_name}', field '{field_name}'");

    match &field.field_type {
        FieldType::Ref => {
            // ref fields must have a target
            if field.target.is_none() {
                errors.push(format!("{ctx}: ref type requires a 'target'"));
            } else if let Some(target) = &field.target {
                // Validate that target collections exist
                for t in target.targets() {
                    if !schema.collections.contains_key(t) {
                        errors.push(format!(
                            "{ctx}: ref target '{t}' is not a defined collection"
                        ));
                    }
                }
            }
        }
        FieldType::List => {
            // list fields should have an items definition
            if field.items.is_none() {
                // Not an error, but worth noting -- items defaults to any
            }
            // If items is a ref, validate the target
            if let Some(ItemType::Complex(item_def)) = &field.items {
                if item_def.field_type == FieldType::Ref {
                    if let Some(target) = &item_def.target {
                        for t in target.targets() {
                            if !schema.collections.contains_key(t) {
                                errors.push(format!(
                                    "{ctx}: list item ref target '{t}' is not a defined collection"
                                ));
                            }
                        }
                    }
                }
            }
        }
        FieldType::Custom(type_name) => {
            // Custom type must be defined in the types section
            if !schema.is_custom_type(type_name) {
                errors.push(format!(
                    "{ctx}: type '{type_name}' is not a built-in type or defined in 'types:'"
                ));
            }
        }
        _ => {}
    }

    // Enum values should only be set for string type
    if field.enum_values.is_some() && field.field_type != FieldType::String {
        errors.push(format!("{ctx}: enum values are only valid for string type"));
    }

    // on_delete is only valid for ref fields
    if field.on_delete.is_some() && field.field_type != FieldType::Ref {
        // Also valid on list items of type ref, handled via ItemType
        if field.field_type != FieldType::List {
            errors.push(format!("{ctx}: on_delete is only valid for ref fields"));
        }
    }
}

fn validate_view(
    name: &str,
    view: &ViewDefinition,
    errors: &mut Vec<String>,
) {
    if view.query.trim().is_empty() {
        errors.push(format!("View '{name}': query is empty"));
    }

    // Query template views must have params
    if view.view_type == Some(ViewType::Query) && view.params.is_none() {
        errors.push(format!(
            "View '{name}': query type views must define params"
        ));
    }

    // Buffer must be a valid multiplier like "2x"
    if let Some(buffer) = &view.buffer {
        if !buffer.ends_with('x') || buffer[..buffer.len() - 1].parse::<f64>().is_err() {
            errors.push(format!(
                "View '{name}': buffer must be a multiplier like '2x', got '{buffer}'"
            ));
        }
    }
}

/// Compute a deterministic hash of a schema YAML string for change detection
pub fn hash_schema(content: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_SCHEMA: &str = r#"
types:
  address:
    street: { type: string, required: true }
    city: { type: string, required: true }
    state: { type: string }
    zip: { type: string }

collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }
      address: { type: address }
    additional_properties: false
    strict: true
    on_delete: error

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    id: { on_conflict: suffix }
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true, on_delete: cascade }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: false
    strict: true

  comments:
    path: "comments/{parent:type}/{parent:id}/{user:id}-{created_at:YYYY-MM-DDTHHMM}.md"
    fields:
      user: { type: ref, target: users, required: true, on_delete: cascade }
      parent: { type: ref, target: [posts, comments], required: true, on_delete: cascade }
    content: true

  events:
    path: "events/{id}.md"
    id: { auto: ulid }
    fields:
      type: { type: string, required: true }
      severity: { type: string, enum: [info, warn, error], default: info }
      payload: { type: object }
    additional_properties: true
    strict: false

views:
  post_feed:
    query: |
      SELECT p.title, p.date, p.tags, u.name AS author_name, u.id AS author_id
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.status = 'published'
      ORDER BY p.date DESC
      LIMIT 100
    materialize: true
    buffer: 2x

  user_lookup:
    query: |
      SELECT id, name, email, role
      FROM users
      ORDER BY name ASC
    materialize: true

  recent_activity:
    query: |
      SELECT id, title, modified_at, status
      FROM posts
      ORDER BY modified_at DESC
      LIMIT 50
    materialize: true
    buffer: 2x

  post_comments:
    type: query
    query: |
      SELECT c.id, c.created_at, c.content, u.name AS commenter_name
      FROM comments c
      JOIN users u ON c.user = u.id
      WHERE c.parent = :post_id
      ORDER BY c.created_at ASC
    params:
      post_id: { type: string }
"#;

    #[test]
    fn test_parse_full_schema() {
        let schema = parse_schema_str(FULL_SCHEMA).unwrap();

        // Types
        assert!(schema.types.contains_key("address"));
        let address = &schema.types["address"];
        assert!(address.contains_key("street"));
        assert!(address.contains_key("city"));
        assert!(address["street"].required);

        // Collections
        assert_eq!(schema.collections.len(), 4);
        assert!(schema.collections.contains_key("users"));
        assert!(schema.collections.contains_key("posts"));
        assert!(schema.collections.contains_key("comments"));
        assert!(schema.collections.contains_key("events"));

        // Users collection
        let users = &schema.collections["users"];
        assert_eq!(users.path, "users/{name}.md");
        assert!(!users.additional_properties);
        assert!(users.strict);
        assert_eq!(users.on_delete, Some(OnDeletePolicy::Error));
        assert_eq!(users.fields.len(), 4);
        assert!(users.fields["name"].required);
        assert_eq!(
            users.fields["role"].enum_values,
            Some(vec!["admin".into(), "member".into(), "guest".into()])
        );
        assert_eq!(
            users.fields["role"].default,
            Some(serde_yaml::Value::String("member".into()))
        );
        // Custom type field
        assert_eq!(users.fields["address"].field_type, FieldType::Custom("address".into()));

        // Posts collection
        let posts = &schema.collections["posts"];
        assert!(posts.content);
        assert_eq!(posts.on_conflict(), OnConflict::Suffix);
        assert!(posts.fields["author_id"].required);
        assert_eq!(posts.fields["author_id"].field_type, FieldType::Ref);
        assert_eq!(
            posts.fields["author_id"].target,
            Some(RefTarget::Single("users".into()))
        );
        assert_eq!(
            posts.fields["author_id"].on_delete,
            Some(OnDeletePolicy::Cascade)
        );

        // Comments - polymorphic ref
        let comments = &schema.collections["comments"];
        assert_eq!(
            comments.fields["parent"].target,
            Some(RefTarget::Multiple(vec!["posts".into(), "comments".into()]))
        );

        // Events - auto id
        let events = &schema.collections["events"];
        assert_eq!(events.auto_id(), Some(&AutoIdStrategy::Ulid));
        assert!(events.additional_properties);
        assert!(!events.strict);

        // Views
        assert_eq!(schema.views.len(), 4);
        assert!(schema.views["post_feed"].materialize);
        assert_eq!(schema.views["post_feed"].buffer, Some("2x".into()));
        assert_eq!(
            schema.views["post_comments"].view_type,
            Some(ViewType::Query)
        );
        assert!(schema.views["post_comments"].params.is_some());
    }

    #[test]
    fn test_invalid_ref_target() {
        let yaml = r#"
collections:
  posts:
    path: "posts/{title}.md"
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: nonexistent }
"#;
        let result = parse_schema_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"));
    }

    #[test]
    fn test_ref_missing_target() {
        let yaml = r#"
collections:
  posts:
    path: "posts/{title}.md"
    fields:
      title: { type: string, required: true }
      author_id: { type: ref }
"#;
        let result = parse_schema_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("target"));
    }

    #[test]
    fn test_empty_path() {
        let yaml = r#"
collections:
  posts:
    path: ""
    fields:
      title: { type: string }
"#;
        let result = parse_schema_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_view_needs_params() {
        let yaml = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string }
views:
  my_query:
    type: query
    query: "SELECT * FROM users WHERE id = :id"
"#;
        let result = parse_schema_str(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("params"));
    }

    #[test]
    fn test_invalid_buffer_format() {
        let yaml = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string }
views:
  feed:
    query: "SELECT * FROM users"
    buffer: "abc"
"#;
        let result = parse_schema_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_hash_deterministic() {
        let h1 = hash_schema("test content");
        let h2 = hash_schema("test content");
        assert_eq!(h1, h2);
        let h3 = hash_schema("different content");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_minimal_schema() {
        let yaml = r#"
collections:
  notes:
    path: "notes/{title}.md"
    fields:
      title: { type: string, required: true }
    content: true
"#;
        let schema = parse_schema_str(yaml).unwrap();
        assert_eq!(schema.collections.len(), 1);
        assert!(schema.collections["notes"].content);
    }
}
