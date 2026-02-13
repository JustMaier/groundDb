use crate::error::{GroundDbError, Result};
use crate::schema::{CollectionDefinition, FieldDefinition, FieldType, SchemaDefinition};

/// Result of validating a document
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Validate a document's data against its collection definition.
/// Returns ValidationResult with errors and warnings.
/// If strict mode is on, validation errors cause rejection.
/// If strict mode is off, validation issues are warnings only.
pub fn validate_document(
    schema: &SchemaDefinition,
    collection: &CollectionDefinition,
    data: &serde_yaml::Value,
) -> ValidationResult {
    let mut result = ValidationResult {
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    let mapping = match data.as_mapping() {
        Some(m) => m,
        None => {
            result
                .errors
                .push("Document data must be a YAML mapping".into());
            return result;
        }
    };

    // Check required fields and validate each declared field
    for (field_name, field_def) in &collection.fields {
        let value = mapping.get(serde_yaml::Value::String(field_name.clone()));

        if field_def.required && (value.is_none() || value == Some(&serde_yaml::Value::Null)) {
            if field_def.default.is_none() {
                add_issue(
                    &mut result,
                    collection.strict,
                    format!("Required field '{field_name}' is missing"),
                );
            }
            continue;
        }

        if let Some(val) = value {
            if *val != serde_yaml::Value::Null {
                validate_field_value(schema, field_name, field_def, val, collection.strict, &mut result);
            }
        }
    }

    // Check for additional properties
    if !collection.additional_properties {
        for key in mapping.keys() {
            if let serde_yaml::Value::String(key_str) = key {
                if !collection.fields.contains_key(key_str) {
                    add_issue(
                        &mut result,
                        collection.strict,
                        format!("Unexpected field '{key_str}' (additional_properties is false)"),
                    );
                }
            }
        }
    }

    result
}

/// Apply default values to a document's data. Modifies the data in place.
/// Returns the data with defaults applied.
pub fn apply_defaults(
    collection: &CollectionDefinition,
    data: &mut serde_yaml::Value,
) {
    let mapping = match data.as_mapping_mut() {
        Some(m) => m,
        None => return,
    };

    for (field_name, field_def) in &collection.fields {
        let key = serde_yaml::Value::String(field_name.clone());
        let has_value = mapping
            .get(&key)
            .map(|v| *v != serde_yaml::Value::Null)
            .unwrap_or(false);

        if !has_value {
            if let Some(default) = &field_def.default {
                mapping.insert(key, default.clone());
            }
        }
    }
}

fn validate_field_value(
    schema: &SchemaDefinition,
    field_name: &str,
    field_def: &FieldDefinition,
    value: &serde_yaml::Value,
    strict: bool,
    result: &mut ValidationResult,
) {
    match &field_def.field_type {
        FieldType::String => {
            if !value.is_string() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected string, got {}", type_name(value)),
                );
                return;
            }

            // Check enum values
            if let Some(enum_values) = &field_def.enum_values {
                if let Some(s) = value.as_str() {
                    if !enum_values.contains(&s.to_string()) {
                        add_issue(
                            result,
                            strict,
                            format!(
                                "Field '{field_name}' value '{}' is not in enum: {:?}",
                                s, enum_values
                            ),
                        );
                    }
                }
            }
        }
        FieldType::Number => {
            if !value.is_number() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected number, got {}", type_name(value)),
                );
            }
        }
        FieldType::Boolean => {
            if !value.is_bool() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected boolean, got {}", type_name(value)),
                );
            }
        }
        FieldType::Date | FieldType::Datetime => {
            // Dates are stored as strings in YAML
            if !value.is_string() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected date string, got {}", type_name(value)),
                );
            }
        }
        FieldType::List => {
            if !value.is_sequence() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected list, got {}", type_name(value)),
                );
            }
            // Could validate items here but keeping it simple for v1
        }
        FieldType::Object => {
            if !value.is_mapping() {
                add_issue(
                    result,
                    strict,
                    format!("Field '{field_name}' expected object, got {}", type_name(value)),
                );
            }
        }
        FieldType::Ref => {
            // Refs can be strings (single target) or mappings (polymorphic)
            match &field_def.target {
                Some(crate::schema::RefTarget::Single(_)) => {
                    if !value.is_string() {
                        add_issue(
                            result,
                            strict,
                            format!(
                                "Field '{field_name}' (ref) expected string ID, got {}",
                                type_name(value)
                            ),
                        );
                    }
                }
                Some(crate::schema::RefTarget::Multiple(_)) => {
                    // Polymorphic ref: either a string or a mapping with type+id
                    if !value.is_string() && !value.is_mapping() {
                        add_issue(
                            result,
                            strict,
                            format!(
                                "Field '{field_name}' (polymorphic ref) expected string or {{type, id}} mapping, got {}",
                                type_name(value)
                            ),
                        );
                    }
                }
                None => {
                    // Already caught by schema validation, but be defensive
                }
            }
        }
        FieldType::Custom(type_name_str) => {
            // Validate against reusable type definition
            if let Some(type_fields) = schema.get_custom_type(type_name_str) {
                if let Some(obj) = value.as_mapping() {
                    for (sub_field_name, sub_field_def) in type_fields {
                        let sub_val =
                            obj.get(serde_yaml::Value::String(sub_field_name.clone()));

                        if sub_field_def.required
                            && (sub_val.is_none()
                                || sub_val == Some(&serde_yaml::Value::Null))
                        {
                            add_issue(
                                result,
                                strict,
                                format!(
                                    "Field '{field_name}.{sub_field_name}' is required in type '{type_name_str}'"
                                ),
                            );
                        }
                    }
                } else {
                    add_issue(
                        result,
                        strict,
                        format!(
                            "Field '{field_name}' expected object (type '{type_name_str}'), got {}",
                            type_name(value)
                        ),
                    );
                }
            }
        }
    }
}

fn add_issue(result: &mut ValidationResult, strict: bool, message: String) {
    if strict {
        result.errors.push(message);
    } else {
        result.warnings.push(message);
    }
}

fn type_name(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "null",
        serde_yaml::Value::Bool(_) => "boolean",
        serde_yaml::Value::Number(_) => "number",
        serde_yaml::Value::String(_) => "string",
        serde_yaml::Value::Sequence(_) => "list",
        serde_yaml::Value::Mapping(_) => "object",
        serde_yaml::Value::Tagged(_) => "tagged",
    }
}

/// Validate and apply defaults. Returns an error if strict validation fails.
pub fn validate_and_prepare(
    schema: &SchemaDefinition,
    collection: &CollectionDefinition,
    data: &mut serde_yaml::Value,
) -> Result<Vec<String>> {
    apply_defaults(collection, data);
    let result = validate_document(schema, collection, data);

    if !result.is_ok() {
        return Err(GroundDbError::Validation(format!(
            "Document validation failed:\n  - {}",
            result.errors.join("\n  - ")
        )));
    }

    Ok(result.warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema_str;

    fn test_schema() -> SchemaDefinition {
        parse_schema_str(
            r#"
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

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: false
    strict: true

  events:
    path: "events/{id}.md"
    id: { auto: ulid }
    fields:
      type: { type: string, required: true }
      payload: { type: object }
    additional_properties: true
    strict: false
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_valid_user() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\nrole: admin",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(result.is_ok(), "Errors: {:?}", result.errors);
    }

    #[test]
    fn test_missing_required_field() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice").unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("email")));
    }

    #[test]
    fn test_invalid_enum_value() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\nrole: superadmin",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("superadmin")));
    }

    #[test]
    fn test_type_mismatch() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: 42\nemail: alice@test.com",
        )
        .unwrap();

        // name: 42 -- YAML parses this as number, not string
        let result = validate_document(&schema, collection, &data);
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("name")));
    }

    #[test]
    fn test_additional_properties_rejected() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\nextra_field: oops",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("extra_field")));
    }

    #[test]
    fn test_additional_properties_allowed() {
        let schema = test_schema();
        let collection = &schema.collections["events"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "type: click\nextra: data",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        // events has additional_properties: true and strict: false
        assert!(result.is_ok());
    }

    #[test]
    fn test_non_strict_mode_warnings() {
        let schema = test_schema();
        let collection = &schema.collections["events"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "type: 123", // wrong type for string field, but strict: false
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(result.is_ok()); // no errors
        assert!(result.has_warnings()); // but has warnings
    }

    #[test]
    fn test_apply_defaults() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let mut data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com",
        )
        .unwrap();

        apply_defaults(collection, &mut data);
        assert_eq!(
            data["role"],
            serde_yaml::Value::String("member".into())
        );
    }

    #[test]
    fn test_apply_defaults_doesnt_overwrite() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let mut data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\nrole: admin",
        )
        .unwrap();

        apply_defaults(collection, &mut data);
        assert_eq!(
            data["role"],
            serde_yaml::Value::String("admin".into())
        );
    }

    #[test]
    fn test_validate_and_prepare() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let mut data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com",
        )
        .unwrap();

        let warnings = validate_and_prepare(&schema, collection, &mut data).unwrap();
        assert!(warnings.is_empty());
        // Default should be applied
        assert_eq!(
            data["role"],
            serde_yaml::Value::String("member".into())
        );
    }

    #[test]
    fn test_custom_type_validation() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\naddress:\n  city: NYC",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        // address.street is required but missing
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("street")));
    }

    #[test]
    fn test_valid_custom_type() {
        let schema = test_schema();
        let collection = &schema.collections["users"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "name: Alice\nemail: alice@test.com\naddress:\n  street: '123 Main St'\n  city: NYC",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(result.is_ok(), "Errors: {:?}", result.errors);
    }

    #[test]
    fn test_list_type_validation() {
        let schema = test_schema();
        let collection = &schema.collections["posts"];
        let data: serde_yaml::Value = serde_yaml::from_str(
            "title: Test\nauthor_id: alice\ndate: '2026-01-01'\ntags: not-a-list",
        )
        .unwrap();

        let result = validate_document(&schema, collection, &data);
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("tags")));
    }
}
