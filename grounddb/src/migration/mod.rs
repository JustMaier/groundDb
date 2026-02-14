use crate::schema::SchemaDefinition;
use std::collections::HashSet;

/// Represents a single schema change detected between two schema versions.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaMigration {
    CollectionAdded { name: String },
    CollectionRemoved { name: String },
    FieldAdded {
        collection: String,
        field: String,
        has_default: bool,
        required: bool,
    },
    FieldRemoved {
        collection: String,
        field: String,
    },
    FieldTypeChanged {
        collection: String,
        field: String,
    },
    EnumValueAdded {
        collection: String,
        field: String,
        value: String,
    },
    EnumValueRemoved {
        collection: String,
        field: String,
        value: String,
    },
    PathTemplateChanged {
        collection: String,
        old_template: String,
        new_template: String,
    },
    DefaultChanged {
        collection: String,
        field: String,
    },
}

impl SchemaMigration {
    /// Whether this migration is safe to auto-apply without user intervention.
    pub fn is_safe(&self) -> bool {
        match self {
            SchemaMigration::CollectionAdded { .. } => true,
            SchemaMigration::EnumValueAdded { .. } => true,
            SchemaMigration::FieldAdded { has_default, required, .. } => {
                // Safe if has a default or is not required
                *has_default || !*required
            }
            SchemaMigration::DefaultChanged { .. } => true,
            _ => false,
        }
    }

    /// Human-readable description of this migration.
    pub fn describe(&self) -> String {
        match self {
            SchemaMigration::CollectionAdded { name } => {
                format!("Collection '{}' added", name)
            }
            SchemaMigration::CollectionRemoved { name } => {
                format!("Collection '{}' removed (data preserved)", name)
            }
            SchemaMigration::FieldAdded { collection, field, has_default, required } => {
                let qualifier = if *required && !*has_default {
                    " (required, no default — ERROR)"
                } else if *has_default {
                    " (has default — will backfill)"
                } else {
                    " (optional)"
                };
                format!("Field '{}.{}' added{}", collection, field, qualifier)
            }
            SchemaMigration::FieldRemoved { collection, field } => {
                format!("Field '{}.{}' removed", collection, field)
            }
            SchemaMigration::FieldTypeChanged { collection, field } => {
                format!("Field '{}.{}' type changed (ERROR)", collection, field)
            }
            SchemaMigration::EnumValueAdded { collection, field, value } => {
                format!("Enum value '{}' added to '{}.{}'", value, collection, field)
            }
            SchemaMigration::EnumValueRemoved { collection, field, value } => {
                format!("Enum value '{}' removed from '{}.{}' (WARNING)", value, collection, field)
            }
            SchemaMigration::PathTemplateChanged { collection, old_template, new_template } => {
                format!(
                    "Path template for '{}' changed: '{}' -> '{}' (WARNING)",
                    collection, old_template, new_template
                )
            }
            SchemaMigration::DefaultChanged { collection, field } => {
                format!("Default value for '{}.{}' changed", collection, field)
            }
        }
    }
}

/// Compare two schema versions and return a list of detected migrations.
pub fn diff_schemas(old: &SchemaDefinition, new: &SchemaDefinition) -> Vec<SchemaMigration> {
    let mut migrations = Vec::new();

    let old_names: HashSet<&String> = old.collections.keys().collect();
    let new_names: HashSet<&String> = new.collections.keys().collect();

    // Collections added
    for name in new_names.difference(&old_names) {
        migrations.push(SchemaMigration::CollectionAdded {
            name: (*name).clone(),
        });
    }

    // Collections removed
    for name in old_names.difference(&new_names) {
        migrations.push(SchemaMigration::CollectionRemoved {
            name: (*name).clone(),
        });
    }

    // Collections present in both — diff fields
    for name in old_names.intersection(&new_names) {
        let old_col = &old.collections[*name];
        let new_col = &new.collections[*name];

        // Path template changed
        if old_col.path != new_col.path {
            migrations.push(SchemaMigration::PathTemplateChanged {
                collection: (*name).clone(),
                old_template: old_col.path.clone(),
                new_template: new_col.path.clone(),
            });
        }

        let old_fields: HashSet<&String> = old_col.fields.keys().collect();
        let new_fields: HashSet<&String> = new_col.fields.keys().collect();

        // Fields added
        for field_name in new_fields.difference(&old_fields) {
            let field_def = &new_col.fields[*field_name];
            migrations.push(SchemaMigration::FieldAdded {
                collection: (*name).clone(),
                field: (*field_name).clone(),
                has_default: field_def.default.is_some(),
                required: field_def.required,
            });
        }

        // Fields removed
        for field_name in old_fields.difference(&new_fields) {
            migrations.push(SchemaMigration::FieldRemoved {
                collection: (*name).clone(),
                field: (*field_name).clone(),
            });
        }

        // Fields in both — check for type changes, enum changes, default changes
        for field_name in old_fields.intersection(&new_fields) {
            let old_field = &old_col.fields[*field_name];
            let new_field = &new_col.fields[*field_name];

            // Type changed
            if old_field.field_type != new_field.field_type {
                migrations.push(SchemaMigration::FieldTypeChanged {
                    collection: (*name).clone(),
                    field: (*field_name).clone(),
                });
            }

            // Enum values changed
            if let (Some(old_enums), Some(new_enums)) =
                (&old_field.enum_values, &new_field.enum_values)
            {
                let old_set: HashSet<&String> = old_enums.iter().collect();
                let new_set: HashSet<&String> = new_enums.iter().collect();

                for val in new_set.difference(&old_set) {
                    migrations.push(SchemaMigration::EnumValueAdded {
                        collection: (*name).clone(),
                        field: (*field_name).clone(),
                        value: (*val).clone(),
                    });
                }

                for val in old_set.difference(&new_set) {
                    migrations.push(SchemaMigration::EnumValueRemoved {
                        collection: (*name).clone(),
                        field: (*field_name).clone(),
                        value: (*val).clone(),
                    });
                }
            }

            // Default changed
            if old_field.default != new_field.default {
                migrations.push(SchemaMigration::DefaultChanged {
                    collection: (*name).clone(),
                    field: (*field_name).clone(),
                });
            }
        }
    }

    migrations
}

/// Check if any migrations are unsafe (would block boot).
pub fn has_unsafe_migrations(migrations: &[SchemaMigration]) -> Vec<&SchemaMigration> {
    migrations.iter().filter(|m| !m.is_safe()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema_str;

    #[test]
    fn test_diff_no_changes() {
        let schema = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&schema, &schema);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_new_field_with_default() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      role: { type: string, default: member }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert_eq!(diffs.len(), 1);
        match &diffs[0] {
            SchemaMigration::FieldAdded { field, has_default, .. } => {
                assert_eq!(field, "role");
                assert!(has_default);
            }
            _ => panic!("Expected FieldAdded"),
        }
        assert!(diffs[0].is_safe());
    }

    #[test]
    fn test_diff_removed_field() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert!(matches!(&diffs[0], SchemaMigration::FieldRemoved { .. }));
        assert!(!diffs[0].is_safe());
    }

    #[test]
    fn test_diff_path_template_change() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "people/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert_eq!(diffs.len(), 1);
        match &diffs[0] {
            SchemaMigration::PathTemplateChanged { old_template, new_template, .. } => {
                assert_eq!(old_template, "users/{name}.md");
                assert_eq!(new_template, "people/{name}.md");
            }
            _ => panic!("Expected PathTemplateChanged"),
        }
        assert!(!diffs[0].is_safe());
    }

    #[test]
    fn test_diff_new_required_field_without_default_is_unsafe() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert!(!diffs[0].is_safe());
    }

    #[test]
    fn test_diff_collection_added() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
  posts:
    path: "posts/{title}.md"
    fields:
      title: { type: string, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert!(diffs.iter().any(|d| matches!(d, SchemaMigration::CollectionAdded { name } if name == "posts")));
    }

    #[test]
    fn test_diff_enum_value_added() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      role: { type: string, enum: [admin, member] }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest] }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert!(diffs.iter().any(|d| matches!(d, SchemaMigration::EnumValueAdded { value, .. } if value == "guest")));
    }

    #[test]
    fn test_diff_field_type_changed_is_unsafe() {
        let old = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
"#,
        )
        .unwrap();
        let new = parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: number, required: true }
"#,
        )
        .unwrap();
        let diffs = diff_schemas(&old, &new);
        assert_eq!(diffs.len(), 1);
        assert!(matches!(&diffs[0], SchemaMigration::FieldTypeChanged { .. }));
        assert!(!diffs[0].is_safe());
    }
}
