//! GroundDB code generation - generates Rust types from schema.yaml at compile time.
//!
//! The main entry point is [`generate_from_schema`], which reads a schema.yaml file
//! and writes a complete Rust source file with typed structs, enums, and store accessors.

mod enum_gen;
mod generator;
mod store_gen;
mod struct_gen;
pub mod type_utils;
mod view_gen;

use std::path::Path;

/// Generate Rust types from a schema.yaml file.
///
/// Reads the schema at `schema_path`, generates typed Rust code, and writes
/// the output to `output_path`. This is intended to be called from a `build.rs`
/// build script.
///
/// # Example
///
/// ```no_run
/// // In build.rs:
/// grounddb_codegen::generate_from_schema("schema.yaml", "src/generated.rs").unwrap();
/// ```
pub fn generate_from_schema(
    schema_path: &str,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = grounddb::schema::parse_schema(Path::new(schema_path))?;
    let tokens = generator::generate_all(&schema);
    let formatted = generator::format_token_stream(&tokens);
    std::fs::write(output_path, formatted)?;
    Ok(())
}

/// Generate Rust types from a schema YAML string.
///
/// Like [`generate_from_schema`] but takes the schema content directly
/// instead of reading from a file. Useful for testing.
pub fn generate_from_schema_str(
    schema_yaml: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let schema = grounddb::schema::parse_schema_str(schema_yaml)?;
    let tokens = generator::generate_all(&schema);
    let formatted = generator::format_token_stream(&tokens);
    Ok(formatted)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SCHEMA: &str = r#"
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
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: false
    strict: true

  comments:
    path: "comments/{parent:type}/{parent:id}/{user:id}.md"
    fields:
      user: { type: ref, target: users, required: true }
      parent: { type: ref, target: [posts, comments], required: true }
    content: true

  events:
    path: "events/{id}.md"
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
    fn test_generate_from_schema_str_full() {
        let result = generate_from_schema_str(TEST_SCHEMA);
        assert!(result.is_ok(), "Generation failed: {:?}", result.err());

        let code = result.unwrap();

        // Verify it's valid Rust
        assert!(
            syn::parse_file(&code).is_ok(),
            "Generated code is not valid Rust:\n{}",
            &code[..code.len().min(2000)]
        );

        // Enum types
        assert!(code.contains("UserRole"), "Missing UserRole enum");
        assert!(code.contains("PostStatus"), "Missing PostStatus enum");
        assert!(code.contains("EventSeverity"), "Missing EventSeverity enum");

        // Enum variants
        assert!(code.contains("Admin"), "Missing Admin variant");
        assert!(code.contains("Member"), "Missing Member variant");
        assert!(code.contains("Guest"), "Missing Guest variant");
        assert!(code.contains("Draft"), "Missing Draft variant");
        assert!(code.contains("Published"), "Missing Published variant");

        // Default impls
        assert!(
            code.contains("impl Default for UserRole"),
            "Missing UserRole Default impl"
        );
        assert!(
            code.contains("impl Default for PostStatus"),
            "Missing PostStatus Default impl"
        );

        // Document structs
        assert!(code.contains("pub struct User"), "Missing User struct");
        assert!(code.contains("pub struct Post"), "Missing Post struct");
        assert!(code.contains("pub struct Comment"), "Missing Comment struct");
        assert!(code.contains("pub struct Event"), "Missing Event struct");

        // Reusable types
        assert!(code.contains("pub struct Address"), "Missing Address struct");

        // Polymorphic ref
        assert!(code.contains("ParentRef"), "Missing ParentRef enum");

        // Partial structs
        assert!(code.contains("pub struct UserPartial"), "Missing UserPartial");
        assert!(code.contains("pub struct PostPartial"), "Missing PostPartial");
        assert!(code.contains("pub struct CommentPartial"), "Missing CommentPartial");
        assert!(code.contains("pub struct EventPartial"), "Missing EventPartial");

        // View row structs
        assert!(code.contains("PostFeedRow"), "Missing PostFeedRow");
        assert!(code.contains("UserLookupRow"), "Missing UserLookupRow");
        assert!(code.contains("RecentActivityRow"), "Missing RecentActivityRow");
        assert!(code.contains("PostCommentsRow"), "Missing PostCommentsRow");

        // View params
        assert!(code.contains("PostCommentsParams"), "Missing PostCommentsParams");

        // Store extension
        assert!(code.contains("StoreExt"), "Missing StoreExt trait");
        assert!(code.contains("fn users"), "Missing users accessor");
        assert!(code.contains("fn posts"), "Missing posts accessor");
        assert!(code.contains("fn comments"), "Missing comments accessor");
        assert!(code.contains("fn events"), "Missing events accessor");
    }

    #[test]
    fn test_generate_minimal_schema() {
        let schema = r#"
collections:
  items:
    path: "items/{name}.md"
    fields:
      name: { type: string, required: true }
"#;
        let result = generate_from_schema_str(schema);
        assert!(result.is_ok(), "Generation failed: {:?}", result.err());

        let code = result.unwrap();
        assert!(syn::parse_file(&code).is_ok(), "Not valid Rust");
        assert!(code.contains("pub struct Item"));
        assert!(code.contains("pub struct ItemPartial"));
    }

    #[test]
    fn test_generate_all_field_types() {
        let schema = r#"
collections:
  records:
    path: "records/{id}.md"
    fields:
      name: { type: string, required: true }
      count: { type: number, required: true }
      active: { type: boolean, required: true }
      birthday: { type: date }
      updated: { type: datetime }
      tags: { type: list, items: string }
      metadata: { type: object }
      owner: { type: ref, target: records }
"#;
        let result = generate_from_schema_str(schema);
        assert!(result.is_ok(), "Generation failed: {:?}", result.err());

        let code = result.unwrap();
        assert!(syn::parse_file(&code).is_ok(), "Not valid Rust:\n{}", &code[..code.len().min(2000)]);

        assert!(code.contains("String"), "Missing String type");
        assert!(code.contains("f64"), "Missing f64 type");
        assert!(code.contains("bool"), "Missing bool type");
        assert!(code.contains("NaiveDate"), "Missing NaiveDate type");
        assert!(code.contains("DateTime"), "Missing DateTime type");
        assert!(code.contains("Vec"), "Missing Vec type");
        assert!(code.contains("serde_json"), "Missing serde_json::Value type");
    }

    #[test]
    fn test_rust_keyword_field_names() {
        let schema = r#"
collections:
  events:
    path: "events/{id}.md"
    fields:
      type: { type: string, required: true }
      ref: { type: string }
"#;
        let result = generate_from_schema_str(schema);
        assert!(result.is_ok(), "Generation failed: {:?}", result.err());

        let code = result.unwrap();
        assert!(syn::parse_file(&code).is_ok(), "Not valid Rust:\n{}", &code[..code.len().min(2000)]);
    }
}
