use grounddb::schema::{FieldDefinition, FieldType, ItemType, RefTarget};
use heck::{ToPascalCase, ToSnakeCase};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Convert a collection name to its singular PascalCase struct name.
/// e.g. "users" -> "User", "posts" -> "Post", "comments" -> "Comment"
pub fn collection_struct_name(collection_name: &str) -> String {
    singularize(collection_name).to_pascal_case()
}

/// Convert a collection name to a snake_case method name.
/// e.g. "users" -> "users", "post_feed" -> "post_feed"
pub fn collection_method_name(collection_name: &str) -> String {
    collection_name.to_snake_case()
}

/// Generate an enum name from collection singular + field name.
/// e.g. ("users", "role") -> "UserRole"
pub fn enum_type_name(collection_name: &str, field_name: &str) -> String {
    let singular = singularize(collection_name);
    format!(
        "{}{}",
        singular.to_pascal_case(),
        field_name.to_pascal_case()
    )
}

/// Generate a polymorphic ref enum name from field name.
/// e.g. "parent" -> "ParentRef"
pub fn ref_enum_name(field_name: &str) -> String {
    format!("{}Ref", field_name.to_pascal_case())
}

/// Generate a view row struct name.
/// e.g. "post_feed" -> "PostFeedRow"
pub fn view_row_name(view_name: &str) -> String {
    format!("{}Row", view_name.to_pascal_case())
}

/// Generate a view params struct name.
/// e.g. "post_comments" -> "PostCommentsParams"
pub fn view_params_name(view_name: &str) -> String {
    format!("{}Params", view_name.to_pascal_case())
}

/// Generate a partial struct name.
/// e.g. "User" -> "UserPartial"
pub fn partial_struct_name(struct_name: &str) -> String {
    format!("{}Partial", struct_name)
}

/// Map a schema field to its Rust type as a TokenStream.
/// `collection_name` is used for naming generated enums.
/// `known_types` is the set of reusable type names from the schema.
pub fn field_to_rust_type(
    field: &FieldDefinition,
    collection_name: &str,
    field_name: &str,
    known_types: &[String],
) -> TokenStream {
    let base_type = field_base_type(field, collection_name, field_name, known_types);

    // Wrap in Option if not required and no default
    if !field.required && field.default.is_none() {
        // Lists default to empty vec, objects default to empty value - don't wrap those
        match &field.field_type {
            FieldType::List => base_type,
            FieldType::Object => {
                quote! { Option<#base_type> }
            }
            _ => {
                quote! { Option<#base_type> }
            }
        }
    } else {
        base_type
    }
}

/// Get the base Rust type (without Option wrapping) for a field.
pub fn field_base_type(
    field: &FieldDefinition,
    collection_name: &str,
    field_name: &str,
    known_types: &[String],
) -> TokenStream {
    // If field has enum values, use the generated enum type
    if field.enum_values.is_some() {
        let name = enum_type_name(collection_name, field_name);
        let ident = format_ident!("{}", name);
        return quote! { #ident };
    }

    match &field.field_type {
        FieldType::String => quote! { String },
        FieldType::Number => quote! { f64 },
        FieldType::Boolean => quote! { bool },
        FieldType::Date => quote! { chrono::NaiveDate },
        FieldType::Datetime => quote! { chrono::DateTime<chrono::Utc> },
        FieldType::Object => quote! { serde_json::Value },
        FieldType::List => {
            let item_type = list_item_type(field, collection_name, field_name, known_types);
            quote! { Vec<#item_type> }
        }
        FieldType::Ref => ref_rust_type(field, field_name),
        FieldType::Custom(type_name) => {
            if known_types.contains(type_name) {
                let ident = format_ident!("{}", type_name.to_pascal_case());
                quote! { #ident }
            } else {
                // Fallback to serde_json::Value for unknown types
                quote! { serde_json::Value }
            }
        }
    }
}

/// Get the Rust type for a list's item type.
fn list_item_type(
    field: &FieldDefinition,
    _collection_name: &str,
    _field_name: &str,
    known_types: &[String],
) -> TokenStream {
    match &field.items {
        Some(ItemType::Simple(s)) => match s.as_str() {
            "string" => quote! { String },
            "number" => quote! { f64 },
            "boolean" => quote! { bool },
            "date" => quote! { chrono::NaiveDate },
            "datetime" => quote! { chrono::DateTime<chrono::Utc> },
            "object" => quote! { serde_json::Value },
            other => {
                if known_types.contains(&other.to_string()) {
                    let ident = format_ident!("{}", other.to_pascal_case());
                    quote! { #ident }
                } else {
                    quote! { serde_json::Value }
                }
            }
        },
        Some(ItemType::Complex(inner)) => {
            // Complex item: check if it's a ref type
            match &inner.field_type {
                FieldType::Ref => {
                    // List of refs - just use String for now
                    quote! { String }
                }
                _ => quote! { serde_json::Value },
            }
        }
        None => quote! { serde_json::Value },
    }
}

/// Get the Rust type for a ref field.
fn ref_rust_type(field: &FieldDefinition, field_name: &str) -> TokenStream {
    match &field.target {
        Some(RefTarget::Single(_)) => quote! { String },
        Some(RefTarget::Multiple(_)) => {
            let name = ref_enum_name(field_name);
            let ident = format_ident!("{}", name);
            quote! { #ident }
        }
        None => quote! { String },
    }
}

/// Naive singularization of English words.
pub fn singularize(word: &str) -> String {
    let w = word.to_lowercase();
    if w.ends_with("ies") {
        format!("{}y", &w[..w.len() - 3])
    } else if w.ends_with("ses") || w.ends_with("xes") || w.ends_with("zes") {
        w[..w.len() - 2].to_string()
    } else if w.ends_with("ves") {
        format!("{}f", &w[..w.len() - 3])
    } else if w.ends_with('s') && !w.ends_with("ss") {
        w[..w.len() - 1].to_string()
    } else {
        w
    }
}

/// Check if a field name is a Rust keyword and needs raw identifier syntax.
pub fn safe_field_ident(name: &str) -> proc_macro2::Ident {
    match name {
        "type" | "struct" | "enum" | "fn" | "let" | "mut" | "ref" | "self" | "super" | "crate"
        | "mod" | "use" | "pub" | "impl" | "trait" | "for" | "loop" | "while" | "if" | "else"
        | "match" | "return" | "break" | "continue" | "as" | "in" | "where" | "async"
        | "await" | "dyn" | "move" | "static" | "const" | "unsafe" | "extern" | "true"
        | "false" | "abstract" | "become" | "box" | "do" | "final" | "macro" | "override"
        | "priv" | "typeof" | "unsized" | "virtual" | "yield" | "try" => {
            format_ident!("r#{}", name)
        }
        _ => format_ident!("{}", name.to_snake_case()),
    }
}

/// Convert a string to a valid Rust identifier, preserving original casing for enum variants.
pub fn enum_variant_ident(value: &str) -> proc_macro2::Ident {
    format_ident!("{}", value.to_pascal_case())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singularize() {
        assert_eq!(singularize("users"), "user");
        assert_eq!(singularize("posts"), "post");
        assert_eq!(singularize("comments"), "comment");
        assert_eq!(singularize("events"), "event");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("addresses"), "address");
    }

    #[test]
    fn test_collection_struct_name() {
        assert_eq!(collection_struct_name("users"), "User");
        assert_eq!(collection_struct_name("posts"), "Post");
        assert_eq!(collection_struct_name("comments"), "Comment");
        assert_eq!(collection_struct_name("events"), "Event");
    }

    #[test]
    fn test_enum_type_name() {
        assert_eq!(enum_type_name("users", "role"), "UserRole");
        assert_eq!(enum_type_name("posts", "status"), "PostStatus");
        assert_eq!(enum_type_name("events", "severity"), "EventSeverity");
    }

    #[test]
    fn test_ref_enum_name() {
        assert_eq!(ref_enum_name("parent"), "ParentRef");
    }

    #[test]
    fn test_view_names() {
        assert_eq!(view_row_name("post_feed"), "PostFeedRow");
        assert_eq!(view_params_name("post_comments"), "PostCommentsParams");
    }

    #[test]
    fn test_safe_field_ident() {
        let ident = safe_field_ident("type");
        assert_eq!(ident.to_string(), "r#type");

        let ident = safe_field_ident("name");
        assert_eq!(ident.to_string(), "name");
    }
}
