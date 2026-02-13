use grounddb::schema::{CollectionDefinition, FieldDefinition, RefTarget, SchemaDefinition};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::type_utils::{enum_type_name, enum_variant_ident, ref_enum_name};

/// Generate all enum types from the schema.
/// This includes:
/// 1. Enums for fields with `enum` values (e.g., UserRole, PostStatus)
/// 2. Polymorphic ref enums for multi-target ref fields (e.g., ParentRef)
pub fn generate_enums(schema: &SchemaDefinition) -> TokenStream {
    let mut tokens = TokenStream::new();

    // Sort collections for deterministic output
    let mut collections: Vec<_> = schema.collections.iter().collect();
    collections.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (collection_name, collection_def) in &collections {
        let enum_tokens = generate_collection_enums(collection_name, collection_def);
        tokens.extend(enum_tokens);
    }

    tokens
}

/// Generate enums for a single collection's fields.
fn generate_collection_enums(
    collection_name: &str,
    collection_def: &CollectionDefinition,
) -> TokenStream {
    let mut tokens = TokenStream::new();

    // Sort fields for deterministic output
    let mut fields: Vec<_> = collection_def.fields.iter().collect();
    fields.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (field_name, field_def) in &fields {
        // Generate value enums for fields with enum values
        if let Some(ref enum_values) = field_def.enum_values {
            let enum_tokens =
                generate_value_enum(collection_name, field_name, enum_values, field_def);
            tokens.extend(enum_tokens);
        }

        // Generate polymorphic ref enums for multi-target refs
        if let Some(RefTarget::Multiple(targets)) = &field_def.target {
            let ref_tokens = generate_ref_enum(field_name, targets);
            tokens.extend(ref_tokens);
        }
    }

    tokens
}

/// Generate a value enum like UserRole or PostStatus.
fn generate_value_enum(
    collection_name: &str,
    field_name: &str,
    enum_values: &[String],
    field_def: &FieldDefinition,
) -> TokenStream {
    let type_name = enum_type_name(collection_name, field_name);
    let type_ident = format_ident!("{}", type_name);

    let variants: Vec<_> = enum_values
        .iter()
        .map(|v| enum_variant_ident(v))
        .collect();

    let default_impl = if let Some(ref default_val) = field_def.default {
        let default_str = match default_val {
            serde_yaml::Value::String(s) => s.clone(),
            other => other.as_str().unwrap_or("").to_string(),
        };
        let default_variant = enum_variant_ident(&default_str);
        Some(quote! {
            impl Default for #type_ident {
                fn default() -> Self {
                    Self::#default_variant
                }
            }
        })
    } else {
        None
    };

    quote! {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum #type_ident {
            #(#variants),*
        }

        #default_impl
    }
}

/// Generate a polymorphic ref enum like ParentRef.
fn generate_ref_enum(field_name: &str, targets: &[String]) -> TokenStream {
    let enum_name = ref_enum_name(field_name);
    let enum_ident = format_ident!("{}", enum_name);

    let variants: Vec<_> = targets
        .iter()
        .map(|t| {
            let variant = enum_variant_ident(t);
            quote! { #variant(String) }
        })
        .collect();

    quote! {
        /// Polymorphic reference.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "type", content = "id")]
        pub enum #enum_ident {
            #(#variants),*
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grounddb::schema::{FieldDefinition, FieldType};

    #[test]
    fn test_generate_value_enum_with_default() {
        let field = FieldDefinition {
            field_type: FieldType::String,
            required: false,
            enum_values: Some(vec![
                "admin".to_string(),
                "member".to_string(),
                "guest".to_string(),
            ]),
            default: Some(serde_yaml::Value::String("member".to_string())),
            target: None,
            items: None,
            on_delete: None,
        };

        let tokens = generate_value_enum("users", "role", field.enum_values.as_ref().unwrap(), &field);
        let code = tokens.to_string();

        assert!(code.contains("UserRole"));
        assert!(code.contains("Admin"));
        assert!(code.contains("Member"));
        assert!(code.contains("Guest"));
        assert!(code.contains("impl Default for UserRole"));
    }

    #[test]
    fn test_generate_ref_enum() {
        let tokens = generate_ref_enum("parent", &["posts".to_string(), "comments".to_string()]);
        let code = tokens.to_string();

        assert!(code.contains("ParentRef"));
        assert!(code.contains("Posts"));
        assert!(code.contains("Comments"));
        assert!(code.contains("tag"));
        assert!(code.contains("content"));
    }
}
