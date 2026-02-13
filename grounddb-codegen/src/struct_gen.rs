use grounddb::schema::{CollectionDefinition, FieldDefinition, FieldType, SchemaDefinition};
use heck::ToPascalCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::type_utils::{
    collection_struct_name, enum_type_name, field_to_rust_type, partial_struct_name,
    safe_field_ident,
};

/// Generate document structs and partial structs for all collections.
pub fn generate_structs(schema: &SchemaDefinition) -> TokenStream {
    let mut tokens = TokenStream::new();

    let known_types: Vec<String> = schema.types.keys().cloned().collect();

    // Generate reusable type structs first
    let type_tokens = generate_reusable_types(schema, &known_types);
    tokens.extend(type_tokens);

    // Sort collections for deterministic output
    let mut collections: Vec<_> = schema.collections.iter().collect();
    collections.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (collection_name, collection_def) in &collections {
        let struct_tokens =
            generate_collection_struct(collection_name, collection_def, &known_types);
        tokens.extend(struct_tokens);

        let partial_tokens =
            generate_partial_struct(collection_name, collection_def, &known_types);
        tokens.extend(partial_tokens);
    }

    tokens
}

/// Generate structs for reusable types defined in the `types:` section.
fn generate_reusable_types(schema: &SchemaDefinition, known_types: &[String]) -> TokenStream {
    let mut tokens = TokenStream::new();

    let mut types: Vec<_> = schema.types.iter().collect();
    types.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (type_name, fields) in types {
        let struct_name = format_ident!("{}", type_name.to_pascal_case());

        let mut field_entries: Vec<_> = fields.iter().collect();
        field_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let field_tokens: Vec<_> = field_entries
            .iter()
            .map(|(field_name, field_def)| {
                let ident = safe_field_ident(field_name);
                let ty = field_to_rust_type(field_def, type_name, field_name, known_types);
                generate_field_with_attrs(&ident, &ty, field_def, type_name, field_name)
            })
            .collect();

        tokens.extend(quote! {
            #[derive(Debug, Clone, Serialize, Deserialize)]
            pub struct #struct_name {
                #(#field_tokens)*
            }
        });
    }

    tokens
}

/// Generate a document struct for a collection.
fn generate_collection_struct(
    collection_name: &str,
    collection_def: &CollectionDefinition,
    known_types: &[String],
) -> TokenStream {
    let struct_name_str = collection_struct_name(collection_name);
    let struct_ident = format_ident!("{}", struct_name_str);

    let path = &collection_def.path;
    let doc_comment = format!(
        " A document in the `{}` collection.\n Path: {}",
        collection_name, path
    );

    let mut fields: Vec<_> = collection_def.fields.iter().collect();
    fields.sort_by(|(a, _), (b, _)| a.cmp(b));

    let field_tokens: Vec<_> = fields
        .iter()
        .map(|(field_name, field_def)| {
            let ident = safe_field_ident(field_name);
            let ty = field_to_rust_type(field_def, collection_name, field_name, known_types);
            generate_field_with_attrs(&ident, &ty, field_def, collection_name, field_name)
        })
        .collect();

    quote! {
        #[doc = #doc_comment]
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct #struct_ident {
            #(#field_tokens)*
        }
    }
}

/// Generate a partial update struct for a collection.
fn generate_partial_struct(
    collection_name: &str,
    collection_def: &CollectionDefinition,
    known_types: &[String],
) -> TokenStream {
    let base_name = collection_struct_name(collection_name);
    let partial_name_str = partial_struct_name(&base_name);
    let partial_ident = format_ident!("{}", partial_name_str);

    let mut fields: Vec<_> = collection_def.fields.iter().collect();
    fields.sort_by(|(a, _), (b, _)| a.cmp(b));

    let field_tokens: Vec<_> = fields
        .iter()
        .map(|(field_name, field_def)| {
            let ident = safe_field_ident(field_name);
            // For partial structs, all fields are Option<BaseType>
            let base_ty =
                crate::type_utils::field_base_type(field_def, collection_name, field_name, known_types);
            let serde_attr = generate_partial_serde_attr(field_name);
            quote! {
                #serde_attr
                pub #ident: Option<#base_ty>,
            }
        })
        .collect();

    quote! {
        #[derive(Debug, Clone, Default, Serialize, Deserialize)]
        pub struct #partial_ident {
            #(#field_tokens)*
        }
    }
}

/// Generate a struct field with appropriate serde attributes.
fn generate_field_with_attrs(
    ident: &proc_macro2::Ident,
    ty: &TokenStream,
    field_def: &FieldDefinition,
    collection_name: &str,
    field_name: &str,
) -> TokenStream {
    let serde_attrs = generate_serde_attrs(field_def, collection_name, field_name);

    let rename_attr = if ident.to_string().starts_with("r#") {
        let original = field_name;
        Some(quote! { #[serde(rename = #original)] })
    } else if ident.to_string() != *field_name {
        let original = field_name;
        Some(quote! { #[serde(rename = #original)] })
    } else {
        None
    };

    quote! {
        #rename_attr
        #serde_attrs
        pub #ident: #ty,
    }
}

/// Generate serde attributes for a field.
fn generate_serde_attrs(
    field_def: &FieldDefinition,
    collection_name: &str,
    field_name: &str,
) -> TokenStream {
    match &field_def.field_type {
        FieldType::List => {
            // Lists always get #[serde(default)]
            quote! { #[serde(default)] }
        }
        _ => {
            if let Some(ref _default_val) = field_def.default {
                if field_def.enum_values.is_some() {
                    // Enum with default - use the enum's Default impl
                    let enum_name = enum_type_name(collection_name, field_name);
                    let default_fn = format!("{}::default", enum_name);
                    quote! { #[serde(default = #default_fn)] }
                } else {
                    // Non-enum with default
                    quote! { #[serde(default)] }
                }
            } else if !field_def.required {
                // Optional fields without default need skip_serializing_if for Option
                match &field_def.field_type {
                    FieldType::Object => {
                        quote! { #[serde(default, skip_serializing_if = "Option::is_none")] }
                    }
                    _ => {
                        quote! { #[serde(default, skip_serializing_if = "Option::is_none")] }
                    }
                }
            } else {
                quote! {}
            }
        }
    }
}

/// Generate serde attribute for partial struct fields.
fn generate_partial_serde_attr(field_name: &str) -> TokenStream {
    let ident_str = crate::type_utils::safe_field_ident(field_name).to_string();
    if ident_str.starts_with("r#") || ident_str != field_name {
        let original = field_name;
        quote! {
            #[serde(rename = #original, default, skip_serializing_if = "Option::is_none")]
        }
    } else {
        quote! {
            #[serde(default, skip_serializing_if = "Option::is_none")]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grounddb::schema::{CollectionDefinition, FieldDefinition, FieldType};
    use std::collections::HashMap;

    fn make_string_field(required: bool) -> FieldDefinition {
        FieldDefinition {
            field_type: FieldType::String,
            required,
            enum_values: None,
            default: None,
            target: None,
            items: None,
            on_delete: None,
        }
    }

    #[test]
    fn test_generate_collection_struct() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), make_string_field(true));
        fields.insert("email".to_string(), make_string_field(true));

        let collection = CollectionDefinition {
            path: "users/{name}.md".to_string(),
            fields,
            content: false,
            additional_properties: false,
            strict: true,
            readonly: false,
            on_delete: None,
            id: None,
            records: None,
        };

        let tokens = generate_collection_struct("users", &collection, &[]);
        let code = tokens.to_string();

        assert!(code.contains("pub struct User"));
        assert!(code.contains("pub name : String"));
        assert!(code.contains("pub email : String"));
    }

    #[test]
    fn test_generate_partial_struct() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), make_string_field(true));
        fields.insert("email".to_string(), make_string_field(true));

        let collection = CollectionDefinition {
            path: "users/{name}.md".to_string(),
            fields,
            content: false,
            additional_properties: false,
            strict: true,
            readonly: false,
            on_delete: None,
            id: None,
            records: None,
        };

        let tokens = generate_partial_struct("users", &collection, &[]);
        let code = tokens.to_string();

        assert!(code.contains("pub struct UserPartial"));
        assert!(code.contains("Option < String >"));
    }
}
