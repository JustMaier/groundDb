use grounddb::schema::{FieldType, SchemaDefinition};
use heck::ToPascalCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::type_utils::{view_params_name, view_row_name};

/// A parsed SELECT column from a SQL query.
#[derive(Debug, Clone)]
struct SelectColumn {
    /// The table alias (e.g., "p" from "p.title")
    table_alias: Option<String>,
    /// The original column name (e.g., "title" from "p.title")
    column_name: String,
    /// The output alias (e.g., "author_name" from "u.name AS author_name")
    output_name: String,
}

/// A parsed FROM/JOIN clause mapping table aliases to collection names.
#[derive(Debug, Clone)]
struct TableRef {
    collection_name: String,
    alias: Option<String>,
}

/// Generate view row structs and param structs for all views.
pub fn generate_views(schema: &SchemaDefinition) -> TokenStream {
    let mut tokens = TokenStream::new();

    let mut views: Vec<_> = schema.views.iter().collect();
    views.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (view_name, view_def) in &views {
        let row_tokens = generate_view_row_struct(view_name, &view_def.query, schema);
        tokens.extend(row_tokens);

        // Generate params struct if this view has parameters
        if let Some(ref params) = view_def.params {
            let params_tokens = generate_view_params_struct(view_name, params);
            tokens.extend(params_tokens);
        }
    }

    tokens
}

/// Generate a view row struct from a SQL query.
fn generate_view_row_struct(
    view_name: &str,
    query: &str,
    schema: &SchemaDefinition,
) -> TokenStream {
    let struct_name = view_row_name(view_name);
    let struct_ident = format_ident!("{}", struct_name);

    let columns = parse_select_columns(query);
    let table_refs = parse_table_refs(query);

    let field_tokens: Vec<_> = columns
        .iter()
        .map(|col| {
            let field_ident = format_ident!("{}", &col.output_name);
            let ty = resolve_column_type(col, &table_refs, schema);
            quote! {
                pub #field_ident: #ty,
            }
        })
        .collect();

    let doc_comment = format!(" Row type for the `{}` view.", view_name);

    quote! {
        #[doc = #doc_comment]
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct #struct_ident {
            #(#field_tokens)*
        }
    }
}

/// Generate a params struct for a parameterized view.
fn generate_view_params_struct(
    view_name: &str,
    params: &std::collections::HashMap<String, grounddb::schema::ParamDefinition>,
) -> TokenStream {
    let struct_name = view_params_name(view_name);
    let struct_ident = format_ident!("{}", struct_name);

    let mut param_entries: Vec<_> = params.iter().collect();
    param_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let field_tokens: Vec<_> = param_entries
        .iter()
        .map(|(param_name, param_def)| {
            let field_ident = format_ident!("{}", param_name);
            let ty = param_type_to_rust(&param_def.param_type);
            quote! {
                pub #field_ident: #ty,
            }
        })
        .collect();

    quote! {
        #[derive(Debug, Clone)]
        pub struct #struct_ident {
            #(#field_tokens)*
        }
    }
}

/// Parse SELECT columns from a SQL query.
/// Handles: SELECT col, alias.col, col AS name, alias.col AS name
fn parse_select_columns(query: &str) -> Vec<SelectColumn> {
    let query_upper = query.to_uppercase();
    let select_start = match query_upper.find("SELECT") {
        Some(pos) => pos + 6,
        None => return vec![],
    };

    let from_start = match query_upper.find("FROM") {
        Some(pos) => pos,
        None => return vec![],
    };

    let select_clause = &query[select_start..from_start].trim();

    select_clause
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            Some(parse_single_column(part))
        })
        .collect()
}

/// Parse a single column expression like "p.title" or "u.name AS author_name".
fn parse_single_column(expr: &str) -> SelectColumn {
    // Check for AS alias (case-insensitive)
    let (col_expr, alias) = if let Some(as_pos) = expr
        .to_uppercase()
        .find(" AS ")
    {
        let col = expr[..as_pos].trim();
        let alias = expr[as_pos + 4..].trim();
        (col, Some(alias.to_string()))
    } else {
        (expr.trim(), None)
    };

    // Check for table.column
    if let Some(dot_pos) = col_expr.find('.') {
        let table = col_expr[..dot_pos].trim().to_string();
        let column = col_expr[dot_pos + 1..].trim().to_string();
        let output = alias.unwrap_or_else(|| column.clone());
        SelectColumn {
            table_alias: Some(table),
            column_name: column,
            output_name: output,
        }
    } else {
        let column = col_expr.to_string();
        let output = alias.unwrap_or_else(|| column.clone());
        SelectColumn {
            table_alias: None,
            column_name: column,
            output_name: output,
        }
    }
}

/// Parse FROM and JOIN clauses to build table alias -> collection name mapping.
fn parse_table_refs(query: &str) -> Vec<TableRef> {
    let mut refs = Vec::new();
    let query_upper = query.to_uppercase();

    // Parse FROM clause: "FROM collection alias" or "FROM collection"
    if let Some(from_pos) = query_upper.find("FROM") {
        let after_from = &query[from_pos + 4..];
        if let Some(table_ref) = parse_table_ref_token(after_from) {
            refs.push(table_ref);
        }
    }

    // Parse JOIN clauses
    let mut search_pos = 0;
    while let Some(join_pos) = query_upper[search_pos..].find("JOIN") {
        let abs_pos = search_pos + join_pos + 4;
        let after_join = &query[abs_pos..];
        if let Some(table_ref) = parse_table_ref_token(after_join) {
            refs.push(table_ref);
        }
        search_pos = abs_pos;
    }

    refs
}

/// Parse a table reference token like "posts p" or "users".
fn parse_table_ref_token(text: &str) -> Option<TableRef> {
    let text = text.trim();
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.is_empty() {
        return None;
    }

    let collection_name = words[0].to_lowercase();

    // Second word is the alias if it's not a SQL keyword
    let alias = words.get(1).and_then(|w| {
        let upper = w.to_uppercase();
        if ["ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "WHERE", "ORDER", "GROUP", "HAVING", "LIMIT"].contains(&upper.as_str()) {
            None
        } else {
            Some(w.to_lowercase())
        }
    });

    Some(TableRef {
        collection_name,
        alias,
    })
}

/// Resolve a column's Rust type by looking up the field in the schema.
fn resolve_column_type(
    col: &SelectColumn,
    table_refs: &[TableRef],
    schema: &SchemaDefinition,
) -> TokenStream {
    // Find the collection name for this column
    let collection_name = if let Some(ref alias) = col.table_alias {
        table_refs
            .iter()
            .find(|r| r.alias.as_deref() == Some(alias.as_str()) || r.collection_name == *alias)
            .map(|r| r.collection_name.clone())
    } else {
        // No alias - use the first (FROM) table
        table_refs.first().map(|r| r.collection_name.clone())
    };

    let collection_name = match collection_name {
        Some(name) => name,
        None => return quote! { String }, // fallback
    };

    // Check implicit fields first
    match col.column_name.as_str() {
        "id" => return quote! { String },
        "created_at" => return quote! { chrono::DateTime<chrono::Utc> },
        "modified_at" => return quote! { chrono::DateTime<chrono::Utc> },
        "content" => return quote! { Option<String> },
        _ => {}
    }

    // Look up the field in the collection
    let collection = match schema.collections.get(&collection_name) {
        Some(c) => c,
        None => return quote! { String }, // fallback
    };

    let field_def = match collection.fields.get(&col.column_name) {
        Some(f) => f,
        None => return quote! { String }, // fallback for unknown fields
    };

    // Map field type to Rust type (simplified for views - no Option wrapping)
    match &field_def.field_type {
        FieldType::String => {
            if field_def.enum_values.is_some() {
                let enum_name = crate::type_utils::enum_type_name(&collection_name, &col.column_name);
                let ident = format_ident!("{}", enum_name);
                quote! { #ident }
            } else {
                quote! { String }
            }
        }
        FieldType::Number => quote! { f64 },
        FieldType::Boolean => quote! { bool },
        FieldType::Date => quote! { chrono::NaiveDate },
        FieldType::Datetime => quote! { chrono::DateTime<chrono::Utc> },
        FieldType::List => quote! { Vec<String> },
        FieldType::Object => quote! { serde_json::Value },
        FieldType::Ref => quote! { String },
        FieldType::Custom(type_name) => {
            let ident = format_ident!("{}", type_name.to_pascal_case());
            quote! { #ident }
        }
    }
}

/// Convert a param type string to a Rust type.
fn param_type_to_rust(param_type: &str) -> TokenStream {
    match param_type {
        "string" => quote! { String },
        "number" => quote! { f64 },
        "boolean" => quote! { bool },
        "date" => quote! { chrono::NaiveDate },
        "datetime" => quote! { chrono::DateTime<chrono::Utc> },
        _ => quote! { String },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_columns_simple() {
        let sql = "SELECT id, name, email FROM users ORDER BY name ASC";
        let cols = parse_select_columns(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].output_name, "id");
        assert_eq!(cols[1].output_name, "name");
        assert_eq!(cols[2].output_name, "email");
    }

    #[test]
    fn test_parse_select_columns_with_aliases() {
        let sql = "SELECT p.title, p.date, u.name AS author_name FROM posts p JOIN users u ON p.author_id = u.id";
        let cols = parse_select_columns(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].table_alias.as_deref(), Some("p"));
        assert_eq!(cols[0].column_name, "title");
        assert_eq!(cols[0].output_name, "title");
        assert_eq!(cols[2].output_name, "author_name");
    }

    #[test]
    fn test_parse_table_refs() {
        let sql = "SELECT p.title FROM posts p JOIN users u ON p.author_id = u.id WHERE p.status = 'published'";
        let refs = parse_table_refs(sql);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].collection_name, "posts");
        assert_eq!(refs[0].alias.as_deref(), Some("p"));
        assert_eq!(refs[1].collection_name, "users");
        assert_eq!(refs[1].alias.as_deref(), Some("u"));
    }

    #[test]
    fn test_parse_table_refs_no_alias() {
        let sql = "SELECT id, name FROM users ORDER BY name ASC";
        let refs = parse_table_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].collection_name, "users");
        assert_eq!(refs[0].alias, None);
    }
}
