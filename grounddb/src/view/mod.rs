use crate::error::{GroundDbError, Result};
use crate::schema::{SchemaDefinition, ViewDefinition, ViewType};
use crate::system_db::SystemDb;
use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;

/// A reference to a table/collection in a FROM or JOIN clause, with optional alias.
#[derive(Debug, Clone)]
pub struct TableRef {
    pub collection: String,
    pub alias: Option<String>,
}

/// Parsed information about a SQL view query
#[derive(Debug, Clone)]
pub struct ParsedView {
    pub name: String,
    /// The original SQL text from the schema
    pub original_sql: String,
    /// Table references with aliases from FROM and JOIN clauses
    pub table_refs: Vec<TableRef>,
    /// Column aliases in the result
    pub columns: Vec<ViewColumn>,
    /// Whether the view has a LIMIT clause
    pub limit: Option<u64>,
    /// Buffer multiplier (e.g., 2.0 for "2x")
    pub buffer_multiplier: f64,
    /// Whether to materialize this view
    pub materialize: bool,
    /// Whether this is a parameterized query template
    pub is_query_template: bool,
    /// Parameter names for query templates
    pub param_names: Vec<String>,
}

impl ParsedView {
    /// Get the set of collection names referenced by this view.
    pub fn referenced_collections(&self) -> HashSet<String> {
        self.table_refs.iter().map(|r| r.collection.clone()).collect()
    }
}

/// A column in a view result
#[derive(Debug, Clone)]
pub struct ViewColumn {
    pub name: String,
    pub source_collection: Option<String>,
    pub source_field: Option<String>,
}

/// The view engine maintains view state and rebuilds views from the document index.
/// Uses Mutex on view_data so the cache can be updated from shared (&self) references.
pub struct ViewEngine {
    views: HashMap<String, ParsedView>,
    view_data: Mutex<HashMap<String, Vec<serde_json::Value>>>,
}

impl ViewEngine {
    /// Create a new view engine from schema view definitions
    pub fn new(schema: &SchemaDefinition) -> Result<Self> {
        let mut views = HashMap::new();

        for (name, view_def) in &schema.views {
            let parsed = parse_view_query(name, view_def)?;
            views.insert(name.clone(), parsed);
        }

        Ok(ViewEngine {
            views,
            view_data: Mutex::new(HashMap::new()),
        })
    }

    /// Get the parsed view metadata
    pub fn get_view(&self, name: &str) -> Option<&ParsedView> {
        self.views.get(name)
    }

    /// Check which views are affected by a change in the given collection
    pub fn affected_views(&self, collection: &str) -> Vec<&str> {
        self.views
            .iter()
            .filter(|(_, v)| v.referenced_collections().contains(collection))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Load cached view data from the system database
    pub fn load_from_db(&self, db: &SystemDb) -> Result<()> {
        let mut cache = self.view_data.lock().unwrap();
        for name in self.views.keys() {
            if let Some(json_str) = db.get_view_data(name)? {
                let data: Vec<serde_json::Value> = serde_json::from_str(&json_str)?;
                cache.insert(name.clone(), data);
            }
        }
        Ok(())
    }

    /// Save view data to the system database
    pub fn save_to_db(&self, db: &SystemDb) -> Result<()> {
        let cache = self.view_data.lock().unwrap();
        for (name, data) in cache.iter() {
            let json_str = serde_json::to_string(data)?;
            db.set_view_data(name, &json_str)?;
        }
        Ok(())
    }

    /// Get a clone of the current data for a static view
    pub fn get_view_data(&self, name: &str) -> Option<Vec<serde_json::Value>> {
        let cache = self.view_data.lock().unwrap();
        cache.get(name).cloned()
    }

    /// Update the cached data for a view
    pub fn set_view_data(&self, name: &str, data: Vec<serde_json::Value>) {
        let mut cache = self.view_data.lock().unwrap();
        cache.insert(name.to_string(), data);
    }

    /// Materialize a single view to the views/ directory as a YAML file.
    pub fn materialize_view(&self, root: &Path, view_name: &str) -> Result<()> {
        let parsed = match self.views.get(view_name) {
            Some(p) if p.materialize => p,
            _ => return Ok(()),
        };

        let cache = self.view_data.lock().unwrap();
        if let Some(data) = cache.get(view_name) {
            let views_dir = root.join("views");
            std::fs::create_dir_all(&views_dir)?;
            let output_path = views_dir.join(format!("{view_name}.yaml"));

            // Apply limit for materialized output (buffer has more data)
            let limited_data: Vec<&serde_json::Value> = if let Some(limit) = parsed.limit {
                data.iter().take(limit as usize).collect()
            } else {
                data.iter().collect()
            };

            let yaml = serde_yaml::to_string(&limited_data)?;
            std::fs::write(&output_path, &yaml)?;
        }

        Ok(())
    }

    /// Materialize all materialized views to the views/ directory as YAML files.
    pub fn materialize_views(&self, root: &Path) -> Result<()> {
        let view_names: Vec<String> = self.views.keys().cloned().collect();
        for name in &view_names {
            self.materialize_view(root, name)?;
        }
        Ok(())
    }
}

/// Rewritten SQL query ready for execution against the documents table.
#[derive(Debug, Clone)]
pub struct RewrittenQuery {
    /// The CTE-wrapped SQL ready for rusqlite execution
    pub sql: String,
    /// Ordered parameter names for binding (e.g., ["post_id"])
    pub param_names: Vec<String>,
    /// limit * buffer_multiplier — used for buffered views
    pub buffer_limit: Option<usize>,
    /// The original LIMIT from the user's SQL
    pub original_limit: Option<usize>,
}

/// Rewrite a parsed view's SQL into a CTE-wrapped query against the `documents` table.
///
/// For each collection referenced in the view, generates a CTE that extracts
/// all schema-defined fields from `data_json` via `json_extract()`. The user's
/// original SQL is appended verbatim after the CTEs.
pub fn rewrite_view_sql(
    parsed: &ParsedView,
    schema: &SchemaDefinition,
) -> Result<RewrittenQuery> {
    let mut cte_parts = Vec::new();

    for table_ref in &parsed.table_refs {
        let collection_name = &table_ref.collection;
        let col_def = schema.collections.get(collection_name);
        if col_def.is_none() {
            return Err(GroundDbError::SqlParse(format!(
                "View '{}': referenced collection '{}' not found in schema",
                parsed.name, collection_name
            )));
        }
        let col_def = col_def.unwrap();

        // Build SELECT columns for this CTE
        let mut cte_columns = Vec::new();

        // Implicit fields: id, created_at, modified_at are direct columns
        cte_columns.push("id".to_string());
        cte_columns.push("created_at".to_string());
        cte_columns.push("modified_at".to_string());

        // If collection has content: true, expose content_text as "content"
        if col_def.content {
            cte_columns.push("content_text AS content".to_string());
        }

        // Schema-defined fields extracted via json_extract
        for (field_name, _field_def) in &col_def.fields {
            cte_columns.push(format!(
                "json_extract(data_json, '$.{field_name}') AS {field_name}"
            ));
        }

        let columns_sql = cte_columns.join(",\n      ");
        let cte = format!(
            "{collection_name} AS (\n    SELECT\n      {columns_sql}\n    FROM documents\n    WHERE collection = '{collection_name}'\n  )"
        );
        cte_parts.push(cte);
    }

    // Build the final SQL
    let original_sql = parsed.original_sql.trim();

    let full_sql = if cte_parts.is_empty() {
        original_sql.to_string()
    } else {
        format!("WITH {}\n{}", cte_parts.join(",\n  "), original_sql)
    };

    // Calculate buffer limit
    let buffer_limit = parsed.limit.map(|l| {
        (l as f64 * parsed.buffer_multiplier).ceil() as usize
    });

    log::debug!(
        "View '{}' rewritten SQL:\n{}",
        parsed.name,
        full_sql
    );

    Ok(RewrittenQuery {
        sql: full_sql,
        param_names: parsed.param_names.clone(),
        buffer_limit,
        original_limit: parsed.limit.map(|l| l as usize),
    })
}

/// Parse a SQL view query to extract metadata (referenced collections, columns, etc.)
fn parse_view_query(name: &str, view_def: &ViewDefinition) -> Result<ParsedView> {
    // Replace :param placeholders with NULL for parsing purposes
    let sql = view_def.query.trim().to_string();
    let clean_sql = replace_params(&sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &clean_sql)
        .map_err(|e| GroundDbError::SqlParse(format!("View '{name}': {e}")))?;

    if statements.is_empty() {
        return Err(GroundDbError::SqlParse(format!(
            "View '{name}': no SQL statements found"
        )));
    }

    let stmt = &statements[0];
    let mut table_refs = Vec::new();
    let mut columns = Vec::new();
    let mut limit = None;

    if let Statement::Query(query) = stmt {
        extract_from_query(query, &mut table_refs, &mut columns, &mut limit);
    }

    // Parse buffer multiplier
    let buffer_multiplier = view_def
        .buffer
        .as_ref()
        .and_then(|b| {
            b.strip_suffix('x')
                .and_then(|n| n.parse::<f64>().ok())
        })
        .unwrap_or(1.0);

    // Determine if this is a query template
    let is_query_template = view_def.view_type == Some(ViewType::Query);
    let param_names = view_def
        .params
        .as_ref()
        .map(|p| p.keys().cloned().collect())
        .unwrap_or_default();

    Ok(ParsedView {
        name: name.to_string(),
        original_sql: sql,
        table_refs,
        columns,
        limit,
        buffer_multiplier,
        materialize: view_def.materialize,
        is_query_template,
        param_names,
    })
}

/// Replace :param placeholders in SQL with NULL for parsing
fn replace_params(sql: &str) -> String {
    let mut result = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == ':' {
            // Check if it's a parameter (followed by alphanumeric/underscore)
            if chars.peek().map(|ch| ch.is_alphabetic() || *ch == '_').unwrap_or(false) {
                // Consume the parameter name
                while chars
                    .peek()
                    .map(|ch| ch.is_alphanumeric() || *ch == '_')
                    .unwrap_or(false)
                {
                    chars.next();
                }
                result.push_str("NULL");
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Extract metadata from a parsed SQL query
fn extract_from_query(
    query: &Query,
    table_refs: &mut Vec<TableRef>,
    columns: &mut Vec<ViewColumn>,
    limit: &mut Option<u64>,
) {
    if let SetExpr::Select(select) = query.body.as_ref() {
        extract_from_select(select, table_refs, columns);
    }

    // Extract LIMIT
    if let Some(expr) = &query.limit {
        if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = expr {
            if let Ok(l) = n.parse::<u64>() {
                *limit = Some(l);
            }
        }
    }
}

/// Extract metadata from a SELECT clause
fn extract_from_select(
    select: &Select,
    table_refs: &mut Vec<TableRef>,
    columns: &mut Vec<ViewColumn>,
) {
    // Extract FROM tables
    for table in &select.from {
        extract_from_table_with_joins(table, table_refs);
    }

    // Extract columns
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let (col_name, source_col, source_field) = extract_column_info(expr);
                columns.push(ViewColumn {
                    name: col_name,
                    source_collection: source_col,
                    source_field,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let (_, source_col, source_field) = extract_column_info(expr);
                columns.push(ViewColumn {
                    name: alias.value.clone(),
                    source_collection: source_col,
                    source_field,
                });
            }
            SelectItem::Wildcard(_) => {
                columns.push(ViewColumn {
                    name: "*".to_string(),
                    source_collection: None,
                    source_field: None,
                });
            }
            _ => {}
        }
    }
}

/// Extract table/collection names from FROM and JOIN clauses
fn extract_from_table_with_joins(
    table_with_joins: &TableWithJoins,
    table_refs: &mut Vec<TableRef>,
) {
    extract_table_name(&table_with_joins.relation, table_refs);

    for join in &table_with_joins.joins {
        extract_table_name(&join.relation, table_refs);
    }
}

/// Extract a table name and alias from a table factor
fn extract_table_name(
    factor: &TableFactor,
    table_refs: &mut Vec<TableRef>,
) {
    if let TableFactor::Table { name, alias, .. } = factor {
        let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
        if !table_name.is_empty() {
            let alias_name = alias.as_ref().map(|a| a.name.value.clone());
            table_refs.push(TableRef {
                collection: table_name,
                alias: alias_name,
            });
        }
    }
}

/// Extract column information from an expression
fn extract_column_info(expr: &Expr) -> (String, Option<String>, Option<String>) {
    match expr {
        Expr::Identifier(ident) => (ident.value.clone(), None, Some(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                (
                    parts[1].value.clone(),
                    Some(parts[0].value.clone()),
                    Some(parts[1].value.clone()),
                )
            } else {
                let name = parts.last().map(|p| p.value.clone()).unwrap_or_default();
                (name, None, None)
            }
        }
        _ => (format!("{expr}"), None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema_str;

    fn test_schema() -> SchemaDefinition {
        parse_schema_str(
            r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true }
      date: { type: date, required: true }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true

views:
  post_feed:
    query: |
      SELECT p.title, p.date, u.name AS author_name
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

  post_comments:
    type: query
    query: |
      SELECT c.id, c.created_at
      FROM posts c
      WHERE c.id = :post_id
      ORDER BY c.created_at ASC
    params:
      post_id: { type: string }
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_view_engine_creation() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        assert_eq!(engine.views.len(), 3);
        assert!(engine.views.contains_key("post_feed"));
        assert!(engine.views.contains_key("user_lookup"));
        assert!(engine.views.contains_key("post_comments"));
    }

    #[test]
    fn test_post_feed_view_parsing() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let feed = engine.get_view("post_feed").unwrap();
        let feed_collections = feed.referenced_collections();
        assert!(feed_collections.contains("posts"));
        assert!(feed_collections.contains("users"));
        assert_eq!(feed.limit, Some(100));
        assert_eq!(feed.buffer_multiplier, 2.0);
        assert!(feed.materialize);
        assert!(!feed.is_query_template);
        assert_eq!(feed.columns.len(), 3);
    }

    #[test]
    fn test_user_lookup_view_parsing() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let lookup = engine.get_view("user_lookup").unwrap();
        let lookup_collections = lookup.referenced_collections();
        assert!(lookup_collections.contains("users"));
        assert_eq!(lookup_collections.len(), 1);
        assert!(lookup.materialize);
        assert_eq!(lookup.limit, None);
    }

    #[test]
    fn test_query_template_parsing() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let comments = engine.get_view("post_comments").unwrap();
        assert!(comments.is_query_template);
        assert!(comments.param_names.contains(&"post_id".to_string()));
    }

    #[test]
    fn test_affected_views() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let affected = engine.affected_views("posts");
        assert!(affected.contains(&"post_feed"));
        assert!(affected.contains(&"post_comments"));

        let affected_users = engine.affected_views("users");
        assert!(affected_users.contains(&"post_feed"));
        assert!(affected_users.contains(&"user_lookup"));
    }

    #[test]
    fn test_replace_params() {
        let sql = "SELECT * FROM posts WHERE id = :post_id AND status = :status";
        let cleaned = replace_params(sql);
        assert_eq!(
            cleaned,
            "SELECT * FROM posts WHERE id = NULL AND status = NULL"
        );
    }

    // ── Phase 5: rewrite_view_sql unit tests ──

    #[test]
    fn test_rewrite_simple_select() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let view = engine.get_view("user_lookup").unwrap();
        let rewritten = rewrite_view_sql(view, &schema).unwrap();

        // Should contain a CTE for users
        assert!(rewritten.sql.contains("WITH users AS"));
        // Should contain json_extract for schema fields
        assert!(rewritten.sql.contains("json_extract(data_json, '$.name') AS name"));
        assert!(rewritten.sql.contains("json_extract(data_json, '$.email') AS email"));
        assert!(rewritten.sql.contains("json_extract(data_json, '$.role') AS role"));
        // Should contain the WHERE collection filter
        assert!(rewritten.sql.contains("WHERE collection = 'users'"));
        // Should contain implicit fields
        assert!(rewritten.sql.contains("id"));
        assert!(rewritten.sql.contains("created_at"));
        assert!(rewritten.sql.contains("modified_at"));
        // No buffer since no limit
        assert!(rewritten.buffer_limit.is_none());
        assert!(rewritten.original_limit.is_none());
    }

    #[test]
    fn test_rewrite_join_query() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let view = engine.get_view("post_feed").unwrap();
        let rewritten = rewrite_view_sql(view, &schema).unwrap();

        // Should contain CTEs for both posts and users
        assert!(rewritten.sql.contains("posts AS"));
        assert!(rewritten.sql.contains("users AS"));
        // Should contain the original SQL after CTEs
        assert!(rewritten.sql.contains("JOIN"));
        assert!(rewritten.sql.contains("p.author_id = u.id"));
        assert!(rewritten.sql.contains("p.status = 'published'"));
        assert!(rewritten.sql.contains("ORDER BY p.date DESC"));
        // Buffer should be 200 (100 * 2x)
        assert_eq!(rewritten.buffer_limit, Some(200));
        assert_eq!(rewritten.original_limit, Some(100));
    }

    #[test]
    fn test_rewrite_preserves_implicit_fields() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let view = engine.get_view("user_lookup").unwrap();
        let rewritten = rewrite_view_sql(view, &schema).unwrap();

        // id, created_at, modified_at should be direct columns (not json_extract)
        let cte_start = rewritten.sql.find("users AS").unwrap();
        let cte_section = &rewritten.sql[cte_start..];
        // These should appear as direct column references, not via json_extract
        assert!(!cte_section.contains("json_extract(data_json, '$.id')"));
        assert!(!cte_section.contains("json_extract(data_json, '$.created_at')"));
    }

    #[test]
    fn test_rewrite_content_collection() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let view = engine.get_view("post_feed").unwrap();
        let rewritten = rewrite_view_sql(view, &schema).unwrap();

        // Posts have content: true, so should expose content_text AS content
        let posts_cte_start = rewritten.sql.find("posts AS").unwrap();
        let posts_section = &rewritten.sql[posts_cte_start..];
        assert!(posts_section.contains("content_text AS content"));
    }

    #[test]
    fn test_rewrite_parameterized_query() {
        let schema = test_schema();
        let engine = ViewEngine::new(&schema).unwrap();

        let view = engine.get_view("post_comments").unwrap();
        let rewritten = rewrite_view_sql(view, &schema).unwrap();

        // Should contain the :post_id parameter in the SQL
        assert!(rewritten.sql.contains(":post_id"));
        assert!(rewritten.param_names.contains(&"post_id".to_string()));
    }

    #[test]
    fn test_rewrite_unknown_collection_errors() {
        let schema = test_schema();

        let parsed = ParsedView {
            name: "bad_view".to_string(),
            original_sql: "SELECT * FROM nonexistent".to_string(),
            table_refs: vec![TableRef {
                collection: "nonexistent".to_string(),
                alias: None,
            }],
            columns: vec![],
            limit: None,
            buffer_multiplier: 1.0,
            materialize: false,
            is_query_template: false,
            param_names: vec![],
        };

        let result = rewrite_view_sql(&parsed, &schema);
        assert!(result.is_err());
    }
}
