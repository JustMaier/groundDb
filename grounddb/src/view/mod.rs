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

/// Parsed information about a SQL view query
#[derive(Debug, Clone)]
pub struct ParsedView {
    pub name: String,
    /// Collections referenced by the query (from FROM and JOIN clauses)
    pub referenced_collections: HashSet<String>,
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

/// A column in a view result
#[derive(Debug, Clone)]
pub struct ViewColumn {
    pub name: String,
    pub source_collection: Option<String>,
    pub source_field: Option<String>,
}

/// The view engine maintains view state and rebuilds views from the document index
pub struct ViewEngine {
    views: HashMap<String, ParsedView>,
    view_data: HashMap<String, Vec<serde_json::Value>>,
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
            view_data: HashMap::new(),
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
            .filter(|(_, v)| v.referenced_collections.contains(collection))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Load cached view data from the system database
    pub fn load_from_db(&mut self, db: &SystemDb) -> Result<()> {
        for name in self.views.keys() {
            if let Some(json_str) = db.get_view_data(name)? {
                let data: Vec<serde_json::Value> = serde_json::from_str(&json_str)?;
                self.view_data.insert(name.clone(), data);
            }
        }
        Ok(())
    }

    /// Save view data to the system database
    pub fn save_to_db(&self, db: &SystemDb) -> Result<()> {
        for (name, data) in &self.view_data {
            let json_str = serde_json::to_string(data)?;
            db.set_view_data(name, &json_str)?;
        }
        Ok(())
    }

    /// Get the current data for a static view
    pub fn get_view_data(&self, name: &str) -> Option<&Vec<serde_json::Value>> {
        self.view_data.get(name)
    }

    /// Materialize views to the views/ directory as YAML files
    pub fn materialize_views(&self, root: &Path) -> Result<()> {
        let views_dir = root.join("views");

        for (name, parsed) in &self.views {
            if parsed.materialize {
                if let Some(data) = self.view_data.get(name) {
                    std::fs::create_dir_all(&views_dir)?;
                    let output_path = views_dir.join(format!("{name}.yaml"));

                    // Apply limit for materialized output (buffer has more data)
                    let limited_data: Vec<&serde_json::Value> = if let Some(limit) = parsed.limit {
                        data.iter().take(limit as usize).collect()
                    } else {
                        data.iter().collect()
                    };

                    let yaml = serde_yaml::to_string(&limited_data)?;
                    std::fs::write(&output_path, &yaml)?;
                }
            }
        }

        Ok(())
    }
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
    let mut referenced_collections = HashSet::new();
    let mut columns = Vec::new();
    let mut limit = None;

    if let Statement::Query(query) = stmt {
        extract_from_query(query, &mut referenced_collections, &mut columns, &mut limit);
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
        referenced_collections,
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
    collections: &mut HashSet<String>,
    columns: &mut Vec<ViewColumn>,
    limit: &mut Option<u64>,
) {
    if let SetExpr::Select(select) = query.body.as_ref() {
        extract_from_select(select, collections, columns);
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
    collections: &mut HashSet<String>,
    columns: &mut Vec<ViewColumn>,
) {
    // Extract FROM tables
    for table in &select.from {
        extract_from_table_with_joins(table, collections);
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
    collections: &mut HashSet<String>,
) {
    extract_table_name(&table_with_joins.relation, collections);

    for join in &table_with_joins.joins {
        extract_table_name(&join.relation, collections);
    }
}

/// Extract a table name from a table factor
fn extract_table_name(factor: &TableFactor, collections: &mut HashSet<String>) {
    if let TableFactor::Table { name, .. } = factor {
        let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
        if !table_name.is_empty() {
            collections.insert(table_name);
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
        assert!(feed.referenced_collections.contains("posts"));
        assert!(feed.referenced_collections.contains("users"));
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
        assert!(lookup.referenced_collections.contains("users"));
        assert_eq!(lookup.referenced_collections.len(), 1);
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
}
