use crate::error::{GroundDbError, Result};
use chrono::NaiveDate;

use std::collections::{HashMap, HashSet};

/// A parsed path template with segments for interpolation
#[derive(Debug, Clone)]
pub struct PathTemplate {
    pub raw: String,
    pub segments: Vec<PathSegment>,
}

/// A single segment of a path template
#[derive(Debug, Clone, PartialEq)]
pub enum PathSegment {
    /// A literal string (e.g., "posts/", "/", ".md")
    Literal(String),
    /// A field reference, optionally with a format specifier (e.g., {title}, {date:YYYY-MM-DD})
    Field { name: String, format: Option<String> },
    /// A nested field reference for refs (e.g., {parent:type}, {parent:id}, {user:id})
    NestedField { parent: String, child: String },
}

impl PathTemplate {
    /// Parse a path template string into structured segments.
    ///
    /// Handles:
    /// - Literal text: "posts/"
    /// - Simple fields: {title}
    /// - Fields with date format: {date:YYYY-MM-DD}
    /// - Nested ref fields: {parent:type}, {parent:id}, {user:id}
    /// - The implicit {id} field
    pub fn parse(template: &str) -> Result<Self> {
        let mut segments = Vec::new();
        let mut remaining = template;

        while !remaining.is_empty() {
            if let Some(start) = remaining.find('{') {
                // Push any literal text before the {
                if start > 0 {
                    segments.push(PathSegment::Literal(remaining[..start].to_string()));
                }

                // Find the matching }
                let end = remaining[start..]
                    .find('}')
                    .ok_or_else(|| {
                        GroundDbError::Schema(format!(
                            "Unclosed '{{' in path template: {template}"
                        ))
                    })?
                    + start;

                let field_expr = &remaining[start + 1..end];
                if field_expr.is_empty() {
                    return Err(GroundDbError::Schema(format!(
                        "Empty field reference '{{}}' in path template: {template}"
                    )));
                }

                let segment = parse_field_expr(field_expr);
                segments.push(segment);
                remaining = &remaining[end + 1..];
            } else {
                // Rest is all literal
                segments.push(PathSegment::Literal(remaining.to_string()));
                break;
            }
        }

        Ok(PathTemplate {
            raw: template.to_string(),
            segments,
        })
    }

    /// Render the template with the given front matter values and optional document id.
    /// Returns the complete file path.
    pub fn render(&self, fields: &serde_yaml::Value, id: Option<&str>) -> Result<String> {
        let mut result = String::new();

        for segment in &self.segments {
            match segment {
                PathSegment::Literal(s) => result.push_str(s),
                PathSegment::Field { name, format } => {
                    let raw_value = if name == "id" {
                        if let Some(id) = id {
                            serde_yaml::Value::String(id.to_string())
                        } else {
                            get_yaml_field(fields, name)?
                        }
                    } else if name == "created_at" || name == "modified_at" {
                        // Implicit fields may be provided in the value map
                        get_yaml_field(fields, name)?
                    } else {
                        get_yaml_field(fields, name)?
                    };
                    let rendered = format_value(&raw_value, format.as_deref())?;
                    result.push_str(&slugify(&rendered));
                }
                PathSegment::NestedField { parent, child } => {
                    let raw_value = get_nested_yaml_field(fields, parent, child)?;
                    let rendered = value_to_string(&raw_value)?;
                    result.push_str(&slugify(&rendered));
                }
            }
        }

        Ok(result)
    }

    /// Returns the set of field names referenced in this template.
    /// This is used to detect which fields are "path-relevant" -- meaning
    /// changes to these fields require file movement.
    pub fn referenced_fields(&self) -> HashSet<String> {
        let mut fields = HashSet::new();
        for segment in &self.segments {
            match segment {
                PathSegment::Field { name, .. } => {
                    fields.insert(name.clone());
                }
                PathSegment::NestedField { parent, .. } => {
                    fields.insert(parent.clone());
                }
                PathSegment::Literal(_) => {}
            }
        }
        fields
    }

    /// Returns true if the given field name is referenced in the path template
    pub fn references_field(&self, field_name: &str) -> bool {
        self.segments.iter().any(|s| match s {
            PathSegment::Field { name, .. } => name == field_name,
            PathSegment::NestedField { parent, .. } => parent == field_name,
            PathSegment::Literal(_) => false,
        })
    }

    /// Extract the collection base directory from the template
    /// (everything before the first field reference)
    pub fn base_directory(&self) -> String {
        let mut base = String::new();
        for segment in &self.segments {
            match segment {
                PathSegment::Literal(s) => base.push_str(s),
                _ => break,
            }
        }
        // Strip any trailing filename part (after last /)
        if let Some(pos) = base.rfind('/') {
            base[..=pos].to_string()
        } else {
            String::new()
        }
    }

    /// Reverse of `render()` — extract field values from a relative file path
    /// by matching it against the template segments.
    ///
    /// Returns `None` if the path doesn't match the template structure.
    /// Skips `NestedField` segments (consumes the text but doesn't include
    /// them in the result map).
    pub fn extract(&self, path: &str) -> Option<HashMap<String, String>> {
        let mut fields = HashMap::new();
        let mut remaining = path;

        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                PathSegment::Literal(lit) => {
                    if remaining.starts_with(lit.as_str()) {
                        remaining = &remaining[lit.len()..];
                    } else {
                        return None;
                    }
                }
                PathSegment::Field { name, format } => {
                    let value = self.extract_field_value(remaining, i, format.as_deref())?;
                    remaining = &remaining[value.len()..];
                    fields.insert(name.clone(), value);
                }
                PathSegment::NestedField { .. } => {
                    let value = self.extract_field_value(remaining, i, None)?;
                    remaining = &remaining[value.len()..];
                    // NestedField values are not stored
                }
            }
        }

        if remaining.is_empty() {
            Some(fields)
        } else {
            None
        }
    }

    /// Helper: extract a single field's value from `remaining`, given the
    /// segment index `idx` and an optional format specifier.
    ///
    /// For date-formatted fields, consumes exactly `format.len()` characters
    /// (the format string length equals the rendered output length).
    /// For plain fields, consumes text up to the next literal delimiter.
    fn extract_field_value(&self, remaining: &str, idx: usize, format: Option<&str>) -> Option<String> {
        // Date-formatted fields have a known fixed length
        if let Some(fmt) = format {
            let len = fmt.len();
            if remaining.len() >= len {
                return Some(remaining[..len].to_string());
            } else {
                return None;
            }
        }

        // Find the next literal delimiter after this field
        let delimiter = self.segments[idx + 1..]
            .iter()
            .find_map(|s| match s {
                PathSegment::Literal(lit) => Some(lit.as_str()),
                _ => None,
            });

        if let Some(delim) = delimiter {
            if let Some(pos) = remaining.find(delim) {
                Some(remaining[..pos].to_string())
            } else {
                None
            }
        } else {
            // Last field — consume the rest
            Some(remaining.to_string())
        }
    }
}

/// Parse a field expression inside {}.
///
/// Rules:
/// - "title" -> Field { name: "title", format: None }
/// - "date:YYYY-MM-DD" -> Field { name: "date", format: Some("YYYY-MM-DD") }
/// - "parent:type" -> NestedField { parent: "parent", child: "type" }
/// - "parent:id" -> NestedField { parent: "parent", child: "id" }
/// - "user:id" -> NestedField { parent: "user", child: "id" }
/// - "created_at:YYYY-MM-DDTHHMM" -> Field { name: "created_at", format: Some(...) }
///
/// The disambiguation rule: if the part after ":" looks like a date format
/// (contains Y, M, D, H, or uppercase characters typical of format strings),
/// treat it as a format specifier. If it's a simple word like "type" or "id",
/// treat it as a nested field. Exception: "id" after a colon is always a nested
/// field reference, since "id" is never a date format.
fn parse_field_expr(expr: &str) -> PathSegment {
    if let Some(colon_pos) = expr.find(':') {
        let left = &expr[..colon_pos];
        let right = &expr[colon_pos + 1..];

        if is_date_format(right) {
            PathSegment::Field {
                name: left.to_string(),
                format: Some(right.to_string()),
            }
        } else {
            // Nested field reference like {parent:type} or {user:id}
            PathSegment::NestedField {
                parent: left.to_string(),
                child: right.to_string(),
            }
        }
    } else {
        PathSegment::Field {
            name: expr.to_string(),
            format: None,
        }
    }
}

/// Determine if a string looks like a date format specifier.
/// Date formats contain characters like Y, M, D, H (uppercase) in sequences.
/// Simple field names like "type", "id" are lowercase alpha only.
fn is_date_format(s: &str) -> bool {
    // If it contains any date format characters (YMDHST), it's a format specifier
    let format_chars = ['Y', 'M', 'D', 'H', 'T', 'S'];
    s.chars().any(|c| format_chars.contains(&c))
}

/// Get a field value from a YAML value (expected to be a mapping)
fn get_yaml_field(value: &serde_yaml::Value, field: &str) -> Result<serde_yaml::Value> {
    match value {
        serde_yaml::Value::Mapping(map) => {
            map.get(serde_yaml::Value::String(field.to_string()))
                .cloned()
                .ok_or_else(|| {
                    GroundDbError::Validation(format!(
                        "Field '{field}' required by path template but not found in document"
                    ))
                })
        }
        _ => Err(GroundDbError::Validation(
            "Document data is not a YAML mapping".into(),
        )),
    }
}

/// Get a nested field value for ref fields.
/// For a polymorphic ref, the value is either a string (single-target ref) or a
/// mapping with "type" and "id" keys (multi-target ref).
fn get_nested_yaml_field(
    value: &serde_yaml::Value,
    parent: &str,
    child: &str,
) -> Result<serde_yaml::Value> {
    let parent_val = get_yaml_field(value, parent)?;

    match &parent_val {
        // For a polymorphic ref stored as {type: "posts", id: "abc"}
        serde_yaml::Value::Mapping(map) => {
            map.get(serde_yaml::Value::String(child.to_string()))
                .cloned()
                .ok_or_else(|| {
                    GroundDbError::Validation(format!(
                        "Nested field '{parent}:{child}' not found in ref value"
                    ))
                })
        }
        // For a simple string ref, "id" returns the string itself, "type" is unknown
        serde_yaml::Value::String(s) => {
            if child == "id" {
                Ok(serde_yaml::Value::String(s.clone()))
            } else {
                Err(GroundDbError::Validation(format!(
                    "Cannot access '{child}' on a simple ref value (string). \
                     Use a polymorphic ref (mapping with type/id) for '{parent}'"
                )))
            }
        }
        _ => Err(GroundDbError::Validation(format!(
            "Ref field '{parent}' is not a string or mapping"
        ))),
    }
}

/// Format a YAML value using an optional date format specifier
fn format_value(value: &serde_yaml::Value, format: Option<&str>) -> Result<String> {
    match format {
        Some(fmt) => {
            let date_str = value_to_string(value)?;
            format_date(&date_str, fmt)
        }
        None => value_to_string(value),
    }
}

/// Convert a YAML value to a string representation suitable for paths
fn value_to_string(value: &serde_yaml::Value) -> Result<String> {
    match value {
        serde_yaml::Value::String(s) => Ok(s.clone()),
        serde_yaml::Value::Number(n) => Ok(n.to_string()),
        serde_yaml::Value::Bool(b) => Ok(b.to_string()),
        serde_yaml::Value::Null => Ok(String::new()),
        _ => Err(GroundDbError::Validation(format!(
            "Cannot convert value to path string: {value:?}"
        ))),
    }
}

/// Format a date string according to a format specifier.
/// Input can be ISO date (2026-02-13) or datetime (2026-02-13T14:30:00).
/// Format: YYYY=year, MM=month, DD=day, HH=hour, MM(in time context)=minute, SS=second
fn format_date(date_str: &str, format: &str) -> Result<String> {
    // Try to parse as NaiveDate first, then NaiveDateTime
    if let Ok(date) = date_str.parse::<NaiveDate>() {
        let mut result = format.to_string();
        result = result.replace("YYYY", &format!("{:04}", date.format("%Y")));
        result = result.replace("MM", &format!("{:02}", date.format("%m")));
        result = result.replace("DD", &format!("{:02}", date.format("%d")));
        return Ok(result);
    }

    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S") {
        return Ok(format_datetime(dt, format));
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M") {
        return Ok(format_datetime(dt, format));
    }

    // Try parsing as a chrono DateTime<Utc>
    if let Ok(dt) = date_str.parse::<chrono::DateTime<chrono::Utc>>() {
        return Ok(format_datetime(dt.naive_utc(), format));
    }

    // Fall back to returning the raw string
    Ok(date_str.to_string())
}

fn format_datetime(dt: chrono::NaiveDateTime, format: &str) -> String {
    let mut result = format.to_string();
    result = result.replace("YYYY", &format!("{}", dt.format("%Y")));
    // Must replace HH before MM to avoid ambiguity (month vs minute)
    result = result.replace("HH", &format!("{}", dt.format("%H")));
    // After replacing HH, any remaining MM is for month in date context
    // If format has T preceding MM, the MM is minute; otherwise month
    // Simple approach: replace first MM with month, remaining with minute
    // Actually: in "YYYY-MM-DDTHHMM" the pattern is clear
    // We need a smarter approach: scan and replace
    result = replace_date_tokens(result, dt);
    result
}

/// Smart token replacement to handle the MM ambiguity (month vs minute)
fn replace_date_tokens(format: String, dt: chrono::NaiveDateTime) -> String {
    // Replace in order: YYYY, DD first (unambiguous), then handle MM/HH/SS
    let mut result = format;

    // If HH was already replaced, the remaining MM tokens are either month or minute
    // Strategy: tokenize and handle contextually
    // For simplicity: if the format still contains both date and time portions,
    // use positional replacement

    // First pass: replace DD and SS (unambiguous)
    result = result.replace("DD", &format!("{}", dt.format("%d")));
    result = result.replace("SS", &format!("{}", dt.format("%S")));

    // Now handle MM: the first occurrence before any T/space is month,
    // subsequent are minutes. But HH was already replaced so we can use
    // a simple regex to find the context.
    let month = format!("{}", dt.format("%m"));
    let minute = format!("{}", dt.format("%M"));

    // If there are two MM remaining, first is month, second is minute
    if let Some(first_pos) = result.find("MM") {
        let after_first = first_pos + 2;
        if result[after_first..].contains("MM") {
            // Two MMs: first = month, second = minute
            result = result.replacen("MM", &month, 1);
            result = result.replacen("MM", &minute, 1);
        } else {
            // Only one MM: determine by context
            // If HH appears before this MM (already replaced to digits), it's minute
            // Check if there are two digits followed by MM (HH pattern already replaced)
            let before = &result[..first_pos];
            if before.ends_with(|c: char| c.is_ascii_digit()) && before.len() >= 2 {
                let last_two = &before[before.len() - 2..];
                if last_two.chars().all(|c| c.is_ascii_digit()) {
                    // Likely time context (digits before MM = HH was replaced)
                    result = result.replacen("MM", &minute, 1);
                } else {
                    result = result.replacen("MM", &month, 1);
                }
            } else {
                // Default: month
                result = result.replacen("MM", &month, 1);
            }
        }
    }

    result
}

/// Slugify a string for use in file paths.
/// Lowercase, replace spaces/special chars with hyphens, strip non-alphanumeric.
pub fn slugify(input: &str) -> String {
    slug::slugify(input)
}

/// Resolve a path conflict by appending a suffix (-2, -3, etc.)
pub fn resolve_suffix(base_path: &str, exists_fn: impl Fn(&str) -> bool) -> String {
    if !exists_fn(base_path) {
        return base_path.to_string();
    }

    // Split at the last dot (extension)
    let (stem, ext) = if let Some(dot_pos) = base_path.rfind('.') {
        (&base_path[..dot_pos], &base_path[dot_pos..])
    } else {
        (base_path, "")
    };

    let mut counter = 2;
    loop {
        let candidate = format!("{stem}-{counter}{ext}");
        if !exists_fn(&candidate) {
            return candidate;
        }
        counter += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;

    #[test]
    fn test_parse_simple_template() {
        let t = PathTemplate::parse("users/{name}.md").unwrap();
        assert_eq!(t.segments.len(), 3);
        assert_eq!(t.segments[0], PathSegment::Literal("users/".to_string()));
        assert_eq!(
            t.segments[1],
            PathSegment::Field {
                name: "name".to_string(),
                format: None,
            }
        );
        assert_eq!(t.segments[2], PathSegment::Literal(".md".to_string()));
    }

    #[test]
    fn test_parse_template_with_date_format() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        assert_eq!(t.segments.len(), 7);
        assert_eq!(t.segments[0], PathSegment::Literal("posts/".to_string()));
        assert_eq!(
            t.segments[1],
            PathSegment::Field {
                name: "status".to_string(),
                format: None,
            }
        );
        assert_eq!(t.segments[2], PathSegment::Literal("/".to_string()));
        assert_eq!(
            t.segments[3],
            PathSegment::Field {
                name: "date".to_string(),
                format: Some("YYYY-MM-DD".to_string()),
            }
        );
        assert_eq!(t.segments[4], PathSegment::Literal("-".to_string()));
        assert_eq!(
            t.segments[5],
            PathSegment::Field {
                name: "title".to_string(),
                format: None,
            }
        );
        assert_eq!(t.segments[6], PathSegment::Literal(".md".to_string()));
    }

    #[test]
    fn test_parse_template_with_nested_refs() {
        let t = PathTemplate::parse(
            "comments/{parent:type}/{parent:id}/{user:id}-{created_at:YYYY-MM-DDTHHMM}.md",
        )
        .unwrap();

        let nested_fields: Vec<_> = t
            .segments
            .iter()
            .filter(|s| matches!(s, PathSegment::NestedField { .. }))
            .collect();
        assert_eq!(nested_fields.len(), 3); // parent:type, parent:id, user:id

        assert!(t.segments.contains(&PathSegment::NestedField {
            parent: "parent".to_string(),
            child: "type".to_string(),
        }));
        assert!(t.segments.contains(&PathSegment::NestedField {
            parent: "parent".to_string(),
            child: "id".to_string(),
        }));
        assert!(t.segments.contains(&PathSegment::NestedField {
            parent: "user".to_string(),
            child: "id".to_string(),
        }));
    }

    #[test]
    fn test_parse_id_template() {
        let t = PathTemplate::parse("events/{id}.md").unwrap();
        assert_eq!(t.segments.len(), 3);
        assert_eq!(
            t.segments[1],
            PathSegment::Field {
                name: "id".to_string(),
                format: None,
            }
        );
    }

    #[test]
    fn test_render_simple() {
        let t = PathTemplate::parse("users/{name}.md").unwrap();
        let data: Value = serde_yaml::from_str("name: Alice Chen").unwrap();
        let result = t.render(&data, None).unwrap();
        assert_eq!(result, "users/alice-chen.md");
    }

    #[test]
    fn test_render_with_date() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        let data: Value = serde_yaml::from_str(
            "title: Quarterly Review\nstatus: published\ndate: '2026-02-13'",
        )
        .unwrap();
        let result = t.render(&data, None).unwrap();
        assert_eq!(result, "posts/published/2026-02-13-quarterly-review.md");
    }

    #[test]
    fn test_render_with_id() {
        let t = PathTemplate::parse("events/{id}.md").unwrap();
        let data: Value = serde_yaml::from_str("type: test").unwrap();
        let result = t.render(&data, Some("01JMCX7K9A")).unwrap();
        assert_eq!(result, "events/01jmcx7k9a.md");
    }

    #[test]
    fn test_render_nested_ref() {
        let t = PathTemplate::parse("comments/{parent:type}/{parent:id}.md").unwrap();
        let data: Value =
            serde_yaml::from_str("parent:\n  type: posts\n  id: my-post").unwrap();
        let result = t.render(&data, None).unwrap();
        assert_eq!(result, "comments/posts/my-post.md");
    }

    #[test]
    fn test_referenced_fields() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        let fields = t.referenced_fields();
        assert!(fields.contains("status"));
        assert!(fields.contains("date"));
        assert!(fields.contains("title"));
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn test_referenced_fields_with_nested() {
        let t = PathTemplate::parse(
            "comments/{parent:type}/{parent:id}/{user:id}.md",
        )
        .unwrap();
        let fields = t.referenced_fields();
        assert!(fields.contains("parent"));
        assert!(fields.contains("user"));
    }

    #[test]
    fn test_references_field() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        assert!(t.references_field("status"));
        assert!(t.references_field("title"));
        assert!(!t.references_field("author_id"));
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("My Draft Post"), "my-draft-post");
        assert_eq!(slugify("UPPERCASE"), "uppercase");
        assert_eq!(slugify("special!@#chars"), "special-chars");
    }

    #[test]
    fn test_resolve_suffix() {
        let existing = vec!["test.md".to_string(), "test-2.md".to_string()];
        let result = resolve_suffix("test.md", |p| existing.contains(&p.to_string()));
        assert_eq!(result, "test-3.md");
    }

    #[test]
    fn test_resolve_suffix_no_conflict() {
        let result = resolve_suffix("test.md", |_| false);
        assert_eq!(result, "test.md");
    }

    #[test]
    fn test_unclosed_brace() {
        let result = PathTemplate::parse("posts/{title");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_field_ref() {
        let result = PathTemplate::parse("posts/{}.md");
        assert!(result.is_err());
    }

    #[test]
    fn test_base_directory() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        assert_eq!(t.base_directory(), "posts/");

        let t2 = PathTemplate::parse("users/{name}.md").unwrap();
        assert_eq!(t2.base_directory(), "users/");

        let t3 = PathTemplate::parse("{id}.md").unwrap();
        assert_eq!(t3.base_directory(), "");
    }

    #[test]
    fn test_extract_simple() {
        let t = PathTemplate::parse("users/{name}.md").unwrap();
        let fields = t.extract("users/alice-chen.md").unwrap();
        assert_eq!(fields.get("name").unwrap(), "alice-chen");
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn test_extract_with_date_and_status() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        let fields = t
            .extract("posts/published/2026-02-13-quarterly-review.md")
            .unwrap();
        assert_eq!(fields.get("status").unwrap(), "published");
        assert_eq!(fields.get("date").unwrap(), "2026-02-13");
        assert_eq!(fields.get("title").unwrap(), "quarterly-review");
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn test_extract_wrong_prefix() {
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        assert!(t.extract("users/alice-chen.md").is_none());
    }

    #[test]
    fn test_extract_id_only() {
        let t = PathTemplate::parse("events/{id}.md").unwrap();
        let fields = t.extract("events/01jmcx7k9a.md").unwrap();
        assert_eq!(fields.get("id").unwrap(), "01jmcx7k9a");
    }

    #[test]
    fn test_extract_nested_ref_skipped() {
        let t = PathTemplate::parse("comments/{parent:type}/{parent:id}/{user:id}-{created_at:YYYY-MM-DDTHHMM}.md").unwrap();
        // The format YYYY-MM-DDTHHMM is 15 chars; a real rendered+slugified
        // datetime like "2026-02-13T14:30" → format → "2026-02-13T1430" → slug → "2026-02-13t1430"
        let fields = t
            .extract("comments/posts/my-post/alice-2026-02-13t1430.md")
            .unwrap();
        // NestedField segments are not included in the result
        assert!(!fields.contains_key("parent"));
        assert!(!fields.contains_key("user"));
        assert_eq!(fields.get("created_at").unwrap(), "2026-02-13t1430");
    }

    #[test]
    fn test_extract_roundtrip() {
        // Render a path, then extract — should get back the slugified values
        let t = PathTemplate::parse("posts/{status}/{date:YYYY-MM-DD}-{title}.md").unwrap();
        let data: Value = serde_yaml::from_str(
            "title: Quarterly Review\nstatus: published\ndate: '2026-02-13'",
        )
        .unwrap();
        let rendered = t.render(&data, None).unwrap();
        let extracted = t.extract(&rendered).unwrap();
        assert_eq!(extracted.get("status").unwrap(), "published");
        assert_eq!(extracted.get("title").unwrap(), "quarterly-review");
    }
}
