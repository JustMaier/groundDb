use crate::error::{GroundDbError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A loaded document with implicit fields and typed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document<T> {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub data: T,
    pub content: Option<String>,
}

/// The front matter separator used in Markdown documents
const FRONT_MATTER_FENCE: &str = "---";

/// Read a markdown document from disk.
/// Parses YAML front matter and optional markdown body.
/// The `id` is derived from the filename (without extension).
pub fn read_document(path: &Path) -> Result<Document<serde_yaml::Value>> {
    let raw = std::fs::read_to_string(path)?;
    let metadata = std::fs::metadata(path)?;

    let id = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| GroundDbError::Other(format!("Cannot extract ID from path: {path:?}")))?
        .to_string();

    let created_at = metadata
        .created()
        .unwrap_or(metadata.modified()?)
        .into();
    let modified_at: DateTime<Utc> = metadata.modified()?.into();

    let (data, content) = parse_front_matter(&raw)?;

    Ok(Document {
        id,
        created_at,
        modified_at,
        data,
        content,
    })
}

/// Parse a markdown string into front matter data and optional body content.
pub fn parse_front_matter(raw: &str) -> Result<(serde_yaml::Value, Option<String>)> {
    let trimmed = raw.trim_start();

    if !trimmed.starts_with(FRONT_MATTER_FENCE) {
        // No front matter -- treat entire content as body with empty data
        return Ok((
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
            if raw.trim().is_empty() {
                None
            } else {
                Some(raw.to_string())
            },
        ));
    }

    // Find the second --- fence
    let after_first = &trimmed[3..];
    let after_first = after_first.trim_start_matches(|c: char| c == '\r' || c == '\n');

    if let Some(end_pos) = after_first.find("\n---") {
        let yaml_str = &after_first[..end_pos];
        let after_fence = &after_first[end_pos + 4..]; // skip "\n---"

        // Skip any trailing newlines after the closing fence
        let body = after_fence.strip_prefix('\r').unwrap_or(after_fence);
        let body = body.strip_prefix('\n').unwrap_or(body);

        let data: serde_yaml::Value = if yaml_str.trim().is_empty() {
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
        } else {
            serde_yaml::from_str(yaml_str)?
        };

        let content = if body.trim().is_empty() {
            None
        } else {
            Some(body.to_string())
        };

        Ok((data, content))
    } else {
        // Only one fence -- entire content after first --- is YAML (data-only doc)
        let data: serde_yaml::Value = if after_first.trim().is_empty() {
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
        } else {
            serde_yaml::from_str(after_first)?
        };
        Ok((data, None))
    }
}

/// Serialize front matter and optional body content into a markdown string.
pub fn serialize_document(data: &serde_yaml::Value, content: Option<&str>) -> Result<String> {
    let yaml = serde_yaml::to_string(data)?;
    let mut output = String::new();
    output.push_str(FRONT_MATTER_FENCE);
    output.push('\n');
    output.push_str(&yaml);
    // serde_yaml adds a trailing newline, but make sure
    if !yaml.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(FRONT_MATTER_FENCE);
    output.push('\n');

    if let Some(body) = content {
        if !body.is_empty() {
            output.push('\n');
            output.push_str(body);
            if !body.ends_with('\n') {
                output.push('\n');
            }
        }
    }

    Ok(output)
}

/// Write a document to disk. Creates parent directories as needed.
/// Uses atomic write (write to temp file, then rename) for safety.
pub fn write_document(
    path: &Path,
    data: &serde_yaml::Value,
    content: Option<&str>,
) -> Result<()> {
    let serialized = serialize_document(data, content)?;

    // Create parent directories
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Atomic write: write to temp file, then rename
    let parent = path.parent().unwrap_or(Path::new("."));
    let temp = tempfile::NamedTempFile::new_in(parent)?;
    std::fs::write(temp.path(), &serialized)?;
    temp.persist(path).map_err(|e| {
        GroundDbError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to persist temp file: {e}"),
        ))
    })?;

    Ok(())
}

/// Delete a document from disk.
pub fn delete_document(path: &Path) -> Result<()> {
    std::fs::remove_file(path)?;

    // Clean up empty parent directories
    let mut dir = path.parent();
    while let Some(parent) = dir {
        if parent.read_dir()?.next().is_none() {
            std::fs::remove_dir(parent).ok();
            dir = parent.parent();
        } else {
            break;
        }
    }

    Ok(())
}

/// Move a document from one path to another. Creates parent directories as needed.
pub fn move_document(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::rename(from, to)?;

    // Clean up empty parent directories of the old path
    let mut dir = from.parent();
    while let Some(parent) = dir {
        if parent.exists() && parent.read_dir()?.next().is_none() {
            std::fs::remove_dir(parent).ok();
            dir = parent.parent();
        } else {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_data_only_document() {
        let raw = "---\nname: Alice Chen\nemail: alice@example.com\nrole: admin\n---\n";
        let (data, content) = parse_front_matter(raw).unwrap();
        assert!(content.is_none());
        assert_eq!(
            data["name"],
            serde_yaml::Value::String("Alice Chen".into())
        );
        assert_eq!(
            data["role"],
            serde_yaml::Value::String("admin".into())
        );
    }

    #[test]
    fn test_parse_content_document() {
        let raw = "---\ntitle: My Post\nstatus: draft\n---\n\n## Summary\n\nHello world.\n";
        let (data, content) = parse_front_matter(raw).unwrap();
        assert_eq!(
            data["title"],
            serde_yaml::Value::String("My Post".into())
        );
        let body = content.unwrap();
        assert!(body.contains("## Summary"));
        assert!(body.contains("Hello world."));
    }

    #[test]
    fn test_parse_empty_body() {
        let raw = "---\nname: Test\n---\n";
        let (data, content) = parse_front_matter(raw).unwrap();
        assert_eq!(
            data["name"],
            serde_yaml::Value::String("Test".into())
        );
        assert!(content.is_none());
    }

    #[test]
    fn test_serialize_data_only() {
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("name".into()),
            serde_yaml::Value::String("Alice".into()),
        );
        let data = serde_yaml::Value::Mapping(mapping);
        let result = serialize_document(&data, None).unwrap();
        assert!(result.starts_with("---\n"));
        assert!(result.contains("name: Alice"));
        // Should have closing fence
        let lines: Vec<&str> = result.lines().collect();
        assert!(lines.iter().any(|l| *l == "---"));
    }

    #[test]
    fn test_serialize_with_content() {
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("title".into()),
            serde_yaml::Value::String("My Post".into()),
        );
        let data = serde_yaml::Value::Mapping(mapping);
        let result = serialize_document(&data, Some("## Hello\n\nWorld.")).unwrap();
        assert!(result.contains("## Hello"));
        assert!(result.contains("World."));
    }

    #[test]
    fn test_roundtrip_data_only() {
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("name".into()),
            serde_yaml::Value::String("Alice".into()),
        );
        mapping.insert(
            serde_yaml::Value::String("email".into()),
            serde_yaml::Value::String("alice@test.com".into()),
        );
        let data = serde_yaml::Value::Mapping(mapping);

        let serialized = serialize_document(&data, None).unwrap();
        let (parsed_data, parsed_content) = parse_front_matter(&serialized).unwrap();

        assert!(parsed_content.is_none());
        assert_eq!(
            parsed_data["name"],
            serde_yaml::Value::String("Alice".into())
        );
        assert_eq!(
            parsed_data["email"],
            serde_yaml::Value::String("alice@test.com".into())
        );
    }

    #[test]
    fn test_roundtrip_with_content() {
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("title".into()),
            serde_yaml::Value::String("My Post".into()),
        );
        let data = serde_yaml::Value::Mapping(mapping);
        let body = "## Hello\n\nThis is the content.\n";

        let serialized = serialize_document(&data, Some(body)).unwrap();
        let (parsed_data, parsed_content) = parse_front_matter(&serialized).unwrap();

        assert_eq!(
            parsed_data["title"],
            serde_yaml::Value::String("My Post".into())
        );
        let parsed_body = parsed_content.unwrap();
        assert!(parsed_body.contains("## Hello"));
        assert!(parsed_body.contains("This is the content."));
    }

    #[test]
    fn test_write_and_read_document() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test-doc.md");

        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            serde_yaml::Value::String("name".into()),
            serde_yaml::Value::String("Test User".into()),
        );
        let data = serde_yaml::Value::Mapping(mapping);

        write_document(&path, &data, Some("Hello body")).unwrap();
        assert!(path.exists());

        let doc = read_document(&path).unwrap();
        assert_eq!(doc.id, "test-doc");
        assert_eq!(
            doc.data["name"],
            serde_yaml::Value::String("Test User".into())
        );
        assert!(doc.content.unwrap().contains("Hello body"));
    }

    #[test]
    fn test_write_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested").join("dir").join("doc.md");

        let data = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
        write_document(&path, &data, None).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_delete_document() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("collection");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("doc.md");

        let data = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
        write_document(&path, &data, None).unwrap();
        assert!(path.exists());

        delete_document(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_move_document() {
        let tmp = TempDir::new().unwrap();
        let from = tmp.path().join("old").join("doc.md");
        let to = tmp.path().join("new").join("doc.md");

        let data = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
        write_document(&from, &data, Some("body")).unwrap();
        assert!(from.exists());

        move_document(&from, &to).unwrap();
        assert!(!from.exists());
        assert!(to.exists());

        let doc = read_document(&to).unwrap();
        assert!(doc.content.unwrap().contains("body"));
    }
}
