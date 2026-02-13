use crate::error::{GroundDbError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::Path;

/// The system database that manages document index, schema state, and view cache.
pub struct SystemDb {
    conn: Connection,
}

impl SystemDb {
    /// Open or create the system database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = SystemDb { conn };
        db.initialize_tables()?;
        Ok(db)
    }

    /// Open an in-memory system database (for testing).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = SystemDb { conn };
        db.initialize_tables()?;
        Ok(db)
    }

    fn initialize_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS schema_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT NOT NULL,
                schema_yaml TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS documents (
                id TEXT NOT NULL,
                collection TEXT NOT NULL,
                path TEXT NOT NULL,
                data_json TEXT NOT NULL,
                PRIMARY KEY (collection, id)
            );

            CREATE INDEX IF NOT EXISTS idx_documents_path ON documents(path);
            CREATE INDEX IF NOT EXISTS idx_documents_collection ON documents(collection);

            CREATE TABLE IF NOT EXISTS view_data (
                view_name TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS view_metadata (
                view_name TEXT PRIMARY KEY,
                last_built TEXT,
                source_hashes TEXT
            );

            CREATE TABLE IF NOT EXISTS directory_hashes (
                collection TEXT PRIMARY KEY,
                hash TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "
        )?;
        Ok(())
    }

    // ── Schema State ─────────────────────────────────────────────────

    /// Get the most recent schema hash.
    pub fn get_last_schema_hash(&self) -> Result<Option<String>> {
        let result = self.conn.query_row(
            "SELECT hash FROM schema_history ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Record a new schema version.
    pub fn record_schema(&self, hash: &str, yaml: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO schema_history (hash, schema_yaml) VALUES (?1, ?2)",
            params![hash, yaml],
        )?;
        Ok(())
    }

    /// Record a migration.
    pub fn record_migration(&self, description: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO migrations (description) VALUES (?1)",
            params![description],
        )?;
        Ok(())
    }

    // ── Document Index ───────────────────────────────────────────────

    /// Upsert a document into the index.
    pub fn upsert_document(
        &self,
        id: &str,
        collection: &str,
        path: &str,
        data: &serde_yaml::Value,
    ) -> Result<()> {
        let data_json = serde_json::to_string(data)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO documents (id, collection, path, data_json) VALUES (?1, ?2, ?3, ?4)",
            params![id, collection, path, data_json],
        )?;
        Ok(())
    }

    /// Get a document from the index by collection and id.
    pub fn get_document(&self, collection: &str, id: &str) -> Result<Option<DocumentRecord>> {
        let result = self.conn.query_row(
            "SELECT id, collection, path, data_json FROM documents WHERE collection = ?1 AND id = ?2",
            params![collection, id],
            |row| {
                Ok(DocumentRecord {
                    id: row.get(0)?,
                    collection: row.get(1)?,
                    path: row.get(2)?,
                    data_json: row.get(3)?,
                })
            },
        ).optional()?;
        Ok(result)
    }

    /// List all documents in a collection.
    pub fn list_documents(&self, collection: &str) -> Result<Vec<DocumentRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, collection, path, data_json FROM documents WHERE collection = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![collection], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                collection: row.get(1)?,
                path: row.get(2)?,
                data_json: row.get(3)?,
            })
        })?;

        let mut docs = Vec::new();
        for row in rows {
            docs.push(row?);
        }
        Ok(docs)
    }

    /// Delete a document from the index.
    pub fn delete_document(&self, collection: &str, id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM documents WHERE collection = ?1 AND id = ?2",
            params![collection, id],
        )?;
        Ok(())
    }

    /// Find all documents that reference a given target document.
    /// Searches the data_json column for the target ID string.
    pub fn find_references(
        &self,
        target_collection: &str,
        target_id: &str,
    ) -> Result<Vec<DocumentRecord>> {
        // Search for any document whose data_json contains the target id as a value
        // This is a broad search; the caller should refine by checking actual ref fields
        let pattern = format!("%\"{}\"%" , target_id);

        let mut stmt = self.conn.prepare(
            "SELECT id, collection, path, data_json FROM documents
             WHERE collection != ?1 AND data_json LIKE ?2",
        )?;
        let rows = stmt.query_map(params![target_collection, pattern], |row| {
            Ok(DocumentRecord {
                id: row.get(0)?,
                collection: row.get(1)?,
                path: row.get(2)?,
                data_json: row.get(3)?,
            })
        })?;

        let mut docs = Vec::new();
        for row in rows {
            docs.push(row?);
        }
        Ok(docs)
    }

    /// Delete all documents in a collection from the index.
    pub fn delete_collection_documents(&self, collection: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM documents WHERE collection = ?1",
            params![collection],
        )?;
        Ok(())
    }

    // ── Directory Hashes ─────────────────────────────────────────────

    /// Get the stored directory hash for a collection.
    pub fn get_directory_hash(&self, collection: &str) -> Result<Option<String>> {
        let result = self.conn.query_row(
            "SELECT hash FROM directory_hashes WHERE collection = ?1",
            params![collection],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Update the directory hash for a collection.
    pub fn set_directory_hash(&self, collection: &str, hash: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO directory_hashes (collection, hash) VALUES (?1, ?2)",
            params![collection, hash],
        )?;
        Ok(())
    }

    // ── View State ───────────────────────────────────────────────────

    /// Get cached view data.
    pub fn get_view_data(&self, view_name: &str) -> Result<Option<String>> {
        let result = self.conn.query_row(
            "SELECT data_json FROM view_data WHERE view_name = ?1",
            params![view_name],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Store view data.
    pub fn set_view_data(&self, view_name: &str, data_json: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO view_data (view_name, data_json) VALUES (?1, ?2)",
            params![view_name, data_json],
        )?;
        Ok(())
    }

    /// Get view metadata.
    pub fn get_view_metadata(&self, view_name: &str) -> Result<Option<(String, String)>> {
        let result = self.conn.query_row(
            "SELECT last_built, source_hashes FROM view_metadata WHERE view_name = ?1",
            params![view_name],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        ).optional()?;
        Ok(result)
    }

    /// Update view metadata.
    pub fn set_view_metadata(
        &self,
        view_name: &str,
        last_built: &str,
        source_hashes: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO view_metadata (view_name, last_built, source_hashes) VALUES (?1, ?2, ?3)",
            params![view_name, last_built, source_hashes],
        )?;
        Ok(())
    }

    // ── Transaction Support ──────────────────────────────────────────

    /// Begin a transaction. Returns the connection for executing in-transaction ops.
    pub fn begin_transaction(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        Ok(())
    }

    /// Commit the current transaction.
    pub fn commit_transaction(&self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    /// Rollback the current transaction.
    pub fn rollback_transaction(&self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        Ok(())
    }

    // ── SQL Query Execution (for views) ──────────────────────────────

    /// Execute a SQL query against the documents table, returning results as
    /// a list of JSON objects. This powers the view engine.
    pub fn query_documents_sql(
        &self,
        sql: &str,
        _params_map: &HashMap<String, String>,
    ) -> Result<Vec<serde_json::Value>> {
        // For safety, we create a view of documents that the SQL can query against.
        // The view engine will have already translated the SQL to work against our tables.
        // For now, this is a simple implementation that works with the documents table directly.
        let mut stmt = self.conn.prepare(sql)
            .map_err(|e| GroundDbError::SqlParse(format!("Failed to prepare SQL: {e}")))?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let rows = stmt.query_map([], |row| {
            let mut obj = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let val: rusqlite::types::Value = row.get(i)?;
                let json_val = match val {
                    rusqlite::types::Value::Null => serde_json::Value::Null,
                    rusqlite::types::Value::Integer(n) => serde_json::Value::Number(n.into()),
                    rusqlite::types::Value::Real(f) => {
                        serde_json::Number::from_f64(f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                    rusqlite::types::Value::Blob(b) => {
                        serde_json::Value::String(String::from_utf8_lossy(&b).into())
                    }
                };
                obj.insert(name.clone(), json_val);
            }
            Ok(serde_json::Value::Object(obj))
        }).map_err(|e| GroundDbError::SqlParse(format!("SQL query failed: {e}")))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}

/// A record from the documents table
#[derive(Debug, Clone)]
pub struct DocumentRecord {
    pub id: String,
    pub collection: String,
    pub path: String,
    pub data_json: String,
}

impl DocumentRecord {
    /// Parse the stored JSON data back into a serde_yaml::Value
    pub fn parse_data(&self) -> Result<serde_yaml::Value> {
        let json: serde_json::Value = serde_json::from_str(&self.data_json)?;
        let yaml = json_to_yaml(&json);
        Ok(yaml)
    }
}

/// Convert a serde_json::Value to serde_yaml::Value
fn json_to_yaml(json: &serde_json::Value) -> serde_yaml::Value {
    match json {
        serde_json::Value::Null => serde_yaml::Value::Null,
        serde_json::Value::Bool(b) => serde_yaml::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_yaml::Value::Number(serde_yaml::Number::from(i))
            } else if let Some(f) = n.as_f64() {
                serde_yaml::Value::Number(serde_yaml::Number::from(f))
            } else {
                serde_yaml::Value::Null
            }
        }
        serde_json::Value::String(s) => serde_yaml::Value::String(s.clone()),
        serde_json::Value::Array(arr) => {
            serde_yaml::Value::Sequence(arr.iter().map(json_to_yaml).collect())
        }
        serde_json::Value::Object(map) => {
            let mut m = serde_yaml::Mapping::new();
            for (k, v) in map {
                m.insert(serde_yaml::Value::String(k.clone()), json_to_yaml(v));
            }
            serde_yaml::Value::Mapping(m)
        }
    }
}

/// Compute a directory hash from a list of (filename, mtime) pairs.
/// Used for change detection during boot.
pub fn compute_directory_hash(entries: &[(String, u64)]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = DefaultHasher::new();
    for (name, mtime) in &sorted {
        name.hash(&mut hasher);
        mtime.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_query_document() {
        let db = SystemDb::open_in_memory().unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();

        db.upsert_document("alice-chen", "users", "users/alice-chen.md", &data)
            .unwrap();

        let doc = db.get_document("users", "alice-chen").unwrap().unwrap();
        assert_eq!(doc.id, "alice-chen");
        assert_eq!(doc.collection, "users");
        assert_eq!(doc.path, "users/alice-chen.md");

        let parsed = doc.parse_data().unwrap();
        assert_eq!(parsed["name"], serde_yaml::Value::String("Alice".into()));
    }

    #[test]
    fn test_list_documents() {
        let db = SystemDb::open_in_memory().unwrap();

        let data1: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        let data2: serde_yaml::Value = serde_yaml::from_str("name: Bob").unwrap();

        db.upsert_document("alice", "users", "users/alice.md", &data1).unwrap();
        db.upsert_document("bob", "users", "users/bob.md", &data2).unwrap();

        let docs = db.list_documents("users").unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_delete_document() {
        let db = SystemDb::open_in_memory().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();

        db.upsert_document("alice", "users", "users/alice.md", &data).unwrap();
        db.delete_document("users", "alice").unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_upsert_updates_existing() {
        let db = SystemDb::open_in_memory().unwrap();

        let data1: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data1).unwrap();

        let data2: serde_yaml::Value = serde_yaml::from_str("name: Alice Updated").unwrap();
        db.upsert_document("alice", "users", "users/alice-updated.md", &data2).unwrap();

        let docs = db.list_documents("users").unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].path, "users/alice-updated.md");
    }

    #[test]
    fn test_schema_history() {
        let db = SystemDb::open_in_memory().unwrap();

        assert!(db.get_last_schema_hash().unwrap().is_none());

        db.record_schema("abc123", "collections: {}").unwrap();
        assert_eq!(
            db.get_last_schema_hash().unwrap(),
            Some("abc123".to_string())
        );

        db.record_schema("def456", "collections: { users: {} }").unwrap();
        assert_eq!(
            db.get_last_schema_hash().unwrap(),
            Some("def456".to_string())
        );
    }

    #[test]
    fn test_directory_hashes() {
        let db = SystemDb::open_in_memory().unwrap();

        assert!(db.get_directory_hash("users").unwrap().is_none());

        db.set_directory_hash("users", "hash123").unwrap();
        assert_eq!(
            db.get_directory_hash("users").unwrap(),
            Some("hash123".to_string())
        );
    }

    #[test]
    fn test_view_data() {
        let db = SystemDb::open_in_memory().unwrap();

        assert!(db.get_view_data("post_feed").unwrap().is_none());

        db.set_view_data("post_feed", "[{\"title\": \"test\"}]").unwrap();
        let data = db.get_view_data("post_feed").unwrap().unwrap();
        assert!(data.contains("test"));
    }

    #[test]
    fn test_find_references() {
        let db = SystemDb::open_in_memory().unwrap();

        let user_data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &user_data).unwrap();

        let post_data: serde_yaml::Value =
            serde_yaml::from_str("title: Test\nauthor_id: alice").unwrap();
        db.upsert_document("test-post", "posts", "posts/test.md", &post_data).unwrap();

        let refs = db.find_references("users", "alice").unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].id, "test-post");
    }

    #[test]
    fn test_compute_directory_hash() {
        let entries = vec![
            ("a.md".to_string(), 100u64),
            ("b.md".to_string(), 200u64),
        ];
        let h1 = compute_directory_hash(&entries);
        let h2 = compute_directory_hash(&entries);
        assert_eq!(h1, h2);

        let different = vec![
            ("a.md".to_string(), 100u64),
            ("b.md".to_string(), 300u64), // different mtime
        ];
        let h3 = compute_directory_hash(&different);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_transaction() {
        let db = SystemDb::open_in_memory().unwrap();

        db.begin_transaction().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data).unwrap();
        db.commit_transaction().unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_some());
    }

    #[test]
    fn test_transaction_rollback() {
        let db = SystemDb::open_in_memory().unwrap();

        db.begin_transaction().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data).unwrap();
        db.rollback_transaction().unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_none());
    }
}
