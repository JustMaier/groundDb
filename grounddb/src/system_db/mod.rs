use crate::error::{GroundDbError, Result};
use crate::util::json_to_yaml;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

/// How long a connection retries a lock held by another connection before
/// giving up with `SQLITE_BUSY`.
///
/// This happens to match the default rusqlite installs on every connection, but
/// we set it explicitly: that default is an implementation detail of the
/// dependency, and the correctness of multi-process access should not silently
/// change when rusqlite is upgraded.
pub const BUSY_TIMEOUT_MS: u64 = 5000;

/// The system database that manages document index, schema state, and view cache.
/// Uses a Mutex around the connection so Store can be Send + Sync.
pub struct SystemDb {
    conn: Mutex<Connection>,
}

impl SystemDb {
    /// Open or create the system database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::configure(&conn, true)?;
        let db = SystemDb { conn: Mutex::new(conn) };
        db.initialize_tables()?;
        Ok(db)
    }

    /// Open an in-memory system database (for testing).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn, false)?;
        let db = SystemDb { conn: Mutex::new(conn) };
        db.initialize_tables()?;
        Ok(db)
    }

    /// Apply connection-level pragmas. Must run before any statement that opens
    /// a transaction, because `journal_mode` cannot be changed inside one.
    ///
    /// ## What WAL buys us, and what it does not
    ///
    /// The store is reachable from more than one OS process. The `Mutex` around
    /// the connection is an *in-process* lock — it exists so `Store` can be
    /// `Send + Sync` — so between processes the only arbiter is SQLite itself.
    ///
    /// In the default rollback-journal mode a writer's `RESERVED` lock escalates
    /// to `EXCLUSIVE` at commit, which locks readers out, and a reader that is
    /// already mid-statement can force the writer to fail. WAL removes that
    /// contention in one direction only: **readers never block writers and
    /// writers never block readers**, because readers work from a snapshot.
    ///
    /// WAL does *not* make multi-process writing free:
    ///
    /// - **Writers are still serialized.** Exactly one write transaction may be
    ///   in flight per database. The improvement is that a second writer now
    ///   *waits* out the [`BUSY_TIMEOUT_MS`] window instead of erroring
    ///   immediately — and it still errors if the holder outlives that window.
    /// - **WAL needs shared memory.** It coordinates through a `-shm` file
    ///   mapped by every connection, so it does not work over network
    ///   filesystems (NFS, SMB, most container/VM shares). SQLite will refuse to
    ///   enter WAL there, which is why the mode is read back rather than
    ///   assumed.
    /// - **Two extra files appear** next to the database (`-wal` and `-shm`).
    ///   Anything that copies or deletes the store must treat them as part of it.
    ///
    /// WAL is meaningless for an in-memory database — there is no second process
    /// to share it with and no file to map — so it is only requested on disk.
    fn configure(conn: &Connection, on_disk: bool) -> Result<()> {
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))?;

        if on_disk {
            // `PRAGMA journal_mode` returns the mode actually in effect, which
            // may not be the one asked for (a network filesystem cannot do WAL).
            // Reading it back keeps a silent downgrade from looking like success
            // — the store still works, just with the old contention behaviour.
            let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
            if !mode.eq_ignore_ascii_case("wal") {
                log::warn!(
                    "system db could not enter WAL mode (still '{mode}'); \
                     concurrent access will contend as before. \
                     This is expected on network filesystems."
                );
            }
        }

        Ok(())
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }

    /// The journal mode this connection is running in (`"wal"`, `"memory"`,
    /// `"delete"`, …). Exposed for diagnostics and tests.
    pub fn journal_mode(&self) -> Result<String> {
        let mode = self
            .conn()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        Ok(mode)
    }

    /// How long, in milliseconds, this connection will retry a lock held by
    /// another connection before giving up with `SQLITE_BUSY`. Exposed for
    /// diagnostics and tests.
    pub fn busy_timeout_ms(&self) -> Result<i64> {
        let ms = self
            .conn()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        Ok(ms)
    }

    fn initialize_tables(&self) -> Result<()> {
        // First create all tables, then migrate existing ones if needed
        self.conn().execute_batch(
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
                created_at TEXT,
                modified_at TEXT,
                content_text TEXT,
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
        // Migrate existing documents table: add columns if missing
        self.migrate_documents_table()?;
        Ok(())
    }

    /// Check if the documents table has the newer columns and add them if missing.
    fn migrate_documents_table(&self) -> Result<()> {
        let conn = self.conn();
        let mut has_created_at = false;
        let mut has_modified_at = false;
        let mut has_content_text = false;

        let mut stmt = conn.prepare("PRAGMA table_info(documents)")?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            Ok(name)
        })?;
        for row in rows {
            match row?.as_str() {
                "created_at" => has_created_at = true,
                "modified_at" => has_modified_at = true,
                "content_text" => has_content_text = true,
                _ => {}
            }
        }
        drop(stmt);

        if !has_created_at {
            conn.execute_batch("ALTER TABLE documents ADD COLUMN created_at TEXT")?;
        }
        if !has_modified_at {
            conn.execute_batch("ALTER TABLE documents ADD COLUMN modified_at TEXT")?;
        }
        if !has_content_text {
            conn.execute_batch("ALTER TABLE documents ADD COLUMN content_text TEXT")?;
        }

        Ok(())
    }

    // ── Schema State ─────────────────────────────────────────────────

    /// Get the most recent schema hash.
    pub fn get_last_schema_hash(&self) -> Result<Option<String>> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT hash FROM schema_history ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Get the most recent schema YAML content.
    pub fn get_last_schema_yaml(&self) -> Result<Option<String>> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT schema_yaml FROM schema_history ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Record a new schema version.
    pub fn record_schema(&self, hash: &str, yaml: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO schema_history (hash, schema_yaml) VALUES (?1, ?2)",
            params![hash, yaml],
        )?;
        Ok(())
    }

    /// Record a migration.
    pub fn record_migration(&self, description: &str) -> Result<()> {
        self.conn().execute(
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
        created_at: Option<&str>,
        modified_at: Option<&str>,
        content_text: Option<&str>,
    ) -> Result<()> {
        let data_json = serde_json::to_string(data)?;
        self.conn().execute(
            "INSERT OR REPLACE INTO documents (id, collection, path, data_json, created_at, modified_at, content_text) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, collection, path, data_json, created_at, modified_at, content_text],
        )?;
        Ok(())
    }

    /// Get a document from the index by collection and id.
    pub fn get_document(&self, collection: &str, id: &str) -> Result<Option<DocumentRecord>> {
        let conn = self.conn();
        let result = conn.query_row(
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

    /// Look up a document's id by its root-relative path. The path is unique
    /// per document, so this is the authoritative way to recover the stored id
    /// for an already-indexed file — used by id derivation to avoid re-parsing
    /// the id out of a (possibly ambiguous) filename.
    pub fn get_id_by_path(&self, collection: &str, path: &str) -> Result<Option<String>> {
        let conn = self.conn();
        let result = conn
            .query_row(
                "SELECT id FROM documents WHERE collection = ?1 AND path = ?2",
                params![collection, path],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(result)
    }

    /// List all documents in a collection.
    pub fn list_documents(&self, collection: &str) -> Result<Vec<DocumentRecord>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
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
        self.conn().execute(
            "DELETE FROM documents WHERE collection = ?1 AND id = ?2",
            params![collection, id],
        )?;
        Ok(())
    }

    /// Find all documents that reference a given target document.
    pub fn find_references(
        &self,
        target_collection: &str,
        target_id: &str,
    ) -> Result<Vec<DocumentRecord>> {
        let pattern = format!("%\"{}\"%" , target_id);
        let conn = self.conn();

        let mut stmt = conn.prepare(
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
        self.conn().execute(
            "DELETE FROM documents WHERE collection = ?1",
            params![collection],
        )?;
        Ok(())
    }

    // ── Directory Hashes ─────────────────────────────────────────────

    /// Get the stored directory hash for a collection.
    pub fn get_directory_hash(&self, collection: &str) -> Result<Option<String>> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT hash FROM directory_hashes WHERE collection = ?1",
            params![collection],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Update the directory hash for a collection.
    pub fn set_directory_hash(&self, collection: &str, hash: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO directory_hashes (collection, hash) VALUES (?1, ?2)",
            params![collection, hash],
        )?;
        Ok(())
    }

    // ── View State ───────────────────────────────────────────────────

    /// Get cached view data.
    pub fn get_view_data(&self, view_name: &str) -> Result<Option<String>> {
        let conn = self.conn();
        let result = conn.query_row(
            "SELECT data_json FROM view_data WHERE view_name = ?1",
            params![view_name],
            |row| row.get(0),
        ).optional()?;
        Ok(result)
    }

    /// Store view data.
    pub fn set_view_data(&self, view_name: &str, data_json: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO view_data (view_name, data_json) VALUES (?1, ?2)",
            params![view_name, data_json],
        )?;
        Ok(())
    }

    /// Get view metadata.
    pub fn get_view_metadata(&self, view_name: &str) -> Result<Option<(String, String)>> {
        let conn = self.conn();
        let result = conn.query_row(
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
        self.conn().execute(
            "INSERT OR REPLACE INTO view_metadata (view_name, last_built, source_hashes) VALUES (?1, ?2, ?3)",
            params![view_name, last_built, source_hashes],
        )?;
        Ok(())
    }

    // ── Transaction Support ──────────────────────────────────────────

    /// Begin a transaction.
    ///
    /// `IMMEDIATE`, not the SQLite default of `DEFERRED`, because every caller
    /// here is about to write. A deferred transaction takes no lock until its
    /// first statement, so one that reads before it writes — which
    /// `Batch::commit` does, reading each document to save its file for
    /// rollback — has to *upgrade* to a write lock later. If another connection
    /// committed in between, that upgrade fails with `SQLITE_BUSY_SNAPSHOT`
    /// (extended code 517), and the busy timeout does **not** retry it: the read
    /// already saw a snapshot that can no longer be extended, so waiting cannot
    /// help. It fails instantly no matter how long the timeout is.
    ///
    /// `IMMEDIATE` takes the write lock up front, where the busy timeout does
    /// apply, so a contended batch waits instead of erroring.
    pub fn begin_transaction(&self) -> Result<()> {
        self.conn().execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    /// Commit the current transaction.
    pub fn commit_transaction(&self) -> Result<()> {
        self.conn().execute_batch("COMMIT")?;
        Ok(())
    }

    /// Rollback the current transaction.
    pub fn rollback_transaction(&self) -> Result<()> {
        self.conn().execute_batch("ROLLBACK")?;
        Ok(())
    }

    // ── SQL Query Execution (for views) ──────────────────────────────

    /// Execute a SQL query against the documents table, returning results as
    /// a list of JSON objects. This powers the view engine.
    ///
    /// `params` is a list of `(":name", value)` pairs for named parameter binding.
    pub fn query_documents_sql(
        &self,
        sql: &str,
        params_map: &HashMap<String, String>,
    ) -> Result<Vec<serde_json::Value>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(sql)
            .map_err(|e| GroundDbError::SqlParse(format!("Failed to prepare SQL: {e}")))?;

        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        // Build named parameter bindings for rusqlite
        let named_params: Vec<(String, String)> = params_map
            .iter()
            .map(|(k, v)| {
                let key = if k.starts_with(':') {
                    k.clone()
                } else {
                    format!(":{k}")
                };
                (key, v.clone())
            })
            .collect();
        let param_refs: Vec<(&str, &dyn rusqlite::types::ToSql)> = named_params
            .iter()
            .map(|(k, v)| (k.as_str(), v as &dyn rusqlite::types::ToSql))
            .collect();

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
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

        db.upsert_document("alice-chen", "users", "users/alice-chen.md", &data, None, None, None)
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

        db.upsert_document("alice", "users", "users/alice.md", &data1, None, None, None).unwrap();
        db.upsert_document("bob", "users", "users/bob.md", &data2, None, None, None).unwrap();

        let docs = db.list_documents("users").unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_delete_document() {
        let db = SystemDb::open_in_memory().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();

        db.upsert_document("alice", "users", "users/alice.md", &data, None, None, None).unwrap();
        db.delete_document("users", "alice").unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn test_upsert_updates_existing() {
        let db = SystemDb::open_in_memory().unwrap();

        let data1: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data1, None, None, None).unwrap();

        let data2: serde_yaml::Value = serde_yaml::from_str("name: Alice Updated").unwrap();
        db.upsert_document("alice", "users", "users/alice-updated.md", &data2, None, None, None).unwrap();

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
        db.upsert_document("alice", "users", "users/alice.md", &user_data, None, None, None).unwrap();

        let post_data: serde_yaml::Value =
            serde_yaml::from_str("title: Test\nauthor_id: alice").unwrap();
        db.upsert_document("test-post", "posts", "posts/test.md", &post_data, None, None, None).unwrap();

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
            ("b.md".to_string(), 300u64),
        ];
        let h3 = compute_directory_hash(&different);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_transaction() {
        let db = SystemDb::open_in_memory().unwrap();

        db.begin_transaction().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data, None, None, None).unwrap();
        db.commit_transaction().unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_some());
    }

    #[test]
    fn test_transaction_rollback() {
        let db = SystemDb::open_in_memory().unwrap();

        db.begin_transaction().unwrap();
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        db.upsert_document("alice", "users", "users/alice.md", &data, None, None, None).unwrap();
        db.rollback_transaction().unwrap();

        let doc = db.get_document("users", "alice").unwrap();
        assert!(doc.is_none());
    }
}
