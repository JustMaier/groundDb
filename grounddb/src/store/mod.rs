use crate::document::{self, Document};
use crate::error::{GroundDbError, Result};
use crate::path_template::{self, PathSegment, PathTemplate};
use crate::schema::{
    hash_schema, parse_schema, AutoIdStrategy, CollectionDefinition, FieldType, OnConflict,
    OnDeletePolicy, SchemaDefinition,
};
use crate::system_db::{compute_directory_hash, SystemDb};
use crate::util::json_to_yaml as json_value_to_yaml;
use crate::validation;
use crate::migration;
use crate::view::{self as view_engine, ViewEngine};
use crate::watcher::{ChangeKind, FileWatcher, WatcherEvent};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};

/// Unique subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

/// An event describing a change to a document in a collection.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    Inserted { id: String, data: serde_json::Value },
    Updated { id: String, data: serde_json::Value },
    Deleted { id: String },
}

type ViewCallback = Box<dyn Fn(&[serde_json::Value]) + Send>;
type CollectionCallback = Box<dyn Fn(ChangeEvent) + Send>;

enum Subscription {
    View {
        view_name: String,
        callback: ViewCallback,
    },
    Collection {
        collection_name: String,
        callback: CollectionCallback,
    },
}

/// Manages subscriptions for change notifications.
struct SubscriptionManager {
    next_id: AtomicU64,
    subs: Mutex<HashMap<u64, Subscription>>,
}

impl SubscriptionManager {
    fn new() -> Self {
        SubscriptionManager {
            next_id: AtomicU64::new(1),
            subs: Mutex::new(HashMap::new()),
        }
    }

    fn add_view_sub(&self, view_name: &str, callback: ViewCallback) -> SubscriptionId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut subs = self.subs.lock().unwrap();
        subs.insert(
            id,
            Subscription::View {
                view_name: view_name.to_string(),
                callback,
            },
        );
        SubscriptionId(id)
    }

    fn add_collection_sub(&self, collection: &str, callback: CollectionCallback) -> SubscriptionId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut subs = self.subs.lock().unwrap();
        subs.insert(
            id,
            Subscription::Collection {
                collection_name: collection.to_string(),
                callback,
            },
        );
        SubscriptionId(id)
    }

    fn remove(&self, id: SubscriptionId) {
        let mut subs = self.subs.lock().unwrap();
        subs.remove(&id.0);
    }

    fn notify_view(&self, view_name: &str, data: &[serde_json::Value]) {
        let subs = self.subs.lock().unwrap();
        for sub in subs.values() {
            if let Subscription::View { view_name: vn, callback } = sub {
                if vn == view_name {
                    callback(data);
                }
            }
        }
    }

    fn notify_collection(&self, collection: &str, event: ChangeEvent) {
        let subs = self.subs.lock().unwrap();
        for sub in subs.values() {
            if let Subscription::Collection { collection_name, callback } = sub {
                if collection_name == collection {
                    callback(event.clone());
                }
            }
        }
    }
}

/// The main entry point for GroundDB.
/// Opens a data directory, parses the schema, manages the system database,
/// and provides collection handles for CRUD operations.
pub struct Store {
    root: PathBuf,
    schema: SchemaDefinition,
    schema_yaml: String,
    db: SystemDb,
    path_templates: HashMap<String, PathTemplate>,
    view_engine: ViewEngine,
    subscriptions: Arc<SubscriptionManager>,
    /// File watcher handle. None until `watch()` is called.
    _watcher: Mutex<Option<FileWatcher>>,
}

impl Store {
    /// Open a GroundDB store at the given data directory path.
    /// Parses schema.yaml, opens/creates _system.db, and runs the boot lifecycle.
    pub fn open(path: &str) -> Result<Self> {
        // Resolve to absolute path so file watcher events (which use absolute
        // paths) can be matched back to collections via strip_prefix.
        let root = {
            let p = PathBuf::from(path);
            if p.is_absolute() {
                p
            } else {
                std::env::current_dir()
                    .map_err(|e| GroundDbError::Other(format!(
                        "Failed to resolve data directory: {e}"
                    )))?
                    .join(p)
            }
        };
        if !root.exists() {
            return Err(GroundDbError::Other(format!(
                "Data directory does not exist: {}",
                root.display()
            )));
        }

        let schema_path = root.join("schema.yaml");
        if !schema_path.exists() {
            return Err(GroundDbError::Schema(format!(
                "schema.yaml not found in {}",
                root.display()
            )));
        }

        let schema_yaml = std::fs::read_to_string(&schema_path)?;
        let schema = parse_schema(&schema_path)?;

        let db_path = root.join("_system.db");
        let db = SystemDb::open(&db_path)?;

        // Parse all path templates
        let mut path_templates = HashMap::new();
        for (name, collection) in &schema.collections {
            let template = PathTemplate::parse(&collection.path)?;
            path_templates.insert(name.clone(), template);
        }

        let view_engine = ViewEngine::new(&schema)?;

        let store = Store {
            root,
            schema,
            schema_yaml,
            db,
            path_templates,
            view_engine,
            subscriptions: Arc::new(SubscriptionManager::new()),
            _watcher: Mutex::new(None),
        };

        store.boot()?;

        // Load cached view data
        store.view_engine.load_from_db(&store.db)?;

        Ok(store)
    }

    /// Boot lifecycle: check schema, scan collections, run migrations, rebuild views
    fn boot(&self) -> Result<()> {
        let current_hash = hash_schema(&self.schema_yaml);

        // Check schema hash
        let last_hash = self.db.get_last_schema_hash()?;
        if last_hash.as_deref() != Some(&current_hash) {
            // Schema changed (or first boot)
            // Run migration if there's a previous schema to diff against
            if let Some(old_yaml) = self.db.get_last_schema_yaml()? {
                self.run_schema_migration(&old_yaml)?;
            }
            self.db.record_schema(&current_hash, &self.schema_yaml)?;
            // On first boot or schema change, do a full scan
            self.full_scan()?;
        } else {
            // Schema unchanged -- incremental scan using directory hashes
            self.incremental_scan()?;
        }

        // Rebuild all static views so they are fresh on startup
        self.rebuild_all_static_views()?;

        Ok(())
    }

    /// Run schema migration: diff old vs new schema and apply safe changes.
    fn run_schema_migration(&self, old_yaml: &str) -> Result<()> {
        use crate::schema::parse_schema_str;

        let old_schema = match parse_schema_str(old_yaml) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("Failed to parse old schema for migration: {e}");
                return Ok(());
            }
        };

        let migrations = migration::diff_schemas(&old_schema, &self.schema);
        if migrations.is_empty() {
            return Ok(());
        }

        // Check for unsafe migrations
        let unsafe_migrations = migration::has_unsafe_migrations(&migrations);
        for m in &unsafe_migrations {
            match m {
                migration::SchemaMigration::FieldAdded { required: true, has_default: false, collection, field, .. } => {
                    return Err(GroundDbError::Schema(format!(
                        "Migration error: new required field '{}.{}' has no default value",
                        collection, field
                    )));
                }
                migration::SchemaMigration::FieldTypeChanged { collection, field } => {
                    return Err(GroundDbError::Schema(format!(
                        "Migration error: field type changed for '{}.{}'",
                        collection, field
                    )));
                }
                other => {
                    log::warn!("Schema migration warning: {}", other.describe());
                }
            }
        }

        // Apply safe migrations
        for m in &migrations {
            match m {
                migration::SchemaMigration::CollectionAdded { name } => {
                    let template = &self.path_templates[name];
                    let base_dir = self.root.join(template.base_directory());
                    if !base_dir.exists() {
                        std::fs::create_dir_all(&base_dir)?;
                    }
                    self.db.record_migration(&m.describe())?;
                }
                migration::SchemaMigration::FieldAdded { collection, field, has_default: true, .. } => {
                    // Backfill default value to documents missing this field
                    let field_def = &self.schema.collections[collection].fields[field];
                    if let Some(default_val) = &field_def.default {
                        let records = self.db.list_documents(collection)?;
                        for record in &records {
                            let mut data = record.parse_data()?;
                            if let Some(mapping) = data.as_mapping_mut() {
                                let key = serde_yaml::Value::String(field.clone());
                                if !mapping.contains_key(&key) {
                                    mapping.insert(key, default_val.clone());
                                    let file_path = self.root.join(&record.path);
                                    // Read existing document to preserve content and get timestamps
                                    let existing_doc = document::read_document(&file_path)?;
                                    document::write_document(&file_path, &data, existing_doc.content.as_deref())?;
                                    // Read timestamps from the updated file
                                    let meta = std::fs::metadata(&file_path)?;
                                    let created: chrono::DateTime<chrono::Utc> = meta
                                        .created()
                                        .unwrap_or(meta.modified()?)
                                        .into();
                                    let modified: chrono::DateTime<chrono::Utc> = meta.modified()?.into();
                                    self.db.upsert_document(
                                        &record.id,
                                        &record.collection,
                                        &record.path,
                                        &data,
                                        Some(&created.to_rfc3339()),
                                        Some(&modified.to_rfc3339()),
                                        existing_doc.content.as_deref(),
                                    )?;
                                }
                            }
                        }
                    }
                    self.db.record_migration(&m.describe())?;
                }
                migration::SchemaMigration::EnumValueAdded { .. } => {
                    // No action needed, just record it
                    self.db.record_migration(&m.describe())?;
                }
                migration::SchemaMigration::DefaultChanged { .. } => {
                    self.db.record_migration(&m.describe())?;
                }
                _ => {
                    // Unsafe migrations are either errored above or warned
                    log::info!("Skipping migration: {}", m.describe());
                }
            }
        }

        Ok(())
    }

    /// Rebuild all non-query-template (static) views.
    fn rebuild_all_static_views(&self) -> Result<()> {
        let view_names: Vec<String> = self.schema.views.keys().cloned().collect();
        for name in &view_names {
            if let Some(parsed) = self.view_engine.get_view(name) {
                if !parsed.is_query_template {
                    self.rebuild_view(name)?;
                }
            }
        }
        Ok(())
    }

    /// Full scan: read all documents in all collections, populate the index
    fn full_scan(&self) -> Result<()> {
        for (name, _collection) in &self.schema.collections {
            self.scan_collection(name)?;
        }
        Ok(())
    }

    /// Incremental scan: only scan collections whose directory hash changed
    fn incremental_scan(&self) -> Result<()> {
        for (name, _collection) in &self.schema.collections {
            let stored_hash = self.db.get_directory_hash(name)?;
            let current_hash = self.compute_collection_hash(name)?;

            if stored_hash.as_deref() != Some(&current_hash) {
                self.scan_collection(name)?;
            }
        }
        Ok(())
    }

    /// Scan a single collection: read all files, update the document index
    fn scan_collection(&self, name: &str) -> Result<()> {
        let collection = &self.schema.collections[name];
        let template = &self.path_templates[name];
        let base_dir = self.root.join(template.base_directory());

        if !base_dir.exists() {
            // Collection directory doesn't exist yet -- create it
            std::fs::create_dir_all(&base_dir)?;
            self.db
                .set_directory_hash(name, &compute_directory_hash(&[]))?;
            return Ok(());
        }

        // Find all matching files recursively
        let ext = collection.file_extension();
        let pattern = format!("{}/**/*.{}", base_dir.display(), ext);
        let files: Vec<PathBuf> = glob::glob(&pattern)
            .map_err(|e| GroundDbError::Other(format!("Glob error: {e}")))?
            .filter_map(|r| r.ok())
            .collect();

        // Clear existing documents for this collection and re-index
        self.db.delete_collection_documents(name)?;

        let mut entries = Vec::new();
        for file_path in &files {
            let doc = document::read_document(file_path)?;
            let rel_path = file_path
                .strip_prefix(&self.root)
                .unwrap_or(file_path)
                .to_string_lossy()
                .replace('\\', "/");

            let created_str = doc.created_at.to_rfc3339();
            let modified_str = doc.modified_at.to_rfc3339();
            self.db.upsert_document(
                &doc.id,
                name,
                &rel_path,
                &doc.data,
                Some(&created_str),
                Some(&modified_str),
                doc.content.as_deref(),
            )?;

            let mtime = std::fs::metadata(file_path)?
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            entries.push((
                file_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                mtime,
            ));
        }

        let hash = compute_directory_hash(&entries);
        self.db.set_directory_hash(name, &hash)?;

        Ok(())
    }

    /// Compute the current directory hash for a collection
    fn compute_collection_hash(&self, name: &str) -> Result<String> {
        let collection = &self.schema.collections[name];
        let template = &self.path_templates[name];
        let base_dir = self.root.join(template.base_directory());

        if !base_dir.exists() {
            return Ok(compute_directory_hash(&[]));
        }

        let ext = collection.file_extension();
        let pattern = format!("{}/**/*.{}", base_dir.display(), ext);
        let files: Vec<PathBuf> = glob::glob(&pattern)
            .map_err(|e| GroundDbError::Other(format!("Glob error: {e}")))?
            .filter_map(|r| r.ok())
            .collect();

        let mut entries = Vec::new();
        for file_path in &files {
            let mtime = std::fs::metadata(&file_path)?
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            entries.push((
                file_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                mtime,
            ));
        }

        Ok(compute_directory_hash(&entries))
    }

    /// Get a dynamic collection handle (uses serde_yaml::Value as the data type)
    pub fn collection(&self, name: &str) -> Result<Collection<'_>> {
        if !self.schema.collections.contains_key(name) {
            return Err(GroundDbError::Other(format!(
                "Collection '{name}' not found in schema"
            )));
        }
        Ok(Collection {
            store: self,
            name: name.to_string(),
        })
    }

    /// Get the schema definition
    pub fn schema(&self) -> &SchemaDefinition {
        &self.schema
    }

    /// Get the root data directory path
    pub fn root(&self) -> &Path {
        &self.root
    }

    // ── Typed API (used by codegen-generated StoreExt) ──────────────

    /// Get a typed document from a collection.
    pub fn get_document<T: DeserializeOwned>(
        &self,
        collection_name: &str,
        id: &str,
    ) -> Result<Document<T>> {
        let record = self
            .db
            .get_document(collection_name, id)?
            .ok_or_else(|| GroundDbError::NotFound {
                collection: collection_name.to_string(),
                id: id.to_string(),
            })?;

        let file_path = self.root.join(&record.path);
        let raw_doc = document::read_document(&file_path)?;
        let data: T = serde_yaml::from_value(raw_doc.data)?;

        Ok(Document {
            id: raw_doc.id,
            created_at: raw_doc.created_at,
            modified_at: raw_doc.modified_at,
            data,
            content: raw_doc.content,
        })
    }

    /// List all typed documents in a collection.
    pub fn list_documents<T: DeserializeOwned>(
        &self,
        collection_name: &str,
    ) -> Result<Vec<Document<T>>> {
        let records = self.db.list_documents(collection_name)?;
        let mut docs = Vec::new();

        for record in records {
            let file_path = self.root.join(&record.path);
            if file_path.exists() {
                if let Ok(raw_doc) = document::read_document(&file_path) {
                    if let Ok(data) = serde_yaml::from_value(raw_doc.data) {
                        docs.push(Document {
                            id: raw_doc.id,
                            created_at: raw_doc.created_at,
                            modified_at: raw_doc.modified_at,
                            data,
                            content: raw_doc.content,
                        });
                    }
                }
            }
        }

        Ok(docs)
    }

    /// Insert a new typed document. Returns the generated ID.
    pub fn insert_document<T: Serialize>(
        &self,
        collection_name: &str,
        data: &T,
        content: Option<&str>,
    ) -> Result<String> {
        let json_data = serde_json::to_value(data)?;
        self.insert_dynamic(collection_name, json_data, content)
    }

    /// Update a typed document.
    pub fn update_document<T: Serialize>(
        &self,
        collection_name: &str,
        id: &str,
        data: &T,
    ) -> Result<()> {
        let json_data = serde_json::to_value(data)?;
        self.update_dynamic(collection_name, id, json_data)
    }

    /// Partially update a typed document. Merges partial fields into the existing document.
    pub fn update_partial_document<T: Serialize>(
        &self,
        collection_name: &str,
        id: &str,
        partial: &T,
    ) -> Result<()> {
        let json_data = serde_json::to_value(partial)?;
        self.update_partial_dynamic(collection_name, id, json_data)
    }

    /// Delete a typed document.
    pub fn delete_document(&self, collection_name: &str, id: &str) -> Result<()> {
        self.delete_dynamic(collection_name, id)
    }

    /// Read a static view (no params), returning typed rows.
    pub fn read_view<T: DeserializeOwned>(&self, view_name: &str) -> Result<Vec<T>> {
        let json = self.view_dynamic(view_name)?;
        let rows: Vec<T> = serde_json::from_value(json)?;
        Ok(rows)
    }

    /// Execute a parameterized view/query, returning typed rows.
    pub fn query_view<T: DeserializeOwned, P: Serialize>(
        &self,
        view_name: &str,
        params: &P,
    ) -> Result<Vec<T>> {
        let params_json = serde_json::to_value(params)?;
        let params_map = json_to_string_map(&params_json);
        let json = self.query_dynamic(view_name, &params_map)?;
        let rows: Vec<T> = serde_json::from_value(json)?;
        Ok(rows)
    }

    // ── Dynamic (untyped) API for CLI and HTTP server ──────────────

    /// Get a single document by collection name and ID.
    /// Returns the document as a JSON value with id, fields, content, and timestamps.
    pub fn get_dynamic(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<serde_json::Value> {
        let col = self.collection(collection)?;
        let doc = col.get(id)?;
        doc_to_json(&doc)
    }

    /// List all documents in a collection, optionally filtered by field values.
    /// Filter keys match against document data fields.
    pub fn list_dynamic(
        &self,
        collection: &str,
        filters: &HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        let col = self.collection(collection)?;
        let docs = col.list()?;
        let items: Vec<serde_json::Value> = docs
            .iter()
            .filter_map(|doc| doc_to_json(doc).ok())
            .filter(|json| {
                filters.iter().all(|(key, value)| {
                    match json.get(key) {
                        Some(serde_json::Value::String(s)) => s == value,
                        Some(serde_json::Value::Number(n)) => &n.to_string() == value,
                        Some(serde_json::Value::Bool(b)) => &b.to_string() == value,
                        _ => false,
                    }
                })
            })
            .collect();
        Ok(serde_json::Value::Array(items))
    }

    /// Insert a new document into a collection.
    /// Returns the generated document ID.
    pub fn insert_dynamic(
        &self,
        collection: &str,
        data: serde_json::Value,
        content: Option<&str>,
    ) -> Result<String> {
        let col = self.collection(collection)?;
        let yaml_data = json_value_to_yaml(&data);
        col.insert(yaml_data, content)
    }

    /// Update an existing document's fields.
    pub fn update_dynamic(
        &self,
        collection: &str,
        id: &str,
        data: serde_json::Value,
    ) -> Result<()> {
        let col = self.collection(collection)?;
        let yaml_data = json_value_to_yaml(&data);
        col.update(id, yaml_data, None)
    }

    /// Partially update a document, merging the given fields into existing data.
    pub fn update_partial_dynamic(
        &self,
        collection: &str,
        id: &str,
        partial_data: serde_json::Value,
    ) -> Result<()> {
        let col = self.collection(collection)?;
        let yaml_data = json_value_to_yaml(&partial_data);
        col.update_partial(id, yaml_data, None)
    }

    /// Delete a document by collection name and ID.
    pub fn delete_dynamic(&self, collection: &str, id: &str) -> Result<()> {
        let col = self.collection(collection)?;
        col.delete(id)
    }

    /// Read a static view by name.
    pub fn view_dynamic(&self, name: &str) -> Result<serde_json::Value> {
        // Check view exists
        if !self.schema.views.contains_key(name) {
            return Err(GroundDbError::NotFound {
                collection: "views".to_string(),
                id: name.to_string(),
            });
        }

        // Check cached data first
        if let Some(data) = self.view_engine.get_view_data(name) {
            return Ok(serde_json::Value::Array(data));
        }

        // Check system DB cache
        if let Some(json_str) = self.db.get_view_data(name)? {
            let val: serde_json::Value = serde_json::from_str(&json_str)?;
            return Ok(val);
        }

        // No cached data — return empty for now (views are rebuilt on document changes)
        Ok(serde_json::Value::Array(vec![]))
    }

    /// Execute a parameterized query/view with the given parameters.
    pub fn query_dynamic(
        &self,
        name: &str,
        params: &HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        // Verify the view exists in the schema
        if !self.schema.views.contains_key(name) {
            return Err(GroundDbError::NotFound {
                collection: "views".to_string(),
                id: name.to_string(),
            });
        }

        let parsed = match self.view_engine.get_view(name) {
            Some(p) => p.clone(),
            None => return Ok(serde_json::Value::Array(vec![])),
        };

        // Rewrite the view SQL into CTE-wrapped form
        let rewritten = view_engine::rewrite_view_sql(&parsed, &self.schema)?;

        // Execute with named parameter bindings
        let results = self.db.query_documents_sql(&rewritten.sql, params)?;

        Ok(serde_json::Value::Array(results))
    }

    /// Show pending schema migrations (dry-run or apply).
    pub fn migrate(&self, dry_run: bool) -> Result<serde_json::Value> {
        use crate::schema::parse_schema_str;

        let old_yaml = self.db.get_last_schema_yaml()?;
        if old_yaml.is_none() {
            return Ok(serde_json::json!({
                "message": "No previous schema found. This is the first schema version.",
                "migrations": []
            }));
        }
        let old_yaml = old_yaml.unwrap();

        let old_schema = match parse_schema_str(&old_yaml) {
            Ok(s) => s,
            Err(e) => {
                return Ok(serde_json::json!({
                    "error": format!("Failed to parse old schema: {e}"),
                    "migrations": []
                }));
            }
        };

        let migrations = migration::diff_schemas(&old_schema, &self.schema);

        let descriptions: Vec<serde_json::Value> = migrations
            .iter()
            .map(|m| {
                serde_json::json!({
                    "description": m.describe(),
                    "safe": m.is_safe()
                })
            })
            .collect();

        if dry_run {
            Ok(serde_json::json!({
                "dry_run": true,
                "migration_count": migrations.len(),
                "migrations": descriptions
            }))
        } else {
            // Actually apply -- done at boot time, but we can re-run
            self.run_schema_migration(&old_yaml)?;
            Ok(serde_json::json!({
                "ok": true,
                "applied": migrations.len(),
                "migrations": descriptions
            }))
        }
    }

    /// Explain a view: return the rewritten SQL and metadata for debugging.
    pub fn explain_view(&self, name: &str) -> Result<serde_json::Value> {
        let parsed = self
            .view_engine
            .get_view(name)
            .ok_or_else(|| GroundDbError::NotFound {
                collection: "views".to_string(),
                id: name.to_string(),
            })?
            .clone();

        let rewritten = view_engine::rewrite_view_sql(&parsed, &self.schema)?;

        let ref_collections = parsed.referenced_collections();
        let collections: Vec<&str> = ref_collections
            .iter()
            .map(|s| s.as_str())
            .collect();

        Ok(serde_json::json!({
            "view": name,
            "original_sql": parsed.original_sql.trim(),
            "rewritten_sql": rewritten.sql,
            "collections": collections,
            "limit": rewritten.original_limit,
            "buffer_limit": rewritten.buffer_limit,
            "is_query_template": parsed.is_query_template,
            "param_names": rewritten.param_names,
        }))
    }

    /// Validate all documents in all collections against the schema.
    /// Returns a report of validation results.
    pub fn validate_all(&self) -> Result<serde_json::Value> {
        let mut results = serde_json::Map::new();

        for (name, collection_def) in &self.schema.collections {
            let col = self.collection(name)?;
            let docs = col.list()?;
            let mut col_results = Vec::new();

            for doc in &docs {
                let vr = validation::validate_document(&self.schema, collection_def, &doc.data);
                if !vr.is_ok() || vr.has_warnings() {
                    let mut entry = serde_json::Map::new();
                    entry.insert("id".into(), serde_json::Value::String(doc.id.clone()));
                    if !vr.errors.is_empty() {
                        entry.insert(
                            "errors".into(),
                            serde_json::Value::Array(
                                vr.errors.iter().map(|e| serde_json::Value::String(e.clone())).collect(),
                            ),
                        );
                    }
                    if !vr.warnings.is_empty() {
                        entry.insert(
                            "warnings".into(),
                            serde_json::Value::Array(
                                vr.warnings.iter().map(|w| serde_json::Value::String(w.clone())).collect(),
                            ),
                        );
                    }
                    col_results.push(serde_json::Value::Object(entry));
                }
            }

            results.insert(
                name.clone(),
                serde_json::json!({
                    "total": docs.len(),
                    "issues": col_results,
                }),
            );
        }

        Ok(serde_json::Value::Object(results))
    }

    /// Get status information: schema hash, collection stats, view health.
    pub fn status(&self) -> Result<serde_json::Value> {
        let schema_hash = hash_schema(&self.schema_yaml);
        let mut collections = serde_json::Map::new();

        for name in self.schema.collections.keys() {
            let docs = self.db.list_documents(name)?;
            collections.insert(
                name.clone(),
                serde_json::json!({ "count": docs.len() }),
            );
        }

        Ok(serde_json::json!({
            "schema_hash": schema_hash,
            "collections": collections,
            "views": self.schema.views.keys().collect::<Vec<_>>(),
        }))
    }

    /// Create a batch for all-or-nothing execution of multiple write operations.
    pub fn batch(&self) -> Batch<'_> {
        Batch {
            store: self,
            ops: Vec::new(),
        }
    }

    /// Force rebuild of indexes and views, optionally for a specific collection.
    pub fn rebuild(&self, collection: Option<&str>) -> Result<()> {
        match collection {
            Some(name) => {
                self.scan_collection(name)?;
                // Rebuild views affected by this collection
                let affected = self.view_engine.affected_views(name);
                for view_name in affected {
                    if let Some(parsed) = self.view_engine.get_view(view_name) {
                        if !parsed.is_query_template {
                            self.rebuild_view(view_name)?;
                        }
                    }
                }
                Ok(())
            }
            None => {
                self.full_scan()?;
                self.rebuild_all_static_views()
            }
        }
    }

    // ── Subscription API ────────────────────────────────────────────

    /// Subscribe to changes on a specific view. Callback fires when view data changes.
    pub fn on_view_change(
        &self,
        view_name: &str,
        callback: Box<dyn Fn(&[serde_json::Value]) + Send>,
    ) -> SubscriptionId {
        self.subscriptions.add_view_sub(view_name, callback)
    }

    /// Subscribe to changes on a specific collection. Callback fires on insert/update/delete.
    pub fn on_collection_change(
        &self,
        collection: &str,
        callback: Box<dyn Fn(ChangeEvent) + Send>,
    ) -> SubscriptionId {
        self.subscriptions.add_collection_sub(collection, callback)
    }

    /// Unsubscribe from change notifications.
    pub fn unsubscribe(&self, id: SubscriptionId) {
        self.subscriptions.remove(id);
    }

    // ── File Watching ───────────────────────────────────────────────

    /// Start watching collection directories for external file changes.
    /// When a file is created, modified, or deleted externally, the index
    /// and affected views are updated automatically.
    ///
    /// Returns a `WatcherHandle` that the caller should use to poll for events
    /// via `process_watcher_events()`, e.g. on a timer or in an event loop.
    pub fn watch(&self) -> Result<()> {
        let dirs: Vec<PathBuf> = self
            .path_templates
            .values()
            .map(|t| PathBuf::from(t.base_directory()))
            .collect();

        let watcher = FileWatcher::start(&self.root, &dirs)
            .map_err(|e| GroundDbError::Other(format!("Failed to start file watcher: {e}")))?;

        let mut guard = self._watcher.lock().unwrap();
        *guard = Some(watcher);
        Ok(())
    }

    /// Process any pending file watcher events. Call this periodically
    /// (e.g. on a timer or after receiving a notification) to apply
    /// external file changes to the index and views.
    pub fn process_watcher_events(&self) -> Result<()> {
        let guard = self._watcher.lock().unwrap();
        let watcher = match guard.as_ref() {
            Some(w) => w,
            None => return Ok(()),
        };

        // Drain all pending events (non-blocking)
        let mut events = Vec::new();
        while let Ok(event) = watcher.event_rx.try_recv() {
            events.push(event);
        }
        drop(guard); // Release lock before doing work

        if events.is_empty() {
            return Ok(());
        }

        // Group by collection so we can batch updates
        let mut affected_collections = std::collections::HashSet::new();
        for event in &events {
            if let Some(collection_name) = self.collection_for_path(&event.path) {
                affected_collections.insert(collection_name.clone());
                self.process_single_watcher_event(&collection_name, event)?;
            }
        }

        // Rebuild affected views
        for collection_name in &affected_collections {
            let hash = self.compute_collection_hash(collection_name)?;
            self.db.set_directory_hash(collection_name, &hash)?;

            let affected_views = self.view_engine.affected_views(collection_name);
            for view_name in affected_views {
                if let Some(parsed) = self.view_engine.get_view(view_name) {
                    if !parsed.is_query_template {
                        self.rebuild_view(view_name)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Determine which collection a file path belongs to.
    fn collection_for_path(&self, path: &Path) -> Option<String> {
        let rel = path.strip_prefix(&self.root).ok()?;
        let rel_str = rel.to_string_lossy().replace('\\', "/");

        for (name, template) in &self.path_templates {
            let base = template.base_directory();
            if rel_str.starts_with(&base) {
                return Some(name.clone());
            }
        }
        None
    }

    /// Process a single file watcher event: update the document index.
    fn process_single_watcher_event(
        &self,
        collection_name: &str,
        event: &WatcherEvent,
    ) -> Result<()> {
        let rel_path = event
            .path
            .strip_prefix(&self.root)
            .unwrap_or(&event.path)
            .to_string_lossy()
            .replace('\\', "/");

        match event.kind {
            ChangeKind::Created | ChangeKind::Modified => {
                if event.path.exists() {
                    let mut doc = document::read_document(&event.path)?;

                    // Reconcile path-extracted values with YAML front matter.
                    // When a file is moved between directories, the path may
                    // encode a new value for a field (e.g. status: published).
                    if let Some(template) = self.path_templates.get(collection_name) {
                        if let Some(extracted) = template.extract(&rel_path) {
                            let col_def = self.schema.collections.get(collection_name);
                            let mut changed = false;

                            for segment in &template.segments {
                                let (field_name, has_format) = match segment {
                                    PathSegment::Field { name, format } => (name, format.is_some()),
                                    _ => continue,
                                };

                                // Skip fields that shouldn't be reconciled
                                if field_name == "id" || has_format {
                                    continue;
                                }

                                let path_value = match extracted.get(field_name) {
                                    Some(v) => v,
                                    None => continue,
                                };

                                // Get current YAML value for this field
                                let current_slug = doc.data
                                    .as_mapping()
                                    .and_then(|m| m.get(serde_yaml::Value::String(field_name.clone())))
                                    .and_then(|v| v.as_str())
                                    .map(path_template::slugify);

                                if current_slug.as_deref() == Some(path_value) {
                                    continue; // already matches
                                }

                                // Determine the value to write back into YAML.
                                // For enum fields, find the original variant whose
                                // slug matches the extracted path value.
                                let new_value = col_def
                                    .and_then(|c| c.fields.get(field_name))
                                    .and_then(|f| f.enum_values.as_ref())
                                    .and_then(|variants| {
                                        variants.iter().find(|v| path_template::slugify(v) == *path_value)
                                    })
                                    .cloned()
                                    .unwrap_or_else(|| path_value.clone());

                                if let Some(map) = doc.data.as_mapping_mut() {
                                    map.insert(
                                        serde_yaml::Value::String(field_name.clone()),
                                        serde_yaml::Value::String(new_value),
                                    );
                                    changed = true;
                                }
                            }

                            if changed {
                                document::write_document(
                                    &event.path,
                                    &doc.data,
                                    doc.content.as_deref(),
                                )?;
                            }
                        }
                    }

                    let created_str = doc.created_at.to_rfc3339();
                    let modified_str = doc.modified_at.to_rfc3339();
                    self.db.upsert_document(
                        &doc.id,
                        collection_name,
                        &rel_path,
                        &doc.data,
                        Some(&created_str),
                        Some(&modified_str),
                        doc.content.as_deref(),
                    )?;

                    let change = if event.kind == ChangeKind::Created {
                        let json_data = serde_json::to_value(&doc.data)?;
                        ChangeEvent::Inserted {
                            id: doc.id,
                            data: json_data,
                        }
                    } else {
                        let json_data = serde_json::to_value(&doc.data)?;
                        ChangeEvent::Updated {
                            id: doc.id,
                            data: json_data,
                        }
                    };
                    self.subscriptions.notify_collection(collection_name, change);
                } else {
                    // File no longer exists at this path — this is the "from" side
                    // of a rename/move event. Treat it as a delete so stale records
                    // are cleaned up.
                    let id = event
                        .path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    if !id.is_empty() {
                        self.db.delete_document(collection_name, &id)?;
                        self.subscriptions.notify_collection(
                            collection_name,
                            ChangeEvent::Deleted { id },
                        );
                    }
                }
            }
            ChangeKind::Deleted => {
                // Extract ID from the filename
                let id = event
                    .path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                if !id.is_empty() {
                    self.db.delete_document(collection_name, &id)?;
                    self.subscriptions.notify_collection(
                        collection_name,
                        ChangeEvent::Deleted { id },
                    );
                }
            }
        }

        Ok(())
    }

    /// Called after any write (insert/update/delete) to a collection.
    /// Updates the directory hash and rebuilds affected views.
    fn post_write(&self, collection_name: &str) -> Result<()> {
        // Update directory hash for this collection
        let hash = self.compute_collection_hash(collection_name)?;
        self.db.set_directory_hash(collection_name, &hash)?;

        // Rebuild affected static views
        let affected = self.view_engine.affected_views(collection_name);
        for view_name in affected {
            if let Some(parsed) = self.view_engine.get_view(view_name) {
                // Only rebuild non-query-template (static) views
                if !parsed.is_query_template {
                    self.rebuild_view(view_name)?;
                }
            }
        }

        Ok(())
    }

    /// Rebuild a single static view by executing rewritten SQL against the documents table.
    fn rebuild_view(&self, view_name: &str) -> Result<()> {
        let parsed = match self.view_engine.get_view(view_name) {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        // Rewrite the view SQL into CTE-wrapped form
        let rewritten = view_engine::rewrite_view_sql(&parsed, &self.schema)?;

        // For buffered views, apply buffer_limit via SQL LIMIT
        let exec_sql = if let Some(buffer_limit) = rewritten.buffer_limit {
            // Replace or append LIMIT with the buffer limit
            // The original SQL already has a LIMIT; we need the buffer-extended version
            // Strategy: strip any existing LIMIT from the CTE-wrapped SQL and add our own
            let base = strip_limit(&rewritten.sql);
            format!("{base} LIMIT {buffer_limit}")
        } else {
            rewritten.sql.clone()
        };

        // Execute against the documents table
        let empty_params = HashMap::new();
        let rows = self.db.query_documents_sql(&exec_sql, &empty_params)?;

        // Update in-memory cache and persist to DB
        let json_str = serde_json::to_string(&rows)?;
        self.db.set_view_data(view_name, &json_str)?;
        self.view_engine.set_view_data(view_name, rows.clone());

        // Notify view subscribers
        self.subscriptions.notify_view(view_name, &rows);

        // Materialize if needed
        if parsed.materialize {
            self.view_engine.materialize_view(&self.root, view_name)?;
        }

        Ok(())
    }
}

// ── Batch Operations ───────────────────────────────────────────

/// A deferred write operation for batch execution.
enum BatchOp {
    Insert {
        collection: String,
        data: serde_json::Value,
        content: Option<String>,
    },
    Update {
        collection: String,
        id: String,
        data: serde_json::Value,
    },
    Delete {
        collection: String,
        id: String,
    },
}

/// A batch of write operations that execute all-or-nothing.
/// On failure, files written during the batch are rolled back.
pub struct Batch<'a> {
    store: &'a Store,
    ops: Vec<BatchOp>,
}

/// A scoped handle for queuing batch writes to a specific collection.
pub struct BatchCollection<'a, 'b> {
    batch: &'b mut Batch<'a>,
    collection: String,
}

impl<'a> Batch<'a> {
    /// Get a handle for queuing operations on a collection.
    pub fn collection(&mut self, name: &str) -> BatchCollection<'a, '_> {
        BatchCollection {
            batch: self,
            collection: name.to_string(),
        }
    }

    /// Execute all queued operations atomically.
    /// If any operation fails, all file changes in this batch are rolled back:
    /// created files are removed, and updated/deleted files are restored.
    pub fn execute(self) -> Result<Vec<String>> {
        // Track file changes for rollback
        let mut created_files: Vec<PathBuf> = Vec::new();
        // (path, original_content) for files that were modified or deleted
        let mut saved_files: Vec<(PathBuf, Vec<u8>)> = Vec::new();
        let mut results: Vec<String> = Vec::new();

        // Begin a DB transaction
        self.store.db.begin_transaction()?;

        for op in &self.ops {
            let res = match op {
                BatchOp::Insert { collection, data, content } => {
                    self.store
                        .insert_dynamic(collection, data.clone(), content.as_deref())
                        .map(|id| {
                            results.push(id.clone());
                            // Track the file that was created
                            if let Ok(Some(record)) = self.store.db.get_document(collection, &id) {
                                created_files.push(self.store.root.join(&record.path));
                            }
                        })
                }
                BatchOp::Update { collection, id, data } => {
                    // Save old file content before updating
                    if let Ok(Some(record)) = self.store.db.get_document(collection, id) {
                        let file_path = self.store.root.join(&record.path);
                        if let Ok(content) = std::fs::read(&file_path) {
                            saved_files.push((file_path, content));
                        }
                    }
                    self.store
                        .update_dynamic(collection, id, data.clone())
                        .map(|_| {
                            results.push(id.clone());
                        })
                }
                BatchOp::Delete { collection, id } => {
                    // Save old file content before deleting
                    if let Ok(Some(record)) = self.store.db.get_document(collection, id) {
                        let file_path = self.store.root.join(&record.path);
                        if let Ok(content) = std::fs::read(&file_path) {
                            saved_files.push((file_path, content));
                        }
                    }
                    self.store
                        .delete_dynamic(collection, id)
                        .map(|_| {
                            results.push(id.clone());
                        })
                }
            };

            if let Err(e) = res {
                // Roll back: remove files created during this batch
                for path in &created_files {
                    let _ = std::fs::remove_file(path);
                }
                // Restore files that were modified or deleted
                for (path, content) in &saved_files {
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let _ = std::fs::write(path, content);
                }
                self.store.db.rollback_transaction()?;
                return Err(e);
            }
        }

        self.store.db.commit_transaction()?;
        Ok(results)
    }
}

impl<'a, 'b> BatchCollection<'a, 'b> {
    /// Queue an insert operation.
    pub fn insert(&mut self, data: serde_json::Value, content: Option<&str>) -> &mut Self {
        self.batch.ops.push(BatchOp::Insert {
            collection: self.collection.clone(),
            data,
            content: content.map(|s| s.to_string()),
        });
        self
    }

    /// Queue an update operation.
    pub fn update(&mut self, id: &str, data: serde_json::Value) -> &mut Self {
        self.batch.ops.push(BatchOp::Update {
            collection: self.collection.clone(),
            id: id.to_string(),
            data,
        });
        self
    }

    /// Queue a delete operation.
    pub fn delete(&mut self, id: &str) -> &mut Self {
        self.batch.ops.push(BatchOp::Delete {
            collection: self.collection.clone(),
            id: id.to_string(),
        });
        self
    }
}

/// A handle to a collection within a store.
/// Provides CRUD operations using serde_yaml::Value for dynamic data.
pub struct Collection<'a> {
    store: &'a Store,
    name: String,
}

impl<'a> Collection<'a> {
    fn definition(&self) -> &CollectionDefinition {
        &self.store.schema.collections[&self.name]
    }

    fn template(&self) -> &PathTemplate {
        &self.store.path_templates[&self.name]
    }

    /// Get a document by ID
    pub fn get(&self, id: &str) -> Result<Document<serde_yaml::Value>> {
        let record = self
            .store
            .db
            .get_document(&self.name, id)?
            .ok_or_else(|| GroundDbError::NotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            })?;

        let file_path = self.store.root.join(&record.path);
        document::read_document(&file_path)
    }

    /// List all documents in this collection
    pub fn list(&self) -> Result<Vec<Document<serde_yaml::Value>>> {
        let records = self.store.db.list_documents(&self.name)?;
        let mut docs = Vec::new();

        for record in &records {
            let file_path = self.store.root.join(&record.path);
            if file_path.exists() {
                match document::read_document(&file_path) {
                    Ok(doc) => docs.push(doc),
                    Err(e) => {
                        log::warn!("Failed to read document {}: {}", record.path, e);
                    }
                }
            }
        }

        Ok(docs)
    }

    /// Insert a new document. Returns the document ID.
    pub fn insert(
        &self,
        mut data: serde_yaml::Value,
        content: Option<&str>,
    ) -> Result<String> {
        let definition = self.definition();

        if definition.readonly {
            return Err(GroundDbError::Other(format!(
                "Collection '{}' is readonly",
                self.name
            )));
        }

        // Apply defaults and validate
        validation::validate_and_prepare(&self.store.schema, definition, &mut data)?;

        // Generate or determine ID
        let id = self.determine_id(&data)?;

        // Compute target path
        let template = self.template();
        let rel_path = template.render(&data, Some(&id))?;
        let abs_path = self.store.root.join(&rel_path);

        // Check for path conflict
        if abs_path.exists() {
            match definition.on_conflict() {
                OnConflict::Error => {
                    return Err(GroundDbError::PathConflict { path: rel_path });
                }
                OnConflict::Suffix => {
                    let resolved = path_template::resolve_suffix(&rel_path, |p| {
                        self.store.root.join(p).exists()
                    });
                    let abs_resolved = self.store.root.join(&resolved);

                    // Write the file
                    document::write_document(&abs_resolved, &data, content)?;

                    // Extract ID from the resolved filename
                    let resolved_id = Path::new(&resolved)
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&id)
                        .to_string();

                    // Read timestamps from the newly written file
                    let meta = std::fs::metadata(&abs_resolved)?;
                    let created: chrono::DateTime<chrono::Utc> = meta
                        .created()
                        .unwrap_or(meta.modified()?)
                        .into();
                    let modified: chrono::DateTime<chrono::Utc> = meta.modified()?.into();

                    // Update the index
                    self.store.db.upsert_document(
                        &resolved_id,
                        &self.name,
                        &resolved,
                        &data,
                        Some(&created.to_rfc3339()),
                        Some(&modified.to_rfc3339()),
                        content,
                    )?;

                    self.store.post_write(&self.name)?;
                    self.store.subscriptions.notify_collection(
                        &self.name,
                        ChangeEvent::Inserted {
                            id: resolved_id.clone(),
                            data: serde_json::to_value(&data)?,
                        },
                    );
                    return Ok(resolved_id);
                }
            }
        }

        // Write the file
        document::write_document(&abs_path, &data, content)?;

        // Read timestamps from the newly written file
        let meta = std::fs::metadata(&abs_path)?;
        let created: chrono::DateTime<chrono::Utc> = meta
            .created()
            .unwrap_or(meta.modified()?)
            .into();
        let modified: chrono::DateTime<chrono::Utc> = meta.modified()?.into();

        // Update the index
        self.store.db.upsert_document(
            &id,
            &self.name,
            &rel_path,
            &data,
            Some(&created.to_rfc3339()),
            Some(&modified.to_rfc3339()),
            content,
        )?;

        self.store.post_write(&self.name)?;
        self.store.subscriptions.notify_collection(
            &self.name,
            ChangeEvent::Inserted {
                id: id.clone(),
                data: serde_json::to_value(&data)?,
            },
        );
        Ok(id)
    }

    /// Update an existing document. Handles file movement if path-relevant fields changed.
    pub fn update(
        &self,
        id: &str,
        mut data: serde_yaml::Value,
        content: Option<&str>,
    ) -> Result<()> {
        let definition = self.definition();

        if definition.readonly {
            return Err(GroundDbError::Other(format!(
                "Collection '{}' is readonly",
                self.name
            )));
        }

        // Get the existing document record
        let record = self
            .store
            .db
            .get_document(&self.name, id)?
            .ok_or_else(|| GroundDbError::NotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            })?;

        // Apply defaults and validate
        validation::validate_and_prepare(&self.store.schema, definition, &mut data)?;

        // Compute new path
        let template = self.template();
        let new_rel_path = template.render(&data, Some(id))?;
        let old_abs_path = self.store.root.join(&record.path);
        let new_abs_path = self.store.root.join(&new_rel_path);

        if record.path != new_rel_path {
            // Path changed -- file needs to move
            // Write to new location first
            document::write_document(&new_abs_path, &data, content)?;
            // Delete old file
            if old_abs_path.exists() {
                document::delete_document(&old_abs_path)?;
            }
        } else {
            // Same path -- just update the file
            document::write_document(&new_abs_path, &data, content)?;
        }

        // Read timestamps from the written file
        let meta = std::fs::metadata(&new_abs_path)?;
        let created: chrono::DateTime<chrono::Utc> = meta
            .created()
            .unwrap_or(meta.modified()?)
            .into();
        let modified: chrono::DateTime<chrono::Utc> = meta.modified()?.into();

        // Update the index
        self.store.db.upsert_document(
            id,
            &self.name,
            &new_rel_path,
            &data,
            Some(&created.to_rfc3339()),
            Some(&modified.to_rfc3339()),
            content,
        )?;

        self.store.post_write(&self.name)?;
        self.store.subscriptions.notify_collection(
            &self.name,
            ChangeEvent::Updated {
                id: id.to_string(),
                data: serde_json::to_value(&data)?,
            },
        );
        Ok(())
    }

    /// Partially update a document. Merges the given partial data into the existing
    /// document data, only overwriting fields that are present and non-null.
    pub fn update_partial(
        &self,
        id: &str,
        partial: serde_yaml::Value,
        content: Option<&str>,
    ) -> Result<()> {
        // Read existing document
        let existing = self.get(id)?;
        let mut merged = existing.data;

        // Merge partial data into existing
        if let (Some(base_map), Some(partial_map)) =
            (merged.as_mapping_mut(), partial.as_mapping())
        {
            for (key, value) in partial_map {
                if *value != serde_yaml::Value::Null {
                    base_map.insert(key.clone(), value.clone());
                }
            }
        }

        // Use the existing content if no new content was provided
        let effective_content = content.or(existing.content.as_deref());

        self.update(id, merged, effective_content)
    }

    /// Delete a document by ID. Enforces referential integrity.
    pub fn delete(&self, id: &str) -> Result<()> {
        let definition = self.definition();

        if definition.readonly {
            return Err(GroundDbError::Other(format!(
                "Collection '{}' is readonly",
                self.name
            )));
        }

        // Get the existing document record
        let record = self
            .store
            .db
            .get_document(&self.name, id)?
            .ok_or_else(|| GroundDbError::NotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            })?;

        // Check referential integrity
        self.check_referential_integrity(id)?;

        // Delete the file
        let abs_path = self.store.root.join(&record.path);
        if abs_path.exists() {
            document::delete_document(&abs_path)?;
        }

        // Remove from index
        self.store.db.delete_document(&self.name, id)?;

        self.store.post_write(&self.name)?;
        self.store.subscriptions.notify_collection(
            &self.name,
            ChangeEvent::Deleted {
                id: id.to_string(),
            },
        );
        Ok(())
    }

    /// Check if deleting this document would violate referential integrity.
    /// Examines all documents that reference this one and applies on_delete policies.
    fn check_referential_integrity(&self, id: &str) -> Result<()> {
        let refs = self.store.db.find_references(&self.name, id)?;

        if refs.is_empty() {
            return Ok(());
        }

        // Check each referencing document's collection schema for on_delete policies
        for ref_doc in &refs {
            if let Some(ref_collection) = self.store.schema.collections.get(&ref_doc.collection) {
                for (field_name, field_def) in &ref_collection.fields {
                    if field_def.field_type == FieldType::Ref {
                        if let Some(target) = &field_def.target {
                            if target.targets().contains(&self.name.as_str()) {
                                // This field references our collection
                                let policy = field_def
                                    .effective_on_delete(ref_collection.on_delete.as_ref());

                                // Check if this document actually references us
                                let data = ref_doc.parse_data()?;
                                if let Some(val) = data.get(field_name) {
                                    let ref_id = match val {
                                        serde_yaml::Value::String(s) => Some(s.as_str()),
                                        serde_yaml::Value::Mapping(m) => m
                                            .get(&serde_yaml::Value::String("id".into()))
                                            .and_then(|v| v.as_str()),
                                        _ => None,
                                    };

                                    if ref_id == Some(id) {
                                        match policy {
                                            OnDeletePolicy::Error => {
                                                return Err(GroundDbError::ReferentialIntegrity(
                                                    format!(
                                                        "Cannot delete {}/{}: referenced by {}/{} (field '{}')",
                                                        self.name, id, ref_doc.collection, ref_doc.id, field_name
                                                    ),
                                                ));
                                            }
                                            OnDeletePolicy::Cascade => {
                                                // Delete the referencing document
                                                let ref_col =
                                                    self.store.collection(&ref_doc.collection)?;
                                                ref_col.delete(&ref_doc.id)?;
                                            }
                                            OnDeletePolicy::Nullify => {
                                                // Set the reference field to null
                                                let mut data = ref_doc.parse_data()?;
                                                if let Some(mapping) = data.as_mapping_mut() {
                                                    mapping.insert(
                                                        serde_yaml::Value::String(
                                                            field_name.clone(),
                                                        ),
                                                        serde_yaml::Value::Null,
                                                    );
                                                }
                                                let file_path =
                                                    self.store.root.join(&ref_doc.path);
                                                // Read the existing document to preserve content
                                                let existing_doc = document::read_document(&file_path)?;
                                                document::write_document(
                                                    &file_path, &data, existing_doc.content.as_deref(),
                                                )?;
                                                // Read timestamps from the updated file
                                                let meta = std::fs::metadata(&file_path)?;
                                                let created: chrono::DateTime<chrono::Utc> = meta
                                                    .created()
                                                    .unwrap_or(meta.modified()?)
                                                    .into();
                                                let modified: chrono::DateTime<chrono::Utc> = meta.modified()?.into();
                                                self.store.db.upsert_document(
                                                    &ref_doc.id,
                                                    &ref_doc.collection,
                                                    &ref_doc.path,
                                                    &data,
                                                    Some(&created.to_rfc3339()),
                                                    Some(&modified.to_rfc3339()),
                                                    existing_doc.content.as_deref(),
                                                )?;
                                            }
                                            OnDeletePolicy::Archive => {
                                                // Move to _archive/ subdirectory
                                                let old_path =
                                                    self.store.root.join(&ref_doc.path);
                                                let archive_path = self
                                                    .store
                                                    .root
                                                    .join("_archive")
                                                    .join(&ref_doc.path);
                                                document::move_document(&old_path, &archive_path)?;
                                                self.store
                                                    .db
                                                    .delete_document(
                                                        &ref_doc.collection,
                                                        &ref_doc.id,
                                                    )?;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Determine the document ID: either from the data (filename-derived) or auto-generated
    fn determine_id(&self, data: &serde_yaml::Value) -> Result<String> {
        let definition = self.definition();

        // Check for auto-generated ID
        if let Some(strategy) = definition.auto_id() {
            return Ok(match strategy {
                AutoIdStrategy::Ulid => ulid::Ulid::new().to_string().to_lowercase(),
                AutoIdStrategy::Uuid => uuid::Uuid::new_v4().to_string(),
                AutoIdStrategy::Nanoid => nanoid::nanoid!(),
            });
        }

        // For path-based IDs, render the template and extract the filename stem
        let template = self.template();
        let rendered = template.render(data, None)?;
        let id = Path::new(&rendered)
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                GroundDbError::Other(format!(
                    "Cannot extract ID from rendered path: {rendered}"
                ))
            })?
            .to_string();

        Ok(id)
    }
}

/// Convert a Document to a JSON value for the dynamic API
fn doc_to_json(doc: &Document<serde_yaml::Value>) -> Result<serde_json::Value> {
    let data_json = serde_json::to_value(&doc.data)?;

    let mut obj = serde_json::Map::new();
    obj.insert("id".into(), serde_json::Value::String(doc.id.clone()));
    obj.insert(
        "created_at".into(),
        serde_json::Value::String(doc.created_at.to_rfc3339()),
    );
    obj.insert(
        "modified_at".into(),
        serde_json::Value::String(doc.modified_at.to_rfc3339()),
    );

    // Merge data fields into the top level
    if let serde_json::Value::Object(fields) = data_json {
        for (k, v) in fields {
            obj.insert(k, v);
        }
    }

    if let Some(content) = &doc.content {
        obj.insert("content".into(), serde_json::Value::String(content.clone()));
    }

    Ok(serde_json::Value::Object(obj))
}


/// Strip a trailing LIMIT clause from SQL. Used to replace the user's LIMIT with
/// a buffer-extended LIMIT for buffered views.
///
/// Only strips a LIMIT that appears at the very end of the SQL (after trimming),
/// not one embedded inside a CTE or subquery. Handles optional trailing semicolons.
fn strip_limit(sql: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Find the last occurrence of LIMIT preceded by whitespace (space, newline, tab)
    // We search for "LIMIT " and check the character before it is whitespace
    for candidate in find_all_positions(&upper, "LIMIT ") {
        if candidate == 0 {
            continue;
        }
        let before = trimmed.as_bytes()[candidate - 1];
        if before == b' ' || before == b'\n' || before == b'\r' || before == b'\t' {
            let after_limit = &trimmed[candidate + 6..].trim();
            // Verify what follows LIMIT is just a number (possibly with whitespace)
            if after_limit.chars().all(|c| c.is_ascii_digit() || c.is_whitespace()) {
                return trimmed[..candidate - 1].trim_end().to_string();
            }
        }
    }
    trimmed.to_string()
}

/// Find all positions of a substring in a string, returning them in reverse order
/// (last match first) for use with strip_limit's "last LIMIT" logic.
fn find_all_positions(haystack: &str, needle: &str) -> Vec<usize> {
    let mut positions = Vec::new();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        positions.push(start + pos);
        start += pos + 1;
    }
    positions.reverse();
    positions
}

/// Convert a JSON value to a HashMap<String, String> for query parameters.
fn json_to_string_map(json: &serde_json::Value) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(obj) = json.as_object() {
        for (k, v) in obj {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => v.to_string(),
            };
            map.insert(k.clone(), s);
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_store() -> (TempDir, Store) {
        let tmp = TempDir::new().unwrap();
        let schema = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }
    additional_properties: false
    strict: true
    on_delete: error

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    id: { on_conflict: suffix }
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true, on_delete: cascade }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: false
    strict: true

  events:
    path: "events/{id}.md"
    id: { auto: ulid }
    fields:
      type: { type: string, required: true }
      payload: { type: object }
    additional_properties: true
    strict: false
"#;

        std::fs::write(tmp.path().join("schema.yaml"), schema).unwrap();
        std::fs::create_dir_all(tmp.path().join("users")).unwrap();
        std::fs::create_dir_all(tmp.path().join("posts")).unwrap();
        std::fs::create_dir_all(tmp.path().join("events")).unwrap();

        let store = Store::open(tmp.path().to_str().unwrap()).unwrap();
        (tmp, store)
    }

    #[test]
    fn test_open_store() {
        let (_tmp, store) = setup_test_store();
        assert_eq!(store.schema().collections.len(), 3);
    }

    #[test]
    fn test_insert_and_get_user() {
        let (_tmp, store) = setup_test_store();
        let users = store.collection("users").unwrap();

        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice Chen\nemail: alice@test.com").unwrap();

        let id = users.insert(data, None).unwrap();
        assert_eq!(id, "alice-chen");

        let doc = users.get("alice-chen").unwrap();
        assert_eq!(doc.id, "alice-chen");
        assert_eq!(
            doc.data["name"],
            serde_yaml::Value::String("Alice Chen".into())
        );
        // Default should have been applied
        assert_eq!(
            doc.data["role"],
            serde_yaml::Value::String("member".into())
        );
    }

    #[test]
    fn test_insert_and_list() {
        let (_tmp, store) = setup_test_store();
        let users = store.collection("users").unwrap();

        let data1: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        let data2: serde_yaml::Value =
            serde_yaml::from_str("name: Bob\nemail: bob@test.com").unwrap();

        users.insert(data1, None).unwrap();
        users.insert(data2, None).unwrap();

        let docs = users.list().unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_insert_post_with_content() {
        let (_tmp, store) = setup_test_store();

        // First create the author
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        // Now create a post
        let posts = store.collection("posts").unwrap();
        let post_data: serde_yaml::Value = serde_yaml::from_str(
            "title: Hello World\nauthor_id: alice\ndate: '2026-02-13'\nstatus: published",
        )
        .unwrap();

        let id = posts
            .insert(post_data, Some("## Hello\n\nThis is my post."))
            .unwrap();

        let doc = posts.get(&id).unwrap();
        assert_eq!(
            doc.data["title"],
            serde_yaml::Value::String("Hello World".into())
        );
        assert!(doc.content.unwrap().contains("This is my post."));
    }

    #[test]
    fn test_update_causes_file_movement() {
        let (tmp, store) = setup_test_store();

        // Create user first
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        // Create a draft post
        let posts = store.collection("posts").unwrap();
        let post_data: serde_yaml::Value = serde_yaml::from_str(
            "title: My Post\nauthor_id: alice\ndate: '2026-02-13'\nstatus: draft",
        )
        .unwrap();

        let id = posts.insert(post_data, Some("Body")).unwrap();

        // Verify it's in the draft directory
        let draft_path = tmp.path().join("posts/draft/2026-02-13-my-post.md");
        assert!(draft_path.exists(), "Draft file should exist");

        // Update status to published -- should move the file
        let updated_data: serde_yaml::Value = serde_yaml::from_str(
            "title: My Post\nauthor_id: alice\ndate: '2026-02-13'\nstatus: published",
        )
        .unwrap();
        posts.update(&id, updated_data, Some("Body")).unwrap();

        // Old path should be gone, new path should exist
        assert!(!draft_path.exists(), "Draft file should be gone");
        let published_path = tmp.path().join("posts/published/2026-02-13-my-post.md");
        assert!(published_path.exists(), "Published file should exist");
    }

    #[test]
    fn test_delete_user() {
        let (_tmp, store) = setup_test_store();
        let users = store.collection("users").unwrap();

        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        users.delete("alice").unwrap();

        let result = users.get("alice");
        assert!(result.is_err());
    }

    #[test]
    fn test_referential_integrity_cascade() {
        let (_tmp, store) = setup_test_store();

        // Create user
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        // Create post referencing user
        let posts = store.collection("posts").unwrap();
        let post_data: serde_yaml::Value = serde_yaml::from_str(
            "title: Test Post\nauthor_id: alice\ndate: '2026-02-13'\nstatus: draft",
        )
        .unwrap();
        posts.insert(post_data, Some("Body")).unwrap();

        // Delete user -- should cascade and delete the post too (author_id has on_delete: cascade)
        users.delete("alice").unwrap();

        // Post should also be gone
        let post_list = posts.list().unwrap();
        assert_eq!(post_list.len(), 0);
    }

    #[test]
    fn test_auto_id_generation() {
        let (_tmp, store) = setup_test_store();
        let events = store.collection("events").unwrap();

        let data: serde_yaml::Value = serde_yaml::from_str("type: click").unwrap();
        let id = events.insert(data, None).unwrap();

        // Auto-generated ULID should be non-empty
        assert!(!id.is_empty());

        // Should be retrievable
        let doc = events.get(&id).unwrap();
        assert_eq!(
            doc.data["type"],
            serde_yaml::Value::String("click".into())
        );
    }

    #[test]
    fn test_validation_rejects_invalid() {
        let (_tmp, store) = setup_test_store();
        let users = store.collection("users").unwrap();

        // Missing required email
        let data: serde_yaml::Value = serde_yaml::from_str("name: Alice").unwrap();
        let result = users.insert(data, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_path_conflict_suffix() {
        let (_tmp, store) = setup_test_store();

        // Create user first
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        // Create two posts with same resolved path
        let posts = store.collection("posts").unwrap();
        let post_data: serde_yaml::Value = serde_yaml::from_str(
            "title: Hello\nauthor_id: alice\ndate: '2026-02-13'\nstatus: draft",
        )
        .unwrap();
        let id1 = posts.insert(post_data.clone(), Some("Body 1")).unwrap();

        let id2 = posts.insert(post_data, Some("Body 2")).unwrap();

        // Second post should get a suffixed ID
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_collection_not_found() {
        let (_tmp, store) = setup_test_store();
        let result = store.collection("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_api() {
        let (_tmp, store) = setup_test_store();

        // Insert via dynamic API
        let data = serde_json::json!({
            "name": "Alice",
            "email": "alice@test.com"
        });
        let id = store.insert_dynamic("users", data, None).unwrap();
        assert_eq!(id, "alice");

        // Get via dynamic API
        let doc = store.get_dynamic("users", "alice").unwrap();
        assert_eq!(doc["id"], "alice");
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["email"], "alice@test.com");
        assert!(doc["created_at"].is_string());

        // List via dynamic API
        let list = store
            .list_dynamic("users", &HashMap::new())
            .unwrap();
        assert_eq!(list.as_array().unwrap().len(), 1);

        // Delete via dynamic API
        store.delete_dynamic("users", "alice").unwrap();
        let list = store
            .list_dynamic("users", &HashMap::new())
            .unwrap();
        assert_eq!(list.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_status() {
        let (_tmp, store) = setup_test_store();
        let status = store.status().unwrap();
        assert!(status["schema_hash"].is_string());
        assert!(status["collections"].is_object());
    }

    #[test]
    fn test_validate_all() {
        let (_tmp, store) = setup_test_store();

        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        store.collection("users").unwrap().insert(data, None).unwrap();

        let report = store.validate_all().unwrap();
        assert!(report["users"]["total"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn test_update_partial() {
        let (_tmp, store) = setup_test_store();
        let users = store.collection("users").unwrap();

        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com\nrole: member").unwrap();
        users.insert(data, None).unwrap();

        // Partially update just the email
        let partial: serde_yaml::Value =
            serde_yaml::from_str("email: alice@newdomain.com").unwrap();
        users.update_partial("alice", partial, None).unwrap();

        let doc = users.get("alice").unwrap();
        assert_eq!(
            doc.data["email"],
            serde_yaml::Value::String("alice@newdomain.com".into())
        );
        // Name should be unchanged
        assert_eq!(
            doc.data["name"],
            serde_yaml::Value::String("Alice".into())
        );
        // Role should be unchanged
        assert_eq!(
            doc.data["role"],
            serde_yaml::Value::String("member".into())
        );
    }

    #[test]
    fn test_directory_hash_updated_on_write() {
        let (_tmp, store) = setup_test_store();

        // Get initial hash for users
        let hash_before = store.db.get_directory_hash("users").unwrap();

        // Insert a document
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        // Hash should have changed
        let hash_after = store.db.get_directory_hash("users").unwrap();
        assert_ne!(hash_before, hash_after);
    }

    #[test]
    fn test_batch_insert() {
        let (_tmp, store) = setup_test_store();

        let mut batch = store.batch();
        batch.collection("users").insert(
            serde_json::json!({ "name": "Alice", "email": "a@test.com" }),
            None,
        );
        batch.collection("users").insert(
            serde_json::json!({ "name": "Bob", "email": "b@test.com" }),
            None,
        );
        let results = batch.execute().unwrap();
        assert_eq!(results.len(), 2);

        // Both documents should exist
        let users = store.collection("users").unwrap();
        let all = users.list().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_batch_rollback_on_failure() {
        let (_tmp, store) = setup_test_store();

        // Insert one user first so we can reference it
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        // Batch: insert a valid user, then try to insert an invalid one (missing required field)
        let mut batch = store.batch();
        batch.collection("users").insert(
            serde_json::json!({ "name": "Bob", "email": "b@test.com" }),
            None,
        );
        // This insert is missing the required "email" field — should fail validation
        batch.collection("users").insert(
            serde_json::json!({ "name": "Charlie" }),
            None,
        );
        let result = batch.execute();
        assert!(result.is_err());

        // The first insert in the batch (Bob) should be rolled back
        // Only Alice should exist
        let all = store.collection("users").unwrap().list().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "alice");
    }

    // ── Phase 5: Integration tests ──

    fn setup_store_with_views() -> (TempDir, Store) {
        let tmp = TempDir::new().unwrap();
        let schema = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }
    additional_properties: false
    strict: true
    on_delete: error

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    id: { on_conflict: suffix }
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true, on_delete: cascade }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: false
    strict: true

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
    materialize: false

  all_posts:
    query: |
      SELECT id, title, status, date
      FROM posts
      ORDER BY date DESC
    materialize: false
"#;

        std::fs::write(tmp.path().join("schema.yaml"), schema).unwrap();
        std::fs::create_dir_all(tmp.path().join("users")).unwrap();
        std::fs::create_dir_all(tmp.path().join("posts")).unwrap();

        let store = Store::open(tmp.path().to_str().unwrap()).unwrap();
        (tmp, store)
    }

    /// Helper: seed some users and posts for view tests.
    fn seed_view_data(store: &Store) {
        // Create users
        let users = store.collection("users").unwrap();
        users.insert(
            serde_yaml::from_str("name: Alice\nemail: alice@test.com\nrole: admin").unwrap(),
            None,
        ).unwrap();
        users.insert(
            serde_yaml::from_str("name: Bob\nemail: bob@test.com\nrole: member").unwrap(),
            None,
        ).unwrap();

        // Create posts
        let posts = store.collection("posts").unwrap();
        posts.insert(
            serde_yaml::from_str("title: First Post\nauthor_id: alice\ndate: '2026-01-10'\nstatus: published").unwrap(),
            Some("First post content"),
        ).unwrap();
        posts.insert(
            serde_yaml::from_str("title: Second Post\nauthor_id: bob\ndate: '2026-01-15'\nstatus: published").unwrap(),
            Some("Second post content"),
        ).unwrap();
        posts.insert(
            serde_yaml::from_str("title: Draft Post\nauthor_id: alice\ndate: '2026-01-20'\nstatus: draft").unwrap(),
            Some("Draft content"),
        ).unwrap();
    }

    #[test]
    fn test_view_execution_user_lookup() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // user_lookup should return all users ordered by name
        let result = store.view_dynamic("user_lookup").unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        // Sorted by name ASC: Alice, Bob
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[1]["name"], "Bob");
        // Should include all selected fields
        assert!(rows[0]["email"].is_string());
        assert!(rows[0]["role"].is_string());
    }

    #[test]
    fn test_view_execution_post_feed_join() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // post_feed should return published posts joined with author names
        let result = store.view_dynamic("post_feed").unwrap();
        let rows = result.as_array().unwrap();
        // Only 2 published posts (not the draft)
        assert_eq!(rows.len(), 2);
        // Sorted by date DESC: Second Post (Jan 15), First Post (Jan 10)
        assert_eq!(rows[0]["title"], "Second Post");
        assert_eq!(rows[0]["author_name"], "Bob");
        assert_eq!(rows[1]["title"], "First Post");
        assert_eq!(rows[1]["author_name"], "Alice");
    }

    #[test]
    fn test_view_execution_where_filter() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // post_feed only includes published posts
        let result = store.view_dynamic("post_feed").unwrap();
        let rows = result.as_array().unwrap();
        for row in rows {
            // All rows should have an author_name (from join) — no draft posts
            assert!(row["author_name"].is_string());
        }
        // Draft Post should NOT appear
        let titles: Vec<&str> = rows.iter().filter_map(|r| r["title"].as_str()).collect();
        assert!(!titles.contains(&"Draft Post"));
    }

    #[test]
    fn test_view_execution_order_by() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // all_posts should return posts ordered by date DESC
        let result = store.view_dynamic("all_posts").unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 3);
        // Should be sorted: Draft (Jan 20), Second (Jan 15), First (Jan 10)
        assert_eq!(rows[0]["title"], "Draft Post");
        assert_eq!(rows[1]["title"], "Second Post");
        assert_eq!(rows[2]["title"], "First Post");
    }

    #[test]
    fn test_view_execution_limit() {
        let tmp = TempDir::new().unwrap();
        let schema = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
    additional_properties: false
    strict: true

views:
  recent_users:
    query: |
      SELECT id, name
      FROM users
      ORDER BY name ASC
      LIMIT 2
    materialize: false
"#;
        std::fs::write(tmp.path().join("schema.yaml"), schema).unwrap();
        std::fs::create_dir_all(tmp.path().join("users")).unwrap();
        let store = Store::open(tmp.path().to_str().unwrap()).unwrap();

        // Insert 3 users
        let users = store.collection("users").unwrap();
        users.insert(
            serde_yaml::from_str("name: Alice\nemail: a@test.com").unwrap(),
            None,
        ).unwrap();
        users.insert(
            serde_yaml::from_str("name: Bob\nemail: b@test.com").unwrap(),
            None,
        ).unwrap();
        users.insert(
            serde_yaml::from_str("name: Charlie\nemail: c@test.com").unwrap(),
            None,
        ).unwrap();

        let result = store.view_dynamic("recent_users").unwrap();
        let rows = result.as_array().unwrap();
        // LIMIT 2 should restrict to 2 rows
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_view_materialization() {
        let (tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // post_feed has materialize: true, so check the views/ directory
        let views_dir = tmp.path().join("views");
        let materialized = views_dir.join("post_feed.yaml");
        assert!(materialized.exists(), "Materialized view file should exist");

        // Read and verify content
        let content = std::fs::read_to_string(&materialized).unwrap();
        assert!(content.contains("Second Post"));
        assert!(content.contains("First Post"));
        assert!(!content.contains("Draft Post"));
    }

    #[test]
    fn test_view_buffer_multiplier() {
        let tmp = TempDir::new().unwrap();
        let schema = r#"
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
    additional_properties: false
    strict: true

views:
  buffered_users:
    query: |
      SELECT id, name
      FROM users
      ORDER BY name ASC
      LIMIT 2
    materialize: true
    buffer: 2x
"#;
        std::fs::write(tmp.path().join("schema.yaml"), schema).unwrap();
        std::fs::create_dir_all(tmp.path().join("users")).unwrap();
        let store = Store::open(tmp.path().to_str().unwrap()).unwrap();

        // Insert 5 users
        for name in &["Alice", "Bob", "Charlie", "Diana", "Eve"] {
            let data: serde_yaml::Value = serde_yaml::from_str(
                &format!("name: {name}\nemail: {}@test.com", name.to_lowercase()),
            ).unwrap();
            store.collection("users").unwrap().insert(data, None).unwrap();
        }

        // In-memory cache should hold up to 4 rows (LIMIT 2 * buffer 2x)
        let result = store.view_dynamic("buffered_users").unwrap();
        let rows = result.as_array().unwrap();
        assert!(rows.len() <= 4, "Buffer should limit to 4 rows, got {}", rows.len());

        // Materialized file should have only 2 rows (original LIMIT)
        let materialized = tmp.path().join("views/buffered_users.yaml");
        assert!(materialized.exists());
        let content = std::fs::read_to_string(&materialized).unwrap();
        let yaml_rows: Vec<serde_yaml::Value> = serde_yaml::from_str(&content).unwrap();
        assert_eq!(yaml_rows.len(), 2, "Materialized output should have exactly 2 rows");
    }

    #[test]
    fn test_subscription_on_insert() {
        let (_tmp, store) = setup_test_store();

        let received = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        let received_clone = received.clone();

        store.on_collection_change(
            "users",
            Box::new(move |event| {
                received_clone.lock().unwrap().push(event);
            }),
        );

        // Insert a user — should trigger the subscription
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ChangeEvent::Inserted { id, .. } => assert_eq!(id, "alice"),
            other => panic!("Expected Inserted event, got {:?}", other),
        }
    }

    #[test]
    fn test_subscription_on_update() {
        let (_tmp, store) = setup_test_store();

        let received = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        let received_clone = received.clone();

        store.on_collection_change(
            "users",
            Box::new(move |event| {
                received_clone.lock().unwrap().push(event);
            }),
        );

        // Insert then update
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        let updated: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@new.com").unwrap();
        users.update("alice", updated, None).unwrap();

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            ChangeEvent::Updated { id, .. } => assert_eq!(id, "alice"),
            other => panic!("Expected Updated event, got {:?}", other),
        }
    }

    #[test]
    fn test_subscription_on_delete() {
        let (_tmp, store) = setup_test_store();

        let received = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        let received_clone = received.clone();

        store.on_collection_change(
            "users",
            Box::new(move |event| {
                received_clone.lock().unwrap().push(event);
            }),
        );

        // Insert then delete
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();
        users.delete("alice").unwrap();

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 2);
        match &events[1] {
            ChangeEvent::Deleted { id } => assert_eq!(id, "alice"),
            other => panic!("Expected Deleted event, got {:?}", other),
        }
    }

    #[test]
    fn test_subscription_unsubscribe() {
        let (_tmp, store) = setup_test_store();

        let received = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        let received_clone = received.clone();

        let sub_id = store.on_collection_change(
            "users",
            Box::new(move |event| {
                received_clone.lock().unwrap().push(event);
            }),
        );

        // Insert then unsubscribe
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        store.unsubscribe(sub_id);

        // This should NOT trigger the callback
        let data2: serde_yaml::Value =
            serde_yaml::from_str("name: Bob\nemail: bob@test.com").unwrap();
        users.insert(data2, None).unwrap();

        let events = received.lock().unwrap();
        assert_eq!(events.len(), 1, "Should only have 1 event after unsubscribe");
    }

    #[test]
    fn test_view_subscription() {
        let (_tmp, store) = setup_store_with_views();

        let received = Arc::new(Mutex::new(Vec::<Vec<serde_json::Value>>::new()));
        let received_clone = received.clone();

        store.on_view_change(
            "user_lookup",
            Box::new(move |data| {
                received_clone.lock().unwrap().push(data.to_vec());
            }),
        );

        // Insert a user — should trigger view rebuild and notify subscribers
        let users = store.collection("users").unwrap();
        let data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(data, None).unwrap();

        let events = received.lock().unwrap();
        assert!(!events.is_empty(), "View subscriber should have been notified");
        // The most recent view data should contain Alice
        let latest = events.last().unwrap();
        assert!(latest.iter().any(|row| row["name"] == "Alice"));
    }

    #[test]
    fn test_list_dynamic_with_filters() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // Filter users by role
        let mut filters = HashMap::new();
        filters.insert("role".to_string(), "admin".to_string());

        let result = store.list_dynamic("users", &filters).unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Alice");

        // Filter by member role
        filters.insert("role".to_string(), "member".to_string());
        let result = store.list_dynamic("users", &filters).unwrap();
        let rows = result.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Bob");
    }

    #[test]
    fn test_rebuild_also_rebuilds_views() {
        let (_tmp, store) = setup_store_with_views();
        seed_view_data(&store);

        // Verify views have data
        let result = store.view_dynamic("user_lookup").unwrap();
        assert_eq!(result.as_array().unwrap().len(), 2);

        // Force rebuild (should re-scan and rebuild views)
        store.rebuild(None).unwrap();

        // Views should still have data after rebuild
        let result = store.view_dynamic("user_lookup").unwrap();
        assert_eq!(result.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_explain_view() {
        let (_tmp, store) = setup_store_with_views();

        let result = store.explain_view("post_feed").unwrap();
        assert_eq!(result["view"], "post_feed");
        assert!(result["original_sql"].as_str().unwrap().contains("SELECT"));
        assert!(result["rewritten_sql"].as_str().unwrap().contains("WITH"));
        assert_eq!(result["limit"], 100);
        assert_eq!(result["buffer_limit"], 200);
        assert_eq!(result["is_query_template"], false);
    }

    #[test]
    fn test_strip_limit_basic() {
        assert_eq!(strip_limit("SELECT * FROM t LIMIT 10"), "SELECT * FROM t");
        assert_eq!(strip_limit("SELECT * FROM t"), "SELECT * FROM t");
        assert_eq!(strip_limit("SELECT * FROM t LIMIT 100  "), "SELECT * FROM t");
    }

    #[test]
    fn test_strip_limit_newline_prefix() {
        // LIMIT preceded by newline (as in rewritten SQL)
        assert_eq!(strip_limit("SELECT * FROM t\nLIMIT 10"), "SELECT * FROM t");
        assert_eq!(strip_limit("SELECT * FROM t\n  LIMIT 100"), "SELECT * FROM t");
    }

    #[test]
    fn test_strip_limit_preserves_inner_limit() {
        // Should strip the outer LIMIT 10, leaving the CTE intact
        let sql = "WITH t AS (SELECT * FROM x LIMIT 5) SELECT * FROM t LIMIT 10";
        let result = strip_limit(sql);
        assert_eq!(result, "WITH t AS (SELECT * FROM x LIMIT 5) SELECT * FROM t");
    }

    #[test]
    fn test_file_move_reconciles_yaml_status() {
        let (tmp, store) = setup_test_store();

        // Create a user (needed as author ref for posts)
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Alice\nemail: alice@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        // Create a draft post via the API
        let posts = store.collection("posts").unwrap();
        let post_data: serde_yaml::Value = serde_yaml::from_str(
            "title: My Post\nauthor_id: alice\ndate: '2026-02-13'\nstatus: draft",
        )
        .unwrap();
        posts.insert(post_data, Some("Hello world")).unwrap();

        let draft_path = tmp.path().join("posts/draft/2026-02-13-my-post.md");
        assert!(draft_path.exists(), "Draft file should exist");

        // Simulate a manual file move: draft -> published
        let published_dir = tmp.path().join("posts/published");
        std::fs::create_dir_all(&published_dir).unwrap();
        let published_path = published_dir.join("2026-02-13-my-post.md");
        std::fs::rename(&draft_path, &published_path).unwrap();

        // Verify the file still says status: draft before processing
        let before = document::read_document(&published_path).unwrap();
        assert_eq!(
            before.data["status"],
            serde_yaml::Value::String("draft".into()),
            "Status should still be 'draft' before reconciliation"
        );

        // Process a watcher event for the new path (as the watcher would)
        let event = WatcherEvent {
            path: published_path.clone(),
            kind: ChangeKind::Created,
        };
        store
            .process_single_watcher_event("posts", &event)
            .unwrap();

        // Read the file again — YAML should now say status: published
        let after = document::read_document(&published_path).unwrap();
        assert_eq!(
            after.data["status"],
            serde_yaml::Value::String("published".into()),
            "Status should be reconciled to 'published' after file move"
        );

        // Body content should be preserved
        assert!(
            after.content.as_deref().unwrap().contains("Hello world"),
            "Body content should be preserved"
        );
    }

    #[test]
    fn test_file_move_no_change_when_already_matching() {
        let (tmp, store) = setup_test_store();

        // Create a user
        let users = store.collection("users").unwrap();
        let user_data: serde_yaml::Value =
            serde_yaml::from_str("name: Bob\nemail: bob@test.com").unwrap();
        users.insert(user_data, None).unwrap();

        let user_path = tmp.path().join("users/bob.md");
        assert!(user_path.exists());

        // Read original file content
        let original_content = std::fs::read_to_string(&user_path).unwrap();

        // Process a Modified event (e.g. user touched the file)
        let event = WatcherEvent {
            path: user_path.clone(),
            kind: ChangeKind::Modified,
        };
        store
            .process_single_watcher_event("users", &event)
            .unwrap();

        // File should not have been rewritten since name already matches
        let after_content = std::fs::read_to_string(&user_path).unwrap();
        assert_eq!(original_content, after_content, "File should not be rewritten when path already matches YAML");
    }
}
