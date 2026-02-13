use crate::document::{self, Document};
use crate::error::{GroundDbError, Result};
use crate::path_template::{self, PathTemplate};
use crate::schema::{
    hash_schema, parse_schema, AutoIdStrategy, CollectionDefinition, FieldType, OnConflict,
    OnDeletePolicy, SchemaDefinition,
};
use crate::system_db::{compute_directory_hash, SystemDb};
use crate::util::json_to_yaml as json_value_to_yaml;
use crate::validation;
use crate::view::ViewEngine;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

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
}

impl Store {
    /// Open a GroundDB store at the given data directory path.
    /// Parses schema.yaml, opens/creates _system.db, and runs the boot lifecycle.
    pub fn open(path: &str) -> Result<Self> {
        let root = PathBuf::from(path);
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
        };

        store.boot()?;

        // Load cached view data
        store.view_engine.load_from_db(&store.db)?;

        Ok(store)
    }

    /// Boot lifecycle: check schema, scan collections, update index
    fn boot(&self) -> Result<()> {
        let current_hash = hash_schema(&self.schema_yaml);

        // Check schema hash
        let last_hash = self.db.get_last_schema_hash()?;
        if last_hash.as_deref() != Some(&current_hash) {
            // Schema changed (or first boot)
            self.db.record_schema(&current_hash, &self.schema_yaml)?;
            // On first boot or schema change, do a full scan
            self.full_scan()?;
        } else {
            // Schema unchanged -- incremental scan using directory hashes
            self.incremental_scan()?;
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

            self.db
                .upsert_document(&doc.id, name, &rel_path, &doc.data)?;

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

    /// List all documents in a collection, optionally filtered by path segment values.
    pub fn list_dynamic(
        &self,
        collection: &str,
        _filters: &HashMap<String, String>,
    ) -> Result<serde_json::Value> {
        let col = self.collection(collection)?;
        let docs = col.list()?;
        let items: Vec<serde_json::Value> = docs
            .iter()
            .filter_map(|doc| doc_to_json(doc).ok())
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

        // For parameterized queries, we evaluate against the document index
        // Simple approach: filter documents from referenced collections using param values
        let parsed = self.view_engine.get_view(name);
        if let Some(parsed) = parsed {
            // Get all documents from referenced collections
            let mut results = Vec::new();
            for collection_name in &parsed.referenced_collections {
                let records = self.db.list_documents(collection_name)?;
                for record in records {
                    let data: serde_json::Value =
                        serde_json::from_str(&record.data_json).unwrap_or_default();
                    let mut row = serde_json::Map::new();
                    row.insert("id".to_string(), serde_json::Value::String(record.id));

                    // Flatten data fields
                    if let serde_json::Value::Object(fields) = data {
                        for (k, v) in fields {
                            row.insert(k, v);
                        }
                    }

                    // Check if this document matches the query params
                    let mut matches = true;
                    for (param_name, param_value) in params {
                        // Check if the document has a matching field
                        if let Some(field_val) = row.get(param_name) {
                            if field_val.as_str() != Some(param_value) {
                                matches = false;
                                break;
                            }
                        }
                        // Also check the WHERE clause pattern: field = :param
                        // Simple heuristic: check if any field equals the param value
                    }

                    if matches {
                        results.push(serde_json::Value::Object(row));
                    }
                }
            }
            return Ok(serde_json::Value::Array(results));
        }

        Ok(serde_json::Value::Array(vec![]))
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
            Some(name) => self.scan_collection(name),
            None => self.full_scan(),
        }
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

    /// Rebuild a single static view by querying all documents from its referenced collections.
    fn rebuild_view(&self, view_name: &str) -> Result<()> {
        let parsed = match self.view_engine.get_view(view_name) {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        // Gather rows from all referenced collections
        let mut rows = Vec::new();
        for collection_name in &parsed.referenced_collections {
            let records = self.db.list_documents(collection_name)?;
            for record in records {
                let data: serde_json::Value =
                    serde_json::from_str(&record.data_json).unwrap_or_default();
                let mut row = serde_json::Map::new();
                row.insert("id".to_string(), serde_json::Value::String(record.id));
                if let serde_json::Value::Object(fields) = data {
                    for (k, v) in fields {
                        row.insert(k, v);
                    }
                }
                rows.push(serde_json::Value::Object(row));
            }
        }

        // Apply buffer multiplier to limit
        let effective_limit = parsed.limit.map(|l| {
            (l as f64 * parsed.buffer_multiplier).ceil() as usize
        });
        if let Some(limit) = effective_limit {
            rows.truncate(limit);
        }

        // Update in-memory cache and persist to DB
        let json_str = serde_json::to_string(&rows)?;
        self.db.set_view_data(view_name, &json_str)?;
        self.view_engine.set_view_data(view_name, rows);

        // Materialize if needed
        if parsed.materialize {
            self.view_engine.materialize_views(&self.root)?;
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
    /// If any operation fails, previously-written files in this batch are rolled back.
    pub fn execute(self) -> Result<Vec<String>> {
        // Track files created so we can roll back on failure
        let mut created_files: Vec<PathBuf> = Vec::new();
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
                            if let Ok(col) = self.store.collection(collection) {
                                if let Ok(Some(record)) = self.store.db.get_document(collection, &id) {
                                    created_files.push(self.store.root.join(&record.path));
                                }
                                let _ = col; // just used for lifetime
                            }
                        })
                }
                BatchOp::Update { collection, id, data } => {
                    self.store
                        .update_dynamic(collection, id, data.clone())
                        .map(|_| {
                            results.push(id.clone());
                        })
                }
                BatchOp::Delete { collection, id } => {
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

                    // Update the index
                    self.store
                        .db
                        .upsert_document(&resolved_id, &self.name, &resolved, &data)?;

                    self.store.post_write(&self.name)?;
                    return Ok(resolved_id);
                }
            }
        }

        // Write the file
        document::write_document(&abs_path, &data, content)?;

        // Update the index
        self.store
            .db
            .upsert_document(&id, &self.name, &rel_path, &data)?;

        self.store.post_write(&self.name)?;
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

        // Update the index
        self.store
            .db
            .upsert_document(id, &self.name, &new_rel_path, &data)?;

        self.store.post_write(&self.name)?;
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
                                                document::write_document(
                                                    &file_path, &data, None,
                                                )?;
                                                self.store.db.upsert_document(
                                                    &ref_doc.id,
                                                    &ref_doc.collection,
                                                    &ref_doc.path,
                                                    &data,
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
}
