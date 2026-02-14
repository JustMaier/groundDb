# GroundDB Gap Fix — Implementation Plan v2

> Synthesized from three independent architectural analyses, reviewed by a fourth agent, and updated with Tauri/reactive UI requirements.

---

## Guiding Principle

**Let SQLite do what SQLite does.** The `documents` table already contains every document's front matter as JSON. The `SystemDb::query_documents_sql()` method already exists but is unused (and has a bug: ignores its params). The path is: rewrite user-facing view SQL into CTE-wrapped SQLite queries against the `documents` table, then execute them.

**Files remain the source of truth.** SQLite is a derived index/cache. The file watching system ensures the index stays in sync with external edits.

---

## Phase 1: SystemDb Schema Upgrade + SQL View Execution Engine

Everything else depends on this. Views, parameterized queries, buffers, and materialization all become correct once SQL actually executes.

### Step 1.1: Add columns to `documents` table and fix upsert

The `recent_activity` view references `modified_at` and `post_comments` references `content`. These aren't currently stored in the document index.

- Add `created_at TEXT`, `modified_at TEXT`, and `content_text TEXT` columns to the `documents` table
- Handle system DB migration: on open, check if columns exist via `PRAGMA table_info(documents)`, `ALTER TABLE ADD COLUMN` if missing
- Update `upsert_document()` signature to accept timestamps and content: `upsert_document(id, collection, path, data, created_at, modified_at, content)`
- Update `scan_collection()` to pass filesystem timestamps (already read at `store/mod.rs:158-162`) and document content to `upsert_document()`
- Update all write paths (insert, update, delete) to pass timestamps
- Add `get_last_schema_yaml()` method to SystemDb (needed for Phase 3, simple `SELECT schema_yaml FROM schema_history ORDER BY id DESC LIMIT 1`)

### Step 1.2: Store original SQL and table aliases in ParsedView

The current `ParsedView` doesn't store the original SQL text or table aliases — both are needed for SQL rewriting.

- Add `pub original_sql: String` to `ParsedView`
- Change `referenced_collections: HashSet<String>` to `pub table_refs: Vec<TableRef>` where `TableRef { collection: String, alias: Option<String> }`
- Extract alias from `TableFactor::Table { name, alias, .. }` in `extract_table_name()` (currently discarded with `..`)
- Store the original SQL during `parse_view_query()`

### Step 1.3: Build SQL rewriting engine in `view/mod.rs`

Add `fn rewrite_view_sql(parsed: &ParsedView, schema: &SchemaDefinition) -> RewrittenQuery`:

1. For each table reference in FROM/JOIN, generate a CTE named after the collection:
   ```sql
   WITH posts AS (
     SELECT
       id,
       created_at,
       modified_at,
       content_text,
       json_extract(data_json, '$.title') AS title,
       json_extract(data_json, '$.author_id') AS author_id,
       json_extract(data_json, '$.date') AS date,
       json_extract(data_json, '$.status') AS status,
       json_extract(data_json, '$.tags') AS tags
     FROM documents
     WHERE collection = 'posts'
   ),
   users AS (
     SELECT
       id,
       created_at,
       modified_at,
       json_extract(data_json, '$.name') AS name,
       json_extract(data_json, '$.email') AS email,
       json_extract(data_json, '$.role') AS role
     FROM documents
     WHERE collection = 'users'
   )
   -- user's original SQL follows verbatim --
   SELECT p.title, p.date, u.name AS author_name ...
   ```
2. Extract ALL fields from each collection's schema definition for the CTE columns — not just fields referenced in the query — to ensure completeness.
3. Implicit fields (`id`, `created_at`, `modified_at`) map to direct columns on `documents` table, NOT `json_extract`.
4. `content_text` is exposed as `content` in the CTE for collections with `content: true`.
5. The user's original SQL appended verbatim after the CTEs — it works as-is because CTE names match collection names.
6. Log rewritten SQL at `log::debug!` level for debugging. Output it via `grounddb explain <view>`.

`RewrittenQuery` struct:
```
struct RewrittenQuery {
    sql: String,                          // the CTE-wrapped SQL
    param_names: Vec<String>,             // ordered parameter names for binding
    buffer_limit: Option<usize>,          // limit * buffer_multiplier
    original_limit: Option<usize>,        // original limit for materialization
}
```

### Step 1.4: Fix `query_documents_sql()` parameter binding

The current implementation at `system_db/mod.rs:313-354` accepts `_params_map: &HashMap<String, String>` but **ignores it** — passes `[]` to `stmt.query_map`. Fix:

- Accept `params: &[(&str, &dyn rusqlite::types::ToSql)]`
- Use `stmt.query_map(params, ...)` or rusqlite named parameter binding
- For parameterized views, keep `:param_name` syntax in rewritten SQL (rusqlite handles it natively)

### Step 1.5: Rewrite `rebuild_view()` and `query_dynamic()`

Replace `Store::rebuild_view()` (`store/mod.rs:616-659`):
- Call `rewrite_view_sql()` to get the CTE-wrapped SQL
- Use `buffer_limit` in the SQL for buffered views
- Execute via `self.db.query_documents_sql(&rewritten.sql, &[])`
- Store results in view_data (in-memory + system DB)
- Materialize if configured
- **Notify subscribers** of view change (Phase 4)

Replace `Store::query_dynamic()` (`store/mod.rs:452-509`):
- Call `rewrite_view_sql()` for the parameterized view
- Bind named parameters from the params map
- Execute via `self.db.query_documents_sql()` with bound params
- Return results

### Step 1.6: Rebuild views during boot

Currently `boot()` (`store/mod.rs:82-98`) scans collections but does NOT rebuild views after detecting changes. Add:
- After `full_scan()` or `incremental_scan()` detects changes, rebuild all static views
- This ensures view data is fresh on startup, not stale from the last session's cache

---

## Phase 2: Buffer Strategy + Materialization

### Step 2.1: Buffer-aware LIMIT

The `RewrittenQuery` from Phase 1 already includes `buffer_limit`. In `rebuild_view()`:
- Execute SQL with `LIMIT buffer_limit` (e.g., 200 for a view with LIMIT 100 and buffer 2x)
- Store all buffered rows in `view_data`
- ORDER BY is handled by SQLite, so buffered rows are already sorted

### Step 2.2: Materialization with original LIMIT

Existing `materialize_views()` (`view/mod.rs:113-137`) already applies the smaller limit for output. Once the data is correct (Phase 1), materialization works. Verify:
- Buffered internal data has `limit * buffer` rows, sorted
- Materialized file has exactly `limit` rows
- Sort order preserved

---

## Phase 3: Schema Migration

### Step 3.1: Schema diffing engine

Create `grounddb/src/migration/mod.rs`:

```rust
pub enum SchemaMigration {
    CollectionAdded { name: String },
    CollectionRemoved { name: String },
    FieldAdded { collection: String, field: String, has_default: bool, required: bool },
    FieldRemoved { collection: String, field: String },
    FieldTypeChanged { collection: String, field: String },
    EnumValueAdded { collection: String, field: String, value: String },
    EnumValueRemoved { collection: String, field: String, value: String },
    PathTemplateChanged { collection: String, old_template: String, new_template: String },
    DefaultChanged { collection: String, field: String },
}

pub fn diff_schemas(old: &SchemaDefinition, new: &SchemaDefinition) -> Vec<SchemaMigration>;
```

### Step 3.2: Safe auto-apply during boot

Integrate into `Store::boot()`:
- When schema hash changes, retrieve old schema YAML via `get_last_schema_yaml()`, parse it
- Compute diff
- Safe changes (auto-apply):
  - `FieldAdded` with default: scan collection, write default to documents missing the field
  - `CollectionAdded`: create directory
  - `EnumValueAdded`: no action
- Unsafe changes (error/warn):
  - `FieldAdded` with `required: true` and no default: error at startup
  - `FieldTypeChanged`: error at startup
  - `PathTemplateChanged`: warn (full reorganization deferred)
  - `CollectionRemoved`: warn, never delete data
- Log all migrations in `migrations` table

### Step 3.3: CLI integration

Wire the `migrate` CLI command to:
- Show pending migrations (from schema diff)
- Support `--dry-run` to preview changes
- Apply migrations explicitly

---

## Phase 4: File Watching + Subscriptions (Tauri/Reactive UI Support)

### Step 4.1: File watcher via `notify` crate

Add `notify` as a dependency to `grounddb/Cargo.toml`.

Add `Store::watch()` method that:
- Creates a `RecommendedWatcher` watching all collection directories recursively
- On file create/modify/delete events:
  1. Determine which collection the file belongs to (from path)
  2. Re-read the document from disk
  3. Update the document index in SystemDb
  4. Update the directory hash
  5. Rebuild affected static views
  6. Notify subscribers

Debounce events (e.g., 100ms) to handle rapid saves and atomic write patterns (temp file + rename).

### Step 4.2: Subscription/callback system

Add a subscription API so consumers (Tauri backend, etc.) can react to changes:

```rust
impl Store {
    /// Subscribe to changes on a specific view. Callback fires when view data changes.
    pub fn on_view_change(&self, view_name: &str, callback: Box<dyn Fn(&[serde_json::Value]) + Send>) -> SubscriptionId;

    /// Subscribe to changes on a specific collection. Callback fires on insert/update/delete.
    pub fn on_collection_change(&self, collection: &str, callback: Box<dyn Fn(ChangeEvent) + Send>) -> SubscriptionId;

    /// Unsubscribe.
    pub fn unsubscribe(&self, id: SubscriptionId);
}

pub enum ChangeEvent {
    Inserted { id: String, data: serde_json::Value },
    Updated { id: String, old_data: serde_json::Value, new_data: serde_json::Value },
    Deleted { id: String },
}
```

This integrates with both:
- **API writes** (insert/update/delete already go through `post_write()` — add notification there)
- **File watcher events** (external edits trigger the same notification path)

### Step 4.3: Tauri integration pattern

The subscription API enables a clean Tauri pattern:
```rust
// In Tauri setup
store.on_view_change("post_feed", Box::new(move |rows| {
    app_handle.emit_all("post_feed_updated", rows).unwrap();
}));
```

Frontend subscribes to the event and re-renders when the view changes, regardless of whether the change came from the API or a direct file edit.

---

## Phase 5: Tests

### View execution tests
- Unit: `rewrite_view_sql()` on simple SELECT/WHERE produces correct CTE-wrapped SQLite SQL
- Unit: `rewrite_view_sql()` on JOIN queries produces correct structure
- Unit: `rewrite_view_sql()` handles implicit fields (id, modified_at)
- Integration: Create users + posts, rebuild `post_feed`, verify correct joined rows with author_name
- Integration: Verify `WHERE p.status = 'published'` filters correctly
- Integration: Verify `ORDER BY p.date DESC` sorts correctly
- Integration: Verify `LIMIT` restricts row count
- Integration: Verify `recent_activity` view uses `modified_at` correctly

### Parameterized query tests
- Integration: `query_dynamic("post_comments", {"post_id": "xxx"})` returns only matching comments with joined user names

### Buffer tests
- Integration: View with LIMIT 5 and buffer 2x keeps 10 rows internally, materializes 5
- Integration: Verify materialized file has correct sort order

### Schema migration tests
- Unit: `diff_schemas()` detects new field with default
- Unit: `diff_schemas()` detects removed field
- Unit: `diff_schemas()` detects path template change
- Unit: `diff_schemas()` detects new required field without default → error
- Integration: Add field with default to schema, reopen store, verify existing docs get default written

### File watcher tests
- Integration: Start watcher, externally modify a file, verify index updates
- Integration: Start watcher, externally create a file, verify it appears in collection
- Integration: Verify view subscribers are notified on external file change

---

## Files Changed

| File | Changes |
|------|---------|
| `grounddb/Cargo.toml` | Add `notify` dependency for file watching |
| `grounddb/src/system_db/mod.rs` | Add created_at, modified_at, content_text columns; fix upsert signature; fix query_documents_sql param binding; add get_last_schema_yaml(); system DB migration |
| `grounddb/src/view/mod.rs` | Store original SQL + table aliases in ParsedView; add `rewrite_view_sql()` function |
| `grounddb/src/store/mod.rs` | Rewrite `rebuild_view()` and `query_dynamic()`; update scan_collection; add view rebuilds to boot; add watch() and subscription API |
| `grounddb/src/migration/mod.rs` | New file: schema diffing + migration types + safe auto-apply |
| `grounddb/src/watcher/mod.rs` | New file: filesystem watcher using notify crate |
| `grounddb/src/lib.rs` | Add `mod migration`, `mod watcher` |
| `grounddb-cli/src/main.rs` | Wire `migrate` command |

## Dependencies

| Crate | Purpose | New? |
|-------|---------|------|
| `sqlparser` | SQL AST parsing | Existing |
| `rusqlite` | SQLite execution + JSON1 | Existing |
| `notify` | Filesystem watching | **New** |
| `serde_json`/`serde_yaml` | Serialization | Existing |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| SQL rewriting handles only simple queries | Restrict to SELECT, JOIN (INNER/LEFT), WHERE, ORDER BY, LIMIT. Error on unsupported features. |
| json_extract performance | Fine for GroundDB's scale. Optimize later if needed. |
| Schema migration corrupts files | Only auto-apply safe changes. Dangerous ops (path moves) deferred with --dry-run. |
| created_at unreliable on git clone | Acknowledge limitation. Future: store timestamps in front matter. |
| content_text increases DB size | Only for content:true collections. Acceptable at target scale. |
| File watcher event storms | Debounce events (100ms). Batch index updates. |
| query_documents_sql ignores params | Fix the bug as part of Step 1.4. |
