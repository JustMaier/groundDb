# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- SQL view execution engine with CTE-rewriting: view SQL is parsed, wrapped in CTEs that extract fields via `json_extract()`, and executed against the SQLite documents table
- File watching via `notify` crate: monitors collection directories for external changes with 100ms debounce
- Subscription API: `on_view_change()` and `on_collection_change()` callbacks for reactive UI integration (Tauri pattern)
- `process_watcher_events()` method to drain and apply file watcher events to the index and views
- Path field reconciliation: when a file is moved between directories, YAML front matter is updated to match path-extracted values (e.g. moving `posts/draft/` → `posts/published/` sets `status: published`)
- `PathTemplate::extract()` method: reverse of `render()` — extracts field values from a file path by matching against the template
- Schema migration engine (`migration/mod.rs`): diffs old and new schemas, auto-applies safe changes, blocks unsafe ones
- `explain` CLI command: shows original and CTE-rewritten SQL for any view
- `migrate` CLI command with `--dry-run` support for previewing pending schema migrations
- `update_partial` method on codegen `TypedCollection` for partial document updates
- `materialize_view()` for single-view materialization (scoped, not all-views)
- `list_dynamic` filter support: filters documents by field values
- Live dashboard example (`examples/live-dashboard`): real-time SSE-powered browser dashboard with file watching
- 24 new Phase 5 tests: view execution (joins, WHERE, ORDER BY, LIMIT), subscriptions, buffer/materialization, migration, filters

### Changed

- `FileWatcher` uses mpsc channel instead of callback for Store-compatible event processing
- `ParsedView` stores `table_refs` instead of redundant `referenced_collections` field (now a derived method)
- `rebuild()` now rebuilds affected views after re-scanning collections
- `strip_limit()` handles newline-prefixed LIMIT clauses and validates trailing content
- Batch rollback saves and restores file content for updates and deletes (not just created files)

### Fixed

- File watcher callback was a no-op (only logged events, never updated the index)
- `notify_collection` was never called from CRUD operations; subscribers were never notified
- Batch rollback did not restore updated or deleted files, only removed created files
- `Nullify` on-delete policy lost timestamps and content when updating the referencing document
- Schema migration backfill lost timestamps and content when writing default values
- `list_dynamic` ignored the `filters` parameter entirely
- `rebuild()` did not rebuild views after scanning

## [1.0.0] - 2026-02-13

### Added

- Schema-driven collections defined in `schema.yaml` with typed fields, enums, defaults, and validation
- Path templates with field interpolation, slugification, and date formatting for natural filesystem organization
- YAML front matter parsing and serialization for Markdown documents
- Optional Markdown body support (`content: true`) for content-rich collections
- SQL-defined views with automatic maintenance from a SQLite document index
- Parameterized query templates with typed parameters
- Materialized views written as YAML files in `views/`
- View buffering (`buffer: 2x`) for stable results beyond the LIMIT
- Compile-time Rust code generation (`grounddb-codegen`) producing typed structs, enums, partial-update structs, and store accessors from the schema
- Referential integrity with `error`, `cascade`, `nullify`, and `archive` deletion policies
- Polymorphic references (`target: [posts, comments]`) with tagged enum codegen
- Reusable types block for shared field structures across collections
- Auto-generated IDs (`ulid`, `uuid`, `nanoid`)
- Conflict resolution strategies (`error` / `suffix`) for path collisions
- Atomic file writes and moves for safe concurrent access
- Batch operations with all-or-nothing rollback
- Schema migration with automatic diff detection on startup
- `additional_properties` and `strict` mode per collection
- SQLite system database (`_system.db`) for document index, view cache, and schema history
- Directory hash-based change detection for incremental boot
- CLI (`grounddb-cli`) with `get`, `list`, `insert`, `update`, `delete`, `view`, `query`, `validate`, `rebuild`, and `status` commands
- JSON and YAML output formats
