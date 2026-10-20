# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.0] - 2026-10-20

### Added
- **`Store::ensure_field_index` / `ensure_fields_index`.** Expression indexes
  over view-filtered fields, single or composite, so a parameterised view
  filter and an index-only GROUP BY stop reading the whole collection
  (recipient + status lookup on a 31.8k-document store: 855 ms to 2.2 ms).
- **`find_dynamic`, `count_dynamic`, `explain_find`.** Run-time equality
  filters bound by JSON type, one sort with the id as tie-break, and a keyset
  cursor, served by the same expressions `ensure_fields_index` indexes.
- **`Store::analyze`.** Runs `ANALYZE` so the planner picks the right field
  index; examples `find_bench` and `open_check`.
- **Append-only JSONL record collections.** A collection whose path ends in
  `.jsonl` is indexed one record per line in a new `records` table (the
  `documents` table and every markdown path are untouched). `append_record` /
  `append_record_to` write one line with a single write + `sync_data`, and
  terminate a torn last line left by a crash instead of gluing onto it.
  `find_records` / `count_records` read newest first with equality filters,
  ranges and an id cursor; `ensure_record_index` serves a filter in id order;
  `tail_records` and `jsonl::ReverseLines` read a file backwards from EOF.
  Boot and the watcher index a file from where its index stopped, and reindex
  a file that shrank or was replaced. Design: `docs/jsonl-collections.md`.

### Fixed
- **Writes survive a hard crash.** `write_document` syncs the temp file
  before renaming it over the document; a crash used to leave the rename
  without the data, a zero-filled file.
- **A zero-filled file is never merged onto or indexed.** `update_partial`
  refuses a file with no front matter and an all-NUL body instead of
  rewriting it from defaults, and a rescan skips such a file with a warning.
  A real NUL inside a body is not treated as damage.
- **Rename retried on Windows.** Replacing a document while another handle
  holds it (indexer, antivirus, backup) is retried for up to 750 ms instead
  of losing the write.

## [1.3.0] - 2026-08-14

Boot and write cost no longer scale with how much data a collection already
holds. On a 6,800-document store, opening went from 154s to under 3s and a
single write went from ~990ms to ~6ms.

### Added

- `(collection, path)` composite index on the document index. `get_id_by_path`
  filters on both columns, and with only single-column indexes the planner chose
  the collection index — which matches every row, because a store has one
  collection. Each id lookup was O(rows) and the boot scan does one per file.
- `collection_entries` table mirroring each file's `(path, mtime)`, so a write
  updates one row instead of restating the directory.

### Changed

- The boot scan re-reads only files whose mtime moved, instead of deleting and
  re-reading every document in the collection on any change.
- The re-index runs in a single transaction. Outside one, SQLite gives each
  document its own durable commit.
- `post_write` derives the collection hash from the mirrored entry set rather
  than globbing the collection and calling `metadata()` once per file. Boot
  still derives the hash from disk, so a stale mirror costs a rescan and never a
  wrong index; the watcher, which ingests other processes' edits and cannot know
  what moved, still restates from disk.

### Fixed

- Views whose SQL carries `:named` placeholders are no longer treated as static.
  Materialising one binds every placeholder to NULL, so it stored `[]` and every
  boot and every write rebuilt that same `[]`. They are query templates whether
  or not the schema declares `type: query` or a `params:` block.
- The boot scan could not see two kinds of change, because its directory hash
  keyed on bare filenames at whole-second resolution: a file moved between
  subdirectories (same name, same mtime), and an edit landing in the same second
  as the previous one. It now keys on the collection-relative path at full mtime
  resolution.
- SQLite runs in WAL mode with an explicit busy timeout and IMMEDIATE write
  transactions.

## [1.2.0] and [1.1.0]

Released without changelog entries. What follows sat under `[Unreleased]` until
1.3.0 was cut, so it describes work that had already shipped across these two
versions. It is grouped here rather than split between them, because which half
landed in which release was not recorded at the time.

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
