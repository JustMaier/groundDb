# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
