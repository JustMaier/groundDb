//! # GroundDB
//!
//! A schema-driven data layer that uses Markdown files as the source of truth.
//!
//! GroundDB stores data as plain Markdown files with YAML front matter, organized
//! on disk by configurable path templates. A SQLite system database keeps an index
//! for fast queries, and SQL-defined views are maintained automatically.
//!
//! ## Core concepts
//!
//! - **Collections** — folders of Markdown files, each file is a document
//! - **Schema** — a `schema.yaml` defining collections, fields, types, and views
//! - **Path templates** — map field values to filesystem paths (e.g., `posts/{status}/{date}-{title}.md`)
//! - **Views** — SQL queries over collections, maintained incrementally from the document index
//! - **System database** — a SQLite file (`_system.db`) storing the document index, view cache, and schema history
//!
//! ## Usage
//!
//! ```no_run
//! use grounddb::Store;
//!
//! let store = Store::open("data").unwrap();
//!
//! // Access a collection
//! let users = store.collection("users").unwrap();
//! let docs = users.list().unwrap();
//! ```
//!
//! For compile-time typed access, use [`grounddb-codegen`](https://crates.io/crates/grounddb-codegen)
//! to generate Rust structs from your schema.
//!
//! ## Feature highlights
//!
//! - Schema validation with enums, defaults, and required fields
//! - Referential integrity (`error`, `cascade`, `nullify`, `archive`)
//! - Auto-generated IDs (`ulid`, `uuid`, `nanoid`)
//! - Atomic file writes and batch operations with rollback
//! - Incremental boot with directory-hash change detection

pub mod schema;
pub mod path_template;
pub mod document;
pub mod system_db;
pub mod store;
pub mod validation;
pub mod view;
pub mod migration;
pub mod watcher;
pub mod error;
pub mod util;

pub use error::{GroundDbError, Result};
pub use schema::SchemaDefinition;
pub use store::{Store, Collection, Batch, SubscriptionId, ChangeEvent};
pub use document::Document;
pub use view::ViewEngine;
