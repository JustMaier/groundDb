# GroundDB — Specification v1

> A schema-driven data layer that uses Markdown files as the source of truth, with YAML front matter for structured data, path templates for natural filesystem organization, SQL-defined views, compile-time type generation, and a lightweight system database for state management.

---

## 1. Core Concepts

### 1.1 Collections

A **collection** is a folder of Markdown files. Each file is a **document**. The filename (without extension) is the document's **ID**.

Every document in the system implicitly has:

| Field         | Type     | Source                 |
|---------------|----------|------------------------|
| `id`          | string   | Filename (immutable)   |
| `created_at`  | datetime | Filesystem birthtime   |
| `modified_at` | datetime | Filesystem mtime       |

These are never declared in the schema. They are always available in queries and views.

Documents have two parts:

- **Front matter** — YAML between `---` fences. Structured data defined by the schema.
- **Body** — Markdown content below the front matter. Only present in collections where `content: true`.

A data-only document:

```markdown
---
name: Alice Chen
email: alice@example.com
role: admin
---
```

A content document:

```markdown
---
title: Quarterly Review
author_id: usr_29gk
status: published
tags: [engineering, planning]
---

## Summary

The team shipped 14 features this quarter...
```

### 1.2 Path Templates

Each collection declares a **path template** that determines where its files live on disk. The template can reference front matter fields, controlling both the filename and the directory structure.

```yaml
posts:
  path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
```

Produces:

```
posts/
  draft/
    2026-02-10-my-draft-post.md
  published/
    2026-02-13-quarterly-review.md
```

**File movement:** Path segments that reference mutable fields cause files to move when those fields are updated. If a post's `status` changes from `draft` to `published`, the crate atomically moves the file to the new path. Consumers using the crate's API never see stale paths. Agents browsing the filesystem can infer the new location from the schema's path template.

**Slugification:** Field values in path templates are automatically slugified — lowercased, spaces replaced with hyphens, special characters stripped. The crate provides a deterministic, default slugifier.

**Format specifiers:** Date fields support format strings like `{date:YYYY-MM-DD}`. Other fields are slugified as-is.

**Uniqueness is a filesystem property.** If two documents resolve to the same path, they conflict. This is the only uniqueness mechanism in GroundDB — there are no separate unique constraints on fields. If a value must be unique, it belongs in the path template. The default behavior on conflict is `error` (reject the write). This can be configured to `suffix` (append `-2`, `-3`, etc.) for collections where collisions are acceptable.

**File extension determines format:**

| Extension | Format                                    |
|-----------|-------------------------------------------|
| `.md`     | YAML front matter + Markdown body         |
| `.json`   | Plain JSON object                         |
| `.jsonl`  | Line-delimited JSON (multi-record files)  |

### 1.3 Folders as Natural Indexes

Path templates implicitly create a browsable index through the filesystem structure. `posts/{status}/` means you can `ls posts/published/` to see all published posts without any tooling. This is a first-class design feature, not a side effect.

When designing path templates, structure them so the most common access patterns are reflected in the folder hierarchy. This also means that for JSONL collections, the path segments act as required query constraints — the crate uses pinned path segments to narrow file scans.

### 1.4 Views

A **view** is derived data defined by a SQL query over one or more collections. The crate parses the SQL to understand which collections and fields are involved, then maintains the view incrementally as documents change.

Views are maintained in memory at runtime and cached in the system database between restarts. Optionally, a view can be **materialized** — written as a readable file in the `views/` directory.

**Static views** have no parameters and are maintained automatically:

```yaml
views:
  post_feed:
    query: |
      SELECT p.title, p.date, p.tags, u.name AS author_name
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.status = 'published'
      ORDER BY p.date DESC
      LIMIT 100
    materialize: true
    buffer: 2x
```

**Query templates** are parameterized and executed on demand:

```yaml
views:
  post_comments:
    type: query
    query: |
      SELECT c.body, c.created_at, u.name AS commenter_name
      FROM comments c
      JOIN users u ON c.commenter_id = u.id
      WHERE c.post_id = :post_id
      ORDER BY c.created_at ASC
    params:
      post_id: { type: string }
```

Invoked via the API or CLI:

```bash
grounddb query post_comments --post_id post_a1b2
```

**View limits and buffers:** When a view has a `LIMIT`, the crate keeps a buffer in memory (default `2x` the limit) to avoid rescanning when items leave the result set. The system database persists the full buffer. Materialized files contain only the limited result.

**View dependency chain:** Views are derived from the document index in the system database, not from files. When a file write occurs, the document index updates first, then affected views rebuild from the index. This means multi-collection joins and filters are resolved entirely in memory / SQLite — the crate never opens other files to rebuild a view.

**Path-constrained queries for JSONL collections:** For collections backed by JSONL files, the crate analyzes the query's WHERE clause against the path template to determine how many files it needs to scan. If path segments aren't pinned, the crate warns about query cost via `--explain` and can enforce that path-segment fields must be specified for large collections.

---

## 2. Schema Format

The schema is a YAML file (`schema.yaml`) at the root of the data directory.

### 2.1 Collection Definitions

```yaml
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
      author_id: { type: ref, target: users, required: true }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true
    additional_properties: true
    strict: false

  comments:
    path: "comments/{parent:type}/{parent:id}/{user:id}-{created_at:YYYY-MM-DDTHHMM}.md"
    fields:
      user: { type: ref, target: users, required: true }
      parent: { type: ref, target: [posts, comments], required: true }
    content: true

  events:
    path: "events/{id}.md"
    id: { auto: ulid }
    fields:
      type: { type: string, required: true }
      payload: { type: object }
    additional_properties: true
```

### 2.2 Field Options

| Option       | Description                                                                  |
|--------------|------------------------------------------------------------------------------|
| `type`       | `string`, `number`, `boolean`, `date`, `datetime`, `list`, `object`, `ref`   |
| `required`   | Document is invalid without this field. Default: `false`                     |
| `enum`       | Restrict to a set of allowed values                                          |
| `default`    | Value applied on create if not provided. Written explicitly to the file      |
| `target`     | For `ref` type: names another collection (or list of collections). Value must be a valid document ID in that collection |
| `items`      | For `list` type: the type of each element                                    |
| `on_delete`  | For `ref` type: policy when the referenced document is deleted (see §4.2)    |

### 2.3 Collection Options

| Option                    | Description                                                          |
|---------------------------|----------------------------------------------------------------------|
| `path`                    | Path template with field interpolation and slugification             |
| `content`                 | `true` if documents have a Markdown body. Default: `false`           |
| `additional_properties`   | Allow fields not declared in the schema. Default: `false`            |
| `strict`                  | `true` = validation errors reject the write. `false` = warn only     |
| `readonly`                | `true` = crate will never modify files in this collection            |
| `id.auto`                 | Auto-generate IDs: `ulid`, `uuid`, `nanoid`                         |
| `id.on_conflict`          | `error` (default, reject) or `suffix` (append `-2`, `-3`, etc.)     |
| `on_delete`               | Default referential integrity policy for references pointing here    |

### 2.4 View Definitions

```yaml
views:
  post_feed:
    query: |
      SELECT p.title, p.date, p.tags, u.name AS author_name
      FROM posts p
      JOIN users u ON p.author_id = u.id
      WHERE p.status = 'published'
      ORDER BY p.date DESC
      LIMIT 100
    materialize: true
    buffer: 2x

  user_lookup:
    query: |
      SELECT id, email, name
      FROM users
      ORDER BY name ASC
    materialize: true

  post_comments:
    type: query
    query: |
      SELECT c.body, c.created_at, u.name AS commenter_name
      FROM comments c
      JOIN users u ON c.commenter_id = u.id
      WHERE c.post_id = :post_id
      ORDER BY c.created_at ASC
    params:
      post_id: { type: string }
```

The crate parses each SQL statement to extract referenced collections, joined fields, filter conditions, sort order, and limits. This drives view maintenance — the crate knows which file changes affect which views without trial and error.

### 2.5 JSONL / External Collections

For multi-record files and external data sources:

```yaml
collections:
  sessions:
    path: "~/.claude/projects/{project}/{id}.jsonl"
    readonly: true
    records:
      base:
        type: { type: string, enum: [user, assistant, progress, system], required: true }
        uuid: { type: string, required: true }
        timestamp: { type: datetime, required: true }
        sessionId: { type: string, required: true }
      by: type
      variants:
        user:
          fields:
            message: { type: object }
        assistant:
          fields:
            requestId: string
            message: { type: object }
```

The `records` block defines a discriminated union using `by` as the discriminator field and `variants` for type-specific fields. The `base` block defines fields shared across all variants.

### 2.6 Reusable Types

```yaml
types:
  address:
    street: { type: string, required: true }
    city: { type: string, required: true }
    state: { type: string }
    zip: { type: string }

collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      home_address: { type: address }
      work_address: { type: address }
```

Reusable types generate their own Rust structs during codegen and can be shared across collections.

### 2.7 Referential Integrity

Each reference field can declare its own deletion policy, or fall back to the collection-level `on_delete` default:

```yaml
fields:
  author_id:
    type: ref
    target: users
    required: true
    on_delete: error       # refuse to delete the user

  reviewer_id:
    type: ref
    target: users
    on_delete: nullify     # set to null if user is deleted

  attachments:
    type: list
    items: { type: ref, target: media, on_delete: cascade }
```

| Policy     | Behavior                                                      |
|------------|---------------------------------------------------------------|
| `error`    | Reject the delete if any document references this one         |
| `cascade`  | Delete all documents that reference this one                  |
| `nullify`  | Set the reference field to null in all referencing documents   |
| `archive`  | Move referencing documents to an `_archive/` subdirectory     |

---

## 3. Code Generation

GroundDB generates Rust types from `schema.yaml` at compile time via a `build.rs` build script. This provides full type safety, autocomplete, and compile-time validation of all data access.

### 3.1 Generated Types

From the schema in §2.1, the build script generates:

```rust
// ── Enums ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    Admin,
    Member,
    Guest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostStatus {
    Draft,
    Published,
    Archived,
}

// ── Documents ──────────────────────────────────────────

/// A document in the `users` collection.
/// Path: users/{name}.md
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub email: String,
    #[serde(default = "UserRole::default")]
    pub role: UserRole,
}

/// A document in the `posts` collection.
/// Path: posts/{status}/{date}-{title}.md
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Post {
    pub title: String,
    pub author_id: String,
    pub date: NaiveDate,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "PostStatus::default")]
    pub status: PostStatus,
}

/// A document in the `comments` collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comment {
    pub user: String,
    pub parent: ParentRef,
}

/// Polymorphic reference to posts or comments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "id")]
pub enum ParentRef {
    Posts(String),
    Comments(String),
}

/// A document in the `events` collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub r#type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

// ── Document wrappers with metadata ────────────────────

/// A loaded document with implicit fields.
#[derive(Debug, Clone)]
pub struct Document<T> {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub data: T,
    pub content: Option<String>,  // Markdown body, if collection has content: true
}
```

### 3.2 Typed Collection Handles

```rust
// ── Store interface ────────────────────────────────────

impl Store {
    pub fn users(&self) -> Collection<User> { ... }
    pub fn posts(&self) -> Collection<Post> { ... }
    pub fn comments(&self) -> Collection<Comment> { ... }
    pub fn events(&self) -> Collection<Event> { ... }
}

impl<T> Collection<T> {
    pub fn get(&self, id: &str) -> Result<Document<T>>;
    pub fn list(&self) -> Result<Vec<Document<T>>>;
    pub fn insert(&self, data: T, content: Option<&str>) -> Result<String>;
    pub fn update(&self, id: &str, data: T) -> Result<()>;
    pub fn update_partial(&self, id: &str, fields: Partial<T>) -> Result<()>;
    pub fn delete(&self, id: &str) -> Result<()>;
}
```

Usage:

```rust
let store = Store::open("./my-app-data")?;

// Insert — compile error if you miss a required field or use wrong type
let id = store.posts().insert(
    Post {
        title: "Hello World".into(),
        author_id: "alice-chen".into(),
        date: NaiveDate::from_ymd(2026, 2, 13),
        status: PostStatus::Draft,
        tags: vec!["intro".into()],
    },
    Some("# Hello\n\nThis is my first post."),
)?;

// Get — returns Document<Post>, fully typed
let post = store.posts().get(&id)?;
println!("{}", post.data.title);       // autocomplete works
println!("{}", post.modified_at);      // implicit fields available
println!("{}", post.content.unwrap()); // markdown body

// Update — file moves from draft/ to published/ automatically
store.posts().update(&id, Post {
    status: PostStatus::Published,
    ..post.data
})?;

// Delete — referential integrity enforced at runtime
store.users().delete("alice-chen")?;
```

### 3.3 Typed Views

View result types are generated from the SQL SELECT columns:

```rust
// Generated from post_feed view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostFeedRow {
    pub title: String,
    pub date: NaiveDate,
    pub tags: Vec<String>,
    pub author_name: String,
}

// Generated from post_comments view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostCommentsRow {
    pub body: String,
    pub created_at: DateTime<Utc>,
    pub commenter_name: String,
}

#[derive(Debug, Clone)]
pub struct PostCommentsParams {
    pub post_id: String,
}

// ── Typed view access ──────────────────────────────────

impl Store {
    pub fn post_feed(&self) -> Result<Vec<PostFeedRow>>;
    pub fn post_comments(&self, params: PostCommentsParams) -> Result<Vec<PostCommentsRow>>;
}
```

Usage:

```rust
// Static view — fully typed result
let feed = store.post_feed()?;
for row in &feed {
    println!("{} by {}", row.title, row.author_name);  // autocomplete
}

// Parameterized query — typed params and results
let comments = store.post_comments(PostCommentsParams {
    post_id: "post_a1b2".into(),
})?;
```

### 3.4 Partial Updates

For `update_partial`, the build script generates a companion struct where all fields are `Option`:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostPartial {
    pub title: Option<String>,
    pub author_id: Option<String>,
    pub date: Option<NaiveDate>,
    pub tags: Option<Vec<String>>,
    pub status: Option<PostStatus>,
}

// Only update status — other fields untouched
store.posts().update_partial(&id, PostPartial {
    status: Some(PostStatus::Published),
    ..Default::default()
})?;
```

### 3.5 Batch Operations

```rust
let batch = store.batch();
batch.posts().insert(new_post, Some(&body))?;
batch.users().update("alice-chen", updated_user)?;
batch.execute()?;  // all-or-nothing
```

On failure, any files written as part of the batch are rolled back. The system database update is done in a single SQLite transaction.

---

## 4. System Database

The crate maintains a SQLite file (`_system.db`) at the data directory root. This is operational state that the crate depends on for correctness and performance.

### 4.1 What It Stores

#### Schema State

| Table            | Purpose                                                           |
|------------------|-------------------------------------------------------------------|
| `schema_history` | Hash and snapshot of every schema version the crate has seen      |
| `migrations`     | Log of all migrations applied (field additions, removals, etc.)   |

On startup, the crate hashes the current `schema.yaml` and compares it against the last-known hash. If changed, it diffs the old and new schemas to determine what migrations are needed.

#### Document Index

| Table            | Purpose                                                           |
|------------------|-------------------------------------------------------------------|
| `documents`      | Every document's ID, collection, current file path, and all front matter field values |

This is the master index. It allows the crate to resolve queries and rebuild views without scanning files at runtime. It is populated on first boot (full scan) and maintained incrementally on writes.

#### View State

| Table               | Purpose                                                        |
|---------------------|----------------------------------------------------------------|
| `view_data`         | Serialized view contents including the buffer beyond the limit |
| `view_metadata`     | Per-view: last-built timestamp, source directory hashes        |
| `directory_hashes`  | Per-collection: hash of (filenames + mtimes) for change detection |

### 4.2 Boot Lifecycle

```
1. Open _system.db
2. Check schema hash
   ├─ Match    → schema unchanged, continue
   └─ Mismatch → diff schemas, compute migrations (see §5.1)
3. For each collection:
   │  Compute directory hash (filenames + mtimes)
   │  Compare against stored hash
   ├─ Match    → collection unchanged, load views from view_data
   └─ Mismatch → identify changed/new/deleted files
                  ├─ Read only the changed files
                  ├─ Update documents table
                  └─ Mark affected views for rebuild
4. Rebuild affected views from the documents table (not from files)
5. If views have materialize: true, write view files to views/
6. Store updated directory hashes
7. Ready — all views are in memory
```

**Non-blocking boot:** The crate can serve reads immediately after step 1 using stale view data from the system database. Steps 2–6 run in the background. Once complete, the in-memory state is atomically swapped.

### 4.3 Runtime Write Lifecycle

```
1. Validate document against schema (type checking, required fields, enums)
2. Apply defaults for missing fields with default values
3. Compute target file path from path template
4. Check for path conflict
   ├─ on_conflict: error  → reject if path already exists
   └─ on_conflict: suffix → append disambiguator
5. If path changed (mutable field in path template updated):
   ├─ Atomic move: write new path, delete old path
6. Else:
   ├─ Atomic write: write to temp file, rename over target
7. Update documents table in system db
8. Update directory hash for collection
9. Check which views reference this collection
10. For each affected view:
    ├─ Determine if the changed fields are relevant to the view's query
    ├─ If yes: update the view in memory from the documents table
    │          If materialize: true, rewrite the view file
    └─ If no: skip
```

---

## 5. Lifecycle Operations

### 5.1 Schema Migration

When the schema changes between restarts, the crate computes a diff and applies migrations:

| Change                        | Action                                                       |
|-------------------------------|--------------------------------------------------------------|
| New field with `default`      | Scan all files, write default value to each document         |
| New field without `default`   | If `required: true`, error. Otherwise, no action needed      |
| New `required` field          | Error at startup unless a `default` is provided              |
| Field removed                 | Optionally strip from all files, or just stop validating     |
| Field type changed            | Error unless a migration function is provided                |
| `enum` values added           | No action needed                                             |
| `enum` values removed         | Warn about documents with now-invalid values                 |
| New collection                | Create the directory                                         |
| Collection removed            | Warn (never auto-delete data)                                |
| Path template changed         | Full reorganization: move all files to new paths             |

Migration actions are logged in the `migrations` table. File modifications are processed in batches with checkpointed progress so interrupted migrations can resume.

### 5.2 Deletion and Referential Integrity

When a document is deleted:

1. The crate queries the `documents` table for all references to the deleted document's ID.
2. For each referencing field, it applies the `on_delete` policy.
3. If any policy is `error` and referencing documents exist, the delete is rejected.
4. Otherwise, cascades, nullifications, and archives are applied atomically.
5. The system database and views update accordingly.

---

## 6. Directory Layout

```
my-app-data/
  schema.yaml                      # Collection & view definitions
  _system.db                       # SQLite — document index, view cache, schema state

  users/                           # Collection: users
    alice-chen.md
    bob-martinez.md

  posts/                           # Collection: posts
    draft/
      2026-02-10-my-draft.md
    published/
      2026-02-13-quarterly-review.md

  comments/                        # Collection: comments
    posts/
      2026-02-13-quarterly-review/
        alice-chen-2026-02-13T1430.md

  events/                          # Collection: events
    01JMCX7K9A.md
    01JMCX8B2F.md

  views/                           # Materialized view output files
    post_feed.yaml
    user_lookup.yaml
```

---

## 7. CLI Interface

The CLI wraps the crate library and provides a consistent interface for agents and humans.

### 7.1 Output Formats

All commands default to YAML output and support `--format` for alternatives:

```bash
grounddb get users alice-chen                  # YAML by default
grounddb get users alice-chen --format json    # JSON output
grounddb list posts --status published         # filter by path segment
```

### 7.2 Commands

```bash
# Document operations
grounddb get <collection> <id>
grounddb list <collection> [--path-filters...]
grounddb insert <collection> [--field value...] [--content-file body.md | --content-stdin]
grounddb update <collection> <id> [--field value...]
grounddb delete <collection> <id> [--dry-run]

# View operations
grounddb view <view_name>                        # read a static view
grounddb query <view_name> --param value         # execute a parameterized query

# Schema operations
grounddb validate                                # check all documents against schema
grounddb migrate [--dry-run]                     # apply pending migrations
grounddb status                                  # schema hash, collection stats, view health

# Utility
grounddb rebuild [--collection name]             # force full rebuild of indexes/views
grounddb export <collection> --format json       # bulk export
grounddb explain <view_name> [--params...]       # show query cost: how many files scanned
```

### 7.3 Agent Ergonomics

- **Predictable output:** Always structured, parseable. No decorative formatting by default.
- **Error output:** Machine-readable on stderr with error codes. E.g., `ERROR:PATH_CONFLICT:users/alice-chen.md already exists`
- **Stdin support:** `--content-stdin` for piping Markdown body content.
- **Dry run:** `--dry-run` on all mutations shows what would change without writing.
- **Query cost:** `grounddb explain` shows how many files a query would scan, helping agents choose efficient queries.
- **Schema introspection:** `grounddb status` gives agents a quick understanding of the current data model, collection sizes, and view health.

---

## 8. Complete Schema Example

A single file that an agent can read and immediately understand the entire data model:

```yaml
# schema.yaml

types:
  address:
    street: { type: string, required: true }
    city: { type: string, required: true }
    state: { type: string }
    zip: { type: string }

collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }
      address: { type: address }
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

  comments:
    path: "comments/{parent:type}/{parent:id}/{user:id}-{created_at:YYYY-MM-DDTHHMM}.md"
    fields:
      user: { type: ref, target: users, required: true, on_delete: cascade }
      parent: { type: ref, target: [posts, comments], required: true, on_delete: cascade }
    content: true

  events:
    path: "events/{id}.md"
    id: { auto: ulid }
    fields:
      type: { type: string, required: true }
      severity: { type: string, enum: [info, warn, error], default: info }
      payload: { type: object }
    additional_properties: true
    strict: false

views:
  post_feed:
    query: |
      SELECT p.title, p.date, p.tags, u.name AS author_name, u.id AS author_id
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
    materialize: true

  recent_activity:
    query: |
      SELECT id, title, modified_at, status
      FROM posts
      ORDER BY modified_at DESC
      LIMIT 50
    materialize: true
    buffer: 2x

  post_comments:
    type: query
    query: |
      SELECT c.id, c.created_at, c.content, u.name AS commenter_name
      FROM comments c
      JOIN users u ON c.user = u.id
      WHERE c.parent = :post_id
      ORDER BY c.created_at ASC
    params:
      post_id: { type: string }
```

---

## 9. Future Considerations

Items deliberately deferred from v1 but architecturally accounted for:

- **Heavy indexes** — Full-text search and large aggregations stored in the system database rather than as materialized view files.
- **Filesystem watcher** — `notify` crate integration for detecting external edits in real-time rather than only on boot.
- **Before/after write hooks** — Validation against external services, notifications, or data transformation pipelines.
- **Automatic path sharding** — For flat collections at scale, e.g. `events/{id:shard(2)}/{id}.md` to distribute across subdirectories automatically.
- **Cross-process locking** — Advisory locks for multiple simultaneous CLI or app instances writing to the same store.
- **Content indexing** — Making the Markdown body searchable via full-text index, not just front matter fields.
- **Encryption at rest** — For sensitive collections, transparent encryption/decryption through the crate.
- **Watch/subscribe API** — Real-time change notifications for building reactive UIs on top of the store.
