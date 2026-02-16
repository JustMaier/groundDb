# GroundDB

[![Crates.io](https://img.shields.io/crates/v/grounddb.svg)](https://crates.io/crates/grounddb)
[![docs.rs](https://img.shields.io/docsrs/grounddb)](https://docs.rs/grounddb)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A schema-driven data layer that uses Markdown files as the source of truth.**

GroundDB stores your data as plain Markdown files with YAML front matter, organized on disk by path templates you define. A SQLite index keeps queries fast, SQL-defined views stay up to date automatically, and compile-time codegen gives you fully typed Rust structs.

## Features

- **Schema-driven** — collections, fields, types, and views defined in a single `schema.yaml`
- **Path templates** — `posts/{status}/{date:YYYY-MM-DD}-{title}.md` maps fields to filesystem paths; files move automatically when fields change
- **YAML front matter** — structured data in every document, validated against the schema
- **Markdown body** — optional rich content below the front matter
- **SQL views** — define views with SQL queries (JOIN, WHERE, ORDER BY, LIMIT) over collections; CTE-rewritten and executed against the SQLite index
- **File watching** — monitors collection directories for external changes; updates the index and rebuilds affected views automatically
- **Subscriptions** — register callbacks for collection changes (`on_collection_change`) or view updates (`on_view_change`) to drive reactive UIs
- **Schema migration** — diffs old and new schemas on startup; auto-applies safe changes (new fields with defaults, new collections), blocks unsafe ones
- **Compile-time codegen** — `grounddb-codegen` generates typed Rust structs, enums, partial-update types, and store accessors
- **Referential integrity** — `error`, `cascade`, `nullify`, and `archive` deletion policies across collections
- **Auto IDs** — `ulid`, `uuid`, or `nanoid` generation
- **Batch operations** — all-or-nothing writes with full file rollback
- **CLI** — `grounddb-cli` for humans and agents alike

## Quick Start

Add GroundDB to your project:

```sh
cargo add grounddb
cargo add grounddb-codegen --build
```

### Define your schema

Create a `schema.yaml` in your data directory:

```yaml
collections:
  users:
    path: "users/{name}.md"
    fields:
      name: { type: string, required: true }
      email: { type: string, required: true }
      role: { type: string, enum: [admin, member, guest], default: member }

  posts:
    path: "posts/{status}/{date:YYYY-MM-DD}-{title}.md"
    fields:
      title: { type: string, required: true }
      author_id: { type: ref, target: users, required: true }
      date: { type: date, required: true }
      tags: { type: list, items: string }
      status: { type: string, enum: [draft, published, archived], default: draft }
    content: true

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
```

### Set up code generation

Add a `build.rs`:

```rust
fn main() {
    grounddb_codegen::generate_from_schema(
        "data/schema.yaml",
        "src/generated.rs",
    ).expect("codegen failed");
}
```

Then include the generated module in your code:

```rust
mod generated;
use generated::*;

use grounddb::Store;

fn main() -> grounddb::Result<()> {
    let store = Store::open("data")?;

    // Insert a user — compile error if you miss a required field
    store.users().insert(User {
        name: "Alice Chen".into(),
        email: "alice@example.com".into(),
        role: UserRole::Admin,
    }, None)?;

    // Query a view — fully typed result
    let feed = store.post_feed()?;
    for row in &feed {
        println!("{} by {}", row.title, row.author_name);
    }

    Ok(())
}
```

## CLI

Download a pre-built binary from the [latest release](https://github.com/JustMaier/groundDb/releases/latest), or build from source:

```sh
cargo install --path grounddb-cli
```

```sh
grounddb get users alice-chen                   # get a document
grounddb list posts --filter status=published   # list with path filters
grounddb insert users --field name="Bob" --field email=bob@example.com
grounddb update posts my-post --field status=published
grounddb delete users alice-chen
grounddb view post_feed                         # read a materialized view
grounddb query post_comments --param post_id=my-post  # parameterized query
grounddb validate                               # check all docs against schema
grounddb status                                 # schema info and stats
grounddb explain post_feed                      # show rewritten SQL for a view
grounddb migrate --dry-run                      # preview pending schema migrations
grounddb rebuild                                # force re-index and view rebuild
```

## AI Agents

GroundDB is designed to be agent-friendly. If you're an AI agent (or setting one up to work with GroundDB data):

1. **Get the CLI** — download the `grounddb` binary for your platform from the [latest release](https://github.com/JustMaier/groundDb/releases/latest)
2. **Install the Claude Code skill** — copy [`.claude/skills/grounddb/`](.claude/skills/grounddb/) into your project's `.claude/skills/` directory for automatic CLI guidance
3. **Read the schema** — run `grounddb status --data-dir <path>` or read `schema.yaml` directly to understand the data model

The CLI outputs YAML by default or JSON with `--format json` for easy parsing.

## Directory Layout

```
my-app-data/
  schema.yaml                        # collection & view definitions
  _system.db                         # SQLite index (auto-managed)
  users/
    alice-chen.md
    bob-martinez.md
  posts/
    draft/
      2026-02-10-my-draft.md
    published/
      2026-02-13-quarterly-review.md
  views/
    post_feed.yaml
```

## Document Format

Data-only document:

```markdown
---
name: Alice Chen
email: alice@example.com
role: admin
---
```

Content document (`content: true`):

```markdown
---
title: Quarterly Review
author_id: alice-chen
date: 2026-02-13
tags: [engineering, planning]
status: published
---

## Summary

The team shipped 14 features this quarter...
```

## Specification

See the full specification at [`docs/grounddb-spec-v1.md`](docs/grounddb-spec-v1.md).

## License

MIT — see [LICENSE](LICENSE).
