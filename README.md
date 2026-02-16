# GroundDB

[![Crates.io](https://img.shields.io/crates/v/grounddb.svg)](https://crates.io/crates/grounddb)
[![docs.rs](https://img.shields.io/docsrs/grounddb)](https://docs.rs/grounddb)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A schema-driven database that stores data as Markdown files.** Define your schema in YAML, get SQL views, file watching, and fully typed Rust structs — all backed by plain Markdown files you can read, edit, and version with Git.

## Why Markdown as a Database?

Most databases lock your data behind binary formats and query languages. GroundDB takes a different approach: **your data lives as Markdown files on disk** — human-readable, git-diffable, and editable with any text editor.

A SQLite index keeps queries fast. SQL-defined views stay in sync automatically. Compile-time codegen gives you typed Rust structs. You get the developer experience of a real database with the transparency and portability of flat files.

**Built for AI agents too** — the CLI and Markdown format make GroundDB data fully accessible to AI coding agents without custom tooling.

### What makes GroundDB different

- **Human-readable storage** — every record is a Markdown file with YAML front matter, not a binary blob
- **Git-friendly** — diff, branch, merge, and review data changes like code
- **Schema-validated** — catch errors before they hit your data, with automatic migrations for schema changes
- **SQL-powered queries** — define views with JOIN, WHERE, ORDER BY over your Markdown collections
- **Type-safe Rust** — compile-time codegen means missing a required field is a build error, not a runtime crash
- **Agent-ready** — ships with a CLI and a Claude Code skill file so AI agents can work with your data out of the box

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

## Use Cases

- **Content management** — blog posts, documentation, knowledge bases stored as Markdown you can edit anywhere
- **Configuration & settings** — schema-validated config files with automatic migration when your schema evolves
- **AI agent data layer** — give agents structured read/write access to data they can also read as plain text
- **Prototyping** — stand up a typed, queryable data layer in minutes without a database server
- **Static site generators** — power your build with queryable, validated Markdown content
- **Local-first apps** — file-based storage that works offline and syncs through Git or any file sync tool

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

GroundDB is built to be used by AI agents. One prompt is all it takes to get your agent set up.

### Give your agent this prompt

```
Read the GroundDB skill file at https://raw.githubusercontent.com/JustMaier/groundDb/main/.claude/skills/grounddb/SKILL.md — it has everything you need. Download the CLI binary for your platform, then copy the skill into this project's .claude/skills/grounddb/SKILL.md so you have it as a reference going forward.
```

That's it. The skill file tells your agent how to download the CLI, use every command, understand the schema format, and work with documents. Once installed, Claude Code will automatically surface it whenever your agent works with GroundDB data.

### What the agent gets

- **CLI download links** for every platform (Linux, macOS Intel/ARM, Windows)
- **Full command reference** — insert, update, delete, list, views, queries, validation
- **Schema and document format** guide
- **Auto-permissioned tool access** — the skill pre-authorizes `grounddb` CLI commands so the agent can work without interruptions

### Manual setup

If you prefer to set things up yourself:

1. **Get the CLI** — download the `grounddb` binary for your platform from the [latest release](https://github.com/JustMaier/groundDb/releases/latest)
2. **Install the skill** — copy [`.claude/skills/grounddb/`](.claude/skills/grounddb/) into your project's `.claude/skills/` directory
3. **Read the schema** — run `grounddb status --data-dir <path>` or read `schema.yaml` directly to understand the data model

The CLI outputs YAML by default or JSON with `--format json` for easy parsing.

---

<details>
<summary><strong>Technical Reference</strong></summary>

### Directory Layout

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

### Document Format

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

### Specification

See the full specification at [`docs/grounddb-spec-v1.md`](docs/grounddb-spec-v1.md).

</details>

## License

MIT — see [LICENSE](LICENSE).
