---
name: grounddb
description: Interact with a GroundDB data store using the grounddb CLI. Use when you need to read, write, query, or manage documents stored as Markdown files with YAML front matter.
allowed-tools: Bash(grounddb *), Read, Grep, Glob
---

# GroundDB Skill

GroundDB is a schema-driven data layer that uses Markdown files as the source of truth.
Documents are `.md` files with YAML front matter for structured data and an optional Markdown
body. Collections, views, and path templates are all defined in `schema.yaml`.

## Prerequisites

This skill requires the `grounddb` CLI binary. Check if it's available:

```bash
grounddb --help
```

If not installed, download the pre-built binary for your platform from the
[latest GitHub release](https://github.com/JustMaier/groundDb/releases/latest):

| Platform | Archive | Binary inside |
|---|---|---|
| Linux x86_64 | `grounddb-linux-amd64.tar.gz` | `grounddb-linux-amd64` |
| macOS x86_64 | `grounddb-macos-amd64.tar.gz` | `grounddb-macos-amd64` |
| macOS ARM (Apple Silicon) | `grounddb-macos-arm64.tar.gz` | `grounddb-macos-arm64` |
| Windows x86_64 | `grounddb-windows-amd64.zip` | `grounddb-windows-amd64.exe` |

Extract the archive, rename the binary to `grounddb` (or `grounddb.exe` on Windows),
make it executable (`chmod +x grounddb` on Unix), and place it on your PATH.

Alternatively, build from source with `cargo install --path grounddb-cli` from the repository.

## Getting Started

1. **Find the data directory** — look for a folder containing `schema.yaml` and `_system.db`
2. **Read the schema** — `cat <data-dir>/schema.yaml` to understand collections, fields, and views
3. **Check status** — `grounddb status --data-dir <data-dir>` for stats and view health

All commands accept `--data-dir <path>` (defaults to `.`) and `--format json|yaml` (defaults to `yaml`).

## CLI Reference

### Documents

```bash
grounddb get <collection> <id>                          # Get a document
grounddb list <collection>                              # List all documents
grounddb list <collection> --filter status=published    # List with path filters
grounddb insert <collection> --field name="Alice"       # Insert a document
grounddb insert <collection> --field title="Post" \
    --content-file body.md                              # Insert with Markdown body
grounddb update <collection> <id> --field status=published  # Update fields
grounddb delete <collection> <id>                       # Delete a document
grounddb delete <collection> <id> --dry-run             # Preview deletion
grounddb export <collection>                            # Bulk export all docs
```

### Views & Queries

```bash
grounddb view <view_name>                               # Read a materialized view
grounddb query <view_name> --param key=value            # Run a parameterized query
grounddb explain <view_name>                            # Show rewritten SQL for a view
```

### Admin

```bash
grounddb status                                         # Schema info, stats, view health
grounddb validate                                       # Check all documents against schema
grounddb rebuild                                        # Force rebuild indexes and views
grounddb migrate --dry-run                              # Preview pending schema migrations
grounddb migrate                                        # Apply schema migrations
```

## Data Layout

Documents live on disk organized by path templates defined in the schema:

```
<data-dir>/
  schema.yaml           # Schema definition (collections, fields, views)
  _system.db            # SQLite index (auto-managed, do not edit)
  users/                # collection with path: users/{name}.md
    alice-chen.md
  posts/                # collection with path: posts/{status}/{date}-{title}.md
    draft/
      2026-02-10-my-draft.md
    published/
      2026-02-13-quarterly-review.md
  views/                # Materialized view output
    post_feed.yaml
```

## Document Format

**Data-only document** (structured fields only):

```markdown
---
name: Alice Chen
email: alice@example.com
role: admin
---
```

**Content document** (fields + Markdown body, when `content: true` in schema):

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

## Tips

- **Read the schema first.** It tells you every collection, field, type, and view available.
- **Use `--format json`** when you need to parse output programmatically.
- **Documents are just files.** You can also read/edit `.md` files directly — the CLI is a convenience layer over the filesystem.
- **Path templates matter.** When you update fields used in the path template (like `status`), the file moves automatically on disk.
- **Views stay current.** Materialized views rebuild automatically when underlying documents change.
