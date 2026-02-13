---
name: grounddb
description: Interact with a GroundDB data store
---

# GroundDB Skill

GroundDB is a schema-driven data layer that uses Markdown files as the source of truth.
Documents are stored as `.md` files with YAML front matter for structured data and an
optional Markdown body. Collections, views, and path templates are defined in `schema.yaml`.

## Quick Reference

### Schema Location

The schema file is at the root of the data directory:

```
<data-dir>/schema.yaml
```

Read it to understand the full data model -- collections, fields, types, views, and path templates.

### CLI (Rust binary: `grounddb`)

```bash
# Document operations
grounddb get <collection> <id>                        # Get a document
grounddb list <collection>                            # List all documents
grounddb insert <collection> --field value ...        # Insert a document
grounddb update <collection> <id> --field value ...   # Update fields
grounddb delete <collection> <id>                     # Delete a document

# View operations
grounddb view <view_name>                             # Read a static view
grounddb query <view_name> --param value              # Run a parameterized query

# Schema / admin
grounddb status                                       # Schema info, stats, view health
grounddb validate                                     # Check all documents against schema
grounddb rebuild                                      # Force rebuild indexes/views

# Options
--data-dir <path>     # Data directory (default: current directory)
--format json|yaml    # Output format (default: yaml)
```

### Node.js Client (for the example HTTP server)

The client is at `tools/grounddb-client/grounddb-client.js`. It requires Node.js 18+.

```bash
# Server status
node grounddb-client.js status

# Users
node grounddb-client.js list-users
node grounddb-client.js get-user <id>
node grounddb-client.js create-user --name "Alice" --email alice@example.com

# Posts
node grounddb-client.js list-posts
node grounddb-client.js get-post <id>
node grounddb-client.js create-post --title "Hello" --author alice-chen --date 2026-02-13
node grounddb-client.js update-post <id> --status published
node grounddb-client.js delete-post <id>

# Views
node grounddb-client.js feed                          # Published posts with author info
node grounddb-client.js users-lookup                  # User directory
node grounddb-client.js recent                        # Recently modified posts
node grounddb-client.js comments --post-id <post_id>  # Comments for a post

# Options
--server <url>   # Server URL (default: http://localhost:8080)
```

### Data Layout

Documents live in the data directory. Path templates in the schema control filesystem layout:

```
<data-dir>/
  schema.yaml           # Schema definition
  _system.db            # SQLite system database (auto-managed)
  users/                # users collection -- path: users/{name}.md
    alice-chen.md
    bob-martinez.md
  posts/                # posts collection -- path: posts/{status}/{date}-{title}.md
    draft/
      2026-02-10-my-draft-post.md
    published/
      2026-02-13-quarterly-review.md
  comments/             # comments collection
    posts/
      <post-id>/
        <user-id>-<timestamp>.md
  events/               # events collection -- path: events/{id}.md
    01JMCX7K9A.md
  views/                # Materialized view output
    post_feed.yaml
    user_lookup.yaml
```

### Document Format

Data-only document (no Markdown body):

```markdown
---
name: Alice Chen
email: alice@example.com
role: admin
---
```

Content document (has Markdown body):

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

### Common Tasks

**Find all documents in a collection:** Browse the filesystem path defined in the schema,
or use `grounddb list <collection>`.

**Modify a document:** Edit the `.md` file directly (front matter for fields, body for content),
or use `grounddb update <collection> <id> --field value`.

**Add a new collection:** Edit `schema.yaml` to add the collection definition with path
template, fields, and options. Run `grounddb rebuild` afterward.

**Check data integrity:** Run `grounddb validate` to check all documents against the schema.

**Query views:** Use `grounddb view <name>` for static views or
`grounddb query <name> --param value` for parameterized queries.
