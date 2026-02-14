# Live Dashboard Example

A real-time dashboard that streams view updates to the browser using Server-Sent Events (SSE) and GroundDB's file watcher.

## What it does

- Watches the `data/` directory for file changes (creates, edits, moves, deletes)
- When a Markdown file changes on disk, the watcher detects it, updates the SQLite index, rebuilds affected views, and pushes the new data to all connected browsers via SSE
- Demonstrates the reactive pattern: **file change on disk -> watcher event -> view rebuild -> SSE push -> browser update**
- If you move a file between path-template directories (e.g. `posts/draft/` to `posts/published/`), the YAML front matter is automatically reconciled to match the new path

## Schema

The schema (`data/schema.yaml`) defines two collections and two materialized views:

| Collection | Path template |
|---|---|
| users | `users/{name}.md` |
| posts | `posts/{status}/{date:YYYY-MM-DD}-{title}.md` |

| View | Description |
|---|---|
| `post_feed` | Published posts joined with author names |
| `user_lookup` | All users sorted by name |

## Running

From the repository root:

```sh
cargo run -p live-dashboard
```

Or with environment variables:

```sh
RUST_LOG=info GROUNDDB_DATA_DIR=data GROUNDDB_HOST=127.0.0.1 GROUNDDB_PORT=8080 cargo run -p live-dashboard
```

Defaults: `data` directory, `127.0.0.1:8080`.

Open **http://127.0.0.1:8080** in your browser to see the dashboard.

## Testing the live updates

While the dashboard is running, try editing files in the `data/` directory:

```sh
# Add a new user — the user_lookup view updates in the browser
cat > data/users/charlie.md << 'EOF'
---
name: Charlie
email: charlie@example.com
role: member
---
EOF

# Move a draft post to published — status updates automatically
mv data/posts/draft/2026-02-10-my-draft-post.md data/posts/published/

# Delete a file — the views update in the browser
rm data/users/charlie.md
```

## API

```sh
# Read a view
curl http://localhost:8080/api/views/post_feed
curl http://localhost:8080/api/views/user_lookup

# SSE event stream (keep open — updates push automatically)
curl -N http://localhost:8080/api/events

# Create a document via API
curl -X POST http://localhost:8080/api/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Dana", "email": "dana@example.com"}'

# Delete a document via API
curl -X DELETE http://localhost:8080/api/users/dana
```

## Architecture

```
Browser  <--SSE--  actix-web  <--broadcast channel--  watcher poll loop
                                                           |
                                                     process_watcher_events()
                                                           |
                                                     on_view_change() callbacks
```

The server spawns a background task that polls `process_watcher_events()` every 200ms. When views are rebuilt, registered `on_view_change` callbacks push data through a `tokio::sync::broadcast` channel to all connected SSE clients.
