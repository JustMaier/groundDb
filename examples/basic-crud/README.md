# Basic CRUD Example

An HTTP API server demonstrating GroundDB's core CRUD operations, views, and codegen.

## What it does

- Exposes REST endpoints for four collections: **users**, **posts**, **comments**, and **events**
- Uses compile-time codegen (`build.rs`) to generate typed Rust structs from `schema.yaml`
- Demonstrates referential integrity (deleting a user cascades to their posts and comments)
- Serves four SQL-defined views: `post_feed`, `user_lookup`, `recent_activity`, `post_comments`

## Schema

The schema (`data/schema.yaml`) defines:

| Collection | Path template | Notes |
|---|---|---|
| users | `users/{name}.md` | name, email, role (enum) |
| posts | `posts/{status}/{date:YYYY-MM-DD}-{title}.md` | status organizes files into directories |
| comments | `comments/{parent:type}/{parent:id}/{user:id}-{created_at}.md` | polymorphic refs |
| events | `events/{id}.md` | auto-generated ULID |

## Running

From the repository root:

```sh
cargo run -p basic-crud
```

Or with environment variables:

```sh
GROUNDDB_DATA_DIR=data GROUNDDB_HOST=127.0.0.1 GROUNDDB_PORT=8080 cargo run -p basic-crud
```

Defaults: `data` directory, `127.0.0.1:8080`.

## API

```sh
# Status
curl http://localhost:8080/api/status

# Users
curl http://localhost:8080/api/users
curl http://localhost:8080/api/users/alice-chen
curl -X POST http://localhost:8080/api/users \
  -H 'Content-Type: application/json' \
  -d '{"name": "Charlie", "email": "charlie@example.com"}'
curl -X PUT http://localhost:8080/api/users/charlie \
  -H 'Content-Type: application/json' \
  -d '{"name": "Charlie", "email": "charlie@example.com", "role": "admin"}'
curl -X DELETE http://localhost:8080/api/users/charlie

# Posts (with Markdown body via "content" field)
curl -X POST http://localhost:8080/api/posts \
  -H 'Content-Type: application/json' \
  -d '{"title": "Hello", "author_id": "alice-chen", "date": "2026-02-14", "status": "draft", "content": "## Hello\n\nWorld."}'

# Views
curl http://localhost:8080/api/views/post_feed
curl http://localhost:8080/api/views/user_lookup
curl http://localhost:8080/api/views/recent_activity
curl "http://localhost:8080/api/views/post_comments?post_id=2026-02-13-quarterly-review"
```

## Directory layout

```
data/
  schema.yaml
  _system.db
  users/
    alice-chen.md
    bob-martinez.md
  posts/
    draft/
      2026-02-10-my-draft-post.md
    published/
      2026-02-13-quarterly-review.md
  events/
    01jmcx7k9a.md
  views/
    post_feed.yaml
    user_lookup.yaml
    recent_activity.yaml
```
