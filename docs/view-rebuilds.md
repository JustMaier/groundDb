# View Rebuild Behavior

Practical reference for when GroundDB rebuilds views, what it caches,
and how to force a rebuild. Complements [the spec](grounddb-spec-v1.md)
(sections 1.4, 4.3, 5.1) with a closer look at the runtime mechanics.

## TL;DR

- Views rebuild **automatically** on every write to a dependent collection.
- Dependency is derived from the `FROM` and `JOIN` tables in the view's SQL.
- Rebuilds read from the **system database index**, never from the `.md`
  files themselves — so joins and filters are fast.
- Results live in memory; **materialized** views also write JSON/Markdown
  to disk in `views/`.
- Schema changes or direct file edits require an **explicit rebuild**
  (`Store::rebuild` or `grounddb rebuild`).

## What triggers an automatic rebuild

Every write path through the `Store` ends with `post_write(collection)`.
That hook:

1. Asks the view engine which views depend on the collection (static only —
   query templates are never eagerly rebuilt).
2. For each affected view: re-runs the compiled SQL against the document
   index, caches the resulting rows, and re-materializes to disk if the
   view is marked `materialize: true`.
3. Fires any `on_view_change` subscribers with the new rows.

Write paths covered:
- `Store::insert_dynamic` / `Store::insert_many_dynamic`
- `Store::update_dynamic` / `Store::update_partial_dynamic` / `Store::patch_field`
- `Store::delete_dynamic`
- `Store::batch().execute()` (runs `post_write` once per affected collection
  after commit, so 1 000 inserts in a single batch ⇒ one view rebuild, not
  1 000)
- File watcher → reconciliation writes

## What does NOT trigger a rebuild

- Reads: `get_dynamic`, `list_dynamic`, `view_dynamic`, `query_dynamic`.
- Collections outside the view's FROM / JOIN set.
- Schema-only changes (new field, new enum value). The index is rebuilt by
  the migration step; views have to be rebuilt separately — see below.
- Direct edits to `.md` files while the store is open *without* the file
  watcher running. Start `Store::watch()` or reopen the store to pick them
  up.

## When to force a rebuild

| Trigger                               | Use                                |
|---------------------------------------|-------------------------------------|
| Schema change (new view, altered SQL) | `Store::rebuild(None)` on open     |
| Suspect stale view after CLI edits    | `grounddb rebuild`                 |
| Single collection re-scan             | `Store::rebuild(Some("posts"))`    |
| After a manual migration script       | `Store::rebuild(None)`             |

```rust
use grounddb::Store;

let store = Store::open("data")?;
store.rebuild(None)?;                   // everything
store.rebuild(Some("posts"))?;          // only posts + views referencing posts
```

## Materialized vs in-memory views

```yaml
views:
  post_feed:
    materialize: true       # writes views/post_feed.md after every rebuild
  comment_feed:
    materialize: false      # memory only + system-db cache
```

- Both get the same in-memory rebuild on writes.
- Materialized views additionally write to `{root}/views/{name}.md`
  (human-readable, git-friendly) on every rebuild.
- Non-materialized views are served from the system database cache
  (`view_data` table).

## Query templates

Templates (with `:param` placeholders) are **not** eagerly rebuilt on
writes. They execute on demand against the current index state. This is
intentional — a parameterized query could have unbounded shape, so the
engine doesn't try to pre-compute it.

```rust
let mut params = HashMap::new();
params.insert("post_id".into(), "2026-04-20-hello".into());
let rows = store.query_dynamic("post_comments", &params)?;
```

## Subscribing to rebuilds

If you need reactive UI or downstream caches:

```rust
let sub = store.on_view_change("post_feed", Box::new(|rows| {
    println!("post_feed now has {} rows", rows.len());
}));

// ...later
store.unsubscribe(sub);
```

Fires once per rebuild on the view. Collection-level subscriptions
(`on_collection_change`) fire once per insert/update/delete.

## Failure modes

- A view whose SQL refers to a collection that no longer exists returns a
  typed `GroundDbError::CollectionNotFound` on rebuild.
- A view with a malformed query surfaces `GroundDbError::SqlParse` at
  open-time, before any writes run.
- Materialized view files are rewritten atomically (temp + rename); a
  crashed materialize won't leave a partial file.
