//! Views with `:named` placeholders are query templates, declared or not.
//!
//! A view whose SQL carries placeholders can only be answered with values
//! supplied at call time. Materialising it binds every placeholder to NULL, so
//! it stores `[]` — and then every boot and every write rebuilds that same `[]`
//! at real cost. Before this was fixed, a mail store with 7 such views spent
//! ~208ms per message rewriting empty arrays.
//!
//! These tests pin both halves: the view is not treated as static, and — the
//! part that matters — reads still see writes immediately, because parameterised
//! views were always answered by live SQL rather than from the materialised
//! blob. The performance fix must not turn into a staleness change.

use grounddb::Store;
use std::collections::HashMap;
use tempfile::TempDir;

/// Neither `type: query` nor a `params:` block is declared — the placeholders in
/// the SQL are the only evidence that this view is parameterised. That is
/// exactly the shape the mail store ships.
const SCHEMA: &str = r#"collections:
  messages:
    path: "{recipient}/{id}.md"
    fields:
      recipient: { type: string, required: true }
      status: { type: string, default: unread }
    content: true

views:
  inbox_by_recipient:
    query: |
      SELECT id, recipient, status, content
      FROM messages
      WHERE recipient = :recipient
      ORDER BY id

  all_messages:
    query: |
      SELECT id, recipient, status, content
      FROM messages
      ORDER BY id
"#;

fn store() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), SCHEMA).unwrap();
    let store = Store::open(&dir.path().to_string_lossy()).unwrap();
    (dir, store)
}

fn insert(store: &Store, id: &str, recipient: &str, body: &str) {
    let data = serde_json::json!({
        "id": id,
        "recipient": recipient,
        "status": "unread",
    });
    store.insert_dynamic("messages", data, Some(body)).unwrap();
}

fn params(recipient: &str) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("recipient".into(), recipient.to_string());
    p
}

/// The property Tom flagged: whether a view is materialised or computed is
/// observable as *staleness*, not just speed. A message inserted and immediately
/// queried must be visible — `drain_unread_to_delivered` runs at connect and
/// depends on seeing writes that just landed.
#[test]
fn a_message_is_visible_to_the_view_immediately_after_insert() {
    let (_dir, store) = store();

    insert(&store, "m1", "justin", "first");
    let rows = store.query_dynamic("inbox_by_recipient", &params("justin")).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1, "insert must be visible at once");

    insert(&store, "m2", "justin", "second");
    let rows = store.query_dynamic("inbox_by_recipient", &params("justin")).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        2,
        "a second insert must also be visible with no rebuild in between"
    );
}

/// The parameter has to actually filter. A view answered from a materialised
/// blob would have bound `:recipient` to NULL and matched nothing.
#[test]
fn the_parameter_filters_rather_than_matching_nothing() {
    let (_dir, store) = store();

    insert(&store, "m1", "justin", "for justin");
    insert(&store, "m2", "tom", "for tom");

    let justin = store.query_dynamic("inbox_by_recipient", &params("justin")).unwrap();
    let tom = store.query_dynamic("inbox_by_recipient", &params("tom")).unwrap();

    assert_eq!(justin.as_array().unwrap().len(), 1);
    assert_eq!(tom.as_array().unwrap().len(), 1);
    assert_eq!(
        justin.as_array().unwrap()[0]["recipient"], "justin",
        "the bound parameter must select the row, not NULL-match everything"
    );
}

/// A parameterised view is never materialised, so nothing is persisted for it.
/// This is what removes the per-write rebuild cost.
#[test]
fn a_parameterised_view_is_not_materialised() {
    let (dir, store) = store();
    insert(&store, "m1", "justin", "body");

    let db = rusqlite::Connection::open(dir.path().join("_system.db")).unwrap();
    let stored: Option<String> = db
        .query_row(
            "SELECT data_json FROM view_data WHERE view_name = 'inbox_by_recipient'",
            [],
            |r| r.get(0),
        )
        .ok();

    assert!(
        stored.is_none(),
        "a parameterised view should not be materialised; found {stored:?}"
    );
}

/// The classification must stay narrow: a view with no placeholders is still
/// static and still materialised. Otherwise this fix would quietly disable
/// materialisation for every view in the schema.
#[test]
fn a_view_without_placeholders_is_still_materialised() {
    let (dir, store) = store();
    insert(&store, "m1", "justin", "body");

    let db = rusqlite::Connection::open(dir.path().join("_system.db")).unwrap();
    let stored: String = db
        .query_row(
            "SELECT data_json FROM view_data WHERE view_name = 'all_messages'",
            [],
            |r| r.get(0),
        )
        .expect("a placeholder-free view must still be materialised");

    let rows: serde_json::Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        rows.as_array().unwrap().len(),
        1,
        "and it must hold real rows, not an empty array"
    );
}
