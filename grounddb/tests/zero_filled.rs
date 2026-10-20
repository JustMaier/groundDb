//! A file zero-filled by a hard crash (the rename survived, the data did not)
//! must not be "updated" into a document that has lost every field.
//!
//! On 2026-10-19 an answered ask was written over exactly such a file: the
//! partial update read no front matter, merged the answer onto an empty base
//! plus defaults, and saved it. Every other field was gone.

use grounddb::Store;

const SCHEMA: &str = r#"collections:
  asks:
    path: "asks/{id}.md"
    fields:
      summary: { type: string, required: true }
      status: { type: string, default: "pending" }
    content: true
"#;

#[test]
fn update_partial_refuses_a_zero_filled_file_and_leaves_it_alone() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), SCHEMA).unwrap();
    let store = Store::open(&dir.path().to_string_lossy()).unwrap();
    let id = store
        .insert_dynamic("asks", serde_json::json!({ "id": "a1", "summary": "ship it?" }), Some("the body"))
        .unwrap();

    // What the crash left: the same length, every byte NUL.
    let path = dir.path().join("asks").join(format!("{id}.md"));
    let len = std::fs::metadata(&path).unwrap().len() as usize;
    std::fs::write(&path, vec![0u8; len]).unwrap();

    let got = store.update_partial_dynamic("asks", &id, serde_json::json!({ "status": "answered" }));
    assert!(got.is_err(), "a zero-filled file must be refused, not merged onto: {got:?}");
    assert_eq!(std::fs::read(&path).unwrap(), vec![0u8; len], "the damaged file is left as it was");
}

#[test]
fn update_partial_still_merges_onto_a_whole_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), SCHEMA).unwrap();
    let store = Store::open(&dir.path().to_string_lossy()).unwrap();
    let id = store
        .insert_dynamic("asks", serde_json::json!({ "id": "a1", "summary": "ship it?" }), Some("the body"))
        .unwrap();
    store.update_partial_dynamic("asks", &id, serde_json::json!({ "status": "answered" })).unwrap();
    let doc = store.get_dynamic("asks", &id).unwrap();
    assert_eq!(doc["summary"], "ship it?");
    assert_eq!(doc["status"], "answered");
}
