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

const BY_STATUS: &str = r#"collections:
  asks:
    path: "asks/{status}/{id}.md"
    fields:
      summary: { type: string, required: true }
      status: { type: string, default: "pending" }
    content: true
"#;

/// After a rollback, a zero-filled `pending/<id>.md` can sit beside the good
/// `answered/<id>.md`. A rescan must not index the zeroed one over it.
#[test]
fn a_rescan_skips_a_zero_filled_file_and_keeps_the_good_copy() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), BY_STATUS).unwrap();
    {
        let store = Store::open(&dir.path().to_string_lossy()).unwrap();
        store.insert_dynamic("asks", serde_json::json!({ "id": "a1", "summary": "ship it?" }), Some("the body")).unwrap();
        store.update_partial_dynamic("asks", "a1", serde_json::json!({ "status": "answered" })).unwrap();
    }
    let good = dir.path().join("asks").join("answered").join("a1.md");
    assert!(good.exists(), "the answer moved the file to answered/");
    let zeroed = dir.path().join("asks").join("pending").join("a1.md");
    std::fs::create_dir_all(zeroed.parent().unwrap()).unwrap();
    std::fs::write(&zeroed, vec![0u8; 300]).unwrap();

    // Reopening rescans the changed directory.
    let store = Store::open(&dir.path().to_string_lossy()).unwrap();
    let doc = store.get_dynamic("asks", "a1").unwrap();
    assert_eq!(doc["summary"], "ship it?", "the good copy is what the index serves");
    assert_eq!(doc["status"], "answered");
    assert!(zeroed.exists(), "the damaged file is left on disk");
}

/// A real document may quote a NUL in its body (a mail citing `"\x00"`,
/// 2026-10-20). Only a file with no front matter and nothing but NULs is
/// crash damage: this one must index on a rescan and accept updates.
#[test]
fn a_whole_file_with_a_nul_in_its_body_indexes_and_updates() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), SCHEMA).unwrap();
    let body = "const SEP = \"\0\";\nthe rest of the mail";
    {
        let store = Store::open(&dir.path().to_string_lossy()).unwrap();
        store.insert_dynamic("asks", serde_json::json!({ "id": "a1", "summary": "ship it?" }), Some(body)).unwrap();
    }
    // Rewrite the same bytes so the reopen rescans the file.
    let path = dir.path().join("asks").join("a1.md");
    let raw = std::fs::read(&path).unwrap();
    assert!(raw.contains(&0u8), "the body really carries a NUL");
    std::fs::write(&path, &raw).unwrap();

    let store = Store::open(&dir.path().to_string_lossy()).unwrap();
    assert_eq!(store.get_dynamic("asks", "a1").unwrap()["summary"], "ship it?", "indexed despite the NUL");
    store.update_partial_dynamic("asks", "a1", serde_json::json!({ "status": "answered" })).unwrap();
    assert_eq!(store.get_dynamic("asks", "a1").unwrap()["status"], "answered");
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
