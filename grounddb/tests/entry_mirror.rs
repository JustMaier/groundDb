//! A write updates the collection hash without restating the directory.
//!
//! The hash is a function of every file's path and mtime. Deriving it by
//! statting every file cost ~700ms per write on a 6,800-message store, and grew
//! with the store — the price of sending a message scaled with how much mail
//! already existed. A writer instead updates the one entry it touched.
//!
//! That is only safe if the write-derived hash is **the same value boot derives
//! from disk**. If it ever differs, boot sees a mismatch and rescans: correct,
//! but silently slow forever. So every test here pins the same invariant —
//! write, reopen, and assert the stored hash did not move — across each shape
//! of change a writer can make.

use grounddb::Store;
use std::path::Path;
use tempfile::TempDir;

const SCHEMA: &str = r#"collections:
  messages:
    path: "{recipient}/{id}.md"
    fields:
      recipient: { type: string, required: true }
      subject: { type: string, default: "" }
    content: true
"#;

fn new_store() -> TempDir {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("schema.yaml"), SCHEMA).unwrap();
    dir
}

fn open(dir: &Path) -> Store {
    Store::open(&dir.to_string_lossy()).unwrap()
}

fn stored_hash(dir: &Path) -> String {
    let db = rusqlite::Connection::open(dir.join("_system.db")).unwrap();
    db.query_row(
        "SELECT hash FROM directory_hashes WHERE collection = 'messages'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn insert(store: &Store, id: &str, recipient: &str, body: &str) {
    let data = serde_json::json!({ "id": id, "recipient": recipient, "subject": "s" });
    store.insert_dynamic("messages", data, Some(body)).unwrap();
}

/// Reopening must not move the hash. Boot recomputes from disk, so a hash that
/// changes on reopen is proof the write derived a different answer than the
/// filesystem gives.
fn assert_survives_reopen(dir: &Path, what: &str) {
    let after_write = stored_hash(dir);
    drop(open(dir));
    let after_reopen = stored_hash(dir);
    assert_eq!(
        after_write, after_reopen,
        "hash moved when reopening after {what}: the write derived a hash \
         that disagrees with disk, so every boot will rescan"
    );
}

#[test]
fn insert_leaves_a_hash_that_matches_disk() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "hello");
    }
    assert_survives_reopen(dir.path(), "an insert");
}

#[test]
fn update_in_place_leaves_a_hash_that_matches_disk() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "hello");
        store
            .update_dynamic_with_content(
                "messages",
                "m1",
                serde_json::json!({ "id": "m1", "recipient": "justin", "subject": "edited" }),
                Some("edited body"),
            )
            .unwrap();
    }
    assert_survives_reopen(dir.path(), "an in-place update");
}

/// The path is part of the template, so changing `recipient` MOVES the file.
/// If the writer reports only the new path and forgets the old one, the mirror
/// keeps a path that no longer exists and the hash is wrong forever.
#[test]
fn update_that_moves_the_file_forgets_the_old_path() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "hello");
        store
            .update_dynamic_with_content(
                "messages",
                "m1",
                serde_json::json!({ "id": "m1", "recipient": "tom", "subject": "moved" }),
                Some("hello"),
            )
            .unwrap();
    }

    assert!(
        dir.path().join("tom").join("m1.md").exists(),
        "precondition: the file really moved"
    );
    assert!(
        !dir.path().join("justin").join("m1.md").exists(),
        "precondition: the old file is gone"
    );
    assert_survives_reopen(dir.path(), "an update that moved the file");
}

#[test]
fn delete_leaves_a_hash_that_matches_disk() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "hello");
        insert(&store, "m2", "justin", "second");
        store.delete_dynamic("messages", "m1").unwrap();
    }
    assert_survives_reopen(dir.path(), "a delete");
}

/// The staleness bug that bit the boot scan: filename keys at whole-second
/// resolution made an edit landing in the same second as the previous one
/// invisible. The entry mirror stores nanoseconds for the same reason.
#[test]
fn two_edits_inside_the_same_second_are_both_reflected() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "first");
        // Deliberately no sleep — both writes land in the same wall-clock second.
        store
            .update_dynamic_with_content(
                "messages",
                "m1",
                serde_json::json!({ "id": "m1", "recipient": "justin", "subject": "a" }),
                Some("second"),
            )
            .unwrap();
        store
            .update_dynamic_with_content(
                "messages",
                "m1",
                serde_json::json!({ "id": "m1", "recipient": "justin", "subject": "b" }),
                Some("third"),
            )
            .unwrap();
    }
    assert_survives_reopen(dir.path(), "two edits inside one second");
}

/// Many writes in sequence, mixing every shape at once — the case a real
/// mailbox actually produces.
#[test]
fn a_long_mixed_sequence_still_agrees_with_disk() {
    let dir = new_store();
    {
        let store = open(dir.path());
        for i in 0..25 {
            insert(&store, &format!("m{i}"), "justin", &format!("body {i}"));
        }
        for i in 0..5 {
            store.delete_dynamic("messages", &format!("m{i}")).unwrap();
        }
        for i in 5..10 {
            store
                .update_dynamic_with_content(
                    "messages",
                    &format!("m{i}"),
                    serde_json::json!({ "id": format!("m{i}"), "recipient": "tom", "subject": "x" }),
                    Some("moved"),
                )
                .unwrap();
        }
    }
    assert_survives_reopen(dir.path(), "a mixed insert/delete/move sequence");
}

/// A store written before the entry mirror existed has no mirrored rows. The
/// first boot must reseed them rather than deriving the next write's hash from
/// an empty set.
#[test]
fn a_store_with_no_mirrored_entries_reseeds_on_boot() {
    let dir = new_store();
    {
        let store = open(dir.path());
        insert(&store, "m1", "justin", "hello");
        insert(&store, "m2", "tom", "hi");
    }

    // Simulate the pre-upgrade state: index and hash present, mirror absent.
    {
        let db = rusqlite::Connection::open(dir.path().join("_system.db")).unwrap();
        db.execute("DELETE FROM collection_entries", []).unwrap();
    }

    drop(open(dir.path()));

    let db = rusqlite::Connection::open(dir.path().join("_system.db")).unwrap();
    let mirrored: i64 = db
        .query_row("SELECT COUNT(*) FROM collection_entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mirrored, 2, "boot must reseed the entry mirror it found empty");

    // And a write on top of the reseeded mirror still agrees with disk.
    {
        let store = open(dir.path());
        insert(&store, "m3", "justin", "third");
    }
    assert_survives_reopen(dir.path(), "a write after the mirror was reseeded");
}
