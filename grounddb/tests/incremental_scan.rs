//! Boot-scan correctness.
//!
//! `Store::open` reconciles the index against the tree without re-reading every
//! file. These tests pin the property that makes that safe: **whatever changed
//! while the process was down, the incremental scan must leave the index in
//! exactly the state a full rescan would leave it in.** Speed is only allowed to
//! come from doing less work, never from reaching a different answer.
//!
//! The index is read straight out of `_system.db` with rusqlite rather than
//! through the public API, because the property is about the stored rows —
//! including columns like `path` and `modified_at` that no query surfaces.

use grounddb::Store;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

const SCHEMA: &str = r#"collections:
  messages:
    path: "{recipient}/{id}.md"
    fields:
      recipient: { type: string, required: true }
      subject: { type: string, required: true }
    content: true
"#;

/// One row of the document index, in full.
type IndexRow = (
    String,         // id
    String,         // collection
    String,         // path
    String,         // data_json
    Option<String>, // created_at
    Option<String>, // modified_at
    Option<String>, // content_text
);

fn snapshot(dir: &Path) -> Vec<IndexRow> {
    let conn = rusqlite::Connection::open(dir.join("_system.db")).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT id, collection, path, data_json, created_at, modified_at, content_text
             FROM documents ORDER BY collection, id",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn paths(rows: &[IndexRow]) -> Vec<String> {
    rows.iter().map(|r| r.2.clone()).collect()
}

fn write_message(dir: &Path, recipient: &str, id: &str, subject: &str, body: &str) {
    let bx = dir.join(recipient);
    std::fs::create_dir_all(&bx).unwrap();
    std::fs::write(
        bx.join(format!("{id}.md")),
        format!("---\nrecipient: {recipient}\nsubject: {subject}\n---\n\n{body}\n"),
    )
    .unwrap();
}

/// A store with a warm index: three messages, already scanned, process closed.
fn warm_store(n: usize) -> TempDir {
    let tmp = TempDir::new().unwrap();
    std::fs::write(tmp.path().join("schema.yaml"), SCHEMA).unwrap();
    for i in 0..n {
        write_message(
            tmp.path(),
            if i % 2 == 0 { "alice" } else { "bob" },
            &format!("msg{i:03}"),
            &format!("subject {i}"),
            &format!("body {i}"),
        );
    }
    drop(Store::open(tmp.path().to_str().unwrap()).unwrap());
    tmp
}

/// mtime granularity on Windows is coarser than the gap between two sequential
/// writes, so a rewrite has to be separated from the original in wall-clock time
/// or it lands on the same timestamp and legitimately looks unchanged.
fn let_the_clock_tick() {
    std::thread::sleep(Duration::from_millis(50));
}

/// Reopen (incremental), snapshot, then force a full rescan and snapshot again.
/// The two must agree.
fn assert_incremental_matches_full_rescan(dir: &Path) -> Vec<IndexRow> {
    let store = Store::open(dir.to_str().unwrap()).unwrap();
    drop(store);
    let incremental = snapshot(dir);

    let store = Store::open(dir.to_str().unwrap()).unwrap();
    store.rebuild(None).unwrap();
    drop(store);
    let full = snapshot(dir);

    assert_eq!(
        incremental, full,
        "incremental scan and full rescan disagree about the index"
    );
    incremental
}

#[test]
fn added_file_is_indexed_and_matches_full_rescan() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    write_message(dir, "carol", "msg999", "late arrival", "hello");

    let rows = assert_incremental_matches_full_rescan(dir);
    assert_eq!(rows.len(), 5);
    assert!(paths(&rows).contains(&"carol/msg999.md".to_string()));
}

#[test]
fn modified_file_is_reread_and_matches_full_rescan() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    let_the_clock_tick();
    write_message(dir, "alice", "msg000", "edited subject", "edited body");

    let rows = assert_incremental_matches_full_rescan(dir);
    assert_eq!(rows.len(), 4);
    let edited = rows.iter().find(|r| r.0 == "msg000").unwrap();
    assert!(
        edited.3.contains("edited subject"),
        "index kept the stale front matter: {}",
        edited.3
    );
    assert!(
        edited.6.as_deref().unwrap_or_default().contains("edited body"),
        "index kept the stale body: {:?}",
        edited.6
    );
}

#[test]
fn deleted_file_is_dropped_and_matches_full_rescan() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    std::fs::remove_file(dir.join("alice").join("msg000.md")).unwrap();

    let rows = assert_incremental_matches_full_rescan(dir);
    assert_eq!(rows.len(), 3);
    assert!(!rows.iter().any(|r| r.0 == "msg000"));
}

#[test]
fn moved_file_keeps_one_row_at_the_new_path() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    std::fs::create_dir_all(dir.join("carol")).unwrap();
    std::fs::rename(
        dir.join("alice").join("msg000.md"),
        dir.join("carol").join("msg000.md"),
    )
    .unwrap();

    let rows = assert_incremental_matches_full_rescan(dir);
    assert_eq!(rows.len(), 4, "a move must not duplicate or drop the row");
    let moved = rows.iter().find(|r| r.0 == "msg000").unwrap();
    assert_eq!(moved.2, "carol/msg000.md");
}

/// The interesting case: a move that a filename-and-second-granularity hash
/// cannot see at all. Same name, same mtime, different directory.
#[test]
fn move_that_changes_nothing_but_the_directory_is_still_noticed() {
    let tmp = warm_store(2);
    let dir = tmp.path();

    std::fs::create_dir_all(dir.join("dave")).unwrap();
    std::fs::rename(
        dir.join("bob").join("msg001.md"),
        dir.join("dave").join("msg001.md"),
    )
    .unwrap();

    // No mtime change, no name change — only the parent directory moved.
    let rows = assert_incremental_matches_full_rescan(dir);
    let moved = rows.iter().find(|r| r.0 == "msg001").unwrap();
    assert_eq!(moved.2, "dave/msg001.md");
}

#[test]
fn everything_at_once_matches_full_rescan() {
    let tmp = warm_store(6);
    let dir = tmp.path();

    let_the_clock_tick();
    write_message(dir, "alice", "msg000", "edited", "edited body"); // modified
    write_message(dir, "carol", "msg900", "new", "new body"); // added
    std::fs::remove_file(dir.join("bob").join("msg001.md")).unwrap(); // deleted
    std::fs::create_dir_all(dir.join("dave")).unwrap();
    std::fs::rename(
        dir.join("alice").join("msg002.md"),
        dir.join("dave").join("msg002.md"),
    )
    .unwrap(); // moved

    let rows = assert_incremental_matches_full_rescan(dir);
    assert_eq!(rows.len(), 6);
    let by_path = paths(&rows);
    assert!(by_path.contains(&"carol/msg900.md".to_string()));
    assert!(by_path.contains(&"dave/msg002.md".to_string()));
    assert!(!by_path.contains(&"bob/msg001.md".to_string()));
    assert!(!by_path.contains(&"alice/msg002.md".to_string()));
}

/// Proves the scan is *actually* incremental rather than merely correct.
///
/// A sentinel is written directly into the index row of a file that did not
/// change. A scan that re-reads every file would overwrite it; an incremental
/// one leaves it alone. `rebuild()` must overwrite it, which is what keeps this
/// from passing for the wrong reason.
#[test]
fn unchanged_files_are_not_reread() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    {
        let conn = rusqlite::Connection::open(dir.join("_system.db")).unwrap();
        conn.execute(
            "UPDATE documents SET content_text = 'SENTINEL' WHERE id = 'msg002'",
            [],
        )
        .unwrap();
    }

    // A new message arrives, so the collection hash changes and the scan runs.
    write_message(dir, "carol", "msg900", "new", "new body");

    drop(Store::open(dir.to_str().unwrap()).unwrap());
    let rows = snapshot(dir);
    let untouched = rows.iter().find(|r| r.0 == "msg002").unwrap();
    assert_eq!(
        untouched.6.as_deref(),
        Some("SENTINEL"),
        "an unchanged document was re-read; the scan is not incremental"
    );
    assert!(rows.iter().any(|r| r.0 == "msg900"), "the new file was missed");

    // rebuild() is still a real full rescan.
    let store = Store::open(dir.to_str().unwrap()).unwrap();
    store.rebuild(None).unwrap();
    drop(store);
    let rows = snapshot(dir);
    let rebuilt = rows.iter().find(|r| r.0 == "msg002").unwrap();
    assert_eq!(
        rebuilt.6.as_deref().map(|s| s.contains("body 2")),
        Some(true),
        "rebuild() did not re-read from disk: {:?}",
        rebuilt.6
    );
}

/// Nothing changed at all: the collection hash matches and the scan must not
/// touch a single row.
#[test]
fn an_unchanged_tree_is_not_scanned() {
    let tmp = warm_store(4);
    let dir = tmp.path();

    {
        let conn = rusqlite::Connection::open(dir.join("_system.db")).unwrap();
        conn.execute("UPDATE documents SET content_text = 'SENTINEL'", [])
            .unwrap();
    }

    drop(Store::open(dir.to_str().unwrap()).unwrap());
    for row in snapshot(dir) {
        assert_eq!(row.6.as_deref(), Some("SENTINEL"), "row {} was re-read", row.0);
    }
}

/// Two files can claim the same id — `{recipient}/{id}.md` allows the same
/// message id under two recipients, and the index is keyed by `(collection,
/// id)`. Whichever file a full rescan would file, the incremental scan must file
/// the same one, even though only one of them changed.
#[test]
fn contested_id_resolves_the_way_a_full_rescan_would() {
    let tmp = warm_store(2);
    let dir = tmp.path();

    // msg000 already exists under alice. Give bob one too.
    write_message(dir, "bob", "msg000", "bob's copy", "bob body");

    let rows = assert_incremental_matches_full_rescan(dir);
    let contested = rows.iter().find(|r| r.0 == "msg000").unwrap();
    assert!(
        contested.2 == "alice/msg000.md" || contested.2 == "bob/msg000.md",
        "unexpected path {}",
        contested.2
    );
}
