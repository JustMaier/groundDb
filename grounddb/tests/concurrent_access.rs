//! Cross-process / cross-connection concurrency behaviour of the system DB.
//!
//! These tests use two *separate* `SystemDb` values pointed at the same file.
//! That is the same situation two OS processes are in: the in-process
//! `Mutex<Connection>` inside each `SystemDb` guards only its own connection,
//! so the only thing arbitrating between them is SQLite's own locking.

use grounddb::system_db::SystemDb;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn doc(title: &str) -> serde_yaml::Value {
    serde_yaml::from_str(&format!("title: {title}")).unwrap()
}

/// A long-lived *reader* must not be able to fail another connection's write.
///
/// This is the test that actually demonstrates WAL. In the default
/// rollback-journal mode a commit has to take an `EXCLUSIVE` lock, which is
/// incompatible with the `SHARED` lock an open read transaction holds — so the
/// writer below burns the whole busy-timeout window and then dies with
/// SQLITE_BUSY. In WAL mode readers work from a snapshot and take no lock the
/// writer cares about, so the commit lands immediately.
///
/// Verified to fail before the pragma change: `database is locked` after ~5s.
#[test]
fn an_open_reader_does_not_block_a_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("_system.db");

    let writer = SystemDb::open(&path).unwrap();
    writer
        .upsert_document("seed", "posts", "posts/seed.md", &doc("seed"), None, None, None)
        .unwrap();

    // A genuine read-only transaction, held open. This uses a raw connection
    // because SystemDb::begin_transaction is deliberately IMMEDIATE — a writer.
    let reader = rusqlite::Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN DEFERRED").unwrap();
    let seen: i64 = reader
        .query_row("SELECT count(*) FROM documents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(seen, 1);

    // The writer must get through while that read transaction is still open.
    let started = Instant::now();
    let result = writer.upsert_document(
        "written",
        "posts",
        "posts/written.md",
        &doc("written"),
        None,
        None,
        None,
    );
    let elapsed = started.elapsed();

    result.expect("an open reader must not be able to fail a write");
    assert!(
        elapsed < Duration::from_secs(1),
        "write took {elapsed:?} — it contended with the reader instead of \
         proceeding, which means WAL is not in effect"
    );

    reader.execute_batch("COMMIT").unwrap();
}

/// A second connection that writes while the first holds an open write
/// transaction waits for the commit rather than failing.
///
/// Note: this passes both before and after the pragma change — rusqlite already
/// installs a 5s busy timeout on every connection it opens, so the contending
/// write was already retrying. It is kept as a guard on that behaviour, not as
/// evidence for WAL.
#[test]
fn second_connection_waits_for_a_held_write_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("_system.db");

    let writer = SystemDb::open(&path).unwrap();
    let contender = SystemDb::open(&path).unwrap();

    // Writer takes and holds a write lock.
    writer.begin_transaction().unwrap();
    writer
        .upsert_document("held", "posts", "posts/held.md", &doc("held"), None, None, None)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let contender_thread = thread::spawn(move || {
        // Signal that we are about to contend, then block on the write.
        tx.send(()).unwrap();
        let result = contender.upsert_document(
            "other",
            "posts",
            "posts/other.md",
            &doc("other"),
            None,
            None,
            None,
        );
        (contender, result)
    });

    // Wait until the contender is about to write, then hold the lock a little
    // longer so its write genuinely collides with ours.
    rx.recv().unwrap();
    thread::sleep(Duration::from_millis(300));
    writer.commit_transaction().unwrap();

    let (contender, result) = contender_thread.join().unwrap();
    result.expect("contending write should have waited for the commit, not returned SQLITE_BUSY");

    // Both writes are durable and visible to a third connection.
    let reader = SystemDb::open(&path).unwrap();
    assert!(reader.get_document("posts", "held").unwrap().is_some());
    assert!(reader.get_document("posts", "other").unwrap().is_some());
    drop(contender);
}

/// A reader is not blocked by an in-flight writer, and sees the pre-commit
/// snapshot rather than an error or partial state.
///
/// Note: this also passes in rollback-journal mode, where a `SHARED` read lock
/// is compatible with the writer's `RESERVED` lock. It guards the snapshot
/// semantics, not WAL.
#[test]
fn reader_is_not_blocked_by_an_uncommitted_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("_system.db");

    let writer = SystemDb::open(&path).unwrap();
    writer
        .upsert_document(
            "before",
            "posts",
            "posts/before.md",
            &doc("before"),
            None,
            None,
            None,
        )
        .unwrap();

    let reader = SystemDb::open(&path).unwrap();

    writer.begin_transaction().unwrap();
    writer
        .upsert_document(
            "during",
            "posts",
            "posts/during.md",
            &doc("during"),
            None,
            None,
            None,
        )
        .unwrap();

    // Reader sees the committed row and not the uncommitted one — and crucially
    // does not error.
    assert!(reader.get_document("posts", "before").unwrap().is_some());
    assert!(reader.get_document("posts", "during").unwrap().is_none());

    writer.commit_transaction().unwrap();
    assert!(reader.get_document("posts", "during").unwrap().is_some());
}

/// A transaction that reads before it writes — the shape `Batch::commit` has,
/// since it reads each document to save its file for rollback — must survive
/// another connection committing in between.
///
/// With the SQLite-default `BEGIN DEFERRED` this failed *instantly* under WAL
/// with `SQLITE_BUSY_SNAPSHOT` (extended code 517): the transaction's read had
/// pinned a snapshot that the other connection's commit made unextendable, and
/// the busy timeout does not retry that error because waiting cannot fix it.
/// `BEGIN IMMEDIATE` takes the write lock up front instead, so the contention
/// resolves by waiting.
///
/// Verified to fail before the `BEGIN IMMEDIATE` change.
#[test]
fn transaction_that_reads_before_writing_survives_a_concurrent_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("_system.db");

    let a = SystemDb::open(&path).unwrap();
    let b = SystemDb::open(&path).unwrap();
    a.upsert_document("seed", "posts", "posts/seed.md", &doc("seed"), None, None, None)
        .unwrap();

    a.begin_transaction().unwrap();
    // Read first, exactly as the batch path does.
    assert!(a.get_document("posts", "seed").unwrap().is_some());

    // Another connection writes while A's transaction is open. Under the old
    // DEFERRED behaviour A holds no lock yet, so this commits straight away and
    // invalidates A's snapshot. Under IMMEDIATE it blocks until A commits.
    let (tx, rx) = mpsc::channel();
    let contender = thread::spawn(move || {
        tx.send(()).unwrap();
        let r = b.upsert_document("theirs", "posts", "posts/t.md", &doc("t"), None, None, None);
        (b, r)
    });

    // Give the contender long enough to actually get its write in, if it can.
    rx.recv().unwrap();
    thread::sleep(Duration::from_millis(300));

    // A writes and commits. This must not fail because of the contender.
    a.upsert_document("mine", "posts", "posts/m.md", &doc("mine"), None, None, None)
        .expect("write after read inside a transaction must not hit BUSY_SNAPSHOT");
    a.commit_transaction().unwrap();

    let (b, contender_result) = contender.join().unwrap();
    contender_result.expect("contending write should have waited for A's commit");

    let reader = SystemDb::open(&path).unwrap();
    assert!(reader.get_document("posts", "mine").unwrap().is_some());
    assert!(reader.get_document("posts", "theirs").unwrap().is_some());
    drop(b);
}

/// The pragmas are actually set on an on-disk database.
#[test]
fn on_disk_db_uses_wal_and_a_busy_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("_system.db");
    let db = SystemDb::open(&path).unwrap();

    assert_eq!(db.journal_mode().unwrap().to_lowercase(), "wal");
    assert_eq!(db.busy_timeout_ms().unwrap(), 5000);
}

/// WAL is meaningless for an in-memory database — there is no second process to
/// share it with and no shared-memory file — so we do not try to set it. The
/// busy timeout is still applied.
#[test]
fn in_memory_db_does_not_claim_wal() {
    let db = SystemDb::open_in_memory().unwrap();

    assert_ne!(db.journal_mode().unwrap().to_lowercase(), "wal");
    assert_eq!(db.busy_timeout_ms().unwrap(), 5000);
}
