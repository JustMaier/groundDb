//! Record collections: a collection whose path ends in `.jsonl` holds many
//! records per file, one per line. This is the store half; the file half is
//! [`crate::jsonl`], the index half is `system_db::records`. Design:
//! `docs/jsonl-collections.md`.
//!
//! Every path here reaches the index the same way: [`Store::index_record_file`]
//! reads a file from where its index stopped. Appends, boot and the watcher
//! all call it, so a record is indexed from the bytes on disk, never from the
//! value a caller passed in, and the watcher echo of an append finds nothing
//! new to read.

use super::{ChangeEvent, Store};
use crate::error::{GroundDbError, Result};
use crate::jsonl;
use crate::schema::CollectionDefinition;
use crate::system_db::{record_id, FoundRecord, RecordFile, RecordQuery};
use crate::util::json_to_yaml;
use crate::validation;
use crate::watcher::{ChangeKind, WatcherEvent};
use std::collections::HashSet;

/// How far into a file to look for its first line when hashing its head.
const HEAD_LIMIT: u64 = 1 << 20;

/// What one indexing pass over a file did.
#[derive(Debug, Default)]
pub(super) struct IndexPass {
    /// Records newly indexed, in file order, with their data. Empty unless
    /// the pass was asked to keep them.
    pub new: Vec<(String, serde_json::Value)>,
    /// How many records were newly indexed.
    pub indexed: usize,
    /// The file had shrunk or been replaced, so it was reindexed from 0.
    pub reindexed: bool,
}

impl Store {
    /// Whether `name` is a record (`.jsonl`) collection.
    pub fn is_record_collection(&self, name: &str) -> bool {
        self.schema
            .collections
            .get(name)
            .is_some_and(|c| c.file_extension() == "jsonl")
    }

    fn record_collection(&self, name: &str) -> Result<&CollectionDefinition> {
        let def = self.schema.collections.get(name).ok_or_else(|| GroundDbError::CollectionNotFound {
            name: name.to_string(),
        })?;
        if def.file_extension() != "jsonl" {
            return Err(GroundDbError::Other(format!(
                "Collection '{name}' is not a record (.jsonl) collection"
            )));
        }
        Ok(def)
    }

    /// Append `record` to the partition its own fields render the path
    /// template to. Returns the new record's id. See [`Self::append_record_to`].
    pub fn append_record(&self, collection: &str, record: serde_json::Value) -> Result<String> {
        let partition = record.clone();
        self.append_record_to(collection, &partition, record)
    }

    /// Append `record` to the file `partition` renders the collection's path
    /// template to (`{date}` → `ambient/2026-10-14.jsonl`), and index it.
    /// The partition values are used as given: nothing is derived from the
    /// record. Returns the new record's id (`{path}@{offset:012}`).
    ///
    /// One write of the whole line, then `sync_data`; a torn tail left by an
    /// earlier crash is terminated first (see [`crate::jsonl`]). Appends in
    /// one process are serialised. Appends from another process at the same
    /// time are not supported.
    pub fn append_record_to(
        &self,
        collection: &str,
        partition: &serde_json::Value,
        record: serde_json::Value,
    ) -> Result<String> {
        let def = self.record_collection(collection)?;
        if def.readonly {
            return Err(GroundDbError::Other(format!("Collection '{collection}' is readonly")));
        }
        self.validate_record(def, &record)?;
        let rel = self.path_templates[collection].render(&json_to_yaml(partition), None)?;
        if !rel.ends_with(".jsonl") {
            return Err(GroundDbError::Other(format!("partition path '{rel}' is not a .jsonl file")));
        }
        let abs = self.root.join(&rel);

        let _guard = self.record_lock.lock().unwrap();
        let offset = jsonl::append_line(&abs, &record)?;
        let pass = self.index_record_file_locked(collection, &rel, true)?;
        self.notify_records(collection, &pass);
        let id = record_id(&rel, offset);
        if !pass.new.iter().any(|(i, _)| *i == id) {
            // Our line ends in `\n` and nothing but this store writes the
            // file, so the pass must have read it. If it did not, say so
            // rather than hand back an id that resolves to nothing.
            return Err(GroundDbError::Other(format!(
                "appended to {rel} at byte {offset} but the line was not indexed"
            )));
        }
        Ok(id)
    }

    /// Check a record against the collection's `records` block (base fields
    /// plus the variant its `by` field names), or against its `fields` when
    /// there is no `records` block.
    fn validate_record(&self, def: &CollectionDefinition, record: &serde_json::Value) -> Result<()> {
        if !record.is_object() {
            return Err(GroundDbError::Validation("a JSONL record must be a JSON object".into()));
        }
        let mut flat = def.clone();
        if let Some(records) = &def.records {
            let tag = record.get(&records.by).and_then(|v| v.as_str()).ok_or_else(|| {
                GroundDbError::Validation(format!("record has no string '{}' field", records.by))
            })?;
            let variant = records.variants.get(tag);
            if variant.is_none() && def.strict {
                return Err(GroundDbError::Validation(format!(
                    "unknown record variant '{}: {tag}'",
                    records.by
                )));
            }
            flat.fields = records.base.clone();
            if let Some(v) = variant {
                flat.fields.extend(v.fields.clone());
            }
        } else if def.fields.is_empty() {
            return Ok(());
        }
        let data = json_to_yaml(record);
        let result = validation::validate_document(&self.schema, &flat, &data);
        if !result.is_ok() {
            return Err(GroundDbError::Validation(format!(
                "Record validation failed:\n  - {}",
                result.errors.join("\n  - ")
            )));
        }
        Ok(())
    }

    /// Index one file from where its index stopped. Takes the record lock.
    pub(super) fn index_record_file(&self, collection: &str, rel: &str, keep: bool) -> Result<IndexPass> {
        let _guard = self.record_lock.lock().unwrap();
        self.index_record_file_locked(collection, rel, keep)
    }

    /// [`Self::index_record_file`], for a caller already holding the lock.
    /// With `keep`, the new records' data is returned for notifying; without,
    /// lines are only checked to be JSON objects, which is what keeps a cold
    /// index of a large collection cheap.
    ///
    /// | file vs its `record_files` row | pass |
    /// |---|---|
    /// | same length, same head | nothing (an append's echo lands here) |
    /// | longer, same head | from `indexed_to` |
    /// | shorter, or a different first line | everything, from 0, warned |
    fn index_record_file_locked(&self, collection: &str, rel: &str, keep: bool) -> Result<IndexPass> {
        let abs = self.root.join(rel);
        let len = std::fs::metadata(&abs)?.len();
        let head = jsonl::head_hash(&abs, HEAD_LIMIT)?.map(|h| format!("{h:016x}")).unwrap_or_default();
        let prior = self.db.get_record_file(collection, rel)?;

        let mut pass = IndexPass::default();
        let (from, lines) = match &prior {
            None => (0, 0),
            Some(p) if len < p.indexed_to || (!p.head_hash.is_empty() && p.head_hash != head) => {
                log::warn!(
                    "{rel}: record file was {} (indexed to byte {}, now {len} bytes); \
                     an append-only file was rewritten, so it is reindexed from the start",
                    if len < p.indexed_to { "truncated" } else { "replaced" },
                    p.indexed_to
                );
                pass.reindexed = true;
                (0, 0)
            }
            Some(p) if len == p.indexed_to => return Ok(pass),
            Some(p) => (p.indexed_to, p.lines),
        };

        self.db.begin_transaction()?;
        let result = (|| -> Result<()> {
            if pass.reindexed {
                self.db.delete_file_records(collection, rel)?;
            }
            let end = jsonl::scan_from(&abs, from, lines, |line| {
                if line.bytes.iter().all(|b| b.is_ascii_whitespace()) {
                    return Ok(());
                }
                let parsed = if keep {
                    match serde_json::from_slice::<serde_json::Value>(line.bytes) {
                        Ok(v @ serde_json::Value::Object(_)) => Some(Some(v)),
                        _ => None,
                    }
                } else {
                    let object = line.bytes.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{');
                    (object && serde_json::from_slice::<serde::de::IgnoredAny>(line.bytes).is_ok()).then_some(None)
                };
                match (parsed, std::str::from_utf8(line.bytes)) {
                    (Some(value), Ok(text)) => {
                        let id = self.db.upsert_record(collection, rel, line.offset, line.line_no, text)?;
                        pass.indexed += 1;
                        if let Some(v) = value {
                            pass.new.push((id, v));
                        }
                    }
                    _ => log::warn!(
                        "{rel}: line {} (byte {}) is not a JSON object; skipped",
                        line.line_no + 1,
                        line.offset
                    ),
                }
                Ok(())
            })?;
            self.db.set_record_file(
                collection,
                &RecordFile { path: rel.to_string(), indexed_to: end.indexed_to, lines: end.lines, head_hash: head },
            )?;
            Ok(())
        })();
        self.commit_or_rollback(result)?;
        Ok(pass)
    }

    fn notify_records(&self, collection: &str, pass: &IndexPass) {
        for (id, data) in &pass.new {
            self.subscriptions
                .notify_collection(collection, ChangeEvent::Inserted { id: id.clone(), data: data.clone() });
        }
    }

    /// Bring a record collection's index in line with its files: new and
    /// grown files are read from where they stopped, vanished files dropped.
    /// With `from_scratch`, every file is reindexed from 0.
    pub(super) fn sync_record_collection(&self, name: &str, from_scratch: bool) -> Result<()> {
        let base = self.root.join(self.path_templates[name].base_directory());
        if !base.exists() {
            std::fs::create_dir_all(&base)?;
        }
        let entries = self.collection_entries(name)?;
        let on_disk: HashSet<&str> = entries.iter().map(|e| e.rel.as_str()).collect();
        for f in self.db.list_record_files(name)? {
            if from_scratch || !on_disk.contains(f.path.as_str()) {
                self.db.delete_file_records(name, &f.path)?;
            }
        }
        for e in &entries {
            self.index_record_file(name, &e.rel, false)?;
        }
        Ok(())
    }

    /// Apply one watcher event to a record collection. True if the index
    /// changed.
    pub(super) fn apply_record_event(&self, collection: &str, event: &WatcherEvent) -> Result<bool> {
        let rel = event
            .path
            .strip_prefix(&self.root)
            .unwrap_or(&event.path)
            .to_string_lossy()
            .replace('\\', "/");
        if !rel.ends_with(".jsonl") {
            return Ok(false);
        }
        match event.kind {
            ChangeKind::Created | ChangeKind::Modified if event.path.exists() => {
                let pass = self.index_record_file(collection, &rel, true)?;
                self.notify_records(collection, &pass);
                Ok(pass.indexed > 0 || pass.reindexed)
            }
            _ => {
                if event.path.exists() {
                    return Ok(false);
                }
                let _guard = self.record_lock.lock().unwrap();
                Ok(self.db.delete_file_records(collection, &rel)? > 0)
            }
        }
    }

    /// Records of a record collection matching `q`, from the index.
    pub fn find_records(&self, collection: &str, q: &RecordQuery) -> Result<Vec<FoundRecord>> {
        self.record_collection(collection)?;
        self.db.find_records(collection, q)
    }

    /// How many records match `q`'s filters, ranges and path.
    pub fn count_records(&self, collection: &str, q: &RecordQuery) -> Result<u64> {
        self.record_collection(collection)?;
        self.db.count_records(collection, q)
    }

    /// The plan SQLite picks for `q`. For tests and diagnostics.
    pub fn explain_records(&self, collection: &str, q: &RecordQuery) -> Result<Vec<String>> {
        self.record_collection(collection)?;
        self.db.explain_records(collection, q)
    }

    /// Index record fields (any field, declared or not), so filters on them
    /// are served in id order. See `SystemDb::ensure_record_index`.
    pub fn ensure_record_index(&self, collection: &str, fields: &[&str]) -> Result<()> {
        self.record_collection(collection)?;
        self.db.ensure_record_index(collection, fields)
    }

    /// One record by id.
    pub fn get_record(&self, collection: &str, id: &str) -> Result<FoundRecord> {
        self.record_collection(collection)?;
        self.db.get_record(collection, id)?.ok_or_else(|| GroundDbError::NotFound {
            collection: collection.to_string(),
            id: id.to_string(),
        })
    }

    /// The newest `limit` records of one partition file, read backwards from
    /// the file itself rather than the index (the chat-log pattern), starting
    /// before byte `before` when given. A torn tail, blank lines and lines
    /// that are not JSON objects are skipped.
    pub fn tail_records(
        &self,
        collection: &str,
        rel: &str,
        before: Option<u64>,
        limit: usize,
    ) -> Result<Vec<FoundRecord>> {
        self.record_collection(collection)?;
        let mut out = Vec::with_capacity(limit.min(1024));
        for line in jsonl::ReverseLines::open(&self.root.join(rel), before)? {
            if out.len() >= limit {
                break;
            }
            let (offset, bytes) = line?;
            if let Ok(data @ serde_json::Value::Object(_)) = serde_json::from_slice(&bytes) {
                out.push(FoundRecord { id: record_id(rel, offset), path: rel.to_string(), offset, line_no: None, data });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    const SCHEMA: &str = r#"collections:
  ambient:
    path: "ambient/{date}.jsonl"
    additional_properties: true
"#;

    fn open() -> (TempDir, Store) {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("schema.yaml"), SCHEMA).unwrap();
        let store = Store::open(tmp.path().to_str().unwrap()).unwrap();
        (tmp, store)
    }

    fn inserted(store: &Store) -> Arc<Mutex<Vec<String>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        store.on_collection_change(
            "ambient",
            Box::new(move |e| {
                if let ChangeEvent::Inserted { id, .. } = e {
                    s.lock().unwrap().push(id);
                }
            }),
        );
        seen
    }

    fn event(store: &Store, rel: &str, kind: ChangeKind) -> WatcherEvent {
        WatcherEvent { path: store.root.join(rel), kind }
    }

    #[test]
    fn the_watcher_echo_of_an_append_indexes_nothing_new() {
        let (_tmp, store) = open();
        let seen = inserted(&store);
        let rel = "ambient/2026-10-14.jsonl";
        let a = store.append_record_to("ambient", &json!({"date": "2026-10-14"}), json!({"n": 1})).unwrap();
        let b = store.append_record_to("ambient", &json!({"date": "2026-10-14"}), json!({"n": 2})).unwrap();
        assert_eq!(*seen.lock().unwrap(), [a.clone(), b.clone()]);

        // The Modified events those appends cause arrive later, twice over.
        store.apply_watcher_events(vec![event(&store, rel, ChangeKind::Modified), event(&store, rel, ChangeKind::Modified)]).unwrap();
        assert_eq!(*seen.lock().unwrap(), [a.clone(), b.clone()], "an echo re-announced records");

        // An outside writer's line is announced once, from its offset.
        let mut f = std::fs::OpenOptions::new().append(true).open(store.root.join(rel)).unwrap();
        std::io::Write::write_all(&mut f, b"{\"n\":3}\n").unwrap();
        drop(f);
        store.apply_watcher_events(vec![event(&store, rel, ChangeKind::Modified)]).unwrap();
        let seen_now = seen.lock().unwrap().clone();
        assert_eq!(seen_now.len(), 3);
        assert_eq!(seen_now[2], record_id(rel, 16));
        assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 3);

        // A deleted file takes its records with it.
        std::fs::remove_file(store.root.join(rel)).unwrap();
        store.apply_watcher_events(vec![event(&store, rel, ChangeKind::Deleted)]).unwrap();
        assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 0);
    }

    #[test]
    fn a_line_finished_after_a_watcher_pass_is_indexed_whole() {
        let (_tmp, store) = open();
        let rel = "ambient/x.jsonl";
        let abs = store.root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, b"{\"n\":1}\n{\"n\":").unwrap();
        store.apply_watcher_events(vec![event(&store, rel, ChangeKind::Created)]).unwrap();
        assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 1);
        let mut f = std::fs::OpenOptions::new().append(true).open(&abs).unwrap();
        std::io::Write::write_all(&mut f, b"2}\n").unwrap();
        drop(f);
        store.apply_watcher_events(vec![event(&store, rel, ChangeKind::Modified)]).unwrap();
        let rows = store.find_records("ambient", &RecordQuery::default()).unwrap();
        assert_eq!(rows.iter().map(|r| r.data["n"].as_i64().unwrap()).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(rows[1].offset, 8);
    }
}
