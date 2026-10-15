//! Record (`.jsonl`) collections through the public API: append, reopen,
//! newest-first reads with a cursor, the torn last line, and a file rewritten
//! under the index. Design: `docs/jsonl-collections.md`.

use grounddb::{RecordQuery, RecordRange, Store};
use serde_json::json;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

const SCHEMA: &str = r#"collections:
  ambient:
    path: "ambient/{date}.jsonl"
    additional_properties: true
    strict: true
    records:
      base:
        type: { type: string, enum: [segment, edit], required: true }
        t0: { type: number, required: true }
      by: type
      variants:
        segment:
          fields:
            text: { type: string }
        edit:
          fields:
            ref: { type: string, required: true }
  transcripts:
    path: "transcripts/{id}.jsonl"
    readonly: true
  notes:
    path: "notes/{id}.md"
    fields:
      title: { type: string }
"#;

fn open(dir: &Path) -> Store {
    std::fs::write(dir.join("schema.yaml"), SCHEMA).unwrap();
    Store::open(dir.to_str().unwrap()).unwrap()
}

fn seg(t0: i64, text: &str, sphere: &str) -> serde_json::Value {
    json!({ "type": "segment", "t0": t0, "text": text, "sphere": sphere })
}

fn day(d: &str) -> serde_json::Value {
    json!({ "date": d })
}

fn t0s(rows: &[grounddb::FoundRecord]) -> Vec<i64> {
    rows.iter().map(|r| r.data["t0"].as_i64().unwrap()).collect()
}

/// Walk every page of `q`, `n` at a time, following the cursor.
fn walk(store: &Store, mut q: RecordQuery, n: usize) -> Vec<i64> {
    q.limit = n;
    let mut seen = Vec::new();
    loop {
        let page = store.find_records("ambient", &q).unwrap();
        if page.is_empty() {
            return seen;
        }
        q.after = Some(page.last().unwrap().id.clone());
        seen.extend(t0s(&page));
    }
}

#[test]
fn appends_survive_a_reopen_and_read_newest_first_across_days() {
    let tmp = TempDir::new().unwrap();
    let ids;
    {
        let store = open(tmp.path());
        let mut v = Vec::new();
        // Out of date order on purpose: order comes from the partition path.
        v.push(store.append_record_to("ambient", &day("2026-10-14"), seg(1, "a", "work")).unwrap());
        v.push(store.append_record_to("ambient", &day("2026-10-14"), seg(2, "b", "home")).unwrap());
        v.push(store.append_record_to("ambient", &day("2026-10-15"), seg(4, "d", "work")).unwrap());
        v.push(store.append_record_to("ambient", &day("2026-10-14"), seg(3, "c", "work")).unwrap());
        v.push(store.append_record_to("ambient", &day("2026-10-15"), seg(5, "e\nwith a newline", "work")).unwrap());
        assert_eq!(v[0], "ambient/2026-10-14.jsonl@000000000000");
        ids = v;
    }
    let store = open(tmp.path());
    let newest = RecordQuery { newest_first: true, ..Default::default() };
    assert_eq!(walk(&store, newest.clone(), 2), [5, 4, 3, 2, 1]);
    assert_eq!(walk(&store, RecordQuery::default(), 3), [1, 2, 3, 4, 5]);

    let work = RecordQuery { filters: vec![("sphere".into(), json!("work"))], newest_first: true, ..Default::default() };
    assert_eq!(walk(&store, work.clone(), 1), [5, 4, 3, 1]);
    assert_eq!(store.count_records("ambient", &work).unwrap(), 4);
    let window = RecordQuery {
        ranges: vec![RecordRange { field: "t0".into(), gte: Some(json!(2)), lt: Some(json!(5)) }],
        ..Default::default()
    };
    assert_eq!(t0s(&store.find_records("ambient", &window).unwrap()), [2, 3, 4]);

    // Ids are offsets, so they survive a reopen and a from-scratch rebuild.
    let got = store.get_record("ambient", &ids[4]).unwrap();
    assert_eq!(got.data["text"], "e\nwith a newline");
    assert_eq!(got.line_no, Some(1));
    store.rebuild(Some("ambient")).unwrap();
    assert_eq!(store.get_record("ambient", &ids[3]).unwrap().data["t0"], 3);

    // Reading the file backwards gives the same newest-first order for one day.
    let tail = store.tail_records("ambient", "ambient/2026-10-14.jsonl", None, 2).unwrap();
    assert_eq!(t0s(&tail), [3, 2]);
    let older = store.tail_records("ambient", "ambient/2026-10-14.jsonl", Some(tail[1].offset), 10).unwrap();
    assert_eq!(t0s(&older), [1]);
    assert_eq!(tail[0].id, ids[3], "the file reader and the index agree on ids");
}

#[test]
fn record_collections_refuse_document_calls_and_bad_records() {
    let tmp = TempDir::new().unwrap();
    let store = open(tmp.path());
    let id = store.append_record_to("ambient", &day("2026-10-14"), seg(1, "a", "work")).unwrap();

    let col = store.collection("ambient").unwrap();
    for err in [
        col.get(&id).map(|_| ()),
        col.list().map(|_| ()),
        col.delete(&id),
        col.insert(serde_yaml::from_str("{type: segment, t0: 1}").unwrap(), None).map(|_| ()),
    ] {
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("record (.jsonl) collection"), "{msg}");
    }
    // Validation: missing required base field, unknown variant (strict),
    // missing variant-required field, not an object.
    for bad in [
        json!({ "type": "segment" }),
        json!({ "type": "nope", "t0": 1 }),
        json!({ "type": "edit", "t0": 1 }),
        json!([1, 2]),
    ] {
        assert!(store.append_record_to("ambient", &day("2026-10-14"), bad.clone()).is_err(), "{bad}");
    }
    let edit = json!({ "type": "edit", "t0": 2, "ref": id, "field": "text", "value": "A" });
    store.append_record_to("ambient", &day("2026-10-14"), edit).unwrap();

    assert!(store.append_record("transcripts", json!({ "id": "x", "a": 1 })).unwrap_err().to_string().contains("readonly"));
    assert!(store.find_records("notes", &RecordQuery::default()).is_err());
    assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 2);
    // validate_all must still run over a store that has record collections.
    store.validate_all().unwrap();
}

#[test]
fn a_torn_last_line_is_held_back_then_terminated_by_the_next_append() {
    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("ambient/2026-10-14.jsonl");
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let good = "{\"type\":\"segment\",\"t0\":1}\n{\"type\":\"segment\",\"t0\":2}\n";
    let torn = "{\"type\":\"segment\",\"t0\":";
    std::fs::write(&file, format!("{good}{torn}")).unwrap();

    let store = open(tmp.path());
    assert_eq!(t0s(&store.find_records("ambient", &RecordQuery::default()).unwrap()), [1, 2]);

    let id = store.append_record_to("ambient", &day("2026-10-14"), seg(3, "c", "work")).unwrap();
    let expected_offset = (good.len() + torn.len() + 1) as u64;
    assert_eq!(id, format!("ambient/2026-10-14.jsonl@{expected_offset:012}"));
    let all = store.find_records("ambient", &RecordQuery::default()).unwrap();
    assert_eq!(t0s(&all), [1, 2, 3], "the torn bytes are skipped, not glued to the record");
    assert_eq!(all[2].line_no, Some(3), "the torn line still counts as a line");
    let raw = std::fs::read_to_string(&file).unwrap();
    assert!(raw.starts_with(good) && raw.contains(&format!("{torn}\n{{")), "{raw}");

    // A writer that finishes its line later is picked up from the offset.
    drop(store);
    let mut f = std::fs::OpenOptions::new().append(true).open(&file).unwrap();
    f.write_all(b"{\"type\":\"segment\",\"t0\":4").unwrap();
    drop(f);
    let store = open(tmp.path());
    assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 3, "still partial");
    drop(store);
    let mut f = std::fs::OpenOptions::new().append(true).open(&file).unwrap();
    f.write_all(b"}\n").unwrap();
    drop(f);
    let store = open(tmp.path());
    assert_eq!(t0s(&store.find_records("ambient", &RecordQuery::default()).unwrap()), [1, 2, 3, 4]);
}

#[test]
fn a_rewritten_file_is_reindexed_and_a_deleted_one_dropped() {
    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("ambient/2026-10-14.jsonl");
    {
        let store = open(tmp.path());
        for t in 1..=3 {
            store.append_record_to("ambient", &day("2026-10-14"), seg(t, "x", "work")).unwrap();
        }
        store.append_record_to("ambient", &day("2026-10-15"), seg(9, "x", "work")).unwrap();
    }
    // Truncated to a different, shorter content.
    std::fs::write(&file, "{\"type\":\"segment\",\"t0\":7}\n").unwrap();
    let store = open(tmp.path());
    assert_eq!(t0s(&store.find_records("ambient", &RecordQuery::default()).unwrap()), [7, 9]);
    drop(store);

    // Replaced by a LONGER file with a different first line: same check, by head.
    let body: String = (10..15).map(|t| format!("{{\"type\":\"segment\",\"t0\":{t}}}\n")).collect();
    std::fs::write(&file, body).unwrap();
    let store = open(tmp.path());
    assert_eq!(t0s(&store.find_records("ambient", &RecordQuery::default()).unwrap()), [10, 11, 12, 13, 14, 9]);
    drop(store);

    std::fs::remove_file(tmp.path().join("ambient/2026-10-15.jsonl")).unwrap();
    let store = open(tmp.path());
    assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap(), 5);
}

#[test]
fn a_record_index_serves_a_filter_in_newest_first_order() {
    let tmp = TempDir::new().unwrap();
    let store = open(tmp.path());
    for t in 0..50 {
        let conv = if t % 5 == 0 { "c1" } else { "c2" };
        let mut r = seg(t, "x", "work");
        r["conv"] = json!(conv);
        store.append_record_to("ambient", &day("2026-10-14"), r).unwrap();
    }
    store.ensure_record_index("ambient", &["conv"]).unwrap();
    let q = RecordQuery { filters: vec![("conv".into(), json!("c1"))], newest_first: true, limit: 3, ..Default::default() };
    assert_eq!(t0s(&store.find_records("ambient", &q).unwrap()), [45, 40, 35]);
    let plan = store.explain_records("ambient", &q).unwrap().join(" | ");
    assert!(plan.contains("idx_records_ambient_conv") && !plan.contains("TEMP B-TREE"), "{plan}");
}
