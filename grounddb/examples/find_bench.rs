//! Boot and query timings for a large collection on GroundDB's own path
//! (`Store::open`, `insert_dynamic`, `find_dynamic`), for the hub's data
//! collections. Run:
//!
//!   cargo run --release --example find_bench -- <dir> [rows]
//!
//! `<dir>` must not exist. Prints one line per measurement.

use grounddb::{FindQuery, Store};
use serde_json::json;
use std::time::Instant;

const SCHEMA: &str = r#"collections:
  rows:
    path: "rows/{id}.md"
    additional_properties: true
    fields:
      title: { type: string }
      done: { type: string }
      list: { type: string }
      assignee: { type: string }
      due: { type: string }
      rev: { type: string }
"#;

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn time_q(store: &Store, q: &FindQuery, label: &str) {
    let mut runs = Vec::new();
    let mut n = 0;
    for _ in 0..15 {
        let t = Instant::now();
        n = store.find_dynamic("rows", q).unwrap().len();
        runs.push(ms(t));
    }
    println!("{label}: {n} rows, median {:.2} ms", median(runs));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&args[1]);
    let rows: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let fresh = !dir.exists();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("schema.yaml"), SCHEMA).unwrap();

    // The rows are written as files, the way GroundDB writes them, so the
    // measurement is the boot of an existing 100k collection, not the load.
    let lists = ["house", "kids", "work", "other"];
    let t = Instant::now();
    std::fs::create_dir_all(dir.join("rows")).unwrap();
    for i in (0..rows).filter(|_| fresh) {
        let body = format!(
            "---
assignee: p{}
done: {}
due: 2026-{:02}-{:02}
id: r{i:07}
list: {}
rev: 1
title: task {i}
---
",
            i % 50, i % 3 == 0, 1 + i % 12, 1 + i % 28, lists[i % 4]
        );
        std::fs::write(dir.join("rows").join(format!("r{i:07}.md")), body).unwrap();
    }
    println!("write {rows} row files directly: {:.0} ms", ms(t));

    // A cold index: drop _system.db so open rebuilds it from the files.
    if fresh { std::fs::remove_file(dir.join("_system.db")).ok();
    for ext in ["-wal", "-shm"] {
        std::fs::remove_file(dir.join(format!("_system.db{ext}"))).ok();
    } }
    let t = Instant::now();
    let store = Store::open(dir.to_str().unwrap()).unwrap();
    println!("open with no _system.db (index rebuilt from {rows} files): {:.0} ms", ms(t));
    drop(store);
    let t = Instant::now();
    let store = Store::open(dir.to_str().unwrap()).unwrap();
    println!("open warm (index present, files unchanged): {:.0} ms", ms(t));

    let q = |filters: Vec<(&str, serde_json::Value)>, sort: Option<(&str, bool)>| FindQuery {
        filters: filters.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        sort: sort.map(|(f, d)| (f.to_string(), d)),
        after: None,
        limit: 50,
    };
    let house_open = q(vec![("list", json!("house")), ("done", json!(false))], Some(("due", false)));
    let nobody = q(vec![("list", json!("house")), ("assignee", json!("nobody"))], None);
    time_q(&store, &house_open, "no index: list=house, done=false, by due, 50");
    time_q(&store, &nobody, "no index: list=house, assignee=nobody (no match)");
    let t = Instant::now();
    for f in ["list", "done", "due", "assignee"] {
        store.ensure_fields_index("rows", &[f]).unwrap();
    }
    println!("build 4 single-field indexes: {:.0} ms", ms(t));
    time_q(&store, &house_open, "indexed, no stats: list=house, done=false, by due, 50");
    time_q(&store, &nobody, "indexed, no stats: list=house, assignee=nobody (no match)");
    println!("plan, no stats: {}", store.explain_find("rows", &house_open).unwrap().join(" | "));
    let t = Instant::now();
    store.analyze().unwrap();
    println!("ANALYZE: {:.0} ms", ms(t));
    time_q(&store, &house_open, "indexed + stats: list=house, done=false, by due, 50");
    time_q(&store, &nobody, "indexed + stats: list=house, assignee=nobody (no match)");
    println!("plan, stats: {}", store.explain_find("rows", &house_open).unwrap().join(" | "));
    println!("plan, stats, no match: {}", store.explain_find("rows", &nobody).unwrap().join(" | "));
    let t = Instant::now();
    let n = store.count_dynamic("rows", &q(vec![("done", json!(false))], None)).unwrap();
    println!("count done=false: {n} in {:.1} ms", ms(t));
    let mut runs = Vec::new();
    for i in 0..15 {
        let t = Instant::now();
        store.update_partial_dynamic("rows", &format!("r{:07}", (i * 997) % rows), json!({ "done": true, "rev": 2 })).unwrap();
        runs.push(ms(t));
    }
    println!("one row update (file + index), median of 15: {:.2} ms", median(runs));
    let mut runs = Vec::new();
    for i in 0..15 {
        let t = Instant::now();
        store.insert_dynamic("rows", json!({ "id": format!("new{i}x{}", std::process::id()), "title": "x", "done": false, "list": "house" }), None).unwrap();
        runs.push(ms(t));
    }
    println!("one row insert (file + index), median of 15: {:.2} ms", median(runs));
}
