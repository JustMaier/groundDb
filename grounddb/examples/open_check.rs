//! Open a GroundDB store (e.g. a collection restored from a backup) and say
//! whether its index agrees with its files:
//!
//!   cargo run --release --example open_check -- <dir> <collection>
//!
//! Prints the open time, the rows the index holds, the `.md` files on disk,
//! and any id that is in one and not the other.

use grounddb::{FindQuery, Store};
use std::collections::BTreeSet;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dir, coll) = (&args[1], &args[2]);
    let t = Instant::now();
    let store = Store::open(dir).expect("open");
    println!("open: {:.0} ms", t.elapsed().as_secs_f64() * 1000.0);
    let mut indexed = BTreeSet::new();
    let mut q = FindQuery { limit: 1000, ..Default::default() };
    loop {
        let page = store.find_dynamic(coll, &q).expect("find");
        let Some(last) = page.last() else { break };
        q.after = Some((serde_json::Value::Null, last["id"].as_str().unwrap().to_string()));
        indexed.extend(page.iter().map(|r| r["id"].as_str().unwrap().to_string()));
    }
    let files: BTreeSet<String> = std::fs::read_dir(std::path::Path::new(dir).join(coll))
        .map(|d| {
            d.filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".md")).map(String::from))
                .collect()
        })
        .unwrap_or_default();
    println!("index rows: {}, files: {}", indexed.len(), files.len());
    let only_index: Vec<_> = indexed.difference(&files).collect();
    let only_files: Vec<_> = files.difference(&indexed).collect();
    println!("in index only: {only_index:?}");
    println!("on disk only: {only_files:?}");
    println!("{}", if only_index.is_empty() && only_files.is_empty() { "AGREE" } else { "DISAGREE" });
}
