//! Reverse-read and index benchmark over ~1M records. Ignored by default:
//!
//! ```text
//! cargo test --release --test jsonl_bench -- --ignored --nocapture
//! ```
//!
//! `JSONL_BENCH_ROWS` overrides the row count. It asserts only what must hold
//! at any size (counts, order); the timings are printed for the report.

use grounddb::jsonl::ReverseLines;
use grounddb::{RecordQuery, Store};
use serde_json::json;
use std::io::{BufWriter, Write};
use std::time::Instant;
use tempfile::TempDir;

const SCHEMA: &str = r#"collections:
  ambient:
    path: "ambient/{date}.jsonl"
    additional_properties: true
"#;

fn ms(t: Instant) -> String {
    format!("{:.1} ms", t.elapsed().as_secs_f64() * 1000.0)
}

#[test]
#[ignore]
fn reverse_read_and_index_one_million_rows() {
    let rows: usize = std::env::var("JSONL_BENCH_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let days = 10;
    let per_day = rows / days;
    let tmp = TempDir::new().unwrap();
    std::fs::write(tmp.path().join("schema.yaml"), SCHEMA).unwrap();
    std::fs::create_dir_all(tmp.path().join("ambient")).unwrap();

    let t = Instant::now();
    let mut bytes = 0u64;
    for d in 0..days {
        let f = std::fs::File::create(tmp.path().join(format!("ambient/2026-10-{:02}.jsonl", d + 1))).unwrap();
        let mut w = BufWriter::new(f);
        for i in 0..per_day {
            let n = d * per_day + i;
            let line = json!({
                "type": "segment", "t0": 1_760_000_000_000i64 + n as i64 * 1000,
                "conv": format!("c{}", n / 40), "speaker": n % 3, "sphere": if n % 4 == 0 { "home" } else { "work" },
                "text": "the quick brown fox jumps over the lazy dog, roughly a sentence of speech",
            });
            let s = serde_json::to_string(&line).unwrap();
            bytes += s.len() as u64 + 1;
            writeln!(w, "{s}").unwrap();
        }
    }
    let total = per_day * days;
    eprintln!("wrote {total} rows, {:.1} MB over {days} files: {}", bytes as f64 / 1e6, ms(t));

    let day1 = tmp.path().join("ambient/2026-10-10.jsonl");
    let t = Instant::now();
    let tail: Vec<_> = ReverseLines::open(&day1, None).unwrap().take(50).map(|r| r.unwrap()).collect();
    eprintln!("ReverseLines: newest 50 of one {per_day}-row file: {}", ms(t));
    assert_eq!(tail.len(), 50);
    let t = Instant::now();
    let again = ReverseLines::open(&day1, None).unwrap().take(50).count();
    eprintln!("ReverseLines: newest 50 again (file already opened once): {}", ms(t));
    assert_eq!(again, 50);
    let t = Instant::now();
    let n = ReverseLines::open(&day1, None).unwrap().count();
    eprintln!("ReverseLines: whole {per_day}-row file backwards: {}", ms(t));
    assert_eq!(n, per_day);
    let t = Instant::now();
    let fwd = grounddb::jsonl::scan_from(&day1, 0, 0, |_| Ok(())).unwrap();
    eprintln!("scan_from: whole {per_day}-row file forwards: {}", ms(t));
    assert_eq!(fwd.lines as usize, per_day);

    let t = Instant::now();
    let store = Store::open(tmp.path().to_str().unwrap()).unwrap();
    eprintln!("Store::open, cold index of {total} rows: {}", ms(t));
    drop(store);
    let t = Instant::now();
    let store = Store::open(tmp.path().to_str().unwrap()).unwrap();
    eprintln!("Store::open again, nothing changed: {}", ms(t));

    let all = RecordQuery { newest_first: true, limit: 50, ..Default::default() };
    let t = Instant::now();
    let page = store.find_records("ambient", &all).unwrap();
    eprintln!("find_records newest 50 (no filter): {}", ms(t));
    assert_eq!(page[0].data["t0"], json!(1_760_000_000_000i64 + (total as i64 - 1) * 1000));
    // Halfway down: the first line of the middle day file.
    let mid = grounddb::system_db::record_id("ambient/2026-10-06.jsonl", 0);
    let t = Instant::now();
    let deep = store.find_records("ambient", &RecordQuery { after: Some(mid), ..all.clone() }).unwrap();
    eprintln!("find_records 50 after a cursor halfway down: {}", ms(t));
    assert_eq!(deep.len(), 50);

    let conv = RecordQuery { filters: vec![("conv".into(), json!("c100"))], newest_first: true, limit: 50, ..Default::default() };
    let t = Instant::now();
    let unindexed = store.find_records("ambient", &conv).unwrap();
    eprintln!("find_records conv=c100, no field index: {}", ms(t));
    let t = Instant::now();
    store.ensure_record_index("ambient", &["conv"]).unwrap();
    eprintln!("ensure_record_index(conv) over {total} rows: {}", ms(t));
    let t = Instant::now();
    let indexed = store.find_records("ambient", &conv).unwrap();
    eprintln!("find_records conv=c100, indexed: {}", ms(t));
    assert_eq!(indexed, unindexed);
    assert_eq!(indexed.len(), 40);

    let t = Instant::now();
    let n = 200;
    for i in 0..n {
        store
            .append_record_to("ambient", &json!({"date": "2026-10-10"}), json!({"type": "segment", "t0": i, "conv": "live"}))
            .unwrap();
    }
    eprintln!("append_record_to x{n} onto a {per_day}-row file (write + fsync + index): {} total", ms(t));
    assert_eq!(store.count_records("ambient", &RecordQuery::default()).unwrap() as usize, total + n);
}

