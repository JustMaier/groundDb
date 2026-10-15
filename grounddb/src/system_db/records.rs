//! The index of JSONL record collections: one `records` row per line, and one
//! `record_files` row per file saying how far it has been indexed.
//!
//! Kept apart from `documents` on purpose: that table, and every path through
//! it, assumes one row per file. See `docs/jsonl-collections.md`.

use super::find::{field_expr, ident, sql_value};
use super::SystemDb;
use crate::error::{GroundDbError, Result};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, OptionalExtension};

/// Width the offset is zero-padded to in a record id. 12 digits is a
/// terabyte, far past any one partition file.
pub const OFFSET_DIGITS: usize = 12;

/// The id of the record whose line starts at `offset` in `path`:
/// `{path}@{offset:012}`. Text order of ids is (path, offset) order.
pub fn record_id(path: &str, offset: u64) -> String {
    format!("{path}@{offset:0width$}", width = OFFSET_DIGITS)
}

/// Split a record id back into its path and offset.
pub fn parse_record_id(id: &str) -> Option<(&str, u64)> {
    let (path, off) = id.rsplit_once('@')?;
    if off.len() != OFFSET_DIGITS {
        return None;
    }
    Some((path, off.parse().ok()?))
}

/// How far one file has been indexed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFile {
    pub path: String,
    /// Just past the last complete line indexed.
    pub indexed_to: u64,
    /// Complete lines indexed (malformed ones included).
    pub lines: u64,
    /// [`crate::jsonl::head_hash`] of the file when indexed, as text; empty
    /// if it had no complete line yet.
    pub head_hash: String,
}

/// A range on one field: `gte <= field < lt`, either bound optional.
#[derive(Debug, Clone, Default)]
pub struct RecordRange {
    pub field: String,
    pub gte: Option<serde_json::Value>,
    pub lt: Option<serde_json::Value>,
}

/// One query over a record collection.
#[derive(Debug, Clone, Default)]
pub struct RecordQuery {
    /// `field = value`, all of them. A JSON `null` matches a missing or null
    /// field. Any field of the record, declared or not.
    pub filters: Vec<(String, serde_json::Value)>,
    /// Ranges, all of them.
    pub ranges: Vec<RecordRange>,
    /// Only the records of this one partition file (root-relative path).
    pub path: Option<String>,
    /// Newest first: descending id, i.e. file order reversed, partitions by
    /// descending path.
    pub newest_first: bool,
    /// Resume after this record id, in the query's order.
    pub after: Option<String>,
    /// Most rows to return. 0 means no limit.
    pub limit: usize,
}

/// One indexed record.
#[derive(Debug, Clone, PartialEq)]
pub struct FoundRecord {
    pub id: String,
    pub path: String,
    pub offset: u64,
    /// 0-based line number. `None` when read straight from the file
    /// backwards, which cannot know it without reading everything before.
    pub line_no: Option<u64>,
    pub data: serde_json::Value,
}

impl SystemDb {
    pub(super) fn initialize_record_tables(&self) -> Result<()> {
        self.conn().execute_batch(
            "
            CREATE TABLE IF NOT EXISTS records (
                collection TEXT NOT NULL,
                id TEXT NOT NULL,
                path TEXT NOT NULL,
                offset INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                PRIMARY KEY (collection, id)
            );
            CREATE INDEX IF NOT EXISTS idx_records_path ON records(collection, path, offset);

            CREATE TABLE IF NOT EXISTS record_files (
                collection TEXT NOT NULL,
                path TEXT NOT NULL,
                indexed_to INTEGER NOT NULL,
                lines INTEGER NOT NULL,
                head_hash TEXT NOT NULL,
                PRIMARY KEY (collection, path)
            );
            ",
        )?;
        Ok(())
    }

    /// Insert or replace one record row. `data_json` is the line itself when it
    /// parsed as a JSON object.
    pub fn upsert_record(
        &self,
        collection: &str,
        path: &str,
        offset: u64,
        line_no: u64,
        data_json: &str,
    ) -> Result<String> {
        let id = record_id(path, offset);
        // Cached: a cold index runs this once per line.
        self.conn()
            .prepare_cached(
                "INSERT OR REPLACE INTO records (collection, id, path, offset, line_no, data_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?
            .execute(params![collection, id, path, offset as i64, line_no as i64, data_json])?;
        Ok(id)
    }

    /// Drop every record of one file, and its `record_files` row.
    pub fn delete_file_records(&self, collection: &str, path: &str) -> Result<usize> {
        let conn = self.conn();
        let n = conn.execute("DELETE FROM records WHERE collection = ?1 AND path = ?2", params![collection, path])?;
        conn.execute("DELETE FROM record_files WHERE collection = ?1 AND path = ?2", params![collection, path])?;
        Ok(n)
    }

    pub fn get_record_file(&self, collection: &str, path: &str) -> Result<Option<RecordFile>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT path, indexed_to, lines, head_hash FROM record_files WHERE collection = ?1 AND path = ?2",
                params![collection, path],
                |r| {
                    Ok(RecordFile {
                        path: r.get(0)?,
                        indexed_to: r.get::<_, i64>(1)? as u64,
                        lines: r.get::<_, i64>(2)? as u64,
                        head_hash: r.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn set_record_file(&self, collection: &str, f: &RecordFile) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO record_files (collection, path, indexed_to, lines, head_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![collection, f.path, f.indexed_to as i64, f.lines as i64, f.head_hash],
        )?;
        Ok(())
    }

    /// Every file of a collection that has a `record_files` row.
    pub fn list_record_files(&self, collection: &str) -> Result<Vec<RecordFile>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT path, indexed_to, lines, head_hash FROM record_files WHERE collection = ?1 ORDER BY path",
        )?;
        let rows = stmt.query_map(params![collection], |r| {
            Ok(RecordFile {
                path: r.get(0)?,
                indexed_to: r.get::<_, i64>(1)? as u64,
                lines: r.get::<_, i64>(2)? as u64,
                head_hash: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// One record by id.
    pub fn get_record(&self, collection: &str, id: &str) -> Result<Option<FoundRecord>> {
        let conn = self.conn();
        let row = conn
            .query_row(
                "SELECT id, path, offset, line_no, data_json FROM records WHERE collection = ?1 AND id = ?2",
                params![collection, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(id, path, offset, line_no, data)| {
            Ok(FoundRecord { id, path, offset: offset as u64, line_no: Some(line_no as u64), data: serde_json::from_str(&data)? })
        })
        .transpose()
    }

    /// Records matching `q`, in `q`'s order.
    pub fn find_records(&self, collection: &str, q: &RecordQuery) -> Result<Vec<FoundRecord>> {
        let (sql, params) = build_records(collection, q, false)?;
        let conn = self.conn();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, path, offset, line_no, data) = row?;
            out.push(FoundRecord {
                id,
                path,
                offset: offset as u64,
                line_no: Some(line_no as u64),
                data: serde_json::from_str(&data)?,
            });
        }
        Ok(out)
    }

    /// How many records match `q`'s filters, ranges and path.
    pub fn count_records(&self, collection: &str, q: &RecordQuery) -> Result<u64> {
        let (sql, params) = build_records(collection, q, true)?;
        let n: i64 = self.conn().query_row(&sql, rusqlite::params_from_iter(params.iter()), |r| r.get(0))?;
        Ok(n.max(0) as u64)
    }

    /// The plan SQLite picks for `q` (`EXPLAIN QUERY PLAN`), for tests.
    pub fn explain_records(&self, collection: &str, q: &RecordQuery) -> Result<Vec<String>> {
        let (sql, params) = build_records(collection, q, false)?;
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| r.get::<_, String>(3))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// An index on `(collection, <field exprs>..., id)` over `records`, so a
    /// filter on those fields is served in id order (newest first included)
    /// without a sort. Named `idx_records_{collection}_{f1}_{f2}...`.
    pub fn ensure_record_index(&self, collection: &str, fields: &[&str]) -> Result<()> {
        if !ident(collection) || fields.is_empty() || !fields.iter().all(|f| ident(f)) {
            return Err(GroundDbError::Schema(format!(
                "cannot index '{collection}.{}': names must be [A-Za-z0-9_]",
                fields.join(",")
            )));
        }
        let exprs: Vec<String> = fields.iter().map(|f| field_expr(f)).collect();
        self.conn().execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS idx_records_{collection}_{} ON records(collection, {}, id);",
            fields.join("_"),
            exprs.join(", ")
        ))?;
        Ok(())
    }
}

fn build_records(collection: &str, q: &RecordQuery, count: bool) -> Result<(String, Vec<SqlValue>)> {
    let mut params: Vec<SqlValue> = vec![SqlValue::Text(collection.to_string())];
    let mut wheres = vec!["collection = ?1".to_string()];
    fn bind(params: &mut Vec<SqlValue>, v: SqlValue) -> String {
        params.push(v);
        format!("?{}", params.len())
    }
    let checked = |field: &str| -> Result<String> {
        if !ident(field) {
            return Err(GroundDbError::Schema(format!("cannot filter on '{field}': names must be [A-Za-z0-9_]")));
        }
        Ok(field_expr(field))
    };
    for (field, v) in &q.filters {
        let e = checked(field)?;
        match sql_value(field, v)? {
            None => wheres.push(format!("{e} IS NULL")),
            Some(v) => {
                let p = bind(&mut params, v);
                wheres.push(format!("{e} = {p}"));
            }
        }
    }
    for r in &q.ranges {
        let e = checked(&r.field)?;
        for (bound, op) in [(&r.gte, ">="), (&r.lt, "<")] {
            if let Some(v) = bound {
                let v = sql_value(&r.field, v)?.ok_or_else(|| {
                    GroundDbError::Other(format!("a range bound on '{}' cannot be null", r.field))
                })?;
                let p = bind(&mut params, v);
                wheres.push(format!("{e} {op} {p}"));
            }
        }
    }
    if let Some(path) = &q.path {
        let p = bind(&mut params, SqlValue::Text(path.clone()));
        wheres.push(format!("path = {p}"));
    }
    if count {
        return Ok((format!("SELECT COUNT(*) FROM records WHERE {}", wheres.join(" AND ")), params));
    }
    if let Some(after) = &q.after {
        let p = bind(&mut params, SqlValue::Text(after.clone()));
        wheres.push(format!("id {} {p}", if q.newest_first { "<" } else { ">" }));
    }
    let order = if q.newest_first { "id DESC" } else { "id ASC" };
    let mut sql = format!(
        "SELECT id, path, offset, line_no, data_json FROM records WHERE {} ORDER BY {order}",
        wheres.join(" AND ")
    );
    if q.limit > 0 {
        let p = bind(&mut params, SqlValue::Integer(q.limit as i64));
        sql.push_str(&format!(" LIMIT {p}"));
    }
    Ok((sql, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ids_sort_by_path_then_offset_and_round_trip() {
        let a = record_id("d/2026-10-14.jsonl", 999);
        let b = record_id("d/2026-10-14.jsonl", 1000);
        let c = record_id("d/2026-10-15.jsonl", 0);
        assert!(a < b && b < c, "{a} {b} {c}");
        assert_eq!(parse_record_id(&b), Some(("d/2026-10-14.jsonl", 1000)));
        assert_eq!(parse_record_id("x@12"), None);
    }

    fn db() -> SystemDb {
        let db = SystemDb::open_in_memory().unwrap();
        let rows = [
            ("d/1.jsonl", 0, json!({"conv": "a", "t0": 10, "sphere": "work"})),
            ("d/1.jsonl", 30, json!({"conv": "b", "t0": 20, "sphere": "home"})),
            ("d/2.jsonl", 0, json!({"conv": "a", "t0": 30, "sphere": "work"})),
            ("d/2.jsonl", 40, json!({"conv": "a", "t0": 40, "sphere": "work"})),
        ];
        for (i, (p, o, d)) in rows.iter().enumerate() {
            db.upsert_record("c", p, *o, i as u64, &d.to_string()).unwrap();
        }
        db
    }

    fn t0s(rows: &[FoundRecord]) -> Vec<i64> {
        rows.iter().map(|r| r.data["t0"].as_i64().unwrap()).collect()
    }

    #[test]
    fn filters_ranges_order_and_cursor() {
        let db = db();
        let all = |newest_first| RecordQuery { newest_first, ..Default::default() };
        assert_eq!(t0s(&db.find_records("c", &all(false)).unwrap()), [10, 20, 30, 40]);
        assert_eq!(t0s(&db.find_records("c", &all(true)).unwrap()), [40, 30, 20, 10]);

        let work = RecordQuery { filters: vec![("sphere".into(), json!("work"))], newest_first: true, limit: 2, ..Default::default() };
        let page = db.find_records("c", &work).unwrap();
        assert_eq!(t0s(&page), [40, 30]);
        let next = RecordQuery { after: Some(page[1].id.clone()), ..work.clone() };
        assert_eq!(t0s(&db.find_records("c", &next).unwrap()), [10]);
        assert_eq!(db.count_records("c", &work).unwrap(), 3);

        let window = RecordQuery {
            ranges: vec![RecordRange { field: "t0".into(), gte: Some(json!(20)), lt: Some(json!(40)) }],
            ..Default::default()
        };
        assert_eq!(t0s(&db.find_records("c", &window).unwrap()), [20, 30]);
        let one_file = RecordQuery { path: Some("d/1.jsonl".into()), ..Default::default() };
        assert_eq!(db.count_records("c", &one_file).unwrap(), 2);
        assert!(db.find_records("c", &RecordQuery { filters: vec![("bad name".into(), json!(1))], ..Default::default() }).is_err());
    }

    #[test]
    fn a_field_index_serves_a_newest_first_filter_without_a_sort() {
        let db = db();
        let q = RecordQuery { filters: vec![("conv".into(), json!("a"))], newest_first: true, limit: 10, ..Default::default() };
        let before = db.explain_records("c", &q).unwrap().join(" | ");
        db.ensure_record_index("c", &["conv"]).unwrap();
        let after = db.explain_records("c", &q).unwrap().join(" | ");
        assert!(after.contains("idx_records_c_conv"), "{after}");
        assert!(!after.contains("TEMP B-TREE"), "the index already yields id order: {after}");
        assert_ne!(before, after);
    }

    #[test]
    fn deleting_a_file_drops_its_rows_and_its_progress() {
        let db = db();
        db.set_record_file("c", &RecordFile { path: "d/1.jsonl".into(), indexed_to: 60, lines: 2, head_hash: "1".into() }).unwrap();
        assert_eq!(db.delete_file_records("c", "d/1.jsonl").unwrap(), 2);
        assert_eq!(db.get_record_file("c", "d/1.jsonl").unwrap(), None);
        assert_eq!(db.count_records("c", &RecordQuery::default()).unwrap(), 2);
    }
}
