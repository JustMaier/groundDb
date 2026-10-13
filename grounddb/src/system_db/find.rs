//! Ad-hoc equality filters, one sort and a keyset cursor over the document
//! index, without a view in `schema.yaml`.
//!
//! Views are fixed SQL with string parameters. A caller that lets its user
//! pick the filter fields at run time (an app's `list({ where, sort })`) needs
//! the combination built per call, and needs typed parameters: `json_extract`
//! returns an INTEGER for a JSON number or boolean, and an INTEGER never
//! equals the TEXT `'5'` a string parameter would bind.
//!
//! Every field expression is written exactly as
//! [`SystemDb::ensure_fields_index`] writes it, `json_extract(data_json,
//! '$.<field>')`, so an index on a field serves a filter or a sort on it.
//! Rows come from `data_json` in the index, never from the files: a page of
//! 50 out of 100k rows reads 50 rows.

use super::SystemDb;
use crate::error::{GroundDbError, Result};
use rusqlite::types::Value as SqlValue;

/// One query over a collection. Built by the caller, checked here.
#[derive(Debug, Clone, Default)]
pub struct FindQuery {
    /// `field = value`, all of them. A JSON `null` matches a missing or null
    /// field. Arrays and objects are refused.
    pub filters: Vec<(String, serde_json::Value)>,
    /// One field to order by, and whether descending. The document id always
    /// breaks ties, so the order is total and a cursor is exact.
    pub sort: Option<(String, bool)>,
    /// Resume after this row: the sort field's value of the last row seen
    /// (ignored without a sort) and its id.
    pub after: Option<(serde_json::Value, String)>,
    /// Most rows to return.
    pub limit: usize,
}

/// One row: its id and its fields as stored in the index.
#[derive(Debug, Clone, PartialEq)]
pub struct FoundDocument {
    pub id: String,
    pub data: serde_json::Value,
}

fn ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The expression an index on `field` is built over. Must stay byte-identical
/// to `ensure_fields_index`, or SQLite will not use the index.
pub(crate) fn field_expr(field: &str) -> String {
    format!("json_extract(data_json, '$.{field}')")
}

fn sql_value(field: &str, v: &serde_json::Value) -> Result<Option<SqlValue>> {
    Ok(match v {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(SqlValue::Integer(*b as i64)),
        serde_json::Value::Number(n) => Some(match n.as_i64() {
            Some(i) => SqlValue::Integer(i),
            None => SqlValue::Real(n.as_f64().unwrap_or(0.0)),
        }),
        serde_json::Value::String(s) => Some(SqlValue::Text(s.clone())),
        _ => {
            return Err(GroundDbError::Other(format!(
                "cannot compare '{field}' with a list or an object"
            )))
        }
    })
}

impl SystemDb {
    /// Rows of `collection` matching `q`, in `q`'s order, at most `q.limit`.
    pub fn find_documents(&self, collection: &str, q: &FindQuery) -> Result<Vec<FoundDocument>> {
        let (sql, params) = build(collection, q, false)?;
        let conn = self.conn();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, data_json) = row?;
            out.push(FoundDocument { id, data: serde_json::from_str(&data_json)? });
        }
        Ok(out)
    }

    /// How many rows of `collection` match `q.filters` (sort, cursor and limit
    /// are ignored).
    pub fn count_documents(&self, collection: &str, q: &FindQuery) -> Result<u64> {
        let (sql, params) = build(collection, q, true)?;
        let conn = self.conn();
        let n: i64 = conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |r| r.get(0))?;
        Ok(n.max(0) as u64)
    }

    /// Refresh SQLite's statistics (`ANALYZE`). Without them the planner
    /// guesses between field indexes and can pick the one that matches the
    /// sort instead of the selective filter: measured on 100k rows, a
    /// `list = ? AND done = ? ORDER BY due` read every row in due order.
    /// Run it after building indexes or after a large load.
    pub fn analyze(&self) -> Result<()> {
        self.conn().execute_batch("ANALYZE")?;
        Ok(())
    }

    /// The plan SQLite picks for `q`, one line per step (`EXPLAIN QUERY PLAN`).
    /// For tests and diagnostics: it is how a caller proves a filter is served
    /// by an index rather than a scan.
    pub fn explain_find(&self, collection: &str, q: &FindQuery) -> Result<Vec<String>> {
        let (sql, params) = build(collection, q, false)?;
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| r.get::<_, String>(3))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

fn build(collection: &str, q: &FindQuery, count: bool) -> Result<(String, Vec<SqlValue>)> {
    let mut params: Vec<SqlValue> = vec![SqlValue::Text(collection.to_string())];
    let mut wheres = vec!["collection = ?1".to_string()];
    fn bind(params: &mut Vec<SqlValue>, v: SqlValue) -> String {
        params.push(v);
        format!("?{}", params.len())
    }
    for (field, v) in &q.filters {
        if !ident(field) {
            return Err(GroundDbError::Schema(format!("cannot filter on '{field}': names must be [A-Za-z0-9_]")));
        }
        let e = field_expr(field);
        match sql_value(field, v)? {
            None => wheres.push(format!("{e} IS NULL")),
            Some(v) => {
                let p = bind(&mut params, v);
                wheres.push(format!("{e} = {p}"));
            }
        }
    }
    if count {
        let sql = format!("SELECT COUNT(*) FROM documents WHERE {}", wheres.join(" AND "));
        return Ok((sql, params));
    }
    let order = match &q.sort {
        Some((field, desc)) => {
            if !ident(field) {
                return Err(GroundDbError::Schema(format!("cannot sort on '{field}': names must be [A-Za-z0-9_]")));
            }
            let e = field_expr(field);
            if let Some((after_v, after_id)) = &q.after {
                let id = bind(&mut params, SqlValue::Text(after_id.clone()));
                // SQLite sorts NULL first ascending and last descending. A
                // comparison with NULL is never true, so the null rows get
                // their own branch on each side of the cursor.
                let cond = match (sql_value(field, after_v)?, desc) {
                    (None, false) => format!("(({e} IS NULL AND id > {id}) OR {e} IS NOT NULL)"),
                    (None, true) => format!("({e} IS NULL AND id < {id})"),
                    (Some(v), false) => {
                        let p = bind(&mut params, v);
                        format!("({e} > {p} OR ({e} = {p} AND id > {id}))")
                    }
                    (Some(v), true) => {
                        let p = bind(&mut params, v);
                        format!("({e} < {p} OR ({e} = {p} AND id < {id}) OR {e} IS NULL)")
                    }
                };
                wheres.push(cond);
            }
            if *desc {
                format!("{e} DESC, id DESC")
            } else {
                format!("{e} ASC, id ASC")
            }
        }
        None => {
            if let Some((_, after_id)) = &q.after {
                let id = bind(&mut params, SqlValue::Text(after_id.clone()));
                wheres.push(format!("id > {id}"));
            }
            "id ASC".to_string()
        }
    };
    let limit = bind(&mut params, SqlValue::Integer(q.limit as i64));
    let sql = format!(
        "SELECT id, data_json FROM documents WHERE {} ORDER BY {order} LIMIT {limit}",
        wheres.join(" AND ")
    );
    Ok((sql, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn db_with(rows: &[(&str, serde_json::Value)]) -> SystemDb {
        let db = SystemDb::open_in_memory().unwrap();
        for (id, data) in rows {
            db.conn()
                .execute(
                    "INSERT INTO documents (id, collection, path, data_json) VALUES (?1, 't', ?2, ?3)",
                    rusqlite::params![id, format!("t/{id}.md"), data.to_string()],
                )
                .unwrap();
        }
        db
    }

    fn ids(v: &[FoundDocument]) -> Vec<&str> {
        v.iter().map(|d| d.id.as_str()).collect()
    }

    fn rows() -> SystemDb {
        db_with(&[
            ("a", json!({ "list": "house", "done": false, "due": "2026-10-03", "n": 3 })),
            ("b", json!({ "list": "house", "done": true, "due": "2026-10-01", "n": 1 })),
            ("c", json!({ "list": "work", "done": false, "due": null, "n": 2 })),
            ("d", json!({ "list": "house", "done": false, "n": 5 })),
            ("e", json!({ "list": "house", "done": false, "due": "2026-10-02", "n": 4 })),
        ])
    }

    #[test]
    fn typed_filters_match_numbers_and_booleans() {
        let db = rows();
        let q = FindQuery { filters: vec![("done".into(), json!(false)), ("list".into(), json!("house"))], limit: 50, ..Default::default() };
        assert_eq!(ids(&db.find_documents("t", &q).unwrap()), ["a", "d", "e"]);
        let q = FindQuery { filters: vec![("n".into(), json!(2))], limit: 50, ..Default::default() };
        assert_eq!(ids(&db.find_documents("t", &q).unwrap()), ["c"]);
        // A string "2" is not the number 2: the bind is typed.
        let q = FindQuery { filters: vec![("n".into(), json!("2"))], limit: 50, ..Default::default() };
        assert!(db.find_documents("t", &q).unwrap().is_empty());
        assert_eq!(db.count_documents("t", &FindQuery { filters: vec![("done".into(), json!(false))], ..Default::default() }).unwrap(), 4);
    }

    /// Walk every page of `q` with a page size of `n`, following the cursor.
    fn walk(db: &SystemDb, mut q: FindQuery, n: usize) -> Vec<String> {
        q.limit = n;
        let mut seen = Vec::new();
        loop {
            let page = db.find_documents("t", &q).unwrap();
            if page.is_empty() {
                return seen;
            }
            let last = page.last().unwrap();
            let field = q.sort.as_ref().map(|s| s.0.clone());
            let v = field.map(|f| last.data.get(&f).cloned().unwrap_or(serde_json::Value::Null)).unwrap_or_default();
            q.after = Some((v, last.id.clone()));
            seen.extend(page.iter().map(|d| d.id.clone()));
        }
    }

    #[test]
    fn the_cursor_visits_every_row_once_in_both_directions_with_nulls() {
        let db = rows();
        let all = |desc| FindQuery { sort: Some(("due".into(), desc)), limit: 50, ..Default::default() };
        let asc: Vec<String> = ids(&db.find_documents("t", &all(false)).unwrap()).iter().map(|s| s.to_string()).collect();
        let desc: Vec<String> = ids(&db.find_documents("t", &all(true)).unwrap()).iter().map(|s| s.to_string()).collect();
        // Nulls (c, and d with no field) first ascending, last descending.
        assert_eq!(asc, ["c", "d", "b", "e", "a"]);
        assert_eq!(desc, ["a", "e", "b", "d", "c"]);
        for n in 1..=5 {
            assert_eq!(walk(&db, all(false), n), asc, "asc, page size {n}");
            assert_eq!(walk(&db, all(true), n), desc, "desc, page size {n}");
        }
        let by_id = FindQuery::default();
        assert_eq!(walk(&db, by_id, 2), ["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn an_indexed_filter_uses_the_index_and_an_unindexed_one_scans() {
        let db = rows();
        let q = FindQuery { filters: vec![("list".into(), json!("house"))], sort: Some(("due".into(), false)), limit: 50, ..Default::default() };
        let before = db.explain_find("t", &q).unwrap().join(" | ");
        assert!(!before.contains("idx_field_t_list"), "no index yet, plan: {before}");
        db.ensure_fields_index("t", &["list"]).unwrap();
        let after = db.explain_find("t", &q).unwrap().join(" | ");
        assert!(after.contains("idx_field_t_list"), "plan should use the list index: {after}");
    }

    #[test]
    fn bad_names_and_compound_values_are_refused() {
        let db = rows();
        let q = FindQuery { filters: vec![("x') OR 1=1 --".into(), json!(1))], limit: 5, ..Default::default() };
        assert!(db.find_documents("t", &q).is_err());
        let q = FindQuery { filters: vec![("list".into(), json!(["house"]))], limit: 5, ..Default::default() };
        assert!(db.find_documents("t", &q).is_err());
        let q = FindQuery { sort: Some(("a b".into(), false)), limit: 5, ..Default::default() };
        assert!(db.find_documents("t", &q).is_err());
    }
}
