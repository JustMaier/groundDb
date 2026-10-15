# Append-only JSONL collections

Status: design for v1, 2026-10-15. Branch `feat/jsonl-append`.

A `.jsonl` collection holds many records per file, one JSON object per line.
GroundDB indexes every line, appends new lines crash-safely, and reads them
newest first. The first user is the Omi ambient-memory stream: high-volume,
append-only, one file per day.

## Schema

```yaml
collections:
  ambient:
    path: "ambient/{date}.jsonl"   # the partition template; one file per day
    additional_properties: true
    records:                       # optional; validates each appended record
      base:
        type: { type: string, enum: [segment, edit], required: true }
        t0:   { type: integer }
      by: type
      variants:
        segment: { fields: { text: { type: string } } }
        edit:    { fields: { ref: { type: string }, field: { type: string } } }
```

- Any collection whose path ends in `.jsonl` is a **record collection**: it is
  indexed line by line, never as one front-matter document. Today such a
  collection silently became one row per file with the raw text in
  `content_text`; nothing in the hub declares one, so no existing user changes.
- `readonly: true` (spec §2.5, e.g. Claude transcripts) gets the same indexing
  and no writes. Otherwise a record collection accepts `append_record`, and
  that is its only write: GroundDB never rewrites a line. (No new schema flag:
  the `.jsonl` extension is the switch.)
- `records` validates appended records: base fields plus the variant named by
  the `by` field, under the collection's `strict` / `additional_properties`.
  Records the store did not write (a file edited by hand) are indexed without
  validation, so reading never loses a line; only a line that is not a JSON
  object is skipped, with a warning.

## Storage: a separate `records` table

```sql
CREATE TABLE records (
  collection TEXT NOT NULL,
  id         TEXT NOT NULL,     -- '{path}@{offset:012}', see below
  path       TEXT NOT NULL,     -- root-relative partition file
  offset     INTEGER NOT NULL,  -- byte offset of the line's first byte
  line_no    INTEGER NOT NULL,  -- 0-based line number in the file
  data_json  TEXT NOT NULL,
  PRIMARY KEY (collection, id)
);
CREATE TABLE record_files (
  collection TEXT NOT NULL, path TEXT NOT NULL,
  indexed_to INTEGER NOT NULL,  -- byte offset just past the last complete line
  lines      INTEGER NOT NULL,  -- complete lines indexed
  head_hash  TEXT NOT NULL,     -- hash of the first line, detects a replaced file
  PRIMARY KEY (collection, path)
);
```

Why not one row per line in `documents`: that table and every path through it
assume one row per file (id-by-path, the removed-ids diff on boot, the
stale-remove guard, get/list re-reading the file, the schema-migration
backfill that rewrites files). Making each of those multi-row-aware touches
code the hub's markdown collections depend on. A separate table leaves all of
it untouched: record collections branch off at boot, the watcher and the
migration backfill, and nothing else sees them. Cost: views (`schema.yaml`
SQL) do not read record collections in v1.

**Record id** = `{path}@{offset}` with the offset zero-padded to 12 digits
(`ambient/2026-10-14.jsonl@000000001234`). In an append-only file the offset
of a line never changes, so the id is stable across reindexes, unique across
partitions, and needs no counter. The padding makes text order equal
(path, offset) order, so `ORDER BY id DESC` is newest first **provided
partition paths sort chronologically**, which `{date}` as YYYY-MM-DD does. A
template whose paths do not sort by time still works; it just is not
time-ordered across files without a sort field.

## Append (crash safety)

`Collection::append(record)` renders the partition path from the record's own
fields; `append_record_to(partition_fields, record)` takes them separately (Omi
passes `{date}`, Justin's local date, and keeps `t0` as epoch ms; GroundDB
never derives a partition from a timestamp). Steps, under the store's record
lock (one per store, so two appenders in one process serialise; **appends from
two processes at once are not supported**):

1. Validate against `records`.
2. Serialize to one line. `serde_json` escapes newlines inside strings, so a
   record is always exactly one line.
3. Open the file with append. If it is non-empty and its last byte is not
   `\n`, a previous write was torn: prefix the line with `\n`. The torn bytes
   become their own malformed line, which the indexer skips with a warning. We
   never truncate: a byte we did not write could be someone else's.
4. One `write_all` of the whole line, then `sync_data`.
5. Index the file **from `indexed_to`**, reading only the new bytes, and
   return the new record's id. The row comes from the file, never from the
   input value, so what is indexed is what is on disk, even if another writer
   appended in between.

**Echo suppression is free.** The watcher's Modified event later finds
`indexed_to == file length` and does nothing. No hash ledger is needed.

**The torn last line on read.** The indexer only indexes lines that end in
`\n`. A trailing partial line (a writer mid-append, or a crash) is held back:
`indexed_to` stops before it, and the next append or watcher event resumes
from there. A complete line that is not valid JSON is skipped and warned about,
and counted in `lines` so line numbers stay true.

## Indexing (boot and watcher)

Per record file, compare the file with its `record_files` row:

| On disk vs indexed | Action |
|---|---|
| same length, same head hash | nothing |
| longer, same head hash | index from `indexed_to` (incremental) |
| shorter, or head hash differs | **full reindex of that file**, `log::warn!` loudly (truncated or replaced: not append-only any more) |
| file gone | delete its records and its row |
| new file | index from 0 |

Boot runs this per file for record collections instead of the mtime diff; the
watcher runs it for one file per Created/Modified event, and deletes on
Removed. A whole day file is only re-read when it shrank or was replaced.

## Reads

- **Newest first from the index:**
  `find_records(collection, RecordQuery { filters, ranges, newest_first, before/after id, limit })`.
  Equality filters and `>=`/`<` ranges on fields (`json_extract`, same
  expression as `find_documents`), order by id, keyset cursor on id. Index the
  fields you filter on with `ensure_record_index(collection, &["conv"])`.
  `count_records` likewise. Rows come back as `{ id, ...fields }`.
- **Newest first from the file:** `jsonl::ReverseLines` reads a file backwards
  from EOF (or from a given offset) in 64 KiB blocks, yielding `(offset,
  line)`, the way the hub reads chat logs. `Collection::tail(path, before,
  limit)` uses it. It needs no index, so it also serves a reader that opened
  the file without a store.
- `get_record(collection, id)` returns one record from the index.
- `Collection::get/list/insert/update/delete` on a record collection return an
  error naming the record calls. `find_dynamic` reads `documents`, so it finds
  nothing in a record collection.

## Measured (1M records, release build, this desk, 2026-10-15)

`cargo test --release --test jsonl_bench -- --ignored --nocapture`; 10 day
files of 100k records, 165 MB.

| operation | time |
|---|---|
| `find_records` newest 50, no filter | 0.5 ms |
| `find_records` 50 after a cursor halfway down | 0.4 ms |
| `find_records` `conv = ?` newest 50, unindexed / indexed | 1,696 ms / 0.5 ms |
| `ensure_record_index(conv)` over 1M | 10.3 s, once |
| `ReverseLines` newest 50 of a 100k-line file, first open / again | 21.8 ms / 0.2 ms |
| `ReverseLines` whole 100k-line file backwards (forwards: 11 ms) | 44 ms |
| `Store::open`, cold index of 1M | 22.3 s, once |
| `Store::open`, nothing changed | 9 ms |
| `append_record_to` onto a 100k-line file | ~26 ms each (two fsyncs: the file, then SQLite's commit) |

The first-open cost of `ReverseLines` is opening a file, not reading it. At
the Omi stream's rate (~10k records a day) a million records is about three
months, so the cold index is a fresh-clone cost, not a boot cost.

## Rotation and size

Rotation is the partition template: `{date}` gives one file per day, and only
today's file is ever appended to. v1 has no size cap and no reaping (the
ambient stream is kept, like speeches). A busy day at ~10k records x ~500 B is
~5 MB: an incremental index reads only the tail, and a full reindex of one day
is milliseconds. If a partition ever grows past what one file should hold,
add `{hour}` to the template; no code change.

## Edits

Edits are records too (`type: "edit"`, `ref`, `field`, `value`, `by`, `at`).
GroundDB stores and indexes them like any record and does not interpret them
in v1. `update`/`delete` on a record collection are refused with an error that
says to append an edit. A "latest edit per ref" fold can be a later view.

## Out of scope for v1

Views over record collections; codegen for record types; compaction; a `~`
home-relative path (spec §2.5) for collections outside the store root.

## Tests

- red/green: append + reopen + find, newest-first cursor walks every record
  once across two day files; the schema refuses update/delete.
- torn line: a file ending in a partial line indexes everything before it;
  appending after it prefixes `\n`, the torn bytes are skipped, the new record
  is indexed with the right id; a watcher-style incremental pass finishes a
  line completed later.
- truncation/replacement triggers a full reindex of that file only.
- benchmark (`tests/jsonl_bench.rs`, ignored): see Measured above.
- positive controls, each run once and reverted: removing the torn-line
  hold-back in `scan_from` fails 4 tests; resuming every pass from byte 0
  (no offset echo suppression) fails the watcher-echo test; dropping the
  torn-tail terminator in `append_line` fails the append test and the
  integration torn-line test.
