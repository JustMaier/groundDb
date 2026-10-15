//! File-level primitives for append-only JSONL collections: append one record
//! crash-safely, scan the complete lines after an offset, and read lines
//! backwards from the end of a file.
//!
//! Nothing here touches the system database. The store builds the index on
//! top of these (see `docs/jsonl-collections.md`).
//!
//! ## The torn last line
//!
//! A line counts only once its `\n` is on disk. A file that ends without one
//! has a torn tail: a writer mid-append, or a crash. Readers stop before it,
//! and [`append_line`] terminates it with a `\n` of its own before writing,
//! so the torn bytes become one malformed line rather than a prefix glued
//! onto the next record. Nothing here ever truncates a file.

use crate::error::{GroundDbError, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The block size [`ReverseLines`] reads with.
pub const REVERSE_BLOCK: usize = 64 * 1024;

/// FNV-1a, 64-bit. Persisted (as a file's head hash), so it must not depend
/// on the Rust version the way `DefaultHasher` may.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Serialize a record as exactly one line, terminating `\n` included.
/// `serde_json` escapes control characters inside strings, so the only
/// newline in the output is the last byte.
pub fn encode_line(record: &serde_json::Value) -> Result<Vec<u8>> {
    if !record.is_object() {
        return Err(GroundDbError::Validation("a JSONL record must be a JSON object".into()));
    }
    let mut line = serde_json::to_vec(record)?;
    debug_assert!(!line.contains(&b'\n'));
    line.push(b'\n');
    Ok(line)
}

/// Append one record to `path` (created, with its directories, if missing):
/// one `write_all` of the whole line, then `sync_data`. Returns the byte
/// offset the record's line starts at.
///
/// If the file does not end in `\n` its tail is torn (see the module docs),
/// and a `\n` is written in front of the record first.
///
/// Callers must serialise appends to one file: the offset is computed from
/// the length before the write. Appends from more than one process are not
/// supported.
pub fn append_line(path: &Path, record: &serde_json::Value) -> Result<u64> {
    let line = encode_line(record)?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = OpenOptions::new().create(true).read(true).append(true).open(path)?;
    let len = file.metadata()?.len();
    let torn = len > 0 && {
        let mut last = [0u8; 1];
        file.seek(SeekFrom::Start(len - 1))?;
        file.read_exact(&mut last)?;
        last[0] != b'\n'
    };
    let (buf, start) = if torn {
        log::warn!(
            "{}: last line is torn (no trailing newline at byte {len}); terminating it before appending",
            path.display()
        );
        let mut b = Vec::with_capacity(line.len() + 1);
        b.push(b'\n');
        b.extend_from_slice(&line);
        (b, len + 1)
    } else {
        (line, len)
    };
    file.write_all(&buf)?;
    file.sync_data()?;
    Ok(start)
}

/// One complete line found by [`scan_from`].
#[derive(Debug)]
pub struct ScannedLine<'a> {
    /// Byte offset of the line's first byte.
    pub offset: u64,
    /// 0-based line number in the file.
    pub line_no: u64,
    /// The line, without its `\n` (and without a trailing `\r`).
    pub bytes: &'a [u8],
}

/// Where a scan stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanEnd {
    /// Just past the last complete line: where the next scan resumes.
    pub indexed_to: u64,
    /// Complete lines from the start of the file up to `indexed_to`.
    pub lines: u64,
}

/// Visit every complete line of `path` from byte `from` (which must be a line
/// start, `line_no` being its line number), in order. A trailing partial line
/// is not visited and `indexed_to` stops before it.
pub fn scan_from(
    path: &Path,
    from: u64,
    line_no: u64,
    mut visit: impl FnMut(ScannedLine<'_>) -> Result<()>,
) -> Result<ScanEnd> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(from))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut buf = Vec::new();
    let mut offset = from;
    let mut n = line_no;
    loop {
        buf.clear();
        let read = reader.read_until(b'\n', &mut buf)?;
        if read == 0 || buf.last() != Some(&b'\n') {
            // EOF, or a torn tail: hold it back.
            return Ok(ScanEnd { indexed_to: offset, lines: n });
        }
        let mut body = &buf[..buf.len() - 1];
        if body.last() == Some(&b'\r') {
            body = &body[..body.len() - 1];
        }
        visit(ScannedLine { offset, line_no: n, bytes: body })?;
        offset += read as u64;
        n += 1;
    }
}

/// The hash of the first complete line within the first `limit` bytes, or
/// `None` if there is no complete line there. Recorded with an index so a
/// file replaced by a different one of equal or greater length is noticed.
pub fn head_hash(path: &Path, limit: u64) -> Result<Option<u64>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file.take(limit));
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;
    if buf.last() != Some(&b'\n') {
        return Ok(None);
    }
    Ok(Some(fnv1a(&buf)))
}

/// Complete lines of a file, last first, each with the offset it starts at.
/// Reads backwards from the end (or from `before`) in [`REVERSE_BLOCK`]
/// blocks, so the newest lines of a large file cost one block, not the file.
///
/// Only complete lines are yielded: a torn tail is skipped. Blank lines are
/// skipped. Lines are returned without `\n` or a trailing `\r`.
pub struct ReverseLines {
    file: File,
    block: usize,
    /// File offset of `buf[0]`.
    start: u64,
    /// Bytes `[start, start + buf.len())`, not yet yielded.
    buf: Vec<u8>,
    /// Still to drop the bytes after the last `\n` (a torn tail).
    trim: bool,
}

impl ReverseLines {
    /// Lines starting before byte `before` (the whole file when `None`).
    /// `before` should be a line start, e.g. the offset of the last line a
    /// caller already has.
    pub fn open(path: &Path, before: Option<u64>) -> Result<Self> {
        Self::with_block(path, before, REVERSE_BLOCK)
    }

    pub fn with_block(path: &Path, before: Option<u64>, block: usize) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let end = before.map_or(len, |b| b.min(len));
        Ok(Self { file, block: block.max(1), start: end, buf: Vec::new(), trim: true })
    }

    /// Prepend the previous block. False at the start of the file.
    fn load(&mut self) -> std::io::Result<bool> {
        if self.start == 0 {
            return Ok(false);
        }
        let n = (self.block as u64).min(self.start) as usize;
        self.start -= n as u64;
        let mut chunk = vec![0u8; n + self.buf.len()];
        self.file.seek(SeekFrom::Start(self.start))?;
        self.file.read_exact(&mut chunk[..n])?;
        chunk[n..].copy_from_slice(&self.buf);
        self.buf = chunk;
        Ok(true)
    }

    fn next_line(&mut self) -> std::io::Result<Option<(u64, Vec<u8>)>> {
        while self.trim {
            if let Some(i) = self.buf.iter().rposition(|b| *b == b'\n') {
                self.buf.truncate(i + 1);
                self.trim = false;
            } else if !self.load()? {
                self.buf.clear();
                return Ok(None);
            }
        }
        loop {
            // Invariant: buf is empty or ends with the `\n` of its last line.
            if self.buf.is_empty() {
                if !self.load()? {
                    return Ok(None);
                }
                continue;
            }
            let body_end = self.buf.len() - 1;
            let line_start = match self.buf[..body_end].iter().rposition(|b| *b == b'\n') {
                Some(i) => i + 1,
                None if self.start == 0 => 0,
                None => {
                    self.load()?;
                    continue;
                }
            };
            let mut line = self.buf[line_start..body_end].to_vec();
            let offset = self.start + line_start as u64;
            self.buf.truncate(line_start);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            return Ok(Some((offset, line)));
        }
    }
}

impl Iterator for ReverseLines {
    type Item = Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_line().map_err(GroundDbError::from).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
    }

    fn reverse(path: &Path, before: Option<u64>, block: usize) -> Vec<(u64, String)> {
        ReverseLines::with_block(path, before, block)
            .unwrap()
            .map(|r| r.map(|(o, l)| (o, String::from_utf8(l).unwrap())).unwrap())
            .collect()
    }

    fn forward(path: &Path) -> (Vec<(u64, u64, String)>, ScanEnd) {
        let mut out = Vec::new();
        let end = scan_from(path, 0, 0, |l| {
            out.push((l.offset, l.line_no, String::from_utf8(l.bytes.to_vec()).unwrap()));
            Ok(())
        })
        .unwrap();
        (out, end)
    }

    #[test]
    fn reverse_matches_forward_for_every_block_size() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        // Lines of varied length, one longer than the smaller block sizes,
        // a blank line, a CRLF line and a torn tail.
        let mut body = String::new();
        for i in 0..40 {
            body.push_str(&format!("{{\"i\":{i},\"pad\":\"{}\"}}\n", "x".repeat(i * 3)));
            if i == 7 {
                body.push('\n');
            }
            if i == 9 {
                body.push_str("{\"crlf\":true}\r\n");
            }
        }
        body.push_str("{\"torn\":");
        write(&p, body.as_bytes());

        let (fwd, end) = forward(&p);
        let mut expected: Vec<(u64, String)> =
            fwd.iter().filter(|(_, _, l)| !l.trim().is_empty()).map(|(o, _, l)| (*o, l.clone())).collect();
        expected.reverse();
        assert_eq!(end.indexed_to as usize, body.rfind('\n').unwrap() + 1);
        for block in [1, 2, 3, 7, 16, 100, 4096, REVERSE_BLOCK] {
            assert_eq!(reverse(&p, None, block), expected, "block {block}");
        }
        // Resuming before an offset yields exactly the older lines.
        let cut = expected[5].0;
        assert_eq!(reverse(&p, Some(cut), 5), expected[6..].to_vec());
    }

    #[test]
    fn a_torn_tail_is_held_back_by_both_readers() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        write(&p, b"{\"a\":1}\n{\"b\":");
        let (fwd, end) = forward(&p);
        assert_eq!(fwd.len(), 1);
        assert_eq!(end, ScanEnd { indexed_to: 8, lines: 1 });
        assert_eq!(reverse(&p, None, 3), vec![(0, "{\"a\":1}".to_string())]);
        // Entirely torn: nothing.
        write(&p, b"{\"b\":");
        assert!(reverse(&p, None, 2).is_empty());
        assert_eq!(forward(&p).1, ScanEnd { indexed_to: 0, lines: 0 });
    }

    #[test]
    fn append_terminates_a_torn_tail_and_returns_the_line_offset() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sub/day.jsonl");
        assert_eq!(append_line(&p, &json!({"n": 1})).unwrap(), 0);
        let second = append_line(&p, &json!({"n": 2, "text": "two\nlines"})).unwrap();
        assert_eq!(second, 8);
        // Tear it, the way a crash mid-write would.
        let mut f = OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(b"{\"n\":3,\"te").unwrap();
        drop(f);
        let len = std::fs::metadata(&p).unwrap().len();
        let fourth = append_line(&p, &json!({"n": 4})).unwrap();
        assert_eq!(fourth, len + 1, "a newline goes in front of the record");
        let (fwd, _) = forward(&p);
        let lines: Vec<&str> = fwd.iter().map(|(_, _, l)| l.as_str()).collect();
        assert_eq!(lines, [r#"{"n":1}"#, r#"{"n":2,"text":"two\nlines"}"#, r#"{"n":3,"te"#, r#"{"n":4}"#]);
        assert_eq!(fwd[3].0, fourth);
    }

    #[test]
    fn head_hash_needs_a_complete_first_line() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.jsonl");
        write(&p, b"{\"a\":1");
        assert_eq!(head_hash(&p, 4096).unwrap(), None);
        write(&p, b"{\"a\":1}\n{\"b\"");
        let h = head_hash(&p, 4096).unwrap().unwrap();
        write(&p, b"{\"a\":1}\n{\"b\":2}\n");
        assert_eq!(head_hash(&p, 4096).unwrap(), Some(h), "growing the file keeps the head");
        write(&p, b"{\"a\":9}\n");
        assert_ne!(head_hash(&p, 4096).unwrap(), Some(h));
    }

    #[test]
    fn non_objects_are_refused() {
        assert!(encode_line(&json!([1, 2])).is_err());
        assert!(encode_line(&json!("x")).is_err());
    }
}
