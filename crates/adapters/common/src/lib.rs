use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    time::SystemTime,
};

use chrono::{DateTime, TimeZone, Utc};
use core_model::{AgentKind, NativeRecord, NormalizedBatch, deterministic_id};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, instrument, trace, warn};

pub fn collect_files_with_ext(root: &Path, ext: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some(ext)
                && let Some(s) = path.to_str()
            {
                out.push(s.to_string());
            }
        }
    }
    out.sort();
    debug!(ext, count = out.len(), root = %root.display(), "collected files");
    out
}

pub fn file_mtime(path: &str) -> Option<DateTime<Utc>> {
    let modified: SystemTime = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified))
}

/// Tail-cursor state for one JSONL file. Persisted inside the per-agent
/// checkpoint as part of a `BTreeMap<path, FileTail>`; on the next sync the
/// adapter resumes parsing from `offset` (the byte position after the last
/// consumed line) when the size/mtime guard says the prefix is untouched.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileTail {
    /// Byte offset where the next sync should resume parsing (== the file
    /// size at the end of the last parse for a cleanly-terminated file).
    pub offset: u64,
    /// File mtime at the end of the last parse.
    pub mtime: DateTime<Utc>,
    /// blake3 hash of the last `TAIL_ANCHOR_WINDOW` bytes before `offset`;
    /// verified on the grew-path to detect in-place rewrites.
    pub anchor: String,
    /// Parse state (session identity, ordinal counters, …) required to resume
    /// mid-file with stable source_ids.
    pub ctx: TailContext,
}

/// Adapter-specific parse state carried in the tail checkpoint so that a
/// mid-file resume keeps session identity and message ordinals stable.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TailContext {
    pub session_id: Option<String>,
    pub session_ts: Option<String>,
    pub session_title: Option<String>,
    pub cwd: Option<String>,
    pub first_user_text: Option<String>,
    pub msg_index: u64,
}

/// Result of parsing one JSONL file: the records to emit plus the new tail
/// state for the checkpoint map (`None` drops the file from the map).
#[derive(Debug, Default)]
pub struct FileParseResult {
    pub records: Vec<NativeRecord>,
    pub tail: Option<FileTail>,
}

/// How a file should be (re)parsed this sync.
pub enum JsonlResume {
    /// File unchanged since the last sync (size+mtime match): skip entirely.
    Skip,
    /// File opened, positioned per `ResumeKind`.
    Open(OpenedJsonl, ResumeKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeKind {
    /// Parse only from the checkpoint offset (append-only path).
    FromOffset,
    /// Parse the whole file.
    Full,
}

pub struct OpenedJsonl {
    pub reader: BufReader<fs::File>,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

/// Bytes of the pre-offset suffix hashed into `FileTail::anchor`.
pub const TAIL_ANCHOR_WINDOW: u64 = 4096;

#[derive(Serialize, Deserialize)]
struct TailCursorEnvelope {
    v: u32,
    files: BTreeMap<String, FileTail>,
}

/// Parse a checkpoint cursor. Returns `Some(map)` for the per-file tail format
/// and `None` for legacy composite cursors (or anything unparseable).
pub fn parse_tail_cursor(cursor: Option<&str>) -> Option<BTreeMap<String, FileTail>> {
    let cursor = cursor?;
    let envelope: TailCursorEnvelope = serde_json::from_str(cursor).ok()?;
    if envelope.v != 1 {
        return None;
    }
    Some(envelope.files)
}

pub fn encode_tail_cursor(files: &BTreeMap<String, FileTail>) -> String {
    serde_json::to_string(&TailCursorEnvelope {
        v: 1,
        files: files.clone(),
    })
    .expect("tail cursor is serializable")
}

/// Open a JSONL file applying the tail-cursor guard. Returns `None` when the
/// file is unreadable (caller drops it from the checkpoint map).
pub fn open_jsonl(path: &str, checkpoint: Option<&FileTail>) -> Option<JsonlResume> {
    let meta = fs::metadata(path).ok()?;
    let size = meta.len();
    let mtime: DateTime<Utc> = meta.modified().ok()?.into();

    let Some(cp) = checkpoint else {
        return open_from(path, ResumeKind::Full, 0, size, mtime);
    };

    // Unchanged: size and mtime both match the recorded end state.
    if size == cp.offset && mtime == cp.mtime {
        return Some(JsonlResume::Skip);
    }

    // Grew: append-only if the pre-offset suffix still matches; otherwise the
    // file was rewritten (possibly growing) and needs a full re-parse.
    if size > cp.offset {
        let mut file = fs::File::open(path).ok()?;
        if compute_tail_anchor(&mut file, cp.offset) == cp.anchor {
            return open_from(path, ResumeKind::FromOffset, cp.offset, size, mtime);
        }
    }

    // Shrunk or in-place rewrite (same size, different mtime): full re-parse.
    open_from(path, ResumeKind::Full, 0, size, mtime)
}

fn open_from(
    path: &str,
    kind: ResumeKind,
    offset: u64,
    size: u64,
    mtime: DateTime<Utc>,
) -> Option<JsonlResume> {
    let mut file = fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(offset)).ok()?;
    Some(JsonlResume::Open(
        OpenedJsonl {
            reader: BufReader::new(file),
            size,
            mtime,
        },
        kind,
    ))
}

/// Hash the last `TAIL_ANCHOR_WINDOW` bytes ending at `offset` (the suffix a
/// resumed parse implicitly trusts). Leaves the file position at `offset`.
pub fn compute_tail_anchor(file: &mut fs::File, offset: u64) -> String {
    let start = offset.saturating_sub(TAIL_ANCHOR_WINDOW);
    let len = (offset - start) as usize;
    let mut buf = vec![0u8; len];
    if len > 0 {
        if file.seek(SeekFrom::Start(start)).is_err() {
            return String::new();
        }
        if file.read_exact(&mut buf).is_err() {
            return String::new();
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(&buf);
    hasher.finalize().to_hex().to_string()
}

/// Stream records from JSONL files into `tx` in parallel. Each file is parsed
/// independently (tail-resume or full), results are emitted deterministically
/// in `source_paths` order, and the aggregated per-file checkpoint map is
/// returned as the new cursor. `parse_file` materializes at most one file at
/// a time.
pub fn stream_jsonl_sources<F>(
    source_paths: &[String],
    cursor: Option<&str>,
    tx: &std::sync::mpsc::SyncSender<NativeRecord>,
    parse_file: F,
) -> anyhow::Result<Option<String>>
where
    F: Fn(&str, Option<&FileTail>, Option<&ParsedCursor>) -> FileParseResult + Sync,
{
    let tail_map = parse_tail_cursor(cursor);
    let legacy_cursor = if tail_map.is_some() {
        None
    } else {
        cursor.and_then(parse_cursor)
    };

    let results: Vec<(String, FileParseResult)> = source_paths
        .par_iter()
        .map(|path| {
            let cp = tail_map.as_ref().and_then(|m| m.get(path));
            let res = parse_file(path, cp, legacy_cursor.as_ref());
            (path.clone(), res)
        })
        .collect();

    let mut files: BTreeMap<String, FileTail> = BTreeMap::new();
    for (path, res) in results {
        if let Some(tail) = res.tail {
            files.insert(path, tail);
        }
        for rec in res.records {
            tx.send(rec)?;
        }
    }
    // With no source paths there is nothing to observe; keep the stored
    // checkpoint untouched rather than overwriting it with an empty map.
    if source_paths.is_empty() {
        return Ok(None);
    }
    Ok(Some(encode_tail_cursor(&files)))
}

#[instrument(skip(source_paths), fields(files = source_paths.len()))]
pub fn load_jsonl(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    let parsed_cursor = cursor.and_then(parse_cursor);
    let mut out: Vec<NativeRecord> = source_paths
        .par_iter()
        .flat_map(|path| {
            let file_mtime = file_mtime(path);
            if let Some(ref cur) = parsed_cursor
                && let Some(mtime) = file_mtime
                && mtime <= cur.ts
            {
                return Vec::new();
            }
            let stem = Path::new(path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(path)
                .to_string();
            let file = match fs::File::open(path) {
                Ok(file) => file,
                Err(_) => return Vec::new(),
            };
            let reader = std::io::BufReader::new(file);
            let mut records = Vec::new();
            let mut skipped_lines = 0usize;
            for line in reader.lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                let mut val: Value = match serde_json::from_str(&line) {
                    Ok(val) => val,
                    Err(_) => {
                        skipped_lines += 1;
                        continue;
                    }
                };
                let ts = extract_ts(&val).or(file_mtime).unwrap_or_else(Utc::now);
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("__source_path".to_string(), Value::String(path.clone()));
                    obj.insert("__session_seed".to_string(), Value::String(stem.clone()));
                }
                let source_id = val
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| deterministic_id(&[path, &line]));
                if let Some(ref cur) = parsed_cursor
                    && should_skip(ts, &source_id, cur)
                {
                    continue;
                }
                records.push(NativeRecord {
                    source_id,
                    updated_at: ts,
                    payload: val,
                });
            }
            if skipped_lines > 0 {
                warn!(path = %path, skipped_lines, "skipped malformed jsonl lines");
            }
            trace!(path = %path, records = records.len(), "parsed jsonl file");
            records
        })
        .collect();
    out.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.source_id.cmp(&b.source_id))
    });
    debug!(
        total = out.len(),
        cursor = cursor.is_some(),
        "load_jsonl complete"
    );
    Ok(out)
}

pub fn normalize_jsonl_records(kind: AgentKind, records: &[NativeRecord]) -> NormalizedBatch {
    let mut batch = NormalizedBatch::default();
    let mut sessions: HashMap<String, core_model::Session> = HashMap::new();
    for rec in records {
        if rec.payload.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        let Some(message) = rec.payload.get("message") else {
            continue;
        };
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = extract_content_text(message.get("content"));
        if content.is_empty() {
            continue;
        }
        let session_seed = rec
            .payload
            .get("sessionId")
            .and_then(|v| v.as_str())
            .or_else(|| rec.payload.get("session").and_then(|v| v.as_str()))
            .or_else(|| rec.payload.get("__session_seed").and_then(|v| v.as_str()))
            .or_else(|| rec.payload.get("id").and_then(|v| v.as_str()))
            .unwrap_or(&rec.source_id);
        let title = rec
            .payload
            .get("sessionTitle")
            .and_then(|v| v.as_str())
            .unwrap_or(session_seed);
        let session_id = deterministic_id(&[kind.as_str(), "session", session_seed]);
        let message_id = deterministic_id(&[kind.as_str(), "message", &rec.source_id]);
        let now = rec.updated_at;
        let session = sessions
            .entry(session_id.clone())
            .or_insert_with(|| core_model::Session {
                id: session_id.clone(),
                agent: kind,
                source_ref: session_seed.to_string(),
                title: title.to_string(),
                created_at: now,
                updated_at: now,
            });
        if now < session.created_at {
            session.created_at = now;
        }
        if now > session.updated_at {
            session.updated_at = now;
        }
        if (session.title.is_empty() || session.title == session.source_ref)
            && title != session_seed
        {
            session.title = title.to_string();
        }
        batch.messages.push(core_model::Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            role,
            content,
            ts: now,
        });
        batch.provenance.push(core_model::Provenance {
            id: deterministic_id(&["prov", &message_id]),
            entity_type: "message".to_string(),
            entity_id: message_id,
            agent: kind,
            source_path: rec
                .payload
                .get("__source_path")
                .and_then(Value::as_str)
                .unwrap_or(kind.as_str())
                .to_string(),
            source_id: rec.source_id.clone(),
        });
    }
    let mut ordered_sessions: Vec<_> = sessions.into_values().collect();
    ordered_sessions.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    batch.sessions.extend(ordered_sessions);
    batch
}

pub fn checkpoint_cursor_from_records(records: &[NativeRecord]) -> Option<String> {
    records
        .iter()
        .max_by(|a, b| {
            a.updated_at
                .cmp(&b.updated_at)
                .then_with(|| a.source_id.cmp(&b.source_id))
        })
        .map(|r| encode_cursor(r.updated_at, &r.source_id))
}

pub fn encode_cursor(ts: DateTime<Utc>, source_id: &str) -> String {
    format!("{}\x1f{}", ts.to_rfc3339(), source_id)
}

pub struct ParsedCursor {
    pub ts: DateTime<Utc>,
    pub source_id: String,
}

pub fn parse_cursor(cursor: &str) -> Option<ParsedCursor> {
    let (ts_str, id) = cursor.split_once('\x1f')?;
    let ts = DateTime::parse_from_rfc3339(ts_str)
        .ok()
        .map(|d| d.with_timezone(&Utc))?;
    Some(ParsedCursor {
        ts,
        source_id: id.to_string(),
    })
}

pub fn should_skip(ts: DateTime<Utc>, source_id: &str, cursor: &ParsedCursor) -> bool {
    ts < cursor.ts || (ts == cursor.ts && source_id <= cursor.source_id.as_str())
}

pub fn extract_ts(val: &Value) -> Option<DateTime<Utc>> {
    if let Some(s) = val.get("timestamp").and_then(Value::as_str) {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc));
    }
    if let Some(ms) = val
        .get("message")
        .and_then(|m| m.get("timestamp"))
        .and_then(Value::as_i64)
    {
        return Utc.timestamp_millis_opt(ms).single();
    }
    None
}

pub fn extract_content_text(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    let mut lines = Vec::new();
    extract_content_lines(content, &mut lines);
    lines.join("\n")
}

fn extract_content_lines(value: &Value, lines: &mut Vec<String>) {
    match value {
        Value::Null => {}
        Value::String(s) => push_non_empty_line(lines, s),
        Value::Array(arr) => {
            for item in arr {
                extract_content_lines(item, lines);
            }
        }
        Value::Object(obj) => {
            if let Some(kind) = obj.get("type").and_then(Value::as_str) {
                match kind {
                    "tool_use" => {
                        if let Some(text) = format_tool_use(value) {
                            lines.push(text);
                        }
                        return;
                    }
                    "tool_result" => {
                        if let Some(text) = format_tool_result(value) {
                            lines.push(text);
                        }
                        return;
                    }
                    _ => {}
                }
            }

            if let Some(text) = obj.get("text").and_then(Value::as_str) {
                push_non_empty_line(lines, text);
            }
            if let Some(thinking) = obj.get("thinking").and_then(Value::as_str) {
                push_non_empty_line(lines, thinking);
            }
            if let Some(content) = obj.get("content") {
                extract_content_lines(content, lines);
            }
        }
        Value::Bool(b) => lines.push(b.to_string()),
        Value::Number(n) => lines.push(n.to_string()),
    }
}

fn format_tool_use(value: &Value) -> Option<String> {
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let input = value.get("input").and_then(value_to_text);

    if name.is_none() && input.is_none() {
        return None;
    }

    let mut out = String::from("tool_use");
    if let Some(name) = name {
        out.push_str(": ");
        out.push_str(name);
    }
    if let Some(input) = input {
        if name.is_some() {
            out.push(' ');
        } else {
            out.push_str(": ");
        }
        out.push_str(&input);
    }
    Some(out)
}

fn format_tool_result(value: &Value) -> Option<String> {
    let payload = value
        .get("content")
        .or_else(|| value.get("result"))
        .or_else(|| value.get("output"))
        .or_else(|| value.get("run").and_then(|run| run.get("result")));
    let text = payload.and_then(value_to_text)?;
    Some(format!("tool_result: {text}"))
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        }
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(arr) => {
            let mut lines = Vec::new();
            for item in arr {
                if let Some(text) = value_to_text(item) {
                    for part in text.lines() {
                        push_non_empty_line(&mut lines, part);
                    }
                }
            }
            if lines.is_empty() {
                None
            } else {
                Some(lines.join("\n"))
            }
        }
        Value::Object(obj) => {
            if let Some(text) = obj.get("text").and_then(Value::as_str)
                && !text.trim().is_empty()
            {
                return Some(text.to_string());
            }
            if let Some(thinking) = obj.get("thinking").and_then(Value::as_str)
                && !thinking.trim().is_empty()
            {
                return Some(thinking.to_string());
            }
            if let Some(content) = obj.get("content")
                && let Some(text) = value_to_text(content)
            {
                return Some(text);
            }
            if let Some(result) = obj.get("result")
                && let Some(text) = value_to_text(result)
            {
                return Some(text);
            }
            if let Some(output) = obj.get("output")
                && let Some(text) = value_to_text(output)
            {
                return Some(text);
            }
            if let Some(run_result) = obj.get("run").and_then(|run| run.get("result"))
                && let Some(text) = value_to_text(run_result)
            {
                return Some(text);
            }
            serde_json::to_string(value)
                .ok()
                .filter(|s| s != "{}" && s != "[]" && s != "null")
        }
    }
}

fn push_non_empty_line(lines: &mut Vec<String>, text: &str) {
    if text.trim().is_empty() {
        return;
    }
    lines.push(text.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn extract_content_text_string() {
        let val = Value::String("hello".to_string());
        assert_eq!(extract_content_text(Some(&val)), "hello");
    }

    #[test]
    fn extract_content_text_array() {
        let val = serde_json::json!([
            {"text": "first"},
            {"text": "second"}
        ]);
        assert_eq!(extract_content_text(Some(&val)), "first\nsecond");
    }

    #[test]
    fn extract_content_text_thinking() {
        let val = serde_json::json!([
            {"thinking": "thought"},
            {"text": "answer"}
        ]);
        assert_eq!(extract_content_text(Some(&val)), "thought\nanswer");
    }

    #[test]
    fn extract_content_text_none() {
        assert_eq!(extract_content_text(None), "");
    }

    #[test]
    fn extract_content_text_empty_array() {
        let val = serde_json::json!([]);
        assert_eq!(extract_content_text(Some(&val)), "");
    }

    #[test]
    fn extract_content_text_tool_use_and_result() {
        let val = serde_json::json!([
            {"type": "tool_use", "name": "grep", "input": {"pattern": "foo"}},
            {"type": "tool_result", "content": [{"text": "matched line"}]}
        ]);
        assert_eq!(
            extract_content_text(Some(&val)),
            "tool_use: grep {\"pattern\":\"foo\"}\ntool_result: matched line"
        );
    }

    #[test]
    fn extract_content_text_skips_empty_tool_result() {
        let val = serde_json::json!([
            {"type": "tool_result", "content": "   "}
        ]);
        assert_eq!(extract_content_text(Some(&val)), "");
    }

    #[test]
    fn extract_ts_rfc3339() {
        let val = serde_json::json!({"timestamp": "2025-01-15T10:30:00+00:00"});
        let ts = extract_ts(&val).unwrap();
        assert_eq!(ts.to_rfc3339(), "2025-01-15T10:30:00+00:00");
    }

    #[test]
    fn extract_ts_millis() {
        let val = serde_json::json!({"message": {"timestamp": 1705312200000_i64}});
        let ts = extract_ts(&val);
        assert!(ts.is_some());
    }

    #[test]
    fn extract_ts_missing() {
        let val = serde_json::json!({"foo": "bar"});
        assert!(extract_ts(&val).is_none());
    }

    #[test]
    fn checkpoint_cursor_empty() {
        let records: Vec<NativeRecord> = vec![];
        assert!(checkpoint_cursor_from_records(&records).is_none());
    }

    #[test]
    fn checkpoint_cursor_max() {
        let t1 = Utc::now() - chrono::Duration::hours(2);
        let t2 = Utc::now();
        let records = vec![
            NativeRecord {
                source_id: "a".to_string(),
                updated_at: t1,
                payload: Value::Null,
            },
            NativeRecord {
                source_id: "b".to_string(),
                updated_at: t2,
                payload: Value::Null,
            },
        ];
        let cursor = checkpoint_cursor_from_records(&records).unwrap();
        assert_eq!(cursor, encode_cursor(t2, "b"));
    }

    #[test]
    fn checkpoint_cursor_tiebreak_by_source_id() {
        let t = Utc::now();
        let records = vec![
            NativeRecord {
                source_id: "z-record".to_string(),
                updated_at: t,
                payload: Value::Null,
            },
            NativeRecord {
                source_id: "a-record".to_string(),
                updated_at: t,
                payload: Value::Null,
            },
        ];
        let cursor = checkpoint_cursor_from_records(&records).unwrap();
        assert_eq!(cursor, encode_cursor(t, "z-record"));
    }

    #[test]
    fn parse_cursor_composite() {
        let t = Utc::now();
        let encoded = encode_cursor(t, "my-id");
        let parsed = parse_cursor(&encoded).unwrap();
        assert_eq!(parsed.ts, t);
        assert_eq!(parsed.source_id, "my-id");
    }

    #[test]
    fn parse_cursor_rejects_plain_timestamp() {
        assert!(parse_cursor("2025-01-15T10:30:00+00:00").is_none());
    }

    #[test]
    fn parse_cursor_invalid() {
        assert!(parse_cursor("not-a-timestamp").is_none());
    }

    #[test]
    fn should_skip_before_cursor() {
        let cursor = ParsedCursor {
            ts: Utc::now(),
            source_id: "mid".to_string(),
        };
        let earlier = cursor.ts - chrono::Duration::hours(1);
        assert!(should_skip(earlier, "anything", &cursor));
    }

    #[test]
    fn should_skip_same_ts_leq_id() {
        let cursor = ParsedCursor {
            ts: Utc::now(),
            source_id: "mid".to_string(),
        };
        assert!(should_skip(cursor.ts, "mid", &cursor));
        assert!(should_skip(cursor.ts, "aaa", &cursor));
    }

    #[test]
    fn should_not_skip_same_ts_greater_id() {
        let cursor = ParsedCursor {
            ts: Utc::now(),
            source_id: "mid".to_string(),
        };
        assert!(!should_skip(cursor.ts, "zzz", &cursor));
    }

    #[test]
    fn should_not_skip_after_cursor() {
        let cursor = ParsedCursor {
            ts: Utc::now() - chrono::Duration::hours(1),
            source_id: "zzz".to_string(),
        };
        assert!(!should_skip(Utc::now(), "aaa", &cursor));
    }

    #[test]
    fn collect_files_with_ext_finds_files() {
        let dir = tempdir();
        std::fs::write(dir.join("a.jsonl"), "{}").unwrap();
        std::fs::write(dir.join("b.txt"), "{}").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/c.jsonl"), "{}").unwrap();
        let files = collect_files_with_ext(&dir, "jsonl");
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.ends_with(".jsonl")));
    }

    #[test]
    fn collect_files_nonexistent_dir() {
        let files = collect_files_with_ext(Path::new("/nonexistent/path"), "jsonl");
        assert!(files.is_empty());
    }

    #[test]
    fn load_jsonl_basic() {
        let dir = tempdir();
        let file = dir.join("sess.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(f, r#"{{"id":"1","type":"message","message":{{"role":"user","content":[{{"text":"hello"}}]}},"timestamp":"2025-01-15T10:30:00+00:00"}}"#).unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = load_jsonl(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "1");
    }

    #[test]
    fn load_jsonl_cursor_filters() {
        let dir = tempdir();
        let file = dir.join("sess.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(f, r#"{{"id":"1","type":"message","message":{{"role":"user","content":[{{"text":"old"}}]}},"timestamp":"2025-01-10T00:00:00+00:00"}}"#).unwrap();
        writeln!(f, r#"{{"id":"2","type":"message","message":{{"role":"user","content":[{{"text":"new"}}]}},"timestamp":"2025-01-20T00:00:00+00:00"}}"#).unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let cursor = "2025-01-15T00:00:00+00:00\x1fsome-id".to_string();
        let records = load_jsonl(&paths, Some(&cursor)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "2");
    }

    #[test]
    fn load_jsonl_composite_cursor_skips_seen_record() {
        let dir = tempdir();
        let file = dir.join("sess.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(
            f,
            r#"{{"id":"aaa","timestamp":"2025-01-10T00:00:00+00:00"}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"id":"mmm","timestamp":"2025-01-10T00:00:00+00:00"}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"id":"zzz","timestamp":"2025-01-10T00:00:00+00:00"}}"#
        )
        .unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let cursor = "2025-01-10T00:00:00+00:00\x1fmmm".to_string();
        let records = load_jsonl(&paths, Some(&cursor)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "zzz");
    }

    #[test]
    fn load_jsonl_falls_back_to_file_mtime() {
        let dir = tempdir();
        let file = dir.join("sess.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(br#"{"id":"1","type":"message","message":{"content":[{"text":"hello"}]}}"#)
            .unwrap();
        f.write_all(b"\n").unwrap();
        let mtime = file_mtime(file.to_str().unwrap()).unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = load_jsonl(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].updated_at, mtime);
    }

    #[test]
    fn load_jsonl_skips_when_cursor_at_or_after_mtime() {
        let dir = tempdir();
        let file = dir.join("sess.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(br#"{"id":"1","type":"message","message":{"content":[{"text":"hello"}]}}"#)
            .unwrap();
        f.write_all(b"\n").unwrap();
        let mtime = file_mtime(file.to_str().unwrap()).unwrap();
        let cursor = encode_cursor(mtime, "zzz");
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = load_jsonl(&paths, Some(&cursor)).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn normalize_jsonl_records_basic() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "type": "message",
                "message": {"role": "assistant", "content": [{"text": "hi there"}]},
                "sessionId": "sess-abc",
                "__source_path": "/test/path"
            }),
        };
        let batch = normalize_jsonl_records(AgentKind::Pi, &[rec]);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.provenance.len(), 1);
        assert_eq!(batch.messages[0].role, "assistant");
        assert_eq!(batch.messages[0].content, "hi there");
    }

    #[test]
    fn normalize_skips_non_message() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "type": "tool_use",
                "message": {"role": "assistant", "content": [{"text": "hi"}]}
            }),
        };
        let batch = normalize_jsonl_records(AgentKind::Pi, &[rec]);
        assert!(batch.sessions.is_empty());
    }

    #[test]
    fn load_jsonl_skips_malformed_lines() {
        let dir = tempdir();
        let file = dir.join("bad.jsonl");
        let mut f = std::fs::File::create(&file).unwrap();
        writeln!(f, "this is not json").unwrap();
        writeln!(f, r#"{{"id":"1","type":"message","message":{{"role":"user","content":[{{"text":"ok"}}]}},"timestamp":"2025-01-15T10:30:00+00:00"}}"#).unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = load_jsonl(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn collect_files_sorted() {
        let dir = tempdir();
        std::fs::write(dir.join("c.jsonl"), "{}").unwrap();
        std::fs::write(dir.join("a.jsonl"), "{}").unwrap();
        std::fs::write(dir.join("b.jsonl"), "{}").unwrap();
        let files = collect_files_with_ext(&dir, "jsonl");
        let names: Vec<&str> = files
            .iter()
            .map(|f| Path::new(f).file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(names, vec!["a.jsonl", "b.jsonl", "c.jsonl"]);
    }

    #[test]
    fn tail_cursor_roundtrip() {
        let mut files = BTreeMap::new();
        files.insert(
            "/tmp/a.jsonl".to_string(),
            FileTail {
                offset: 123,
                mtime: Utc::now(),
                anchor: "deadbeef".to_string(),
                ctx: TailContext {
                    session_id: Some("s1".to_string()),
                    msg_index: 7,
                    ..Default::default()
                },
            },
        );
        files.insert(
            "/tmp/b.jsonl".to_string(),
            FileTail {
                offset: 456,
                mtime: Utc::now(),
                anchor: "cafebabe".to_string(),
                ctx: TailContext::default(),
            },
        );
        let encoded = encode_tail_cursor(&files);
        let parsed = parse_tail_cursor(Some(&encoded)).unwrap();
        assert_eq!(parsed, files);
        // Keys stay sorted so the encoding is deterministic.
        let keys: Vec<&String> = parsed.keys().collect();
        assert_eq!(
            keys,
            vec![&"/tmp/a.jsonl".to_string(), &"/tmp/b.jsonl".to_string()]
        );
    }

    #[test]
    fn parse_tail_cursor_rejects_legacy_composite() {
        let cursor = "2025-01-15T10:30:00+00:00\x1fmy-id";
        assert!(parse_tail_cursor(Some(cursor)).is_none());
        assert!(parse_tail_cursor(None).is_none());
        assert!(parse_tail_cursor(Some("not-json")).is_none());
    }

    #[test]
    fn open_jsonl_unchanged_skips() {
        let dir = tempdir();
        let path = dir.join("s.jsonl");
        std::fs::write(&path, "line1\nline2\n").unwrap();
        let path_str = path.to_str().unwrap().to_string();

        // First parse: full.
        let full = open_jsonl(&path_str, None).unwrap();
        let JsonlResume::Open(opened, ResumeKind::Full) = full else {
            panic!("first open should be Full");
        };
        let mut reader = opened.reader;
        let mut lines = Vec::new();
        for line in reader.by_ref().lines().map_while(Result::ok) {
            lines.push(line);
        }
        assert_eq!(lines, vec!["line1", "line2"]);
        let offset = reader.stream_position().unwrap();
        let mut raw = reader.into_inner();
        let anchor = compute_tail_anchor(&mut raw, offset);
        let mtime: DateTime<Utc> = fs::metadata(&path).unwrap().modified().unwrap().into();
        let cp = FileTail {
            offset,
            mtime,
            anchor,
            ctx: TailContext::default(),
        };

        // Unchanged: skip.
        let again = open_jsonl(&path_str, Some(&cp)).unwrap();
        assert!(matches!(again, JsonlResume::Skip));

        // Append: resume from offset.
        let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(f, "line3").unwrap();
        drop(f);
        let grew = open_jsonl(&path_str, Some(&cp)).unwrap();
        let JsonlResume::Open(opened, ResumeKind::FromOffset) = grew else {
            panic!("append should resume from offset");
        };
        let tail: Vec<String> = opened.reader.lines().map_while(Result::ok).collect();
        assert_eq!(tail, vec!["line3"]);
        assert!(opened.size > cp.offset);

        // Truncate: full re-parse.
        std::fs::write(&path, "only\n").unwrap();
        let shrunk = open_jsonl(&path_str, Some(&cp)).unwrap();
        let JsonlResume::Open(_, ResumeKind::Full) = shrunk else {
            panic!("truncation should force full re-parse");
        };
    }

    #[test]
    fn open_jsonl_rewrite_with_same_size_forces_full() {
        let dir = tempdir();
        let path = dir.join("s.jsonl");
        std::fs::write(&path, "aaa\n").unwrap();
        let path_str = path.to_str().unwrap().to_string();
        let mtime: DateTime<Utc> = fs::metadata(&path).unwrap().modified().unwrap().into();
        let cp = FileTail {
            offset: 4,
            mtime,
            anchor: "x".to_string(),
            ctx: TailContext::default(),
        };
        // Same size, different bytes (mtime may be identical on coarse clocks;
        // the size+mtime guard alone cannot see it, and same size never resumes).
        std::fs::write(&path, "bbb\n").unwrap();
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), cp.offset);
        let again = open_jsonl(&path_str, Some(&cp)).unwrap();
        let JsonlResume::Open(_, ResumeKind::Full) = again else {
            panic!("same-size rewrite should force full re-parse");
        };
    }

    #[test]
    fn open_jsonl_grew_with_changed_suffix_forces_full() {
        let dir = tempdir();
        let path = dir.join("s.jsonl");
        std::fs::write(&path, "aaaa\n").unwrap();
        let path_str = path.to_str().unwrap().to_string();
        let mtime: DateTime<Utc> = fs::metadata(&path).unwrap().modified().unwrap().into();
        // Record a checkpoint whose anchor does NOT match the current suffix:
        // the file "grew" (size 5 -> 10) but the pre-offset bytes changed.
        let cp = FileTail {
            offset: 5,
            mtime,
            anchor: "wrong-anchor".to_string(),
            ctx: TailContext::default(),
        };
        std::fs::write(&path, "bbbb\nccccc\n").unwrap();
        let again = open_jsonl(&path_str, Some(&cp)).unwrap();
        let JsonlResume::Open(_, ResumeKind::Full) = again else {
            panic!("grew with changed suffix should force full re-parse");
        };
    }

    #[test]
    fn compute_tail_anchor_stable() {
        let dir = tempdir();
        let path = dir.join("s.jsonl");
        std::fs::write(&path, "line1\nline2\n").unwrap();
        let mut file = fs::File::open(&path).unwrap();
        let a1 = compute_tail_anchor(&mut file, 12);
        let a2 = compute_tail_anchor(&mut file, 12);
        assert_eq!(a1, a2);
        assert_eq!(a1.len(), 64); // blake3 hex
        // File position is restored to `offset`.
        assert_eq!(file.stream_position().unwrap(), 12);
        // Zero-length suffix hashes deterministically too.
        let mut file2 = fs::File::open(&path).unwrap();
        let _ = compute_tail_anchor(&mut file2, 0);
    }

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("remi_test_{}_{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
