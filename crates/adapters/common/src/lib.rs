use std::{fs, path::Path};

use chrono::{DateTime, TimeZone, Utc};
use core_model::{AgentKind, NativeRecord, NormalizedBatch, deterministic_id};
use rayon::prelude::*;
use serde_json::Value;

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
    out
}

pub fn load_jsonl(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    let parsed_cursor = cursor.and_then(parse_cursor);
    let mut out: Vec<NativeRecord> = source_paths
        .par_iter()
        .flat_map(|path| {
            let stem = Path::new(path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(path)
                .to_string();
            let Ok(content) = fs::read_to_string(path) else {
                return Vec::new();
            };
            content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|line| {
                    let mut val: Value = serde_json::from_str(line).ok()?;
                    let ts = extract_ts(&val).unwrap_or_else(Utc::now);
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert(
                            "__source_path".to_string(),
                            Value::String(path.clone()),
                        );
                        obj.insert(
                            "__session_seed".to_string(),
                            Value::String(stem.clone()),
                        );
                    }
                    let source_id = val
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| deterministic_id(&[path, line]));
                    if let Some(ref cur) = parsed_cursor
                        && should_skip(ts, &source_id, cur)
                    {
                        return None;
                    }
                    Some(NativeRecord {
                        source_id,
                        updated_at: ts,
                        payload: val,
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.source_id.cmp(&b.source_id))
    });
    Ok(out)
}

pub fn normalize_jsonl_records(kind: AgentKind, records: &[NativeRecord]) -> NormalizedBatch {
    let mut batch = NormalizedBatch::default();
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
        batch.sessions.push(core_model::Session {
            id: session_id.clone(),
            agent: kind,
            source_ref: session_seed.to_string(),
            title: title.to_string(),
            created_at: now,
            updated_at: now,
        });
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
    let mut out = String::new();
    let Some(content) = content else {
        return out;
    };
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    if let Some(arr) = content.as_array() {
        for item in arr {
            if let Some(s) = item.get("text").and_then(Value::as_str)
                && !s.trim().is_empty()
            {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(s);
            }
            if let Some(s) = item.get("thinking").and_then(Value::as_str)
                && !s.trim().is_empty()
            {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(s);
            }
        }
    }
    out
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
        writeln!(f, r#"{{"id":"aaa","timestamp":"2025-01-10T00:00:00+00:00"}}"#).unwrap();
        writeln!(f, r#"{{"id":"mmm","timestamp":"2025-01-10T00:00:00+00:00"}}"#).unwrap();
        writeln!(f, r#"{{"id":"zzz","timestamp":"2025-01-10T00:00:00+00:00"}}"#).unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let cursor = "2025-01-10T00:00:00+00:00\x1fmmm".to_string();
        let records = load_jsonl(&paths, Some(&cursor)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "zzz");
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
