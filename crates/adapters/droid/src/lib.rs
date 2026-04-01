use std::{fs, io::BufRead, path::PathBuf};

use chrono::{DateTime, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

pub struct DroidAdapter;

impl AgentAdapter for DroidAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Droid
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let mut out = Vec::new();
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".factory/sessions"),
            "jsonl",
        ));
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".local/share/factory-droid/sessions"),
            "jsonl",
        ));
        debug!(files = out.len(), "droid adapter discovered source paths");
        Ok(out)
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        load_droid_jsonl(source_paths, cursor)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(normalize_records(records))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}

fn parse_rfc3339(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_text_only(content: Option<&Value>) -> String {
    let Some(Value::Array(arr)) = content else {
        if let Some(Value::String(s)) = content {
            return s.clone();
        }
        return String::new();
    };
    let mut texts = Vec::new();
    for item in arr {
        let Some(obj) = item.as_object() else {
            continue;
        };
        let kind = obj.get("type").and_then(Value::as_str).unwrap_or("");
        if kind != "text" {
            continue;
        }
        if let Some(text) = obj.get("text").and_then(Value::as_str) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                texts.push(trimmed.to_string());
            }
        }
    }
    texts.join("\n")
}

fn has_only_tool_blocks(content: Option<&Value>) -> bool {
    let Some(Value::Array(arr)) = content else {
        return false;
    };
    if arr.is_empty() {
        return true;
    }
    arr.iter().all(|item| {
        let Some(kind) = item.get("type").and_then(Value::as_str) else {
            return false;
        };
        matches!(kind, "tool_use" | "tool_result")
    })
}

fn load_droid_jsonl(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    let parsed_cursor = cursor.and_then(adapter_common::parse_cursor);
    let mut out: Vec<NativeRecord> = source_paths
        .par_iter()
        .flat_map(|path| {
            let file_mtime = adapter_common::file_mtime(path);
            if let Some(ref cur) = parsed_cursor
                && let Some(mtime) = file_mtime
                && mtime <= cur.ts
            {
                return Vec::new();
            }

            let file = match fs::File::open(path) {
                Ok(f) => f,
                Err(_) => return Vec::new(),
            };
            let reader = std::io::BufReader::new(file);
            let lines: Vec<String> = reader.lines().map_while(Result::ok).collect();
            if lines.is_empty() {
                return Vec::new();
            }

            let mut session_id = String::new();
            let mut session_title: Option<String> = None;
            let mut session_ts: Option<DateTime<Utc>> = None;
            let mut cwd: Option<String> = None;
            let mut first_user_text: Option<String> = None;
            let mut records = Vec::new();
            let mut msg_index = 0usize;

            for line in &lines {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(val): Result<Value, _> = serde_json::from_str(trimmed) else {
                    continue;
                };

                let line_type = val.get("type").and_then(Value::as_str).unwrap_or("");

                match line_type {
                    "session_start" => {
                        if let Some(id) = val.get("id").and_then(Value::as_str) {
                            session_id = id.to_string();
                        }
                        if let Some(st) = val.get("sessionTitle").and_then(Value::as_str)
                            && !st.is_empty()
                        {
                            session_title = Some(st.to_string());
                        }
                        if session_title.is_none()
                            && let Some(t) = val.get("title").and_then(Value::as_str)
                            && !t.is_empty()
                        {
                            session_title = Some(t.to_string());
                        }
                        if let Some(dir) = val.get("cwd").and_then(Value::as_str) {
                            cwd = Some(dir.to_string());
                        }
                        let header_ts = val
                            .get("timestamp")
                            .and_then(Value::as_str)
                            .and_then(parse_rfc3339)
                            .or(file_mtime);
                        if session_ts.is_none() {
                            session_ts = header_ts;
                        }
                    }
                    "message" => {
                        let Some(message) = val.get("message") else {
                            continue;
                        };
                        let content = message.get("content");

                        if has_only_tool_blocks(content) {
                            continue;
                        }

                        let text = extract_text_only(content);
                        if text.is_empty() {
                            continue;
                        }

                        let role = message
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or("user");

                        if role == "user" && first_user_text.is_none() {
                            first_user_text = Some(text.clone());
                        }

                        let line_ts = val
                            .get("timestamp")
                            .and_then(Value::as_str)
                            .and_then(parse_rfc3339)
                            .or(file_mtime)
                            .unwrap_or_else(Utc::now);

                        if session_ts.is_none() {
                            session_ts = Some(line_ts);
                        }

                        let sid = if session_id.is_empty() {
                            std::path::Path::new(path)
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or(path)
                                .to_string()
                        } else {
                            session_id.clone()
                        };
                        let source_id = format!("{sid}:{msg_index}");
                        msg_index += 1;

                        if let Some(ref cur) = parsed_cursor
                            && adapter_common::should_skip(line_ts, &source_id, cur)
                        {
                            continue;
                        }

                        let title = session_title
                            .clone()
                            .or_else(|| {
                                first_user_text.as_deref().map(|t| {
                                    if t.chars().count() > 80 {
                                        format!("{}…", t.chars().take(80).collect::<String>())
                                    } else {
                                        t.to_string()
                                    }
                                })
                            })
                            .unwrap_or_else(|| sid.clone());

                        let mut obj = serde_json::Map::new();
                        obj.insert("role".to_string(), Value::String(role.to_string()));
                        obj.insert(
                            "content".to_string(),
                            Value::Array(vec![Value::String(text)]),
                        );
                        obj.insert("__thread_id".to_string(), Value::String(sid.clone()));
                        obj.insert("__thread_title".to_string(), Value::String(title));
                        if let Some(ts) = session_ts {
                            obj.insert("__thread_ts".to_string(), Value::String(ts.to_rfc3339()));
                        }
                        obj.insert("__source_path".to_string(), Value::String(path.clone()));
                        if let Some(ref dir) = cwd {
                            obj.insert("__workspace_path".to_string(), Value::String(dir.clone()));
                        }

                        records.push(NativeRecord {
                            source_id,
                            updated_at: line_ts,
                            payload: Value::Object(obj),
                        });
                    }
                    _ => {}
                }
            }

            records
        })
        .collect();

    out.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.source_id.cmp(&b.source_id))
    });
    debug!(total = out.len(), "droid jsonl loaded");
    Ok(out)
}

fn normalize_records(records: &[NativeRecord]) -> NormalizedBatch {
    let kind = AgentKind::Droid;
    debug!(records = records.len(), "normalizing droid records");
    let mut batch = NormalizedBatch::default();
    let mut sessions: std::collections::HashMap<String, core_model::Session> =
        std::collections::HashMap::new();

    for rec in records {
        let role = rec
            .payload
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = rec
            .payload
            .get("content")
            .and_then(Value::as_array)
            .and_then(|arr| arr.first())
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if content.is_empty() {
            continue;
        }

        let thread_id = rec
            .payload
            .get("__thread_id")
            .and_then(Value::as_str)
            .unwrap_or(&rec.source_id)
            .to_string();
        let title = rec
            .payload
            .get("__thread_title")
            .and_then(Value::as_str)
            .unwrap_or(&thread_id)
            .to_string();
        let created_at = rec
            .payload
            .get("__thread_ts")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339)
            .unwrap_or(rec.updated_at);

        let session_id = deterministic_id(&[kind.as_str(), "session", &thread_id]);
        let message_id = deterministic_id(&[kind.as_str(), "message", &rec.source_id]);
        let session = sessions
            .entry(session_id.clone())
            .or_insert_with(|| core_model::Session {
                id: session_id.clone(),
                agent: kind,
                source_ref: thread_id.clone(),
                title: title.clone(),
                created_at,
                updated_at: rec.updated_at,
            });
        if session.created_at > created_at {
            session.created_at = created_at;
        }
        if session.updated_at < rec.updated_at {
            session.updated_at = rec.updated_at;
        }
        if session.title.is_empty() && !title.is_empty() {
            session.title = title;
        }

        batch.messages.push(core_model::Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            role,
            content,
            ts: rec.updated_at,
        });
        batch.provenance.push(core_model::Provenance {
            id: deterministic_id(&["prov", &message_id]),
            entity_type: "message".to_string(),
            entity_id: message_id,
            agent: kind,
            source_path: rec
                .payload
                .get("__workspace_path")
                .and_then(Value::as_str)
                .or_else(|| rec.payload.get("__source_path").and_then(Value::as_str))
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
    debug!(
        sessions = batch.sessions.len(),
        messages = batch.messages.len(),
        "droid records normalized"
    );
    batch
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("remi_droid_test_{}_{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_session(dir: &std::path::Path, lines: &[&str]) -> String {
        let file = dir.join("session.jsonl");
        let mut f = fs::File::create(&file).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        file.to_str().unwrap().to_string()
    }

    #[test]
    fn load_droid_session_basic() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session_start","id":"sess-1","title":"build release","sessionTitle":"Build Release","cwd":"/home/user/project"}"#,
                r#"{"type":"message","id":"m1","timestamp":"2026-02-11T09:52:37.424Z","message":{"role":"user","content":[{"type":"text","text":"build a bundled release"}]}}"#,
                r#"{"type":"message","id":"m2","timestamp":"2026-02-11T09:52:41.189Z","message":{"role":"assistant","content":[{"type":"text","text":"I'll build a bundled release"}]}}"#,
            ],
        );
        let records = load_droid_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_id, "sess-1:0");
        assert_eq!(records[1].source_id, "sess-1:1");
        assert_eq!(
            records[0].payload.get("role").unwrap().as_str().unwrap(),
            "user"
        );
        assert_eq!(
            records[1].payload.get("role").unwrap().as_str().unwrap(),
            "assistant"
        );
        assert_eq!(
            records[0]
                .payload
                .get("__thread_id")
                .unwrap()
                .as_str()
                .unwrap(),
            "sess-1"
        );
        assert_eq!(
            records[0]
                .payload
                .get("__workspace_path")
                .unwrap()
                .as_str()
                .unwrap(),
            "/home/user/project"
        );
        assert_eq!(
            records[0]
                .payload
                .get("__thread_title")
                .unwrap()
                .as_str()
                .unwrap(),
            "Build Release"
        );
    }

    #[test]
    fn skip_tool_only_messages() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session_start","id":"sess-2","sessionTitle":"Test","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","timestamp":"2026-02-11T09:52:37.424Z","message":{"role":"user","content":[{"type":"text","text":"do something"}]}}"#,
                r#"{"type":"message","id":"m2","timestamp":"2026-02-11T09:52:41.189Z","message":{"role":"assistant","content":[{"type":"text","text":"Sure"},{"type":"tool_use","id":"call_xxx","name":"Execute","input":{}}]}}"#,
                r#"{"type":"message","id":"m3","timestamp":"2026-02-11T09:52:44.410Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_xxx","content":"Output"}]}}"#,
                r#"{"type":"message","id":"m4","timestamp":"2026-02-11T09:52:45.000Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"call_yyy","name":"Read","input":{}}]}}"#,
                r#"{"type":"todo_state","id":"t1","timestamp":"2026-02-11T09:52:45.500Z","todos":{"todos":"1. [done] Done"}}"#,
                r#"{"type":"message","id":"m5","timestamp":"2026-02-11T09:52:46.000Z","message":{"role":"assistant","content":[{"type":"text","text":"All done"}]}}"#,
            ],
        );
        let records = load_droid_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 3);
        let roles: Vec<&str> = records
            .iter()
            .map(|r| r.payload.get("role").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(roles, vec!["user", "assistant", "assistant"]);
    }

    #[test]
    fn normalize_droid_session() {
        let ts1 = DateTime::parse_from_rfc3339("2026-02-11T09:52:37.424Z")
            .unwrap()
            .with_timezone(&Utc);
        let ts2 = DateTime::parse_from_rfc3339("2026-02-11T09:52:41.189Z")
            .unwrap()
            .with_timezone(&Utc);
        let records = vec![
            NativeRecord {
                source_id: "sess-1:0".to_string(),
                updated_at: ts1,
                payload: serde_json::json!({
                    "role": "user",
                    "content": ["build a bundled release"],
                    "__thread_id": "sess-1",
                    "__thread_title": "Build Release",
                    "__thread_ts": "2026-02-11T09:52:37.424Z",
                    "__source_path": "/tmp/session.jsonl",
                    "__workspace_path": "/home/user/project"
                }),
            },
            NativeRecord {
                source_id: "sess-1:1".to_string(),
                updated_at: ts2,
                payload: serde_json::json!({
                    "role": "assistant",
                    "content": ["I'll build a bundled release"],
                    "__thread_id": "sess-1",
                    "__thread_title": "Build Release",
                    "__thread_ts": "2026-02-11T09:52:37.424Z",
                    "__source_path": "/tmp/session.jsonl",
                    "__workspace_path": "/home/user/project"
                }),
            },
        ];

        let batch = normalize_records(&records);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.provenance.len(), 2);
        assert_eq!(batch.sessions[0].source_ref, "sess-1");
        assert_eq!(batch.sessions[0].title, "Build Release");
        assert_eq!(batch.sessions[0].agent, AgentKind::Droid);
        assert_eq!(batch.messages[0].role, "user");
        assert_eq!(batch.messages[0].content, "build a bundled release");
        assert_eq!(batch.messages[1].role, "assistant");
        assert_eq!(batch.messages[1].content, "I'll build a bundled release");
        assert_eq!(batch.provenance[0].source_path, "/home/user/project");
    }

    #[test]
    fn session_title_from_header() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session_start","id":"sess-3","title":"raw title","sessionTitle":"Clean Title","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","timestamp":"2026-02-11T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"first user message as fallback"}]}}"#,
            ],
        );
        let records = load_droid_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]
                .payload
                .get("__thread_title")
                .unwrap()
                .as_str()
                .unwrap(),
            "Clean Title"
        );

        let dir2 = tempdir();
        let path2 = write_session(
            &dir2,
            &[
                r#"{"type":"session_start","id":"sess-4","title":"fallback title","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","timestamp":"2026-02-11T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"user msg"}]}}"#,
            ],
        );
        let records2 = load_droid_jsonl(&[path2], None).unwrap();
        assert_eq!(
            records2[0]
                .payload
                .get("__thread_title")
                .unwrap()
                .as_str()
                .unwrap(),
            "fallback title"
        );

        let dir3 = tempdir();
        let path3 = write_session(
            &dir3,
            &[
                r#"{"type":"session_start","id":"sess-5","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","timestamp":"2026-02-11T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"user msg as title"}]}}"#,
            ],
        );
        let records3 = load_droid_jsonl(&[path3], None).unwrap();
        assert_eq!(
            records3[0]
                .payload
                .get("__thread_title")
                .unwrap()
                .as_str()
                .unwrap(),
            "user msg as title"
        );
    }
}
