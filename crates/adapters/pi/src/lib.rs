use std::{fs, io::BufRead, path::PathBuf};

use chrono::{DateTime, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

pub struct PiAdapter;

const PI_CURSOR_PREFIX: &str = "pi-v2|";

impl AgentAdapter for PiAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Pi
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let mut out = Vec::new();
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".pi/agent/sessions"),
            "jsonl",
        ));
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".pi/sessions"),
            "jsonl",
        ));
        debug!(files = out.len(), "pi adapter discovered source paths");
        Ok(out)
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        load_pi_jsonl(source_paths, cursor)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(normalize_records(records))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
            .map(|cursor| format!("{PI_CURSOR_PREFIX}{cursor}"))
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
        return String::new();
    };
    let mut parts = Vec::new();
    for item in arr {
        let Some(obj) = item.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        if let Some(text) = obj.get("text").and_then(Value::as_str) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed.to_string());
            }
        }
    }
    parts.join("\n")
}

fn extract_message_content(role: &str, content: Option<&Value>) -> String {
    if role == "toolResult" {
        let text = extract_text_only(content);
        if text.is_empty() {
            return String::new();
        }
        return format!("tool_result: {text}");
    }

    let Some(Value::Array(arr)) = content else {
        return String::new();
    };
    let mut parts = Vec::new();
    for item in arr {
        let Some(obj) = item.as_object() else {
            continue;
        };
        match obj.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = obj.get("text").and_then(Value::as_str) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        parts.push(trimmed.to_string());
                    }
                }
            }
            Some("toolCall") => {
                if let Some(line) = format_tool_call(obj) {
                    parts.push(line);
                }
            }
            _ => {}
        }
    }
    parts.join("\n")
}

fn format_tool_call(obj: &serde_json::Map<String, Value>) -> Option<String> {
    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let arguments = obj
        .get("arguments")
        .filter(|value| !value.is_null())
        .map(Value::to_string)
        .filter(|value| !value.is_empty());

    if name.is_none() && arguments.is_none() {
        return None;
    }

    let mut out = String::from("tool_use");
    if let Some(name) = name {
        out.push_str(": ");
        out.push_str(name);
    }
    if let Some(arguments) = arguments {
        if name.is_some() {
            out.push(' ');
        } else {
            out.push_str(": ");
        }
        out.push_str(&arguments);
    }
    Some(out)
}

fn load_pi_jsonl(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    let parsed_cursor = cursor
        .and_then(|cursor| cursor.strip_prefix(PI_CURSOR_PREFIX))
        .and_then(adapter_common::parse_cursor);
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
            let mut session_ts: Option<DateTime<Utc>> = None;
            let mut cwd: Option<String> = None;
            let mut first_user_text: Option<String> = None;
            let mut records = Vec::new();
            let mut visible_msg_index = 0usize;

            for line in &lines {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(val): Result<Value, _> = serde_json::from_str(trimmed) else {
                    continue;
                };

                let line_type = val.get("type").and_then(Value::as_str).unwrap_or("");
                let line_ts = val
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(parse_rfc3339)
                    .or(file_mtime)
                    .unwrap_or_else(Utc::now);

                match line_type {
                    "session" => {
                        if let Some(id) = val.get("id").and_then(Value::as_str) {
                            session_id = id.to_string();
                        }
                        if let Some(dir) = val.get("cwd").and_then(Value::as_str) {
                            cwd = Some(dir.to_string());
                        }
                        if session_ts.is_none() {
                            session_ts = Some(line_ts);
                        }
                    }
                    "message" => {
                        let Some(msg) = val.get("message") else {
                            continue;
                        };
                        let role = msg.get("role").and_then(Value::as_str).unwrap_or("user");

                        let content_text = extract_message_content(role, msg.get("content"));
                        if content_text.is_empty() {
                            continue;
                        }

                        let mapped_role = match role {
                            "user" => "user",
                            _ => "assistant",
                        };

                        if mapped_role == "user" && first_user_text.is_none() {
                            first_user_text = Some(content_text.clone());
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
                        let source_id = if role == "toolResult" {
                            let tool_result_id = val
                                .get("id")
                                .and_then(Value::as_str)
                                .filter(|id| !id.is_empty())
                                .map(ToOwned::to_owned)
                                .unwrap_or_else(|| deterministic_id(&[path, trimmed]));
                            format!("{sid}:toolResult:{tool_result_id}")
                        } else {
                            let source_id = format!("{sid}:{visible_msg_index}");
                            visible_msg_index += 1;
                            source_id
                        };

                        if let Some(ref cur) = parsed_cursor
                            && adapter_common::should_skip(line_ts, &source_id, cur)
                        {
                            continue;
                        }

                        let title = first_user_text
                            .as_deref()
                            .map(|t| {
                                if t.chars().count() > 80 {
                                    format!("{}…", t.chars().take(80).collect::<String>())
                                } else {
                                    t.to_string()
                                }
                            })
                            .unwrap_or_else(|| sid.clone());

                        let mut obj = serde_json::Map::new();
                        obj.insert("role".to_string(), Value::String(role.to_string()));
                        if let Some(content) = msg.get("content") {
                            obj.insert("content".to_string(), content.clone());
                        } else {
                            obj.insert("content".to_string(), Value::Array(vec![]));
                        }
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
    debug!(total = out.len(), "pi jsonl loaded");
    Ok(out)
}

fn normalize_records(records: &[NativeRecord]) -> NormalizedBatch {
    let kind = AgentKind::Pi;
    debug!(records = records.len(), "normalizing pi records");
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
        let normalized_role = if role == "user" { "user" } else { "assistant" };
        let content = extract_message_content(&role, rec.payload.get("content"));
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
            role: normalized_role.to_string(),
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
        "pi records normalized"
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
        let dir = std::env::temp_dir().join(format!("remi_pi_test_{}_{}", std::process::id(), id));
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
    fn load_pi_session_basic() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session","version":3,"id":"sess-pi-1","timestamp":"2026-02-08T10:54:12.530Z","cwd":"/home/leo/code/Remi"}"#,
                r#"{"type":"model_change","id":"63bc714f","parentId":null,"timestamp":"2026-02-08T10:54:12.531Z","provider":"local-openai","modelId":"gpt-5.3-codex"}"#,
                r#"{"type":"message","id":"5a68fc81","parentId":"c725ef67","timestamp":"2026-02-08T10:54:41.688Z","message":{"role":"user","content":[{"type":"text","text":"check this app"}],"timestamp":1770548081684}}"#,
                r#"{"type":"message","id":"bad2ad59","parentId":"5a68fc81","timestamp":"2026-02-08T10:54:45.731Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"let me think..."},{"type":"text","text":"Looking at the code..."}]}}"#,
            ],
        );
        let records = load_pi_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_id, "sess-pi-1:0");
        assert_eq!(records[1].source_id, "sess-pi-1:1");
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
            "sess-pi-1"
        );
        assert_eq!(
            records[0]
                .payload
                .get("__thread_title")
                .unwrap()
                .as_str()
                .unwrap(),
            "check this app"
        );
        assert_eq!(
            records[0]
                .payload
                .get("__workspace_path")
                .unwrap()
                .as_str()
                .unwrap(),
            "/home/leo/code/Remi"
        );
    }

    #[test]
    fn preserves_tool_calls_and_tool_results() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session","version":3,"id":"sess-pi-2","timestamp":"2026-02-08T10:54:12.530Z","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","parentId":null,"timestamp":"2026-02-08T10:55:00.000Z","message":{"role":"user","content":[{"type":"text","text":"run tests"}]}}"#,
                r#"{"type":"message","id":"m2","parentId":"m1","timestamp":"2026-02-08T10:55:01.000Z","message":{"role":"assistant","content":[{"type":"text","text":"Running tests now"},{"type":"toolCall","id":"call_xxx","name":"bash","arguments":{"command":"cargo test"}}]}}"#,
                r#"{"type":"message","id":"m3","parentId":"m2","timestamp":"2026-02-08T10:55:02.000Z","message":{"role":"toolResult","toolCallId":"call_xxx","toolName":"bash","content":[{"type":"text","text":"all tests passed"}]}}"#,
                r#"{"type":"message","id":"m4","parentId":"m3","timestamp":"2026-02-08T10:55:03.000Z","message":{"role":"assistant","content":[{"type":"text","text":"All tests passed!"}]}}"#,
            ],
        );
        let records = load_pi_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 4);
        let source_ids: Vec<&str> = records.iter().map(|r| r.source_id.as_str()).collect();
        assert_eq!(
            source_ids,
            vec![
                "sess-pi-2:0",
                "sess-pi-2:1",
                "sess-pi-2:toolResult:m3",
                "sess-pi-2:2",
            ]
        );
        let roles: Vec<&str> = records
            .iter()
            .map(|r| r.payload.get("role").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(roles, vec!["user", "assistant", "toolResult", "assistant"]);

        let batch = normalize_records(&records);
        let batch_roles: Vec<&str> = batch.messages.iter().map(|msg| msg.role.as_str()).collect();
        assert_eq!(
            batch_roles,
            vec!["user", "assistant", "assistant", "assistant"]
        );
        let contents: Vec<&str> = batch
            .messages
            .iter()
            .map(|msg| msg.content.as_str())
            .collect();
        assert_eq!(
            contents,
            vec![
                "run tests",
                "Running tests now\ntool_use: bash {\"command\":\"cargo test\"}",
                "tool_result: all tests passed",
                "All tests passed!",
            ]
        );
    }

    #[test]
    fn normalize_pi_session() {
        let ts1 = DateTime::parse_from_rfc3339("2026-02-08T10:54:41.688Z")
            .unwrap()
            .with_timezone(&Utc);
        let ts2 = DateTime::parse_from_rfc3339("2026-02-08T10:54:45.731Z")
            .unwrap()
            .with_timezone(&Utc);
        let records = vec![
            NativeRecord {
                source_id: "sess-pi-1:0".to_string(),
                updated_at: ts1,
                payload: serde_json::json!({
                    "role": "user",
                    "content": [{"type": "text", "text": "check this app"}],
                    "__thread_id": "sess-pi-1",
                    "__thread_title": "check this app",
                    "__thread_ts": "2026-02-08T10:54:12.530Z",
                    "__source_path": "/tmp/session.jsonl",
                    "__workspace_path": "/home/leo/code/Remi"
                }),
            },
            NativeRecord {
                source_id: "sess-pi-1:1".to_string(),
                updated_at: ts2,
                payload: serde_json::json!({
                    "role": "assistant",
                    "content": [{"type": "thinking", "thinking": "let me think..."}, {"type": "text", "text": "Looking at the code..."}],
                    "__thread_id": "sess-pi-1",
                    "__thread_title": "check this app",
                    "__thread_ts": "2026-02-08T10:54:12.530Z",
                    "__source_path": "/tmp/session.jsonl",
                    "__workspace_path": "/home/leo/code/Remi"
                }),
            },
        ];

        let batch = normalize_records(&records);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.provenance.len(), 2);
        assert_eq!(batch.sessions[0].source_ref, "sess-pi-1");
        assert_eq!(batch.sessions[0].title, "check this app");
        assert_eq!(batch.sessions[0].agent, AgentKind::Pi);
        assert_eq!(batch.messages[0].role, "user");
        assert_eq!(batch.messages[0].content, "check this app");
        assert_eq!(batch.messages[1].role, "assistant");
        assert_eq!(batch.messages[1].content, "Looking at the code...");
        assert_eq!(batch.provenance[0].source_path, "/home/leo/code/Remi");
    }

    #[test]
    fn ignores_legacy_checkpoint_format_for_full_rescan() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session","version":3,"id":"sess-pi-3","timestamp":"2026-02-08T10:54:12.530Z","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","parentId":null,"timestamp":"2026-02-08T10:55:00.000Z","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            ],
        );

        let legacy_cursor = "2026-02-08T10:55:00+00:00\x1fsess-pi-3:0";
        let records = load_pi_jsonl(&[path], Some(legacy_cursor)).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "sess-pi-3:0");
    }

    #[test]
    fn versioned_checkpoint_still_filters_seen_records() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session","version":3,"id":"sess-pi-4","timestamp":"2026-02-08T10:54:12.530Z","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","parentId":null,"timestamp":"2026-02-08T10:55:00.000Z","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
                r#"{"type":"message","id":"m2","parentId":"m1","timestamp":"2026-02-08T10:55:01.000Z","message":{"role":"assistant","content":[{"type":"text","text":"world"}]}}"#,
            ],
        );

        let cursor = format!("{PI_CURSOR_PREFIX}2026-02-08T10:55:00+00:00\x1fsess-pi-4:0");
        let records = load_pi_jsonl(&[path], Some(&cursor)).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "sess-pi-4:1");
    }
}
