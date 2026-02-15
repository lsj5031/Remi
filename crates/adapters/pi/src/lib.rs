use std::{fs, io::BufRead, path::PathBuf};

use chrono::{DateTime, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;

pub struct PiAdapter;

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

fn load_pi_jsonl(
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

                        if role == "toolResult" {
                            continue;
                        }

                        let content_text = extract_text_only(msg.get("content"));
                        if content_text.is_empty() {
                            continue;
                        }

                        let mapped_role = match role {
                            "user" | "assistant" => role,
                            _ => "user",
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
                        let source_id = format!("{sid}:{msg_index}");
                        msg_index += 1;

                        if let Some(ref cur) = parsed_cursor
                            && adapter_common::should_skip(line_ts, &source_id, cur)
                        {
                            continue;
                        }

                        let title = first_user_text
                            .as_deref()
                            .map(|t| {
                                if t.len() > 80 {
                                    format!("{}…", &t[..80])
                                } else {
                                    t.to_string()
                                }
                            })
                            .unwrap_or_else(|| sid.clone());

                        let mut obj = serde_json::Map::new();
                        obj.insert("role".to_string(), Value::String(mapped_role.to_string()));
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
    Ok(out)
}

fn normalize_records(records: &[NativeRecord]) -> NormalizedBatch {
    let kind = AgentKind::Pi;
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
        let content = extract_text_only(rec.payload.get("content"));
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
    fn skip_tool_result_messages() {
        let dir = tempdir();
        let path = write_session(
            &dir,
            &[
                r#"{"type":"session","version":3,"id":"sess-pi-2","timestamp":"2026-02-08T10:54:12.530Z","cwd":"/tmp"}"#,
                r#"{"type":"message","id":"m1","parentId":null,"timestamp":"2026-02-08T10:55:00.000Z","message":{"role":"user","content":[{"type":"text","text":"run tests"}]}}"#,
                r#"{"type":"message","id":"m2","parentId":"m1","timestamp":"2026-02-08T10:55:01.000Z","message":{"role":"assistant","content":[{"type":"text","text":"Running tests now"}]}}"#,
                r#"{"type":"message","id":"m3","parentId":"m2","timestamp":"2026-02-08T10:55:02.000Z","message":{"role":"toolResult","toolCallId":"call_xxx","toolName":"bash","content":[{"type":"text","text":"all tests passed"}]}}"#,
                r#"{"type":"message","id":"m4","parentId":"m3","timestamp":"2026-02-08T10:55:03.000Z","message":{"role":"assistant","content":[{"type":"text","text":"All tests passed!"}]}}"#,
            ],
        );
        let records = load_pi_jsonl(&[path], None).unwrap();
        assert_eq!(records.len(), 3);
        let roles: Vec<&str> = records
            .iter()
            .map(|r| r.payload.get("role").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(roles, vec!["user", "assistant", "assistant"]);
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
}
