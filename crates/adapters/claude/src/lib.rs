use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;

pub struct ClaudeAdapter;

impl AgentAdapter for ClaudeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Claude
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
        let mut out = Vec::new();
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".claude/transcripts"),
            "jsonl",
        ));
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".claude/projects"),
            "jsonl",
        ));
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".local/share/claude-code"),
            "jsonl",
        ));
        Ok(out)
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        let parsed_cursor = cursor.and_then(adapter_common::parse_cursor);
        let mut out: Vec<NativeRecord> = source_paths
            .par_iter()
            .flat_map(|path| {
                let stem = std::path::Path::new(path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path)
                    .to_string();
                let Ok(content) = std::fs::read_to_string(path) else {
                    return Vec::new();
                };
                content
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .filter_map(|line| {
                        let mut val: Value = serde_json::from_str(line).ok()?;
                        let ts =
                            adapter_common::extract_ts(&val).unwrap_or_else(chrono::Utc::now);
                        if let Some(obj) = val.as_object_mut() {
                            obj.insert(
                                "__source_path".to_string(),
                                Value::String(path.clone()),
                            );
                            if !obj.contains_key("sessionId")
                                && !obj.contains_key("sessionID")
                            {
                                obj.insert(
                                    "__session_seed".to_string(),
                                    Value::String(stem.clone()),
                                );
                            }
                        }
                        let source_id = val
                            .get("id")
                            .and_then(|v| v.as_str())
                            .or_else(|| val.get("uuid").and_then(|v| v.as_str()))
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| deterministic_id(&[path, line]));
                        if let Some(ref cur) = parsed_cursor
                            && adapter_common::should_skip(ts, &source_id, cur)
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

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(normalize_records(AgentKind::Claude, records))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}

fn normalize_records(kind: AgentKind, records: &[NativeRecord]) -> NormalizedBatch {
    let mut batch = NormalizedBatch::default();
    for rec in records {
        let message_node = if rec.payload.get("message").is_some_and(Value::is_object) {
            rec.payload.get("message")
        } else {
            Some(&rec.payload)
        };
        let Some(message) = message_node else {
            continue;
        };
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .or_else(|| {
                rec.payload
                    .get("type")
                    .and_then(Value::as_str)
                    .filter(|t| matches!(*t, "user" | "assistant" | "system" | "tool"))
            })
            .unwrap_or("user")
            .to_string();
        let content = adapter_common::extract_content_text(message.get("content"));
        if content.is_empty() {
            continue;
        }
        let session_seed = rec
            .payload
            .get("sessionId")
            .and_then(|v| v.as_str())
            .or_else(|| rec.payload.get("sessionID").and_then(|v| v.as_str()))
            .or_else(|| rec.payload.get("session").and_then(|v| v.as_str()))
            .or_else(|| rec.payload.get("__session_seed").and_then(|v| v.as_str()))
            .or_else(|| rec.payload.get("id").and_then(|v| v.as_str()))
            .unwrap_or(&rec.source_id);
        let title = rec
            .payload
            .get("slug")
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::NativeRecord;

    #[test]
    fn role_does_not_become_message_type() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "type": "message",
                "message": {"content": [{"text": "hello"}]},
                "sessionId": "s1"
            }),
        };
        let batch = normalize_records(AgentKind::Claude, &[rec]);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].role, "user");
    }

    #[test]
    fn role_from_type_when_valid() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "type": "assistant",
                "content": [{"text": "response"}],
                "sessionId": "s1"
            }),
        };
        let batch = normalize_records(AgentKind::Claude, &[rec]);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].role, "assistant");
    }

    #[test]
    fn message_null_falls_back_to_payload() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "message": null,
                "role": "user",
                "content": "direct text",
                "sessionId": "s1"
            }),
        };
        let batch = normalize_records(AgentKind::Claude, &[rec]);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].content, "direct text");
    }

    #[test]
    fn slug_used_as_title() {
        let rec = NativeRecord {
            source_id: "r1".to_string(),
            updated_at: Utc::now(),
            payload: serde_json::json!({
                "role": "user",
                "content": "hello",
                "sessionId": "s1",
                "slug": "my-conversation"
            }),
        };
        let batch = normalize_records(AgentKind::Claude, &[rec]);
        assert_eq!(batch.sessions[0].title, "my-conversation");
    }

    #[test]
    fn uuid_fallback_for_source_id() {
        let adapter = ClaudeAdapter;
        let dir = std::env::temp_dir().join(format!("remi_claude_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("test.jsonl");
        std::fs::write(
            &file,
            r#"{"uuid":"abc-123","role":"user","content":"hi","timestamp":"2025-01-15T00:00:00+00:00","sessionId":"s1"}"#,
        )
        .unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = adapter.scan_changes_since(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "abc-123");
    }

    #[test]
    fn malformed_line_skipped() {
        let adapter = ClaudeAdapter;
        let dir =
            std::env::temp_dir().join(format!("remi_claude_malformed_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("test.jsonl");
        std::fs::write(
            &file,
            "not valid json\n{\"id\":\"1\",\"role\":\"user\",\"content\":\"ok\",\"timestamp\":\"2025-01-15T00:00:00+00:00\",\"sessionId\":\"s1\"}\n",
        )
        .unwrap();
        let paths = vec![file.to_str().unwrap().to_string()];
        let records = adapter.scan_changes_since(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "1");
    }
}
