use std::{collections::HashMap, fs};

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
        let candidates: Vec<CandidateRecord> = source_paths
            .par_iter()
            .flat_map(|path| {
                let file_mtime = adapter_common::file_mtime(path);
                if let Some(ref cur) = parsed_cursor
                    && let Some(mtime) = file_mtime
                    && mtime <= cur.ts
                {
                    return Vec::new();
                }

                let source_kind = source_kind(path);
                let priority = source_priority(source_kind);
                let stem = std::path::Path::new(path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path)
                    .to_string();

                let Ok(content) = fs::read_to_string(path) else {
                    return Vec::new();
                };

                content
                    .lines()
                    .enumerate()
                    .filter_map(|(line_idx, line)| {
                        if line.trim().is_empty() {
                            return None;
                        }

                        let line_number = line_idx + 1;
                        let mut val: Value = serde_json::from_str(line).ok()?;
                        let ts = adapter_common::extract_ts(&val)
                            .or(file_mtime)
                            .unwrap_or_else(chrono::Utc::now);

                        let source_id = extract_message_identity(&val).unwrap_or_else(|| {
                            deterministic_id(&["claude", path, &line_number.to_string(), line])
                        });
                        if let Some(ref cur) = parsed_cursor
                            && adapter_common::should_skip(ts, &source_id, cur)
                        {
                            return None;
                        }

                        let session_key = resolve_session_key(&val, Some(path), &source_id);

                        if let Some(obj) = val.as_object_mut() {
                            obj.insert("__source_path".to_string(), Value::String(path.clone()));
                            obj.insert("__session_seed".to_string(), Value::String(stem.clone()));
                            obj.insert(
                                "__session_key".to_string(),
                                Value::String(session_key.clone()),
                            );
                            obj.insert(
                                "__source_priority".to_string(),
                                Value::Number(serde_json::Number::from(priority)),
                            );
                        }

                        let dedupe_key = dedupe_key(&val, ts, &session_key, line_number);
                        let richness = payload_richness(&val);

                        Some(CandidateRecord {
                            dedupe_key,
                            priority,
                            richness,
                            record: NativeRecord {
                                source_id,
                                updated_at: ts,
                                payload: val,
                            },
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut deduped: HashMap<String, CandidateRecord> = HashMap::new();
        for candidate in candidates {
            deduped
                .entry(candidate.dedupe_key.clone())
                .and_modify(|existing| {
                    if should_replace(existing, &candidate) {
                        *existing = candidate.clone();
                    }
                })
                .or_insert(candidate);
        }

        let mut out: Vec<NativeRecord> = deduped.into_values().map(|c| c.record).collect();
        out.sort_by(|a, b| {
            a.updated_at
                .cmp(&b.updated_at)
                .then_with(|| a.source_id.cmp(&b.source_id))
        });
        Ok(out)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        normalize_records(AgentKind::Claude, records)
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}

#[derive(Clone)]
struct CandidateRecord {
    dedupe_key: String,
    priority: i64,
    richness: usize,
    record: NativeRecord,
}

#[derive(Clone, Copy)]
enum SourceKind {
    Project,
    Transcript,
    LocalShare,
}

fn source_kind(path: &str) -> SourceKind {
    if path.contains(".claude/projects") {
        SourceKind::Project
    } else if path.contains(".claude/transcripts") {
        SourceKind::Transcript
    } else {
        SourceKind::LocalShare
    }
}

fn source_priority(kind: SourceKind) -> i64 {
    match kind {
        SourceKind::Project => 3,
        SourceKind::Transcript => 2,
        SourceKind::LocalShare => 1,
    }
}

fn should_replace(existing: &CandidateRecord, candidate: &CandidateRecord) -> bool {
    if candidate.priority != existing.priority {
        return candidate.priority > existing.priority;
    }
    if candidate.richness != existing.richness {
        return candidate.richness > existing.richness;
    }
    if candidate.record.updated_at != existing.record.updated_at {
        return candidate.record.updated_at > existing.record.updated_at;
    }
    candidate.record.source_id < existing.record.source_id
}

fn dedupe_key(
    payload: &Value,
    ts: chrono::DateTime<chrono::Utc>,
    session_key: &str,
    line_number: usize,
) -> String {
    if let Some(id) = extract_message_identity(payload) {
        return format!("id:{id}");
    }

    let message_node = if payload.get("message").is_some_and(Value::is_object) {
        payload.get("message")
    } else {
        Some(payload)
    };
    let role = message_node
        .and_then(|node| node.get("role").and_then(Value::as_str))
        .or_else(|| {
            payload
                .get("type")
                .and_then(Value::as_str)
                .filter(|t| matches!(*t, "user" | "assistant" | "system" | "tool"))
        })
        .unwrap_or("user");
    let content =
        adapter_common::extract_content_text(message_node.and_then(|node| node.get("content")));

    deterministic_id(&[
        "claude",
        "dedupe",
        session_key,
        &ts.to_rfc3339(),
        role,
        &content,
        &line_number.to_string(),
    ])
}

fn payload_richness(payload: &Value) -> usize {
    let mut score = payload.as_object().map(|obj| obj.len()).unwrap_or(0);
    if payload.get("message").is_some_and(Value::is_object) {
        score += 50;
    }
    if payload.get("slug").and_then(Value::as_str).is_some() {
        score += 10;
    }
    let message_node = if payload.get("message").is_some_and(Value::is_object) {
        payload.get("message")
    } else {
        Some(payload)
    };
    let content =
        adapter_common::extract_content_text(message_node.and_then(|node| node.get("content")));
    score + content.len().min(1_000)
}

fn extract_message_identity(payload: &Value) -> Option<String> {
    payload
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| payload.get("uuid").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn resolve_session_key(payload: &Value, source_path: Option<&str>, _source_id: &str) -> String {
    let mut candidates = Vec::new();

    if let Some(existing) = payload.get("__session_key").and_then(Value::as_str) {
        candidates.push(existing.to_string());
    }
    for key in [
        "sessionId",
        "sessionID",
        "session",
        "conversationId",
        "chatId",
        "projectId",
    ] {
        if let Some(value) = payload.get(key) {
            if let Some(s) = value.as_str() {
                candidates.push(s.to_string());
            }
            if let Some(id) = value.get("id").and_then(Value::as_str) {
                candidates.push(id.to_string());
            }
        }
    }
    if let Some(seed) = payload.get("__session_seed").and_then(Value::as_str) {
        candidates.push(seed.to_string());
    }

    if let Some(path_key) = fallback_session_key_from_path(source_path) {
        candidates.push(path_key);
    }

    for candidate in candidates {
        let trimmed = candidate.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }

    "session-root".to_string()
}

fn fallback_session_key_from_path(source_path: Option<&str>) -> Option<String> {
    let path = std::path::Path::new(source_path?);
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_records(kind: AgentKind, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
    let mut batch = NormalizedBatch::default();
    let mut sessions: HashMap<String, core_model::Session> = HashMap::new();

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

        let source_path = rec.payload.get("__source_path").and_then(Value::as_str);
        let session_seed = resolve_session_key(&rec.payload, source_path, &rec.source_id);
        let title = rec
            .payload
            .get("slug")
            .or_else(|| rec.payload.get("sessionTitle"))
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&session_seed)
            .to_string();

        let session_id = deterministic_id(&[kind.as_str(), "session", &session_seed]);
        let message_id = deterministic_id(&[kind.as_str(), "message", &rec.source_id]);

        let session = sessions
            .entry(session_id.clone())
            .or_insert_with(|| core_model::Session {
                id: session_id.clone(),
                agent: kind,
                source_ref: session_seed.clone(),
                title: title.clone(),
                created_at: rec.updated_at,
                updated_at: rec.updated_at,
            });
        if rec.updated_at < session.created_at {
            session.created_at = rec.updated_at;
        }
        if rec.updated_at > session.updated_at {
            session.updated_at = rec.updated_at;
        }
        if (session.title.is_empty() || session.title == session.source_ref)
            && title != session_seed
        {
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
            source_path: source_path.unwrap_or(kind.as_str()).to_string(),
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
    Ok(batch)
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
        let batch = normalize_records(AgentKind::Claude, &[rec]).unwrap();
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
        let batch = normalize_records(AgentKind::Claude, &[rec]).unwrap();
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
        let batch = normalize_records(AgentKind::Claude, &[rec]).unwrap();
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
        let batch = normalize_records(AgentKind::Claude, &[rec]).unwrap();
        assert_eq!(batch.sessions[0].title, "my-conversation");
    }

    #[test]
    fn scan_prefers_project_source_for_same_message_id() {
        let adapter = ClaudeAdapter;
        let dir = std::env::temp_dir().join(format!("remi_claude_pref_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let projects_dir = dir.join(".claude/projects");
        let transcripts_dir = dir.join(".claude/transcripts");
        std::fs::create_dir_all(&projects_dir).unwrap();
        std::fs::create_dir_all(&transcripts_dir).unwrap();

        let project_file = projects_dir.join("proj.jsonl");
        let transcript_file = transcripts_dir.join("transcript.jsonl");

        std::fs::write(
            &project_file,
            r#"{"id":"msg-1","role":"assistant","content":"project","timestamp":"2025-01-15T00:00:00+00:00","sessionId":"s1","slug":"project"}"#,
        )
        .unwrap();
        std::fs::write(
            &transcript_file,
            r#"{"id":"msg-1","role":"assistant","content":"transcript","timestamp":"2025-01-15T00:00:00+00:00","sessionId":"s1"}"#,
        )
        .unwrap();

        let paths = vec![
            transcript_file.to_string_lossy().to_string(),
            project_file.to_string_lossy().to_string(),
        ];
        let records = adapter.scan_changes_since(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
        let source_path = records[0]
            .payload
            .get("__source_path")
            .and_then(Value::as_str)
            .unwrap();
        assert!(source_path.contains(".claude/projects"));
    }

    #[test]
    fn scan_dedupes_overlapping_sources_without_ids() {
        let adapter = ClaudeAdapter;
        let dir = std::env::temp_dir().join(format!("remi_claude_overlap_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let projects_dir = dir.join(".claude/projects");
        let transcripts_dir = dir.join(".claude/transcripts");
        std::fs::create_dir_all(&projects_dir).unwrap();
        std::fs::create_dir_all(&transcripts_dir).unwrap();

        let project_file = projects_dir.join("proj.jsonl");
        let transcript_file = transcripts_dir.join("transcript.jsonl");

        let line = r#"{"sessionId":"s1","role":"assistant","content":"same","timestamp":"2025-01-15T00:00:00+00:00"}"#;
        std::fs::write(&project_file, line).unwrap();
        std::fs::write(&transcript_file, line).unwrap();

        let paths = vec![
            transcript_file.to_string_lossy().to_string(),
            project_file.to_string_lossy().to_string(),
        ];
        let records = adapter.scan_changes_since(&paths, None).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn scan_keeps_repeated_no_id_lines_from_same_file() {
        let adapter = ClaudeAdapter;
        let dir = std::env::temp_dir().join(format!("remi_claude_repeat_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let file = dir.join("repeat.jsonl");
        let line = r#"{"sessionId":"s1","role":"assistant","content":"same"}"#;
        std::fs::write(&file, format!("{line}\n{line}\n")).unwrap();

        let paths = vec![file.to_string_lossy().to_string()];
        let records = adapter.scan_changes_since(&paths, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_ne!(records[0].source_id, records[1].source_id);
    }

    #[test]
    fn normalize_groups_by_canonical_session_key() {
        let now = Utc::now();
        let records = vec![
            NativeRecord {
                source_id: "a".to_string(),
                updated_at: now,
                payload: serde_json::json!({
                    "role": "user",
                    "content": "one",
                    "__session_key": "sess-1",
                    "__source_path": "/tmp/.claude/projects/p.jsonl"
                }),
            },
            NativeRecord {
                source_id: "b".to_string(),
                updated_at: now,
                payload: serde_json::json!({
                    "role": "assistant",
                    "content": "two",
                    "sessionId": "sess-1",
                    "__source_path": "/tmp/.claude/transcripts/t.jsonl"
                }),
            },
        ];

        let batch = normalize_records(AgentKind::Claude, &records).unwrap();
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 2);
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
