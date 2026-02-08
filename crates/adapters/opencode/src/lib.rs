use std::{
    fs,
    path::PathBuf,
};

use chrono::{DateTime, TimeZone, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;

pub struct OpenCodeAdapter;

impl AgentAdapter for OpenCodeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::OpenCode
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        Ok(adapter_common::collect_files_with_ext(
            &base.join(".local/share/opencode/storage/message"),
            "json",
        ))
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        load_message_json(source_paths, cursor)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(normalize_records(AgentKind::OpenCode, records))
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
        let role = rec
            .payload
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = rec
            .payload
            .get("__content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if content.is_empty() {
            continue;
        }
        let session_seed = rec
            .payload
            .get("sessionID")
            .and_then(|v| v.as_str())
            .or_else(|| rec.payload.get("sessionId").and_then(|v| v.as_str()))
            .unwrap_or(&rec.source_id);
        let title = rec
            .payload
            .get("__session_title")
            .and_then(Value::as_str)
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

fn load_message_json(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    let parsed_cursor = cursor.and_then(adapter_common::parse_cursor);
    let mut out: Vec<NativeRecord> = source_paths
        .par_iter()
        .filter_map(|path| {
            let content = fs::read_to_string(path).ok()?;
            let mut val: Value = serde_json::from_str(&content).ok()?;
            let ts = extract_ts(&val).unwrap_or_else(Utc::now);
            let source_id = val
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| deterministic_id(&[path]));
            if let Some(ref cur) = parsed_cursor
                && adapter_common::should_skip(ts, &source_id, cur)
            {
                return None;
            }

            let content_text = extract_part_text(&source_id);
            if content_text.is_empty() {
                return None;
            }

            let session_id = val
                .get("sessionID")
                .and_then(Value::as_str)
                .or_else(|| val.get("sessionId").and_then(Value::as_str))
                .unwrap_or("")
                .to_string();

            if let Some(obj) = val.as_object_mut() {
                obj.insert("__source_path".to_string(), Value::String(path.clone()));
                obj.insert("__content".to_string(), Value::String(content_text));
                obj.insert(
                    "__session_title".to_string(),
                    Value::String(session_id.clone()),
                );
            }

            Some(NativeRecord {
                source_id,
                updated_at: ts,
                payload: val,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.source_id.cmp(&b.source_id))
    });
    Ok(out)
}

fn extract_part_text(message_id: &str) -> String {
    let mut out = String::new();
    let Some(home) = dirs::home_dir() else {
        return out;
    };
    let part_dir = home
        .join(".local/share/opencode/storage/part")
        .join(message_id);
    let mut files = adapter_common::collect_files_with_ext(&part_dir, "json");
    files.sort();
    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue;
        };
        let Ok(val): Result<Value, _> = serde_json::from_str(&content) else {
            continue;
        };
        let text = val
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&text);
    }
    out
}

fn extract_ts(val: &Value) -> Option<DateTime<Utc>> {
    if let Some(ms) = val
        .get("time")
        .and_then(|t| t.get("created"))
        .and_then(Value::as_i64)
    {
        return Utc.timestamp_millis_opt(ms).single();
    }
    if let Some(ms) = val.get("timestamp").and_then(Value::as_i64) {
        return Utc.timestamp_millis_opt(ms).single();
    }
    if let Some(s) = val.get("timestamp").and_then(Value::as_str) {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc));
    }
    None
}
