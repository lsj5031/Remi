use std::{collections::HashMap, fs, path::PathBuf};

use chrono::{DateTime, Duration, TimeZone, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, Session,
    deterministic_id,
};
use rayon::prelude::*;
use serde_json::Value;

pub struct AmpAdapter;

impl AgentAdapter for AmpAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Amp
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        Ok(adapter_common::collect_files_with_ext(
            &base.join(".local/share/amp/threads"),
            "json",
        ))
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        load_thread_json(source_paths, cursor)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(normalize_records(AgentKind::Amp, records))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}

#[derive(Debug, Clone)]
struct SessionAccum {
    session: Session,
}

#[derive(Default)]
struct UsageLedgerIndex {
    by_message_id: HashMap<String, DateTime<Utc>>,
    by_index: HashMap<usize, DateTime<Utc>>,
}

fn normalize_records(kind: AgentKind, records: &[NativeRecord]) -> NormalizedBatch {
    let mut batch = NormalizedBatch::default();
    let mut sessions: HashMap<String, SessionAccum> = HashMap::new();
    for rec in records {
        let role = rec
            .payload
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = adapter_common::extract_content_text(rec.payload.get("content"));
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
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(|| SessionAccum {
                session: Session {
                    id: session_id.clone(),
                    agent: kind,
                    source_ref: thread_id.clone(),
                    title: title.clone(),
                    created_at,
                    updated_at: rec.updated_at,
                },
            });
        if entry.session.created_at > created_at {
            entry.session.created_at = created_at;
        }
        if entry.session.updated_at < rec.updated_at {
            entry.session.updated_at = rec.updated_at;
        }
        if entry.session.title.is_empty() && !title.is_empty() {
            entry.session.title = title;
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
                .get("__source_path")
                .and_then(Value::as_str)
                .unwrap_or(kind.as_str())
                .to_string(),
            source_id: rec.source_id.clone(),
        });
    }
    let mut ordered_sessions: Vec<_> = sessions.into_values().map(|entry| entry.session).collect();
    ordered_sessions.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    batch.sessions.extend(ordered_sessions);
    batch
}

fn load_thread_json(
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

            let Ok(content) = fs::read_to_string(path) else {
                return Vec::new();
            };
            let Ok(val): Result<Value, _> = serde_json::from_str(&content) else {
                return Vec::new();
            };

            let thread_id = parse_thread_id(&val, path);
            let title = val
                .get("title")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| thread_id.clone());
            let thread_ts = extract_timestamp(&val).or_else(|| {
                val.get("created")
                    .or_else(|| val.get("createdAt"))
                    .and_then(extract_timestamp)
            });
            let usage_index = build_usage_ledger_index(&val);

            let messages = val.get("messages").and_then(Value::as_array);
            let Some(messages) = messages else {
                return Vec::new();
            };

            messages
                .iter()
                .enumerate()
                .filter_map(|(idx, message)| {
                    let message_id = parse_message_id(message, idx, &thread_id);
                    let ts = message_timestamp(
                        message,
                        &message_id,
                        idx,
                        thread_ts,
                        file_mtime,
                        &usage_index,
                    );
                    let source_id = format!("{thread_id}:{message_id}");
                    if let Some(ref cur) = parsed_cursor
                        && adapter_common::should_skip(ts, &source_id, cur)
                    {
                        return None;
                    }

                    let mut obj = serde_json::Map::new();
                    if let Some(role) = message.get("role") {
                        obj.insert("role".to_string(), role.clone());
                    }
                    if let Some(content) = message.get("content") {
                        obj.insert("content".to_string(), content.clone());
                    }
                    if let Some(meta) = message.get("meta") {
                        obj.insert("meta".to_string(), meta.clone());
                    }
                    obj.insert("messageId".to_string(), Value::String(message_id));
                    obj.insert("__thread_id".to_string(), Value::String(thread_id.clone()));
                    obj.insert("__thread_title".to_string(), Value::String(title.clone()));
                    if let Some(thread_ts) = thread_ts {
                        obj.insert(
                            "__thread_ts".to_string(),
                            Value::String(thread_ts.to_rfc3339()),
                        );
                    }
                    obj.insert("__source_path".to_string(), Value::String(path.clone()));

                    Some(NativeRecord {
                        source_id,
                        updated_at: ts,
                        payload: Value::Object(obj),
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

fn parse_thread_id(thread: &Value, path: &str) -> String {
    if let Some(id) = thread.get("id")
        && let Some(parsed) = parse_id_value(id)
    {
        return parsed;
    }
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| deterministic_id(&["amp", "thread", path]))
}

fn parse_message_id(message: &Value, idx: usize, thread_id: &str) -> String {
    for key in ["messageId", "id", "uuid"] {
        if let Some(value) = message.get(key)
            && let Some(parsed) = parse_id_value(value)
        {
            return parsed;
        }
    }
    deterministic_id(&[
        "amp",
        "message-fallback",
        thread_id,
        &idx.to_string(),
        &serde_json::to_string(message).unwrap_or_default(),
    ])
}

fn parse_id_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Some(i.to_string());
            }
            if let Some(u) = n.as_u64() {
                return Some(u.to_string());
            }
            n.as_f64().map(float_to_id)
        }
        _ => None,
    }
}

fn float_to_id(value: f64) -> String {
    if value.fract() == 0.0 && value.is_finite() {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn message_timestamp(
    message: &Value,
    message_id: &str,
    idx: usize,
    thread_ts: Option<DateTime<Utc>>,
    file_mtime: Option<DateTime<Utc>>,
    usage_index: &UsageLedgerIndex,
) -> DateTime<Utc> {
    if let Some(ts) = extract_timestamp(message) {
        return ts;
    }

    if let Some(ts) = usage_index.by_message_id.get(message_id) {
        return *ts;
    }
    if let Some(ts) = usage_index.by_index.get(&idx) {
        return *ts;
    }

    if let Some(thread_ts) = thread_ts {
        return thread_ts + Duration::milliseconds(idx as i64);
    }

    if let Some(file_mtime) = file_mtime {
        return file_mtime;
    }

    Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now) + Duration::milliseconds(idx as i64)
}

fn build_usage_ledger_index(thread: &Value) -> UsageLedgerIndex {
    let mut out = UsageLedgerIndex::default();
    let Some(entries) = thread.get("usageLedger").and_then(Value::as_array) else {
        return out;
    };

    for (idx, entry) in entries.iter().enumerate() {
        let Some(ts) = extract_timestamp(entry) else {
            continue;
        };

        let mut inserted_index = false;
        if let Some(position) = entry
            .get("messageIndex")
            .and_then(Value::as_u64)
            .map(|v| v as usize)
        {
            out.by_index.entry(position).or_insert(ts);
            inserted_index = true;
        }
        if !inserted_index {
            out.by_index.entry(idx).or_insert(ts);
        }

        for key in ["messageId", "id", "message_id"] {
            if let Some(value) = entry.get(key)
                && let Some(message_id) = parse_id_value(value)
            {
                out.by_message_id.entry(message_id).or_insert(ts);
            }
        }

        if let Some(message) = entry.get("message") {
            for key in ["messageId", "id", "uuid"] {
                if let Some(value) = message.get(key)
                    && let Some(message_id) = parse_id_value(value)
                {
                    out.by_message_id.entry(message_id).or_insert(ts);
                }
            }
        }
    }

    out
}

fn extract_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(ts) = parse_ts_field(value.get("timestamp")) {
        return Some(ts);
    }
    if let Some(ts) = parse_ts_field(value.get("ts")) {
        return Some(ts);
    }
    if let Some(ts) = parse_ts_field(value.get("sentAt")) {
        return Some(ts);
    }
    if let Some(ts) = value.get("meta").and_then(|meta| {
        parse_ts_field(meta.get("sentAt")).or_else(|| parse_ts_field(meta.get("timestamp")))
    }) {
        return Some(ts);
    }
    if let Some(ts) = parse_ts_field(value.get("created")) {
        return Some(ts);
    }
    if let Some(ts) = parse_ts_field(value.get("createdAt")) {
        return Some(ts);
    }
    if let Some(ts) = value.get("time").and_then(|time| {
        parse_ts_field(time.get("created")).or_else(|| parse_ts_field(time.get("timestamp")))
    }) {
        return Some(ts);
    }
    None
}

fn parse_ts_field(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let value = value?;
    match value {
        Value::String(s) => parse_rfc3339(s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return parse_epoch(i);
            }
            if let Some(u) = n.as_u64() {
                return parse_epoch(u as i64);
            }
            let f = n.as_f64()?;
            if !f.is_finite() {
                return None;
            }
            parse_epoch(f as i64)
        }
        _ => None,
    }
}

fn parse_epoch(epoch: i64) -> Option<DateTime<Utc>> {
    let abs = epoch.unsigned_abs();
    if abs >= 1_000_000_000_000_000_000 {
        let secs = epoch.div_euclid(1_000_000_000);
        let nanos = epoch.rem_euclid(1_000_000_000) as u32;
        return Utc.timestamp_opt(secs, nanos).single();
    }
    if abs >= 1_000_000_000_000_000 {
        let secs = epoch.div_euclid(1_000_000);
        let micros = epoch.rem_euclid(1_000_000) as u32;
        return Utc.timestamp_opt(secs, micros * 1_000).single();
    }
    if abs >= 1_000_000_000_000 {
        return Utc.timestamp_millis_opt(epoch).single();
    }
    Utc.timestamp_opt(epoch, 0).single()
}

fn parse_rfc3339(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_amp_thread_hashes_session_id_and_preserves_source_ref() {
        let ts = Utc::now();
        let payload = serde_json::json!({
            "role": "user",
            "content": [{"text": "hello"}],
            "__thread_id": "T-1",
            "__thread_title": "My Thread",
            "__thread_ts": ts.to_rfc3339(),
            "__source_path": "/tmp/T-1.json"
        });
        let rec = NativeRecord {
            source_id: "T-1:0".to_string(),
            updated_at: ts,
            payload,
        };
        let batch = normalize_records(AgentKind::Amp, &[rec]);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(
            batch.sessions[0].id,
            deterministic_id(&[AgentKind::Amp.as_str(), "session", "T-1"])
        );
        assert_eq!(batch.sessions[0].source_ref, "T-1");
        assert_eq!(batch.messages[0].content, "hello");
    }

    #[test]
    fn parse_message_id_string_number_fallback() {
        let string_id = serde_json::json!({"messageId": "m-1"});
        assert_eq!(parse_message_id(&string_id, 0, "thread"), "m-1");

        let numeric_id = serde_json::json!({"messageId": 42});
        assert_eq!(parse_message_id(&numeric_id, 0, "thread"), "42");

        let fallback = serde_json::json!({"role": "user"});
        let id1 = parse_message_id(&fallback, 2, "thread");
        let id2 = parse_message_id(&fallback, 2, "thread");
        assert_eq!(id1, id2);
        assert_ne!(id1, "2");
    }

    #[test]
    fn timestamp_precedence_message_then_usage_then_thread_then_file() {
        let thread_ts = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .unwrap();
        let file_ts = Utc
            .timestamp_millis_opt(1_700_000_100_000)
            .single()
            .unwrap();

        let explicit = serde_json::json!({"timestamp": "2025-01-01T00:00:00Z"});
        let usage_message = serde_json::json!({"messageId": "m-usage"});
        let thread_only = serde_json::json!({"messageId": "m-thread"});

        let mut usage = UsageLedgerIndex::default();
        usage.by_message_id.insert(
            "m-usage".to_string(),
            Utc.timestamp_millis_opt(1_700_000_050_000)
                .single()
                .unwrap(),
        );

        let ts1 = message_timestamp(
            &explicit,
            "m-explicit",
            0,
            Some(thread_ts),
            Some(file_ts),
            &usage,
        );
        let ts2 = message_timestamp(
            &usage_message,
            "m-usage",
            1,
            Some(thread_ts),
            Some(file_ts),
            &usage,
        );
        let ts3 = message_timestamp(
            &thread_only,
            "m-thread",
            2,
            Some(thread_ts),
            Some(file_ts),
            &usage,
        );
        let ts4 = message_timestamp(&thread_only, "m-file", 0, None, Some(file_ts), &usage);

        assert_eq!(ts1.to_rfc3339(), "2025-01-01T00:00:00+00:00");
        assert_eq!(
            ts2,
            Utc.timestamp_millis_opt(1_700_000_050_000)
                .single()
                .unwrap()
        );
        assert_eq!(ts3, thread_ts + Duration::milliseconds(2));
        assert_eq!(ts4, file_ts);
    }

    #[test]
    fn parse_epoch_handles_negative_nanos_and_micros() {
        let neg_nanos = -1_700_000_000_123_456_789_i64;
        let nanos_ts = parse_epoch(neg_nanos).expect("negative nanos timestamp should parse");
        let expected_nanos = Utc
            .timestamp_opt(
                neg_nanos.div_euclid(1_000_000_000),
                neg_nanos.rem_euclid(1_000_000_000) as u32,
            )
            .single()
            .unwrap();
        assert_eq!(nanos_ts, expected_nanos);

        let neg_micros = -1_700_000_123_456_789_i64;
        let micros_ts = parse_epoch(neg_micros).expect("negative micros timestamp should parse");
        let expected_micros = Utc
            .timestamp_opt(
                neg_micros.div_euclid(1_000_000),
                (neg_micros.rem_euclid(1_000_000) as u32) * 1_000,
            )
            .single()
            .unwrap();
        assert_eq!(micros_ts, expected_micros);
    }

    #[test]
    fn load_thread_json_uses_deterministic_ordering() {
        let dir = std::env::temp_dir().join(format!("remi_amp_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("thread.json");
        std::fs::write(
            &file,
            r#"{
                "id":"thread-a",
                "created":1700000000000,
                "messages":[
                    {"messageId":"b","role":"user","content":"later","timestamp":1700000002000},
                    {"messageId":"a","role":"assistant","content":"earlier","timestamp":1700000001000}
                ]
            }"#,
        )
        .unwrap();

        let records = load_thread_json(&[file.to_string_lossy().to_string()], None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_id, "thread-a:a");
        assert_eq!(records[1].source_id, "thread-a:b");
    }
}
