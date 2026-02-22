use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use chrono::{DateTime, TimeZone, Utc};
use core_model::{
    AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch, deterministic_id,
};
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

pub struct OpenCodeAdapter;

impl AgentAdapter for OpenCodeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::OpenCode
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let sqlite = base.join(".local/share/opencode/opencode.db");
        if sqlite.is_file() {
            return Ok(vec![sqlite.to_string_lossy().to_string()]);
        }

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
        Ok(normalize_records(
            AgentKind::OpenCode,
            records,
            cached_session_meta_index(),
        ))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}

static SESSION_META_INDEX: OnceLock<SessionMetaIndex> = OnceLock::new();

fn cached_session_meta_index() -> &'static SessionMetaIndex {
    SESSION_META_INDEX.get_or_init(load_session_meta_index)
}

fn normalize_records(
    kind: AgentKind,
    records: &[NativeRecord],
    session_meta_index: &SessionMetaIndex,
) -> NormalizedBatch {
    let mut batch = NormalizedBatch::default();
    let mut sessions: HashMap<String, core_model::Session> = HashMap::new();

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

        let source_path = rec.payload.get("__source_path").and_then(Value::as_str);
        let session_key = resolve_session_key_for_payload(
            &rec.payload,
            source_path,
            &rec.source_id,
            session_meta_index,
        );
        let session_meta = session_meta_index.meta_for(&session_key);
        let session_title = rec
            .payload
            .get("__session_title")
            .and_then(Value::as_str)
            .filter(|t| !t.trim().is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| session_meta.map(|meta| meta.title.clone()))
            .unwrap_or_else(|| session_key.clone());

        let session_id = deterministic_id(&[kind.as_str(), "session", &session_key]);
        let message_id = deterministic_id(&[kind.as_str(), "message", &rec.source_id]);

        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(|| core_model::Session {
                id: session_id.clone(),
                agent: kind,
                source_ref: session_key.clone(),
                title: session_title.clone(),
                created_at: session_meta
                    .map(|meta| meta.created_at)
                    .unwrap_or(rec.updated_at),
                updated_at: session_meta
                    .map(|meta| meta.updated_at)
                    .unwrap_or(rec.updated_at),
            });

        if rec.updated_at < entry.created_at {
            entry.created_at = rec.updated_at;
        }
        if rec.updated_at > entry.updated_at {
            entry.updated_at = rec.updated_at;
        }
        if let Some(meta) = session_meta {
            if meta.created_at < entry.created_at {
                entry.created_at = meta.created_at;
            }
            if meta.updated_at > entry.updated_at {
                entry.updated_at = meta.updated_at;
            }
            if (entry.title.is_empty() || entry.title == entry.source_ref)
                && !meta.title.trim().is_empty()
            {
                entry.title = meta.title.clone();
            }
        }
        if (entry.title.is_empty() || entry.title == entry.source_ref)
            && !session_title.trim().is_empty()
            && session_title != session_key
        {
            entry.title = session_title;
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
    batch
}

fn load_message_json(
    source_paths: &[String],
    cursor: Option<&str>,
) -> anyhow::Result<Vec<NativeRecord>> {
    if let Some(db_path) = source_paths
        .iter()
        .find(|path| Path::new(path).extension().and_then(|ext| ext.to_str()) == Some("db"))
    {
        return load_message_sqlite(db_path, cursor);
    }

    let parsed_cursor = cursor.and_then(adapter_common::parse_cursor);
    let session_meta_index = cached_session_meta_index();

    let mut out: Vec<NativeRecord> = source_paths
        .par_iter()
        .filter_map(|path| {
            let file_mtime = adapter_common::file_mtime(path);
            if let Some(ref cur) = parsed_cursor
                && let Some(mtime) = file_mtime
                && mtime <= cur.ts
            {
                return None;
            }

            let content = fs::read_to_string(path).ok()?;
            let mut val: Value = serde_json::from_str(&content).ok()?;
            let ts = extract_ts(&val).or(file_mtime).unwrap_or_else(Utc::now);
            let source_id = val
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| deterministic_id(&["opencode", "message", path]));
            if let Some(ref cur) = parsed_cursor
                && adapter_common::should_skip(ts, &source_id, cur)
            {
                return None;
            }

            let content_text = extract_part_text(&source_id);
            if content_text.is_empty() {
                return None;
            }

            let session_key = resolve_session_key_for_payload(
                &val,
                Some(path.as_str()),
                &source_id,
                session_meta_index,
            );
            let session_title = session_meta_index
                .meta_for(&session_key)
                .map(|meta| meta.title.clone())
                .unwrap_or_else(|| session_key.clone());

            if let Some(obj) = val.as_object_mut() {
                obj.insert("__source_path".to_string(), Value::String(path.clone()));
                obj.insert("__content".to_string(), Value::String(content_text));
                obj.insert("__session_key".to_string(), Value::String(session_key));
                obj.insert("__session_title".to_string(), Value::String(session_title));
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

fn load_message_sqlite(db_path: &str, cursor: Option<&str>) -> anyhow::Result<Vec<NativeRecord>> {
    if !Path::new(db_path).is_file() {
        return Ok(Vec::new());
    }

    let parsed_cursor = cursor.and_then(adapter_common::parse_cursor);
    let connection = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let mut out = Vec::new();
    let mut message_stmt = connection.prepare(
        "SELECT m.id, m.session_id, m.time_created, m.time_updated, m.data, \
                s.title, s.directory \
         FROM message m \
         JOIN session s ON s.id = m.session_id \
         ORDER BY m.time_updated ASC, m.id ASC",
    )?;
    let message_rows = message_stmt.query_map([], |row| {
        let message_id: String = row.get(0)?;
        let session_id: String = row.get(1)?;
        let created_ms: i64 = row.get(2)?;
        let updated_ms: i64 = row.get(3)?;
        let data_json: String = row.get(4)?;
        let title: String = row.get(5)?;
        let directory: String = row.get(6)?;
        Ok((
            message_id, session_id, created_ms, updated_ms, data_json, title, directory,
        ))
    })?;

    let mut content_by_message: HashMap<String, String> = HashMap::new();
    let mut part_stmt = connection.prepare(
        "SELECT p.message_id, p.id, p.data \
         FROM part p \
         ORDER BY p.message_id ASC, p.time_created ASC, p.id ASC",
    )?;
    let part_rows = part_stmt.query_map([], |row| {
        let message_id: String = row.get(0)?;
        let _part_id: String = row.get(1)?;
        let data_json: String = row.get(2)?;
        Ok((message_id, data_json))
    })?;

    for row in part_rows.flatten() {
        let (message_id, data_json) = row;
        let Ok(value): Result<Value, _> = serde_json::from_str(&data_json) else {
            continue;
        };
        let Some(text) = extract_sqlite_part_text(&value) else {
            continue;
        };

        let entry = content_by_message.entry(message_id).or_default();
        if !entry.is_empty() {
            entry.push('\n');
        }
        entry.push_str(&text);
    }

    for row in message_rows.flatten() {
        let (message_id, session_id, created_ms, updated_ms, data_json, title, directory) = row;
        let updated_at = Utc
            .timestamp_millis_opt(updated_ms)
            .single()
            .or_else(|| Utc.timestamp_millis_opt(created_ms).single())
            .unwrap_or_else(Utc::now);
        if let Some(ref cur) = parsed_cursor
            && adapter_common::should_skip(updated_at, &message_id, cur)
        {
            continue;
        }

        let mut payload = match serde_json::from_str::<Value>(&data_json) {
            Ok(Value::Object(obj)) => Value::Object(obj),
            _ => Value::Object(serde_json::Map::new()),
        };

        if let Some(obj) = payload.as_object_mut() {
            obj.insert("id".to_string(), Value::String(message_id.clone()));
            obj.insert("sessionId".to_string(), Value::String(session_id.clone()));
            obj.insert(
                "timestamp".to_string(),
                Value::Number(serde_json::Number::from(updated_ms)),
            );
            obj.insert(
                "__source_path".to_string(),
                Value::String(directory.clone()),
            );
            obj.insert(
                "__content".to_string(),
                Value::String(content_by_message.remove(&message_id).unwrap_or_default()),
            );
            obj.insert("__session_key".to_string(), Value::String(session_id));
            obj.insert("__session_title".to_string(), Value::String(title));
            obj.insert(
                "__storage_db_path".to_string(),
                Value::String(db_path.to_string()),
            );
        }

        out.push(NativeRecord {
            source_id: message_id,
            updated_at,
            payload,
        });
    }

    out.sort_by(|a, b| {
        a.updated_at
            .cmp(&b.updated_at)
            .then_with(|| a.source_id.cmp(&b.source_id))
    });
    Ok(out)
}

fn extract_sqlite_part_text(part: &Value) -> Option<String> {
    if part.get("type").and_then(Value::as_str) == Some("tool") {
        return extract_sqlite_tool_part_text(part);
    }
    let text = adapter_common::extract_content_text(Some(part));
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn extract_sqlite_tool_part_text(part: &Value) -> Option<String> {
    let tool = part
        .get("tool")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("tool");
    let state = part.get("state")?;

    let mut lines = Vec::new();
    if let Some(input) = state.get("input").and_then(format_tool_payload) {
        lines.push(format!("tool_use: {tool} {input}"));
    } else {
        lines.push(format!("tool_use: {tool}"));
    }

    match state.get("status").and_then(Value::as_str) {
        Some("completed") => {
            if let Some(output) = extract_tool_output_text(state) {
                lines.push(format!("tool_result: {output}"));
            }
        }
        Some("error") => {
            if let Some(error) = state
                .get("error")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
            {
                lines.push(format!("tool_result: {error}"));
            }
        }
        _ => {}
    }

    Some(lines.join("\n"))
}

fn extract_tool_output_text(state: &Value) -> Option<String> {
    if let Some(output) = state.get("output") {
        let text = adapter_common::extract_content_text(Some(output));
        if !text.trim().is_empty() {
            return Some(text);
        }
        if let Some(fallback) = format_tool_payload(output) {
            return Some(fallback);
        }
    }
    None
}

fn format_tool_payload(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        }
        _ => serde_json::to_string(value)
            .ok()
            .filter(|s| s != "null" && s != "{}" && s != "[]"),
    }
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

fn resolve_session_key_for_payload(
    payload: &Value,
    source_path: Option<&str>,
    source_id: &str,
    session_meta_index: &SessionMetaIndex,
) -> String {
    for candidate in session_candidates(payload, source_path) {
        if let Some(key) = session_meta_index.resolve_key(&candidate) {
            return key;
        }
        if let Some(candidate) = normalize_session_key(&candidate) {
            return candidate;
        }
    }

    fallback_session_key_from_path(source_path)
        .or_else(|| session_meta_index.resolve_key(source_id))
        .unwrap_or_else(|| "message-root".to_string())
}

fn session_candidates(payload: &Value, source_path: Option<&str>) -> Vec<String> {
    let mut out = Vec::new();

    if let Some(candidate) = payload.get("__session_key").and_then(Value::as_str) {
        out.push(candidate.to_string());
    }

    for key in ["sessionID", "sessionId", "session_id", "conversationId"] {
        if let Some(candidate) = payload.get(key).and_then(Value::as_str) {
            out.push(candidate.to_string());
        }
    }

    if let Some(session) = payload.get("session") {
        if let Some(session_str) = session.as_str() {
            out.push(session_str.to_string());
        }
        for key in ["id", "sessionID", "sessionId"] {
            if let Some(candidate) = session.get(key).and_then(Value::as_str) {
                out.push(candidate.to_string());
            }
        }
    }

    if let Some(metadata) = payload.get("metadata") {
        for key in ["sessionID", "sessionId", "conversationId"] {
            if let Some(candidate) = metadata.get(key).and_then(Value::as_str) {
                out.push(candidate.to_string());
            }
        }
    }

    if let Some(path_key) = fallback_session_key_from_path(source_path) {
        out.push(path_key);
    }

    out
}

fn fallback_session_key_from_path(source_path: Option<&str>) -> Option<String> {
    let path = Path::new(source_path?);
    let components: Vec<String> = path
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .map(ToOwned::to_owned)
        .collect();

    if let Some(position) = components.iter().position(|c| c == "message") {
        let group_index = position + 1;
        if group_index < components.len().saturating_sub(1)
            && let Some(candidate) = normalize_session_key(&components[group_index])
        {
            return Some(candidate);
        }
        return Some("message-root".to_string());
    }

    if let Some(parent) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        && parent != "message"
        && parent != "storage"
        && let Some(candidate) = normalize_session_key(parent)
    {
        return Some(candidate);
    }

    Some("message-root".to_string())
}

fn normalize_session_key(candidate: &str) -> Option<String> {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
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

#[derive(Clone)]
struct SessionMeta {
    title: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Default)]
struct SessionMetaIndex {
    by_key: HashMap<String, SessionMeta>,
    alias_to_key: HashMap<String, String>,
}

impl SessionMetaIndex {
    fn resolve_key(&self, candidate: &str) -> Option<String> {
        let normalized = normalize_session_key(candidate)?;
        if let Some(key) = self.alias_to_key.get(&normalized) {
            return Some(key.clone());
        }
        if self.by_key.contains_key(&normalized) {
            return Some(normalized);
        }
        None
    }

    fn meta_for(&self, key: &str) -> Option<&SessionMeta> {
        let resolved = self.resolve_key(key)?;
        self.by_key.get(&resolved)
    }

    fn upsert(&mut self, canonical: String, aliases: Vec<String>, meta: SessionMeta) {
        self.by_key
            .entry(canonical.clone())
            .and_modify(|existing| {
                if meta.created_at < existing.created_at {
                    existing.created_at = meta.created_at;
                }
                if meta.updated_at > existing.updated_at {
                    existing.updated_at = meta.updated_at;
                }
                if (existing.title.is_empty() || existing.title == canonical)
                    && !meta.title.trim().is_empty()
                {
                    existing.title = meta.title.clone();
                }
            })
            .or_insert(meta);

        self.alias_to_key
            .insert(canonical.clone(), canonical.clone());
        for alias in aliases {
            if let Some(alias) = normalize_session_key(&alias) {
                self.alias_to_key.insert(alias, canonical.clone());
            }
        }
    }
}

fn load_session_meta_index() -> SessionMetaIndex {
    let mut out = SessionMetaIndex::default();
    let Some(home) = dirs::home_dir() else {
        return out;
    };
    let root = home.join(".local/share/opencode/storage/session");
    let mut stack = vec![root];

    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(content) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(val): Result<Value, _> = serde_json::from_str(&content) else {
                continue;
            };

            let mut aliases = vec![name.to_string()];
            for key in ["id", "sessionID", "sessionId", "uuid"] {
                if let Some(value) = val.get(key).and_then(Value::as_str) {
                    aliases.push(value.to_string());
                }
            }

            let canonical = aliases
                .iter()
                .find_map(|candidate| normalize_session_key(candidate))
                .unwrap_or_else(|| name.to_string());
            let title = val
                .get("title")
                .or_else(|| val.get("name"))
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(&canonical)
                .to_string();
            let created_at = val
                .get("time")
                .and_then(|t| t.get("created"))
                .and_then(Value::as_i64)
                .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                .unwrap_or_else(Utc::now);
            let updated_at = val
                .get("time")
                .and_then(|t| t.get("updated"))
                .and_then(Value::as_i64)
                .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                .unwrap_or(created_at);

            out.upsert(
                canonical,
                aliases,
                SessionMeta {
                    title,
                    created_at,
                    updated_at,
                },
            );
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_db_path() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("remi_opencode_test_{}_{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("opencode.db")
    }

    fn create_test_sqlite(db_path: &std::path::Path) {
        let conn = Connection::open(db_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                parent_id TEXT,
                slug TEXT NOT NULL,
                directory TEXT NOT NULL,
                title TEXT NOT NULL,
                version TEXT NOT NULL,
                share_url TEXT,
                summary_additions INTEGER,
                summary_deletions INTEGER,
                summary_files INTEGER,
                summary_diffs TEXT,
                revert TEXT,
                permission TEXT,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                time_compacting INTEGER,
                time_archived INTEGER
            );
            CREATE TABLE message (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE part (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );",
        )
        .expect("create schema");

        conn.execute(
            "INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived)
             VALUES (?1, 'project-1', NULL, 'slug', '/worktree', ?2, 'v2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?3, ?4, NULL, NULL)",
            params!["session-1", "Session Title", 1_700_000_000_000_i64, 1_700_000_000_500_i64],
        )
        .expect("insert session");

        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "msg-1",
                "session-1",
                1_700_000_000_100_i64,
                1_700_000_000_200_i64,
                r#"{"role":"assistant"}"#
            ],
        )
        .expect("insert message");

        conn.execute(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "part-1",
                "msg-1",
                "session-1",
                1_700_000_000_150_i64,
                1_700_000_000_150_i64,
                r#"{"type":"text","text":"hello from sqlite"}"#
            ],
        )
        .expect("insert part");
    }

    #[test]
    fn resolve_session_key_prefers_meta_aliases() {
        let now = Utc::now();
        let mut index = SessionMetaIndex::default();
        index.upsert(
            "canonical-session".to_string(),
            vec!["legacy-session".to_string()],
            SessionMeta {
                title: "Session Title".to_string(),
                created_at: now,
                updated_at: now,
            },
        );

        let payload = serde_json::json!({"sessionId":"legacy-session"});
        let key = resolve_session_key_for_payload(
            &payload,
            Some("/tmp/message/canonical-session/msg.json"),
            "msg-1",
            &index,
        );
        assert_eq!(key, "canonical-session");
    }

    #[test]
    fn fallback_session_key_groups_by_path_segment() {
        let index = SessionMetaIndex::default();
        let payload = serde_json::json!({});

        let key1 = resolve_session_key_for_payload(
            &payload,
            Some("/tmp/storage/message/session-1/a.json"),
            "msg-a",
            &index,
        );
        let key2 = resolve_session_key_for_payload(
            &payload,
            Some("/tmp/storage/message/session-1/b.json"),
            "msg-b",
            &index,
        );
        assert_eq!(key1, "session-1");
        assert_eq!(key1, key2);
    }

    #[test]
    fn normalize_prefers_embedded_session_key() {
        let now = Utc::now();
        let index = SessionMetaIndex::default();
        let records = vec![NativeRecord {
            source_id: "msg-a".to_string(),
            updated_at: now,
            payload: serde_json::json!({
                "role": "user",
                "__content": "hello",
                "__session_key": "canonical-from-scan",
                "sessionId": "legacy-id",
                "__source_path": "/tmp/storage/message/path-derived/1.json"
            }),
        }];

        let batch = normalize_records(AgentKind::OpenCode, &records, &index);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.sessions[0].source_ref, "canonical-from-scan");
    }

    #[test]
    fn normalize_records_groups_under_canonical_session() {
        let now = Utc::now();
        let mut index = SessionMetaIndex::default();
        index.upsert(
            "canonical-session".to_string(),
            vec!["legacy-session".to_string()],
            SessionMeta {
                title: "Canonical".to_string(),
                created_at: now,
                updated_at: now,
            },
        );

        let records = vec![
            NativeRecord {
                source_id: "m1".to_string(),
                updated_at: now,
                payload: serde_json::json!({
                    "role": "user",
                    "__content": "hello",
                    "__session_key": "legacy-session",
                    "__source_path": "/tmp/storage/message/canonical-session/1.json"
                }),
            },
            NativeRecord {
                source_id: "m2".to_string(),
                updated_at: now,
                payload: serde_json::json!({
                    "role": "assistant",
                    "__content": "world",
                    "sessionId": "canonical-session",
                    "__source_path": "/tmp/storage/message/canonical-session/2.json"
                }),
            },
        ];

        let batch = normalize_records(AgentKind::OpenCode, &records, &index);
        assert_eq!(batch.sessions.len(), 1);
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.sessions[0].source_ref, "canonical-session");
    }

    #[test]
    fn load_message_sqlite_reads_messages_and_parts() {
        let db_path = temp_db_path();
        create_test_sqlite(&db_path);

        let records = load_message_sqlite(&db_path.to_string_lossy(), None)
            .expect("sqlite records should load");
        assert_eq!(records.len(), 1);
        let payload = &records[0].payload;
        assert_eq!(records[0].source_id, "msg-1");
        assert_eq!(
            payload.get("role").and_then(Value::as_str),
            Some("assistant")
        );
        assert_eq!(
            payload.get("__session_key").and_then(Value::as_str),
            Some("session-1")
        );
        assert_eq!(
            payload.get("__session_title").and_then(Value::as_str),
            Some("Session Title")
        );
        assert_eq!(
            payload.get("__content").and_then(Value::as_str),
            Some("hello from sqlite")
        );
    }

    #[test]
    fn load_message_json_dispatches_to_sqlite_when_db_path_present() {
        let db_path = temp_db_path();
        create_test_sqlite(&db_path);

        let records = load_message_json(&[db_path.to_string_lossy().to_string()], None)
            .expect("load_message_json should read sqlite");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_id, "msg-1");
    }

    #[test]
    fn load_message_sqlite_emits_tool_markers_from_tool_parts() {
        let db_path = temp_db_path();
        let conn = Connection::open(&db_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                parent_id TEXT,
                slug TEXT NOT NULL,
                directory TEXT NOT NULL,
                title TEXT NOT NULL,
                version TEXT NOT NULL,
                share_url TEXT,
                summary_additions INTEGER,
                summary_deletions INTEGER,
                summary_files INTEGER,
                summary_diffs TEXT,
                revert TEXT,
                permission TEXT,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                time_compacting INTEGER,
                time_archived INTEGER
            );
            CREATE TABLE message (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE part (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            );",
        )
        .expect("create schema");

        conn.execute(
            "INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, revert, permission, time_created, time_updated, time_compacting, time_archived)
             VALUES ('session-1', 'project-1', NULL, 'slug', '/worktree', 'Session Title', 'v2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1700000000000, 1700000000500, NULL, NULL)",
            [],
        )
        .expect("insert session");
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data)
             VALUES ('msg-tool', 'session-1', 1700000000100, 1700000000200, '{\"role\":\"assistant\"}')",
            [],
        )
        .expect("insert message");
        conn.execute(
            "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
             VALUES ('part-tool', 'msg-tool', 'session-1', 1700000000150, 1700000000150, ?1)",
            params![
                r#"{"type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"pwd"},"output":"/tmp"}}"#
            ],
        )
        .expect("insert tool part");

        let records = load_message_sqlite(&db_path.to_string_lossy(), None).expect("load sqlite");
        assert_eq!(records.len(), 1);
        let content = records[0]
            .payload
            .get("__content")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(content.contains("tool_use: bash {\"command\":\"pwd\"}"));
        assert!(content.contains("tool_result: /tmp"));
    }
}
