use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use core_model::{
    ArchiveItem, ArchiveRun, Checkpoint, Message, NormalizedBatch, Provenance, Session,
    deterministic_id,
};
use rusqlite::{Connection, OptionalExtension, params};

pub struct SqliteStore {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct SearchRow {
    pub message_id: String,
    pub session_id: String,
    pub content: String,
    pub ts: DateTime<Utc>,
    pub score: f64,
}

impl SqliteStore {
    pub fn open_default() -> anyhow::Result<Self> {
        let base = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
        let path = base.join("remi").join("remi.db");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating parent dir for {}", path.display()))?;
        }
        Self::open(path)
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = Connection::open(path.as_ref())
            .with_context(|| format!("opening sqlite db {}", path.as_ref().display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
        )?;
        Ok(Self { conn })
    }

    pub fn init_schema(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS agents (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS sessions (
              id TEXT PRIMARY KEY,
              agent TEXT NOT NULL,
              source_ref TEXT NOT NULL,
              title TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
              id TEXT PRIMARY KEY,
              session_id TEXT NOT NULL,
              role TEXT NOT NULL,
              content TEXT NOT NULL,
              ts TEXT NOT NULL,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS events (
              id TEXT PRIMARY KEY,
              session_id TEXT NOT NULL,
              kind TEXT NOT NULL,
              payload TEXT NOT NULL,
              ts TEXT NOT NULL,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS artifacts (
              id TEXT PRIMARY KEY,
              session_id TEXT NOT NULL,
              path TEXT NOT NULL,
              checksum TEXT NOT NULL,
              metadata TEXT NOT NULL,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS provenance (
              id TEXT PRIMARY KEY,
              entity_type TEXT NOT NULL,
              entity_id TEXT NOT NULL,
              agent TEXT NOT NULL,
              source_path TEXT NOT NULL,
              source_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
              agent TEXT PRIMARY KEY,
              cursor TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_runs (
              id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              older_than_secs INTEGER NOT NULL,
              keep_latest INTEGER NOT NULL,
              dry_run INTEGER NOT NULL,
              executed INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_items (
              id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              planned_delete INTEGER NOT NULL,
              FOREIGN KEY(run_id) REFERENCES archive_runs(id) ON DELETE CASCADE,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(
              message_id UNINDEXED,
              session_id UNINDEXED,
              content,
              ts UNINDEXED,
              tokenize = 'unicode61 tokenchars ''_./:-'''
            );
            "#,
        )?;
        for (id, name) in [
            ("pi", "pi"),
            ("droid", "droid"),
            ("opencode", "opencode"),
            ("claude", "claude"),
        ] {
            self.conn.execute(
                "INSERT OR IGNORE INTO agents (id, name) VALUES (?1, ?2)",
                params![id, name],
            )?;
        }
        Ok(())
    }

    pub fn save_batch(&mut self, batch: &NormalizedBatch) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt_session = tx.prepare_cached(
                r#"INSERT INTO sessions (id, agent, source_ref, title, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(id) DO UPDATE SET
                  agent=excluded.agent,
                  source_ref=excluded.source_ref,
                  title=excluded.title,
                  updated_at=excluded.updated_at"#,
            )?;
            for s in &batch.sessions {
                stmt_session.execute(params![
                    s.id,
                    s.agent.as_str(),
                    s.source_ref,
                    s.title,
                    s.created_at.to_rfc3339(),
                    s.updated_at.to_rfc3339()
                ])?;
            }
        }
        {
            let mut stmt_msg = tx.prepare_cached(
                r#"INSERT INTO messages (id, session_id, role, content, ts)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(id) DO UPDATE SET
                  role=excluded.role,
                  content=excluded.content,
                  ts=excluded.ts"#,
            )?;
            for m in &batch.messages {
                stmt_msg.execute(params![m.id, m.session_id, m.role, m.content, m.ts.to_rfc3339()])?;
            }
        }
        {
            let mut stmt_fts_del = tx.prepare_cached(
                "DELETE FROM fts_messages WHERE message_id = ?1",
            )?;
            let mut stmt_fts_ins = tx.prepare_cached(
                "INSERT INTO fts_messages (message_id, session_id, content, ts) VALUES (?1, ?2, ?3, ?4)",
            )?;
            for m in &batch.messages {
                stmt_fts_del.execute(params![m.id])?;
                stmt_fts_ins.execute(params![m.id, m.session_id, m.content, m.ts.to_rfc3339()])?;
            }
        }
        {
            let mut stmt_event = tx.prepare_cached(
                r#"INSERT INTO events (id, session_id, kind, payload, ts)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(id) DO UPDATE SET kind=excluded.kind, payload=excluded.payload, ts=excluded.ts"#,
            )?;
            for e in &batch.events {
                stmt_event.execute(params![
                    e.id,
                    e.session_id,
                    e.kind,
                    serde_json::to_string(&e.payload)?,
                    e.ts.to_rfc3339()
                ])?;
            }
        }
        {
            let mut stmt_artifact = tx.prepare_cached(
                r#"INSERT INTO artifacts (id, session_id, path, checksum, metadata)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(id) DO UPDATE SET path=excluded.path, checksum=excluded.checksum, metadata=excluded.metadata"#,
            )?;
            for a in &batch.artifacts {
                stmt_artifact.execute(params![
                    a.id,
                    a.session_id,
                    a.path,
                    a.checksum,
                    serde_json::to_string(&a.metadata)?
                ])?;
            }
        }
        {
            let mut stmt_prov = tx.prepare_cached(
                r#"INSERT INTO provenance (id, entity_type, entity_id, agent, source_path, source_id)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(id) DO UPDATE SET source_path=excluded.source_path"#,
            )?;
            for p in &batch.provenance {
                stmt_prov.execute(params![
                    p.id,
                    p.entity_type,
                    p.entity_id,
                    p.agent.as_str(),
                    p.source_path,
                    p.source_id
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_checkpoint(&self, agent: &str) -> anyhow::Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT cursor FROM checkpoints WHERE agent = ?1",
                params![agent],
                |r| r.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        self.conn.execute(
            r#"INSERT INTO checkpoints (agent, cursor, updated_at) VALUES (?1, ?2, ?3)
            ON CONFLICT(agent) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at"#,
            params![
                checkpoint.agent.as_str(),
                checkpoint.cursor,
                checkpoint.updated_at.to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, agent, source_ref, title, created_at, updated_at FROM sessions ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            let agent_str: String = r.get(1)?;
            Ok(Session {
                id: r.get(0)?,
                agent: parse_agent(&agent_str),
                source_ref: r.get(2)?,
                title: r.get(3)?,
                created_at: parse_ts(r.get(4)?),
                updated_at: parse_ts(r.get(5)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_session_messages(&self, session_id: &str) -> anyhow::Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, role, content, ts FROM messages WHERE session_id = ?1 ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(params![session_id], |r| {
            Ok(Message {
                id: r.get(0)?,
                session_id: r.get(1)?,
                role: r.get(2)?,
                content: r.get(3)?,
                ts: parse_ts(r.get(4)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_session(&self, session_id: &str) -> anyhow::Result<Option<Session>> {
        self.conn
            .query_row(
                "SELECT id, agent, source_ref, title, created_at, updated_at FROM sessions WHERE id = ?1",
                params![session_id],
                |r| {
                    let agent_str: String = r.get(1)?;
                    Ok(Session {
                        id: r.get(0)?,
                        agent: parse_agent(&agent_str),
                        source_ref: r.get(2)?,
                        title: r.get(3)?,
                        created_at: parse_ts(r.get(4)?),
                        updated_at: parse_ts(r.get(5)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_provenance_for_session(&self, session_id: &str) -> anyhow::Result<Vec<Provenance>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.id, p.entity_type, p.entity_id, p.agent, p.source_path, p.source_id FROM provenance p INNER JOIN messages m ON p.entity_id = m.id WHERE m.session_id = ?1",
        )?;
        let rows = stmt.query_map(params![session_id], |r| {
            let agent_str: String = r.get(3)?;
            Ok(Provenance {
                id: r.get(0)?,
                entity_type: r.get(1)?,
                entity_id: r.get(2)?,
                agent: parse_agent(&agent_str),
                source_path: r.get(4)?,
                source_id: r.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn search_lexical(&self, query: &str, limit: i64) -> anyhow::Result<Vec<SearchRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT message_id, session_id, content, ts, bm25(fts_messages) AS rank FROM fts_messages WHERE fts_messages MATCH ?1 ORDER BY rank LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![query, limit], |r| {
            let rank: f64 = r.get(4)?;
            Ok(SearchRow {
                message_id: r.get(0)?,
                session_id: r.get(1)?,
                content: r.get(2)?,
                ts: parse_ts(r.get(3)?),
                score: -rank,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn recent_messages(&self, limit: i64) -> anyhow::Result<Vec<SearchRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.session_id, m.content, m.ts FROM messages m ORDER BY m.ts DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |r| {
            Ok(SearchRow {
                message_id: r.get(0)?,
                session_id: r.get(1)?,
                content: r.get(2)?,
                ts: parse_ts(r.get(3)?),
                score: 0.0,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn search_substring(&self, query: &str, limit: i64) -> anyhow::Result<Vec<SearchRow>> {
        let pattern = format!("%{}%", query.to_lowercase());
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.session_id, m.content, m.ts FROM messages m WHERE lower(m.content) LIKE ?1 ORDER BY m.ts DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![pattern, limit], |r| {
            Ok(SearchRow {
                message_id: r.get(0)?,
                session_id: r.get(1)?,
                content: r.get(2)?,
                ts: parse_ts(r.get(3)?),
                score: 0.0,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn plan_archive(
        &self,
        older_than: Duration,
        keep_latest: usize,
    ) -> anyhow::Result<ArchiveRun> {
        let now = Utc::now();
        let run_id = deterministic_id(&[
            "archive_run",
            &now.timestamp_nanos_opt().unwrap_or_default().to_string(),
            &older_than.num_seconds().to_string(),
            &keep_latest.to_string(),
        ]);
        let cutoff = now - older_than;
        self.conn.execute(
            "INSERT INTO archive_runs (id, created_at, older_than_secs, keep_latest, dry_run, executed) VALUES (?1, ?2, ?3, ?4, 1, 0)",
            params![run_id, now.to_rfc3339(), older_than.num_seconds(), keep_latest as i64],
        )?;

        let sessions = self.list_sessions()?;
        let mut by_agent: std::collections::HashMap<&str, Vec<Session>> =
            std::collections::HashMap::new();
        for s in sessions {
            by_agent.entry(s.agent.as_str()).or_default().push(s);
        }
        for grouped in by_agent.values_mut() {
            grouped.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
            for s in grouped.iter().skip(keep_latest) {
                if s.updated_at < cutoff {
                    let already_planned: bool = self.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM archive_items ai JOIN archive_runs ar ON ai.run_id = ar.id WHERE ai.session_id = ?1 AND ar.executed = 0)",
                        params![s.id],
                        |r| r.get(0),
                    )?;
                    if already_planned {
                        continue;
                    }
                    let item_id = deterministic_id(&[&run_id, &s.id]);
                    self.conn.execute(
                        "INSERT INTO archive_items (id, run_id, session_id, planned_delete) VALUES (?1, ?2, ?3, 1)",
                        params![item_id, run_id, s.id],
                    )?;
                }
            }
        }

        Ok(ArchiveRun {
            id: run_id,
            created_at: now,
            older_than_secs: older_than.num_seconds(),
            keep_latest: keep_latest as i64,
            dry_run: true,
            executed: false,
        })
    }

    pub fn archive_items_for_run(&self, run_id: &str) -> anyhow::Result<Vec<ArchiveItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, run_id, session_id, planned_delete FROM archive_items WHERE run_id = ?1",
        )?;
        let rows = stmt.query_map(params![run_id], |r| {
            Ok(ArchiveItem {
                id: r.get(0)?,
                run_id: r.get(1)?,
                session_id: r.get(2)?,
                planned_delete: r.get::<_, i64>(3)? == 1,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn mark_archive_executed(&self, run_id: &str, dry_run: bool) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE archive_runs SET dry_run = ?2, executed = 1 WHERE id = ?1",
            params![run_id, if dry_run { 1 } else { 0 }],
        )?;
        Ok(())
    }

    pub fn delete_session_cascade(&self, session_id: &str) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
        self.conn.execute(
            "DELETE FROM fts_messages WHERE session_id = ?1",
            params![session_id],
        )?;
        Ok(())
    }

    pub fn integrity_check(&self) -> anyhow::Result<String> {
        self.conn
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .map_err(Into::into)
    }
}

fn parse_ts(ts: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&ts)
        .map(|v| v.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_agent(s: &str) -> core_model::AgentKind {
    match s {
        "pi" => core_model::AgentKind::Pi,
        "droid" => core_model::AgentKind::Droid,
        "opencode" => core_model::AgentKind::OpenCode,
        "claude" => core_model::AgentKind::Claude,
        _ => core_model::AgentKind::OpenCode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_model::AgentKind;

    #[test]
    fn schema_and_integrity() {
        let mut store = SqliteStore::open(":memory:").expect("open");
        store.init_schema().expect("schema");
        let check = store.integrity_check().expect("integrity");
        assert_eq!(check, "ok");

        let batch = NormalizedBatch::default();
        store.save_batch(&batch).expect("empty batch is fine");
    }

    fn make_batch(agent: AgentKind, session_id: &str, msg_id: &str, content: &str) -> NormalizedBatch {
        let now = Utc::now();
        NormalizedBatch {
            sessions: vec![Session {
                id: session_id.to_string(),
                agent,
                source_ref: "test-ref".to_string(),
                title: "test session".to_string(),
                created_at: now,
                updated_at: now,
            }],
            messages: vec![Message {
                id: msg_id.to_string(),
                session_id: session_id.to_string(),
                role: "user".to_string(),
                content: content.to_string(),
                ts: now,
            }],
            events: vec![],
            artifacts: vec![],
            provenance: vec![Provenance {
                id: format!("prov_{}", msg_id),
                entity_type: "message".to_string(),
                entity_id: msg_id.to_string(),
                agent,
                source_path: "/test/path".to_string(),
                source_id: "src-1".to_string(),
            }],
        }
    }

    #[test]
    fn agents_populated() {
        let store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 4);
    }

    #[test]
    fn save_and_list_sessions() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "hello world");
        store.save_batch(&batch).unwrap();
        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "s1");
        assert_eq!(sessions[0].agent, core_model::AgentKind::Pi);
    }

    #[test]
    fn save_batch_idempotent() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "hello");
        store.save_batch(&batch).unwrap();
        store.save_batch(&batch).unwrap();
        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        let msgs = store.get_session_messages("s1").unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn get_session_and_messages() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Droid, "s2", "m2", "test content");
        store.save_batch(&batch).unwrap();
        let session = store.get_session("s2").unwrap();
        assert!(session.is_some());
        assert_eq!(session.unwrap().title, "test session");
        let missing = store.get_session("nonexistent").unwrap();
        assert!(missing.is_none());
        let msgs = store.get_session_messages("s2").unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].content, "test content");
    }

    #[test]
    fn provenance_for_session() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Claude, "s3", "m3", "prov test");
        store.save_batch(&batch).unwrap();
        let prov = store.get_provenance_for_session("s3").unwrap();
        assert_eq!(prov.len(), 1);
        assert_eq!(prov[0].entity_id, "m3");
        assert_eq!(prov[0].source_path, "/test/path");
    }

    #[test]
    fn checkpoint_upsert_and_get() {
        let store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        assert!(store.get_checkpoint("pi").unwrap().is_none());
        let cp = Checkpoint {
            agent: core_model::AgentKind::Pi,
            cursor: "2025-01-01T00:00:00+00:00".to_string(),
            updated_at: Utc::now(),
        };
        store.upsert_checkpoint(&cp).unwrap();
        let got = store.get_checkpoint("pi").unwrap().unwrap();
        assert_eq!(got, "2025-01-01T00:00:00+00:00");
        let cp2 = Checkpoint {
            agent: core_model::AgentKind::Pi,
            cursor: "2025-06-01T00:00:00+00:00".to_string(),
            updated_at: Utc::now(),
        };
        store.upsert_checkpoint(&cp2).unwrap();
        let got2 = store.get_checkpoint("pi").unwrap().unwrap();
        assert_eq!(got2, "2025-06-01T00:00:00+00:00");
    }

    #[test]
    fn fts_search() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "rust programming language");
        store.save_batch(&batch).unwrap();
        let results = store.search_lexical("rust", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message_id, "m1");
        assert!(results[0].score > 0.0, "BM25 score should be positive");
        let empty = store.search_lexical("python", 10).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn delete_session_cascade() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "cascade test");
        store.save_batch(&batch).unwrap();
        assert_eq!(store.list_sessions().unwrap().len(), 1);
        assert_eq!(store.get_session_messages("s1").unwrap().len(), 1);
        store.delete_session_cascade("s1").unwrap();
        assert!(store.list_sessions().unwrap().is_empty());
        assert!(store.get_session_messages("s1").unwrap().is_empty());
        let fts = store.search_lexical("cascade", 10).unwrap();
        assert!(fts.is_empty());
    }

    #[test]
    fn archive_plan_and_idempotency() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let old_time = Utc::now() - Duration::days(60);
        let mut batch = NormalizedBatch::default();
        for i in 0..5 {
            batch.sessions.push(Session {
                id: format!("s{}", i),
                agent: core_model::AgentKind::Pi,
                source_ref: format!("ref{}", i),
                title: format!("session {}", i),
                created_at: old_time,
                updated_at: old_time,
            });
        }
        store.save_batch(&batch).unwrap();
        let run1 = store
            .plan_archive(Duration::days(30), 2)
            .unwrap();
        let items1 = store.archive_items_for_run(&run1.id).unwrap();
        assert_eq!(items1.len(), 3);
        let run2 = store
            .plan_archive(Duration::days(30), 2)
            .unwrap();
        let items2 = store.archive_items_for_run(&run2.id).unwrap();
        assert_eq!(items2.len(), 0, "idempotency: already-planned sessions should be skipped");
    }

    #[test]
    fn init_schema_idempotent() {
        let store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        store.init_schema().unwrap();
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 4);
    }

    #[test]
    fn recent_messages_ordering() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "first message");
        store.save_batch(&batch).unwrap();
        let batch2 = make_batch(core_model::AgentKind::Pi, "s2", "m2", "second message");
        store.save_batch(&batch2).unwrap();
        let recent = store.recent_messages(10).unwrap();
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn substring_search() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let batch = make_batch(core_model::AgentKind::Pi, "s1", "m1", "hello_world function");
        store.save_batch(&batch).unwrap();
        let results = store.search_substring("hello_world", 10).unwrap();
        assert_eq!(results.len(), 1);
        let empty = store.search_substring("nonexistent", 10).unwrap();
        assert!(empty.is_empty());
    }
}
