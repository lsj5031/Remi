use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use core_model::{
    ArchiveItem, ArchiveRun, Checkpoint, Message, NormalizedBatch, Provenance, Session,
    deterministic_id,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::time::Instant;
use tracing::{debug, info, trace};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocSyncState {
    pub root_id: String,
    pub canonical_path: String,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocRootRecord {
    pub root_id: String,
    pub canonical_path: String,
    pub current_generation: i64,
    pub last_completed_generation: i64,
    pub scan_started_at: Option<DateTime<Utc>>,
    pub scan_completed_at: Option<DateTime<Utc>>,
    pub scan_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentRecord {
    pub document_id: String,
    pub root_id: String,
    pub relative_path: String,
    pub title: String,
    pub modified_at: DateTime<Utc>,
    pub size_bytes: i64,
    pub content_hash: String,
    pub last_seen_generation: i64,
    pub indexed_generation: i64,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocFinalizeResult {
    pub deleted_documents: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentSearchRow {
    pub document_id: String,
    pub root_id: String,
    pub root_path: String,
    pub relative_path: String,
    pub title: String,
    pub snippet: String,
    pub score: f64,
}

#[cfg(feature = "semantic")]
#[derive(Debug, Clone, Default)]
pub struct EmbeddingStats {
    pub len: usize,
    pub updated_at: Option<DateTime<Utc>>,
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
        debug!(path = %path.as_ref().display(), "opening sqlite connection");
        let conn = Connection::open(path.as_ref())
            .with_context(|| format!("opening sqlite db {}", path.as_ref().display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(Self { conn })
    }

    pub fn init_schema(&self) -> anyhow::Result<()> {
        let version: i64 = self
            .conn
            .query_row("PRAGMA user_version;", [], |r| r.get(0))?;
        trace!(version, "schema version check");
        if version < 1 {
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
                CREATE TABLE IF NOT EXISTS message_embeddings (
                  message_id TEXT PRIMARY KEY,
                  dim INTEGER NOT NULL,
                  vec BLOB NOT NULL,
                  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS fts_messages USING fts5(
                  message_id UNINDEXED,
                  session_id UNINDEXED,
                  content,
                  ts UNINDEXED,
                  tokenize = 'unicode61 tokenchars ''_./:-'''
                );
                CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
                CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
                CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at);
                CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id);
                CREATE INDEX IF NOT EXISTS idx_artifacts_session_id ON artifacts(session_id);
                CREATE INDEX IF NOT EXISTS idx_provenance_entity_id ON provenance(entity_id);
                CREATE INDEX IF NOT EXISTS idx_archive_items_run_id ON archive_items(run_id);
                CREATE INDEX IF NOT EXISTS idx_archive_items_session_id ON archive_items(session_id);
                PRAGMA user_version = 1;
                "#,
            )?;
        }
        if version < 2 {
            self.conn.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
                CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
                CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON sessions(updated_at);
                CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id);
                CREATE INDEX IF NOT EXISTS idx_artifacts_session_id ON artifacts(session_id);
                CREATE INDEX IF NOT EXISTS idx_provenance_entity_id ON provenance(entity_id);
                CREATE INDEX IF NOT EXISTS idx_archive_items_run_id ON archive_items(run_id);
                CREATE INDEX IF NOT EXISTS idx_archive_items_session_id ON archive_items(session_id);
                PRAGMA user_version = 2;
                "#,
            )?;
        }
        if version < 3 {
            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS doc_roots (
                  root_id TEXT PRIMARY KEY,
                  canonical_path TEXT NOT NULL UNIQUE,
                  current_generation INTEGER NOT NULL DEFAULT 0,
                  last_completed_generation INTEGER NOT NULL DEFAULT 0,
                  scan_started_at TEXT,
                  scan_completed_at TEXT,
                  scan_status TEXT NOT NULL DEFAULT 'idle'
                );
                CREATE TABLE IF NOT EXISTS documents (
                  id TEXT PRIMARY KEY,
                  root_id TEXT NOT NULL,
                  relative_path TEXT NOT NULL,
                  title TEXT NOT NULL,
                  modified_at TEXT NOT NULL,
                  size_bytes INTEGER NOT NULL,
                  content_hash TEXT NOT NULL,
                  last_seen_generation INTEGER NOT NULL,
                  indexed_generation INTEGER NOT NULL,
                  indexed_at TEXT NOT NULL,
                  FOREIGN KEY(root_id) REFERENCES doc_roots(root_id) ON DELETE CASCADE,
                  UNIQUE(root_id, relative_path)
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS fts_documents USING fts5(
                  document_id UNINDEXED,
                  root_id UNINDEXED,
                  path,
                  title,
                  content,
                  tokenize = 'unicode61 tokenchars ''_./:-'''
                );
                CREATE INDEX IF NOT EXISTS idx_documents_root_id ON documents(root_id);
                CREATE INDEX IF NOT EXISTS idx_documents_root_generation ON documents(root_id, last_seen_generation);
                PRAGMA user_version = 3;
                "#,
            )?;
        }
        for (id, name) in [
            ("pi", "pi"),
            ("droid", "droid"),
            ("opencode", "opencode"),
            ("claude", "claude"),
            ("amp", "amp"),
        ] {
            self.conn.execute(
                "INSERT OR IGNORE INTO agents (id, name) VALUES (?1, ?2)",
                params![id, name],
            )?;
        }
        Ok(())
    }

    pub fn save_batch(&mut self, batch: &NormalizedBatch) -> anyhow::Result<()> {
        let started = Instant::now();
        let mut last = started;
        info!(
            sessions = batch.sessions.len(),
            messages = batch.messages.len(),
            events = batch.events.len(),
            artifacts = batch.artifacts.len(),
            provenance = batch.provenance.len(),
            "save_batch start"
        );
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
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "sessions upserted"
        );
        last = now;
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
                stmt_msg.execute(params![
                    m.id,
                    m.session_id,
                    m.role,
                    m.content,
                    m.ts.to_rfc3339()
                ])?;
            }
        }
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "messages upserted"
        );
        last = now;
        {
            let mut seen_message_ids = std::collections::HashSet::new();
            let mut stmt_lookup_rowid =
                tx.prepare_cached("SELECT rowid FROM messages WHERE id = ?1")?;
            let mut stmt_delete_message =
                tx.prepare_cached("DELETE FROM fts_messages WHERE rowid = ?1")?;
            let mut stmt_insert = tx.prepare_cached(
                "INSERT INTO fts_messages (rowid, message_id, session_id, content, ts)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for m in &batch.messages {
                if seen_message_ids.insert(&m.id) {
                    let rowid: i64 = stmt_lookup_rowid.query_row(params![m.id], |r| r.get(0))?;
                    stmt_delete_message.execute(params![rowid])?;
                    stmt_insert.execute(params![
                        rowid,
                        m.id,
                        m.session_id,
                        m.content,
                        m.ts.to_rfc3339()
                    ])?;
                }
            }
        }
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "fts updated"
        );
        last = now;
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
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "events upserted"
        );
        last = now;
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
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "artifacts upserted"
        );
        last = now;
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
        let now = Instant::now();
        info!(
            elapsed = ?now.duration_since(started),
            delta = ?now.duration_since(last),
            "provenance upserted"
        );
        let commit_start = Instant::now();
        tx.commit()?;
        info!(
            commit = ?commit_start.elapsed(),
            total = ?started.elapsed(),
            "commit done"
        );
        debug!(
            sessions = batch.sessions.len(),
            messages = batch.messages.len(),
            provenance = batch.provenance.len(),
            "save_batch complete"
        );
        Ok(())
    }

    #[cfg(feature = "semantic")]
    pub fn save_embedding(&self, message_id: &str, vec: &[f32]) -> anyhow::Result<()> {
        let dim = vec.len() as i64;
        let blob: Vec<u8> = vec.iter().flat_map(|f| f.to_le_bytes()).collect();
        self.conn.execute(
            "INSERT INTO message_embeddings (message_id, dim, vec) VALUES (?1, ?2, ?3) ON CONFLICT(message_id) DO UPDATE SET dim=excluded.dim, vec=excluded.vec",
            params![message_id, dim, blob],
        )?;
        Ok(())
    }

    #[cfg(feature = "semantic")]
    pub fn load_all_embeddings(&self) -> anyhow::Result<Vec<(String, Vec<f32>)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT message_id, vec FROM message_embeddings")?;
        let rows = stmt.query_map([], |r| {
            let id: String = r.get(0)?;
            let blob: Vec<u8> = r.get(1)?;
            let vec: Vec<f32> = blob
                .chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                .collect();
            Ok((id, vec))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    #[cfg(feature = "semantic")]
    pub fn embedding_stats(&self) -> anyhow::Result<EmbeddingStats> {
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*), MAX(m.ts) FROM message_embeddings e LEFT JOIN messages m ON e.message_id = m.id",
        )?;
        let (count, ts): (i64, Option<String>) =
            stmt.query_row([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(EmbeddingStats {
            len: count as usize,
            updated_at: ts.map(parse_ts),
        })
    }

    pub fn get_checkpoint(&self, agent: &str) -> anyhow::Result<Option<String>> {
        let result: Option<String> = self
            .conn
            .query_row(
                "SELECT cursor FROM checkpoints WHERE agent = ?1",
                params![agent],
                |r| r.get(0),
            )
            .optional()?;
        debug!(agent, cursor = ?result.as_deref(), "checkpoint loaded");
        Ok(result)
    }

    pub fn upsert_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        trace!(agent = %checkpoint.agent, cursor = %checkpoint.cursor, "upserting checkpoint");
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

    pub fn begin_doc_sync(&mut self, canonical_path: &str) -> anyhow::Result<DocSyncState> {
        let root_id = doc_root_id(canonical_path);
        let started_at = Utc::now().to_rfc3339();
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO doc_roots (root_id, canonical_path, current_generation, last_completed_generation, scan_status) VALUES (?1, ?2, 0, 0, 'idle')",
            params![root_id.as_str(), canonical_path],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT current_generation + 1 FROM doc_roots WHERE root_id = ?1",
            params![root_id.as_str()],
            |r| r.get(0),
        )?;
        tx.execute(
            "UPDATE doc_roots SET current_generation = ?2, scan_started_at = ?3, scan_completed_at = NULL, scan_status = 'running' WHERE root_id = ?1",
            params![root_id.as_str(), generation, started_at],
        )?;
        tx.commit()?;
        Ok(DocSyncState {
            root_id,
            canonical_path: canonical_path.to_string(),
            generation,
        })
    }

    pub fn get_doc_root(&self, canonical_path: &str) -> anyhow::Result<Option<DocRootRecord>> {
        self.conn
            .query_row(
                "SELECT root_id, canonical_path, current_generation, last_completed_generation, scan_started_at, scan_completed_at, scan_status FROM doc_roots WHERE canonical_path = ?1",
                params![canonical_path],
                |r| {
                    Ok(DocRootRecord {
                        root_id: r.get(0)?,
                        canonical_path: r.get(1)?,
                        current_generation: r.get(2)?,
                        last_completed_generation: r.get(3)?,
                        scan_started_at: parse_optional_ts(r.get(4)?),
                        scan_completed_at: parse_optional_ts(r.get(5)?),
                        scan_status: r.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_document_by_path(
        &self,
        root_id: &str,
        relative_path: &str,
    ) -> anyhow::Result<Option<DocumentRecord>> {
        self.conn
            .query_row(
                "SELECT id, root_id, relative_path, title, modified_at, size_bytes, content_hash, last_seen_generation, indexed_generation, indexed_at FROM documents WHERE root_id = ?1 AND relative_path = ?2",
                params![root_id, relative_path],
                |r| {
                    Ok(DocumentRecord {
                        document_id: r.get(0)?,
                        root_id: r.get(1)?,
                        relative_path: r.get(2)?,
                        title: r.get(3)?,
                        modified_at: parse_ts(r.get(4)?),
                        size_bytes: r.get(5)?,
                        content_hash: r.get(6)?,
                        last_seen_generation: r.get(7)?,
                        indexed_generation: r.get(8)?,
                        indexed_at: parse_ts(r.get(9)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn mark_document_seen(
        &self,
        root_id: &str,
        relative_path: &str,
        generation: i64,
    ) -> anyhow::Result<bool> {
        let updated = self.conn.execute(
            "UPDATE documents SET last_seen_generation = ?3 WHERE root_id = ?1 AND relative_path = ?2",
            params![root_id, relative_path, generation],
        )?;
        Ok(updated > 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upsert_document(
        &mut self,
        root_id: &str,
        relative_path: &str,
        title: &str,
        modified_at: DateTime<Utc>,
        size_bytes: i64,
        content_hash: &str,
        content: &str,
        generation: i64,
    ) -> anyhow::Result<String> {
        let document_id = document_id(root_id, relative_path);
        let indexed_at = Utc::now().to_rfc3339();
        let tx = self.conn.transaction()?;
        tx.execute(
            r#"INSERT INTO documents (id, root_id, relative_path, title, modified_at, size_bytes, content_hash, last_seen_generation, indexed_generation, indexed_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9)
            ON CONFLICT(root_id, relative_path) DO UPDATE SET
              id=excluded.id,
              title=excluded.title,
              modified_at=excluded.modified_at,
              size_bytes=excluded.size_bytes,
              content_hash=excluded.content_hash,
              last_seen_generation=excluded.last_seen_generation,
              indexed_generation=excluded.indexed_generation,
              indexed_at=excluded.indexed_at"#,
            params![
                document_id.as_str(),
                root_id,
                relative_path,
                title,
                modified_at.to_rfc3339(),
                size_bytes,
                content_hash,
                generation,
                indexed_at,
            ],
        )?;
        let rowid: i64 = tx.query_row(
            "SELECT rowid FROM documents WHERE root_id = ?1 AND relative_path = ?2",
            params![root_id, relative_path],
            |r| r.get(0),
        )?;
        tx.execute("DELETE FROM fts_documents WHERE rowid = ?1", params![rowid])?;
        tx.execute(
            "INSERT INTO fts_documents (rowid, document_id, root_id, path, title, content) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![rowid, document_id.as_str(), root_id, relative_path, title, content],
        )?;
        tx.commit()?;
        Ok(document_id)
    }

    pub fn finalize_doc_sync(
        &mut self,
        root_id: &str,
        generation: i64,
    ) -> anyhow::Result<DocFinalizeResult> {
        let completed_at = Utc::now().to_rfc3339();
        let tx = self.conn.transaction()?;
        let current_generation: Option<i64> = tx
            .query_row(
                "SELECT current_generation FROM doc_roots WHERE root_id = ?1",
                params![root_id],
                |r| r.get(0),
            )
            .optional()?;
        let current_generation = current_generation.with_context(|| {
            format!("doc root {root_id} missing while finalizing generation {generation}")
        })?;
        if current_generation != generation {
            return Err(anyhow::anyhow!(
                "stale doc sync finalize for root {root_id}: current generation {current_generation}, attempted {generation}"
            ));
        }
        let deleted_documents: i64 = tx.query_row(
            "SELECT COUNT(*) FROM documents WHERE root_id = ?1 AND last_seen_generation < ?2",
            params![root_id, generation],
            |r| r.get(0),
        )?;
        tx.execute(
            "DELETE FROM fts_documents WHERE rowid IN (SELECT rowid FROM documents WHERE root_id = ?1 AND last_seen_generation < ?2)",
            params![root_id, generation],
        )?;
        tx.execute(
            "DELETE FROM documents WHERE root_id = ?1 AND last_seen_generation < ?2",
            params![root_id, generation],
        )?;
        tx.execute(
            "UPDATE doc_roots SET last_completed_generation = ?2, scan_status = 'completed', scan_completed_at = ?3 WHERE root_id = ?1",
            params![root_id, generation, completed_at],
        )?;
        tx.commit()?;
        Ok(DocFinalizeResult {
            deleted_documents: deleted_documents as usize,
        })
    }

    pub fn fail_doc_sync(&self, root_id: &str, generation: i64) -> anyhow::Result<()> {
        let updated = self.conn.execute(
            "UPDATE doc_roots SET scan_status = 'failed', scan_completed_at = ?3 WHERE root_id = ?1 AND current_generation = ?2",
            params![root_id, generation, Utc::now().to_rfc3339()],
        )?;
        if updated == 0 {
            return Err(anyhow::anyhow!(
                "doc root {root_id} missing or generation {generation} is no longer current"
            ));
        }
        Ok(())
    }

    pub fn search_documents_lexical(
        &self,
        query: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<DocumentSearchRow>> {
        debug!(query, limit, "docs lexical search");
        let mut stmt = self.conn.prepare(
            "SELECT d.id, d.root_id, dr.canonical_path, d.relative_path, d.title, snippet(fts_documents, 4, '[', ']', ' … ', 16), bm25(fts_documents) AS rank FROM fts_documents INNER JOIN documents d ON d.rowid = fts_documents.rowid INNER JOIN doc_roots dr ON dr.root_id = d.root_id WHERE fts_documents MATCH ?1 ORDER BY rank LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![query, limit], |r| {
            let rank: f64 = r.get(6)?;
            Ok(DocumentSearchRow {
                document_id: r.get(0)?,
                root_id: r.get(1)?,
                root_path: r.get(2)?,
                relative_path: r.get(3)?,
                title: r.get(4)?,
                snippet: r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                score: -rank,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn search_documents_substring(
        &self,
        query: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<DocumentSearchRow>> {
        debug!(query, limit, "docs substring search");
        let pattern = format!("%{}%", escape_like_pattern(&query.to_lowercase()));
        let mut stmt = self.conn.prepare(
            "SELECT d.id, d.root_id, dr.canonical_path, d.relative_path, d.title, f.content FROM documents d INNER JOIN doc_roots dr ON dr.root_id = d.root_id INNER JOIN fts_documents f ON f.rowid = d.rowid WHERE lower(f.content) LIKE ?1 ESCAPE '\\' ORDER BY d.indexed_at DESC, d.relative_path ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![pattern, limit], |r| {
            let content: String = r.get(5)?;
            Ok(DocumentSearchRow {
                document_id: r.get(0)?,
                root_id: r.get(1)?,
                root_path: r.get(2)?,
                relative_path: r.get(3)?,
                title: r.get(4)?,
                snippet: build_document_snippet(&content, query),
                score: 0.0,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn list_sessions(&self) -> anyhow::Result<Vec<Session>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, agent, source_ref, title, created_at, updated_at FROM sessions ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            let agent_str: String = r.get(1)?;
            Ok(Session {
                id: r.get(0)?,
                agent: parse_agent(&agent_str)?,
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
            "SELECT id, session_id, role, content, ts FROM messages WHERE session_id = ?1 ORDER BY ts ASC, rowid ASC",
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

    pub fn get_session_events(&self, session_id: &str) -> anyhow::Result<Vec<core_model::Event>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, kind, payload, ts FROM events WHERE session_id = ?1 ORDER BY ts ASC",
        )?;
        let rows = stmt.query_map(params![session_id], |r| {
            Ok(core_model::Event {
                id: r.get(0)?,
                session_id: r.get(1)?,
                kind: r.get(2)?,
                payload: serde_json::from_str(&r.get::<_, String>(3)?).unwrap_or_default(),
                ts: parse_ts(r.get(4)?),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_session_artifacts(
        &self,
        session_id: &str,
    ) -> anyhow::Result<Vec<core_model::Artifact>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, path, checksum, metadata FROM artifacts WHERE session_id = ?1",
        )?;
        let rows = stmt.query_map(params![session_id], |r| {
            Ok(core_model::Artifact {
                id: r.get(0)?,
                session_id: r.get(1)?,
                path: r.get(2)?,
                checksum: r.get(3)?,
                metadata: serde_json::from_str(&r.get::<_, String>(4)?).unwrap_or_default(),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    #[cfg(feature = "semantic")]
    pub fn get_message(&self, message_id: &str) -> anyhow::Result<Option<Message>> {
        self.conn
            .query_row(
                "SELECT id, session_id, role, content, ts FROM messages WHERE id = ?1",
                params![message_id],
                |r| {
                    Ok(Message {
                        id: r.get(0)?,
                        session_id: r.get(1)?,
                        role: r.get(2)?,
                        content: r.get(3)?,
                        ts: parse_ts(r.get(4)?),
                    })
                },
            )
            .optional()
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
                        agent: parse_agent(&agent_str)?,
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
                agent: parse_agent(&agent_str)?,
                source_path: r.get(4)?,
                source_id: r.get(5)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn search_lexical(&self, query: &str, limit: i64) -> anyhow::Result<Vec<SearchRow>> {
        debug!(query, limit, "lexical search");
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
        debug!(query, limit, "substring search");
        let pattern = format!("%{}%", escape_like_pattern(&query.to_lowercase()));
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.session_id, m.content, m.ts FROM messages m WHERE lower(m.content) LIKE ?1 ESCAPE '\\' ORDER BY m.ts DESC LIMIT ?2",
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
        debug!(session_id, "cascading delete session");
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

fn parse_optional_ts(ts: Option<String>) -> Option<DateTime<Utc>> {
    ts.map(parse_ts)
}

fn doc_root_id(canonical_path: &str) -> String {
    deterministic_id(&["doc_root", canonical_path])
}

fn document_id(root_id: &str, relative_path: &str) -> String {
    deterministic_id(&["document", root_id, relative_path])
}

fn build_document_snippet(content: &str, query: &str) -> String {
    const MAX_CHARS: usize = 180;
    let collapsed = content.split_whitespace().collect::<Vec<_>>().join(" "
    );
    if collapsed.is_empty() {
        return String::new();
    }
    if collapsed.is_ascii() && query.is_ascii() {
        let lower = collapsed.to_lowercase();
        let needle = query.trim().to_lowercase();
        if !needle.is_empty() {
            if let Some(start) = lower.find(&needle) {
                let snippet_start = start.saturating_sub(60);
                let snippet_end = (start + needle.len() + 60).min(collapsed.len());
                let mut snippet = collapsed[snippet_start..snippet_end].to_string();
                if snippet_start > 0 {
                    snippet = format!("…{}", snippet.trim_start());
                }
                if snippet_end < collapsed.len() {
                    snippet.push('…');
                }
                return snippet;
            }
        }
    }
    let shortened: String = collapsed.chars().take(MAX_CHARS).collect();
    if shortened.len() == collapsed.len() {
        shortened
    } else {
        format!("{shortened}…")
    }
}

fn parse_agent(s: &str) -> rusqlite::Result<core_model::AgentKind> {
    s.parse::<core_model::AgentKind>().map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
        )
    })
}

fn escape_like_pattern(query: &str) -> String {
    let mut escaped = String::with_capacity(query.len());
    for ch in query.to_lowercase().chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
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

    fn make_batch(
        agent: AgentKind,
        session_id: &str,
        msg_id: &str,
        content: &str,
    ) -> NormalizedBatch {
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

    fn upsert_test_document(
        store: &mut SqliteStore,
        sync: &DocSyncState,
        relative_path: &str,
        title: &str,
        content: &str,
    ) {
        let modified_at = Utc::now();
        store
            .upsert_document(
                &sync.root_id,
                relative_path,
                title,
                modified_at,
                content.len() as i64,
                &format!("hash-{relative_path}-{}", content.len()),
                content,
                sync.generation,
            )
            .unwrap();
    }

    #[test]
    fn agents_populated() {
        let store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM agents", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5);
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
    fn get_session_messages_is_stable_when_timestamps_match() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let now = Utc::now();
        let batch = NormalizedBatch {
            sessions: vec![Session {
                id: "s_same_ts".to_string(),
                agent: core_model::AgentKind::Codex,
                source_ref: "same-ts".to_string(),
                title: "same ts".to_string(),
                created_at: now,
                updated_at: now,
            }],
            messages: vec![
                Message {
                    id: "m1".to_string(),
                    session_id: "s_same_ts".to_string(),
                    role: "user".to_string(),
                    content: "first".to_string(),
                    ts: now,
                },
                Message {
                    id: "m2".to_string(),
                    session_id: "s_same_ts".to_string(),
                    role: "assistant".to_string(),
                    content: "second".to_string(),
                    ts: now,
                },
                Message {
                    id: "m3".to_string(),
                    session_id: "s_same_ts".to_string(),
                    role: "user".to_string(),
                    content: "third".to_string(),
                    ts: now,
                },
            ],
            events: vec![],
            artifacts: vec![],
            provenance: vec![],
        };

        store.save_batch(&batch).unwrap();

        let msgs = store.get_session_messages("s_same_ts").unwrap();
        let contents = msgs.into_iter().map(|msg| msg.content).collect::<Vec<_>>();
        assert_eq!(contents, vec!["first", "second", "third"]);
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
        let batch = make_batch(
            core_model::AgentKind::Pi,
            "s1",
            "m1",
            "rust programming language",
        );
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
        let run1 = store.plan_archive(Duration::days(30), 2).unwrap();
        let items1 = store.archive_items_for_run(&run1.id).unwrap();
        assert_eq!(items1.len(), 3);
        let run2 = store.plan_archive(Duration::days(30), 2).unwrap();
        let items2 = store.archive_items_for_run(&run2.id).unwrap();
        assert_eq!(
            items2.len(),
            0,
            "idempotency: already-planned sessions should be skipped"
        );
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
        assert_eq!(count, 5);
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
        let batch = make_batch(
            core_model::AgentKind::Pi,
            "s1",
            "m1",
            "hello_world function",
        );
        store.save_batch(&batch).unwrap();
        let results = store.search_substring("hello_world", 10).unwrap();
        assert_eq!(results.len(), 1);
        let empty = store.search_substring("nonexistent", 10).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn substring_search_escapes_like_wildcards() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let with_underscore = make_batch(
            core_model::AgentKind::Pi,
            "s1",
            "m1",
            "hello_world function",
        );
        store.save_batch(&with_underscore).unwrap();
        let with_x = make_batch(
            core_model::AgentKind::Pi,
            "s2",
            "m2",
            "helloxworld function",
        );
        store.save_batch(&with_x).unwrap();

        let results = store.search_substring("hello_world", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message_id, "m1");
    }

    #[test]
    fn fts_batch_replaces_only_touched_messages() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();

        let mut batch = NormalizedBatch::default();
        let now = Utc::now();
        batch.sessions.push(Session {
            id: "s1".to_string(),
            agent: core_model::AgentKind::Pi,
            source_ref: "ref".to_string(),
            title: "test".to_string(),
            created_at: now,
            updated_at: now,
        });
        batch.messages.push(Message {
            id: "m1".to_string(),
            session_id: "s1".to_string(),
            role: "user".to_string(),
            content: "alpha beta".to_string(),
            ts: now,
        });
        batch.messages.push(Message {
            id: "m2".to_string(),
            session_id: "s1".to_string(),
            role: "assistant".to_string(),
            content: "gamma delta".to_string(),
            ts: now,
        });
        store.save_batch(&batch).unwrap();

        let r = store.search_lexical("alpha", 10).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].message_id, "m1");
        let r2 = store.search_lexical("gamma", 10).unwrap();
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].message_id, "m2");

        let mut batch2 = NormalizedBatch::default();
        batch2.sessions.push(Session {
            id: "s1".to_string(),
            agent: core_model::AgentKind::Pi,
            source_ref: "ref".to_string(),
            title: "test".to_string(),
            created_at: now,
            updated_at: now,
        });
        batch2.messages.push(Message {
            id: "m1".to_string(),
            session_id: "s1".to_string(),
            role: "user".to_string(),
            content: "updated alpha content".to_string(),
            ts: now,
        });
        batch2.messages.push(Message {
            id: "m3".to_string(),
            session_id: "s1".to_string(),
            role: "assistant".to_string(),
            content: "epsilon zeta".to_string(),
            ts: now,
        });
        store.save_batch(&batch2).unwrap();

        let r3 = store.search_lexical("updated", 10).unwrap();
        assert_eq!(r3.len(), 1, "m1 FTS should reflect updated content");
        let r4 = store.search_lexical("epsilon", 10).unwrap();
        assert_eq!(r4.len(), 1, "new message m3 should be in FTS");
        let r5 = store.search_lexical("gamma", 10).unwrap();
        assert_eq!(r5.len(), 1, "untouched m2 should remain in FTS");
        assert_eq!(r5[0].message_id, "m2");
    }

    #[test]
    fn fts_multi_session_batch() {
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let now = Utc::now();
        let mut batch = NormalizedBatch::default();
        for i in 0..3 {
            batch.sessions.push(Session {
                id: format!("s{i}"),
                agent: core_model::AgentKind::Pi,
                source_ref: format!("ref{i}"),
                title: format!("session {i}"),
                created_at: now,
                updated_at: now,
            });
            batch.messages.push(Message {
                id: format!("m{i}"),
                session_id: format!("s{i}"),
                role: "user".to_string(),
                content: format!("unique_keyword_{i}"),
                ts: now,
            });
        }
        store.save_batch(&batch).unwrap();

        for i in 0..3 {
            let r = store
                .search_lexical(&format!("unique_keyword_{i}"), 10)
                .unwrap();
            assert_eq!(r.len(), 1, "session {i} message should be in FTS");
            assert_eq!(r[0].session_id, format!("s{i}"));
        }
    }
}
