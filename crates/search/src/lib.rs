use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
#[cfg(feature = "semantic")]
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use store_sqlite::SqliteStore;
use tracing::debug;

#[cfg(feature = "semantic")]
use embeddings::Embedder;

#[derive(Debug, Clone)]
pub struct RankedHit {
    pub message_id: String,
    pub session_id: String,
    pub content: String,
    pub score: f32,
}

#[derive(Debug, Clone)]
pub struct SessionHit {
    pub session_id: String,
    pub top_message_id: String,
    pub top_content: String,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocHit {
    pub path: String,
    pub title: String,
    pub snippet: String,
    pub score: f32,
}

#[cfg(feature = "semantic")]
#[derive(Default)]
pub struct SemanticCache {
    embeddings: Option<Vec<(String, Vec<f32>)>>,
    len: usize,
    updated_at: Option<DateTime<Utc>>,
}

#[cfg(feature = "semantic")]
impl SemanticCache {
    pub fn embeddings(&mut self, store: &SqliteStore) -> anyhow::Result<&[(String, Vec<f32>)]> {
        let stats = store.embedding_stats()?;
        let should_refresh = self.embeddings.is_none()
            || stats.len != self.len
            || stats.updated_at != self.updated_at;
        if should_refresh {
            self.embeddings = Some(store.load_all_embeddings()?);
            self.len = stats.len;
            self.updated_at = stats.updated_at;
        }
        Ok(self.embeddings.as_deref().unwrap_or(&[]))
    }

    pub fn clear(&mut self) {
        self.embeddings = None;
        self.len = 0;
        self.updated_at = None;
    }
}

pub fn search(
    store: &SqliteStore,
    query: &str,
    limit: usize,
    raw_fts: bool,
    #[cfg(feature = "semantic")] embedder: Option<&mut Embedder>,
    #[cfg(feature = "semantic")] cache: Option<&mut SemanticCache>,
) -> anyhow::Result<Vec<RankedHit>> {
    let fts_query = if raw_fts {
        query.trim().to_string()
    } else {
        sanitize_fts_query(query)
    };
    debug!(raw_query = %query, fts_query = %fts_query, raw_fts, "search query prepared");

    let bm25_rows = if !fts_query.is_empty() {
        store.search_lexical(&fts_query, 200)?
    } else {
        Vec::new()
    };
    debug!(bm25_rows = bm25_rows.len(), "BM25 results");

    if bm25_rows.is_empty() {
        #[cfg(feature = "semantic")]
        let has_semantic = embedder.is_some();
        #[cfg(not(feature = "semantic"))]
        let has_semantic = false;

        if !has_semantic {
            let fallback = store.search_substring(query, limit as i64)?;
            if !fallback.is_empty() {
                return Ok(fallback
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| RankedHit {
                        message_id: r.message_id,
                        session_id: r.session_id,
                        content: r.content,
                        score: 1.0 / (60.0 + i as f32 + 1.0),
                    })
                    .collect());
            }
            return Ok(Vec::new());
        }
    }

    let recency_rows = store.recent_messages(200)?;
    debug!(recency_rows = recency_rows.len(), "recency rows loaded");

    #[cfg(feature = "semantic")]
    let semantic_rows = if let Some(embedder) = embedder {
        let query_vec = embedder.embed(query, true)?;
        let mut owned_embeddings = None;
        let embeddings = if let Some(cache) = cache {
            cache.embeddings(store)?
        } else {
            owned_embeddings = Some(store.load_all_embeddings()?);
            owned_embeddings.as_deref().unwrap_or(&[])
        };
        let mut scored: Vec<(String, f32)> = embeddings
            .iter()
            .map(|(id, vec)| {
                let score = cosine_similarity(&query_vec, vec);
                (id.clone(), score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.truncate(200);
        scored
    } else {
        Vec::new()
    };

    let mut scores: HashMap<String, (f32, String, String, String)> = HashMap::new();

    let bm25_weight = 1.0_f32;
    let recency_weight = 0.3_f32;
    #[cfg(feature = "semantic")]
    let semantic_weight = 0.5_f32;
    let k = 60.0_f32;

    for (rank, row) in bm25_rows.iter().enumerate() {
        let rrf = bm25_weight / (k + rank as f32 + 1.0);
        scores
            .entry(row.message_id.clone())
            .and_modify(|(s, _, _, _)| *s += rrf)
            .or_insert((
                rrf,
                row.session_id.clone(),
                row.content.clone(),
                row.message_id.clone(),
            ));
    }

    for (rank, row) in recency_rows.iter().enumerate() {
        let rrf = recency_weight / (k + rank as f32 + 1.0);
        scores
            .entry(row.message_id.clone())
            .and_modify(|(s, _, _, _)| *s += rrf)
            .or_insert((
                rrf,
                row.session_id.clone(),
                row.content.clone(),
                row.message_id.clone(),
            ));
    }

    #[cfg(feature = "semantic")]
    for (rank, (msg_id, _score)) in semantic_rows.iter().enumerate() {
        let rrf = semantic_weight / (k + rank as f32 + 1.0);
        scores
            .entry(msg_id.clone())
            .and_modify(|(s, _, _, _)| *s += rrf)
            .or_insert_with(|| {
                if let Ok(Some(msg)) = store.get_message(msg_id) {
                    (rrf, msg.session_id, msg.content, msg_id.clone())
                } else {
                    (0.0, String::new(), String::new(), msg_id.clone())
                }
            });
    }

    let mut out: Vec<RankedHit> = scores
        .into_values()
        .filter(|(s, _, _, _)| *s > 0.0)
        .map(|(score, session_id, content, message_id)| RankedHit {
            message_id,
            session_id,
            content,
            score,
        })
        .collect();

    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(limit);
    debug!(total = out.len(), "RRF scored results");

    Ok(out)
}

pub fn search_sessions(
    store: &SqliteStore,
    query: &str,
    limit: usize,
    raw_fts: bool,
    #[cfg(feature = "semantic")] embedder: Option<&mut Embedder>,
    #[cfg(feature = "semantic")] cache: Option<&mut SemanticCache>,
) -> anyhow::Result<Vec<SessionHit>> {
    let hits = search(
        store,
        query,
        limit * 5,
        raw_fts,
        #[cfg(feature = "semantic")]
        embedder,
        #[cfg(feature = "semantic")]
        cache,
    )?;

    let mut grouped: HashMap<String, (f32, f32, String, String)> = HashMap::new();
    for hit in hits {
        grouped
            .entry(hit.session_id.clone())
            .and_modify(|(total, top_score, top_id, top_content)| {
                *total += hit.score;
                if hit.score > *top_score {
                    *top_score = hit.score;
                    *top_id = hit.message_id.clone();
                    *top_content = hit.content.clone();
                }
            })
            .or_insert((hit.score, hit.score, hit.message_id, hit.content));
    }

    let mut out: Vec<SessionHit> = grouped
        .into_iter()
        .map(
            |(session_id, (score, _top_score, top_message_id, top_content))| SessionHit {
                session_id,
                top_message_id,
                top_content,
                score,
            },
        )
        .collect();

    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(limit);
    debug!(sessions = out.len(), "session hits grouped");
    Ok(out)
}

pub fn search_docs_at(
    db_path: impl AsRef<Path>,
    query: &str,
    limit: usize,
    raw_fts: bool,
) -> anyhow::Result<Vec<DocHit>> {
    let conn = Connection::open(db_path.as_ref())
        .with_context(|| format!("opening sqlite db {}", db_path.as_ref().display()))?;
    if !has_docs_index(&conn)? {
        return Ok(Vec::new());
    }
    search_docs_conn(&conn, query, limit, raw_fts)
}

fn search_docs_conn(
    conn: &Connection,
    query: &str,
    limit: usize,
    raw_fts: bool,
) -> anyhow::Result<Vec<DocHit>> {
    let fts_query = if raw_fts {
        query.trim().to_string()
    } else {
        sanitize_fts_query(query)
    };
    debug!(raw_query = %query, fts_query = %fts_query, raw_fts, "docs search query prepared");

    if !fts_query.is_empty() {
        let mut stmt = conn.prepare(
            "SELECT path,
                    COALESCE(NULLIF(title, ''), path) AS title,
                    snippet(fts_documents, 4, '[', ']', ' … ', 18) AS snippet,
                    bm25(fts_documents) AS rank
             FROM fts_documents
             WHERE fts_documents MATCH ?1
             ORDER BY rank
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![fts_query, limit as i64], |row| {
            let rank: f64 = row.get(3)?;
            Ok(DocHit {
                path: row.get(0)?,
                title: row.get(1)?,
                snippet: row.get(2)?,
                score: (-rank) as f32,
            })
        })?;
        let hits = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        if !hits.is_empty() {
            return Ok(hits);
        }
    }

    if query.trim().is_empty() {
        return Ok(Vec::new());
    }

    let pattern = format!("%{}%", escape_like_pattern(&query.to_lowercase()));
    let mut stmt = conn.prepare(
        "SELECT path,
                COALESCE(NULLIF(title, ''), path) AS title,
                content
         FROM fts_documents
         WHERE lower(path) LIKE ?1 ESCAPE '\\'
            OR lower(title) LIKE ?1 ESCAPE '\\'
            OR lower(content) LIKE ?1 ESCAPE '\\'
         ORDER BY path
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![pattern, limit as i64], |row| {
        let path: String = row.get(0)?;
        let title: String = row.get(1)?;
        let content: String = row.get(2)?;
        Ok(DocHit {
            path,
            title,
            snippet: fallback_doc_snippet(&content, query),
            score: 0.0,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn sanitize_fts_query(query: &str) -> String {
    let terms: Vec<String> = query
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .map(|t| {
            let cleaned: String = t
                .chars()
                .filter(|c| {
                    c.is_alphanumeric()
                        || *c == '_'
                        || *c == '.'
                        || *c == '/'
                        || *c == ':'
                        || *c == '-'
                })
                .collect();
            if cleaned.is_empty() {
                String::new()
            } else {
                format!("\"{}\"", cleaned)
            }
        })
        .filter(|t| !t.is_empty())
        .collect();
    terms.join(" OR ")
}

fn has_docs_index(conn: &Connection) -> anyhow::Result<bool> {
    conn.query_row(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'fts_documents'",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map(|row| row.is_some())
    .map_err(Into::into)
}

fn escape_like_pattern(query: &str) -> String {
    let mut escaped = String::with_capacity(query.len());
    for ch in query.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn fallback_doc_snippet(content: &str, query: &str) -> String {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let query_lower = query.to_lowercase();
    let content_lower = content.to_lowercase();
    if let Some(pos) = content_lower.find(&query_lower) {
        let start = pos.saturating_sub(60);
        let end = (pos + query_lower.len() + 100).min(content.len());
        let prefix = if start > 0 { "… " } else { "" };
        let suffix = if end < content.len() { " …" } else { "" };
        return format!("{}{}{}", prefix, content[start..end].trim(), suffix);
    }

    let snippet: String = trimmed.chars().take(180).collect();
    if trimmed.chars().count() > 180 {
        format!("{snippet}…")
    } else {
        snippet
    }
}

#[cfg(feature = "semantic")]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a < 1e-6 || norm_b < 1e-6 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::{AgentKind, Message, NormalizedBatch, Session};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn setup_store() -> SqliteStore {
        let store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();
        let now = Utc::now();
        let batch = NormalizedBatch {
            sessions: vec![Session {
                id: "s1".to_string(),
                agent: AgentKind::Pi,
                source_ref: "ref".to_string(),
                title: "test".to_string(),
                created_at: now,
                updated_at: now,
            }],
            messages: vec![
                Message {
                    id: "m1".to_string(),
                    session_id: "s1".to_string(),
                    role: "user".to_string(),
                    content: "rust programming".to_string(),
                    ts: now,
                },
                Message {
                    id: "m2".to_string(),
                    session_id: "s1".to_string(),
                    role: "assistant".to_string(),
                    content: "python scripting".to_string(),
                    ts: now,
                },
            ],
            events: vec![],
            artifacts: vec![],
            provenance: vec![],
        };
        let mut store_mut = store;
        store_mut.save_batch(&batch).unwrap();
        store_mut
    }

    fn temp_db_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("remi-search-{name}-{unique}.db"))
    }

    fn setup_docs_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            r#"
            CREATE VIRTUAL TABLE fts_documents USING fts5(
                document_id UNINDEXED,
                root_id UNINDEXED,
                path,
                title,
                content,
                tokenize = 'unicode61 tokenchars ''_./:-'''
            );
            "#,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fts_documents (document_id, root_id, path, title, content)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "doc-1",
                "root-1",
                "guides/setup.md",
                "Setup Guide",
                "Install remi and keep the docs-search token nearby."
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fts_documents (document_id, root_id, path, title, content)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "doc-2",
                "root-1",
                "notes/faq.txt",
                "FAQ",
                "Troubleshooting notes for indexing behaviour."
            ],
        )
        .unwrap();
    }

    #[test]
    fn search_finds_match() {
        let store = setup_store();
        #[cfg(feature = "semantic")]
        let hits = search(&store, "rust", 10, false, None, None).unwrap();
        #[cfg(not(feature = "semantic"))]
        let hits = search(&store, "rust", 10, false).unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].message_id, "m1");
        assert!(hits[0].score > 0.0);
    }

    #[test]
    fn search_no_match() {
        let store = setup_store();
        #[cfg(feature = "semantic")]
        let hits = search(&store, "java", 10, false, None, None).unwrap();
        #[cfg(not(feature = "semantic"))]
        let hits = search(&store, "java", 10, false).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn search_ranked_by_score() {
        let store = setup_store();
        #[cfg(feature = "semantic")]
        let hits = search(&store, "rust OR python", 10, true, None, None).unwrap();
        #[cfg(not(feature = "semantic"))]
        let hits = search(&store, "rust OR python", 10, true).unwrap();
        assert!(!hits.is_empty());
        for w in hits.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
    }

    #[test]
    fn search_sessions_groups_hits() {
        let store = setup_store();
        #[cfg(feature = "semantic")]
        let sessions = search_sessions(&store, "rust", 10, false, None, None).unwrap();
        #[cfg(not(feature = "semantic"))]
        let sessions = search_sessions(&store, "rust", 10, false).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "s1");
        assert!(sessions[0].score > 0.0);
        assert_eq!(sessions[0].top_message_id, "m1");
    }

    #[test]
    fn sanitize_fts_handles_special_chars() {
        assert_eq!(sanitize_fts_query("hello world"), "\"hello\" OR \"world\"");
        assert_eq!(sanitize_fts_query("snake_case"), "\"snake_case\"");
        assert_eq!(sanitize_fts_query("src/lib.rs"), "\"src/lib.rs\"");
        assert_eq!(sanitize_fts_query(""), "");
        assert_eq!(sanitize_fts_query("  "), "");
    }

    #[test]
    fn search_substring_fallback() {
        let store = setup_store();
        #[cfg(feature = "semantic")]
        let hits = search(&store, "progr", 10, false, None, None).unwrap();
        #[cfg(not(feature = "semantic"))]
        let hits = search(&store, "progr", 10, false).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message_id, "m1");
    }

    #[test]
    fn docs_search_returns_doc_hits() {
        let db_path = temp_db_path("docs-fts");
        setup_docs_db(&db_path);

        let hits = search_docs_at(&db_path, "docs-search", 10, false).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "guides/setup.md");
        assert_eq!(hits[0].title, "Setup Guide");
        assert!(hits[0].snippet.to_lowercase().contains("docs-search"));
        assert!(hits[0].score > 0.0);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn docs_search_falls_back_to_substring_when_fts_misses() {
        let db_path = temp_db_path("docs-substring");
        setup_docs_db(&db_path);

        let hits = search_docs_at(&db_path, "docs-sear", 10, false).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "guides/setup.md");
        assert!(hits[0].snippet.to_lowercase().contains("docs-search"));

        let _ = std::fs::remove_file(db_path);
    }
}
