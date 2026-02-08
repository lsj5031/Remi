use std::collections::HashMap;

use store_sqlite::SqliteStore;

#[derive(Debug, Clone)]
pub struct RankedHit {
    pub message_id: String,
    pub session_id: String,
    pub content: String,
    pub score: f32,
}

pub fn search(store: &SqliteStore, query: &str, limit: usize) -> anyhow::Result<Vec<RankedHit>> {
    let fts_query = sanitize_fts_query(query);

    let bm25_rows = if !fts_query.is_empty() {
        store.search_lexical(&fts_query, 200)?
    } else {
        Vec::new()
    };

    if bm25_rows.is_empty() {
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

    let recency_rows = store.recent_messages(200)?;

    let mut scores: HashMap<String, (f32, String, String, String)> = HashMap::new();

    let bm25_weight = 1.0_f32;
    let recency_weight = 0.3_f32;
    let k = 60.0_f32;

    for (rank, row) in bm25_rows.iter().enumerate() {
        let rrf = bm25_weight / (k + rank as f32 + 1.0);
        scores
            .entry(row.message_id.clone())
            .and_modify(|(s, _, _, _)| *s += rrf)
            .or_insert((rrf, row.session_id.clone(), row.content.clone(), row.message_id.clone()));
    }

    for (rank, row) in recency_rows.iter().enumerate() {
        let rrf = recency_weight / (k + rank as f32 + 1.0);
        scores
            .entry(row.message_id.clone())
            .and_modify(|(s, _, _, _)| *s += rrf)
            .or_insert((rrf, row.session_id.clone(), row.content.clone(), row.message_id.clone()));
    }

    let mut out: Vec<RankedHit> = scores
        .into_values()
        .map(|(score, session_id, content, message_id)| RankedHit {
            message_id,
            session_id,
            content,
            score,
        })
        .collect();

    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(limit);
    Ok(out)
}

fn sanitize_fts_query(query: &str) -> String {
    let terms: Vec<String> = query
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .map(|t| {
            let cleaned: String = t
                .chars()
                .filter(|c| {
                    c.is_alphanumeric() || *c == '_' || *c == '.' || *c == '/' || *c == ':' || *c == '-'
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::{AgentKind, Message, NormalizedBatch, Session};

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

    #[test]
    fn search_finds_match() {
        let store = setup_store();
        let hits = search(&store, "rust", 10).unwrap();
        assert!(!hits.is_empty());
        assert_eq!(hits[0].message_id, "m1");
        assert!(hits[0].score > 0.0);
    }

    #[test]
    fn search_no_match() {
        let store = setup_store();
        let hits = search(&store, "java", 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn search_ranked_by_score() {
        let store = setup_store();
        let hits = search(&store, "rust OR python", 10).unwrap();
        assert!(!hits.is_empty());
        for w in hits.windows(2) {
            assert!(w[0].score >= w[1].score);
        }
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
        let hits = search(&store, "progr", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].message_id, "m1");
    }
}
