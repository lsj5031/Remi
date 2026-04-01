use chrono::Utc;
use core_model::{AgentAdapter, Checkpoint};
use store_sqlite::SqliteStore;
use tracing::{debug, trace};

#[derive(Debug, Clone)]
pub enum SyncPhase {
    Discovering,
    Scanning { file_count: usize },
    Normalizing { record_count: usize },
    Saving { message_count: usize },
    Done { total_records: usize },
}

pub fn sync_adapter(
    adapter: &dyn AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] embedder: Option<&mut embeddings::Embedder>,
    on_progress: impl Fn(SyncPhase),
) -> anyhow::Result<usize> {
    on_progress(SyncPhase::Discovering);

    let sources = adapter.discover_source_paths()?;
    debug!(agent = %adapter.kind(), source_count = sources.len(), "discovered source paths");

    on_progress(SyncPhase::Scanning {
        file_count: sources.len(),
    });

    let checkpoint = store.get_checkpoint(adapter.kind().as_str())?;
    trace!(agent = %adapter.kind(), checkpoint = ?checkpoint.as_deref(), "loaded checkpoint");
    let records = adapter.scan_changes_since(&sources, checkpoint.as_deref())?;

    on_progress(SyncPhase::Normalizing {
        record_count: records.len(),
    });

    let batch = adapter.normalize(&records)?;
    debug!(agent = %adapter.kind(), sessions = batch.sessions.len(), messages = batch.messages.len(), "normalized batch");

    on_progress(SyncPhase::Saving {
        message_count: batch.messages.len(),
    });

    store.save_batch(&batch)?;

    #[cfg(feature = "semantic")]
    if let Some(embedder) = embedder {
        let mut embedded = 0usize;
        for msg in &batch.messages {
            if let Ok(vec) = embedder.embed(&msg.content, false) {
                let _ = store.save_embedding(&msg.id, &vec);
                embedded += 1;
            }
        }
        debug!(agent = %adapter.kind(), embedded, total = batch.messages.len(), "computed embeddings");
    }

    if let Some(cursor) = adapter.checkpoint_cursor(&records) {
        trace!(agent = %adapter.kind(), cursor = %cursor, "saving checkpoint");
        store.upsert_checkpoint(&Checkpoint {
            agent: adapter.kind(),
            cursor,
            updated_at: Utc::now(),
        })?;
    }

    let total = records.len();
    on_progress(SyncPhase::Done {
        total_records: total,
    });

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::{AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch};
    use serde_json::Value;

    struct FakeAdapter {
        records: Vec<NativeRecord>,
    }

    impl AgentAdapter for FakeAdapter {
        fn kind(&self) -> AgentKind {
            AgentKind::Pi
        }
        fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
            Ok(vec!["fake/path".to_string()])
        }
        fn scan_changes_since(
            &self,
            _source_paths: &[String],
            _cursor: Option<&str>,
        ) -> anyhow::Result<Vec<NativeRecord>> {
            Ok(self.records.clone())
        }
        fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
            let mut batch = NormalizedBatch::default();
            for rec in records {
                let now = rec.updated_at;
                batch.sessions.push(core_model::Session {
                    id: format!("s_{}", rec.source_id),
                    agent: AgentKind::Pi,
                    source_ref: rec.source_id.clone(),
                    title: "fake".to_string(),
                    created_at: now,
                    updated_at: now,
                });
                batch.messages.push(core_model::Message {
                    id: format!("m_{}", rec.source_id),
                    session_id: format!("s_{}", rec.source_id),
                    role: "user".to_string(),
                    content: rec.payload.to_string(),
                    ts: now,
                });
            }
            Ok(batch)
        }
        fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
            records
                .iter()
                .map(|r| r.updated_at)
                .max()
                .map(|t| t.to_rfc3339())
        }
        fn archive_capability(&self) -> ArchiveCapability {
            ArchiveCapability::CentralizedCopy
        }
    }

    #[test]
    fn sync_adapter_basic() {
        let adapter = FakeAdapter {
            records: vec![NativeRecord {
                source_id: "r1".to_string(),
                updated_at: Utc::now(),
                payload: Value::String("test content".to_string()),
            }],
        };
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();

        #[cfg(feature = "semantic")]
        let count = sync_adapter(&adapter, &mut store, None, |_| {}).unwrap();
        #[cfg(not(feature = "semantic"))]
        let count = sync_adapter(&adapter, &mut store, |_| {}).unwrap();

        assert_eq!(count, 1);
        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
        let checkpoint = store.get_checkpoint("pi").unwrap();
        assert!(checkpoint.is_some());
    }

    #[test]
    fn sync_adapter_idempotent() {
        let adapter = FakeAdapter {
            records: vec![NativeRecord {
                source_id: "r1".to_string(),
                updated_at: Utc::now(),
                payload: Value::String("test".to_string()),
            }],
        };
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();

        #[cfg(feature = "semantic")]
        {
            sync_adapter(&adapter, &mut store, None, |_| {}).unwrap();
            sync_adapter(&adapter, &mut store, None, |_| {}).unwrap();
        }
        #[cfg(not(feature = "semantic"))]
        {
            sync_adapter(&adapter, &mut store, |_| {}).unwrap();
            sync_adapter(&adapter, &mut store, |_| {}).unwrap();
        }

        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn sync_adapter_empty() {
        let adapter = FakeAdapter { records: vec![] };
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();

        #[cfg(feature = "semantic")]
        let count = sync_adapter(&adapter, &mut store, None, |_| {}).unwrap();
        #[cfg(not(feature = "semantic"))]
        let count = sync_adapter(&adapter, &mut store, |_| {}).unwrap();

        assert_eq!(count, 0);
        assert!(store.get_checkpoint("pi").unwrap().is_none());
    }
}
