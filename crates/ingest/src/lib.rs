use chrono::Utc;
use core_model::{AgentAdapter, Checkpoint, NativeRecord};
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

/// Records buffered per flush; bounds peak memory regardless of history size.
const CHUNK: usize = 5000;

pub fn sync_adapter(
    adapter: &dyn AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] mut embedder: Option<&mut embeddings::Embedder>,
    mut on_progress: impl FnMut(SyncPhase),
) -> anyhow::Result<usize> {
    on_progress(SyncPhase::Discovering);

    let sources = adapter.discover_source_paths()?;
    debug!(agent = %adapter.kind(), source_count = sources.len(), "discovered source paths");

    on_progress(SyncPhase::Scanning {
        file_count: sources.len(),
    });

    let checkpoint = store.get_checkpoint(adapter.kind().as_str())?;
    trace!(agent = %adapter.kind(), checkpoint = ?checkpoint.as_deref(), "loaded checkpoint");

    let (tx, rx) = std::sync::mpsc::sync_channel::<NativeRecord>(CHUNK);
    let mut total_records = 0usize;

    let new_cursor: Option<String> =
        std::thread::scope(|scope| -> anyhow::Result<Option<String>> {
            let producer =
                scope.spawn(|| adapter.stream_changes_since(&sources, checkpoint.as_deref(), tx));
            let mut buf: Vec<NativeRecord> = Vec::with_capacity(CHUNK);
            for rec in rx {
                total_records += 1;
                buf.push(rec);
                if buf.len() >= CHUNK {
                    save_chunk(
                        adapter,
                        store,
                        #[cfg(feature = "semantic")]
                        // Reborrow per chunk; `Option<&mut T>` is not Copy.
                        #[allow(clippy::needless_option_as_deref)]
                        embedder.as_deref_mut(),
                        &mut buf,
                        &mut on_progress,
                    )?;
                }
            }
            if !buf.is_empty() {
                save_chunk(
                    adapter,
                    store,
                    #[cfg(feature = "semantic")]
                    // Reborrow per chunk; `Option<&mut T>` is not Copy.
                    #[allow(clippy::needless_option_as_deref)]
                    embedder.as_deref_mut(),
                    &mut buf,
                    &mut on_progress,
                )?;
            }
            producer
                .join()
                .map_err(|_| anyhow::anyhow!("adapter scan thread panicked"))?
        })?;

    if let Some(cursor) = new_cursor {
        trace!(agent = %adapter.kind(), cursor = %cursor, "saving checkpoint");
        store.upsert_checkpoint(&Checkpoint {
            agent: adapter.kind(),
            cursor,
            updated_at: Utc::now(),
        })?;
    }

    on_progress(SyncPhase::Done { total_records });

    Ok(total_records)
}

fn save_chunk(
    adapter: &dyn AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] embedder: Option<&mut embeddings::Embedder>,
    buf: &mut Vec<NativeRecord>,
    on_progress: &mut impl FnMut(SyncPhase),
) -> anyhow::Result<()> {
    on_progress(SyncPhase::Normalizing {
        record_count: buf.len(),
    });
    let batch = adapter.normalize(buf)?;
    debug!(
        agent = %adapter.kind(),
        sessions = batch.sessions.len(),
        messages = batch.messages.len(),
        "normalized batch chunk"
    );

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
        debug!(
            agent = %adapter.kind(),
            embedded,
            total = batch.messages.len(),
            "computed embeddings for chunk"
        );
    }

    buf.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::SyncSender;

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
        fn stream_changes_since(
            &self,
            _source_paths: &[String],
            _cursor: Option<&str>,
            tx: SyncSender<NativeRecord>,
        ) -> anyhow::Result<Option<String>> {
            let mut best: Option<&NativeRecord> = None;
            for rec in &self.records {
                if best.is_none_or(|b| {
                    rec.updated_at > b.updated_at
                        || (rec.updated_at == b.updated_at && rec.source_id > b.source_id)
                }) {
                    best = Some(rec);
                }
                tx.send(rec.clone())?;
            }
            Ok(best.map(|r| format!("{}\x1f{}", r.updated_at.to_rfc3339(), r.source_id)))
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

    #[test]
    fn sync_adapter_many_records_chunks() {
        let now = Utc::now();
        let records: Vec<NativeRecord> = (0..(CHUNK * 2 + 3))
            .map(|i| NativeRecord {
                source_id: format!("r{i}"),
                updated_at: now + chrono::Duration::seconds(i as i64),
                payload: Value::String(format!("content {i}")),
            })
            .collect();
        let adapter = FakeAdapter { records };
        let mut store = SqliteStore::open(":memory:").unwrap();
        store.init_schema().unwrap();

        #[cfg(feature = "semantic")]
        let count = sync_adapter(&adapter, &mut store, None, |_| {}).unwrap();
        #[cfg(not(feature = "semantic"))]
        let count = sync_adapter(&adapter, &mut store, |_| {}).unwrap();

        assert_eq!(count, CHUNK * 2 + 3);
        let sessions = store.list_sessions().unwrap();
        assert_eq!(sessions.len(), CHUNK * 2 + 3);
        let checkpoint = store.get_checkpoint("pi").unwrap();
        assert!(checkpoint.is_some());
        // The checkpoint cursor is the max record, independent of chunking.
        assert!(checkpoint.unwrap().contains(&format!("r{}", CHUNK * 2 + 2)));
    }
}
