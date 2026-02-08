use core_model::{AgentAdapter, AgentKind, ArchiveCapability, NativeRecord, NormalizedBatch};

pub struct PiAdapter;

impl AgentAdapter for PiAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Pi
    }

    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>> {
        let base = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
        let mut out = Vec::new();
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".pi/agent/sessions"),
            "jsonl",
        ));
        out.extend(adapter_common::collect_files_with_ext(
            &base.join(".pi/sessions"),
            "jsonl",
        ));
        Ok(out)
    }

    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>> {
        adapter_common::load_jsonl(source_paths, cursor)
    }

    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch> {
        Ok(adapter_common::normalize_jsonl_records(
            AgentKind::Pi,
            records,
        ))
    }

    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String> {
        adapter_common::checkpoint_cursor_from_records(records)
    }

    fn archive_capability(&self) -> ArchiveCapability {
        ArchiveCapability::CentralizedCopy
    }
}
