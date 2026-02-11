use std::{fs, path::PathBuf};

use anyhow::Context;
use chrono::Duration;
use serde::{Deserialize, Serialize};
use store_sqlite::SqliteStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub run_id: String,
    pub sessions: Vec<String>,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveBundle {
    pub run_id: String,
    pub sessions: Vec<core_model::Session>,
    pub messages: Vec<core_model::Message>,
    pub events: Vec<core_model::Event>,
    pub artifacts: Vec<core_model::Artifact>,
    pub provenance: Vec<core_model::Provenance>,
}

pub fn archive_plan(
    store: &SqliteStore,
    older_than: Duration,
    keep_latest: usize,
) -> anyhow::Result<String> {
    let run = store.plan_archive(older_than, keep_latest)?;
    Ok(run.id)
}

pub fn archive_run(
    store: &SqliteStore,
    run_id: &str,
    execute: bool,
    delete_source: bool,
) -> anyhow::Result<String> {
    let items = store.archive_items_for_run(run_id)?;
    if !execute {
        return Ok(format!(
            "dry-run: would archive {} sessions for run {}",
            items.len(),
            run_id
        ));
    }

    let base = dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("remi")
        .join("archive")
        .join(run_id);
    fs::create_dir_all(&base)?;

    let mut bundle = ArchiveBundle {
        run_id: run_id.to_string(),
        sessions: Vec::new(),
        messages: Vec::new(),
        events: Vec::new(),
        artifacts: Vec::new(),
        provenance: Vec::new(),
    };
    for item in &items {
        if let Some(session) = store.get_session(&item.session_id)? {
            bundle.sessions.push(session);
        }
        bundle
            .messages
            .extend(store.get_session_messages(&item.session_id)?);
        bundle
            .events
            .extend(store.get_session_events(&item.session_id)?);
        bundle
            .artifacts
            .extend(store.get_session_artifacts(&item.session_id)?);
        bundle
            .provenance
            .extend(store.get_provenance_for_session(&item.session_id)?);
    }

    let payload = serde_json::to_vec_pretty(&bundle)?;
    let checksum = blake3::hash(&payload).to_hex().to_string();
    let bundle_path = base.join("sessions.json");
    fs::write(&bundle_path, &payload)?;

    let reloaded = fs::read(&bundle_path).with_context(|| "verify bundle write")?;
    let verify = blake3::hash(&reloaded).to_hex().to_string();
    if verify != checksum {
        anyhow::bail!("archive verification failed; refusing deletion");
    }

    let manifest = ArchiveManifest {
        run_id: run_id.to_string(),
        sessions: bundle.sessions.iter().map(|s| s.id.clone()).collect(),
        checksum,
    };
    fs::write(
        base.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    if delete_source {
        for item in &items {
            if item.planned_delete {
                store.delete_session_cascade(&item.session_id)?;
            }
        }
    }

    store.mark_archive_executed(run_id, false)?;
    Ok(format!("executed: archived run {}", run_id))
}

pub fn archive_restore(store: &mut SqliteStore, bundle_path: &str) -> anyhow::Result<String> {
    let bytes = fs::read(bundle_path)?;
    let bundle: ArchiveBundle = serde_json::from_slice(&bytes)?;
    let batch = core_model::NormalizedBatch {
        sessions: bundle.sessions,
        messages: bundle.messages,
        events: bundle.events,
        artifacts: bundle.artifacts,
        provenance: bundle.provenance,
    };
    let count = batch.sessions.len();
    store.save_batch(&batch)?;
    Ok(format!("restored {} sessions", count))
}
