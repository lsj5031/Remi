use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AgentKind {
    Pi,
    Droid,
    OpenCode,
    Claude,
    Amp,
}

impl AgentKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentKind::Pi => "pi",
            AgentKind::Droid => "droid",
            AgentKind::OpenCode => "opencode",
            AgentKind::Claude => "claude",
            AgentKind::Amp => "amp",
        }
    }
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AgentKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pi" => Ok(AgentKind::Pi),
            "droid" => Ok(AgentKind::Droid),
            "opencode" => Ok(AgentKind::OpenCode),
            "claude" => Ok(AgentKind::Claude),
            "amp" => Ok(AgentKind::Amp),
            _ => anyhow::bail!("unknown agent kind: {s}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub agent: AgentKind,
    pub source_ref: String,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub session_id: String,
    pub role: String,
    pub content: String,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub session_id: String,
    pub kind: String,
    pub payload: Value,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: String,
    pub session_id: String,
    pub path: String,
    pub checksum: String,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub agent: AgentKind,
    pub source_path: String,
    pub source_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub agent: AgentKind,
    pub cursor: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveRun {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub older_than_secs: i64,
    pub keep_latest: i64,
    pub dry_run: bool,
    pub executed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveItem {
    pub id: String,
    pub run_id: String,
    pub session_id: String,
    pub planned_delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeRecord {
    pub source_id: String,
    pub updated_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NormalizedBatch {
    pub sessions: Vec<Session>,
    pub messages: Vec<Message>,
    pub events: Vec<Event>,
    pub artifacts: Vec<Artifact>,
    pub provenance: Vec<Provenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArchiveCapability {
    Native,
    CentralizedCopy,
}

pub trait AgentAdapter {
    fn kind(&self) -> AgentKind;
    fn discover_source_paths(&self) -> anyhow::Result<Vec<String>>;
    fn scan_changes_since(
        &self,
        source_paths: &[String],
        cursor: Option<&str>,
    ) -> anyhow::Result<Vec<NativeRecord>>;
    fn normalize(&self, records: &[NativeRecord]) -> anyhow::Result<NormalizedBatch>;
    fn checkpoint_cursor(&self, records: &[NativeRecord]) -> Option<String>;
    fn archive_capability(&self) -> ArchiveCapability;
}

pub fn deterministic_id(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(&[0x1f]);
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_id_stable() {
        let id1 = deterministic_id(&["a", "b"]);
        let id2 = deterministic_id(&["a", "b"]);
        assert_eq!(id1, id2);
    }

    #[test]
    fn deterministic_id_different_inputs() {
        let id1 = deterministic_id(&["a", "b"]);
        let id2 = deterministic_id(&["a", "c"]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn deterministic_id_order_matters() {
        let id1 = deterministic_id(&["a", "b"]);
        let id2 = deterministic_id(&["b", "a"]);
        assert_ne!(id1, id2);
    }

    #[test]
    fn agent_kind_as_str() {
        assert_eq!(AgentKind::Pi.as_str(), "pi");
        assert_eq!(AgentKind::Droid.as_str(), "droid");
        assert_eq!(AgentKind::OpenCode.as_str(), "opencode");
        assert_eq!(AgentKind::Claude.as_str(), "claude");
        assert_eq!(AgentKind::Amp.as_str(), "amp");
    }
}
