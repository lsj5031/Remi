use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use core_model::{AgentKind, Message, NormalizedBatch, Session};
use serde_json::Value;
use store_sqlite::SqliteStore;

fn fresh_data_home() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("remi-cli-tests-{}-{unique}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn remi_cmd(data_home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_remi"));
    cmd.env("HOME", data_home).env("XDG_DATA_HOME", data_home);
    cmd
}

fn seed_session_store(data_home: &Path, query_term: &str) {
    let db_path = data_home.join("remi").join("remi.db");
    fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    let mut store = SqliteStore::open(&db_path).unwrap();
    store.init_schema().unwrap();

    let now = Utc::now();
    let batch = NormalizedBatch {
        sessions: vec![Session {
            id: "session-1".to_string(),
            agent: AgentKind::Pi,
            source_ref: "test-ref".to_string(),
            title: "docs regression seed".to_string(),
            created_at: now,
            updated_at: now,
        }],
        messages: vec![Message {
            id: "message-1".to_string(),
            session_id: "session-1".to_string(),
            role: "user".to_string(),
            content: format!("seeded searchable content {query_term}"),
            ts: now,
        }],
        events: vec![],
        artifacts: vec![],
        provenance: vec![],
    };

    store.save_batch(&batch).unwrap();
}

#[test]
fn doctor_remains_session_centric() {
    let data_home = fresh_data_home();
    seed_session_store(&data_home, "doctor-regression-token");

    let output = remi_cmd(&data_home).arg("doctor").output().unwrap();
    assert!(
        output.status.success(),
        "doctor failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("integrity_check=ok"), "stdout={stdout}");
    assert!(stdout.contains("sessions=1"), "stdout={stdout}");
}

#[test]
fn search_query_preserves_session_json_flow() {
    let data_home = fresh_data_home();
    seed_session_store(&data_home, "known-session-term");

    let output = remi_cmd(&data_home)
        .args([
            "search",
            "query",
            "known-session-term",
            "--format",
            "json",
            "--no-interactive",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "search query failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["query"], "known-session-term");
    assert_eq!(json["selected_session_id"], "session-1");
    let sessions = json["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1, "json={json}");
    assert_eq!(sessions[0]["id"], "session-1");
    assert_eq!(sessions[0]["title"], "docs regression seed");
    assert!(
        sessions[0]["snippet"]
            .as_str()
            .unwrap()
            .contains("known-session-term"),
        "json={json}"
    );
}

#[test]
fn search_query_empty_db_still_exits_cleanly() {
    let data_home = fresh_data_home();

    let output = remi_cmd(&data_home)
        .args([
            "search",
            "query",
            "no-results-term",
            "--format",
            "json",
            "--no-interactive",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "search query failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).trim().is_empty(),
        "stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn docs_index_search_allowlist_and_regressions() {
    let data_home = fresh_data_home();
    let docs_root = data_home.join("docs-root");
    fs::create_dir_all(docs_root.join("nested")).unwrap();
    seed_session_store(&data_home, "known-session-term");

    fs::write(
        docs_root.join("guide.md"),
        "# Guide\n\nunique-doc-token lives here.\n",
    )
    .unwrap();
    fs::write(
        docs_root.join("nested").join("notes.markdown"),
        "Nested note with unique-doc-token.",
    )
    .unwrap();
    fs::write(docs_root.join("plain.txt"), "plain unique-doc-token text").unwrap();
    fs::write(
        docs_root.join("reference.rst"),
        "Reference\n=========\n\nunique-doc-token\n",
    )
    .unwrap();
    fs::write(docs_root.join(".hidden.md"), "hidden unique-doc-token").unwrap();
    fs::write(docs_root.join("binary.txt"), [0x66, 0x6f, 0x80, 0x6f]).unwrap();

    #[cfg(unix)]
    std::os::unix::fs::symlink(docs_root.join("guide.md"), docs_root.join("guide-link.md"))
        .unwrap();

    let index = remi_cmd(&data_home)
        .args(["docs", "index", "--root", docs_root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        index.status.success(),
        "docs index failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&index.stdout),
        String::from_utf8_lossy(&index.stderr)
    );

    let search = remi_cmd(&data_home)
        .args(["docs", "search", "unique-doc-token"])
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "docs search failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&search.stdout),
        String::from_utf8_lossy(&search.stderr)
    );

    let stdout = String::from_utf8_lossy(&search.stdout);
    assert!(stdout.contains("guide.md"), "stdout={stdout}");
    assert!(stdout.contains("notes.markdown"), "stdout={stdout}");
    assert!(stdout.contains("plain.txt"), "stdout={stdout}");
    assert!(stdout.contains("reference.rst"), "stdout={stdout}");
    assert!(!stdout.contains(".hidden.md"), "stdout={stdout}");
    assert!(!stdout.contains("guide-link.md"), "stdout={stdout}");

    let query = remi_cmd(&data_home)
        .args([
            "search",
            "query",
            "known-session-term",
            "--format",
            "json",
            "--no-interactive",
        ])
        .output()
        .unwrap();
    assert!(query.status.success());

    let doctor = remi_cmd(&data_home).arg("doctor").output().unwrap();
    assert!(doctor.status.success());
}
