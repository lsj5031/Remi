use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use core_model::{AgentKind, Message, NormalizedBatch, Session};
use rusqlite::Connection;
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

#[test]
fn docs_index_updates_stay_store_searchable_and_write_rfc3339_timestamps() {
    let data_home = fresh_data_home();
    let docs_root = data_home.join("docs-contract-root");
    fs::create_dir_all(&docs_root).unwrap();
    let doc_path = docs_root.join("one.md");
    fs::write(&doc_path, "# One\n\nalpha token\n").unwrap();

    let first = remi_cmd(&data_home)
        .args(["docs", "index", "--root", docs_root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "first docs index failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );

    thread::sleep(Duration::from_secs(1));
    fs::write(&doc_path, "# One\n\nalpha token updated\n").unwrap();

    let second = remi_cmd(&data_home)
        .args(["docs", "index", "--root", docs_root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        second.status.success(),
        "second docs index failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );

    let db_path = data_home.join("remi").join("remi.db");
    let store = SqliteStore::open(&db_path).unwrap();
    let hits = store.search_documents_lexical("alpha", 10).unwrap();
    assert_eq!(hits.len(), 1, "hits={hits:?}");
    assert_eq!(hits[0].relative_path, "one.md");
    assert!(hits[0].snippet.contains("alpha"), "hits={hits:?}");

    let canonical_root = docs_root.canonicalize().unwrap();
    let root = store
        .get_doc_root(canonical_root.to_str().unwrap())
        .unwrap()
        .unwrap();
    let doc = store
        .get_document_by_path(&root.root_id, "one.md")
        .unwrap()
        .unwrap();
    assert_eq!(doc.title, "One");

    let conn = Connection::open(&db_path).unwrap();
    let modified_at: String = conn
        .query_row(
            "SELECT modified_at FROM documents WHERE root_id = ?1 AND relative_path = ?2",
            [&root.root_id, "one.md"],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        DateTime::parse_from_rfc3339(&modified_at).is_ok(),
        "modified_at={modified_at}"
    );
}
