# Remi – Unified coding-agent session memory

## Commands
- **Build:** `cargo build --workspace`
- **Check:** `cargo clippy --workspace --all-targets -- -D warnings`
- **Format:** `cargo fmt --all` (check: `cargo fmt --all --check`)
- **Test all:** `cargo test --workspace`
- **Test single:** `cargo test -p <crate> <test_name>` (e.g. `cargo test -p store-sqlite schema_and_integrity`)
- **Docs regression smoke:** `cargo test -p cli docs_index_search_allowlist_and_regressions -- --exact`

## Architecture
Rust workspace (edition 2024) with a CLI binary (`crates/cli`) backed by library crates:
- **core-model** – canonical data types (`Session`, `Message`, `Event`, `Artifact`, `Provenance`, `Checkpoint`, `ArchiveRun`), the `AgentAdapter` trait, and `deterministic_id` (blake3).
- **store-sqlite** – SQLite persistence via `rusqlite` (FTS5 for lexical/docs search, `SqliteStore` API). DB at `~/.local/share/remi/remi.db`.
- **ingest** – `sync_adapter()` orchestrates discover → scan → normalize → save → checkpoint with `SyncPhase` progress reporting.
- **search** – session `search()` uses FTS5 BM25 + recency via Reciprocal Rank Fusion (RRF), with substring fallback and FTS query sanitization; docs search is separate and returns path/snippet hits.
- **archive** – plan/execute/restore session archival with dry-run default.
- **docs indexing** – CLI-owned filesystem scan for one local root at a time; allows `.md`, `.markdown`, `.txt`, `.rst`; skips hidden files, symlinks, unreadable files, and non-UTF-8 content.
- **adapter-common** – shared utilities for adapters (file discovery, JSONL parsing, composite cursor, content extraction).
- **adapters/{pi,droid,opencode,claude,amp,codex}** – per-agent implementations of `AgentAdapter`.

## Performance
- Parallel file I/O via `rayon` in adapter scan phases.
- SQLite WAL mode + prepared statement caching.
- Composite cursor (`{ts}\x1f{source_id}`) for correct incremental sync without data loss.

## CLI notes
- Search renders a session list first, then exports the chosen session to HTML/Markdown.
- Docs search is separate: `remi docs index --root <PATH>` populates docs tables, and `remi docs search <QUERY>` prints title/path/snippet hits without entering the session-search flow.
- Semantic builds accept `--semantic <auto|on|off>`, `--ort-dylib-path`, and `--auto-ort`.

## Code style
- Errors: `anyhow::Result` for app-level, `thiserror` for library error enums.
- Serde derives on all model types. IDs are deterministic blake3 hashes (`deterministic_id`).
- No comments unless complex logic requires context. Use `let`-`else` and `if let`-chains (edition 2024).
- Imports: `use` grouped as std → external crates → workspace crates; multi-imports with `{}`.
