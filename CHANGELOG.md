# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-08-14

### Added

- Bundled release assets for macOS arm64 and Windows x64 (`remi-macos-arm64-bundled.tar.gz`, `remi-windows-x64-bundled.zip`), each shipping the binary built with the `semantic` feature plus the ONNX Runtime and BGE model so `remi embed` runs offline on those platforms too. The release workflow's model fetch now uses a direct keep-list download on all three platforms instead of the full HF snapshot + prune.

### Fixed

- The embedder fed over-long messages (beyond the model's static 512-token sequence length) to the ONNX graph, which failed inside the pooling layer and silently skipped those messages (`remi embed --rebuild` embedded only 490 of 630 real messages). Inputs are now truncated to the model's declared sequence dimension, and embedding failures log a warning instead of failing silently.

## [0.2.2] - 2026-08-14

### Fixed

- The `semantic` feature never compiled: the ingest loop moved the `Option<&mut Embedder>` into `save_chunk` (E0382), two `std::env::set_var` calls were unsafe under edition 2024 (E0133), and a clippy `collapsible_if` lint blocked `-D warnings`. `remi embed` and `--semantic` search are now buildable and verified end-to-end against a downloaded ONNX Runtime + BGE model.
- Integration tests hard-coded the Linux XDG data layout, so they seeded a database the app never opened on macOS (`doctor`, session JSON search, and docs re-index all failed there). Tests now mirror `dirs::data_dir()` per platform; the full suite is green on macOS.

### Changed

- The bundled Linux release asset is now built with `--features semantic`, so the artifact that ships the ONNX Runtime + model actually contains a `remi embed` that can run offline.

## [0.2.1] - 2026-08-14

### Changed

- Bundled Linux release asset pruned to a single ONNX model plus tokenizer/config files: `remi-linux-x64-bundled.tar.gz` drops from ~320 MB to ~85 MB with no functional change (the embedder only loads `model.onnx` and `tokenizer.json`).

## [0.2.0] - 2026-08-14

### Added

- Separate docs indexing/search via `remi docs index --root <PATH>` and `remi docs search <QUERY>`, stored in the same SQLite database as synced sessions.
- Incremental docs reconciliation for rename/delete flows, plus file-policy enforcement for `.md`, `.markdown`, `.txt`, and `.rst` roots.
- Per-file tail cursors for Pi/Codex/Droid JSONL transcripts: incremental syncs resume from a guarded byte offset instead of re-parsing the whole file.
- Benchmark suite (`-- --ignored`) covering save_batch, docs re-index, session displays, and the full sync pipeline.
- `docs/PERFORMANCE.md` with the measured profile and architecture roadmap.

### Changed

- **Streaming ingest**: adapters stream records through a bounded channel; `sync_adapter` normalizes/saves in chunks. First-sync peak RSS drops ~46% and no longer scales with history size.
- **External-content FTS5 for messages**: the duplicated content blob is dropped (only the inverted index remains), shrinking the database ~37% (measured 131 MB → 83 MB on a real corpus); unchanged-content re-syncs now skip FTS writes entirely.
- Docs re-index is metadata-only for unchanged files (mtime+size guard) with batched upserts; session list rendering uses batched queries; archive planning preloads seen IDs; FTS sync drops the separate DELETE.
- README, STATUS, and AGENTS project notes now document the docs-search workflow, file allowlist, and the separation between docs search and session search accurately.

### Fixed

- `delete_session_cascade` deletes FTS entries through the messages index (no full FTS scan); Claude keeps its global cross-tree dedupe in the streaming path; OpenCode's SQLite cursor and session-meta index are preserved.

## [0.1.2] - 2026-04-08

### Added

- JSONL progress reporting for `scripts/remi-diary.sh`, including per-agent sync events for easier timer diagnostics.

### Changed

- `remi-diary` now prefers the installed `remi` binary before falling back to a bundled copy.

### Fixed

- Claude sync performance by replacing session-wide FTS rebuilds with rowid-targeted updates for touched messages only.
- Nightly diary generation now completes reliably against current Claude/Codex backlogs instead of appearing stuck during sync.

## [0.1.0] - 2026-04-01

### Added

- Structured `tracing` instrumentation across all crates (adapters, ingest, search, store-sqlite, archive, CLI).
- `tracing-subscriber` with `json` and `env-filter` feature flags for structured log output.
- Per-adapter scan/save/checkpoint trace and debug spans.
- Lexical search and substring search debug logging.
- Archive bundle verification and cascade delete trace logging.
- Ingest embedding computation progress logging.

### Changed

- Improved diff/render parsing and hardened codex diary command resolution.
- Switched `remi-diary` summary runner to `codex exec` for safer prompt piping.

## [0.0.5] - 2026-02-23

### Added

- OpenCode SQLite ingestion fallback (reads messages and parts from `opencode.db`).
- Modularized session export rendering in CLI.

### Changed

- Refreshed README agent and search option documentation.
- Polished README demo and added cross-platform release packaging.

### Fixed

- Workspace path extraction for `.factory/sessions` and `.pi` directories in diary script.

## [0.0.4] - 2026-02-11

### Added

- Codex adapter for ingesting Codex session transcripts.
- Cross-platform release packaging (macOS/Windows workflow support).

### Changed

- Improved Pi and Droid message handling during normalization.

## [0.0.3] - 2026-02-09

### Fixed

- Prevented duplicate FTS entries by explicit delete before insert.

## [0.0.2] - 2026-02-08

### Added

- AMP adapter and wire sync support.
- Transcript normalization and deduplication improvements.

### Fixed

- Stabilized incremental scan cursors.

## [0.0.1] - 2026-02-06

### Added

- Initial release.
- Unified session memory for Pi, Factory Droid, OpenCode, Claude Code, and Amp.
- Incremental sync with checkpointed ingestion.
- FTS5 lexical search with BM25 + recency RRF ranking.
- Substring fallback search.
- Archive plan/run/restore with dry-run defaults.
- CLI with `init`, `sync`, `sessions`, `search`, `archive`, `doctor` commands.
- Optional semantic search via ONNX Runtime + BGE embeddings.
- GitHub Actions CI and release workflows.
