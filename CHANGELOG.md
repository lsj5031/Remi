# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
