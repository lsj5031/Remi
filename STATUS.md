# Status: main branch (post-v0.1.2)

## Completed
- [x] **Phase 0: Repo Bootstrap**: Workspace structure, `Cargo.toml`, `git init`, `.gitignore`.
- [x] **Phase 1: Ingestion Engine**: `ingest` crate, `SyncPhase`, `checkpoint` logic.
- [x] **Phase 2: Adapters**: `pi`, `droid`, `opencode`, `claude`, `amp`, `codex` implemented with `adapter-common`.
- [x] **Phase 3: Persistence**: `store-sqlite` with WAL, schema, transactional writes.
- [x] **Phase 4: Search**: FTS5 BM25 + Recency RRF, substring fallback.
- [x] **Phase 5: Archive**: Plan/Run/Restore with dry-run and checksum verification.
- [x] **Docs indexing/search**:
    - `remi docs index --root <PATH>` indexes one local docs root at a time into the same SQLite database.
    - `remi docs search <QUERY>` searches indexed docs separately from session search.
    - Incremental re-index reconciles renamed/deleted files after a successful rerun.
    - File policy currently allows `.md`, `.markdown`, `.txt`, `.rst`; hidden files, symlinks, unreadable files, and non-UTF-8 content are skipped.
- [x] **Tier 2: Semantic Search**:
    - Optional `embeddings` crate (ONNX/`ort`).
    - Configurable via `~/.config/remi/config.toml`.
    - Auto-detects model under `models/bge-small-en-v1.5` or `~/.cache/remi/bge-small-en-v1.5`.
    - `--ort-dylib-path` / `--auto-ort` for ONNX Runtime loading.
    - Integrated into `ingest` and `search`.
    - `remi embed --rebuild` command.
- [x] **Structured Tracing**: `tracing` instrumentation across all crates with json+env-filter support.
- [x] **CLI**: `remi` command with `init`, `sync`, `docs`, `sessions`, `search`, `archive`, `embed`, `doctor`.
- [x] **Release**: GitHub Actions workflow builds simple + bundled artifacts.

## Verification
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `cargo fmt --all --check` passes.
- `cargo test --workspace` passes.
- Code review confirms alignment with `plan.md`.

## Next Steps
- **Phase 6: Hardening**: Add property-based tests, stress tests for large datasets, and corruption recovery tests.
