# Status: Implementation Complete (Phase 0-5 + Tier 2)

## Completed
- [x] **Phase 0: Repo Bootstrap**: Workspace structure, `Cargo.toml`, `git init`, `.gitignore`.
- [x] **Phase 1: Ingestion Engine**: `ingest` crate, `SyncPhase`, `checkpoint` logic.
- [x] **Phase 2: Adapters**: `pi`, `droid`, `opencode`, `claude` implemented with `adapter-common`.
- [x] **Phase 3: Persistence**: `store-sqlite` with WAL, schema, transactional writes.
- [x] **Phase 4: Search**: FTS5 BM25 + Recency RRF, substring fallback.
- [x] **Phase 5: Archive**: Plan/Run/Restore with dry-run and checksum verification.
- [x] **Tier 2: Semantic Search**:
    - Optional `embeddings` crate (ONNX/`ort`).
    - Configurable via `~/.config/remi/config.toml`.
    - Auto-detects model under `models/bge-small-en-v1.5` or `~/.cache/remi/bge-small-en-v1.5`.
    - `--ort-dylib-path` / `--auto-ort` for ONNX Runtime loading.
    - Integrated into `ingest` and `search`.
    - `remi embed --rebuild` command.
- [x] **CLI**: `remi` command with `init`, `sync`, `sessions`, `search`, `archive`, `embed`, `doctor`.
- [x] **Release**: GitHub Actions workflow builds simple + bundled artifacts.

## Verification
- `cargo check --workspace` passes (including `--features semantic`).
- `cargo test --workspace` passes (58 tests across all crates).
- Code review confirms alignment with `plan.md`.

## Next Steps
- **Phase 6: Hardening**: Add property-based tests, stress tests for large datasets, and corruption recovery tests.
