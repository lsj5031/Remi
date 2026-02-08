Create a brand-new repository in a new directory named `Remi` and implement a Rust CLI that unifies session storage for Pi, Factory Droid, OpenCode, and Claude Code, with incremental sync, searchable memory, and safe archival.

## Scope you requested
- Start in a **fresh repo**: `Remi/`.
- Unified persistent memory for all four coding agents.
- Queryable by any agent.
- Incremental ingestion with checkpoints.
- Archive old sessions with default dry-run and optional hard-delete once validated.

## Phase 0: New repo bootstrap (Remi)
1. `mkdir Remi` and initialize as standalone git repo.
2. Rust workspace scaffold:
   - `crates/core-model`
   - `crates/store-sqlite`
   - `crates/ingest`
   - `crates/search`
   - `crates/archive`
   - `crates/adapters/pi`
   - `crates/adapters/droid`
   - `crates/adapters/opencode`
   - `crates/adapters/claude`
   - `crates/cli`
3. CI basics: fmt, clippy, tests.
4. Repo hygiene: `.gitignore`, MIT/Apache-2 dual license (or your preferred one).

## Canonical data model (SQLite v1)
- `agents`
- `sessions`
- `messages`
- `events`
- `artifacts`
- `provenance`
- `checkpoints`
- `archive_runs`
- `archive_items`
- `fts_messages` (FTS5)

Deterministic IDs with `blake3` for idempotent sync and dedupe.

## Adapter architecture
Common trait per agent:
- discover source paths
- scan changes since checkpoint
- normalize native records into canonical entities
- emit checkpoint cursor
- report archive capabilities and execute archive actions

Capability-based archival fallback:
- if native archive unsupported, copy raw source to centralized archive directory + manifest + checksum verification, then optional delete.

## Archival behavior
- Default: `dry-run` only.
- Retention policy: **age + keep-latest-N**.
- Central archive path: `~/.local/share/remi/archive/`.
- Hard-delete only with explicit flag and only after verified bundle+manifest write.

## Search behavior
- FTS5 with BM25 ranking for lexical relevance.
- Code-aware tokenizer (`unicode61 tokenchars '_./:-'`) for dev-friendly search.
- Reciprocal Rank Fusion (RRF) combining BM25 relevance + recency signals.
- FTS query sanitization (terms quoted, joined with OR) for safety.
- Substring fallback (LIKE) when FTS returns zero results.

## CLI commands
- `remi init`
- `remi sync --agent <pi|droid|opencode|claude|all>`
- `remi sessions list|show`
- `remi search query "..."`
- `remi archive plan --older-than <dur> --keep-latest <n>`
- `remi archive run --plan <id> --dry-run`
- `remi archive run --plan <id> --execute --delete-source`
- `remi archive restore --bundle <path>`
- `remi doctor`

## Tier 2: Optional local embeddings (future)
Gate behind `--features semantic` cargo feature flag. Do not implement until
BM25 + RRF demonstrably fails on real queries where wording diverges from
stored messages (e.g. "caching" vs "memoization").

### Model
- all-MiniLM-L6-v2 ONNX (~23 MB, 384 dims, CPU-only, sub-50ms per query).
- User supplies model path via `REMI_EMBED_MODEL` env var or `--model` flag;
  no auto-download.

### Dependencies
- `ort` crate (ONNX Runtime Rust bindings) behind the `semantic` feature.
- `tokenizers` crate for WordPiece tokenization (also feature-gated).

### Schema addition
```sql
CREATE TABLE IF NOT EXISTS message_embeddings (
  message_id TEXT PRIMARY KEY,
  dim INTEGER NOT NULL,
  vec BLOB NOT NULL,
  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
);
```
Store vectors as raw `f32` little-endian blobs (`dim * 4` bytes).

### Ingest changes
- In `sync_adapter`, after `save_batch`, if the semantic feature is enabled
  and a model is loaded, compute embeddings for new messages in batches of 64
  and call `store.save_embeddings_blob()`.
- Add a `remi embed --rebuild` CLI command to backfill embeddings for all
  existing messages.

### Search changes (in `search::search()`)
- Embed the query string once.
- Brute-force cosine similarity against all stored vectors (load into memory
  on first search; ~15 MB RAM for 100k × 384-dim f32 vectors).
- Produce a semantic ranked list (top 200).
- Add as a third RRF signal with weight 0.5:
  ```
  rrf_score(d) = bm25_w/(k+rank_bm25) + recency_w/(k+rank_recency) + semantic_w/(k+rank_semantic)
  ```
- No ANN index needed until corpus exceeds ~500k messages.

### When to add ANN
If brute-force cosine exceeds 100ms on the target corpus, consider:
- `usearch` crate (HNSW, pure Rust, no C++ deps) or
- SQLite `vec0` extension (if available).

### Acceptance criteria
- `cargo build` without `--features semantic` does not pull ort/tokenizers.
- `remi search query "caching"` finds messages about "memoization" when
  embeddings are available.
- Embedding computation is idempotent (re-sync does not recompute unchanged
  messages).

## Implementation order
- [x] 1. Repo bootstrap + workspace + SQLite schema/migrations.
- [x] 2. Ingestion engine + checkpoints + provenance.
- [x] 3. All four adapters (ingest first).
- [x] 4. FTS search with recency ranking.
- [x] 5. Archive planner/executor + restore.
- [ ] 6. Hardening: idempotency, corruption, scale tests.

## Acceptance criteria
- Fresh `Remi` repo builds/tests cleanly.
- `sync --agent all` is idempotent.
- Cross-agent search returns provenance-linked results.
- Archive dry-run prints precise actions.
- Hard-delete cannot happen before archive verification.

If you approve this spec, I will proceed by creating `Remi/` as a fresh repository and implementing Phase 0 + Phase 1 first.