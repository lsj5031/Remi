# Remi performance profile & architecture roadmap

Measured on this codebase (release build, synthetic data). All timings come from the
`#[ignore]`d benchmark tests:

- `cargo test -p store-sqlite --release bench_save_batch -- --ignored --nocapture`
- `cargo test -p cli --release bench_docs_reindex -- --ignored --nocapture`
- `cargo test -p cli --release bench_build_session_displays -- --ignored --nocapture`
- `cargo test -p cli --release bench_pipeline_first_sync -- --ignored --nocapture`

## Where time actually goes

First sync of 100k messages (50 JSONL session files):

| Phase | Cost | Share | Parallel? |
|---|---|---|---|
| scan + parse JSONL | ~80 ms | 11% | yes (rayon, per file) |
| normalize to batch | ~130 ms | 17% | no (serial) |
| save_batch (SQLite + FTS5) | ~540 ms | 72% | no (single connection) |

Key finding: **the SQLite write phase dominates the whole ingest pipeline.** Parsing is
already parallelized and cheap; the ceiling on throughput is the per-message
`messages` upsert + FTS5 tokenization/insert. This repeats for docs indexing: the first
index of 8000 files (~19 MB) is ~330–430 ms and is DB-write-bound, not I/O-bound
(forcing rayon to 1 thread changes nothing).

Peak RSS for that 100k-message first sync is **~480 MB** (records hold full JSON
payloads; the normalized batch duplicates content as Strings; SQLite buffers on top).
The pipeline materializes the entire history twice in RAM before anything is written.
This is the real scaling ceiling: a 1M-message first sync would need multiple GB.

### Verified on real agent histories

Profiled an actual first sync on this machine's real transcripts — Pi (29 files, 9.9 MB),
Droid (811 files, 183 MB), Claude (36 files, 3.6 MB), OpenCode (254 MB SQLite) — copied
into an isolated HOME and run through the real binary under `/usr/bin/time -l`:

| Agent | records | scan/parse | normalize | save+commit | total | write share |
|---|---|---|---|---|---|---|
| pi | 630 | 6 ms | 2 ms | 28 ms | 37 ms | **77%** |
| droid | 1725 | 79 ms | 3 ms | 199 ms | 283 ms | **70%** |
| claude | 78 | 2 ms | 0.3 ms | 42 ms | 45 ms | **93%** |
| opencode | 5013 | 626 ms (own SQLite read) | 9 ms | 523 ms | 1.16 s | 45% |

Total: **1.54 s wall, 251 MB peak RSS** for 7,410 messages / 574 sessions / 131 MB DB.
The write-phase dominance holds on real data for every JSONL-family adapter (70–93%);
OpenCode is the exception — 54% of its time is extracting rows from its own 254 MB SQLite
store, not Remi's writes. Peak RSS at 7.4k *real* (payload-heavy) messages is 251 MB,
consistent with the streaming-era bound (262 MB at 100k synthetic): memory no longer
scales with history size, so the pre-streaming 480 MB figure is unreachable today. A
no-change re-sync of the same corpus takes **0.13 s / 94 MB** thanks to the per-file tail
cursors.

## Architecture summary

```
discover ──▶ scan/parse (rayon, per file) ──▶ Vec<NativeRecord>  (all in RAM)
   ──▶ normalize (serial) ──▶ NormalizedBatch  (all in RAM)
   ──▶ save_batch (one SQLite tx) ──▶ messages + FTS5 mirror + provenance
```

- Checkpoint: Pi/Codex/Droid store a per-file tail map (`BTreeMap<path, FileTail>` with
  byte offset, mtime, anchor hash, and parse context); unchanged files are skipped on
  metadata alone, append-only growth resumes from the byte offset, and anything else
  re-parses fully (idempotent upserts). Amp/Claude/OpenCode keep the `max(ts, source_id)`
  composite cursor since their sources don't map to a single growing JSONL file.
- Source formats are heterogeneous. Pi/Codex/Droid/Amp are per-file JSONL. Claude reads
  three trees (`.claude/projects`, `.claude/transcripts`, local share) and **dedupes
  globally** by `dedupe_key` with priority/richness replacement before normalize.
  OpenCode normalizes against a global `SessionMetaIndex` loaded once (its own storage),
  not against a single session file. So "a session lives in one file" is only true for
  the JSONL-family adapters; any streaming design must keep adapter-global dedupe/meta
  state intact.
- Search = FTS5 BM25 + recency RRF, with a `LIKE '%…%'` substring fallback that is a
  full table scan (no index can serve it). At 100k messages a miss costs ~20 ms and
  grows linearly with the DB. Not yet a product problem; revisit when a real DB is
  large enough to feel it.
- Docs index = directory walk (metadata) → read/hash changed files → batched upsert
  into `documents` + `fts_documents`. Re-index is now metadata-only for unchanged files.
- Semantic search (optional feature) brute-forces cosine similarity over all embeddings
  loaded in RAM — O(N) per query, no ANN index. Same "wait for a large real DB" call.

## Already landed (measured)

| Change | Before → After |
|---|---|
| Docs re-index skips unchanged files (mtime+size), hash-verifies on change | re-index 8000 files ≈ 700 ms+ → **48 ms** |
| Docs upserts batched into one transaction | first index 8000 files ≈ 700 ms+ → **330–430 ms** |
| FTS sync uses `INSERT OR REPLACE` (no separate DELETE) | re-upsert 30k msgs 175 ms → **~120–150 ms** |
| Session list display: 3 batched queries instead of 2 per session | 969 µs → **~530 µs** |
| `plan_archive` preloads already-planned IDs + one transaction | N+1 EXISTS queries + autocommit per row eliminated |
| `delete_session_cascade` deletes FTS via messages index | no full FTS scan per delete |
| Per-file tail cursors (Pi/Codex/Droid) with anchor-guarded resume | incremental +1 msg/file ≈ 700 ms → **~2 ms**; no-change re-sync ≈ 700 ms → **~0.4 ms** |
| External-content FTS5 for messages (v4 migration, trigger-free manual sync) | DB shrinks 131 MB → **83 MB (−37%)** on the real corpus; idempotent re-sync 111 ms → **~80 ms** (unchanged content skips FTS) |

## Prioritized roadmap

### 1. Stream out of the adapters in bounded chunks — IMPLEMENTED
`AgentAdapter::stream_changes_since` is now the required trait method: it pushes
`NativeRecord`s into a `SyncSender`, and `sync_adapter` runs the scan on a scoped
thread against a bounded (`sync_channel(5000)`) channel while the main thread
normalizes + saves 5000-record chunks. Peak memory no longer scales with history size.

- Pi/Codex/Droid/Amp stream per file (parallel parse, one file materialized at a time;
  the shared `adapter_common::stream_jsonl_sources` helper drives it).
- Claude keeps its global cross-tree dedupe: candidates are materialized, then the
  deduped output is streamed (bounded dedupe is a later refinement).
- OpenCode keeps its SQLite cursor and global `SessionMetaIndex`; records are loaded
  then streamed.
- `stream_changes_since` now returns the new checkpoint cursor (per-file tail map for
  the JSONL family, composite max for the rest), replacing the old running-max logic in
  `sync_adapter`. Session merge semantics are preserved per chunk and the
  deterministic-id upserts keep re-syncs idempotent.

Measured: 100k-message first sync peak RSS **484 MB → 262 MB** (−46%) with parse
overlapping writes; total wall time unchanged; incremental sync verified end-to-end
(append a message → re-sync → exactly 2 messages, search finds the new one).
`scan_changes_since` remains as a provided collector for tests/callers.

### 2. Per-file tail cursors for append-only JSONL only (with rewrite detection) — IMPLEMENTED
Narrower than it sounds: it only helps JSONL files that grew by appending since the
last sync. Rewritten/truncated/compacted files make a stored byte offset unsafe, so the
resume is **guarded**:

- **Unchanged** (size + mtime match the recorded end state) → skip entirely, zero I/O.
- **Grew** → resume from the stored byte offset *only if* a blake3 hash of the last 4 KB
  before the offset still matches (the `anchor`). This catches in-place rewrites that
  touch the tail region; a mismatch (or a shrink / same-size rewrite) forces a full
  re-parse, which re-emits everything and relies on idempotent upserts.
- Each file's checkpoint carries the parse context needed for stable source_ids across
  a mid-file resume (`session_id`, cwd, title seed, `msg_index`, …) — the ordinals are
  positional, so they cannot be re-derived from a byte offset.
- Applies to **Pi/Codex/Droid only** (line-oriented JSONL where the ordinal + session
  context is per file). **Amp is excluded**: whole-document JSON per thread, so a byte
  offset is meaningless — mtime skip remains its fast path. Claude/OpenCode keep their
  global dedupe / SQL cursor and composite cursors.
- The checkpoint is now a deterministic `BTreeMap<path, FileTail>` JSON (`{"v":1,…}`).
  Legacy composite cursors are detected and honored with the old mtime-skip + filter
  behavior for exactly one sync, then migrated to the per-file format.

Measured (100k msgs / 50 files, release):

| Sync | Before (full re-parse + composite filter) | After (tail cursors) |
|---|---|---|
| First sync | ~800 ms | ~780 ms (unchanged; writes dominate) |
| Incremental, +1 msg/file | ~700 ms (100k lines re-parsed) | **~2 ms** (50 lines) |
| No-change re-sync | ~700 ms | **~0.4 ms** (metadata only) |

End-to-end verified with the real binary: append a message → re-sync → exactly 2
messages, no duplicates, search finds the new one, and the stored checkpoint is the
per-file format. Residual risk (documented in code): a content change with identical
size *and* mtime that leaves the tail region untouched is invisible to the guard — the
same trade-off as the docs mtime+size fast path.

### 3. Defer: external-content FTS5, indexed substring, semantic ANN
- External-content FTS5 (`content='messages'`): **measured on the real corpus** — the
  duplicated content blob is **50.6 MB (37.5% of the 131 MB DB)**, exactly equal to
  `messages.content` (45.6 MB) plus the duplicated message_id/session_id/ts columns,
  while the inverted index is only **18.7 MB (14%)** (`fts_messages_data` segment
  b-trees + 90 KB `_idx` + 98 KB `_docsize`). The prior assumption that "the index is a
  large fraction of FTS for short chat messages" does **not** hold here: these
  transcripts average ~6.1 KB/message (tool outputs, diffs), so content dominates the
  FTS footprint 2.7:1 and content-mode would reclaim **~38% of the DB**.
  Schema work is still real: `fts_messages` maps columns by name (`message_id` → `id`),
  deletes/updates need triggers or explicit content-supplying deletes, and snippet
  queries must join back to `messages` for content. Net call: **worth it if DB size
  matters** — the measurement gate the roadmap demanded is now satisfied.
- Substring `LIKE` fallback: ~20 ms miss at 100k messages, linear in DB size. Not a
  product problem yet.
- Semantic search: O(N) brute-force cosine; add ANN only when a real corpus is large
  enough to feel it.

## Anti-goals / measured dead ends

- Parallelizing the docs read/hash phase: no gain — first index is DB-write-bound, not
  I/O-bound (verified: `RAYON_NUM_THREADS=1` ≈ default).
- `INSERT OR REPLACE ... SELECT rowid FROM …` for FTS sync: 3× slower than a Rust-side
  rowid lookup + `INSERT OR REPLACE ... VALUES` (verified empirically).
- `INSERT ... ON CONFLICT ... RETURNING rowid` to skip the rowid SELECT: **slower** than
  the plain upsert + indexed SELECT (155–165 ms vs 111–121 ms for 30k cold inserts) —
  RETURNING row-materialization per statement costs more than the lookup it replaces.
- Chunking `normalize`/`save_batch` alone does not fix memory — the adapter's
  `Vec<NativeRecord>` payloads dominate; only streaming out of the adapter does.
