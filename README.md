# Remi

[![CI](https://github.com/lsj5031/Remi/actions/workflows/ci.yml/badge.svg)](https://github.com/lsj5031/Remi/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lsj5031/Remi?display_name=tag)](https://github.com/lsj5031/Remi/releases)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

Unified coding-agent session memory for **Pi**, **Factory Droid**, **OpenCode**, **Claude Code**, **Amp**, and **Codex**.

Tired of hunting through multiple agent transcript formats and folders? Remi gives you one searchable memory layer.

Remi ingests local agent transcripts into one SQLite database, keeps sync state with checkpoints, supports ranked search, and provides safe archive/restore workflows.

Current support status: **Linux-first** (default source discovery paths are validated on Linux).

---

## Table of contents

- [What Remi does](#what-remi-does)
- [Demo](#demo)
- [Quickstart](#quickstart)
- [Install / build](#install--build)
- [Data locations](#data-locations)
- [Supported agent sources](#supported-agent-sources)
- [CLI reference](#cli-reference)
  - [`remi init`](#remi-init)
  - [`remi sync`](#remi-sync)
  - [`remi sessions`](#remi-sessions)
  - [`remi search query`](#remi-search-query)
  - [`remi archive`](#remi-archive)
  - [`remi doctor`](#remi-doctor)
- [Semantic search (optional feature)](#semantic-search-optional-feature)
- [End-to-end workflow examples](#end-to-end-workflow-examples)
- [Helper scripts (examples)](#helper-scripts-examples)
- [Architecture at a glance](#architecture-at-a-glance)
- [Release artifacts](#release-artifacts)
- [Homebrew (optional)](#homebrew-optional)
- [License](#license)

---

## What Remi does

- **Incremental sync** from multiple coding-agent sources.
- **Deterministic IDs** (`blake3`) for idempotent upserts.
- **Checkpointed ingestion** using a composite cursor (`timestamp + source_id`) to avoid missing same-timestamp records.
- **Structured content normalization** including tool-call/tool-result payloads into searchable text.
- **Lexical search** using SQLite FTS5 + BM25.
- **Ranking fusion** with recency via Reciprocal Rank Fusion (RRF).
- **Substring fallback** when lexical matches are empty.
- **Two-layer search UX**: ranked session list first, then export selected session to HTML/Markdown (or emit JSON).
- **Archive planning/execution/restore** with dry-run defaults and verification before optional deletion.

---

## Demo

Full CLI demo (55s):

![Remi CLI demo](docs/assets/remi-demo.gif)

---

## Quickstart

```bash
remi init && remi sync --agent all && remi search query "panic"
```

If `remi` is not on your path yet, run from source:

```bash
cargo run -p cli -- init
cargo run -p cli -- sync --agent all
cargo run -p cli -- search query "panic"
```

---

## Install / build

### Install from source

```bash
cargo install --path crates/cli
```

### Install from git (optional)

```bash
cargo install --git https://github.com/lsj5031/Remi --bin remi
```

### Build workspace

```bash
cargo build --workspace
```

### Run from source

```bash
cargo run -p cli -- --help
```

---

## Data locations

Default paths on Linux (via `dirs` crate):

- **Database**: `~/.local/share/remi/remi.db`
- **Search exports** (HTML/Markdown default output): `~/.local/share/remi/exports/`
- **Archive bundles**: `~/.local/share/remi/archive/<run_id>/`

macOS and Windows builds are available in releases, but default agent source discovery paths are currently Linux-oriented.

---

## Supported agent sources

Remi currently discovers and ingests from:

| Agent | Paths scanned |
|---|---|
| Pi | `~/.pi/agent/sessions/**/*.jsonl`, `~/.pi/sessions/**/*.jsonl` |
| Factory Droid | `~/.factory/sessions/**/*.jsonl`, `~/.local/share/factory-droid/sessions/**/*.jsonl` |
| OpenCode | `~/.local/share/opencode/opencode.db` (preferred) or legacy `~/.local/share/opencode/storage/message/**/*.json` (+ part text from `~/.local/share/opencode/storage/part/<message_id>/*.json`; session metadata from `~/.local/share/opencode/storage/session/**/*.json`) |
| Claude Code | `~/.claude/transcripts/**/*.jsonl`, `~/.claude/projects/**/*.jsonl`, `~/.local/share/claude-code/**/*.jsonl` |
| Amp | `~/.local/share/amp/threads/**/*.json` |
| Codex | `~/.codex/sessions/**/*.jsonl` |

---

## CLI reference

Top-level commands:

```text
remi init
remi sync --agent <pi|droid|opencode|claude|amp|codex|all>
remi sessions <list|show>
remi search query <QUERY> [options]
remi archive <plan|run|restore>
remi doctor
```

If built with `--features semantic`, Remi also supports:
- `remi embed --rebuild`
- Global flags: `remi --ort-dylib-path <PATH> ...` and `remi --auto-ort ...`

### `remi init`

Initializes/open database schema (schema is also initialized automatically by other commands).

```bash
remi init
```

---

### `remi sync`

Sync a specific adapter or all adapters:

```bash
remi sync --agent pi
remi sync --agent droid
remi sync --agent opencode
remi sync --agent claude
remi sync --agent amp
remi sync --agent codex
remi sync --agent all
```

Behavior:

- Discovers source files.
- Scans only records after the last checkpoint.
- Normalizes to canonical sessions/messages/provenance.
- Upserts into SQLite + refreshes FTS rows for touched sessions.
- Updates checkpoint cursor.

---

### `remi sessions`

List sessions:

```bash
remi sessions list
```

Show one session’s messages:

```bash
remi sessions show <session_id>
```

Example:

```bash
remi sessions show 0d5f0e...c9a
```

---

### `remi search query`

Usage:

```bash
remi search query [OPTIONS] <QUERY>
```

Options:

- `--format <html|markdown|json>` (default: `html`)
- `--no-interactive`
- `--select <auto|index>` (default: `auto`)
- `--index <N>` (required when `--select index` in non-interactive mode)
- `--agent <STRING>`
- `--title <STRING>`
- `--id <STRING>`
- `--contains <STRING>`
- `--raw-fts`
- `--html-safety <strict|relaxed|trusted>` (default: `relaxed`)
- `--output-dir <PATH>`

#### Interactive mode (default)

```bash
remi search query "retry logic"
```

Flow:
1. Remi ranks matching sessions.
2. You can type an optional fuzzy filter.
3. You choose an index.
4. Remi exports selected session (HTML by default) and prints the file path.

Interactive fuzzy filter supports field prefixes:

```text
agent:claude title:auth contains:oauth refresh token
```

Supported fuzzy fields:
- `agent:`
- `title:`
- `id:`
- `contains:`

#### Non-interactive examples

Auto-select top result, export Markdown:

```bash
remi search query "panic on startup" \
  --no-interactive \
  --select auto \
  --format markdown
```

Select exact ranked session index:

```bash
remi search query "sql migration" \
  --no-interactive \
  --select index \
  --index 2 \
  --format html \
  --output-dir ./exports
```

Emit JSON instead of writing HTML/Markdown:

```bash
remi search query "cache invalidation" \
  --no-interactive \
  --select auto \
  --format json
```

Filter sessions before selection:

```bash
remi search query "build failure" \
  --no-interactive \
  --agent droid \
  --title release \
  --contains linker
```

---

### `remi archive`

#### 1) Create an archive plan

```bash
remi archive plan --older-than 30d --keep-latest 5
```

- `--older-than` uses human duration parsing (examples: `30d`, `12h`, `90m`)
- `--keep-latest` is applied **per agent**
- Output format includes: `plan <run_id>`

#### 2) Dry-run an archive run (default behavior)

```bash
remi archive run --plan <run_id>
```

Equivalent explicit dry-run:

```bash
remi archive run --plan <run_id> --dry-run
```

#### 3) Execute archive

```bash
remi archive run --plan <run_id> --execute
```

This writes:
- `~/.local/share/remi/archive/<run_id>/sessions.json`
- `~/.local/share/remi/archive/<run_id>/manifest.json`

Remi verifies the bundle checksum after writing before allowing deletion.

#### 4) Execute and delete source sessions from DB

```bash
remi archive run --plan <run_id> --execute --delete-source
```

Flag precedence:
- Writes/deletes only occur when `--execute` is set.
- If both `--execute` and `--dry-run` are passed, `--dry-run` wins and execution is suppressed.

#### 5) Restore from a bundle

```bash
remi archive restore --bundle ~/.local/share/remi/archive/<run_id>/sessions.json
```

---

### `remi doctor`

Run integrity checks and basic stats:

```bash
remi doctor
```

Current output includes:
- SQLite `PRAGMA integrity_check` result
- total session count

---

## Semantic search (optional feature)

Semantic support is feature-gated at compile time.

### Build with semantic feature

```bash
cargo build -p cli --features semantic
```

When built with `semantic`, additional CLI surface is enabled:

- Global flags on `remi`:
  - `--ort-dylib-path <PATH>`
  - `--auto-ort`
- `remi embed --rebuild`
- `remi search query ... --semantic <auto|on|off>`

### Semantic config

`~/.config/remi/config.toml`:

```toml
[semantic]
enabled = true
model_path = "/path/to/bge-small-en-v1.5"
pooling = "cls" # or "mean"
query_prefix = "Represent this sentence for searching relevant passages: "
```

Model directory must contain:
- `model.onnx`
- `tokenizer.json`

### Auto-detected model locations

If `model_path` is not set, Remi checks:
- `<binary_dir>/models/bge-small-en-v1.5`
- `<binary_dir>/model`
- `~/.cache/remi/bge-small-en-v1.5`

### Semantic command examples

Rebuild embeddings:

```bash
remi embed --rebuild
```

Search with semantic mode:

```bash
remi search query "memoization strategy" --semantic auto
remi search query "memoization strategy" --semantic on
remi search query "memoization strategy" --semantic off
```

Set ONNX Runtime path explicitly:

```bash
remi --ort-dylib-path /opt/onnx/libonnxruntime.so search query "vector index" --semantic on
```

Auto-detect ONNX Runtime shared library:

```bash
remi --auto-ort search query "vector index" --semantic auto
```

---

## End-to-end workflow examples

### Workflow A: First-time ingest + browse

```bash
remi init
remi sync --agent all
remi sessions list
```

Then inspect a session:

```bash
remi sessions show <session_id>
```

### Workflow B: Investigate an issue across all agents

```bash
remi search query "panic: index out of bounds" --format markdown
```

Open the emitted `.md` export path and review the full conversation.

### Workflow C: CI/script-friendly JSON output

```bash
remi search query "release tagging" --no-interactive --format json --select auto
```

### Workflow D: Safe archival lifecycle

```bash
# plan
remi archive plan --older-than 60d --keep-latest 10

# inspect impact
remi archive run --plan <run_id>

# execute archive without deletion
remi archive run --plan <run_id> --execute

# optional deletion after validation
remi archive run --plan <run_id> --execute --delete-source
```

Restore when needed:

```bash
remi archive restore --bundle ~/.local/share/remi/archive/<run_id>/sessions.json
```

---

## Helper scripts (examples)

This repo includes helper scripts in `scripts/`:

- `scripts/remi-diary.sh` — generate a daily Markdown summary from `remi.db`, with optional Telegram send.
- `scripts/markie-export.sh` — render Markdown to SVG/PNG using local `markie` CLI ([MarkieCli](https://github.com/lsj5031/MarkieCli)).

Example diary usage:

```bash
# Generate today's diary markdown
scripts/remi-diary.sh --date today

# Generate and send yesterday's diary image to Telegram
scripts/remi-diary.sh --date yesterday --send
```

Example Markdown → SVG rendering:

```bash
scripts/markie-export.sh \
  --input ~/diary/remi/$(date +%F).md \
  --output /tmp/remi-diary.svg \
  --format svg
```

`remi-diary.sh` prints a safety warning by default because it may send transcript data to external services:

- your configured `pi` model provider (for summary generation)
- Telegram (when `--send` is enabled)

If you intentionally want to suppress the warning:

```bash
DIARY_SKIP_EXTERNAL_WARNING=1 scripts/remi-diary.sh --date today
```

Use `DIARY_*` environment overrides (shown in `scripts/remi-diary.sh --help`) to adapt paths/commands for your environment.

---

## Architecture at a glance

Workspace crates:

- `core-model`: canonical types + adapter trait + deterministic IDs
- `store-sqlite`: SQLite schema, upserts, FTS index maintenance, archive planning helpers
- `ingest`: sync orchestration with progress phases
- `search`: lexical + recency (+ optional semantic) ranking
- `archive`: plan/run/restore archive workflows
- `adapter-common` (at `crates/adapters/common`): shared file/JSON parsing + cursor logic
- `adapters/{pi,droid,opencode,claude,amp,codex}`: per-agent ingestion adapters
- `embeddings` (optional): ONNX + tokenizer embedding generation
- `cli`: `remi` command-line interface

---

## Release artifacts

GitHub release workflow publishes:

- `remi-linux-x64-simple.tar.gz` (binary only)
- `remi-linux-x64-bundled.tar.gz` (binary + ONNX Runtime + BGE model files)

As of `v0.0.4` (February 11, 2026), published assets are Linux x64.
The workflow in this repo is set up to also produce macOS/Windows simple artifacts on future tagged releases.

---

## Homebrew (optional)

Homebrew tap setup (recommended layout):

1. Create a tap repo, e.g. `lsj5031/homebrew-remi`.
2. Start from this repo’s `Formula/remi.rb` and update URL/version/SHA per tagged release.
3. Update SHA256 per release.

End-user install:

```bash
brew tap lsj5031/remi
brew install remi
```

---

## License

Dual-licensed under:

- MIT
- Apache-2.0
