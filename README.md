# Remi

Unified coding-agent session memory with ingestion, search, and archive workflows for Pi, Droid, OpenCode, and Claude.

## Quick start

### Build
```bash
cargo build --workspace
```

### Sync
```bash
remi sync --agent all
```

### Search (two-layer UX)
```bash
remi search query "error"
```

## Semantic search (optional)

Enable in `~/.config/remi/config.toml`:
```toml
[semantic]
enabled = true
model_path = "/path/to/bge-small-en-v1.5"
pooling = "cls"
query_prefix = "Represent this sentence for searching relevant passages: "
```

Rebuild embeddings:
```bash
remi embed --rebuild
```

## Releases
- **Simple**: `remi` binary only.
- **Bundled**: `remi` + `libonnxruntime.so` + `bge-small-en-v1.5`.
