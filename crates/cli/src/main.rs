use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    time::{Instant, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use ingest::SyncPhase;
use render::HtmlSafety;
use rusqlite::{Connection, OptionalExtension, params};
use store_sqlite::SqliteStore;
use tracing::{debug, info, trace};

#[cfg(feature = "semantic")]
mod config;
mod render;
mod ui;

#[derive(Parser)]
#[command(name = "remi", version)]
#[command(about = "Unified coding-agent session memory")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[cfg(feature = "semantic")]
    #[arg(long)]
    ort_dylib_path: Option<PathBuf>,
    #[cfg(feature = "semantic")]
    #[arg(long, default_value_t = false)]
    auto_ort: bool,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Sync(SyncArgs),
    Docs {
        #[command(subcommand)]
        command: DocsCommand,
    },
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    Search {
        #[command(subcommand)]
        command: SearchCommand,
    },
    Archive {
        #[command(subcommand)]
        command: ArchiveCommand,
    },
    #[cfg(feature = "semantic")]
    Embed {
        #[arg(long)]
        rebuild: bool,
    },
    Doctor,
}

#[derive(Args)]
struct SyncArgs {
    #[arg(long, value_enum)]
    agent: AgentOpt,
}

#[derive(Clone, Copy, ValueEnum)]
enum AgentOpt {
    Pi,
    Droid,
    Opencode,
    Claude,
    Amp,
    Codex,
    All,
}

#[derive(Subcommand)]
enum SessionsCommand {
    List,
    Show { session_id: String },
}

#[derive(Subcommand)]
enum DocsCommand {
    Index {
        #[arg(long)]
        root: PathBuf,
    },
    Search {
        query: String,
        #[arg(long, default_value_t = false)]
        raw_fts: bool,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[derive(Subcommand)]
enum SearchCommand {
    Query {
        query: String,
        #[arg(long, value_enum, default_value_t = SearchFormat::Html)]
        format: SearchFormat,
        #[arg(long, default_value_t = false)]
        no_interactive: bool,
        #[arg(long, value_enum, default_value_t = SelectMode::Auto)]
        select: SelectMode,
        #[arg(long)]
        index: Option<usize>,
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        contains: Option<String>,
        #[arg(long, default_value_t = false)]
        raw_fts: bool,
        #[arg(long, value_enum, default_value_t = HtmlSafety::Relaxed)]
        html_safety: HtmlSafety,
        #[cfg(feature = "semantic")]
        #[arg(long, value_enum, default_value_t = SemanticMode::Auto)]
        semantic: SemanticMode,
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ArchiveCommand {
    Plan {
        #[arg(long)]
        older_than: String,
        #[arg(long)]
        keep_latest: usize,
    },
    Run {
        #[arg(long)]
        plan: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        execute: bool,
        #[arg(long, default_value_t = false)]
        delete_source: bool,
    },
    Restore {
        #[arg(long)]
        bundle: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SearchFormat {
    Html,
    Markdown,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum SelectMode {
    Auto,
    Index,
}

#[cfg(feature = "semantic")]
#[derive(Clone, Copy, ValueEnum)]
enum SemanticMode {
    Auto,
    On,
    Off,
}

impl SearchFormat {
    fn extension(self) -> &'static str {
        match self {
            SearchFormat::Html => "html",
            SearchFormat::Markdown => "md",
            SearchFormat::Json => "json",
        }
    }
}

fn sanitize_title(title: &str) -> String {
    let first_line = title.split('\n').next().unwrap_or(title);
    let char_count = first_line.chars().count();
    if char_count > 80 {
        let truncated: String = first_line.chars().take(80).collect();
        format!("{truncated}…")
    } else {
        first_line.to_string()
    }
}

fn command_name(cmd: &Commands) -> &'static str {
    match cmd {
        Commands::Init => "init",
        Commands::Sync(_) => "sync",
        Commands::Docs { .. } => "docs",
        Commands::Sessions { .. } => "sessions",
        Commands::Search { .. } => "search",
        Commands::Archive { .. } => "archive",
        #[cfg(feature = "semantic")]
        Commands::Embed { .. } => "embed",
        Commands::Doctor => "doctor",
    }
}

fn main() -> anyhow::Result<()> {
    let fmt = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_writer(std::io::stderr);

    if std::env::var("REMI_LOG_FORMAT").as_deref() == Ok("json") {
        fmt.json()
            .flatten_event(true)
            .with_file(true)
            .with_line_number(true)
            .with_span_list(false)
            .init();
    } else {
        fmt.init();
    }
    let cli = Cli::parse();
    debug!(command = %command_name(&cli.command), "cli args parsed");
    #[cfg(feature = "semantic")]
    let config = config::Config::load()?;
    let t = Instant::now();

    #[cfg(feature = "semantic")]
    configure_ort(&cli)?;

    info!("opening database");
    let mut store = SqliteStore::open_default()?;
    store.init_schema()?;

    #[cfg(feature = "semantic")]
    let mut embedder = if let Some(semantic) = &config.semantic {
        if semantic.enabled {
            let model_path = semantic
                .model_path
                .as_ref()
                .map(PathBuf::from)
                .or_else(detect_model_path);
            if let Some(path) = model_path {
                info!(path = %path.display(), "loading embedding model");
                Some(embeddings::Embedder::new(
                    path,
                    semantic.pooling.as_deref(),
                    semantic.query_prefix.as_deref(),
                )?)
            } else {
                tracing::warn!("semantic search enabled but no model_path configured; skipping");
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    #[cfg(feature = "semantic")]
    let mut semantic_cache = search::SemanticCache::default();

    match cli.command {
        Commands::Init => {
            info!(elapsed = ?t.elapsed(), "initialized");
        }
        Commands::Sync(args) => {
            let synced = match args.agent {
                AgentOpt::Pi => sync_with_timing(
                    "pi",
                    &pi::PiAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Droid => sync_with_timing(
                    "droid",
                    &droid::DroidAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Opencode => sync_with_timing(
                    "opencode",
                    &opencode::OpenCodeAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Claude => sync_with_timing(
                    "claude",
                    &claude::ClaudeAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Amp => sync_with_timing(
                    "amp",
                    &amp::AmpAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Codex => sync_with_timing(
                    "codex",
                    &codex::CodexAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::All => {
                    let mut total = 0;
                    for (name, adapter) in adapters() {
                        total += sync_with_timing(
                            name,
                            adapter.as_ref(),
                            &mut store,
                            #[cfg(feature = "semantic")]
                            embedder.as_mut(),
                        )?;
                    }
                    total
                }
            };
            info!(records = synced, elapsed = ?t.elapsed(), "synced");
        }
        Commands::Docs { command } => match command {
            DocsCommand::Index { root } => {
                let summary = index_docs_root(&root)?;
                info!(
                    root = %root.display(),
                    indexed = summary.indexed,
                    updated = summary.updated,
                    skipped = summary.skipped,
                    deleted = summary.deleted,
                    errors = summary.errors,
                    elapsed = ?t.elapsed(),
                    "docs index complete"
                );
                println!("root={}", root.display());
                println!("indexed={}", summary.indexed);
                println!("updated={}", summary.updated);
                println!("skipped={}", summary.skipped);
                println!("deleted={}", summary.deleted);
                println!("errors={}", summary.errors);
            }
            DocsCommand::Search {
                query,
                raw_fts,
                limit,
            } => {
                info!(query = %query, raw_fts, limit, "docs searching");
                let hits = search::search_docs_at(default_db_path(), &query, limit, raw_fts)?;
                debug!(hits = hits.len(), "docs search returned hits");
                if hits.is_empty() {
                    info!(elapsed = ?t.elapsed(), "no docs results");
                    return Ok(());
                }
                for hit in hits {
                    println!("{}", hit.title);
                    println!("  path: {}", hit.path);
                    println!("  snippet: {}", hit.snippet);
                    println!();
                }
            }
        },
        Commands::Sessions { command } => match command {
            SessionsCommand::List => {
                let sessions = store.list_sessions()?;
                info!(sessions = sessions.len(), "sessions listed");
                for s in &sessions {
                    println!("{} {} {}", s.id, s.agent.as_str(), sanitize_title(&s.title));
                }
            }
            SessionsCommand::Show { session_id } => {
                trace!(session_id, "showing session messages");
                let session = store.get_session(&session_id)?;
                if session.is_none() {
                    return Err(anyhow::anyhow!("session not found: {session_id}"));
                }
                let msgs = store.get_session_messages(&session_id)?;
                info!(messages = msgs.len(), "session messages listed");
                for m in &msgs {
                    println!("{} [{}] {}", m.ts.to_rfc3339(), m.role, m.content);
                }
            }
        },
        Commands::Search { command } => match command {
            SearchCommand::Query {
                query,
                format,
                no_interactive,
                select,
                index,
                agent,
                title,
                id,
                contains,
                raw_fts,
                html_safety,
                #[cfg(feature = "semantic")]
                semantic,
                output_dir,
            } => {
                info!(query = %query, "searching");
                trace!(no_interactive, raw_fts, "search parameters");
                #[cfg(feature = "semantic")]
                let search_embedder = match semantic {
                    SemanticMode::Off => None,
                    SemanticMode::Auto => embedder.as_mut(),
                    SemanticMode::On => {
                        if embedder.is_none() {
                            tracing::warn!("semantic search requested but no embedder configured");
                        }
                        embedder.as_mut()
                    }
                };
                let hits = search::search_sessions(
                    &store,
                    &query,
                    20,
                    raw_fts,
                    #[cfg(feature = "semantic")]
                    search_embedder,
                    #[cfg(feature = "semantic")]
                    Some(&mut semantic_cache),
                )?;
                debug!(hits = hits.len(), "search returned hits");
                if hits.is_empty() {
                    info!(elapsed = ?t.elapsed(), "no results");
                    return Ok(());
                }
                let mut sessions = ui::build_session_displays(&store, &hits)?;
                if sessions.is_empty() {
                    info!(elapsed = ?t.elapsed(), "no sessions to display");
                    return Ok(());
                }

                let filters = ui::FilterSpec {
                    agent,
                    title,
                    id,
                    contains,
                };
                trace!(agent = ?filters.agent, title = ?filters.title, id = ?filters.id, contains = ?filters.contains, "applying filters");
                sessions = ui::apply_filters(&sessions, &filters);
                if sessions.is_empty() {
                    return Err(anyhow::anyhow!("no sessions matched filters"));
                }

                let (selected, selected_index, sessions) = if no_interactive {
                    let selected_index = match select {
                        SelectMode::Auto => {
                            sessions.sort_by(|a, b| b.score.total_cmp(&a.score));
                            0
                        }
                        SelectMode::Index => {
                            index.with_context(|| "--index required when --select index")?
                        }
                    };
                    if selected_index >= sessions.len() {
                        return Err(anyhow::anyhow!("index out of range"));
                    }
                    let selected = sessions[selected_index].clone();
                    (selected, selected_index, sessions)
                } else {
                    info!(sessions = sessions.len(), elapsed = ?t.elapsed(), "sessions matched");
                    ui::print_session_list(&sessions, &[]);
                    let filter = ui::prompt_line(
                        "filter (fuzzy; fields: agent:, title:, id:, contains:; example: \"agent:claude auth login\") — enter to keep: ",
                    )?;
                    let (mut filtered, terms) = if filter.trim().is_empty() {
                        (sessions.clone(), Vec::new())
                    } else {
                        ui::fuzzy_filter_sessions(&sessions, &filter)
                    };
                    if filtered.is_empty() {
                        info!("no sessions matched filter; keeping original list");
                        filtered = sessions.clone();
                    } else {
                        info!(sessions = filtered.len(), "sessions matched filter");
                    }
                    ui::print_session_list(&filtered, &terms);
                    let choice = ui::prompt_line("select index (default 0): ")?;
                    let selected_index = ui::parse_index(&choice, filtered.len())?;
                    let selected = filtered[selected_index].clone();
                    (selected, selected_index, filtered)
                };

                if matches!(format, SearchFormat::Json) {
                    let sessions = sessions
                        .into_iter()
                        .map(|item| ui::JsonSession {
                            id: item.session_id,
                            title: item.title,
                            agent: item.agent,
                            updated_at: item.updated_at,
                            message_count: item.message_count,
                            snippet: item.snippet,
                            score: item.score,
                        })
                        .collect();
                    let output = ui::JsonSearchOutput {
                        query: query.clone(),
                        selected_index,
                        selected_session_id: selected.session_id.clone(),
                        sessions,
                    };
                    println!("{}", serde_json::to_string_pretty(&output)?);
                    return Ok(());
                }

                let session = store
                    .get_session(&selected.session_id)?
                    .with_context(|| "selected session missing")?;
                let messages = store.get_session_messages(&selected.session_id)?;
                let rendered = match format {
                    SearchFormat::Html => {
                        render::render_session_html(&session, &messages, html_safety)?
                    }
                    SearchFormat::Markdown => render::render_session_markdown(&session, &messages),
                    SearchFormat::Json => unreachable!("handled earlier"),
                };
                let out_dir = ui::resolve_output_dir(output_dir)?;
                let file_path =
                    out_dir.join(format!("session_{}.{}", session.id, format.extension()));
                std::fs::write(&file_path, rendered)?;
                println!("{}", file_path.display());
            }
        },
        Commands::Archive { command } => match command {
            ArchiveCommand::Plan {
                older_than,
                keep_latest,
            } => {
                let d = humantime::parse_duration(&older_than)
                    .with_context(|| "invalid --older-than")?;
                debug!(older_than = %older_than, keep_latest, "archive plan parameters");
                info!(older_than = %older_than, keep_latest, "planning archive");
                let run_id =
                    archive::archive_plan(&store, chrono::Duration::from_std(d)?, keep_latest)?;
                let items = store.archive_items_for_run(&run_id)?;
                info!(sessions = items.len(), elapsed = ?t.elapsed(), "sessions selected");
                println!("plan {run_id}");
            }
            ArchiveCommand::Run {
                plan,
                dry_run,
                execute,
                delete_source,
            } => {
                let should_execute = execute && !dry_run;
                debug!(
                    execute,
                    dry_run, delete_source, should_execute, "archive run flags"
                );
                if should_execute {
                    info!(run_id = %plan, "executing archive run");
                    if delete_source {
                        info!("--delete-source: will remove archived sessions from DB");
                    }
                } else {
                    info!(run_id = %plan, "dry-run for archive run");
                }
                let msg = archive::archive_run(&store, &plan, should_execute, delete_source)?;
                info!(elapsed = ?t.elapsed(), "archive run done");
                println!("{msg}");
            }
            ArchiveCommand::Restore { bundle } => {
                info!(bundle = %bundle, "restoring archive");
                let msg = archive::archive_restore(&mut store, &bundle)?;
                info!(elapsed = ?t.elapsed(), "restore done");
                println!("{msg}");
            }
        },
        #[cfg(feature = "semantic")]
        Commands::Embed { rebuild } => {
            if let Some(embedder) = embedder.as_mut() {
                if rebuild {
                    info!("rebuilding embeddings");
                    let sessions = store.list_sessions()?;
                    let mut count = 0;
                    for s in &sessions {
                        let msgs = store.get_session_messages(&s.id)?;
                        for m in msgs {
                            if m.content.trim().is_empty() {
                                continue;
                            }
                            if let Ok(vec) = embedder.embed(&m.content, false) {
                                store.save_embedding(&m.id, &vec)?;
                                count += 1;
                            }
                        }
                        if count > 0 && count % 100 == 0 {
                            info!(processed = count, "processed messages");
                        }
                    }
                    info!(count, elapsed = ?t.elapsed(), "computed embeddings");
                } else {
                    info!("use --rebuild to rebuild all embeddings");
                }
            } else {
                info!("semantic search not enabled or configured");
            }
        }
        Commands::Doctor => {
            info!("running integrity check");
            let check = store.integrity_check()?;
            let sessions = store.list_sessions()?;
            info!(elapsed = ?t.elapsed(), "integrity check done");
            println!("integrity_check={check}");
            println!("sessions={}", sessions.len());
        }
    }

    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DocsIndexSummary {
    indexed: usize,
    updated: usize,
    skipped: usize,
    deleted: usize,
    errors: usize,
}

#[derive(Debug)]
struct ScannedDoc {
    document_id: String,
    relative_path: String,
    title: String,
    modified_at: String,
    size: i64,
    content_hash: String,
    content: String,
}

fn index_docs_root(root: &Path) -> anyhow::Result<DocsIndexSummary> {
    index_docs_root_with_db(root, &default_db_path())
}

fn index_docs_root_with_db(root: &Path, db_path: &Path) -> anyhow::Result<DocsIndexSummary> {
    let canonical_root = root
        .canonicalize()
        .with_context(|| format!("canonicalizing root {}", root.display()))?;
    if !canonical_root.is_dir() {
        bail!("docs root is not a directory: {}", canonical_root.display());
    }

    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", db_path.display()))?;
    }
    let mut conn = Connection::open(db_path)
        .with_context(|| format!("opening sqlite db {}", db_path.display()))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
    )?;
    ensure_docs_schema(&conn)?;

    let now = chrono::Utc::now().to_rfc3339();
    let root_key = canonical_root.to_string_lossy().to_string();
    let root_id = stable_id("doc-root", &[&root_key]);
    let generation = reserve_generation(&conn, &root_id, &root_key, &now)?;
    let scan = scan_docs_tree(&canonical_root, &root_id)?;

    let tx = conn.transaction()?;
    let mut indexed = 0usize;
    let mut updated = 0usize;
    let skipped = scan.skipped;
    for doc in &scan.docs {
        let existing: Option<(String, i64, String)> = tx
            .query_row(
                "SELECT modified_at, size, content_hash
                 FROM documents
                 WHERE root_id = ?1 AND path = ?2",
                params![root_id, doc.relative_path],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        let unchanged = existing
            .as_ref()
            .is_some_and(|(modified_at, size, content_hash)| {
                modified_at == &doc.modified_at
                    && *size == doc.size
                    && content_hash == &doc.content_hash
            });
        if unchanged {
            tx.execute(
                "UPDATE documents
                 SET last_seen_generation = ?3,
                     completed_generation = ?3,
                     indexed_at = ?4
                 WHERE root_id = ?1 AND path = ?2",
                params![root_id, doc.relative_path, generation, now],
            )?;
            continue;
        }

        if existing.is_some() {
            updated += 1;
        } else {
            indexed += 1;
        }

        tx.execute(
            "INSERT INTO documents
                (id, root_id, path, title, modified_at, size, content_hash,
                 last_seen_generation, completed_generation, indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9)
             ON CONFLICT(root_id, path) DO UPDATE SET
                id = excluded.id,
                title = excluded.title,
                modified_at = excluded.modified_at,
                size = excluded.size,
                content_hash = excluded.content_hash,
                last_seen_generation = excluded.last_seen_generation,
                completed_generation = excluded.completed_generation,
                indexed_at = excluded.indexed_at",
            params![
                doc.document_id,
                root_id,
                doc.relative_path,
                doc.title,
                doc.modified_at,
                doc.size,
                doc.content_hash,
                generation,
                now,
            ],
        )?;
        tx.execute(
            "DELETE FROM fts_documents WHERE document_id = ?1",
            params![doc.document_id],
        )?;
        tx.execute(
            "INSERT INTO fts_documents (document_id, root_id, path, title, content)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                doc.document_id,
                root_id,
                doc.relative_path,
                doc.title,
                doc.content
            ],
        )?;
    }

    let stale_ids: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT id
             FROM documents
             WHERE root_id = ?1 AND last_seen_generation <> ?2",
        )?;
        let rows = stmt.query_map(params![root_id, generation], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let deleted = stale_ids.len();
    for stale_id in &stale_ids {
        tx.execute(
            "DELETE FROM fts_documents WHERE document_id = ?1",
            params![stale_id],
        )?;
        tx.execute("DELETE FROM documents WHERE id = ?1", params![stale_id])?;
    }
    tx.execute(
        "UPDATE doc_roots
         SET generation = ?2,
             scan_started_at = ?3,
             scan_completed_at = ?3,
             scan_status = 'ready'
         WHERE root_id = ?1",
        params![root_id, generation, now],
    )?;
    tx.commit()?;

    Ok(DocsIndexSummary {
        indexed,
        updated,
        skipped,
        deleted,
        errors: scan.errors,
    })
}

struct DocsScan {
    docs: Vec<ScannedDoc>,
    skipped: usize,
    errors: usize,
}

fn scan_docs_tree(root: &Path, root_id: &str) -> anyhow::Result<DocsScan> {
    let mut docs = Vec::new();
    let mut skipped = 0usize;
    let mut errors = 0usize;
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries =
            fs::read_dir(&dir).with_context(|| format!("reading directory {}", dir.display()))?;
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let path = entry.path();
            let file_name = entry.file_name();
            if is_hidden_name(&file_name) {
                skipped += 1;
                continue;
            }

            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            if file_type.is_symlink() {
                skipped += 1;
                continue;
            }
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || !is_supported_doc(&path) {
                skipped += 1;
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let content = match String::from_utf8(bytes) {
                Ok(content) => content,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let relative_path = normalize_relative_path(root, &path)?;
            let title = doc_title(&path, &content);
            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
                .map(|ts| ts.as_secs().to_string())
                .unwrap_or_else(|| "0".to_string());
            let content_hash = blake3::hash(content.as_bytes()).to_hex().to_string();
            docs.push(ScannedDoc {
                document_id: stable_id("doc", &[root_id, &relative_path]),
                relative_path,
                title,
                modified_at,
                size: metadata.len() as i64,
                content_hash,
                content,
            });
        }
    }

    docs.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

    Ok(DocsScan {
        docs,
        skipped,
        errors,
    })
}

fn ensure_docs_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS doc_roots (
          root_id TEXT PRIMARY KEY,
          canonical_path TEXT NOT NULL UNIQUE,
          generation INTEGER NOT NULL DEFAULT 0,
          scan_started_at TEXT,
          scan_completed_at TEXT,
          scan_status TEXT NOT NULL DEFAULT 'idle'
        );
        CREATE TABLE IF NOT EXISTS documents (
          id TEXT PRIMARY KEY,
          root_id TEXT NOT NULL,
          path TEXT NOT NULL,
          title TEXT NOT NULL,
          modified_at TEXT NOT NULL,
          size INTEGER NOT NULL,
          content_hash TEXT NOT NULL,
          last_seen_generation INTEGER NOT NULL,
          completed_generation INTEGER NOT NULL,
          indexed_at TEXT NOT NULL,
          UNIQUE(root_id, path),
          FOREIGN KEY(root_id) REFERENCES doc_roots(root_id) ON DELETE CASCADE
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_documents USING fts5(
          document_id UNINDEXED,
          root_id UNINDEXED,
          path,
          title,
          content,
          tokenize = 'unicode61 tokenchars ''_./:-'''
        );
        CREATE INDEX IF NOT EXISTS idx_documents_root_path ON documents(root_id, path);
        "#,
    )?;
    Ok(())
}

fn reserve_generation(
    conn: &Connection,
    root_id: &str,
    canonical_path: &str,
    now: &str,
) -> anyhow::Result<i64> {
    let previous_generation: i64 = conn
        .query_row(
            "SELECT generation FROM doc_roots WHERE root_id = ?1",
            params![root_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let generation = previous_generation + 1;
    conn.execute(
        "INSERT INTO doc_roots
            (root_id, canonical_path, generation, scan_started_at, scan_completed_at, scan_status)
         VALUES (?1, ?2, ?3, ?4, NULL, 'running')
         ON CONFLICT(root_id) DO UPDATE SET
            canonical_path = excluded.canonical_path,
            generation = excluded.generation,
            scan_started_at = excluded.scan_started_at,
            scan_completed_at = NULL,
            scan_status = excluded.scan_status",
        params![root_id, canonical_path, generation, now],
    )?;
    Ok(generation)
}

fn default_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("remi")
        .join("remi.db")
}

fn normalize_relative_path(root: &Path, path: &Path) -> anyhow::Result<String> {
    let rel = path
        .strip_prefix(root)
        .with_context(|| format!("computing relative path for {}", path.display()))?;
    let parts = rel
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    Ok(parts.join("/"))
}

fn is_supported_doc(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref(),
        Some("md" | "markdown" | "txt" | "rst")
    )
}

fn is_hidden_name(name: &std::ffi::OsStr) -> bool {
    name.to_str().is_some_and(|name| name.starts_with('.'))
}

fn doc_title(path: &Path, content: &str) -> String {
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(title) = trimmed.strip_prefix("# ")
            && !title.trim().is_empty()
        {
            return title.trim().to_string();
        }
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| stem.to_string())
        .unwrap_or_else(|| path.display().to_string())
}

fn stable_id(prefix: &str, parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    for part in parts {
        hasher.update(&[0]);
        hasher.update(part.as_bytes());
    }
    format!("{prefix}-{}", hasher.finalize().to_hex())
}

#[cfg(feature = "semantic")]
fn configure_ort(cli: &Cli) -> anyhow::Result<()> {
    if let Some(path) = &cli.ort_dylib_path {
        if !path.exists() {
            return Err(anyhow::anyhow!("ORT dylib not found at {}", path.display()));
        }
        std::env::set_var("ORT_DYLIB_PATH", path);
        return Ok(());
    }

    if cli.auto_ort {
        if std::env::var_os("ORT_DYLIB_PATH").is_some() {
            return Ok(());
        }
        if let Some(found) = find_ort_dylib()? {
            info!(path = %found.display(), "using ORT dylib");
            std::env::set_var("ORT_DYLIB_PATH", found);
        } else {
            tracing::warn!("failed to auto-detect libonnxruntime.so");
        }
    }

    Ok(())
}

#[cfg(feature = "semantic")]
fn find_ort_dylib() -> anyhow::Result<Option<PathBuf>> {
    let mut roots = Vec::new();
    if let Some(cache_dir) = dirs::cache_dir() {
        roots.push(cache_dir.join("onnxruntime"));
    }
    roots.push(PathBuf::from("/usr/lib"));
    roots.push(PathBuf::from("/usr/local/lib"));

    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for root in roots {
        if !root.exists() {
            continue;
        }
        for path in walk_paths(&root)? {
            if path.file_name().and_then(|s| s.to_str()) == Some("libonnxruntime.so") {
                let modified = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                if best.as_ref().is_none_or(|(t, _)| modified > *t) {
                    best = Some((modified, path));
                }
            }
        }
    }

    Ok(best.map(|(_, path)| path))
}

#[cfg(feature = "semantic")]
fn walk_paths(root: &std::path::Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn adapters() -> Vec<(&'static str, Box<dyn core_model::AgentAdapter>)> {
    vec![
        ("pi", Box::new(pi::PiAdapter)),
        ("droid", Box::new(droid::DroidAdapter)),
        ("opencode", Box::new(opencode::OpenCodeAdapter)),
        ("claude", Box::new(claude::ClaudeAdapter)),
        ("amp", Box::new(amp::AmpAdapter)),
        ("codex", Box::new(codex::CodexAdapter)),
    ]
}

fn sync_with_timing(
    name: &str,
    adapter: &dyn core_model::AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] embedder: Option<&mut embeddings::Embedder>,
) -> anyhow::Result<usize> {
    let started = Instant::now();
    info!(name, "sync start");
    let count = sync_one(
        name,
        adapter,
        store,
        #[cfg(feature = "semantic")]
        embedder,
    )?;
    info!(name, count, elapsed = ?started.elapsed(), "sync done");
    Ok(count)
}

fn sync_one(
    name: &str,
    adapter: &dyn core_model::AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] embedder: Option<&mut embeddings::Embedder>,
) -> anyhow::Result<usize> {
    let started = Instant::now();
    let last = RefCell::new(started);
    ingest::sync_adapter(
        adapter,
        store,
        #[cfg(feature = "semantic")]
        embedder,
        |phase| match phase {
            SyncPhase::Discovering => {
                let now = Instant::now();
                let since_last = now.duration_since(*last.borrow());
                *last.borrow_mut() = now;
                info!(
                    name,
                    elapsed = ?started.elapsed(),
                    delta = ?since_last,
                    "discovering source files"
                );
            }
            SyncPhase::Scanning { file_count } => {
                let now = Instant::now();
                let since_last = now.duration_since(*last.borrow());
                *last.borrow_mut() = now;
                info!(
                    name,
                    file_count,
                    elapsed = ?started.elapsed(),
                    delta = ?since_last,
                    "scanning files"
                );
            }
            SyncPhase::Normalizing { record_count } => {
                let now = Instant::now();
                let since_last = now.duration_since(*last.borrow());
                *last.borrow_mut() = now;
                info!(
                    name,
                    record_count,
                    elapsed = ?started.elapsed(),
                    delta = ?since_last,
                    "normalizing records"
                );
            }
            SyncPhase::Saving { message_count } => {
                let now = Instant::now();
                let since_last = now.duration_since(*last.borrow());
                *last.borrow_mut() = now;
                info!(
                    name,
                    message_count,
                    elapsed = ?started.elapsed(),
                    delta = ?since_last,
                    "saving messages"
                );
            }
            SyncPhase::Done { total_records } => {
                let now = Instant::now();
                let since_last = now.duration_since(*last.borrow());
                *last.borrow_mut() = now;
                info!(
                    name,
                    total_records,
                    elapsed = ?started.elapsed(),
                    delta = ?since_last,
                    "sync done"
                );
            }
        },
    )
}

#[cfg(feature = "semantic")]
fn detect_model_path() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("models").join("bge-small-en-v1.5"));
            candidates.push(dir.join("model"));
        }
    }
    if let Some(cache_dir) = dirs::cache_dir() {
        candidates.push(cache_dir.join("remi").join("bge-small-en-v1.5"));
    }

    for path in candidates {
        let model = path.join("model.onnx");
        let tokenizer = path.join("tokenizer.json");
        if model.exists() && tokenizer.exists() {
            info!(path = %path.display(), "auto-detected model");
            return Some(path);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sanitize_title_strips_newlines() {
        assert_eq!(sanitize_title("hello\nworld"), "hello");
    }

    #[test]
    fn sanitize_title_truncates_long() {
        let long = "a".repeat(100);
        let result = sanitize_title(&long);
        assert_eq!(result.chars().count(), 81);
        assert!(result.ends_with('…'));
        assert!(result.starts_with(&"a".repeat(80)));
    }

    #[test]
    fn sanitize_title_short_unchanged() {
        assert_eq!(sanitize_title("short title"), "short title");
    }

    #[test]
    fn sanitize_title_empty() {
        assert_eq!(sanitize_title(""), "");
    }

    #[test]
    fn sanitize_title_only_newlines() {
        assert_eq!(sanitize_title("\n\n\n"), "");
    }

    #[test]
    fn sanitize_title_long_first_line_with_newline() {
        let input = format!("{}\nsecond line", "a".repeat(100));
        let result = sanitize_title(&input);
        assert_eq!(result.chars().count(), 81);
        assert!(result.ends_with('…'));
        assert!(!result.contains('\n'));
    }

    fn temp_path(label: &str, suffix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("remi-cli-{label}-{unique}{suffix}"))
    }

    #[test]
    fn docs_index_round_trip_handles_rerun_and_rename() {
        let root = temp_path("docs-root", "");
        let db_path = temp_path("docs-db", ".sqlite");
        fs::create_dir_all(&root).unwrap();
        let doc_path = root.join("guide.md");
        fs::write(&doc_path, "# Guide\n\nunique-doc-token").unwrap();
        fs::write(root.join(".hidden.md"), "hidden").unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&doc_path, root.join("guide-link.md")).unwrap();

        let first = index_docs_root_with_db(&root, &db_path).unwrap();
        assert_eq!(first.indexed, 1);
        #[cfg(unix)]
        assert!(first.skipped >= 2);
        #[cfg(not(unix))]
        assert!(first.skipped >= 1);

        let second = index_docs_root_with_db(&root, &db_path).unwrap();
        assert_eq!(second.indexed, 0);
        assert_eq!(second.updated, 0);

        let renamed = root.join("renamed-guide.md");
        fs::rename(&doc_path, &renamed).unwrap();
        let third = index_docs_root_with_db(&root, &db_path).unwrap();
        assert_eq!(third.deleted, 1);
        assert_eq!(third.indexed, 1);

        let hits = search::search_docs_at(&db_path, "unique-doc-token", 10, false).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "renamed-guide.md");

        let _ = fs::remove_file(&db_path);
        let _ = fs::remove_dir_all(&root);
    }
}
