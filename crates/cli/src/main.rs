use std::{cell::RefCell, path::PathBuf, time::Instant};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use ingest::SyncPhase;
use store_sqlite::SqliteStore;
use tracing::info;

#[cfg(feature = "semantic")]
mod config;
mod ui;

#[derive(Parser)]
#[command(name = "remi")]
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
    All,
}

#[derive(Subcommand)]
enum SessionsCommand {
    List,
    Show { session_id: String },
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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
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
        Commands::Sessions { command } => match command {
            SessionsCommand::List => {
                let sessions = store.list_sessions()?;
                info!(sessions = sessions.len(), "sessions listed");
                for s in &sessions {
                    println!("{} {} {}", s.id, s.agent.as_str(), s.title);
                }
            }
            SessionsCommand::Show { session_id } => {
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
                #[cfg(feature = "semantic")]
                semantic,
                output_dir,
            } => {
                info!(query = %query, "searching");
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
                    SearchFormat::Html => ui::render_session_html(&session, &messages),
                    SearchFormat::Markdown => ui::render_session_markdown(&session, &messages),
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
