use std::{
    io::{self, Write},
    path::PathBuf,
    time::Instant,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use core_model::{Message, Session};
use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use ingest::SyncPhase;
use store_sqlite::SqliteStore;

#[cfg(feature = "semantic")]
mod config;

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
        }
    }
}

#[derive(Clone)]
struct SessionDisplay {
    session_id: String,
    title: String,
    agent: String,
    updated_at: DateTime<Utc>,
    message_count: usize,
    snippet: String,
    score: f32,
    match_text: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    #[cfg(feature = "semantic")]
    let config = config::Config::load()?;
    let t = Instant::now();

    #[cfg(feature = "semantic")]
    configure_ort(&cli)?;

    eprintln!("opening database...");
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
                eprintln!("loading embedding model from {}...", path.display());
                Some(embeddings::Embedder::new(
                    path,
                    semantic.pooling.as_deref(),
                    semantic.query_prefix.as_deref(),
                )?)
            } else {
                eprintln!(
                    "warning: semantic search enabled but no model_path configured; skipping"
                );
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    match cli.command {
        Commands::Init => {
            eprintln!("initialized in {:.0?}", t.elapsed());
        }
        Commands::Sync(args) => {
            let synced = match args.agent {
                AgentOpt::Pi => sync_one(
                    "pi",
                    &pi::PiAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Droid => sync_one(
                    "droid",
                    &droid::DroidAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Opencode => sync_one(
                    "opencode",
                    &opencode::OpenCodeAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::Claude => sync_one(
                    "claude",
                    &claude::ClaudeAdapter,
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut(),
                )?,
                AgentOpt::All => {
                    let mut total = 0;
                    for (name, adapter) in adapters() {
                        total += sync_one(
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
            eprintln!("synced {synced} records in {:.1?}", t.elapsed());
        }
        Commands::Sessions { command } => match command {
            SessionsCommand::List => {
                let sessions = store.list_sessions()?;
                eprintln!("{} sessions", sessions.len());
                for s in &sessions {
                    println!("{} {} {}", s.id, s.agent.as_str(), s.title);
                }
            }
            SessionsCommand::Show { session_id } => {
                let msgs = store.get_session_messages(&session_id)?;
                eprintln!("{} messages", msgs.len());
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
                #[cfg(feature = "semantic")]
                semantic,
                output_dir,
            } => {
                eprintln!("searching for \"{}\"...", query);
                #[cfg(feature = "semantic")]
                let search_embedder = match semantic {
                    SemanticMode::Off => None,
                    SemanticMode::Auto => embedder.as_mut(),
                    SemanticMode::On => {
                        if embedder.is_none() {
                            eprintln!(
                                "warning: semantic search requested but no embedder configured"
                            );
                        }
                        embedder.as_mut()
                    }
                };
                let hits = search::search_sessions(
                    &store,
                    &query,
                    20,
                    #[cfg(feature = "semantic")]
                    search_embedder,
                )?;
                if hits.is_empty() {
                    eprintln!("no results in {:.1?}", t.elapsed());
                    return Ok(());
                }
                let mut sessions = build_session_displays(&store, &hits)?;
                if sessions.is_empty() {
                    eprintln!("no sessions to display in {:.1?}", t.elapsed());
                    return Ok(());
                }

                let selected = if no_interactive {
                    sessions.sort_by(|a, b| b.score.total_cmp(&a.score));
                    sessions[0].clone()
                } else {
                    eprintln!("{} sessions matched in {:.1?}", sessions.len(), t.elapsed());
                    print_session_list(&sessions);
                    let filter = prompt_line("filter (fuzzy, enter to keep): ")?;
                    let filtered = if filter.trim().is_empty() {
                        sessions
                    } else {
                        fuzzy_filter_sessions(&sessions, &filter)
                    };
                    if filtered.is_empty() {
                        eprintln!("no sessions matched filter");
                        return Ok(());
                    }
                    print_session_list(&filtered);
                    let choice = prompt_line("select index (default 0): ")?;
                    let index = parse_index(&choice, filtered.len())?;
                    filtered[index].clone()
                };

                let session = store
                    .get_session(&selected.session_id)?
                    .with_context(|| "selected session missing")?;
                let messages = store.get_session_messages(&selected.session_id)?;
                let rendered = match format {
                    SearchFormat::Html => render_session_html(&session, &messages),
                    SearchFormat::Markdown => render_session_markdown(&session, &messages),
                };
                let out_dir = resolve_output_dir(output_dir)?;
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
                eprintln!(
                    "planning archive: older than {}, keep latest {}...",
                    older_than, keep_latest
                );
                let run_id =
                    archive::archive_plan(&store, chrono::Duration::from_std(d)?, keep_latest)?;
                let items = store.archive_items_for_run(&run_id)?;
                eprintln!("{} sessions selected in {:.1?}", items.len(), t.elapsed());
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
                    eprintln!("executing archive run {}...", plan);
                    if delete_source {
                        eprintln!("  --delete-source: will remove archived sessions from DB");
                    }
                } else {
                    eprintln!("dry-run for archive run {}...", plan);
                }
                let msg = archive::archive_run(&store, &plan, should_execute, delete_source)?;
                eprintln!("done in {:.1?}", t.elapsed());
                println!("{msg}");
            }
            ArchiveCommand::Restore { bundle } => {
                eprintln!("restoring from {}...", bundle);
                let msg = archive::archive_restore(&mut store, &bundle)?;
                eprintln!("done in {:.1?}", t.elapsed());
                println!("{msg}");
            }
        },
        #[cfg(feature = "semantic")]
        Commands::Embed { rebuild } => {
            if let Some(embedder) = embedder.as_mut() {
                if rebuild {
                    eprintln!("rebuilding embeddings...");
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
                            eprintln!("processed {} messages...", count);
                        }
                    }
                    eprintln!("computed {} embeddings in {:.1?}", count, t.elapsed());
                } else {
                    eprintln!("use --rebuild to rebuild all embeddings");
                }
            } else {
                eprintln!("semantic search not enabled or configured");
            }
        }
        Commands::Doctor => {
            eprintln!("running integrity check...");
            let check = store.integrity_check()?;
            let sessions = store.list_sessions()?;
            eprintln!("done in {:.1?}", t.elapsed());
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
            eprintln!("using ORT dylib at {}", found.display());
            std::env::set_var("ORT_DYLIB_PATH", found);
        } else {
            eprintln!("warning: failed to auto-detect libonnxruntime.so");
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
    ]
}

fn sync_one(
    name: &str,
    adapter: &dyn core_model::AgentAdapter,
    store: &mut SqliteStore,
    #[cfg(feature = "semantic")] embedder: Option<&mut embeddings::Embedder>,
) -> anyhow::Result<usize> {
    ingest::sync_adapter(
        adapter,
        store,
        #[cfg(feature = "semantic")]
        embedder,
        |phase| match phase {
            SyncPhase::Discovering => eprintln!("[{name}] discovering source files..."),
            SyncPhase::Scanning { file_count } => {
                eprintln!("[{name}] scanning {file_count} files...")
            }
            SyncPhase::Normalizing { record_count } => {
                eprintln!("[{name}] normalizing {record_count} records...")
            }
            SyncPhase::Saving { message_count } => {
                eprintln!("[{name}] saving {message_count} messages...")
            }
            SyncPhase::Done { total_records } => {
                eprintln!("[{name}] done: {total_records} records")
            }
        },
    )
}

fn build_session_displays(
    store: &SqliteStore,
    hits: &[search::SessionHit],
) -> anyhow::Result<Vec<SessionDisplay>> {
    let mut out = Vec::with_capacity(hits.len());
    for hit in hits {
        let Some(session) = store.get_session(&hit.session_id)? else {
            continue;
        };
        let messages = store.get_session_messages(&hit.session_id)?;
        let message_count = messages.len();
        let title = session_title(&session, &messages);
        let snippet = truncate_text(&hit.top_content, 140);
        let match_text = format!(
            "{} {} {} {}",
            title,
            session.id,
            snippet,
            session.agent.as_str()
        );
        out.push(SessionDisplay {
            session_id: session.id.clone(),
            title,
            agent: session.agent.as_str().to_string(),
            updated_at: session.updated_at,
            message_count,
            snippet,
            score: hit.score,
            match_text,
        });
    }
    Ok(out)
}

fn session_title(session: &Session, messages: &[Message]) -> String {
    let title = session.title.trim();
    if !title.is_empty() {
        return title.to_string();
    }
    messages
        .iter()
        .find(|m| m.role == "user")
        .map(|m| truncate_text(&m.content, 60))
        .unwrap_or_else(|| "Untitled session".to_string())
}

fn prompt_line(prompt: &str) -> anyhow::Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(buf.trim_end().to_string())
}

fn parse_index(input: &str, len: usize) -> anyhow::Result<usize> {
    if input.trim().is_empty() {
        return Ok(0);
    }
    let idx: usize = input.trim().parse().with_context(|| "invalid index")?;
    if idx >= len {
        return Err(anyhow::anyhow!("index out of range"));
    }
    Ok(idx)
}

fn print_session_list(items: &[SessionDisplay]) {
    for (i, item) in items.iter().enumerate() {
        println!(
            "[{i}] {} | {} | {} msgs | {} | {}",
            item.title,
            item.agent,
            item.message_count,
            item.updated_at.to_rfc3339(),
            item.snippet
        );
    }
}

fn fuzzy_filter_sessions(items: &[SessionDisplay], query: &str) -> Vec<SessionDisplay> {
    let matcher = SkimMatcherV2::default();
    let mut scored: Vec<(i64, SessionDisplay)> = items
        .iter()
        .filter_map(|item| {
            matcher
                .fuzzy_match(&item.match_text, query)
                .map(|s| (s, item.clone()))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0));
    scored.into_iter().map(|(_, item)| item).collect()
}

fn truncate_text(input: &str, max: usize) -> String {
    let mut out = String::new();
    for (i, ch) in input.chars().enumerate() {
        if i >= max {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn render_session_html(session: &Session, messages: &[Message]) -> String {
    let title = escape_html(&session.title);
    let mut body = String::new();
    body.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
    body.push_str("<style>body{font-family:system-ui,Arial,sans-serif;max-width:900px;margin:2rem auto;line-height:1.5}h1{font-size:1.6rem} .meta{color:#555;font-size:.9rem;margin-bottom:1rem} .msg{padding:.6rem .8rem;border:1px solid #e3e3e3;border-radius:8px;margin:.6rem 0} .role{font-weight:600;margin-bottom:.4rem} pre{white-space:pre-wrap}</style></head><body>");
    body.push_str(&format!("<h1>{}</h1>", title));
    body.push_str(&format!(
        "<div class=\"meta\">Session {} · {} · {} messages</div>",
        escape_html(&session.id),
        escape_html(session.agent.as_str()),
        messages.len()
    ));
    for msg in messages {
        let role = escape_html(&msg.role);
        let ts = escape_html(&msg.ts.to_rfc3339());
        let content = escape_html(&msg.content);
        body.push_str("<div class=\"msg\">");
        body.push_str(&format!("<div class=\"role\">{} · {}</div>", role, ts));
        body.push_str(&format!("<pre>{}</pre>", content));
        body.push_str("</div>");
    }
    body.push_str("</body></html>");
    body
}

fn render_session_markdown(session: &Session, messages: &[Message]) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {}\n\n", session.title));
    out.push_str(&format!(
        "Session `{}` · `{}` · {} messages\n\n",
        session.id,
        session.agent.as_str(),
        messages.len()
    ));
    for msg in messages {
        out.push_str(&format!("## {} ({})\n\n", msg.role, msg.ts.to_rfc3339()));
        out.push_str(&msg.content);
        out.push_str("\n\n");
    }
    out
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn resolve_output_dir(dir: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let base = if let Some(dir) = dir {
        dir
    } else {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("remi")
            .join("exports")
    };
    std::fs::create_dir_all(&base)
        .with_context(|| format!("creating output dir {}", base.display()))?;
    Ok(base)
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
            eprintln!("auto-detected model at {}", path.display());
            return Some(path);
        }
    }

    None
}
