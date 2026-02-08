use std::time::Instant;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
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
    Query { query: String },
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    #[cfg(feature = "semantic")]
    let config = config::Config::load()?;
    let t = Instant::now();

    eprintln!("opening database...");
    let mut store = SqliteStore::open_default()?;
    store.init_schema()?;

    #[cfg(feature = "semantic")]
    let mut embedder = if let Some(semantic) = &config.semantic {
        if semantic.enabled {
            if let Some(path) = &semantic.model_path {
                 eprintln!("loading embedding model from {}...", path);
                 Some(embeddings::Embedder::new(path, semantic.pooling.as_deref(), semantic.query_prefix.as_deref())?)
            } else {
                 eprintln!("warning: semantic search enabled but no model_path configured; skipping");
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
                    embedder.as_mut()
                )?,
                AgentOpt::Droid => sync_one(
                    "droid", 
                    &droid::DroidAdapter, 
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut()
                )?,
                AgentOpt::Opencode => {
                    sync_one(
                        "opencode", 
                        &opencode::OpenCodeAdapter, 
                        &mut store,
                        #[cfg(feature = "semantic")]
                        embedder.as_mut()
                    )?
                }
                AgentOpt::Claude => sync_one(
                    "claude", 
                    &claude::ClaudeAdapter, 
                    &mut store,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut()
                )?,
                AgentOpt::All => {
                    let mut total = 0;
                    for (name, adapter) in adapters() {
                        total += sync_one(
                            name, 
                            adapter.as_ref(), 
                            &mut store,
                            #[cfg(feature = "semantic")]
                            embedder.as_mut()
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
            SearchCommand::Query { query } => {
                eprintln!("searching for \"{}\"...", query);
                let hits = search::search(
                    &store, 
                    &query, 
                    20,
                    #[cfg(feature = "semantic")]
                    embedder.as_mut()
                )?;
                eprintln!("{} results in {:.1?}", hits.len(), t.elapsed());
                for hit in &hits {
                    println!("{:.3} {} {}", hit.score, hit.session_id, hit.content);
                }
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
                             if m.content.trim().is_empty() { continue; }
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
    #[cfg(feature = "semantic")]
    embedder: Option<&mut embeddings::Embedder>,
) -> anyhow::Result<usize> {
    ingest::sync_adapter(adapter, store, 
        #[cfg(feature = "semantic")]
        embedder,
        |phase| match phase {
        SyncPhase::Discovering => eprintln!("[{name}] discovering source files..."),
        SyncPhase::Scanning { file_count } => eprintln!("[{name}] scanning {file_count} files..."),
        SyncPhase::Normalizing { record_count } => {
            eprintln!("[{name}] normalizing {record_count} records...")
        }
        SyncPhase::Saving { message_count } => {
            eprintln!("[{name}] saving {message_count} messages...")
        }
        SyncPhase::Done { total_records } => eprintln!("[{name}] done: {total_records} records"),
    })
}
