use std::{
    io::{self, IsTerminal, Write},
    path::PathBuf,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use core_model::{Message, Session};
use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use owo_colors::OwoColorize;
use serde::Serialize;
use store_sqlite::SqliteStore;

#[derive(Clone)]
pub struct SessionDisplay {
    pub session_id: String,
    pub title: String,
    pub agent: String,
    pub updated_at: DateTime<Utc>,
    pub message_count: usize,
    pub snippet: String,
    pub score: f32,
    pub match_text: String,
}

#[derive(Default, Clone)]
pub struct FilterSpec {
    pub agent: Option<String>,
    pub title: Option<String>,
    pub id: Option<String>,
    pub contains: Option<String>,
}

#[derive(Serialize)]
pub struct JsonSession {
    pub id: String,
    pub title: String,
    pub agent: String,
    pub updated_at: DateTime<Utc>,
    pub message_count: usize,
    pub snippet: String,
    pub score: f32,
}

#[derive(Serialize)]
pub struct JsonSearchOutput {
    pub query: String,
    pub selected_index: usize,
    pub selected_session_id: String,
    pub sessions: Vec<JsonSession>,
}

pub fn build_session_displays(
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

pub fn session_title(session: &Session, messages: &[Message]) -> String {
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

pub fn prompt_line(prompt: &str) -> anyhow::Result<String> {
    if color_enabled() {
        print!("{}", prompt.cyan());
    } else {
        print!("{prompt}");
    }
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(buf.trim_end().to_string())
}

pub fn parse_index(input: &str, len: usize) -> anyhow::Result<usize> {
    if input.trim().is_empty() {
        return Ok(0);
    }
    let idx: usize = input.trim().parse().with_context(|| "invalid index")?;
    if idx >= len {
        return Err(anyhow::anyhow!("index out of range"));
    }
    Ok(idx)
}

pub fn print_session_list(items: &[SessionDisplay], terms: &[String]) {
    let use_color = color_enabled();
    for (i, item) in items.iter().enumerate() {
        let title = highlight_terms(&item.title, terms, use_color);
        let agent = highlight_terms(&item.agent, terms, use_color);
        let snippet = highlight_terms(&item.snippet, terms, use_color);
        let date = item.updated_at.to_rfc3339();
        let count = format!("{} msgs", item.message_count);
        let separator = if use_color {
            " | ".dimmed().to_string()
        } else {
            " | ".to_string()
        };
        let title = if use_color {
            title.bold().to_string()
        } else {
            title
        };
        let agent = if use_color {
            agent.cyan().to_string()
        } else {
            agent
        };
        let count = if use_color {
            count.magenta().to_string()
        } else {
            count
        };
        let date = if use_color {
            date.dimmed().to_string()
        } else {
            date
        };
        let snippet = if use_color {
            snippet.dimmed().to_string()
        } else {
            snippet
        };
        println!(
            "[{i}] {title}{separator}{agent}{separator}{count}{separator}{date}{separator}{snippet}"
        );
    }
}

pub fn fuzzy_filter_sessions(
    items: &[SessionDisplay],
    query: &str,
) -> (Vec<SessionDisplay>, Vec<String>) {
    let (filters, terms) = parse_fuzzy_query(query);
    let filtered = apply_filters(items, &filters);
    if terms.is_empty() {
        return (filtered, terms);
    }
    let matcher = SkimMatcherV2::default();
    let mut scored: Vec<(i64, SessionDisplay)> = filtered
        .iter()
        .filter_map(|item| {
            let mut total = 0i64;
            for term in &terms {
                let score = matcher.fuzzy_match(&item.match_text, term)?;
                total += score;
            }
            Some((total, item.clone()))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0));
    (scored.into_iter().map(|(_, item)| item).collect(), terms)
}

pub fn parse_fuzzy_query(input: &str) -> (FilterSpec, Vec<String>) {
    let mut filters = FilterSpec::default();
    let mut terms = Vec::new();
    for raw in input.split_whitespace() {
        let Some((key, value)) = raw.split_once(':') else {
            terms.push(raw.to_string());
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        match key {
            "agent" => filters.agent = Some(value.to_string()),
            "title" => filters.title = Some(value.to_string()),
            "id" => filters.id = Some(value.to_string()),
            "contains" => filters.contains = Some(value.to_string()),
            _ => terms.push(raw.to_string()),
        }
    }
    (filters, terms)
}

pub fn apply_filters(items: &[SessionDisplay], filters: &FilterSpec) -> Vec<SessionDisplay> {
    let agent = filters.agent.as_ref().map(|s| s.to_lowercase());
    let title = filters.title.as_ref().map(|s| s.to_lowercase());
    let id = filters.id.as_ref().map(|s| s.to_lowercase());
    let contains = filters.contains.as_ref().map(|s| s.to_lowercase());
    items
        .iter()
        .filter(|item| {
            if let Some(agent) = agent.as_ref()
                && !item.agent.to_lowercase().contains(agent)
            {
                return false;
            }
            if let Some(title) = title.as_ref()
                && !item.title.to_lowercase().contains(title)
            {
                return false;
            }
            if let Some(id) = id.as_ref()
                && !item.session_id.to_lowercase().contains(id)
            {
                return false;
            }
            if let Some(contains) = contains.as_ref()
                && !item.match_text.to_lowercase().contains(contains)
            {
                return false;
            }
            true
        })
        .cloned()
        .collect()
}

pub fn color_enabled() -> bool {
    io::stdout().is_terminal()
        && io::stderr().is_terminal()
        && std::env::var_os("NO_COLOR").is_none()
}

pub fn highlight_terms(text: &str, terms: &[String], use_color: bool) -> String {
    if !use_color || terms.is_empty() {
        return text.to_string();
    }
    let mut ranges = Vec::new();
    let text_lower = text.to_lowercase();
    for term in terms {
        let term_lower = term.to_lowercase();
        if term_lower.is_empty() {
            continue;
        }
        for (start, _) in text_lower.match_indices(&term_lower) {
            ranges.push((start, start + term_lower.len()));
        }
    }
    if ranges.is_empty() {
        return text.to_string();
    }
    ranges.sort_by(|a, b| a.0.cmp(&b.0));
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
            continue;
        }
        merged.push((start, end));
    }
    let mut out = String::new();
    let mut cursor = 0;
    for (start, end) in merged {
        if cursor < start {
            out.push_str(&text[cursor..start]);
        }
        let slice = &text[start..end];
        out.push_str(&slice.yellow().bold().to_string());
        cursor = end;
    }
    if cursor < text.len() {
        out.push_str(&text[cursor..]);
    }
    out
}

pub fn truncate_text(input: &str, max: usize) -> String {
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

pub fn render_session_html(session: &Session, messages: &[Message]) -> String {
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

pub fn render_session_markdown(session: &Session, messages: &[Message]) -> String {
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

pub fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn resolve_output_dir(dir: Option<PathBuf>) -> anyhow::Result<PathBuf> {
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
