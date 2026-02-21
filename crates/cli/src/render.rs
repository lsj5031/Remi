use std::collections::VecDeque;

use anyhow::Context;
use askama::Template;
use clap::ValueEnum;
use core_model::{Message, Session};
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use serde_json::Value;

use crate::ui::truncate_text;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HtmlSafety {
    Strict,
    Relaxed,
    Trusted,
}

#[derive(Template)]
#[template(path = "session.html")]
pub struct SessionTemplate<'a> {
    pub title: &'a str,
    pub agent: &'a str,
    pub id: &'a str,
    pub message_count: usize,
    pub messages: Vec<ViewMessage>,
}

pub struct ViewMessage {
    pub role: String,
    pub is_user: bool,
    pub is_tool: bool,
    pub ts: String,
    pub content_html: String,
}

#[derive(Clone)]
struct PendingToolCall {
    name: String,
    label: String,
}

pub fn render_session_html(
    session: &Session,
    messages: &[Message],
    safety: HtmlSafety,
) -> anyhow::Result<String> {
    let view_messages = if safety == HtmlSafety::Strict {
        build_strict_messages(messages)
    } else {
        build_markdown_messages(messages, safety)
    };
    let tpl = SessionTemplate {
        title: &session.title,
        agent: session.agent.as_str(),
        id: &session.id,
        message_count: messages.len(),
        messages: view_messages,
    };
    tpl.render()
        .with_context(|| "rendering session HTML template")
}

fn build_markdown_messages(messages: &[Message], safety: HtmlSafety) -> Vec<ViewMessage> {
    let mut pending_tool_calls = VecDeque::new();
    let mut view_messages = Vec::with_capacity(messages.len());
    for m in messages {
        let is_tool = message_contains_tool_markers(&m.content);
        if !is_tool {
            pending_tool_calls.clear();
        }
        view_messages.push(ViewMessage {
            role: if is_tool {
                "tool".to_string()
            } else {
                m.role.clone()
            },
            is_user: m.role.eq_ignore_ascii_case("user") && !is_tool,
            is_tool,
            ts: m.ts.to_rfc3339(),
            content_html: render_markdown_to_html(&m.content, &mut pending_tool_calls, safety),
        });
    }
    view_messages
}

fn build_strict_messages(messages: &[Message]) -> Vec<ViewMessage> {
    let mut view_messages = Vec::with_capacity(messages.len());
    for m in messages {
        let is_tool = message_contains_tool_markers(&m.content);
        view_messages.push(ViewMessage {
            role: if is_tool {
                "tool".to_string()
            } else {
                m.role.clone()
            },
            is_user: m.role.eq_ignore_ascii_case("user") && !is_tool,
            is_tool,
            ts: m.ts.to_rfc3339(),
            content_html: format!("<pre>{}</pre>", escape_html(&m.content)),
        });
    }
    view_messages
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

fn message_contains_tool_markers(text: &str) -> bool {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .any(|line| strip_tool_use_line(line).is_some() || strip_tool_result_line(line).is_some())
}

fn strip_tool_use_line(line: &str) -> Option<&str> {
    strip_tool_line(line, "tool_use")
}

fn strip_tool_result_line(line: &str) -> Option<&str> {
    strip_tool_line(line, "tool_result")
}

fn strip_tool_line<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(marker)?.strip_prefix(':')?;
    Some(rest.strip_prefix(' ').unwrap_or(rest))
}

fn format_tool_call_label(raw: &str) -> String {
    let mut parts = raw.trim().splitn(2, ' ');
    let Some(name) = parts.next() else {
        return String::new();
    };
    let params = parts.next().unwrap_or("").trim();
    if params.is_empty() {
        return name.to_string();
    }
    if name == "apply_patch"
        && let Some(summary) = summarize_apply_patch_params(params)
    {
        return format!("{name} {summary}");
    }
    if let Ok(value) = serde_json::from_str::<Value>(params)
        && let Some(summary) = summarize_tool_params(name, &value)
    {
        return format!("{name} {summary}");
    }
    format!("{name} {}", truncate_text(params, 240))
}

fn extract_tool_name(raw: &str) -> String {
    raw.split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

fn summarize_apply_patch_params(params: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(params).ok()?;
    let patch_text = value.get("patchText").and_then(Value::as_str)?;
    let mut files = Vec::new();
    let mut updates = 0usize;
    let mut adds = 0usize;
    let mut deletes = 0usize;
    for line in patch_text.lines() {
        if let Some(path) = line.strip_prefix("*** Update File: ") {
            updates += 1;
            push_unique_file(&mut files, path);
        } else if let Some(path) = line.strip_prefix("*** Add File: ") {
            adds += 1;
            push_unique_file(&mut files, path);
        } else if let Some(path) = line.strip_prefix("*** Delete File: ") {
            deletes += 1;
            push_unique_file(&mut files, path);
        }
    }
    if files.is_empty() {
        return Some(format!(
            "{{\"patchText\":\"{}\"}}",
            truncate_text(patch_text, 120)
        ));
    }
    let preview = files.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
    let extra = if files.len() > 3 {
        format!(" (+{} more)", files.len() - 3)
    } else {
        String::new()
    };
    let mut ops = Vec::new();
    if updates > 0 {
        ops.push(format!("update:{updates}"));
    }
    if adds > 0 {
        ops.push(format!("add:{adds}"));
    }
    if deletes > 0 {
        ops.push(format!("delete:{deletes}"));
    }
    Some(format!("files: {preview}{extra}; ops {}", ops.join(", ")))
}

fn push_unique_file(files: &mut Vec<String>, path: &str) {
    let path = path.trim().to_string();
    if !path.is_empty() && !files.contains(&path) {
        files.push(path);
    }
}

fn summarize_tool_params(name: &str, value: &Value) -> Option<String> {
    let Value::Object(map) = value else {
        return None;
    };
    let mut ordered_keys = Vec::new();
    match name {
        "shell_command" => ordered_keys.extend(["command", "workdir", "timeout_ms"]),
        "Read" => ordered_keys.extend(["path", "read_range"]),
        "finder" => ordered_keys.extend(["path", "query", "type"]),
        _ => ordered_keys.extend(["path", "query", "command", "workdir", "name", "type"]),
    }
    for key in map.keys() {
        if !ordered_keys.iter().any(|k| k == key) {
            ordered_keys.push(key.as_str());
        }
    }
    let mut parts = Vec::new();
    for key in ordered_keys {
        let Some(val) = map.get(key) else {
            continue;
        };
        parts.push(format!("{key}={}", format_tool_param_value(val)));
        if parts.len() >= 5 {
            break;
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join(" "))
}

fn format_tool_param_value(value: &Value) -> String {
    match value {
        Value::String(s) => format!("\"{}\"", truncate_text(s, 100)),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        _ => truncate_text(&value.to_string(), 120),
    }
}

fn render_markdown_to_html(
    text: &str,
    pending_tool_calls: &mut VecDeque<PendingToolCall>,
    safety: HtmlSafety,
) -> String {
    let preprocessed = preprocess_tools(text, pending_tool_calls);

    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);

    let parser = Parser::new_ext(&preprocessed.markdown, options);
    let mut in_diff = false;
    let mut diff_content = String::new();
    let mut out_events = Vec::new();

    for event in parser {
        match event {
            Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(lang)))
                if lang.as_ref() == "diff" =>
            {
                in_diff = true;
                diff_content.clear();
            }
            Event::End(TagEnd::CodeBlock) if in_diff => {
                in_diff = false;
                out_events.push(Event::Html(render_sota_diff(&diff_content).into()));
            }
            Event::Text(text) if in_diff => diff_content.push_str(&text),
            Event::Html(html) | Event::InlineHtml(html) if safety != HtmlSafety::Trusted => {
                out_events.push(Event::Text(html.into_string().into()))
            }
            Event::Html(html) => out_events.push(Event::Html(html)),
            Event::InlineHtml(html) => out_events.push(Event::InlineHtml(html)),
            _ if in_diff => {}
            _ => out_events.push(event),
        }
    }

    let mut html_output = String::new();
    pulldown_cmark::html::push_html(&mut html_output, out_events.into_iter());
    for (placeholder, block_html) in preprocessed.replacements {
        let wrapped = format!("<p>{placeholder}</p>");
        let wrapped_newline = format!("<p>\n{placeholder}\n</p>");
        let wrapped_leading = format!("<p>\n{placeholder}</p>");
        let wrapped_trailing = format!("<p>{placeholder}\n</p>");
        html_output = html_output.replace(&wrapped, &block_html);
        html_output = html_output.replace(&wrapped_newline, &block_html);
        html_output = html_output.replace(&wrapped_leading, &block_html);
        html_output = html_output.replace(&wrapped_trailing, &block_html);
        html_output = html_output.replace(&placeholder, &block_html);
    }
    html_output = html_output.replace(
        "<p><details class=\"tool-block\">",
        "<details class=\"tool-block\">",
    );
    html_output = html_output.replace(
        "<p>\n<details class=\"tool-block\">",
        "<details class=\"tool-block\">",
    );
    html_output = html_output.replace("</details></p>", "</details>");
    html_output = html_output.replace("</details>\n</p>", "</details>");
    html_output
}

struct ToolPreprocessResult {
    markdown: String,
    replacements: Vec<(String, String)>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ToolBlockKind {
    Use,
    Result,
}

fn preprocess_tools(
    text: &str,
    pending_tool_calls: &mut VecDeque<PendingToolCall>,
) -> ToolPreprocessResult {
    let mut markdown = String::new();
    let mut replacements = Vec::new();
    let mut in_tool = false;
    let mut tool_kind = None;
    let mut tool_title = String::new();
    let mut tool_content = String::new();

    let flush_tool = |markdown: &mut String,
                      replacements: &mut Vec<(String, String)>,
                      title: &str,
                      content: &str| {
        let token = format!("REMI_TOOL_BLOCK_TOKEN_{}", replacements.len());
        let rendered_content = if !content.trim().is_empty()
            && !title.ends_with(" - result")
            && title != "tool_result"
        {
            String::new()
        } else {
            render_tool_block_content(content.trim())
        };
        let block = if rendered_content.is_empty() {
            format!(
                "<details class=\"tool-block\"><summary>{}</summary></details>",
                escape_html(title)
            )
        } else {
            format!(
                "<details class=\"tool-block\"><summary>{}</summary><div class=\"tool-content\">{}</div></details>",
                escape_html(title),
                rendered_content
            )
        };
        if !markdown.ends_with("\n\n") {
            if !markdown.ends_with('\n') {
                markdown.push('\n');
            }
            markdown.push('\n');
        }
        markdown.push_str(&token);
        markdown.push_str("\n\n");
        replacements.push((token, block));
    };

    let lines: Vec<&str> = text.lines().collect();
    for i in 0..lines.len() {
        let line = lines[i];
        if tool_kind != Some(ToolBlockKind::Result) {
            if let Some(rest) = strip_tool_use_line(line) {
                if in_tool {
                    flush_tool(&mut markdown, &mut replacements, &tool_title, &tool_content);
                }
                let call_name = extract_tool_name(rest);
                let call_label = format_tool_call_label(rest);
                pending_tool_calls.push_back(PendingToolCall {
                    name: call_name,
                    label: call_label.clone(),
                });
                in_tool = true;
                tool_kind = Some(ToolBlockKind::Use);
                tool_title = call_label;
                tool_content.clear();
                continue;
            }
            if let Some(rest) = strip_tool_result_line(line) {
                if in_tool {
                    flush_tool(&mut markdown, &mut replacements, &tool_title, &tool_content);
                }
                in_tool = true;
                tool_kind = Some(ToolBlockKind::Result);
                tool_title =
                    if let Some(call) = pop_pending_tool_for_result(pending_tool_calls, rest) {
                        format!("{} - result", call.label)
                    } else {
                        "tool_result".to_string()
                    };
                tool_content.clear();
                tool_content.push_str(rest);
                tool_content.push('\n');
                continue;
            }
        }
        if in_tool {
            if tool_kind == Some(ToolBlockKind::Result) {
                if let Some(rest) = strip_tool_result_line(line)
                    && !pending_tool_calls.is_empty()
                {
                    flush_tool(&mut markdown, &mut replacements, &tool_title, &tool_content);
                    tool_title =
                        if let Some(call) = pop_pending_tool_for_result(pending_tool_calls, rest) {
                            format!("{} - result", call.label)
                        } else {
                            "tool_result".to_string()
                        };
                    tool_content.clear();
                    tool_content.push_str(rest);
                    tool_content.push('\n');
                    continue;
                }
                tool_content.push_str(line);
                tool_content.push('\n');
                continue;
            }
            let closes_tool = line.trim().is_empty()
                && lines.get(i + 1).is_some_and(|next| {
                    next.chars().next().is_some_and(|c| {
                        c.is_alphanumeric() || matches!(c, '#' | '`' | '-' | '*' | '>')
                    })
                });
            if closes_tool {
                flush_tool(&mut markdown, &mut replacements, &tool_title, &tool_content);
                in_tool = false;
                tool_kind = None;
            } else {
                tool_content.push_str(line);
                tool_content.push('\n');
            }
        } else {
            markdown.push_str(line);
            markdown.push('\n');
        }
    }

    if in_tool {
        flush_tool(&mut markdown, &mut replacements, &tool_title, &tool_content);
    }

    ToolPreprocessResult {
        markdown,
        replacements,
    }
}

fn pop_pending_tool_for_result(
    pending_tool_calls: &mut VecDeque<PendingToolCall>,
    result_head: &str,
) -> Option<PendingToolCall> {
    if pending_tool_calls.is_empty() {
        return None;
    }
    if result_looks_like_diff(result_head)
        && let Some(index) = pending_tool_calls
            .iter()
            .rposition(|call| call.name == "apply_patch")
    {
        return pending_tool_calls.remove(index);
    }
    pending_tool_calls.pop_front()
}

fn result_looks_like_diff(content: &str) -> bool {
    render_diff_from_tool_json(content).is_some()
        || render_diff_from_jsonish(content).is_some()
        || looks_like_unified_diff(content)
}

fn render_tool_block_content(content: &str) -> String {
    if content.is_empty() {
        return String::new();
    }
    if let Some(diff_html) = render_diff_from_tool_json(content) {
        return diff_html;
    }
    if let Some(diff_html) = render_diff_from_jsonish(content) {
        return diff_html;
    }
    if looks_like_unified_diff(content) {
        return render_sota_diff(content);
    }
    format!("<pre>{}</pre>", escape_html(content))
}

fn render_diff_from_tool_json(content: &str) -> Option<String> {
    let Ok(value) = serde_json::from_str::<Value>(content) else {
        return None;
    };
    let mut diffs = Vec::new();
    collect_diff_fields(&value, &mut diffs);
    if diffs.is_empty() {
        collect_synthetic_file_diffs(&value, &mut diffs);
    }
    if diffs.is_empty() {
        return None;
    }
    let mut out = String::new();
    for diff in diffs {
        out.push_str(&render_sota_diff(&diff));
    }
    Some(out)
}

fn render_diff_from_jsonish(content: &str) -> Option<String> {
    let diffs = extract_diff_strings_from_jsonish(content);
    if diffs.is_empty() {
        return None;
    }
    let mut out = String::new();
    for diff in diffs {
        out.push_str(&render_sota_diff(&diff));
    }
    Some(out)
}

fn collect_diff_fields(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(diff)) = map.get("diff") {
                out.push(diff.clone());
            }
            for child in map.values() {
                collect_diff_fields(child, out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_diff_fields(child, out);
            }
        }
        _ => {}
    }
}

fn collect_synthetic_file_diffs(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            let op = map
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| map.get("operation").and_then(Value::as_str));
            let path = map
                .get("path")
                .and_then(Value::as_str)
                .or_else(|| map.get("uri").and_then(Value::as_str));
            if let (Some(op), Some(path)) = (op, path)
                && let Some(diff) = synthetic_diff_for_file_op(op, path)
            {
                out.push(diff);
            }
            for child in map.values() {
                collect_synthetic_file_diffs(child, out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_synthetic_file_diffs(child, out);
            }
        }
        _ => {}
    }
}

fn synthetic_diff_for_file_op(op: &str, path: &str) -> Option<String> {
    let op = op.to_ascii_lowercase();
    if matches!(
        op.as_str(),
        "add" | "create" | "create_file" | "write_file" | "new_file"
    ) {
        return Some(format!(
            "Index: {path}\n--- /dev/null\n+++ {path}\n@@ -0,0 +1 @@\n+ [file created]"
        ));
    }
    if matches!(op.as_str(), "delete" | "remove" | "delete_file" | "rm") {
        return Some(format!(
            "Index: {path}\n--- {path}\n+++ /dev/null\n@@ -1 +0,0 @@\n- [file deleted]"
        ));
    }
    if matches!(
        op.as_str(),
        "write" | "update" | "modify" | "edit" | "overwrite"
    ) {
        return Some(format!(
            "Index: {path}\n--- {path}\n+++ {path}\n@@ -1 +1 @@\n- [previous content]\n+ [file updated]"
        ));
    }
    None
}

fn extract_diff_strings_from_jsonish(content: &str) -> Vec<String> {
    let needle = "\"diff\":\"";
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while let Some(found) = content[cursor..].find(needle) {
        let start = cursor + found + needle.len();
        let Some((decoded, end)) = decode_json_string_at(content, start) else {
            break;
        };
        out.push(decoded);
        cursor = end;
    }
    out
}

fn decode_json_string_at(content: &str, start: usize) -> Option<(String, usize)> {
    let bytes = content.as_bytes();
    let mut i = start;
    let mut out = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    return None;
                }
                match bytes[i] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        if i + 4 >= bytes.len() {
                            return None;
                        }
                        let hex = std::str::from_utf8(&bytes[i + 1..i + 5]).ok()?;
                        let codepoint = u16::from_str_radix(hex, 16).ok()?;
                        let ch = char::from_u32(codepoint as u32)?;
                        out.push(ch);
                        i += 4;
                    }
                    other => out.push(other as char),
                }
            }
            b'"' => return Some((out, i + 1)),
            byte => out.push(byte as char),
        }
        i += 1;
    }
    None
}

fn looks_like_unified_diff(content: &str) -> bool {
    if !(content.contains("diff --git")
        || content.contains("Index:")
        || content.lines().any(|line| line.starts_with("@@")))
    {
        return false;
    }
    let has_add = content
        .lines()
        .any(|line| line.starts_with('+') && !line.starts_with("+++"));
    let has_del = content
        .lines()
        .any(|line| line.starts_with('-') && !line.starts_with("---"));
    has_add && has_del
}

fn render_sota_diff(diff: &str) -> String {
    let mut html = String::from(
        r#"<div class="diff-viewer"><div class="diff-header">Code Changes</div><table class="diff-table"><tbody>"#,
    );
    for line in diff.lines() {
        let (row_class, marker, code) = if line.starts_with("+++ ") || line.starts_with("--- ") {
            ("diff-ctx", "", line)
        } else if let Some(rest) = line.strip_prefix('+') {
            ("diff-add", "+", rest)
        } else if let Some(rest) = line.strip_prefix('-') {
            ("diff-rem", "-", rest)
        } else if line.starts_with("@@") {
            ("diff-hunk", "", line)
        } else {
            ("diff-ctx", "", line)
        };
        html.push_str(&format!(
            r#"<tr class="{row_class}"><td class="diff-marker">{marker}</td><td class="diff-code">{}</td></tr>"#,
            escape_html(code)
        ));
    }
    html.push_str("</tbody></table></div>");
    html
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_diff_from_tool_json_payload() {
        let payload =
            r#"{"files":[{"diff":"Index: a\n@@ -1,2 +1,2 @@\n-old\n+new\n keep"}],"summary":"ok"}"#;
        let html = render_tool_block_content(payload);
        assert!(html.contains("diff-viewer"));
        assert!(html.contains("Code Changes"));
    }

    #[test]
    fn renders_synthetic_diff_for_write_and_delete_ops() {
        let payload = r#"{"files":[{"type":"write_file","path":"a.txt"},{"type":"delete_file","path":"b.txt"}]}"#;
        let html = render_tool_block_content(payload);
        assert!(html.contains("diff-viewer"));
        assert!(html.contains("a.txt"));
        assert!(html.contains("b.txt"));
    }

    #[test]
    fn carries_tool_call_label_into_following_result() {
        let mut queue = VecDeque::new();
        let use_html = render_markdown_to_html(
            r#"tool_use: Read {"path":"x.rs"}"#,
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let result_html =
            render_markdown_to_html("tool_result: ok", &mut queue, HtmlSafety::Relaxed);
        assert!(use_html.contains("Read path=&quot;x.rs&quot;"));
        assert!(result_html.contains("Read path=&quot;x.rs&quot; - result"));
    }

    #[test]
    fn nested_tool_markers_inside_result_are_not_reparsed() {
        let mut queue = VecDeque::new();
        let _ = render_markdown_to_html(
            r#"tool_use: Read {"path":"x.rs"}"#,
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let first_result = render_markdown_to_html(
            "tool_result: ok\n\ntool_use: Fake {\"path\":\"bad.rs\"}\n",
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let second_result =
            render_markdown_to_html("tool_result: follow-up", &mut queue, HtmlSafety::Relaxed);
        assert!(first_result.contains("Read path=&quot;x.rs&quot; - result"));
        assert!(first_result.contains("tool_use: Fake"));
        assert!(second_result.contains("<summary>tool_result</summary>"));
    }

    #[test]
    fn splits_multiple_result_lines_when_pending_tool_calls_exist() {
        let mut queue = VecDeque::new();
        let _ = render_markdown_to_html(
            "tool_use: Read {\"path\":\"a.rs\"}\ntool_use: Read {\"path\":\"b.rs\"}",
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let html = render_markdown_to_html(
            "tool_result: first\ntool_result: second",
            &mut queue,
            HtmlSafety::Relaxed,
        );
        assert!(html.contains("Read path=&quot;a.rs&quot; - result"));
        assert!(html.contains("Read path=&quot;b.rs&quot; - result"));
    }

    #[test]
    fn accepts_tool_result_marker_without_trailing_space() {
        let mut queue = VecDeque::new();
        let _ = render_markdown_to_html(
            r#"tool_use: shell_command {"command":"npm install"}"#,
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let html =
            render_markdown_to_html("tool_result:\nup to date", &mut queue, HtmlSafety::Relaxed);
        assert!(html.contains("shell_command command=&quot;npm install&quot; - result"));
    }

    #[test]
    fn prefers_apply_patch_label_for_diff_when_pending_queue_shifted() {
        let mut queue = VecDeque::new();
        let _ = render_markdown_to_html(
            "tool_use: apply_patch {\"patchText\":\"*** Begin Patch\\n*** Update File: a.rs\\n*** End Patch\"}\n\
             tool_use: Read {\"path\":\"a.rs\"}\n\
             tool_use: apply_patch {\"patchText\":\"*** Begin Patch\\n*** Update File: b.rs\\n*** End Patch\"}",
            &mut queue,
            HtmlSafety::Relaxed,
        );
        let diff_json =
            r#"tool_result: {"files":[{"diff":"Index: b.rs\n@@ -1 +1 @@\n-old\n+new"}]}"#;
        let html = render_markdown_to_html(diff_json, &mut queue, HtmlSafety::Relaxed);
        assert!(html.contains("apply_patch"));
        assert!(!html.contains("Read path=&quot;a.rs&quot; - result"));
        assert!(html.contains("diff-viewer"));
    }

    #[test]
    fn diff_renderer_keeps_headers_context_and_strips_line_prefix_marker_column() {
        let html = render_sota_diff("--- a.rs\n+++ b.rs\n-old\n+new");
        assert!(html.contains(
            r#"class="diff-ctx"><td class="diff-marker"></td><td class="diff-code">--- a.rs"#
        ));
        assert!(html.contains(
            r#"class="diff-ctx"><td class="diff-marker"></td><td class="diff-code">+++ b.rs"#
        ));
        assert!(html.contains(
            r#"class="diff-rem"><td class="diff-marker">-</td><td class="diff-code">old"#
        ));
        assert!(html.contains(
            r#"class="diff-add"><td class="diff-marker">+</td><td class="diff-code">new"#
        ));
    }

    #[test]
    fn summarizes_apply_patch_tool_label() {
        let label = format_tool_call_label(
            r#"apply_patch {"patchText":"*** Begin Patch\n*** Update File: a.rs\n*** Add File: b.rs\n*** Delete File: c.rs\n*** End Patch"}"#,
        );
        assert!(label.starts_with("apply_patch "));
        assert!(label.contains("a.rs"));
        assert!(label.contains("b.rs"));
        assert!(label.contains("c.rs"));
        assert!(!label.contains("*** Begin Patch"));
    }

    #[test]
    fn summarizes_shell_command_label() {
        let label = format_tool_call_label(
            r#"shell_command {"command":"cargo test","workdir":"/tmp/demo","timeout_ms":120000}"#,
        );
        assert!(label.contains("shell_command"));
        assert!(label.contains("command=\"cargo test\""));
        assert!(label.contains("workdir=\"/tmp/demo\""));
        assert!(label.contains("timeout_ms=120000"));
    }

    #[test]
    fn trusted_mode_keeps_raw_html() {
        let mut queue = VecDeque::new();
        let html = render_markdown_to_html("<span>ok</span>", &mut queue, HtmlSafety::Trusted);
        assert!(html.contains("<span>ok</span>"));
    }

    #[test]
    fn relaxed_mode_escapes_raw_html() {
        let mut queue = VecDeque::new();
        let html = render_markdown_to_html("<span>ok</span>", &mut queue, HtmlSafety::Relaxed);
        assert!(html.contains("&lt;span&gt;ok&lt;/span&gt;"));
    }

    #[test]
    fn marker_detection_handles_non_first_line_marker() {
        assert!(message_contains_tool_markers(
            "assistant preface\n\ntool_result: payload"
        ));
    }
}
