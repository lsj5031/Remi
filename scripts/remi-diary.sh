#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: remi-diary.sh [--date YYYY-MM-DD] [--yesterday] [--sync] [--send]
                     [--max-sessions N] [--max-chars N] [--model MODEL]
                     [--progress-jsonl PATH]

Generates a daily markdown log from Remi's SQLite database.

Environment overrides:
  REMI_BIN                  Path to remi binary (default: PATH remi, then repo bundled binary)
  REMI_DB                   Path to remi.db (default: ~/.local/share/remi/remi.db)
  DIARY_DIR                 Output directory (default: ~/diary/remi)
  DIARY_MAX_SESSIONS        Max sessions to summarize (0 = all, default: 0)
  DIARY_MAX_CHARS           Max chars per session context (default: 12000)
  DIARY_LLM_MODEL           Optional model name passed to summary command via --model
  DIARY_SUMMARY_CMD         Summary command (default: "codex exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox")
  DIARY_TELEGRAM_CMD        Telegram send command (default: "uvx telegram-send")
  DIARY_PROGRESS_JSONL      Optional JSONL file to append structured progress events
  DIARY_SKIP_EXTERNAL_WARNING  Set to 1 to hide external-send warning

Warning:
  This script may send transcript data to external services:
  - your configured summary command/provider (summary generation)
  - Telegram (when --send is enabled)
USAGE
}

date_arg=""
sync_first="false"
max_sessions="${DIARY_MAX_SESSIONS:-0}"
max_chars="${DIARY_MAX_CHARS:-12000}"
llm_model="${DIARY_LLM_MODEL:-}"
progress_jsonl_path="${DIARY_PROGRESS_JSONL:-}"
run_id="$(date +%s)-$$"
summary_cmd_raw="${DIARY_SUMMARY_CMD:-codex exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox}"
read -r -a summary_cmd <<< "$summary_cmd_raw"
if [[ "${#summary_cmd[@]}" -eq 0 ]]; then
  echo "DIARY_SUMMARY_CMD must not be empty." >&2
  exit 1
fi

resolve_cli_path() {
  local cmd="$1"
  local resolved=""
  local candidate=""

  if resolved="$(command -v "$cmd" 2>/dev/null)"; then
    printf '%s\n' "$resolved"
    return 0
  fi

  candidate="$HOME/.local/bin/$cmd"
  if [[ -x "$candidate" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi

  for candidate in "$HOME"/.nvm/versions/node/*/bin/"$cmd"; do
    [[ -x "$candidate" ]] || continue
    resolved="$candidate"
  done
  if [[ -n "$resolved" ]]; then
    printf '%s\n' "$resolved"
    return 0
  fi

  return 1
}

if resolved_summary_bin="$(resolve_cli_path "${summary_cmd[0]}")"; then
  summary_cmd[0]="$resolved_summary_bin"
fi

send_telegram="false"
telegram_cmd_raw="${DIARY_TELEGRAM_CMD:-uvx telegram-send}"
read -r -a telegram_cmd <<< "$telegram_cmd_raw"
external_warning_shown="false"

warn_external_send() {
  if [[ "${DIARY_SKIP_EXTERNAL_WARNING:-0}" == "1" ]]; then
    return
  fi
  if [[ "$external_warning_shown" == "true" ]]; then
    return
  fi
  external_warning_shown="true"
  cat >&2 <<'WARN'
Warning: this script may send transcript data to external services:
- your configured summary command/provider (summary generation)
- Telegram (when --send is enabled)
Review/redact sensitive data before use.
WARN
}

emit_progress() {
  local phase="$1"
  local status="$2"
  shift 2

  [[ -n "$progress_jsonl_path" ]] || return 0

  python - "$progress_jsonl_path" "$run_id" "$phase" "$status" "$target_date" "$$" "$@" <<'PY'
from datetime import datetime, timezone
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
event = {
    "ts": datetime.now(timezone.utc).isoformat(),
    "run_id": sys.argv[2],
    "phase": sys.argv[3],
    "status": sys.argv[4],
    "target_date": sys.argv[5],
    "pid": int(sys.argv[6]),
}
rest = sys.argv[7:]
if len(rest) % 2 != 0:
    raise SystemExit("emit_progress expects key/value pairs")
for idx in range(0, len(rest), 2):
    event[rest[idx]] = rest[idx + 1]
path.parent.mkdir(parents=True, exist_ok=True)
with path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event, ensure_ascii=False) + "\n")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      date_arg="$2"
      shift 2
      ;;
    --yesterday)
      date_arg="yesterday"
      shift
      ;;
    --sync)
      sync_first="true"
      shift
      ;;
    --send)
      send_telegram="true"
      shift
      ;;
    --max-sessions)
      max_sessions="$2"
      shift 2
      ;;
    --max-chars)
      max_chars="$2"
      shift 2
      ;;
    --model)
      llm_model="$2"
      shift 2
      ;;
    --progress-jsonl)
      progress_jsonl_path="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$date_arg" ]]; then
  date_arg="today"
fi

target_date="$(date -d "$date_arg" +%F)"
generated_at="$(date +"%F %H:%M")"

repo_bin="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/dist/bundled/bin/remi"
remi_bin_source="env"
if [[ -n "${REMI_BIN:-}" ]]; then
  remi_bin="$REMI_BIN"
elif resolved_remi_bin="$(resolve_cli_path "remi")"; then
  remi_bin="$resolved_remi_bin"
  remi_bin_source="path"
elif [[ -x "$repo_bin" ]]; then
  remi_bin="$repo_bin"
  remi_bin_source="bundled"
else
  remi_bin="remi"
  remi_bin_source="fallback"
fi
remi_db="${REMI_DB:-$HOME/.local/share/remi/remi.db}"
diary_dir="${DIARY_DIR:-$HOME/diary/remi}"

if [[ ! -f "$remi_db" ]]; then
  echo "Database not found: $remi_db" >&2
  exit 1
fi

if [[ "$send_telegram" == "true" ]] && ! command -v "${telegram_cmd[0]}" >/dev/null 2>&1; then
  echo "Telegram command not found: ${telegram_cmd[0]}" >&2
  exit 1
fi

sync_agents=(pi droid opencode claude amp codex)

mkdir -p "$diary_dir"

emit_progress "start" "started" \
  "sync_requested" "$sync_first" \
  "send_requested" "$send_telegram" \
  "remi_bin" "$remi_bin" \
  "remi_bin_source" "$remi_bin_source" \
  "remi_db" "$remi_db" \
  "diary_dir" "$diary_dir"

if [[ "$sync_first" == "true" ]]; then
  emit_progress "sync" "started" "agent_count" "${#sync_agents[@]}"
  for agent in "${sync_agents[@]}"; do
    emit_progress "sync-agent" "started" \
      "agent" "$agent" \
      "command" "$remi_bin sync --agent $agent"
    if "$remi_bin" sync --agent "$agent" >/dev/null; then
      emit_progress "sync-agent" "completed" \
        "agent" "$agent" \
        "command" "$remi_bin sync --agent $agent"
    else
      rc=$?
      emit_progress "sync-agent" "failed" \
        "agent" "$agent" \
        "command" "$remi_bin sync --agent $agent" \
        "exit_code" "$rc"
      emit_progress "sync" "failed" "agent" "$agent" "exit_code" "$rc"
      exit "$rc"
    fi
  done
  emit_progress "sync" "completed" "agent_count" "${#sync_agents[@]}"
fi

limit_clause=""
if [[ "$max_sessions" =~ ^[0-9]+$ ]] && [[ "$max_sessions" -gt 0 ]]; then
  limit_clause="limit $max_sessions"
fi

emit_progress "query" "started" "max_sessions" "$max_sessions" "max_chars" "$max_chars"

sessions_tsv="$(sqlite3 -separator $'\t' "$remi_db" "
  select
    s.id,
    s.agent,
    s.title,
    min(m.ts),
    max(m.ts),
    count(m.id)
  from sessions s
  join messages m on m.session_id = s.id
  where date(m.ts, 'localtime') = '$target_date'
  group by s.id
  order by max(m.ts) desc
  $limit_clause;
")"

summary_row="$(sqlite3 -separator $'\t' "$remi_db" "
  select
    count(distinct s.id),
    count(m.id)
  from sessions s
  join messages m on m.session_id = s.id
  where date(m.ts, 'localtime') = '$target_date';
")"

sessions_count="0"
messages_count="0"
if [[ -n "$summary_row" ]]; then
  sessions_count="${summary_row%%$'\t'*}"
  messages_count="${summary_row##*$'\t'}"
fi

emit_progress "query" "completed" \
  "sessions_count" "$sessions_count" \
  "messages_count" "$messages_count"

agents_row="$(sqlite3 -separator $'\t' "$remi_db" "
  select s.agent, count(distinct s.id)
  from sessions s
  join messages m on m.session_id = s.id
  where date(m.ts, 'localtime') = '$target_date'
  group by s.agent
  order by count(distinct s.id) desc;
")"

llm_summary=""
session_context_file=""
prompt_file=""
if [[ -n "$sessions_tsv" ]]; then
  emit_progress "summary" "started" "sessions_count" "$sessions_count"

  project_from_path() {
    local path="$1"
    local tag=""
    local home_dir="${HOME%/}"
    local home_user="${home_dir##*/}"

    if [[ "$path" == file://* ]]; then
      path="${path#file://}"
    fi

    if [[ "$path" == *"/.local/share/opencode/"* ]]; then
      echo "opencode"
      return
    fi
    if [[ "$path" == *"/.claude/"* || "$path" == *"/.local/share/claude-code/"* ]]; then
      echo "claude"
      return
    fi
    if [[ "$path" == *"/.local/share/amp/threads/"* ]]; then
      echo "amp-thread"
      return
    fi
    if [[ "$path" == *"/.factory/sessions/"* ]]; then
      local rel="${path#*/.factory/sessions/}"
      local dir_name="${rel%%/*}"
      local home_dir_esc="${home_dir//\//-}"
      home_dir_esc="${home_dir_esc#-}"
      if [[ "$dir_name" == -mnt-t-* ]]; then
        dir_name="${dir_name#-mnt-t-}"
      elif [[ "$dir_name" == -mnt-* ]]; then
        dir_name="${dir_name#-mnt-}"
      elif [[ "$dir_name" == "-${home_dir_esc}-"* ]]; then
        dir_name="${dir_name#"-${home_dir_esc}-"}"
      fi
      dir_name="${dir_name//-//}"
      echo "${dir_name:-pi}"
      return
    fi
    if [[ "$path" == *"/.pi/"* ]]; then
      echo "pi"
      return
    fi

    if [[ "$path" == "$home_dir/code/"* ]]; then
      local rel="${path#"$home_dir/code/"}"
      local first="${rel%%/*}"
      local second=""
      if [[ "$rel" == */* ]]; then
        local tail="${rel#*/}"
        second="${tail%%/*}"
      fi
      if [[ -n "$first" ]]; then
        if [[ -n "$second" && "$second" != .* ]]; then
          echo "code/$first/$second"
        else
          echo "code/$first"
        fi
        return
      fi
    fi

    if [[ "$path" == "$home_dir/"* ]]; then
      local rel_home="${path#"$home_dir/"}"
      local first_home="${rel_home%%/*}"
      if [[ -n "$first_home" && "$first_home" != .* && "$rel_home" == */* ]]; then
        local tail_home="${rel_home#*/}"
        local second_home="${tail_home%%/*}"
        if [[ -n "$second_home" ]]; then
          echo "$first_home/$second_home"
          return
        fi
      fi
    fi

    if [[ "$path" =~ /--home-${home_user}-([^/]+)--/ ]]; then
      tag="${BASH_REMATCH[1]}"
    elif [[ "$path" =~ /-home-${home_user}-([^/]+)/ ]]; then
      tag="${BASH_REMATCH[1]}"
    fi

    if [[ -n "$tag" ]]; then
      tag="${tag//-/\/}"
      echo "$tag"
      return
    fi

    if [[ -z "$path" ]]; then
      echo "unknown"
    else
      echo "$path"
    fi
  }

  infer_project_from_messages() {
    local sid="$1"
    local sid_sql="${sid//\'/\'\'}"

    local messages_file=""
    messages_file="$(mktemp)"
    sqlite3 "$remi_db" "
      select content
      from messages
      where session_id = '$sid_sql'
        and date(ts, 'localtime') = '$target_date'
      order by ts asc;
    " > "$messages_file" || {
      rm -f "$messages_file"
      return 0
    }

    python - "$messages_file" <<'PY' || true
import collections
import os
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
if not text.strip():
    raise SystemExit(0)

home = (os.environ.get("HOME") or str(pathlib.Path.home())).rstrip("/")
home_prefix = f"{home}/"

skip_second = {
    "src",
    "crates",
    "tests",
    "test",
    "docs",
    "scripts",
    "dist",
    "target",
    ".git",
    "node_modules",
}


def normalize(path: str):
    path = path.rstrip(".,;:)]}>")
    if not path.startswith(home_prefix):
        return None
    if path.startswith(f"{home}/.local/share/"):
        return None
    if path.startswith(f"{home}/.claude/"):
        return "claude"
    if path.startswith(f"{home}/code/"):
        rel = path[len(f"{home}/code/") :]
        parts = [p for p in rel.split("/") if p]
        if parts:
            first = parts[0]
            second = parts[1] if len(parts) >= 2 else ""
            if second and second not in skip_second and not second.startswith("."):
                return f"code/{first}/{second}"
            return f"code/{first}"
        return "code"
    rel_home = path[len(home_prefix) :]
    parts_home = [p for p in rel_home.split("/") if p]
    if len(parts_home) >= 2:
        first, second = parts_home[0], parts_home[1]
        if not first.startswith("."):
            return f"{first}/{second}"
    return None

counter = collections.Counter()
path_re = re.compile(rf"{re.escape(home)}/[^\s\"'`<>]+")
for raw in path_re.findall(text):
    tag = normalize(raw)
    if tag:
        counter[tag] += 1

if counter:
    print(counter.most_common(1)[0][0])
PY

    rm -f "$messages_file"
  }

  amp_workspace_from_thread() {
    local thread_path="$1"
    if [[ ! -f "$thread_path" ]]; then
      return 0
    fi

    python - "$thread_path" <<'PY' || true
import json
import pathlib
import sys
import urllib.parse

path = pathlib.Path(sys.argv[1])
try:
    data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
except Exception:
    raise SystemExit(0)

env = data.get("env")
if not isinstance(env, dict):
    raise SystemExit(0)
initial = env.get("initial")
if not isinstance(initial, dict):
    raise SystemExit(0)
trees = initial.get("trees")
if not isinstance(trees, list):
    raise SystemExit(0)

for tree in trees:
    if not isinstance(tree, dict):
        continue
    uri = tree.get("uri")
    if isinstance(uri, str) and uri:
        if uri.startswith("file://"):
            parsed = urllib.parse.urlparse(uri)
            file_path = urllib.parse.unquote(parsed.path)
            if file_path:
                print(file_path)
                raise SystemExit(0)
        else:
            print(uri)
            raise SystemExit(0)

for tree in trees:
    if not isinstance(tree, dict):
        continue
    display = tree.get("displayName")
    if isinstance(display, str) and display.strip():
        print(display.strip())
        raise SystemExit(0)
PY
  }

  session_context_file="$(mktemp)"
  prompt_file="$(mktemp)"
  trap 'rm -f "$session_context_file" "$prompt_file"' EXIT

  declare -A project_counts

  echo "Daily context for $target_date" > "$session_context_file"
  echo >> "$session_context_file"

  session_index=0
  while IFS=$'\t' read -r session_id agent title first_ts last_ts msg_count; do
    [[ -z "$session_id" ]] && continue
    session_index=$((session_index + 1))
    if [[ -z "$title" ]]; then
      title="(untitled)"
    fi
    source_path="$(sqlite3 -separator $'\t' "$remi_db" "
      select p.source_path
      from messages m
      join provenance p on p.entity_id = m.id
      where m.session_id = '$session_id'
        and p.source_path is not null
      limit 1;
    ")"
    project_name="$(project_from_path "$source_path")"
    if [[ "$project_name" == "amp-thread" ]]; then
      amp_workspace="$(amp_workspace_from_thread "$source_path" || true)"
      if [[ -n "$amp_workspace" ]]; then
        project_name="$(project_from_path "$amp_workspace")"
      fi
    fi
    if [[ "$project_name" == "amp-thread" || "$project_name" == "unknown" || "$project_name" == "$source_path" ]]; then
      inferred_project="$(infer_project_from_messages "$session_id" || true)"
      if [[ -n "$inferred_project" ]]; then
        project_name="$inferred_project"
      elif [[ "$project_name" == "amp-thread" ]]; then
        project_name="amp"
      fi
    fi
    if [[ -n "$project_name" ]]; then
      project_counts["$project_name"]=$(( ${project_counts["$project_name"]:-0} + 1 ))
    fi

    echo "## Session $session_index" >> "$session_context_file"
    echo "Agent: $agent" >> "$session_context_file"
    echo "Title: $title" >> "$session_context_file"
    echo "ID: $session_id" >> "$session_context_file"
    echo "Source Tag: $project_name" >> "$session_context_file"
    echo "Window: $first_ts → $last_ts" >> "$session_context_file"
    echo "Messages: $msg_count" >> "$session_context_file"
    echo >> "$session_context_file"

    messages_tsv="$(sqlite3 -separator $'\t' "$remi_db" "
      select role, ts, content
      from messages
      where session_id = '$session_id'
        and date(ts, 'localtime') = '$target_date'
      order by ts asc;
    ")"

    if [[ -n "$messages_tsv" ]]; then
      formatted_messages="$(printf "%s" "$messages_tsv" | awk -F'\t' -v head=3 -v tail=3 '
        function min(a,b){return a<b?a:b}
        {
          line=sprintf("[%s] %s\n%s\n", $2, $1, $3)
          if (NR<=head) first[NR]=line
          last[NR]=line
          total=NR
        }
        END {
          for (i=1;i<=min(head,total);i++) printf "%s\n", first[i]
          if (total>head+tail) printf "... (skipping %d messages) ...\n\n", total-(head+tail)
          start=total-tail+1
          if (start<1) start=1
          for (i=start;i<=total;i++) printf "%s\n", last[i]
        }
      ')"
      if [[ ${#formatted_messages} -gt $max_chars ]]; then
        formatted_messages="${formatted_messages:0:max_chars}"
        formatted_messages+="\n\n[truncated]"
      fi
      echo "$formatted_messages" >> "$session_context_file"
    else
      echo "(no messages recorded for this date)" >> "$session_context_file"
    fi
    echo >> "$session_context_file"
  done <<< "$sessions_tsv"

  project_list=""
  for project in "${!project_counts[@]}"; do
    if [[ -n "$project_list" ]]; then
      project_list+=", "
    fi
    project_list+="$project"
  done
  if [[ -z "$project_list" ]]; then
    project_list="unknown"
  fi

  cat <<PROMPT > "$prompt_file"
You are summarizing a day's work based on agent session transcripts.

Projects seen today: $project_list

Write a clear, human-readable markdown summary with these sections:
1) Summary (3-6 bullet points)
2) Work Details (bullets with concrete tasks, outcomes, and context)
3) Decisions (bullets; use "None" if none are evident)
4) Open Questions / Follow-ups (bullets; include suggested next steps)
5) Morning Plan (3-6 bullets)

Rules:
- Use plain language, avoid internal tool jargon.
- Mention relevant session IDs in parentheses when useful.
- Prefix bullets with source tags using the Source Tag label, for example "[appifex/appifex] ...".
- Ensure every source tag appears at least once in Summary or Work Details.
- Do not invent facts or tasks that are not in the transcript.
- Keep bullets short and actionable.
- Treat "tool_result:" lines as concrete outcomes and use them as evidence for what was completed.
- Each session block only includes the first and last few messages from that day. Infer purpose accordingly.

Context starts below.
PROMPT

  cat "$session_context_file" >> "$prompt_file"

  warn_external_send
  llm_error_file="$(mktemp)"
  llm_summary_file="$(mktemp)"

  if ! command -v "${summary_cmd[0]}" >/dev/null 2>&1; then
    llm_summary_error="Summary CLI not found: ${summary_cmd[0]}"
    echo "Warning: $llm_summary_error" >&2
    emit_progress "summary" "failed" "reason" "$llm_summary_error"
  else
    llm_cmd=( "${summary_cmd[@]}" )
    if [[ -n "$llm_model" ]]; then
      llm_cmd+=(--model "$llm_model")
    fi
    llm_cmd+=(--output-last-message "$llm_summary_file")

    if ! "${llm_cmd[@]}" < "$prompt_file" > /dev/null 2>"$llm_error_file"; then
      llm_summary_error="$(tr '\n' ' ' < "$llm_error_file" | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//')"
      llm_summary_error="${llm_summary_error:0:500}"
      echo "Warning: failed to generate diary summary: ${llm_summary_error:-unknown error}" >&2
      emit_progress "summary" "failed" "reason" "${llm_summary_error:-unknown error}"
    else
      llm_summary="$(cat "$llm_summary_file")"
      emit_progress "summary" "completed" "summary_chars" "${#llm_summary}"
    fi
  fi

  rm -f "$llm_error_file" "$llm_summary_file"
else
  emit_progress "summary" "skipped" "reason" "no_sessions"
fi

output_path="$diary_dir/$target_date.md"

{
  echo "# Daily Log — $target_date"
  echo
  echo "Generated: $generated_at"
  echo
  if [[ -n "$llm_summary" ]]; then
    echo "$llm_summary"
  elif [[ -n "$sessions_tsv" ]]; then
    echo "## Summary"
    echo "- Sessions were found, but AI summary generation failed."
    if [[ -n "$llm_summary_error" ]]; then
      echo "- Error: $llm_summary_error"
    fi
  else
    echo "## Summary"
    echo "- No sessions found for this date."
  fi
  echo
  if [[ -n "$summary_row" ]]; then
    agents_summary=""
    if [[ -n "$agents_row" ]]; then
      while IFS=$'\t' read -r agent count; do
        [[ -z "$agent" ]] && continue
        if [[ -n "$agents_summary" ]]; then
          agents_summary+=", "
        fi
        agents_summary+="$agent ($count)"
      done <<< "$agents_row"
    fi
    echo "## Metadata"
    echo "- Sessions analyzed: $sessions_count"
    echo "- Messages analyzed: $messages_count"
    if [[ -n "$agents_summary" ]]; then
      echo "- Agents included: $agents_summary"
    fi
  fi
} > "$output_path"

emit_progress "output" "completed" \
  "output_path" "$output_path" \
  "sessions_count" "$sessions_count" \
  "messages_count" "$messages_count"

echo "$output_path"

if [[ "$send_telegram" == "true" ]]; then
  if [[ "$sessions_count" -eq 0 || "$messages_count" -eq 0 ]]; then
    emit_progress "telegram" "skipped" "reason" "no_sessions"
    emit_progress "complete" "completed" "output_path" "$output_path"
    exit 0
  fi
  markie_export="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/markie-export.sh"
  if [[ ! -x "$markie_export" ]]; then
    emit_progress "telegram" "failed" "reason" "markie export script not found"
    echo "Markie export script not found: $markie_export" >&2
    exit 1
  fi
  warn_external_send
  svg_path="$diary_dir/$target_date.svg"
  emit_progress "telegram" "started" "svg_path" "$svg_path"
  "$markie_export" --input "$output_path" --output "$svg_path" --format svg
  "${telegram_cmd[@]}" --file "$svg_path"
  emit_progress "telegram" "completed" "svg_path" "$svg_path"
fi

emit_progress "complete" "completed" "output_path" "$output_path"
