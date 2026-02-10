#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: markie-export.sh --input <markdown.md> --output <output.svg>
                        [--format svg|png] [--theme <theme.toml>]

Exports Markdown to SVG/PNG using the local Markie CLI.

Environment overrides:
  MARKIE_BIN   Path to markie binary (default: ~/.local/bin/markie)
USAGE
}

input_path=""
output_path=""
format="svg"
theme_path=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      input_path="$2"
      shift 2
      ;;
    --output)
      output_path="$2"
      shift 2
      ;;
    --format)
      format="$2"
      shift 2
      ;;
    --theme)
      theme_path="$2"
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

if [[ -z "$input_path" || -z "$output_path" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "$input_path" ]]; then
  echo "Input not found: $input_path" >&2
  exit 1
fi

format_lower="${format,,}"
if [[ "$format_lower" != "svg" && "$format_lower" != "png" ]]; then
  echo "Unsupported format: $format" >&2
  exit 1
fi

output_ext="${output_path##*.}"
output_ext="${output_ext,,}"
if [[ "$output_ext" != "svg" && "$output_ext" != "png" ]]; then
  echo "Output extension must be .svg or .png: $output_path" >&2
  exit 1
fi

if [[ "$output_ext" != "$format_lower" ]]; then
  echo "Output extension (.$output_ext) does not match --format $format_lower" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$theme_path" ]]; then
  theme_path="$script_dir/solarized_light.toml"
fi

if [[ ! -f "$theme_path" ]]; then
  echo "Theme file not found: $theme_path" >&2
  exit 1
fi

markie_bin="${MARKIE_BIN:-$HOME/.local/bin/markie}"
if [[ ! -x "$markie_bin" ]]; then
  if command -v markie >/dev/null 2>&1; then
    markie_bin="$(command -v markie)"
  else
    echo "markie binary not found. Set MARKIE_BIN or install markie." >&2
    exit 1
  fi
fi

"$markie_bin" --theme "$theme_path" --output "$output_path" "$input_path"

echo "$output_path"
