#!/usr/bin/env bash
set -euo pipefail

# ---- defaults ----
BIN="./target/debug/distributed_online_cc"
CONFIG_DIR="run/configs"
LOG_DIR="run/logs"
GENERATE=false
NODES=5
SESSION="p2ptest"
GEN_CMD=(cargo run --quiet --bin gen-config --)

usage() {
  cat <<EOF
Usage: $0 [options]

Launch a local P2P test network in tmux, one window per node.

  -b, --bin PATH         node binary to run          (default: $BIN)
  -d, --config-dir DIR   directory of node*.toml      (default: $CONFIG_DIR)
  -l, --log-dir DIR      directory for node logs      (default: $LOG_DIR)
  -g, --generate         (re)generate configs into DIR before launching
  -n, --nodes N          node count when generating   (default: $NODES)
  -h, --help             show this help

Without --generate, whatever node*.toml files already exist in DIR are launched
as-is.
EOF
}

# ---- parse args ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bin)        BIN="$2"; shift 2 ;;
    -d|--config-dir) CONFIG_DIR="$2"; shift 2 ;;
    -l|--log-dir)    LOG_DIR="$2"; shift 2 ;;
    -g|--generate)   GENERATE=true; shift ;;
    -n|--nodes)      NODES="$2"; shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# ---- generate (clearing stale) ----
if $GENERATE; then
  mkdir -p "$CONFIG_DIR"
  echo "regenerating $NODES configs in $CONFIG_DIR (removing old node*.toml)"
  find "$CONFIG_DIR" -maxdepth 1 -name 'node*.toml' -delete
  "${GEN_CMD[@]}" --nodes "$NODES" --mode local --out-dir "$CONFIG_DIR"
fi

# ---- collect config files in numeric order ----
if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "config dir '$CONFIG_DIR' does not exist (use --generate to create it)" >&2
  exit 1
fi
CONFIGS=()
while IFS= read -r f; do CONFIGS+=("$f"); done \
  < <(find "$CONFIG_DIR" -maxdepth 1 -name 'node*.toml' | sort -V)
if [[ ${#CONFIGS[@]} -eq 0 ]]; then
  echo "no node*.toml in '$CONFIG_DIR' (use --generate to create them)" >&2
  exit 1
fi

# ---- sanity: node binary present ----
if [[ ! -x "$BIN" ]]; then
  echo "node binary '$BIN' not found or not executable (build it, or pass --bin)" >&2
  exit 1
fi

# ---- ensure log dir exists (main.rs also does this, but do it up front) ----
mkdir -p "$LOG_DIR"

# ---- tmux session ----
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -n placeholder
for cfg in "${CONFIGS[@]}"; do
  name="$(basename "$cfg" .toml)"
  tmux new-window -t "$SESSION" -n "$name"
  tmux send-keys -t "$SESSION:$name" "'$BIN' -c '$cfg' -l '$LOG_DIR'" C-m
done
tmux kill-window -t "$SESSION:placeholder"
echo "launched ${#CONFIGS[@]} nodes in tmux session '$SESSION' (logs in $LOG_DIR)"
tmux attach -t "$SESSION"
