#!/usr/bin/env bash
set -euo pipefail

# ---- defaults ----
SESSION="p2ptest"
SOCKET_GLOB="/tmp/event_listener*.sock" 
CLEAN_SOCKETS=true
CLEAN_LOGS=false
LOG_DIR="run/logs"

usage() {
  cat <<EOF
Usage: $0 [options]

Stop the P2P test network: kill the tmux session and clean up leftovers.

  -s, --session NAME     tmux session to kill        (default: $SESSION)
      --socket-glob PAT  socket pattern to remove    (default: $SOCKET_GLOB)
      --keep-sockets     do NOT remove unix sockets
      --clean-logs       also remove $LOG_DIR/node*.log
      --log-dir DIR      log directory for --clean-logs (default: $LOG_DIR)
  -h, --help             show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--session)     SESSION="$2"; shift 2 ;;
    --socket-glob)    SOCKET_GLOB="$2"; shift 2 ;;
    --keep-sockets)   CLEAN_SOCKETS=false; shift ;;
    --clean-logs)     CLEAN_LOGS=true; shift ;;
    --log-dir)        LOG_DIR="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# ---- kill the tmux session ----
if tmux has-session -t "$SESSION" 2>/dev/null; then
  tmux kill-session -t "$SESSION"
  echo "killed tmux session '$SESSION'"
else
  echo "no tmux session '$SESSION' (already stopped?)"
fi

# ---- give nodes a moment to release sockets, then remove them ----
if $CLEAN_SOCKETS; then
  # nullglob so an empty match expands to nothing instead of the literal pattern
  shopt -s nullglob
  sockets=( $SOCKET_GLOB )
  shopt -u nullglob
  if [[ ${#sockets[@]} -gt 0 ]]; then
    rm -f "${sockets[@]}"
    echo "removed ${#sockets[@]} socket(s) matching $SOCKET_GLOB"
  else
    echo "no sockets matching $SOCKET_GLOB"
  fi
fi

# ---- optional: clean logs (off by default) ----
if $CLEAN_LOGS; then
  logs=()
  if [[ -d "$LOG_DIR" ]]; then
    while IFS= read -r f; do logs+=("$f"); done \
      < <(find "$LOG_DIR" -maxdepth 1 -name 'node*.log')
  fi
  if [[ ${#logs[@]} -gt 0 ]]; then
    rm -f "${logs[@]}"
    echo "removed ${#logs[@]} log file(s) from $LOG_DIR"
  else
    echo "no node*.log in $LOG_DIR"
  fi
fi
