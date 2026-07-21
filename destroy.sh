#!/usr/bin/env bash
set -euo pipefail

# stops the conformance-checker networks (kills the tmux sessions) and cleans up
# by default tears down both the distributed and central sessions
 
# ---- defaults ----
SESSION_PREFIX="cc"                          # kills ${PREFIX}-distributed and ${PREFIX}-central
SYSTEM="both"                                # distributed | central | both
SESSIONS=()                                  # explicit sessions (override prefix/system)
SOCKET_GLOB="/tmp/event_listener*.sock"
CLEAN_SOCKETS=true
CLEAN_LOGS=false
LOG_DIR="run/logs"
 
usage() {
  cat <<EOF
Usage: $0 [options]
 
  -s, --system WHICH     which to stop: distributed|central|both (default: $SYSTEM)
      --session-prefix P tmux session prefix           (default: $SESSION_PREFIX)
      --session NAME     kill this exact session (repeatable; overrides --system)
      --socket-glob PAT  socket pattern to remove      (default: $SOCKET_GLOB)
      --keep-sockets     do NOT remove unix sockets
      --clean-logs       also remove \$LOG_DIR/*.log
      --log-dir DIR      log directory for --clean-logs (default: $LOG_DIR)
  -h, --help             show this help
EOF
}
 
while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--system)      SYSTEM="$2"; shift 2 ;;
    --session-prefix) SESSION_PREFIX="$2"; shift 2 ;;
    --session)        SESSIONS+=("$2"); shift 2 ;;
    --socket-glob)    SOCKET_GLOB="$2"; shift 2 ;;
    --keep-sockets)   CLEAN_SOCKETS=false; shift ;;
    --clean-logs)     CLEAN_LOGS=true; shift ;;
    --log-dir)        LOG_DIR="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# build session list from --system if none given explicitly
if [[ ${#SESSIONS[@]} -eq 0 ]]; then
  case "$SYSTEM" in
    distributed) SESSIONS=("${SESSION_PREFIX}-distributed") ;;
    central)     SESSIONS=("${SESSION_PREFIX}-central") ;;
    both)        SESSIONS=("${SESSION_PREFIX}-distributed" "${SESSION_PREFIX}-central") ;;
    *) echo "--system must be distributed|central|both" >&2; exit 1 ;;
  esac
fi

# ---- kill the tmux sessions ----
for session in "${SESSIONS[@]}"; do
  if tmux has-session -t "$session" 2>/dev/null; then
    tmux kill-session -t "$session"
    echo "killed tmux session '$session'"
  else
    echo "no tmux session '$session' (already stopped?)"
  fi
done

# ---- remove unix sockets ----
if $CLEAN_SOCKETS; then
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
      < <(find "$LOG_DIR" -maxdepth 1 -name '*.log')
  fi
  if [[ ${#logs[@]} -gt 0 ]]; then
    rm -f "${logs[@]}"
    echo "removed ${#logs[@]} log file(s) from $LOG_DIR"
  else
    echo "no *.log files in $LOG_DIR"
  fi
fi

