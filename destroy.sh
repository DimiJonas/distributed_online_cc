#!/usr/bin/env bash
set -euo pipefail

# stops the conformance-checker networks (kills the tmux sessions) and cleans up
# by default tears down both the decentralized and central sessions
#
# expected layout:
#
#   <run-dir>/<mode>/configs/    node config toml files
#   <run-dir>/<mode>/logs/       one log file per node
#   <run-dir>/<mode>/results.db  sqlite result db (+ -wal / -shm)

# ---- defaults ----
SESSION_PREFIX="cc"                          # kills ${PREFIX}-decentralized and ${PREFIX}-central
SYSTEM="both"                                # decentralized | central | both
SESSIONS=()                                  # explicit sessions (override prefix/system)
RUN_DIR="run"
LOG_DIR=""                                   # empty => $RUN_DIR/<mode>/logs
DB_DIR=""                                    # empty => $RUN_DIR/<mode>
SOCKET_GLOB=""                               # empty => per-system defaults
CLEAN_SOCKETS=true
CLEAN_LOGS=false
CLEAN_DB=false
CLEAN_CONFIGS=false
PURGE=false
 
sage() {
  cat <<USAGE
Usage: $0 [options]
 
  -s, --system WHICH     which to stop: decentralized|central|both (default: $SYSTEM)
      --session-prefix P tmux session prefix           (default: $SESSION_PREFIX)
      --session NAME     kill this exact session (repeatable; overrides --system)
  -o, --run-dir DIR      base run dir                  (default: $RUN_DIR)
      --log-dir DIR      override log dir  (default: <run-dir>/<mode>/logs)
      --db-dir DIR       override db dir   (default: <run-dir>/<mode>)
      --socket-glob PAT  socket pattern to remove (default: per-system patterns)
      --keep-sockets     do NOT remove unix sockets
      --clean-logs       also remove <log-dir>/*.log
      --clean-db         also remove <db-dir>/*.db, *.db-wal and *.db-shm
      --clean-configs    also remove <run-dir>/<mode>/configs/*-config.toml
      --full             kill sessions and delete sockets, logs and databases
      --purge            remove <run-dir>/<mode> entirely (configs, logs, db)
  -h, --help             show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--system)            SYSTEM="$2"; shift 2 ;;
    --session-prefix)       SESSION_PREFIX="$2"; shift 2 ;;
    --session)              SESSIONS+=("$2"); shift 2 ;;
    -o|--run-dir|--out-dir) RUN_DIR="$2"; shift 2 ;;
    --log-dir)              LOG_DIR="$2"; shift 2 ;;
    --db-dir)               DB_DIR="$2"; shift 2 ;;
    --socket-glob)          SOCKET_GLOB="$2"; shift 2 ;;
    --keep-sockets)         CLEAN_SOCKETS=false; shift ;;
    --clean-logs)           CLEAN_LOGS=true; shift ;;
    --clean-db)             CLEAN_DB=true; shift ;;
    --clean-configs)        CLEAN_CONFIGS=true; shift ;;
    --full)                 CLEAN_LOGS=true; CLEAN_DB=true; shift ;;
    --purge)                PURGE=true; shift ;;
    -h|--help)              usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

case "$SYSTEM" in
  decentralized) MODES=(decentralized) ;;
  central)     MODES=(central) ;;
  both)        MODES=(decentralized central) ;;
  *) echo "--system must be decentralized|central|both (got '$SYSTEM')" >&2; exit 1 ;;
esac

# build session list from --system if none given explicitly
if [[ ${#SESSIONS[@]} -eq 0 ]]; then
  for mode in "${MODES[@]}"; do SESSIONS+=("${SESSION_PREFIX}-${mode}"); done
fi
 
log_dir_for() {
  if [[ -n "$LOG_DIR" ]]; then echo "$LOG_DIR"; else echo "$RUN_DIR/$1/logs"; fi
}
db_dir_for() {
  if [[ -n "$DB_DIR" ]]; then echo "$DB_DIR"; else echo "$RUN_DIR/$1"; fi
}
# sockets are named by gen-config; keep the patterns in sync with it
socket_globs_for() {
  if [[ -n "$SOCKET_GLOB" ]]; then echo "$SOCKET_GLOB"; return; fi
  case "$1" in
    decentralized) echo "/tmp/event_listener_[0-9]*.sock" ;;
    central)     echo "/tmp/event_listener_central.sock /tmp/event_listener_forwarder*.sock" ;;
  esac
}
 
# remove files matching a find expression, reporting what happened
remove_matching() {
  local label="$1" dir="$2"; shift 2
  local files=()
  if [[ -d "$dir" ]]; then
    while IFS= read -r f; do files+=("$f"); done \
      < <(find "$dir" -maxdepth 1 -type f \( "$@" \))
  fi
  if [[ ${#files[@]} -gt 0 ]]; then
    rm -f "${files[@]}"
    echo "removed ${#files[@]} $label file(s) from $dir"
  else
    echo "no $label files in $dir"
  fi
}

# ---- kill the tmux sessions ----
for session in "${SESSIONS[@]}"; do
  if tmux has-session -t "$session" 2>/dev/null; then
    tmux kill-session -t "$session"
    echo "killed tmux session '$session'"
  else
    echo "no tmux session '$session' (already stopped?)"
  fi
done

# ---- per system cleanup ----
for mode in "${MODES[@]}"; do
  echo "--- $mode ---"
 
  if $CLEAN_SOCKETS; then
    shopt -s nullglob
    # shellcheck disable=SC2206  # word splitting is intended because of several patterns
    candidates=( $(socket_globs_for "$mode") )
    shopt -u nullglob
    # a pattern without a wildcard survives nullglob, so keep only real files
    sockets=()
    for s in ${candidates[@]+"${candidates[@]}"}; do
      [[ -e "$s" ]] && sockets+=("$s")
    done
    if [[ ${#sockets[@]} -gt 0 ]]; then
      rm -f "${sockets[@]}"
      echo "removed ${#sockets[@]} socket(s)"
    else
      echo "no sockets matching $(socket_globs_for "$mode")"
    fi
  fi
 
  if $PURGE; then
    if [[ -d "$RUN_DIR/$mode" ]]; then
      rm -rf "${RUN_DIR:?}/$mode"
      echo "purged $RUN_DIR/$mode"
    else
      echo "nothing to purge at $RUN_DIR/$mode"
    fi
    continue   # nothing left to clean selectively
  fi
 
  $CLEAN_LOGS    && remove_matching "log"    "$(log_dir_for "$mode")" -name '*.log'
  $CLEAN_DB      && remove_matching "db"     "$(db_dir_for "$mode")" \
                      -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm'
  $CLEAN_CONFIGS && remove_matching "config" "$RUN_DIR/$mode/configs" -name '*-config.toml'
 
  true   # keep `set -e` happy when all the flags above are false
done
