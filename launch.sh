#!/usr/bin/env bash
set -euo pipefail

# launch the conformance checking test networks in tmux
#
# a single --generate produces both systems (distributed + central) from the
# same constraint catalog, into <out-dir>/distributed and <out-dir>/central
#
# each system runs in its own tmux session, uses their own ports (if local addressing is used) 
# and writes to its own SQLite DB, so they can run side by side

# ---- defaults ----
BIN="./target/debug/node"                    # node binary (override with -b)
RUN_DIR="run"                                # base dir; systems are in $OUT_DIR/{distributed,central}
LOG_DIR=""                                   # empty => $RUN_DIR/<mode>/logs
GENERATE=false
NODES=19                                     # total nodes per system when generating
REPEAT=1                                     # copies of the constraint catalog to spread
ADDRESSING="local"                           # local | remote
SYSTEM="both"                                # distributed | central | both
SESSION_PREFIX="cc"                          # sessions: ${PREFIX}-distributed / ${PREFIX}-central
GEN_CMD=(cargo run --quiet --bin gen-config --)

usage() {
  cat <<USAGE
Usage: $0 [options]
 
  -b, --bin PATH         node binary to run            (default: $BIN)
  -o, --run-dir DIR      base run dir                  (default: $RUN_DIR)
  -l, --log-dir DIR      override log dir for ALL systems (default: <run-dir>/<mode>/logs)
  -g, --generate         (re)generate BOTH systems before launching
  -n, --nodes N          nodes per system when generating (default: $NODES)
  -r, --repeat K         catalog copies to spread      (default: $REPEAT)
  -a, --addressing WHICH local|remote when generating  (default: $ADDRESSING)
  -s, --system WHICH     which to launch: distributed|central|both (default: $SYSTEM)
      --session-prefix P tmux session prefix           (default: $SESSION_PREFIX)
  -h, --help             show this help
 
Layout produced/expected:
 
  $RUN_DIR/distributed/{configs,logs} + results.db
  $RUN_DIR/central/{configs,logs}     + results.db
USAGE
}

# ---- parse args ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bin)               BIN="$2"; shift 2 ;;
    -o|--run-dir|--out-dir) RUN_DIR="$2"; shift 2 ;;
    -l|--log-dir)           LOG_DIR="$2"; shift 2 ;;
    -g|--generate)          GENERATE=true; shift ;;
    -n|--nodes)             NODES="$2"; shift 2 ;;
    -r|--repeat)            REPEAT="$2"; shift 2 ;;
    -a|--addressing)        ADDRESSING="$2"; shift 2 ;;
    -s|--system)            SYSTEM="$2"; shift 2 ;;
    --session-prefix)       SESSION_PREFIX="$2"; shift 2 ;;
    -h|--help)              usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

 
case "$SYSTEM" in distributed|central|both) ;; *)
  echo "--system must be distributed|central|both (got '$SYSTEM')" >&2; exit 1 ;;
esac

# config dir for a system
config_dir() { echo "$RUN_DIR/$1/configs"; }
 
# log dir for a system (an explicit --log-dir wins)
log_dir() {
  if [[ -n "$LOG_DIR" ]]; then echo "$LOG_DIR"; else echo "$RUN_DIR/$1/logs"; fi
}

# ---- generate (clearing old configs) ----
if $GENERATE; then
  echo "regenerating configs into $RUN_DIR/{distributed,central}/configs (nodes=$NODES, repeat=$REPEAT)"
  for sub in distributed central; do
    mkdir -p "$RUN_DIR/$sub/configs" "$RUN_DIR/$sub/logs"
    find "$RUN_DIR/$sub/configs" -maxdepth 1 -name '*-config.toml' -delete
  done
  "${GEN_CMD[@]}" \
    --nodes "$NODES" \
    --repeat "$REPEAT" \
    --addressing "$ADDRESSING" \
    --out-dir "$RUN_DIR"
fi

# ---- sanity: node binary present ----
if [[ ! -x "$BIN" ]]; then
  echo "node binary '$BIN' not found or not executable (build it, or pass --bin)" >&2
  exit 1
fi

# ---- tmux session ----
# launch_system <config-subdir> <session-name>
launch_system() {
  local mode="$1" session="$2"
  local dir logs
  dir="$(config_dir "$mode")"
  logs="$(log_dir "$mode")"
 
  if [[ ! -d "$dir" ]]; then
    echo "config dir '$dir' does not exist (use --generate)" >&2; exit 1
  fi
  local configs=()
  while IFS= read -r f; do configs+=("$f"); done \
    < <(find "$dir" -maxdepth 1 -name '*-config.toml' | sort -V)
  if [[ ${#configs[@]} -eq 0 ]]; then
    echo "no *-config.toml in '$dir' (use --generate)" >&2; exit 1
  fi
 
  # logs and the db live next to the configs; make sure both dirs exist
  mkdir -p "$logs" "$RUN_DIR/$mode"
 
  tmux kill-session -t "$session" 2>/dev/null || true
  tmux new-session -d -s "$session" -n placeholder
  for cfg in "${configs[@]}"; do
    local name; name="$(basename "$cfg" .toml)"
    tmux new-window -t "$session" -n "$name"
    tmux send-keys -t "$session:$name" "'$BIN' -c '$cfg' -l '$logs'" C-m
  done
  tmux kill-window -t "$session:placeholder"
  echo "launched ${#configs[@]} nodes in tmux session '$session'"
  echo "  configs: $dir"
  echo "  logs:    $logs"
  echo "  db:      $RUN_DIR/$mode/results.db"
}
 
[[ "$SYSTEM" == "distributed" || "$SYSTEM" == "both" ]] && \
  launch_system distributed "${SESSION_PREFIX}-distributed"
[[ "$SYSTEM" == "central" || "$SYSTEM" == "both" ]] && \
  launch_system central "${SESSION_PREFIX}-central"
 
echo "attach with: tmux attach -t ${SESSION_PREFIX}-distributed   (or -central)"

