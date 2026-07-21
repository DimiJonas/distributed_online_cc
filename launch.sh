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
OUT_DIR="run"                                # base dir; systems are in $OUT_DIR/{distributed,central}
LOG_DIR="run/logs"
GENERATE=false
NODES=19                                     # total nodes per system when generating
REPEAT=1                                     # copies of the constraint catalog to spread
SYSTEM="both"                                # distributed | central | both
SESSION_PREFIX="cc"                          # sessions: ${PREFIX}-distributed / ${PREFIX}-central
GEN_CMD=(cargo run --quiet --bin gen-config --)


usage() {
  cat <<EOF
Usage: $0 [options]
 
  -b, --bin PATH         node binary to run            (default: $BIN)
  -o, --out-dir DIR      base config dir               (default: $OUT_DIR)
  -l, --log-dir DIR      directory for node logs       (default: $LOG_DIR)
  -g, --generate         (re)generate BOTH systems before launching
  -n, --nodes N          nodes per system when generating (default: $NODES)
  -r, --repeat K         catalog copies to spread      (default: $REPEAT)
  -s, --system WHICH     which to launch: distributed|central|both (default: $SYSTEM)
      --session-prefix P tmux session prefix           (default: $SESSION_PREFIX)
  -h, --help             show this help
 
EOF
}


# ---- parse args ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bin)           BIN="$2"; shift 2 ;;
    -o|--out-dir)       OUT_DIR="$2"; shift 2 ;;
    -l|--log-dir)       LOG_DIR="$2"; shift 2 ;;
    -g|--generate)      GENERATE=true; shift ;;
    -n|--nodes)         NODES="$2"; shift 2 ;;
    -r|--repeat)        REPEAT="$2"; shift 2 ;;
    -s|--system)        SYSTEM="$2"; shift 2 ;;
    --session-prefix)   SESSION_PREFIX="$2"; shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 1 ;;
  esac
done
 
case "$SYSTEM" in distributed|central|both) ;; *)
  echo "--system must be distributed|central|both (got '$SYSTEM')" >&2; exit 1 ;;
esac

# ---- generate (clearing old configs) ----
if $GENERATE; then
  echo "regenerating configs into $OUT_DIR/{distributed,central} (nodes=$NODES, repeat=$REPEAT)"
  for sub in distributed central; do
    mkdir -p "$OUT_DIR/$sub"
    find "$OUT_DIR/$sub" -maxdepth 1 -name '*-config.toml' -delete
  done
  "${GEN_CMD[@]}" --nodes "$NODES" --repeat "$REPEAT" --placement local --out-dir "$OUT_DIR"
fi

# ---- sanity: node binary present ----
if [[ ! -x "$BIN" ]]; then
  echo "node binary '$BIN' not found or not executable (build it, or pass --bin)" >&2
  exit 1
fi

# ---- ensure log dir exists ----
mkdir -p "$LOG_DIR"

# ---- tmux session ----
# launch_system <config-subdir> <session-name>
launch_system() {
  local dir="$1" session="$2"
  if [[ ! -d "$dir" ]]; then
    echo "config dir '$dir' does not exist (use --generate)" >&2; exit 1
  fi
  local configs=()
  while IFS= read -r f; do configs+=("$f"); done \
    < <(find "$dir" -maxdepth 1 -name '*-config.toml' | sort -V)
  if [[ ${#configs[@]} -eq 0 ]]; then
    echo "no *-config.toml in '$dir' (use --generate)" >&2; exit 1
  fi
 
  tmux kill-session -t "$session" 2>/dev/null || true
  tmux new-session -d -s "$session" -n placeholder
  for cfg in "${configs[@]}"; do
    local name; name="$(basename "$cfg" .toml)"
    tmux new-window -t "$session" -n "$name"
    tmux send-keys -t "$session:$name" "'$BIN' -c '$cfg' -l '$LOG_DIR'" C-m
  done
  tmux kill-window -t "$session:placeholder"
  echo "launched ${#configs[@]} nodes in tmux session '$session'"
}
 
[[ "$SYSTEM" == "distributed" || "$SYSTEM" == "both" ]] && \
  launch_system "$OUT_DIR/distributed" "${SESSION_PREFIX}-distributed"
[[ "$SYSTEM" == "central" || "$SYSTEM" == "both" ]] && \
  launch_system "$OUT_DIR/central" "${SESSION_PREFIX}-central"
 
echo "logs in $LOG_DIR"
echo "attach with: tmux attach -t ${SESSION_PREFIX}-distributed   (or -central)"

