#!/usr/bin/env bash
# =============================================================================
# stop_xvenue.sh — shut the cross-venue multi-venue-feed stack down cleanly.
#
# Reverse dependency order (producers first, then TP subscribers, then the bus):
#   coinbase_fh -> spot_fh   (stop the producers first, so no more ticks arrive)
#   bridge      -> rdb       (then the TP subscribers)
#   tp                       (finally the message bus itself)
#
# Signals: Python feed-handlers -> SIGINT (clean asyncio shutdown);
#          q processes -> SIGTERM. Non-exiting processes escalate to SIGKILL.
#
# Usage:  ./stop_xvenue.sh
# =============================================================================
set -uo pipefail

RUN_DIR="${RUN_DIR:-/tmp/mvf-xvenue}"
PID_DIR="$RUN_DIR/pids"
GRACE_TRIES="${GRACE_TRIES:-20}"   # 0.5s each => 10s grace before SIGKILL

stop_one () {
  local name="$1" sig="${2:-TERM}"
  local pf="$PID_DIR/$name.pid"
  local pid i
  if [ ! -f "$pf" ]; then echo "  $name: no pid file (already stopped?)"; return; fi
  pid="$(cat "$pf")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "  $name: pid $pid not running"; rm -f "$pf"; return
  fi
  echo "  stopping $name (pid $pid) [SIG$sig]"
  kill -"$sig" "$pid" 2>/dev/null || true
  for ((i=0; i<GRACE_TRIES; i++)); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  if kill -0 "$pid" 2>/dev/null; then
    echo "    still alive after grace -> SIGKILL"
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pf"
}

echo "== stopping cross-venue stack (reverse order) =="
stop_one recal       TERM    # 0. recal sidecar (consumes RDB/bridge) — stop before them
stop_one perp_fh     INT     # 1. producers first
stop_one okx_fh      INT
stop_one coinbase_fh INT
stop_one spot_fh     INT
stop_one bridge      TERM    # 2. TP subscribers
stop_one rdb         TERM
stop_one tp          TERM    # 3. the bus

rmdir "$PID_DIR" 2>/dev/null || true
echo "== done.  logs preserved under $RUN_DIR/logs =="
