#!/usr/bin/env bash
# =============================================================================
# start_xvenue.sh — bring up the CROSS-VENUE multi-venue-feed stack (Item 4 / G4).
#
# Like start_live.sh, but for a genuine cross-venue lead-lag question: Binance SPOT
# vs Coinbase SPOT (pairMode crossVenue), instead of Binance spot-vs-perp.
#
#   Binance Spot WS  ─┐
#   Coinbase WS      ─┤→  Python feed-handlers  →  Tickerplant (TP) ─┬─→ RDB
#                     │   (normalize @bookTicker / ticker)  :5010    │
#                     └ raw venue symbols (BTCUSDT / BTC-USD),       └─→ live_xvenue bridge :5099
#                       venue/inst-tagged, on the `bookticker` table     (canonicalize -> BTC, crossVenue)
#
# 7 processes, IN ORDER:
#   1. TP   (q tick.q)              — message bus, port 5010.
#   2. RDB  (q tick/r.q)            — captures every TP table in memory, port 5011.
#   3. BRIDGE (q proc/live_xvenue.q)— engine in CROSS-VENUE mode: maps `bookticker` rows
#                                     to quote_norm, canonicalizes raw symbols to roots
#                                     (BTC-USD/BTCUSDT/BTC-USDT -> BTC), pairMode crossVenue. :5099.
#   4. SPOT FH (binanceFH_REST.py)  — Binance SPOT @bookTicker (exchange-clock estimated, G1).
#   5. COINBASE FH (coinbaseFH.py)  — Coinbase Exchange `level2_batch` book-maintained BBO
#                                     (native exchange clock; CB_CHANNEL=ticker for the
#                                     legacy trade-driven path).
#   6. OKX FH (okxFH.py)            — OKX spot `bbo-tbt` tick-by-tick BBO (native exchange
#                                     clock; OKX_CHANNEL=books5 for the depth-5 fallback).
#                                     Offshore/Asia leg — geography-vs-Binance test (OQ-6).
#   7. PERP FH (binanceUMPerpFH.py) — Binance UM-perp trades+mark+depth (Part B capture)
#                                     into ptrades/pmark/pbupd; NO bookticker (kept out of
#                                     the spot crossVenue engine). Routed WS endpoints.
#
# Mutually exclusive with start_live.sh on the TP/RDB/bridge ports (separate RUN_DIR
# so logs/pids don't collide). Symbols are given as ROOTS; the venue-specific symbols
# are derived (BTC -> Binance BTCUSDT, Coinbase BTC-USD; engine uses canonical BTC).
#
# Usage:   ./start_xvenue.sh                 # default roots BTC ETH SOL DOGE XRP
#          ROOTS="BTC ETH" ./start_xvenue.sh
# Stop:    ./stop_xvenue.sh
# =============================================================================
set -uo pipefail

# ---- args -------------------------------------------------------------------
# --dashboard : after the stack is up, launch the live monitor TUI in the
#   foreground (scripts/dashboard.py). The stack is started session-detached
#   (setsid in launch()), so Ctrl+C exits ONLY the dashboard — the stack keeps
#   running. Re-attach later with: PY scripts/dashboard.py --port BRIDGE_PORT
RUN_DASHBOARD=0
for _a in "$@"; do [ "$_a" = "--dashboard" ] && RUN_DASHBOARD=1; done

# ---- configuration (override via env) ---------------------------------------
MVF_DIR="${MVF_DIR:-/home/paul/projects/multi-venue-feed}"
INGEST_DIR="${INGEST_DIR:-$MVF_DIR/ingest}"
Q="${Q:-/home/paul/.kx/bin/q}"
export QHOME="${QHOME:-/home/paul/.kx}"
PY="${PY:-/home/paul/projects/binance/.venv/bin/python}"
ROOTS="${ROOTS:-BTC ETH SOL DOGE XRP}"                        # canonical asset roots
TP_PORT="${TP_PORT:-5010}"
RDB_PORT="${RDB_PORT:-5011}"
BRIDGE_PORT="${BRIDGE_PORT:-5099}"
RECAL_PORT="${RECAL_PORT:-5098}"                             # recal sidecar query/liveness port
RECAL_INTERVAL_MS="${RECAL_INTERVAL_MS:-3600000}"            # hourly vol-adaptive recalibration
MOVE_MULT="${MOVE_MULT:-1}"                                  # global move-bar multiplier on the bridge
                                                             # (recal still targets R*/hr at the BASE bar;
                                                             # raise to e.g. 2 to keep only larger moves)
REST_SNAP_S="${REST_SNAP_S:-300}"
CB_CHANNEL="${CB_CHANNEL:-level2_batch}"                      # ticker = legacy trade-driven BBO
OKX_CHANNEL="${OKX_CHANNEL:-bbo-tbt}"                         # books5 = depth-5 snapshot fallback
RUN_DIR="${RUN_DIR:-/tmp/mvf-xvenue}"                         # separate from start_live.sh
LOG_DIR="$RUN_DIR/logs"
PID_DIR="$RUN_DIR/pids"
# Tickerplant journal dir — the SOLE durable raw-tick record (RDB has no EOD persist).
# Default to /data (1.7 TB) NOT the main drive (~349 GB): tplogs grow ~5 GB/day and the
# main drive filled with them (housekeeping 2026-06-14, retired logs in
# /data/xvenue_tp_logs_archive/). The RDB learns this path from the TP (.u.L) on subscribe,
# so it replays from here automatically. Override with TPLOG_DIR=. for the old in-repo behaviour.
TPLOG_DIR="${TPLOG_DIR:-/data/xvenue_tplogs_live}"

mkdir -p "$LOG_DIR" "$PID_DIR" "$TPLOG_DIR"

# derive per-venue symbol lists + the canonical engine universe from ROOTS
BINANCE_SYMS=""; COINBASE_SYMS=""; OKX_SYMS=""; CANON_SYMS=""
for r in $ROOTS; do
  BINANCE_SYMS="$BINANCE_SYMS ${r}USDT"
  COINBASE_SYMS="$COINBASE_SYMS ${r}-USD"
  OKX_SYMS="$OKX_SYMS ${r}-USDT"
  CANON_SYMS="$CANON_SYMS $r"
done

OTHER_PID_DIR="${OTHER_PID_DIR:-/tmp/mvf-live/pids}"          # sibling stack (start_live.sh)

# ---- topology banner ---------------------------------------------------------
cat <<'EOF'
=============================================================================
  TOPOLOGY: CROSS-VENUE — Binance spot vs Coinbase spot (pairMode crossVenue)
  For the SPOT/PERP single-venue Binance stack use ./start_live.sh instead.
=============================================================================
EOF

# ---- helpers ----------------------------------------------------------------
die () { echo "ERROR: $*" >&2; exit 1; }

# refuse to start if the OTHER topology's stack is (even partially) alive —
# the two stacks share the TP/RDB/bridge ports, and a leftover feed-handler
# from the other stack would publish into this one's tickerplant
other_stack_check () {
  local other_pids="$1" other_stop="$2" pf p alive=""
  [ -d "$other_pids" ] || return 0
  for pf in "$other_pids"/*.pid; do
    [ -f "$pf" ] || continue
    p="$(cat "$pf")"
    kill -0 "$p" 2>/dev/null && alive="$alive $(basename "$pf" .pid)=$p"
  done
  [ -z "$alive" ] || die "the OTHER stack has live processes:$alive — stop it first: $other_stop"
}

wait_port () {
  local port="$1" tries="${2:-40}" i
  for ((i=0; i<tries; i++)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 0.25
  done
  return 1
}

proc_alive () { local pf="$PID_DIR/$1.pid"; [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; }

launch () {
  local name="$1" dir="$2"; shift 2
  proc_alive "$name" && die "$name already running (pid $(cat "$PID_DIR/$name.pid")) — run ./stop_xvenue.sh first"
  # setsid -> own session, detached from the controlling terminal, so a Ctrl+C in
  # the foreground (e.g. exiting the --dashboard TUI) never reaches the stack. The
  # inner shell writes its OWN $$ to the pidfile (survives exec, robust whether or
  # not setsid forks), then exec's the service — pidfile holds the real service PID.
  setsid bash -c 'echo $$ > "$1"; cd "$2" || exit 1; shift 2; exec "$@"' \
    bash "$PID_DIR/$name.pid" "$dir" "$@" </dev/null >"$LOG_DIR/$name.log" 2>&1 &
  local i; for ((i=0; i<20; i++)); do [ -s "$PID_DIR/$name.pid" ] && break; sleep 0.05; done
  echo "  -> $name started (pid $(cat "$PID_DIR/$name.pid" 2>/dev/null))  log: $LOG_DIR/$name.log"
}

# ---- preflight --------------------------------------------------------------
echo "== preflight =="
other_stack_check "$OTHER_PID_DIR" "$MVF_DIR/scripts/stop_live.sh"
[ -x "$Q" ]  || die "q binary not found/executable: $Q"
[ -x "$PY" ] || die "venv python not found/executable: $PY"
[ -d "$INGEST_DIR" ] || die "ingest dir not found: $INGEST_DIR"
{ [ -d "$TPLOG_DIR" ] && [ -w "$TPLOG_DIR" ]; } || die "tplog dir not writable: $TPLOG_DIR"
[ -f "$INGEST_DIR/tick.q" ]            || die "missing $INGEST_DIR/tick.q"
[ -f "$INGEST_DIR/coinbaseFH.py" ]     || die "missing $INGEST_DIR/coinbaseFH.py"
[ -f "$INGEST_DIR/okxFH.py" ]          || die "missing $INGEST_DIR/okxFH.py"
[ -f "$MVF_DIR/proc/live_xvenue.q" ]   || die "missing $MVF_DIR/proc/live_xvenue.q"
for p in "$TP_PORT" "$RDB_PORT" "$BRIDGE_PORT" "$RECAL_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then exec 3>&- 3<&-; die "port $p already in use — stack already running? (./stop_xvenue.sh, or start_live.sh is up)"; fi
done
echo "  q=$Q  py=$PY"
echo "  roots:    $CANON_SYMS"
echo "  moveMult: ${MOVE_MULT}x (bridge move-bar multiplier on top of vol-adaptive base)"
echo "  binance:  $BINANCE_SYMS"
echo "  coinbase: $COINBASE_SYMS"
echo "  okx:      $OKX_SYMS"
echo "  run dir:  $RUN_DIR"
echo "  tplog:    $TPLOG_DIR  (durable raw-tick record)"

# ---- 1) Tickerplant ---------------------------------------------------------
echo "== 1/6  Tickerplant (:$TP_PORT) =="
launch tp "$INGEST_DIR" "$Q" tick.q sym "$TPLOG_DIR" -p "$TP_PORT"
wait_port "$TP_PORT" || die "TP did not open port $TP_PORT — see $LOG_DIR/tp.log"
echo "  TP listening."

# ---- 2) RDB -----------------------------------------------------------------
echo "== 2/6  RDB (:$RDB_PORT) =="
launch rdb "$INGEST_DIR" "$Q" tick/r.q ":$TP_PORT" -p "$RDB_PORT"
wait_port "$RDB_PORT" || die "RDB did not open port $RDB_PORT — see $LOG_DIR/rdb.log"
echo "  RDB listening + subscribed."

# ---- 3) cross-venue bridge --------------------------------------------------
echo "== 3/6  Cross-venue bridge (:$BRIDGE_PORT query port, pairMode crossVenue) =="
# shellcheck disable=SC2086
launch bridge "$MVF_DIR" "$Q" proc/live_xvenue.q ":$TP_PORT" -symbols $CANON_SYMS -moveMult "$MOVE_MULT" -p "$BRIDGE_PORT"
wait_port "$BRIDGE_PORT" || die "bridge did not open port $BRIDGE_PORT — see $LOG_DIR/bridge.log"
# q opens -p BEFORE loading the script, so an open port does NOT mean the bridge
# finished loading + connected to the TP. Recheck liveness after it settles (it now
# retries the TP connect, so this just catches a hard load error).
sleep 3
proc_alive bridge || die "bridge exited after opening port (TP connect / load error?) — see $LOG_DIR/bridge.log"
echo "  bridge listening + subscribed."

# ---- 4) Binance SPOT feed-handler -------------------------------------------
echo "== 4/7  Binance SPOT feed-handler =="
# shellcheck disable=SC2086
launch spot_fh "$INGEST_DIR" "$PY" binanceFH_REST.py --symbols $BINANCE_SYMS --tp-port "$TP_PORT" --rest-snap-interval-s "$REST_SNAP_S"
sleep 4
proc_alive spot_fh || die "spot FH exited early — see $LOG_DIR/spot_fh.log"
echo "  spot FH connecting to Binance."

# ---- 5) Coinbase feed-handler -----------------------------------------------
echo "== 5/7  Coinbase feed-handler =="
# shellcheck disable=SC2086
launch coinbase_fh "$INGEST_DIR" "$PY" coinbaseFH.py --symbols $COINBASE_SYMS --tp-port "$TP_PORT" --channel "$CB_CHANNEL"
sleep 4
proc_alive coinbase_fh || die "coinbase FH exited early — see $LOG_DIR/coinbase_fh.log"
echo "  coinbase FH connecting to Coinbase."

# ---- 6) OKX feed-handler ----------------------------------------------------
# Offshore/Asia-centric leg: tests whether the OQ-6 thin-US-participation lead is a
# geography/flow effect vs Binance-specific (docs/OQ6_POOLED_BYHOUR_FINDINGS.md §5).
echo "== 6/7  OKX feed-handler =="
# shellcheck disable=SC2086
launch okx_fh "$INGEST_DIR" "$PY" okxFH.py --symbols $OKX_SYMS --tp-port "$TP_PORT" --channel "$OKX_CHANNEL"
sleep 4
proc_alive okx_fh || die "okx FH exited early — see $LOG_DIR/okx_fh.log"
echo "  okx FH connecting to OKX."

# ---- 7) Binance UM-perp feed-handler (trades + mark + depth) -----------------
# Perp aggTrades/markPrice carry real-time exchange stamps — captured for the
# trade-clock lead-lag track (Part B) into perp tables ptrades / pmark / pbupd / pbsnap.
# --no-bookticker: perp must NOT write the `bookticker` table — the crossVenue spot
# engine (.u.sub `bookticker`) ingests every row unfiltered, so perp BBO would pollute
# the spot lead-lag. Streams ride Binance's ROUTED WS endpoints (/market aggTrade+markPrice,
# /public depth); the legacy unrouted URL was decommissioned 2026-04-23 and silently
# stopped serving /market streams (aggTrade/markPrice went dark while depth survived).
echo "== 7/7  Binance PERP feed-handler (trades + mark + depth; no bookticker) =="
# shellcheck disable=SC2086
launch perp_fh "$INGEST_DIR" "$PY" binanceUMPerpFH.py --symbols $BINANCE_SYMS --tp-port "$TP_PORT" --no-bookticker
sleep 4
proc_alive perp_fh || die "perp FH exited early — see $LOG_DIR/perp_fh.log"
echo "  perp FH connecting to Binance futures (routed /market + /public)."

# ---- 8) recalibration sidecar (q-native hourly vol-adaptive bars) -----------
# Dedicated process (NOT in the bridge — the multi-minute solve would block live tick
# processing). Niced: a heavy q job beside the live stack (an un-niced one stalled the
# bridge socket before). Re-solves each asset's winreset bar for the target rate and
# hot-pushes into the bridge + rewrites config/move_cal.q. Recalibrates on start, then hourly.
echo "== 8/8  Recal sidecar (:$RECAL_PORT, hourly winreset recalibration) =="
# shellcheck disable=SC2086
launch recal "$MVF_DIR" nice -n 19 ionice -c3 "$Q" proc/recal.q -p "$RECAL_PORT" -intervalMs "$RECAL_INTERVAL_MS"
wait_port "$RECAL_PORT" || die "recal sidecar did not open port $RECAL_PORT — see $LOG_DIR/recal.log"
echo "  recal sidecar up (recalibrates on start, then every $((RECAL_INTERVAL_MS/60000)) min)."

# ---- done -------------------------------------------------------------------
cat <<EOF

== cross-venue stack up ==
  PIDs:    $(for n in tp rdb bridge spot_fh coinbase_fh okx_fh perp_fh recal; do printf '%s=%s ' "$n" "$(cat "$PID_DIR/$n.pid")"; done)
  Logs:    tail -f $LOG_DIR/{tp,rdb,bridge,spot_fh,coinbase_fh,okx_fh,perp_fh,recal}.log

  Inspect the live engine (query the bridge on :$BRIDGE_PORT), e.g.:
    $Q -q <<'Q'
      h:hopen \`::$BRIDGE_PORT;
      show h"0!select ticks:count i by sym,venue from quote_norm";
      show h"0!select events:count i, avgLagMs:avg lagMs by sym,leadVenue,followVenue,direction from leadlag_events";
      show h"0!select sym,venue,leadlagScore,nEvents,pValue from leadership_session";
      hclose h; exit 0
Q

  Dashboard:  $PY $MVF_DIR/scripts/dashboard.py --port $BRIDGE_PORT
              (or re-run this script with --dashboard to launch it inline)

  Stop everything:  $MVF_DIR/scripts/stop_xvenue.sh
EOF

# ---- optional: launch the live monitor TUI in the foreground -----------------
# The stack is session-detached (setsid in launch()), so Ctrl+C here exits ONLY the
# dashboard — every stack process keeps running. exec replaces this shell with the TUI.
if [ "$RUN_DASHBOARD" = 1 ]; then
  echo
  echo "== launching dashboard TUI (Ctrl+C exits the dashboard; the stack keeps running) =="
  [ -x "$PY" ] || die "venv python not found for dashboard: $PY"
  exec "$PY" "$MVF_DIR/scripts/dashboard.py" --port "$BRIDGE_PORT" --refresh 10
fi
