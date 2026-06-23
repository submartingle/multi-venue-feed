// proc/live.q — LIVE ingest bridge (C7/C8). Run from the project root:
//   q proc/live.q :5010 -symbols BTCUSDT ETHUSDT SOLUSDT DOGEUSDT XRPUSDT
//
// Pipeline:  Binance FH -> TP(`bookticker) -> [this bridge] -> .feed.upd
//            -> leadlag / score / regime (the existing engine, unchanged)
//
// bookTicker already carries best bid/ask per leg (the FH tags venue+inst), so there
// is NO q-side order-book reconstruction: each TP row maps straight to a quote_norm
// tick. Single venue (BINANCE), spot vs perp -> pairMode crossInst (the same mode the
// hdb_replay backtest validated). Ingest is on the hot path via .feed.upd; score and
// regime run off the hot path on the .z.ts timer.

// --- load engine (relative paths -> must run from project root) -------------
system "l proc/load.q";

// --- live config (set BEFORE re-resolving leadlag's load-time bindings) ------
.cfg.pairMode:`crossInst;              // single venue, spot vs perp
.cfg.moveThresholdDefault:1f;          // bps — crypto majors run sub-2bp spreads (tune per-sym later)
system "l lib/leadlag.q";              // re-resolve pairMode/followWin at load time

// --- params / CLI -----------------------------------------------------------
.live.opt :.Q.opt .z.x;
// first bare (non-option) arg is the tickerplant host:port, e.g. ":5010"
// guard order matters right-to-left: count runs before the in-test touches first .z.x;
// the old form compared an atom to 1#first (a list) under ~, which never matches
.live.tpArg:$[(not "-" in first .z.x) and count .z.x; first .z.x; ":5010"];
.live.tp  :`$":",.live.tpArg;          // hopen target, e.g. `::5010
.live.syms:$[`symbols in key .live.opt; `$.live.opt`symbols;
  `BTCUSDT`ETHUSDT`SOLUSDT`DOGEUSDT`XRPUSDT];
.live.retain:0D00:10:00;               // quote_norm eviction horizon (>= score/regime window)

// --- bookticker transport schema (mirrors crypto/tick/sym.q) ----------------
bookticker:([] exch_us:`long$(); recv_us:`long$(); sym:`g#`symbol$();
  venue:`g#`symbol$(); inst:`symbol$(); bid:`float$(); bidsz:`float$(); ask:`float$(); asksz:`float$());   // g#venue: O(1) `where venue=v`

.live.us2ts:{1970.01.01D0+1000*x};     // epoch-microseconds (long) -> kdb timestamp

// --- ingest: TP async callback (`upd;`bookticker;x) -------------------------
// x is a list of rows (PyKX list-of-tuples; sym/venue/inst already symbols). Map each
// BBO row to a quote_norm tick; .feed.upd recomputes mid, filters invalid/crossed
// books, detects qualifying moves and drives leadlag.
upd:{[t;x]
  bt:$[98h=type x; x; flip cols[`bookticker]!flip x];      // accept table or row-list
  qn:select eventTs:.live.us2ts exch_us, recvTs:.live.us2ts recv_us,
       sym, venue, inst, bid, ask from bt where sym in .live.syms;  // scope to our universe
  if[count qn; .feed.upd[`quote_norm; qn]];
  };

// --- analytics timer (C8): score + regime off the hot path, snapshot + evict -
.live.snapEvery:0D00:01:00 * @[value;`.cfg.snapshotMins;60];
.live.lastSnap:0Np;
.z.ts:{
  curT:.z.p;
  .skew.calc[];                            // refresh per-leg clock-skew offsets (Item 5)
  .score.calc  curT;
  .score.calcSession curT;                 // cumulative session score (Item 3)
  .regime.calc curT;
  .alert.calc  curT;
  .health.calc curT;                       // feed-health snapshot + edge-triggered alerts (B2)
  // the raw quote_norm table would grow unbounded under a live feed (regime/score
  // only read a trailing window) — evict beyond the retain horizon.
  delete from `quote_norm where recvTs < curT - .live.retain;
  if[curT > .live.lastSnap + .live.snapEvery;
     .log.snapshot[`leadership_score; leadership_score];
     .log.snapshot[`leadership_session; leadership_session];
     .live.lastSnap:curT];
  };

// EOD message from the tickerplant — no-op (bridge is in-memory, no HDB roll).
.u.eod:{[d] .log.info "tickerplant EOD ",string d;};

// --- connect + subscribe (LIVE from now; no log replay) ---------------------
.live.h:hopen (.live.tp; 5000);
// subscribe to ALL bookticker syms (`): this TP's pub only forwards raw row-lists on the
// all-syms branch — a specific sym filter makes it `select` on x (a row-list, not a table)
// and signals 'type. We scope to .live.syms inside `upd` instead.
.live.h(".u.sub";`bookticker;`);
.live.lastSnap:.z.p;
system "t ", string @[value;`.cfg.timerMs;1000i];
.monitor.start[];                        // record start time for the dashboard
.log.info "live bridge up: TP ",string[.live.tp],"  syms ",(", " sv string .live.syms),
  "  pairMode crossInst  thr ",string[.cfg.moveThresholdDefault],"bps";
