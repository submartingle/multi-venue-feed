// proc/live_xvenue.q — CROSS-VENUE live bridge (Item 4 / G4). Run from project root:
//   q proc/live_xvenue.q :5010 -symbols BTC ETH SOL DOGE XRP
//
// Pipeline:  Binance spot FH + Coinbase FH -> TP(`bookticker) -> [this bridge]
//            -> .feed.upd -> leadlag / score / regime (the existing engine)
//
// Same bridge as proc/live.q, but for a GENUINE cross-venue lead-lag question:
//   - pairMode crossVenue (same inst, different venue: Binance SPOT vs Coinbase SPOT).
//   - the raw venue symbol (BTCUSDT / BTC-USD) is retained on `bookticker`; the engine
//     `sym` is the CANONICAL root (BTC) via .cfg.canon, so the legs pair across venues.
//   - both legs carry a trustworthy exchange clock (Binance spot estimated in the FH
//     per G1; Coinbase native), so lag is measured on eventTs (.cfg.lagClock, G2).
// Kept a SEPARATE process so the working spot/perp crossInst setup (proc/live.q) is
// undisturbed. See DECISIONS.md §G and docs/LIVE_RUN_2026-06-09_FINDINGS.md §8.

// --- load engine (relative paths -> must run from project root) -------------
system "l proc/load.q";
system "l config/trade_cal.q";        // .cfg.tradeBarUsd per-sym imbalance thresholds
system "l lib/imbalance.q";           // .imb.* signed-flow imbalance-bar detector
system "l lib/tprice.q";              // .tprice.* trade-price winreset detector

// --- live config (set BEFORE re-resolving leadlag's load-time bindings) ------
.cfg.pairMode:`crossVenue;             // same inst across venues (Binance vs Coinbase)
.cfg.moveMode:`winreset;               // B8: a >=thr swing from the trailing-window extreme, reset on
                                       // fire — fires AT the move's completion tick (correct lead-lag
                                       // timing), no straddle loss. Bars are winreset-derived (config/
                                       // move_cal.q, vol-adaptive via sim/recal_live.q) — detector MUST match.
.cfg.moveThresholdDefault:1f;          // bps — fallback for syms absent from config/move_cal.q
.cfg.lagClock:`eventTs;                // RAW venue-side stamps — the venue-intrinsic frame for
                                       // "which moved first" (per-venue transport cancels; residual
                                       // = inter-exchange clock diff ~5-10ms). calEventTs is an
                                       // ARRIVAL frame (offset floor = min transport to THIS box,
                                       // 2026-06-10 run: Coinbase-leads location artifact) — valid
                                       // only as an execution-side view, scored offline via
                                       // sim/score_archive.q; never for venue-intrinsic lead.
system "l lib/leadlag.q";              // re-resolve pairMode/followWin/clock at load time
system "l lib/comove.q";               // re-resolve lagClock for the score's directed matrix

// --- params / CLI -----------------------------------------------------------
.live.opt :.Q.opt .z.x;
// guard order matters right-to-left: count runs before the in-test touches first .z.x;
// the old form compared an atom to 1#first (a list) under ~, which never matches
.live.tpArg:$[(not "-" in first .z.x) and count .z.x; first .z.x; ":5010"];
.live.tp  :`$":",.live.tpArg;          // hopen target, e.g. `::5010
// -symbols are CANONICAL roots (the engine universe); raw venue symbols are mapped to
// these by .cfg.canon before filtering.
.live.syms:$[`symbols in key .live.opt; `$.live.opt`symbols; `BTC`ETH`SOL`DOGE`XRP];
// optional global move-threshold multiplier (experiment: raise to keep only larger, more
// significant moves). Rides on top of the recal-set vol-adaptive bars via .cfg.threshold;
// recal still targets R*/hr at the BASE bar, so this keeps only the top fraction. Default 1.
if[`moveMult in key .live.opt; .cfg.moveThresholdMult:"F"$first .live.opt`moveMult];
.live.retain:0D00:10:00;               // quote_norm eviction horizon (>= score/regime window)

// --- bookticker transport schema (mirrors crypto/tick/sym.q) ----------------
bookticker:([] exch_us:`long$(); recv_us:`long$(); sym:`g#`symbol$();
  venue:`g#`symbol$(); inst:`symbol$(); bid:`float$(); bidsz:`float$(); ask:`float$(); asksz:`float$());   // g#venue: O(1) `where venue=v`

.live.us2ts:{1970.01.01D0+1000*x};     // epoch-microseconds (long) -> kdb timestamp

// Local trades schema — column order matches the Python FH tuple; used only for
// flip-based rehydration of TP row-lists (no attributes needed; no queries run here).
trades:([] exch_us:`long$(); trade_us:`long$(); recv_us:`long$(); sym:`symbol$();
  px:`float$(); qty:`float$(); side:`symbol$(); tid:`long$());

// Venue from raw-sym suffix: OKX sends BTC-USDT, Coinbase BTC-USD, Binance BTCUSDT
// (no hyphen). Spot only; perp goes to `ptrades` and is not subscribed here.
// Vectorised: applied to the full sym COLUMN in updTrades; $[] is scalar-only.
.live.venueOf:{[s] ?[s like "*-USDT"; `OKX; ?[s like "*-USD"; `COINBASE; `BINANCE]]};

// --- trade-clock move buffers (same schema as .comove.hist) -------------------
// Written by .live.updTrades (directly, not via .comove.record). Evicted on each
// timer tick and passed straight to .comove.calcSessionOn — each scorer reads its
// own buffer, no .comove.hist swap (an aborted pass can't cross-contaminate).
.tr.histFlow :([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  recvTs:`timestamp$(); eventTs:`timestamp$(); direction:`symbol$());
.tr.histPrice:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  recvTs:`timestamp$(); eventTs:`timestamp$(); direction:`symbol$());
.tr.dbNs :`long$50*1000000;           // 50ms dead-band for trade clock (native stamps)
.tr.retain:.comove.retain;            // same 12h session retention as the quote scorer

// Output tables for the two trade-clock session scores — same shape as leadership_session.
leadership_session_tflow :0#leadership_session;
leadership_session_tprice:0#leadership_session;

// --- ingest: TP async callbacks (`upd;`bookticker;x) and (`upd;`trades;x) ----
// Map each BBO row to a quote_norm tick, CANONICALIZING the raw venue symbol to its
// root, then scope to our canonical universe. .feed.upd recomputes mid, filters
// invalid/crossed books, detects qualifying moves and drives leadlag.
.live.updQuote:{[x]
  bt:$[98h=type x; x; flip cols[`bookticker]!flip x];
  qn:select eventTs:.live.us2ts exch_us, recvTs:.live.us2ts recv_us,
       sym:.cfg.canon sym, venue, inst, bid, ask from bt;
  qn:select from qn where sym in .live.syms;
  if[count qn; .feed.upd[`quote_norm; qn]];
  };

// Normalize spot trade rows and run both trade-clock detectors.
// trade_us is the native per-trade exchange stamp on all three spot venues (clean clock).
// Venue derived from raw-sym suffix (.live.venueOf); inst fixed to `SPOT throughout.
.live.updTrades:{[x]
  tr:$[98h=type x; x; flip cols[`trades]!flip x];
  tr:update root:.cfg.canon sym, venue:.live.venueOf sym,
            tradeTs:.live.us2ts trade_us, rTs:.live.us2ts recv_us from tr;
  tr:select from tr where root in .live.syms;
  if[not count tr; :()];
  // trade-price winreset: price-level-retaining, bounce-robust (lib/tprice.q)
  dp:select sym:root, venue, inst:`SPOT, eventTs:tradeTs, recvTs:rTs, price:px from tr;
  mvp:.tprice.detect dp;
  if[count mvp; `.tr.histPrice insert select sym,venue,inst,recvTs,eventTs,direction from mvp];
  // signed-flow imbalance bars: bounce-immune, price-level-blind (lib/imbalance.q)
  df:select sym:root, venue, inst:`SPOT, eventTs:tradeTs, recvTs:rTs,
            signed:px*qty*?[side=`B;1f;-1f] from tr;
  mvf:.imb.detect df;
  if[count mvf; `.tr.histFlow insert select sym,venue,inst,recvTs,eventTs,direction from mvf];
  };

upd:{[t;x]
  $[t=`bookticker; .live.updQuote x;
    t=`trades;     .live.updTrades x;
    ()]};

// --- analytics timer (C8): three-scorer pass + regime + health + snapshot ----
// Three independent scorers (quote / trade-flow / trade-price) share the comove+score
// machinery. The trade scorers pass their own buffer + dead-band directly to
// .comove.calcSessionOn — .comove.hist / .skew.deadBandNs are never reassigned, so an
// error anywhere in the pass cannot leave the quote state pointing at a trade buffer.
.live.snapEvery:0D00:01:00 * @[value;`.cfg.snapshotMins;60];
.live.lastSnap:0Np;
.z.ts:{
  curT:.z.p;
  .skew.calc[];                                         // refresh clock-skew offsets (Item 5)
  // --- 1. QUOTE scorer (.comove.hist = quote buffer, fed by .feed.upd; dead-band
  //        = .skew.deadBandNs, already refreshed by .skew.calc[]) ---------------
  .score.calc curT;                                     // trailing window -> leadership_score
  `leadership_session set .score.fromPairs[.comove.calcSession curT; curT];
  // --- 2. TRADE-FLOW scorer (imbalance bars, dead-band 50ms) -------------------
  .tr.histFlow:select from .tr.histFlow where recvTs>=curT-.tr.retain;
  `leadership_session_tflow set
    .score.fromPairs[.comove.calcSessionOn[.tr.histFlow; .tr.dbNs; curT]; curT];
  // --- 3. TRADE-PRICE scorer (trade-print winreset, same 50ms dead-band) -------
  .tr.histPrice:select from .tr.histPrice where recvTs>=curT-.tr.retain;
  `leadership_session_tprice set
    .score.fromPairs[.comove.calcSessionOn[.tr.histPrice; .tr.dbNs; curT]; curT];
  // --- regime, health, evict, snapshot -----------------------------------------
  .regime.calc curT;
  .alert.calc  curT;
  .health.calc curT;
  delete from `quote_norm where recvTs < curT - .live.retain;
  if[curT > .live.lastSnap + .live.snapEvery;
     .log.snapshot[`leadership_score;           leadership_score];
     .log.snapshot[`leadership_session;         leadership_session];
     .log.snapshot[`leadership_session_tflow;   leadership_session_tflow];
     .log.snapshot[`leadership_session_tprice;  leadership_session_tprice];
     .live.lastSnap:curT];
  };

// EOD message from the tickerplant — no-op (bridge is in-memory, no HDB roll).
.u.eod:{[d] .log.info "tickerplant EOD ",string d;};

// --- connect + subscribe (LIVE from now; no log replay) ---------------------
// Retry the TP connect with backoff: at stack startup the TP may have opened its
// port but not yet be servicing the IPC handshake (a single-shot hopen then 'timeout's
// and — with stdin /dev/null — the bridge q would exit, leaving no :5099). Retry so a
// startup race or a TP restart self-heals.
.live.connect:{[tp]
  i:0;
  while[i<.live.connTries;
    h:@[{hopen (x;2000)}; tp; {[e] 0Ni}];
    if[not null h; :h];
    .log.warn "TP ",string[tp]," not ready (try ",string[i+1],"/",string[.live.connTries],"); retrying in 1s";
    system "sleep 1"; i+:1];
  '"could not connect to TP ",string[tp]," after ",string[.live.connTries]," tries"};
.live.connTries:30;
.live.h:.live.connect .live.tp;
// subscribe to ALL bookticker syms (`): the TP pub only forwards raw row-lists on the
// all-syms branch; we scope (after canonicalization) inside `upd`.
.live.h(".u.sub";`bookticker;`);
.live.h(".u.sub";`trades;`);          // trade prints -> .live.updTrades -> .tr.hist*
.live.lastSnap:.z.p;
system "t ", string @[value;`.cfg.timerMs;1000i];
.monitor.start[];                        // record start time for the dashboard
.log.info "xvenue bridge up: TP ",string[.live.tp],"  syms ",(", " sv string .live.syms),
  "  pairMode crossVenue  thr ",string[.cfg.moveThresholdDefault],"bps";
