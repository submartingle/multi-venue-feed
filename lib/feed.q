// lib/feed.q — feed ingest + move detection (.feed.*)
// Single ingest entry point (C7): .feed.upd[t;data] normalizes incoming quotes,
// maintains per-leg state, and detects qualifying mid moves that drive the
// lead/lag engine. Works whether driven by an upstream tickerplant or by
// sim/feed_sim.q. Scoring / regime / alerts run later off a timer (C8), not here.
//
// Contract: `data` is a table with at least eventTs,recvTs,sym,venue,inst,bid,ask.
// mid is recomputed defensively; any upstream mid is ignored (C11).
// A "leg" is the (sym;venue;inst) tuple (A1).

// Per-leg state: each leg's last ACCEPTED mid + timestamps, plus the cumstale
// accumulator (base = baseline value at the last fire/reset, baseTs = its window
// anchor in ns). Keyed on the leg tuple and carried across .feed.upd calls, so move
// detection is continuous — this is the seed for the per-leg detectors below.
.feed.leg:([sym:`symbol$(); venue:`symbol$(); inst:`symbol$()]
  mid:`float$(); recvTs:`timestamp$(); eventTs:`timestamp$();
  base:`float$(); baseTs:`long$());

// winreset (B8) per-leg carried state: the monotonic min/max deques (time;value) of the trailing
// window, kept separate from .feed.leg so cumstale/tickwise are untouched. Each cell is a vector;
// a leg absent here is a cold start (empty deques). Driven by the shared .move.wrvStep.
.feed.wstate:([sym:`symbol$(); venue:`symbol$(); inst:`symbol$()]
  mnT:(); mnV:(); mxT:(); mxV:());

// Clear leg state (test helper / cold restart).
.feed.reset:{[] .feed.leg:0#.feed.leg; .feed.wstate:0#.feed.wstate;};

// --- detectors: each takes the validated, recvTs-sorted batch `d` and returns
//     (moves ; legUpd) — legUpd is a leg-keyed table of the 5 carried state cols. ----

// tickwise (legacy): one adjacent-tick return >= thr. Vectorised; brand-new leg's
// first tick has null prevMid so it never qualifies. base/baseTs are advanced too
// (= last tick) so they stay warm if the mode is later switched to cumstale.
.feed.detectTickwise:{[d]
  d:d lj `sym`venue`inst xkey select sym,venue,inst,seedMid:mid from 0!.feed.leg;
  d:update prevMid:prev mid by sym,venue,inst from d;     // first row of a leg -> null
  d:update prevMid:seedMid^prevMid from d;                // ...seeded with carried last mid
  d:update moveBps:10000*(mid-prevMid)%prevMid from d;    // null prevMid -> null -> never fires
  d:update thr:.cfg.threshold each sym from d;            // per-sym bar (override else default)
  moves:select sym,venue,inst,recvTs,eventTs,mid,moveBps,direction:?[moveBps>0;`up;`down]
    from d where abs[moveBps]>=thr;
  adv:select mid:last mid, recvTs:last recvTs, eventTs:last eventTs,
        base:last mid, baseTs:`long$last eventTs by sym,venue,inst from d;
  (moves; adv)};

// Per-leg streaming cumstale: scan this leg's new ticks (chronological) seeded from the
// carried (base, baseTs), returning the fired events + the advanced carry. Reuses the tested
// .move.staleStep so live detection matches sim/movethr_calibrate.q exactly.
.feed.csLeg:{[delta;winNs;row]
  v:row`v; clk:row`clk;                                   // mid series + window clock (ns) for this leg
  // carried base/anchor, but fall back to this batch's first tick when the leg is new (null carry).
  // NB fill is `default^value`: (first v)^b0 keeps a non-null carried base, else seeds with first v.
  sb:(first v)^row`b0;
  st:(first clk)^row`t0;
  r:(.move.staleStep[delta; row`thr; winNs])\[(sb; st; 0; 0n); flip(clk; v)];
  d:`long$r[;2];                                          // per-tick direction (0 = no fire)
  f:where d<>0;
  `sym`venue`inst`recvTs`eventTs`mid`moveBps`direction`base`baseTs!
    (row`sym; row`venue; row`inst; row[`rTs] f; row[`eTs] f; v f;
     10000*r[f;3]; ?[0<d f;`up;`down]; last r[;0]; last r[;1])};

// cumstale: cumulative cross within moveWinMs (subsumes tickwise, granularity-bias-free).
// The window runs on eventTs (transport-immune, monotonic per leg) regardless of lagClock,
// which governs lead MEASUREMENT, not the move DEFINITION.
.feed.detectCumstale:{[d]
  winNs:`long$1e6*.cfg.moveWinMs;
  delta:.move.d .cfg.moveBasis;
  // thr fraction: .cfg.threshold is in bps; .move.staleStep compares a raw return (fraction).
  d:update thr:1e-4*.cfg.threshold each sym from d;
  d:d lj `sym`venue`inst xkey select sym,venue,inst,base,baseTs from 0!.feed.leg;  // carried seed
  g:0!select clk:`long$eventTs, v:mid, rTs:recvTs, eTs:eventTs,
       thr:first thr, b0:first base, t0:first baseTs by sym,venue,inst from d;
  res:.feed.csLeg[delta;winNs] each g;                    // one row (dict) per leg
  moves:`recvTs xasc ungroup select sym,venue,inst,recvTs,eventTs,mid,moveBps,direction from res;
  carry:`sym`venue`inst xkey select sym,venue,inst,base,baseTs from res;
  adv:select mid:last mid, recvTs:last recvTs, eventTs:last eventTs by sym,venue,inst from d;
  (moves; adv lj carry)};                                 // advance last-tick fields + cumstale carry

// Per-leg streaming winreset (B8): fold this leg's new ticks through the shared .move.wrvStep,
// seeded from the carried min/max deques (empty => cold start). Window clock = eventTs (transport-
// immune, as for cumstale). Returns the fired moves AND the advanced deques (carried in .feed.wstate).
.feed.wrLeg:{[delta;winNs;row]
  clk:row`clk; v:row`v;
  mnV0:row`mnV0;                                          // cold start if no real carried deque (lj-null/empty)
  seed:$[(0=count mnV0) or not 9h=type mnV0; .move.wrSeed; (row`mnT0; mnV0; row`mxT0; row`mxV0; 0; 0n)];
  r:(.move.wrvStep[delta; row`thr; winNs])\[seed; flip(clk; v)];
  d:`long$r[;4]; f:where d<>0; lastS:last r;              // fires + final deque state
  `sym`venue`inst`recvTs`eventTs`mid`moveBps`direction`mnT`mnV`mxT`mxV!
    (row`sym; row`venue; row`inst; row[`rTs] f; row[`eTs] f; v f;
     10000*r[f;5]; ?[0<d f;`up;`down]; lastS 0; lastS 1; lastS 2; lastS 3)};

// winreset: a >=thr swing from the trailing-window extreme, reset on fire (B8, supersedes cumstale;
// resolves OQ-10 — fires AT the move's completion tick, no straddle loss). thr is per-sym in bps;
// .move.wrvStep compares a log return (fraction), so bps->fraction here.
.feed.detectWinreset:{[d]
  winNs:`long$1e6*.cfg.moveWinMs;
  delta:.move.d .cfg.moveBasis;
  d:update thr:1e-4*.cfg.threshold each sym from d;
  d:d lj `sym`venue`inst xkey select sym,venue,inst,mnT,mnV,mxT,mxV from 0!.feed.wstate;   // carried deques
  g:0!select clk:`long$eventTs, v:mid, rTs:recvTs, eTs:eventTs, thr:first thr,
       mnT0:first mnT, mnV0:first mnV, mxT0:first mxT, mxV0:first mxV by sym,venue,inst from d;
  res:.feed.wrLeg[delta;winNs] each g;                    // one row (dict) per leg
  moves:`recvTs xasc ungroup select sym,venue,inst,recvTs,eventTs,mid,moveBps,direction from res;
  `.feed.wstate upsert `sym`venue`inst xkey select sym,venue,inst,mnT,mnV,mxT,mxV from res;  // advance deques
  adv:select mid:last mid, recvTs:last recvTs, eventTs:last eventTs,
        base:last mid, baseTs:`long$last eventTs by sym,venue,inst from d;
  (moves; adv)};                                          // .feed.leg advance (base/baseTs kept warm)

// Ingest a batch of quotes. Validity filter + persist are vectorised; move
// detection dispatches on .cfg.moveMode to one of the .feed.detect* detectors,
// each seeded with carried per-leg state so detection is continuous across batches.
.feed.upd:{[t;data]
  if[not count data; :()];
  // --- C11: defensive mid + validity filter -------------------------------
  // Recompute mid; keep only two-sided, non-crossed, non-locked books. The mask is
  // computed explicitly (not in a qsql where) so rejected ticks still attribute to
  // their leg in the feed-health counts (B2) before being dropped.
  d:update mid:(bid+ask)%2 from data;
  valid:(not null d`bid) and (not null d`ask) and d[`bid]<d`ask;
  .health.record[d;valid];
  d:d where valid;
  if[n:count[data]-count d; .log.warn "feed.upd skipped ",string[n]," invalid tick(s)"];
  if[not count d; :()];
  d:`recvTs xasc d;                          // chronological per-leg sequences
  // --- persist accepted ticks (g#sym maintained on insert) ----------------
  t insert select eventTs,recvTs,sym,venue,inst,bid,ask,mid from d;
  // --- clock-skew calibration sample (Item 5): every accepted tick feeds the
  // per-leg recvTs-eventTs floor estimate; offsets recomputed on the timer ----
  .skew.record d;
  // --- move detection (mode-switchable: tickwise legacy / cumstale calibrated) ---
  // Both detectors advance the per-leg carried state and return the qualifying moves.
  md:$[.cfg.moveMode=`winreset; .feed.detectWinreset d;
       .cfg.moveMode=`cumstale; .feed.detectCumstale d;
       .feed.detectTickwise d];
  `.feed.leg upsert md 1;                     // advance carried leg state (all 5 cols)
  moves:md 0;
  if[count moves;
    .comove.record moves;                    // un-cooled history for base-rate score (E15)
    .leadlag.onMove moves];                  // cooled operational event stream
  };
