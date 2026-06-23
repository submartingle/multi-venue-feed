// lib/monitor.q — system dashboard metrics collector (.monitor.*)
// Provides .monitor.snap[]: a single IPC-callable function that returns a dict
// of live system stats (table counts, score summaries, feed health, alerts).
// Loaded by the live bridge processes; called by the Python dashboard via PyKX.
// The query is sub-millisecond (counts + meta only) so it does not block .z.ts.

// Bridge (backend q process) start time — the dashboard shows THIS as system uptime,
// not the dashboard's own. Derived from the OS so it is correct regardless of when this
// library was loaded (incl. an IPC hot-reload into a running bridge). `ps -o etimes=`
// gives this PID's elapsed seconds since start; start = now - elapsed. (read0 can't be
// used on /proc — virtual files report size 0, so q reads nothing.)
//   .z.i = this q process's PID; ~1s resolution, fine for an uptime display.
.monitor.procStartTs:{[]
  .z.p - 1000000000j * "J"$ first system "ps -o etimes= -p ",string .z.i};

// Anchor at load to the TRUE process start; fall back to null on a non-Linux/ps failure
// (then .monitor.start[] sets ~now, and uptimeMs floors on the earliest .feed.leg tick).
.monitor.startTs:@[.monitor.procStartTs; ::; {[e] 0Np}];
.monitor.start:{[] if[null .monitor.startTs; .monitor.startTs:.z.p];};

.monitor.uptimeMs:{
  $[not null .monitor.startTs;  (`long$.z.p - `long$.monitor.startTs) div 1000000;
    count .feed.leg;            (`long$.z.p - `long$exec min recvTs from .feed.leg) div 1000000;
    0]};

// Count a global table by name, or null if it isn't defined in this process
// (the trade-clock tables live only on the xvenue three-scorer bridge).
.monitor.cnt:{[t] @[{count get x}; t; 0N]};

// 30-minute score (wider window for quiet markets). Uses the same public
// .comove.calcOver + .score.fromPairs machinery — stateless, recomputes cheaply.
// regimeBucket is a CURRENT-state per-leg label (recomputed each live timer tick into
// .regime.state); .score.fromPairs leaves it null, so stamp the current regime on here
// the same way .regime.calc stamps leadership_score (lj overwrites null for matched legs).
.monitor.win30:0D00:30:00.000000000;
.monitor.score30:{[]
  pairs:.comove.calcOver[.z.p - .monitor.win30; .z.p];
  s:.score.fromPairs[pairs; .z.p];
  rs:@[value; `.regime.state;
    ([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$(); regimeBucket:`symbol$())];
  s:s lj `sym`venue`inst xkey select sym,venue,inst,regimeBucket from rs;
  `sym`venue`inst`leadlagScore`leadlagScoreK`nEvents`pValue`regimeBucket xcols s};

// Return a flat dict of all dashboard metrics. The Python dashboard calls this
// via IPC (h".monitor.snap[]") and renders the result. Keep it cheap: counts,
// first N rows of key tables, no heavy aggregations.
.monitor.snap:{[]
  d:`startTs`now`uptimeMs!(.monitor.startTs; .z.p; .monitor.uptimeMs[]);
  // --- data flow counts -----------------------------------------------------
  d[`quoteNorm]    :count quote_norm;
  d[`comoveHist]   :count .comove.hist;
  d[`leadlagEvents]:count leadlag_events;
  d[`alerts]       :count alerts;
  d[`feedLegs]     :count .feed.leg;
  d[`leadershipPairs]:.monitor.cnt`leadership_pairs;   // directed obs/expected/excess matrix
  d[`leadershipScore]:.monitor.cnt`leadership_score;   // trailing-window per-leg score
  d[`feedHealthRows] :count feed_health;
  // three-scorer pipeline counts (trade-clock tables exist only on the xvenue bridge;
  // .monitor.cnt returns null where a table is absent so the dashboard shows it as "—")
  d[`sessionQuote] :.monitor.cnt`leadership_session;
  d[`trFlowBars]   :.monitor.cnt`.tr.histFlow;
  d[`trPriceMoves] :.monitor.cnt`.tr.histPrice;
  d[`sessionTflow] :.monitor.cnt`leadership_session_tflow;
  d[`sessionTprice]:.monitor.cnt`leadership_session_tprice;
  // --- leadership_score (trailing 1-min window, top by abs score) -----------
  d[`score]:`sym`venue`inst`leadlagScore`leadlagScoreK`nEvents`pValue`regimeBucket
    xcols 0!select from leadership_score where not null leadlagScore;
  // --- score30 (trailing 30-min window, recomputed each snap) ---------------
  d[`score30]:`sym`venue`inst`leadlagScore`leadlagScoreK`nEvents`pValue`regimeBucket
    xcols .monitor.score30[];
  // --- leadership_session (cumulative, top by abs score) ---------------------
  d[`session]:`sym`venue`inst`leadlagScore`nEvents`pValue
    xcols 0!select from leadership_session where not null leadlagScore;
  // --- feed_health snapshot --------------------------------------------------
  d[`feedHealth]:`sym`venue`inst`lastTickAgeMs`ticksPerMin`invalidPct`oneWayFloorMs`fallbackPct
    xcols 0!feed_health;
  // --- recent alerts (last 20) -----------------------------------------------
  d[`recentAlerts]:20 sublist `ts xdesc
    `ts`sym`venue`inst`alertType`msg xcols 0!alerts;
  // --- per-leg last-tick freshness -------------------------------------------
  d[`legFresh]:0!select sym,venue,inst,
    ageMs:1e-6*`long$.z.p-recvTs, mid from .feed.leg;
  // --- memory ----------------------------------------------------------------
  d[`memory]:.Q.w[];
  d};