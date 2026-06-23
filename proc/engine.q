// proc/engine.q — live process orchestration (C7/C8). Run from the project root.
// Loads everything, then runs the analytics timer: recompute leadership_score +
// regime each tick off the hot path (C8), and snapshot the rolling summary to a
// durable file every .cfg.snapshotMins. Ingest is via .feed.upd (driven by a
// tickerplant subscription or a replay harness) — feed -> leadlag is wired by the
// direct .leadlag.onMove call, with load order guaranteed here.
system "l proc/load.q";

.engine.snapEvery:0D00:01:00.000000000 * @[value; `.cfg.snapshotMins; 60];
.engine.lastSnap:0Np;

// Timer handler: rolling recompute + periodic snapshot. Window end is wall-clock
// .z.p (live); offline replay calls .score.calc / .regime.calc itself instead.
.z.ts:{
  curT:.z.p;
  .skew.calc[];                            // refresh per-leg clock-skew offsets (Item 5)
  .score.calc curT;
  .score.calcSession curT;                 // cumulative session score (Item 3)
  .regime.calc curT;
  .alert.calc  curT;
  .health.calc curT;                       // feed-health snapshot + edge-triggered alerts (B2)
  if[curT > .engine.lastSnap + .engine.snapEvery;
     .log.snapshot[`leadership_score; leadership_score];
     .log.snapshot[`leadership_session; leadership_session];
     .engine.lastSnap:curT];
  };

// Start (or stop) the analytics timer.
.engine.start:{.engine.lastSnap:.z.p; system "t ", string @[value; `.cfg.timerMs; 1000i];};
.engine.stop :{system "t 0";};

.engine.start[];
.log.info "engine started: timer ",(string @[value;`.cfg.timerMs;1000i]),"ms, snapshot every ",(string @[value;`.cfg.snapshotMins;60]),"min";
