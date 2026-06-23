// proc/recal.q — dedicated hourly recalibration sidecar (q-native; replaces scripts/recal_loop.sh).
//
// Fires .recal.run[apply:1b]: pull the trailing window from the RDB, solve each asset's winreset bar
// for the target rate, hot-push into the live bridge (IPC) + rewrite config/move_cal.q. Runs as its
// OWN process (NOT inside the bridge — the multi-minute solve would block live tick processing) and
// is niced at launch. Opens its own RDB+bridge handles per run, so it tolerates a stack restart.
// Stays alive on its -p port (queryable: .recal.runs / .recal.last / .recal.nextRun).
//   q proc/recal.q -p 5098 [-intervalMs 3600000] [-R 24] [-windowHr 3]
//
// Scheduling is a WALL-CLOCK GATE, not the raw \t period: .z.ts polls every .recal.pollMs and only
// runs when .z.p has reached .recal.nextRun, which is pushed forward one interval per run. This makes
// the cadence exact and immune to \t firing behaviour / long-handler catch-up. Recalibrates on start
// (the RDB already holds the trailing window via tplog replay), then every interval.

system "l proc/load.q";                  // .cfg.canon/symCanon
system "l lib/movedetect.q";
system "l lib/autocal.q";
system "l lib/recal.q";

opt:.Q.opt .z.x;
if[`R        in key opt; .recal.R       :"F"$first opt`R];
if[`windowHr in key opt; .recal.windowHr:"F"$first opt`windowHr];
.recal.intervalMs:$[`intervalMs in key opt; "J"$first opt`intervalMs; 3600000];
.recal.pollMs    :$[`pollMs     in key opt; "J"$first opt`pollMs;       30000];

.recal.intervalNs:`long$1e6*.recal.intervalMs;
.recal.nextRun:.z.p;                     // due immediately -> recalibrate on start, then every interval
.recal.runs:0; .recal.last:();

// poll: run only when the interval has elapsed. (q can't re-enter .z.ts mid-run, so no extra guard
// needed.) Schedule the next run BEFORE running so a long solve doesn't compound the cadence.
.z.ts:{[t]
  if[.z.p < .recal.nextRun; :()];
  .recal.nextRun:.z.p + .recal.intervalNs;
  .recal.last:@[.recal.run; 1b; {[e] -1 "  recal run error: ",e; ()}];
  .recal.runs+:1; };
system "t ",string .recal.pollMs;

-1 "recal sidecar up ",string[.z.p],": interval ",string[`int$.recal.intervalMs%60000],
   "min (poll ",string[`int$.recal.pollMs%1000],"s), R*=",string["j"$.recal.R],"/hr, window ",string["j"$.recal.windowHr],"h.";
