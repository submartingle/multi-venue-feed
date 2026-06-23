// lib/autocal.q — vol-adaptive move-threshold selection by event-rate targeting (.autocal.*).
//
// A "major move" is defined by how OFTEN it should fire, not by a fixed bps: the bar is whatever
// delivers the target rate on RECENT data, so it tracks volatility — rising in hot regimes,
// falling in quiet ones (a static bps bar cannot: live BTC ran 5x its calm-day calibrated rate).
// Reuses the tested cumstale detector (.move.detect); load lib/movedetect.q first.
//
// Used by BOTH the offline calibrator (sim/movethr_calibrate.q) and the live recalibration driver
// (sim/recal_live.q) — they differ only in data source (HDB vs RDB), the solver is identical.

// winreset detector cfg for a candidate bps threshold (bps -> fraction; .move.wrvStep compares a return).
.autocal.cfg:{[basis;winNs;thrBps] `mode`thr`basis`winNs!(`winreset; thrBps%10000; basis; winNs)};

// the per-asset UNIT / floor = max(noise sigma, median spread), in bps. The bar is never set below
// this — below it a "move" isn't resolvable above the tick/noise grid (same rule as the static calibrator).
.autocal.unit:{[basis;v;medSprBps] medSprBps | 10000*dev .move.d[basis][-1_v; 1_v]};

// detected moves-per-hour on a series (t = ns clock, v = mid) at a candidate bps threshold.
.autocal.rate:{[basis;winNs;t;v;spanHr;thrBps]
  (count (.move.detect[.autocal.cfg[basis;winNs;thrBps]; t; v])`idx) % spanHr};

// Solve for the bps threshold whose detected rate ~= targetRate (events/hr), floored at floorBps.
// rate(thr) is monotone-decreasing in thr, so:
//   - if even the floor already fires at/below the target (quiet regime), return the floor;
//   - else bracket an upper bound (rate < target) and bisect to the crossing.
.autocal.solve:{[basis;winNs;t;v;spanHr;targetRate;floorBps]
  rfn:.autocal.rate[basis;winNs;t;v;spanHr];
  if[targetRate>=rfn floorBps; :floorBps];                 // floor rate already <= target -> can't do better
  hi:2*floorBps;
  while[(targetRate<=rfn hi) and hi<1e6; hi*:2];           // grow until rate(hi) < target (cap guards runaway)
  lo:floorBps;                                             // invariant: rate(lo) >= target > rate(hi)
  do[16; mid:0.5*lo+hi; $[targetRate<=rfn mid; lo:mid; hi:mid]];  // ~16 evals: bps precision = bracket/2^16
  0.5*lo+hi};

// Convenience: one asset's calibrated bar from its raw series. Returns a dict with the chosen bar,
// the unit/floor, whether the floor bound (quiet), the realized rate at the chosen bar, and span.
//   t = ns clock (exch_us in recv order), v = mid, medSprBps = median spread (bps).
.autocal.calibrate:{[basis;winNs;t;v;medSprBps;targetRate]
  spanHr:(`float$(last t)-first t)%3.6e12;
  unit:.autocal.unit[basis; v; medSprBps];
  thrBps:.autocal.solve[basis; winNs; t; v; spanHr; targetRate; unit];
  `thrBps`unitBps`floored`ratePerHr`spanHr`nTicks!
    (thrBps; unit; thrBps<=unit; .autocal.rate[basis;winNs;t;v;spanHr;thrBps]; spanHr; count v)};
