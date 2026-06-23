// sim/skew_demo.q — cross-venue clock-skew calibration ground-truth test (Item 5 / G5).
// Two venues quote the same leg with DIFFERENT exchange-clock offsets from our local
// clock: recvTs-eventTs floors at 130ms (BINANCE) vs 60ms (COINBASE). An injected
// cross-venue lead of a KNOWN 200ms (BINANCE leads COINBASE, in the common frame) is
// recovered exactly on the calibrated clock (calEventTs), while the RAW eventTs clock
// reads 270ms — biased up by the 70ms floor difference. Asserts:
//   1. .skew.calc recovers each leg's offset ~= its floor (the low percentile).
//   2. calEventTs lag == 200ms (true lead, bias removed).
//   3. raw eventTs lag == 270ms (the bias the calibration fixes).
// Run from project root:  q sim/skew_demo.q
system "l proc/load.q";
.log.toFile:0b;                                 // no durable event log for the sim

// --- scenario constants -----------------------------------------------------
ms:1000000;                                     // 1 ms in ns
.sim.t0bg:2026.06.07D09:00:00.000000000;        // background sample window start
.sim.t0  :.sim.t0bg+0D00:00:10;                 // injected-event anchor (well after bg)
.sim.flB :130*ms;                               // BINANCE  recvTs-eventTs floor (ns)
.sim.flC : 60*ms;                               // COINBASE recvTs-eventTs floor (ns)
.sim.trueLag:200*ms;                            // true BINANCE->COINBASE lead (common frame)
N:200;                                          // background ticks per venue

// --- background ticks: many valid (non-moving) quotes per venue, so .skew has a
// rich (recvTs-eventTs) sample. Mid is constant -> no qualifying moves -> no events;
// the ticks exist only to populate the per-leg floor estimate. recvTs-eventTs =
// floor + jitter(0..49ms); the low percentile recovers the floor.
jit:ms*(til N) mod 50;                           // non-negative jitter (ns), min 0
bgRecvB:.sim.t0bg+ms*til N;   bgEvtB:bgRecvB-(.sim.flB+jit);
bgRecvC:.sim.t0bg+ms*til N;   bgEvtC:bgRecvC-(.sim.flC+jit);
bg:([] eventTs:bgEvtB,bgEvtC; recvTs:bgRecvB,bgRecvC; sym:(2*N)#`BTC;
  venue:(N#`BINANCE),N#`COINBASE; inst:(2*N)#`SPOT;
  bid:(2*N)#99.995; ask:(2*N)#100.005; mid:(2*N)#100.00);

// --- injected cross-venue lead (a 5bps up-move on each leg, > 2bps threshold) ---
// Defined in the common (calibrated) frame: BINANCE at t0, COINBASE at t0+trueLag.
// eventTs_v = calEventTs_v - floor_v ; recvTs_v = eventTs_v + floor_v (at the floor).
injEvtB:.sim.t0-.sim.flB;                  injRecvB:injEvtB+.sim.flB;            // = t0
injEvtC:(.sim.t0+.sim.trueLag)-.sim.flC;   injRecvC:injEvtC+.sim.flC;           // = t0+trueLag
inj:([] eventTs:(injEvtB;injEvtC); recvTs:(injRecvB;injRecvC); sym:`BTC`BTC;
  venue:`BINANCE`COINBASE; inst:`SPOT`SPOT;
  bid:100.045 100.045; ask:100.055 100.055; mid:100.05 100.05);

// --- one trial on a given lag clock: reset state, feed background, calibrate, feed
// the injected pair, return the resulting events. Reloads leadlag.q so its load-time
// clock binding picks up the new .cfg.lagClock.
.sim.trial:{[clk]
  .cfg.lagClock:clk; system "l lib/leadlag.q"; system "l lib/comove.q";
  .feed.reset[]; .leadlag.reset[]; .skew.reset[]; .comove.reset[];
  delete from `quote_norm; delete from `leadlag_events;
  .feed.upd[`quote_norm; bg];               // populate skew sample buffer
  .skew.calc[];                             // resolve per-leg offsets from the floor
  .feed.upd[`quote_norm; inj];              // the cross-venue lead, matched on `clk`
  select leadVenue,followVenue,direction,lagMs from leadlag_events};

// --- run + assert -----------------------------------------------------------
evCal:.sim.trial`calEventTs;
offB:.skew.off[`BINANCE`SPOT]`offsetNs;      // resolved offsets from the calibrated trial
offC:.skew.off[`COINBASE`SPOT]`offsetNs;
evRaw:.sim.trial`eventTs;

-1 "resolved offsets (ms):  BINANCE ",string[1e-6*offB],"  COINBASE ",string 1e-6*offC;
-1 "\ncalEventTs events:"; show evCal;
-1 "raw eventTs events:";  show evRaw;

checks:(
  (`offB_130;     10>abs 130-1e-6*offB);                 // BINANCE floor recovered ~130ms
  (`offC_60;      10>abs  60-1e-6*offC);                 // COINBASE floor recovered ~60ms
  (`cal_one;      1=count evCal);
  (`cal_leadBIN;  evCal[`leadVenue]~enlist`BINANCE);
  (`cal_lag200;   1>abs 200-first evCal`lagMs);          // true lead recovered
  (`raw_one;      1=count evRaw);
  (`raw_lag270;   1>abs 270-first evRaw`lagMs);          // biased by the 70ms floor diff
  (`cal_fixes;    (first[evRaw`lagMs]-first evCal`lagMs) within 69 71));  // bias removed = floor diff
-1 "\nchecks:"; show flip `check`pass!flip checks;
$[all checks[;1];
  -1 "\nSKEW_DEMO PASS";
  [-1 "\nSKEW_DEMO FAIL"; exit 1]];
exit 0
