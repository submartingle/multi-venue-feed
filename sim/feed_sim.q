// sim/feed_sim.q — synthetic deterministic lead->lag scenario with KNOWN ground
// truth, to assert engine correctness end-to-end (feed -> leadlag). Real data can
// validate behaviour but not exact lags; this can. Run from project root:
//   q sim/feed_sim.q
system "l proc/load.q";
.log.toFile:0b;                                 // no durable event log for the sim

// --- scenario ---------------------------------------------------------------
// Two same-inst legs across venues (pairMode=crossVenue default): BINANCE leads,
// COINBASE follows exactly 200ms later, three times (up, up, down). Moves are
// ~5bps (> the 2bps default threshold); pairs are 2s apart (> 1s cooldown) so all
// three fire. The 200ms lead is deliberately ABOVE the dead-band (.cfg.deadBandMs, G2 —
// 100ms granularity floor since 2026-06-10, was 150): a sub-dead-band gap is treated as
// a coincident co-move and dropped, so do NOT shrink it below the dead-band.
// Expected: 3 events, BINANCE->COINBASE, lagMs=200, up/up/down.
.sim.t0:2026.06.07D09:00:00.000000000;
offs:0D00:00:00 0D00:00:00 0D00:00:02 0D00:00:02.200 0D00:00:04 0D00:00:04.200 0D00:00:06 0D00:00:06.200;
vens:`BINANCE`COINBASE`BINANCE`COINBASE`BINANCE`COINBASE`BINANCE`COINBASE;
mids:100.00     100.00    100.05    100.05     100.10    100.10     100.05    100.05;
scn:([] eventTs:.sim.t0+offs; recvTs:.sim.t0+offs; sym:8#`BTC; venue:vens; inst:8#`SPOT;
  bid:mids-0.005; ask:mids+0.005; mid:mids);    // tight book straddling the target mid

// --- run --------------------------------------------------------------------
.feed.reset[]; .leadlag.reset[];
delete from `quote_norm; delete from `leadlag_events;
.feed.upd[`quote_norm; scn];

// --- assert -----------------------------------------------------------------
ev:select leadVenue,leadInst,followVenue,followInst,direction,lagMs,moveSizeBps from leadlag_events;
-1 "events:"; show ev;
checks:(
  (`count;        3=count ev);
  (`leadIsBIN;    all (ev`leadVenue)=`BINANCE);
  (`followIsCOIN; all (ev`followVenue)=`COINBASE);
  (`lag200ms;     all 0.001>abs (ev`lagMs)-200);
  (`directions;   (ev`direction)~`up`up`down));
-1 "\nchecks:"; show flip `check`pass!flip checks;
$[all checks[;1];
  -1 "\nFEED_SIM PASS";
  [-1 "\nFEED_SIM FAIL"; exit 1]];
exit 0
