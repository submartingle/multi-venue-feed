// sim/deadband_demo.q — data-driven dead-band ground-truth test (Item 5 slice 2).
// Two legs are fed background ticks with a KNOWN residual timing jitter: per leg,
// recvTs-eventTs = floor + uniform(0..200ms), so the p5..p95 spread (the jitter) is
// 180ms on each leg. The auto dead-band should size itself to the COMBINED cross-leg
// noise ~ sqrt2 * worst-leg jitter = sqrt2 * 180 ~= 254.6ms.
//
// The punchline (the whole point of slice 2): an injected 220ms cross-venue lead is
//   - KEPT under `fixed` mode (220ms > the .cfg.deadBandMs granularity floor), but
//   - DROPPED under `auto` mode (220ms < the 254.6ms MEASURED noise floor).
// i.e. when the feed is genuinely noisy, a 220ms "lead" is within the noise and auto
// correctly refuses to trust it, while the fixed floor would wave it through.
// The checks read the floor from .cfg.deadBandMs (100ms since 2026-06-10; was 150),
// so the demo stays valid if the floor moves — as long as it stays below 220ms.
// Run from project root:  q sim/deadband_demo.q
system "l proc/load.q";
.log.toFile:0b;
.cfg.lagClock:`calEventTs;                  // calibrate (slice 1) so the gap is clean
system "l lib/leadlag.q"; system "l lib/comove.q";

// --- scenario constants -----------------------------------------------------
ms:1000000;                                 // 1ms in ns
N:402;                                      // 2 full cycles of the 0..200 residual
.dd.t0bg:2026.06.07D09:00:00.000000000;
.dd.t0  :.dd.t0bg+0D00:00:10;               // injected-event anchor (after bg)
.dd.flB :130*ms;  .dd.flC :60*ms;           // per-leg recvTs-eventTs floors
.dd.lead:220*ms;                            // injected lead: between .cfg.deadBandMs (fixed) and 254.6 (auto)

// background: recvTs-eventTs = floor + uniform(0..200ms). p5=floor+10, p95=floor+190,
// so offset (p5) = floor+10ms and jitter (p95-p5) = 180ms on each leg.
resid:ms*(til N) mod 201;
recvB:.dd.t0bg+ms*til N;   evtB:recvB-(.dd.flB+resid);
recvC:.dd.t0bg+ms*til N;   evtC:recvC-(.dd.flC+resid);
bg:([] eventTs:evtB,evtC; recvTs:recvB,recvC; sym:(2*N)#`BTC;
  venue:(N#`BINANCE),N#`COINBASE; inst:(2*N)#`SPOT;
  bid:(2*N)#99.995; ask:(2*N)#100.005; mid:(2*N)#100.00);

// injected cross-venue lead, defined in the calibrated frame. offsets are deterministic:
// p5 of each leg's diff = floor+10ms -> offsetB=140ms, offsetC=70ms. So eventTs_v =
// calEventTs_v - offset_v makes calEventTs_C - calEventTs_B == .dd.lead exactly.
offB:.dd.flB+10*ms;  offC:.dd.flC+10*ms;
injEvtB:.dd.t0-offB;            injRecvB:.dd.t0;
injEvtC:(.dd.t0+.dd.lead)-offC; injRecvC:.dd.t0+.dd.lead;
inj:([] eventTs:(injEvtB;injEvtC); recvTs:(injRecvB;injRecvC); sym:`BTC`BTC;
  venue:`BINANCE`COINBASE; inst:`SPOT`SPOT;
  bid:100.045 100.045; ask:100.055 100.055; mid:100.05 100.05);

// --- one trial on a given dead-band mode ------------------------------------
.dd.trial:{[mode]
  .cfg.deadBandMode:mode; system "l lib/skew.q";    // re-resolve mode, reset skew state
  .feed.reset[]; .leadlag.reset[]; .skew.reset[]; .comove.reset[];
  delete from `quote_norm; delete from `leadlag_events;
  .feed.upd[`quote_norm; bg];                       // populate jitter samples
  .skew.calc[];                                     // resolve offsets + dead-band
  .feed.upd[`quote_norm; inj];                      // the 220ms cross-venue lead
  `band`measured`nev!(.skew.deadBandNs; .skew.measuredNs; count leadlag_events)};

// --- run + assert -----------------------------------------------------------
fx:.dd.trial`fixed;
au:.dd.trial`auto;
-1 "fixed: band ",string[1e-6*fx`band],"ms  measured ",string[1e-6*fx`measured],"ms  events ",string fx`nev;
-1 "auto : band ",string[1e-6*au`band],"ms  measured ",string[1e-6*au`measured],"ms  events ",string au`nev;

sqrt2j:1e-6*`long$(sqrt 2)*180*ms;          // expected auto band: sqrt2 * 180ms jitter
checks:(
  (`lead_above_floor; .dd.lead > ms*.cfg.deadBandMs);   // scenario guard: keep 220 > the floor
  (`fixed_band_cfg; 1>abs .cfg.deadBandMs - 1e-6*fx`band); // fixed mode = the configured floor
  (`fixed_keeps220; 1=fx`nev);                          // 220ms > floor -> lead kept
  (`measured_eq;    1>abs sqrt2j - 1e-6*fx`measured);   // measured observable even in fixed mode
  (`auto_band_meas; 1>abs sqrt2j - 1e-6*au`band);       // auto band == sqrt2 * jitter (~254.6ms)
  (`auto_drops220;  0=au`nev);                          // 220ms < 254.6ms floor -> lead dropped
  (`auto_gt_fixed;  (au`band) > fx`band));              // auto floor is stricter on a noisy feed
-1 "\nchecks:"; show flip `check`pass!flip checks;
$[all checks[;1];
  -1 "\nDEADBAND_DEMO PASS";
  [-1 "\nDEADBAND_DEMO FAIL"; exit 1]];
exit 0
