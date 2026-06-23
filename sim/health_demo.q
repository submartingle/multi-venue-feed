// sim/health_demo.q — self-asserting regression test for B2 feed-health (lib/health.q).
// Three scenarios, each on a reset engine, driving .feed.upd with synthetic ticks and
// calling .health.calc directly with controlled times (as the C8 timer would):
//   A. feedstale  — the EDGE-TRIGGER lifecycle: a leg goes silent (>healthQuietMs) ->
//      one alert; stays silent -> NO repeat; resumes -> recovery (state cleared);
//      goes silent again -> fires AGAIN. 2 alerts total = one per episode.
//   B. invalid    — a leg whose C11-rejected share exceeds healthInvalidPct (with at
//      least healthMinTicks in the window) is flagged; the clean sibling is not; once
//      the bad batch ages out of the window the condition recovers.
//   C. clockfallback — a leg stuck on the G1 fallback (eventTs:=recvTs, diffNs=0) is
//      flagged per (venue,inst) with a NULL sym (venue-wide fault); its offset stays
//      uncalibrated (null in feed_health) while the healthy leg calibrates.
// Constant bid/ask throughout -> no qualifying moves -> leadlag/score stay quiet; this
// exercises the feed-health path in isolation. Run from project root: q sim/health_demo.q
system "l proc/load.q";
.log.toFile:0b;

ms:1000000;                                 // 1ms in ns
.hd.t0:2026.06.07D09:00:00.000000000;

// n valid ticks for one leg: 1s apart from `start`, recvTs-eventTs = floorMs (non-zero
// -> not the fallback signature), constant non-crossed book.
.hd.mk:{[start;n;s;v;floorMs]
  r:start+1000*ms*til n;
  ([] eventTs:r-`long$ms*floorMs; recvTs:r; sym:n#s; venue:n#v; inst:n#`SPOT;
     bid:n#99.995; ask:n#100.005)};

// Reset the engine between scenarios.
.hd.reset:{[]
  .feed.reset[]; .leadlag.reset[]; .skew.reset[]; .comove.reset[]; .health.reset[];
  delete from `quote_norm; delete from `alerts; feed_health::0#feed_health;};

.hd.nAlert:{[at] count select from alerts where alertType=at};

checks:();

// --- A. feedstale: edge-trigger lifecycle ------------------------------------
.hd.reset[];
.feed.upd[`quote_norm; .hd.mk[.hd.t0;        45; `BTC;`BINANCE;  120f]];  // ticks to t0+44s
.feed.upd[`quote_norm; .hd.mk[.hd.t0;        11; `BTC;`COINBASE;  50f]];  // ticks to t0+10s, then silent
.health.calc .hd.t0+0D00:00:15;                       // cb age 5s  -> healthy
checks,:enlist (`A_quiet_no_alert;  0=.hd.nAlert`feedstale);
.health.calc .hd.t0+0D00:00:45;                       // cb age 35s -> fires
checks,:enlist (`A_fires_once;      1=.hd.nAlert`feedstale);
checks,:enlist (`A_right_leg;       `COINBASE~first exec venue from alerts where alertType=`feedstale);
checks,:enlist (`A_cb_nAcc;         11=first exec nAccepted from feed_health where venue=`COINBASE);
.health.calc .hd.t0+0D00:00:50;                       // still silent -> NO repeat
checks,:enlist (`A_no_repeat;       1=.hd.nAlert`feedstale);
.feed.upd[`quote_norm; .hd.mk[.hd.t0+0D00:00:45; 12; `BTC;`BINANCE;  120f]];
.feed.upd[`quote_norm; .hd.mk[.hd.t0+0D00:00:55;  1; `BTC;`COINBASE;  50f]];  // cb resumes
.health.calc .hd.t0+0D00:00:56;                       // cb age 1s -> recovery, re-armed
checks,:enlist (`A_recovered;       0=count select from .health.state where alertType=`feedstale);
.feed.upd[`quote_norm; .hd.mk[.hd.t0+0D00:00:57; 32; `BTC;`BINANCE;  120f]];
.health.calc .hd.t0+0D00:01:27;                       // cb silent again (32s) -> fires AGAIN
checks,:enlist (`A_refires;         2=.hd.nAlert`feedstale);

// --- B. invalid: rejected-tick share over the window -------------------------
.hd.reset[];
tB:.hd.t0+0D01:00:00;
bad:.hd.mk[tB; 60; `BTC;`BINANCE; 120f];
bad:update bid:100.01, ask:100.0 from bad where i<10;  // 10/60 crossed -> rejected (C11)
.feed.upd[`quote_norm; bad];
.feed.upd[`quote_norm; .hd.mk[tB; 60; `BTC;`COINBASE; 50f]];
.health.calc tB+0D00:01:01;
checks,:enlist (`B_fires;           1=.hd.nAlert`invalid);
checks,:enlist (`B_right_leg;       `BINANCE~first exec venue from alerts where alertType=`invalid);
checks,:enlist (`B_invalidPct;      0.001>abs (10%60)-first exec invalidPct from feed_health where venue=`BINANCE);
checks,:enlist (`B_nRejected;       10=first exec nRejected from feed_health where venue=`BINANCE);
checks,:enlist (`B_accepted_only;   110=count quote_norm);  // 50+60 valid rows persisted
// bad batch ages out of the 60s window; fresh clean ticks -> condition recovers
.feed.upd[`quote_norm; .hd.mk[tB+0D00:01:10; 5; `BTC;`BINANCE; 120f]];
.feed.upd[`quote_norm; .hd.mk[tB+0D00:01:10; 5; `BTC;`COINBASE; 50f]];
.health.calc tB+0D00:02:10;
checks,:enlist (`B_recovers;        0=count select from .health.state where alertType=`invalid);
checks,:enlist (`B_no_repeat;       1=.hd.nAlert`invalid);

// --- C. clockfallback: G1-fallback share per (venue,inst), null sym ----------
.hd.reset[];
tC:.hd.t0+0D02:00:00;
.feed.upd[`quote_norm; .hd.mk[tC; 60; `BTC;`BINANCE; 0f]];    // eventTs=recvTs: fallback
.feed.upd[`quote_norm; .hd.mk[tC; 60; `BTC;`COINBASE; 50f]];  // healthy 50ms floor
.skew.calc[];                                          // as the timer would, before health
.health.calc tC+0D00:01:01;
checks,:enlist (`C_fires;           1=.hd.nAlert`clockfallback);
checks,:enlist (`C_right_leg;       `BINANCE~first exec venue from alerts where alertType=`clockfallback);
checks,:enlist (`C_null_sym;        null first exec sym from alerts where alertType=`clockfallback);
checks,:enlist (`C_fb_share;        1.0=first exec fallbackPct from feed_health where venue=`BINANCE);
checks,:enlist (`C_no_floor;        null first exec oneWayFloorMs from feed_health where venue=`BINANCE);
checks,:enlist (`C_good_floor;      0.001>abs 50-first exec oneWayFloorMs from feed_health where venue=`COINBASE);

// --- report -------------------------------------------------------------------
-1 "\nchecks:"; show flip `check`pass!flip checks;
$[all checks[;1];
  -1 "\nHEALTH_DEMO PASS";
  [-1 "\nHEALTH_DEMO FAIL"; exit 1]];
exit 0
