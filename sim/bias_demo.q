// sim/bias_demo.q — Stage 2 regression test for base-rate correction (E15).
//
// Drives the engine with controlled scenarios and checks the leadlagScore:
//   A. SYMMETRIC noise (equal tick rate, no lead)   -> both scores ~0
//   B. ASYMMETRIC noise (BINANCE 5x faster, no lead) -> the OLD cooled count score
//      is spuriously +ve (tick-rate artefact); the NEW base-rate-corrected score ~0
//   C. INJECTED LEAD (BINANCE leads COINBASE +200ms) on top of asymmetric noise
//      -> corrected score clearly +ve and significant (real signal survives)
// The injected lead is 200ms, deliberately ABOVE the dead-band (.cfg.deadBandMs,
// G2 — 100ms granularity floor since 2026-06-10, was 150): a sub-dead-band gap is
// dropped as a coincident co-move (in BOTH the event stream and the score's directed
// count). Do NOT shrink .bias.leadMs back below the dead-band.
//
// The mechanism (confirmed earlier): the hard cooldown asymmetrically suppresses the
// direction whose follower ticks fast, so the cooled event stream favours the faster
// venue. lib/comove.q rebuilds the score from un-cooled co-movement EXCESS over the
// coincidence expected under independence, removing the artefact. This script reads
// `corrScore` from leadership_score (corrected) and the old cooled score directly
// from leadlag_events, side by side. Self-asserting (exit 1 on failure).
// Run from project root:  q sim/bias_demo.q   [-cooldownMs 1]

system "l proc/load.q";
.log.toFile:0b;                                 // no durable event log for the sim
.log.level:`error;                              // quiet per-batch skip warnings

// --- fixed knobs ------------------------------------------------------------
.bias.t0     :2026.06.07D09:00:00.000000000;
.bias.stepBps:5f;                               // each move ~5bps (> 2bps default thr)
.bias.px0    :100f;
.bias.leadMs :200f;                             // injected true lead (C); > the dead-band

// Analysis window only: widen so a ~60s run fits inside one score window. A query
// choice (how far back to score), not an engine-behaviour change.
.score.window :0D00:05:00.000000000;
.comove.window:0D00:05:00.000000000;

// Optional cooldown override (ms) to probe the bias mechanism, e.g.
//   q sim/bias_demo.q -cooldownMs 1     (effectively no cooldown)
.bias.opt:.Q.opt .z.x;
if[`cooldownMs in key .bias.opt;
  .leadlag.cooldown:`timespan$`long$1e6*"F"$first .bias.opt`cooldownMs;
  -1"[cooldown overridden to ",(first .bias.opt`cooldownMs)," ms]"];

// --- leg construction -------------------------------------------------------
// Build a leg's quote stream from arrival times `ts` and step directions `dirs`
// (+/-1). Each step is +/- stepBps, so every tick is a qualifying move whose
// detected direction is dirs[i] (mid is an independent random walk per leg).
.bias.mkLeg:{[venue;ts;dirs]
  n:count ts;
  mid:.bias.px0*prds 1+(.bias.stepBps*dirs)%10000;
  ([] eventTs:ts; recvTs:ts; sym:n#`BTC; venue:n#venue; inst:n#`SPOT;
     bid:mid-0.005; ask:mid+0.005; mid:mid) };

// An independent-noise leg: n moves, gaps jittered in [0.5,1.5]*meanGap, random dir.
.bias.genLeg:{[venue;meanGapMs;n]
  ts:.bias.t0+`timespan$`long$1e6*sums meanGapMs*0.5+n?1f;
  .bias.mkLeg[venue; ts; -1+2*n?2] };

// --- scenario builders (functions of seed; seed is set in .bias.trial) ------
// Pure independent noise on both legs (no lead by construction).
.bias.noise:{[bg;cg;bn;cn;seed]
  (.bias.genLeg[`BINANCE;bg;bn]),.bias.genLeg[`COINBASE;cg;cn] };

// Asymmetric noise PLUS an injected BINANCE->COINBASE lead: nSig signal moves on
// BINANCE (evenly spread over 60s), each copied to COINBASE leadMs later, same
// direction. Both legs also carry independent noise at asymmetric rates.
.bias.lead:{[bg;cg;bn;cn;nSig;seed]
  sigTs :.bias.t0+`timespan$`long$1e6*(60000%nSig)*til nSig;   // signal-source times
  sigDir:-1+2*nSig?2;
  binTs :(.bias.t0+`timespan$`long$1e6*sums bg*0.5+bn?1f),sigTs;
  binDir:(-1+2*bn?2),sigDir;
  o:iasc binTs; binLeg:.bias.mkLeg[`BINANCE; binTs o; binDir o];
  folTs :sigTs+`timespan$`long$1e6*.bias.leadMs;              // COINBASE follows
  coiTs :(.bias.t0+`timespan$`long$1e6*sums cg*0.5+cn?1f),folTs;
  coiDir:(-1+2*cn?2),sigDir;
  o2:iasc coiTs; coiLeg:.bias.mkLeg[`COINBASE; coiTs o2; coiDir o2];
  binLeg,coiLeg };

// --- one trial --------------------------------------------------------------
.bias.trial:{[buildScn;seed]
  system "S ",string seed;                       // deterministic per trial
  scn:`recvTs xasc buildScn seed;
  .feed.reset[]; .leadlag.reset[]; .comove.reset[];
  delete from `quote_norm; delete from `leadlag_events;
  .feed.upd[`quote_norm; scn];
  .score.calc last exec recvTs from quote_norm;
  s:0!leadership_score;
  binLed:count select from leadlag_events where leadVenue=`BINANCE;
  coiLed:count select from leadlag_events where leadVenue=`COINBASE;
  enlist `rawScore`corrScore`binP`binLed`coiLed!(
    (binLed-coiLed)%binLed+coiLed;               // OLD cooled count score (biased)
    first exec leadlagScore from s where venue=`BINANCE;   // NEW corrected score
    first exec pValue      from s where venue=`BINANCE;
    binLed; coiLed) };

// --- run a scenario across seeds and summarise ------------------------------
.bias.run:{[label;buildScn;seeds]
  t:raze .bias.trial[buildScn] each seeds;
  -1"\n=== ",label," ===";
  -1"  BINANCE score: old cooled mean = ",string[avg t`rawScore],
    "   |   corrected mean = ",string[avg t`corrScore]," (sd ",string[dev t`corrScore],")";
  -1"  corrected range = [",string[min t`corrScore],", ",string[max t`corrScore],
    "]   median pValue = ",string med t`binP;
  t };

// --- execute ----------------------------------------------------------------
seeds:1+til 30;
aT:.bias.run["A. SYMMETRIC noise (no lead)";          .bias.noise[200f;200f;300;300]; seeds];
bT:.bias.run["B. ASYMMETRIC noise (BINANCE 5x, no lead)"; .bias.noise[200f;1000f;300;60]; seeds];
cT:.bias.run["C. INJECTED LEAD (BINANCE->COINBASE +200ms) + asymmetric noise"; .bias.lead[200f;1000f;300;60;30]; seeds];

// --- assertions -------------------------------------------------------------
checks:(
  (`A_corrected_near0;    0.05>abs avg aT`corrScore);
  (`B_oldCooled_biased;   0.10<avg bT`rawScore);          // the artefact the fix targets
  (`B_corrected_near0;    0.05>abs avg bT`corrScore);     // ... removed by base-rate correction
  (`C_corrected_positive; 0.10<avg cT`corrScore);         // real lead survives
  (`C_corrected_beats_B;  (avg cT`corrScore)>0.05+avg bT`corrScore);   // signal >> noise floor
  (`C_significant;        0.05>med cT`binP));              // and is statistically flagged
-1"\n--------------------------------------------------------------";
show flip `check`pass!flip checks;
$[all checks[;1];
  -1"\nBIAS_DEMO PASS — base-rate correction removes the tick-rate artefact and keeps real leads.";
  [-1"\nBIAS_DEMO FAIL"; exit 1]];
-1"--------------------------------------------------------------";
exit 0
