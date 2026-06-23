// lib/comove.q — base-rate-corrected co-movement statistics (.comove.*)
// Stage 2 / E15. The cooled leadlag_events stream is biased: the hard cooldown
// asymmetrically suppresses the direction whose FOLLOWER ticks fast, so a faster-
// ticking venue is spuriously scored as leader even on pure noise (demonstrated by
// sim/bias_demo.q: +0.21 at 5:1 tick asymmetry). This module recomputes lead/lag
// from an UN-COOLED window of qualifying moves and subtracts the coincidence
// expected under independence, yielding a directed leadership matrix with effect
// sizes and significance. Runs on the timer (C8), off the hot path. The
// leadlag_events stream and the cooldown are left untouched (operational de-dup).
//
//   For an ordered comparable leg pair (A=lead, B=follow) and direction d, over the
//   score window [windowEnd-T, windowEnd]:
//     obs       C  = # (B-move, A-move) pairs, A in [t_B-W, t_B), same d  (W=follow win)
//     expected  mu = N_B,d * (n_A,d / T) * W      (coincidence under independence)
//     excess    e  = C - mu
//   Under pure noise E[e]=0 in BOTH directions (the un-cooled count is symmetric in
//   tick rate); a genuine lead makes the leader's excess positive. Significance:
//   C ~ Poisson(mu) under the null -> one-sided upper-tail p-value.

// Config resolved at load (same trailing window as the score; default 1 minute).
.comove.window:@[value; `.cfg.scoreWindow; 0D00:01:00.000000000];
// Session-score history retention (Item 3): the move history is evicted to this,
// NOT to the score window, so the cumulative session score can look back over the
// whole run. The windowed score still filters to .comove.window. Memory guard.
.comove.retain:@[value; `.cfg.sessionRetain; 0D12:00:00.000000000];
// Lead-lag clock + dead-band, shared with leadlag.q (Item 2). The score's directed
// counts use the same exchange-time clock and noise-floor dead-band as the
// operational event stream, so the two stay consistent.
.comove.lagClock:@[value; `.cfg.lagClock; `eventTs];
// Dead-band is read DYNAMICALLY from .skew.deadBandNs in .comove.calcOver (Item 5 slice 2),
// the SAME effective band the operational matcher uses — so the score's directed counts and
// the coincidence baseline (W - deadBand) stay consistent with the event stream in both
// `fixed` and `auto` modes. (Was a load-time constant from .cfg.deadBandMs.)

// Windowed history of qualifying moves (fed by lib/feed.q), evicted to the score
// window. This is the un-cooled signal the score is built from. eventTs/recvTs both
// carried; window eviction is by recvTs (arrival), lag is measured on .comove.lagClock.
.comove.hist:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  recvTs:`timestamp$(); eventTs:`timestamp$(); direction:`symbol$());

// Empty directed-pair result template (also the leadership_pairs shape).
.comove.pairs0:([] ts:`timestamp$(); sym:`symbol$();
  leadVenue:`symbol$(); leadInst:`symbol$(); followVenue:`symbol$(); followInst:`symbol$();
  direction:`symbol$(); nLead:`long$(); nFollow:`long$(); obs:`long$(); obsK:`float$();
  expected:`float$(); excess:`float$(); lagSumMs:`float$(); avgLagMs:`float$(); pValue:`float$());

.comove.reset:{[] .comove.hist:0#.comove.hist;};

// Record a batch of qualifying moves and evict past the window (called by feed).
.comove.record:{[moves]
  if[count moves;
    `.comove.hist insert select sym,venue,inst,recvTs,eventTs,direction from moves;
    // evict to the SESSION retention (not the score window) so the cumulative session
    // score retains the full run; the windowed score filters to .comove.window itself.
    .comove.hist:select from .comove.hist where recvTs>=(max recvTs)-.comove.retain];
  };

// Standard-normal CDF (Zelen-Severo rational approximation); scalar argument.
.comove.normCdf:{[x]
  $[x<0; 1-.comove.normCdf neg x;
    [t:1%1+0.2316419*x;
     phi:exp[-0.5*x*x]%sqrt acos[-1]*2;
     1-phi*t*(0.319381530+t*(-0.356563782+t*(1.781477937+t*(-1.821255978+t*1.330274429))))]]};

// One-sided upper-tail p-value: P(X>=C) for X~Poisson(mu), via normal approx.
.comove.pois1:{[C;mu] $[mu<=0; 1f; .comove.normCdf neg (C-mu)%sqrt mu]};

// --- E16 directional significance + Wilson CI -------------------------------
// Under coincidence the per-direction expected lead/follow counts are IDENTICAL
// (both nA*nB*W/T), so ledExpected==folExpected and the base-rate correction cancels
// in the asymmetry (ledExcess-folExcess == ledObs-folObs). "Leads above coincidence"
// is therefore exactly a binomial test on the directed split under H0: p=0.5.

// Two-sided binomial sign-test p-value: P(|X-n/2| >= |k-n/2|), X~Bin(n,0.5), normal
// approx with continuity correction. k = #led, n = #led+#follow. n=0 -> 1 (no evidence).
.comove.signTest:{[k;n]
  $[n<=0; 1f;
    [d:0f|abs[k-0.5*n]-0.5;                       // |dev from mean|, continuity-corrected, floored
     2f*1f-.comove.normCdf d%sqrt 0.25*n]]};       // two-sided upper tail, <=1

// Wilson score interval for proportion k/n at z stddevs. Returns (low;high) on the
// PROPORTION scale [0,1]. n=0 -> (0 1f) (no information -> full width).
.comove.wilson:{[k;n;z]
  $[n<=0; 0 1f;
    [ph:k%n; z2:z*z; den:1+z2%n;
     ctr:(ph+z2%2*n)%den;
     hw:(z%den)*sqrt (ph*(1-ph)%n)+z2%4*n*n;
     (ctr-hw;ctr+hw)]]};

// Co-movement count + lag sum for one ordered leg pair within a (sym,direction)
// group. la/lb: sorted lead/follow move times (ns longs). Dns = dead-band (ns):
// only leads at least Dns before the follow count. Returns (C; lagSumNs). Window is
// [t_B-W, t_B-D) — lower inclusive, upper exclusive — matching the matcher.
.comove.pairCount:{[Wns;Dns;la;lb]
  hi:la binr lb-Dns;              // # lead times < (follow - D)  (dead-band upper bound)
  lo:la binr lb-Wns;              // # lead times < (follow - W)
  cnt:hi-lo;                      // lead moves in [t_B-W, t_B-D) per follow move
  PA:0,sums la;                   // prefix sums of lead times (PA[k]=sum of first k)
  (sum cnt; sum (cnt*lb)-PA[hi]-PA[lo])};   // (total matches; sum of (t_B - t_A))

// Build the base-rate-corrected directed-pair table over the move history in
// (lo; windowEnd]. The window is a parameter so the same machinery serves both the
// trailing-window score (.comove.calc) and the cumulative session score
// (.comove.calcSession). Returns a table with the .comove.pairs0 columns.
.comove.calcOver:{[lo; windowEnd]
  Wns:`long$.leadlag.followWin;                    // follow window (ns)
  Wms:1e-6*Wns;                                    // follow window (ms) — for the E17 kernel
  Dns:.skew.deadBandNs;                            // effective dead-band (ns) — same as the matcher
  h:select sym,venue,inst,recvTs,eventTs,direction from .comove.hist where recvTs>=lo, recvTs<=windowEnd;
  if[not count h; :0#.comove.pairs0];
  // tns = the configured lead-lag clock (exchange-time by default); window membership
  // is still by recvTs (arrival). Pick the clock column in PLAIN q (not inside qsql —
  // $[] doesn't vectorise in select/update here, 'rank); lagClock is scalar so $[]
  // selects the whole column. calEventTs (Item 5) = eventTs + per-leg skew offset, the
  // same calibrated clock the operational matcher uses. Then attach as a local vector.
  clk:`long$ $[
    .comove.lagClock=`recvTs;     h`recvTs;
    .comove.lagClock=`calEventTs; .skew.calClk h;
    h`eventTs];
  m:update tns:clk from h;
  // T = ACTUAL observation span of moves in the window (not the nominal window
  // length): the coincidence rate n_A/T must reflect when moves actually occur, or
  // a sparsely-filled window understates the rate and inflates significance.
  Tns:1|(max m`tns)-min m`tns;
  // per (sym,direction,leg): sorted move times + directional count
  legs:update nd:count each tns from 0!select tns:asc tns by sym,direction,venue,inst from m;
  // all ordered leg pairs within each (sym,direction) group
  p:ej[`sym`direction;
       select sym,direction,leadVenue:venue,  leadInst:inst,  la:tns,nLead:nd  from legs;
       select sym,direction,followVenue:venue,followInst:inst,lb:tns,nFollow:nd from legs];
  // drop self-pairs, keep only comparable siblings (pairMode, reused from leadlag)
  p:select from p where not (leadVenue=followVenue)&leadInst=followInst;
  if[count p; p:select from p where .leadlag.sib[leadVenue;leadInst;followVenue;followInst]];
  if[not count p; :0#.comove.pairs0];
  // per-pair co-movement count + lag sum (dead-band excludes sub-noise-floor gaps)
  cl:.comove.pairCount[Wns;Dns] ./: flip (p`la;p`lb);
  p:update obs:cl[;0], lagSumMs:1e-6*cl[;1] from p;
  // coincidence baseline over the EFFECTIVE counting interval (W - deadBand): obs only
  // counts leads in [t_B-W, t_B-D), so expected must use the same width or it overstates.
  // Stays symmetric in (lead,follow) -> ledExpected==folExpected, so the E16 sign test holds.
  p:update expected:nFollow*(nLead%Tns)*(Wns-Dns) from p;
  p:update excess:obs-expected, avgLagMs:lagSumMs%obs from p;   // obs=0 -> avgLagMs 0n
  // E17 kernel-decay weight (NEW column, versioned alongside the count-based obs — the
  // original obs/excess are untouched). Each matched co-movement contributes a linear
  // kernel max(0,1-lag/W) instead of 1, down-weighting slow follows. Every matched lag
  // is in (0,W] by construction, so the weighted count collapses to a closed form over
  // values we already have: obsK = sum(1-lag_i/W) = obs - (sum lag_i)/W = obs - lagSumMs/Wms.
  // No per-pair pass needed; obsK in [0,obs].
  p:update obsK:obs - lagSumMs % Wms from p;
  p:update pValue:.comove.pois1'[obs;expected], ts:windowEnd from p;
  `ts`sym`leadVenue`leadInst`followVenue`followInst`direction`nLead`nFollow`obs`obsK`expected`excess`lagSumMs`avgLagMs`pValue#p};

// Trailing-window directed matrix (the "who leads NOW" view): look back .comove.window.
.comove.calc:{[windowEnd] .comove.calcOver[windowEnd-.comove.window; windowEnd]};

// Cumulative session directed matrix (Item 3): look back over the full retained
// history. The coincidence baseline (n_A/T) uses T = actual span of retained moves,
// so accumulating evidence tightens significance rather than diluting the rate.
.comove.calcSession:{[windowEnd] .comove.calcOver[windowEnd-.comove.retain; windowEnd]};
