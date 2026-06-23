// lib/score.q — rolling leadership score (.score.*)
// Recomputes leadership_score + leadership_pairs over a trailing window, on the
// timer (C8). The score is BASE-RATE CORRECTED (E15): it is built from the un-cooled
// co-movement EXCESS produced by lib/comove.q, NOT from raw cooled leadlag_events
// counts (which the hard cooldown biases toward faster-ticking venues — see
// sim/bias_demo.q). One row per leg active in the window; the table is rebuilt each
// call (snapshot semantics).
//
//   leadlagScore : (ledExcess - folExcess) % (ledObs + folObs), signed, ~[-1,1].
//                  Excess = observed co-movements - coincidence expected under
//                  independence, so ~0 on pure noise irrespective of tick rate.
//   avgLag       : mean lag (ms) over co-movements where the leg LED (Option A,
//                  OQ-5); null when the leg never led (pure follower).
//   nEvents      : total un-cooled co-movements involving the leg.
//   pValue       : significance of the leg's strongest lead (min Poisson upper-tail
//                  p over its leading pairs); ~1 = indistinguishable from coincidence.
//   regimeBucket : left null here; populated by lib/regime.q (B6).
// Division is null-safe: a leg with no co-movement overlap gets 0n, not an error.

// Trailing window resolved at load (config-at-load; default 1 minute).
.score.window:@[value; `.cfg.scoreWindow; 0D00:01:00.000000000];
.score.ciZ:@[value; `.cfg.ciZ; 1.959964];        // z for the Wilson CI (E16); 1.96 = 95%

// Aggregate a base-rate-corrected directed-pair matrix (from .comove.*) into the
// per-leg leadership_score shape. Shared by the trailing-window score (.score.calc)
// and the cumulative session score (.score.calcSession) — same statistics, different
// input window. Returns the per-leg table; empty pairs -> empty (typed) table.
.score.fromPairs:{[pairs; windowEnd]
  if[not count pairs; :0#leadership_score];
  // aggregate the directed pairs to each leg's lead side and follow side
  led:select ledExcess:sum excess, ledObs:sum obs, ledObsK:sum obsK, ledLagMs:sum lagSumMs
    by sym, venue:leadVenue, inst:leadInst from pairs;
  fol:select folExcess:sum excess, folObs:sum obs, folObsK:sum obsK
    by sym, venue:followVenue, inst:followInst from pairs;
  legs:distinct (key led),key fol;               // every leg active in the window
  j:legs lj led; j:j lj fol;
  j:update ledExcess:0f^ledExcess, folExcess:0f^folExcess, ledObs:0^ledObs,
    folObs:0^folObs, ledObsK:0f^ledObsK, folObsK:0f^folObsK, ledLagMs:0f^ledLagMs from j;
  j:update ne:ledObs+folObs, neK:ledObsK+folObsK from j;
  // E16 DIRECTIONAL significance. The OLD pValue was min Poisson upper-tail over the
  // leg's leading pairs — i.e. "co-moves above coincidence", which is ~0 whenever there
  // is ANY real co-movement and says nothing about lead vs follow. Because the
  // coincidence baseline is symmetric (ledExpected==folExpected exactly), the directional
  // test is a binomial sign test on the split: pValue = P(led/follow split != 50/50).
  // ciLow/ciHigh are the Wilson interval on the led-proportion, mapped to the score's
  // [-1,1] scale (score = 2*pLed-1), so a 5-3 split reads wide and a 500-300 reads tight.
  j:update pValue:.comove.signTest'[ledObs;ne] from j;
  w:.comove.wilson[;;.score.ciZ]'[j`ledObs; j`ne];          // (low;high) on proportion scale
  // map proportion CI -> score scale: score = 2*p-1. NB q is right-to-left, so the
  // (2*p) must be parenthesised: `2*p-1` would parse as 2*(p-1).
  j:update ciLow:`float$-1+2*w[;0], ciHigh:`float$-1+2*w[;1] from j;
  // Guard both divisions with a conditional, NOT a 0^/trap: q's % never signals on
  // a zero denominator (it returns IEEE +/-0w when the numerator is non-zero, 0n only
  // for 0%0), so @[;;fallback] would not catch it and 0^ only fixes the 0/0 case.
  // A leg with zero observed co-movements (ne=0) still carries a non-zero numerator
  // (excess = obs-expected = -expected), which would yield +/-0w and poison any
  // avg/sort over the score. ?[den>0; ...; 0n] = "no co-movement -> no evidence".
  // E17 kernel-decay-weighted score, VERSIONED alongside the count-based leadlagScore
  // (the original is untouched). Same shape, but built from the lag-weighted obsK so a
  // leg that leads FAST and follows SLOW scores higher. The base-rate term cancels here
  // too: a coincidental match has lag ~Uniform(0,W] -> mean kernel 0.5, so expectedK =
  // 0.5*expected, still symmetric across directions -> drops out of the asymmetry, leaving
  // leadlagScoreK = (ledObsK-folObsK) % (ledObsK+folObsK).
  res:select ts:windowEnd, sym, venue, inst,
    leadlagScore: ?[ne>0;     (ledExcess-folExcess)%ne; 0n], // no overlap -> 0n
    leadlagScoreK:?[neK>0;    (ledObsK-folObsK)%neK;    0n], // kernel-weighted (E17)
    avgLag:       ?[ledObs>0; ledLagMs%ledObs;          0n], // never led  -> 0n
    nEvents:ne,
    pValue, ciLow, ciHigh,
    regimeBucket:`                                          // filled by lib/regime.q
    from j;
  res};

// Recompute leadership_pairs + leadership_score over the trailing window ending at
// `windowEnd`. Engine passes .z.p on the timer; tests/sim pass a fixed event-time.
.score.calc:{[windowEnd]
  pairs:.comove.calc windowEnd;                  // base-rate-corrected directed matrix
  `leadership_pairs set pairs;
  `leadership_score set .score.fromPairs[pairs; windowEnd];
  };

// Recompute the CUMULATIVE session score over the whole retained history (Item 3).
// Same statistics as .score.calc but over .comove.calcSession, into leadership_session
// (regimeBucket left null — regime is an instantaneous concept, see lib/regime.q).
.score.calcSession:{[windowEnd]
  `leadership_session set .score.fromPairs[.comove.calcSession windowEnd; windowEnd];
  };
