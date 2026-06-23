// schema/output.q — engine output tables
// Three live in-memory tables produced by the engine. In-memory only for MVP
// (no HDB flush yet — see DECISIONS.md C9).

// leadlag_events: tick-by-tick log of every qualifying lead->follow match.
// Each row pairs a leading leg with the following leg of the same root sym.
// Both venue and inst are carried on each side so the generic pairing model
// (Spot-vs-Perp, venue-vs-venue, ETF-vs-basket) all share one schema (A1).
//   ts          : recvTs of the follow move (event emission time)
//   direction   : `up / `down — direction of the shared move
//   lagMs       : recvTs[follow] - recvTs[lead], milliseconds (A2: local clock)
//   moveSizeBps : size of the leading move in bps
//   clockSkewMs : (leadEventTs-leadRecvTs) - (followEventTs-followRecvTs). Each leg's
//                 own (eventTs-recvTs) is <0 (recvTs is local-receive, after exchange
//                 eventTs), but the two telescope to recvLag - eventLag = lagMs minus
//                 the event-timestamp lag: the part of the measured recvTs lead NOT
//                 explained by exchange timestamps (clock offset + transit-latency
//                 asymmetry). On the same row eventLag = lagMs - clockSkewMs; the SIGN
//                 says whether feed-latency asymmetry inflated (-) or deflated (+) the
//                 apparent lead. Diagnostic only — never feeds lag/score (A2: all lag
//                 is single-clock recvTs).
leadlag_events:([] ts:`timestamp$(); sym:`symbol$();
  leadVenue:`symbol$(); leadInst:`symbol$(); followVenue:`symbol$(); followInst:`symbol$();
  direction:`symbol$(); lagMs:`float$(); moveSizeBps:`float$(); clockSkewMs:`float$());

// leadership_score: rolling 1-minute summary per leg, rebuilt on the timer.
//   leadlagScore : BASE-RATE-CORRECTED signed lead score (E15). Built from un-cooled
//                  co-movement EXCESS (observed - expected-under-independence), not raw
//                  cooled event counts: (ledExcess - folExcess) % (ledObs + folObs).
//                  ~0 on pure noise regardless of tick rate; +ve = genuine leader.
//   leadlagScoreK: E17 kernel-decay-weighted variant, versioned alongside (the count-based
//                  leadlagScore is untouched). Same [-1,1] shape from lag-weighted obsK:
//                  (ledObsK - folObsK) % (ledObsK + folObsK). Rewards leading fast /
//                  following slow; equals leadlagScore when all lags -> 0.
//   avgLag       : mean lag (ms) over co-movements where this leg LED (OQ-5 Option A;
//                  null when the leg never led in the window)
//   nEvents      : total un-cooled co-movements involving this leg in the window
//   pValue       : E16 DIRECTIONAL significance — two-sided binomial sign test that the
//                  leg's led/followed split departs from 50/50 (H0: lead-neutral). ~1 =
//                  indistinguishable from coincidence. Replaces the old "co-moves above
//                  coincidence" Poisson test, which was ~0 whenever any co-movement
//                  existed and said nothing about lead vs follow (see DECISIONS E16).
//   ciLow/ciHigh : Wilson confidence interval on leadlagScore, [-1,1] scale (z=.cfg.ciZ,
//                  default 95%), so a 5-vs-3 split (wide CI) and a 500-vs-300 split (tight
//                  CI) do not read identically.
//   regimeBucket : composite spread/vol/time label (see lib/regime.q)
leadership_score:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  leadlagScore:`float$(); leadlagScoreK:`float$(); avgLag:`float$(); nEvents:`long$();
  pValue:`float$(); ciLow:`float$(); ciHigh:`float$(); regimeBucket:`symbol$());

// leadership_session: CUMULATIVE per-leg leadership over the whole session (Item 3).
// Identical shape to leadership_score, but aggregated over the full retained move
// history (.cfg.sessionRetain) instead of the trailing .cfg.scoreWindow — so nEvents
// accumulates, the Wilson CI tightens, and the leader call stops sign-flipping on the
// single-digit n of a quiet window. regimeBucket is null here (regime is instantaneous).
leadership_session:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  leadlagScore:`float$(); leadlagScoreK:`float$(); avgLag:`float$(); nEvents:`long$();
  pValue:`float$(); ciLow:`float$(); ciHigh:`float$(); regimeBucket:`symbol$());

// leadership_pairs: the base-rate-corrected DIRECTED leadership matrix (E15/A3),
// rebuilt on the timer alongside leadership_score. One row per comparable ordered
// leg pair (lead -> follow) and direction. This is the statistical artefact the
// per-leg leadlagScore aggregates; it exposes the counterparty structure the scalar
// score collapses (a leg may lead X but follow Y).
//   obs / expected : observed co-movements vs coincidence expected under independence
//   obsK           : E17 kernel-decay-weighted obs — each match weighted max(0,1-lag/W),
//                    down-weighting slow follows; in [0,obs]. New column, versioned
//                    alongside the count-based obs (original obs/excess untouched).
//   excess         : obs - expected (the de-biased lead evidence)
//   avgLagMs       : mean lag (ms) over the matched co-movements
//   pValue         : one-sided Poisson upper-tail P(>=obs | expected)
leadership_pairs:([] ts:`timestamp$(); sym:`symbol$();
  leadVenue:`symbol$(); leadInst:`symbol$(); followVenue:`symbol$(); followInst:`symbol$();
  direction:`symbol$(); nLead:`long$(); nFollow:`long$(); obs:`long$(); obsK:`float$();
  expected:`float$(); excess:`float$(); lagSumMs:`float$(); avgLagMs:`float$(); pValue:`float$());

// alerts: feed-health / execution-risk events (see lib/alert.q, lib/health.q).
//   alertType : `stale (B1 stale-follower) | `feedstale | `invalid | `clockfallback (B2)
//               | `integrity | `aggression (reserved)
//   msg       : low-cardinality detail symbol (high-cardinality strings interned as
//               symbols live forever — context belongs in dedicated columns)
//   sym       : null for per-(venue,inst) conditions (e.g. `clockfallback — the skew
//               calibration pools symbols per leg, so the fault is venue-wide)
alerts:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$(); alertType:`symbol$(); msg:`symbol$());

// feed_health: per-leg feed plumbing diagnostics (B2, lib/health.q). REBUILT (replaced)
// on each timer tick — a current-state view, not a history; snapshot to file if a
// history is wanted. One row per leg that has ever delivered an accepted tick.
//   lastTickAgeMs : ms since the leg's last ACCEPTED tick (dead-feed signal)
//   ticksPerMin   : accepted ticks per minute over the trailing .cfg.healthWindow
//   nAccepted/nRejected : tick counts in the window (rejected = C11 validity filter)
//   invalidPct    : nRejected % total (null until any tick lands in the window)
//   oneWayFloorMs/oneWayJitterMs : per-(venue,inst) ONE-WAY-DELAY floor + residual spread,
//                   = p5 and (p95-p5) of (recvTs-eventTs) (lib/skew.q). NOT a clock offset:
//                   floor = minTransport + (localClock - venueClock), TRANSPORT-DOMINATED
//                   (e.g. Binance ~122ms = AWS-Tokyo RTT, not clock — see skew.q VERDICT /
//                   G5 2026-06-15). A link-latency/connection-health signal; oneWayJitterMs
//                   also folds in Coinbase's ~50ms level2_batch quantization. Null until the
//                   leg has .cfg.skewMinSamples non-fallback samples.
//   fallbackPct   : share of G1-fallback samples (diffNs=0) in the skew window
feed_health:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  lastTickAgeMs:`float$(); ticksPerMin:`float$(); nAccepted:`long$(); nRejected:`long$();
  invalidPct:`float$(); oneWayFloorMs:`float$(); oneWayJitterMs:`float$(); fallbackPct:`float$());
