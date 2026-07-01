// config/params.q — all engine tunables in one place (.cfg.*)
// Edit here, not in the libraries. Defaults chosen for the MVP; revisit per
// the open questions in DECISIONS.md (B4/B5/B6/C-items).

// --- Qualifying move detection (B4) -----------------------------------------
// Threshold on |mid move| in bps for a tick to qualify as a move.
// Per-sym override dict; falls back to the default for any sym not listed.
.cfg.moveThresholdDefault:2f;          // bps
.cfg.moveThresholdBps:(`symbol$())!`float$();   // e.g. `BTC`AAPL!1.0 0.5
// Global multiplier applied on top of the (vol-adaptive, recal-set) per-asset bar — raise to
// e.g. 2 to capture ONLY larger, more significant moves while keeping the bar vol-tracking
// (recal keeps updating moveThresholdBps; this rides on top). 1 = no change; fully reversible.
.cfg.moveThresholdMult:1f;
// Resolve the threshold for a sym (override else default), scaled by the global multiplier.
.cfg.threshold:{[s] t:.cfg.moveThresholdBps s; .cfg.moveThresholdMult*$[null t; .cfg.moveThresholdDefault; t]};

// Detector mode (lib/feed.q). `tickwise = single adjacent-tick return >= thr (legacy default,
// granularity-biased across venues). `cumstale = cumulative cross within moveWinMs (the mode the
// per-asset thresholds in config/move_cal.q were calibrated under; removes the granularity bias).
// Cross-venue procs set `cumstale. The window runs on eventTs (transport-immune).
// CAVEAT (verified 2026-06-15): cumstale subsumes tickwise ONLY when ticks are denser than the
// window; an isolated jump arriving after a >moveWinMs gap hits the stale-reset FIRST and is
// dropped. Negligible on dense live feeds (Binance ~12us, Coinbase ~50ms); matters only for thin
// assets/quiet periods. Open fork in DECISIONS (B7) on whether to make it a true superset.
.cfg.moveMode:`tickwise;
.cfg.moveWinMs:500f;                   // cumstale accumulation window (ms) — must match calibration
.cfg.moveBasis:`log;                   // return basis for the detector (log: additive over a ramp)

// --- Follow-window matching & cooldown (B5) ---------------------------------
.cfg.followWindowMs:500f;              // a follower must move within this window of the lead
.cfg.cooldownMs:1000f;                 // suppress repeat events on the same directed pair
.cfg.bufferRetainMs:2000f;             // how long qualifying moves stay in the match buffer

// --- Lead-lag clock + dead-band (Item 2, 2026-06-09) ------------------------
// Which timestamp drives cross-leg lag, now that every leg carries a trustworthy
// exchange clock (Binance spot estimated from @trade latency in the FH; perp and
// Coinbase native). Applies to BOTH the operational matcher (leadlag.q) and the
// score's directed-pair matrix (comove.q). See docs/LIVE_RUN_2026-06-09_FINDINGS.md §3-4.
//   `eventTs    : raw exchange time — immune to per-leg transport asymmetry, but each
//                 venue's exchange clock sits at a different offset from ours (default)
//   `calEventTs : eventTs aligned to a common frame via the per-leg clock-skew
//                 calibration (lib/skew.q, Item 5) — use for CROSS-VENUE lead-lag
//   `recvTs     : local receive time — legacy A2 behaviour (single-venue / fallback)
.cfg.lagClock:`eventTs;
// Dead-band: a lead/follow gap smaller than this is BELOW the feed's timing noise
// floor (path skew + ms-grained exchange stamps), so it is a coincident co-move,
// not a real lead. Such pairs are excluded from lead-lag events AND from the
// directional co-movement count. 0 disables (legacy behaviour).
// 100ms ~ a conservative quote-GRANULARITY floor (sits above the measured ~70ms jitter
// band). CORRECTED 2026-06-15 (see DECISIONS Amendment Log): the binding quote granularity
// is COINBASE's level2_batch (~50-60ms server-batched; live median BTC bookticker gap ~61ms)
// — NOT Binance, whose @bookTicker is real-time (live median ~12us gap, 79% sub-1ms; the old
// "Binance rate-limited at 100ms" note conflated bookticker with the @depth diff stream).
// OKX bbo-tbt ~20ms median. So sub-~60ms gaps are not resolvable on the Coinbase leg; 100ms
// stays a safe floor. This is a QUOTE-clock floor only — the trade clock has native per-trade
// timestamps and no such granularity floor, which is why Part B can probe below it.
// (Was 150 — pre-measurement hand-picked; lowered 2026-06-10 to open the majors' BTC/ETH
// window, which showed zero events at 150.)
.cfg.deadBandMs:100f;
// Dead-band mode (Item 5 slice 2):
//   `fixed : the dead-band is exactly .cfg.deadBandMs (the hand-picked default).
//   `auto  : the dead-band is DERIVED from the measured per-leg timing jitter
//            (lib/skew.q) and tracks it on the timer, with .cfg.deadBandMs as a FLOOR.
// Rationale: the right noise floor is whatever the feed's residual jitter actually is,
// not a guess. A pair's gap noise comes from BOTH legs' residual jitter, so the band is
// ~ sqrt(Ja^2+Jb^2) — conservatively sqrt2 * the worst leg's jitter. Opt-in: `fixed`
// keeps the existing pipeline unchanged until live data confirms the measured value.
.cfg.deadBandMode:`fixed;
// Upper percentile of (recvTs-eventTs) used to size each leg's jitter (paired with
// .cfg.skewPctile, the lower/floor percentile): jitter = pctile[hi] - pctile[lo].
.cfg.deadBandPctile:0.95;

// --- Cross-venue clock-skew calibration (Item 5 / G5, 2026-06-09) -----------
// Lag is measured on exchange time (eventTs), but each leg's exchange clock sits at
// a different systematic offset from our local clock: recvTs-eventTs has a per-leg
// FLOOR (Binance spot ~133ms vs Coinbase ~58ms in the 2026-06-09 run). Cross-venue
// lag = eventTs_follow - eventTs_lead therefore carries a systematic bias = the
// difference of those floors (~75ms), which (with the 150ms dead-band) tips every
// marginal pair toward the lower-floor venue. lib/skew.q estimates each leg's offset
// as a LOW percentile of (recvTs-eventTs) over a rolling window (the floor isolates
// the systematic clock+min-transport offset from jitter; a median would over-remove,
// pulling in median transport) and stamps calEventTs = eventTs + offset, putting every
// leg in a common local-equivalent frame. See DECISIONS.md §G G5.
//   NB (honest limit): a one-way delay measurement cannot fully separate a clock offset
//   from MINIMUM transport latency, so this removes the cross-venue bias down to the
//   difference in the two legs' min transport — best achievable one-way, smaller and
//   more stable than a median correction; the dead-band absorbs the residual.
.cfg.skewWindow:0D00:05:00.000000000;  // rolling window of recvTs-eventTs samples per leg
.cfg.skewPctile:0.05;                  // low percentile used as the offset (the floor)
.cfg.skewMinSamples:50;                // min samples before trusting a leg's offset (else 0)

// --- Cross-venue symbol normalization (Item 4, 2026-06-09) ------------------
// Venues quote the same asset under different symbols (Binance `BTCUSDT`, Coinbase
// `BTC-USD`). The RAW venue symbol is retained on the `bookticker` record (and the
// RDB) for traceability; the engine works on a CANONICAL root `sym` so comparable
// legs pair across venues. The map is EXPLICIT for transparency/maintenance; an
// unmapped symbol passes through raw (it simply won't cross-pair — a visible symptom,
// not a silent miscompare). NB: the USD/USDT quote-currency basis is a small constant
// price premium; the engine keys on move DIRECTION, not level, so it does not affect
// co-movement (document, don't correct).
.cfg.symCanon:(`$())!`$();
// Roots: the original 5 majors + 4 liquid alts (AVAX LINK ADA LTC) added 2026-06-13 for
// the OKX 3-venue capture (alts add power where the OQ-6 signal lives — DOGE/SOL; majors
// are floor-limited, see docs/OQ6_POOLED_BYHOUR_FINDINGS.md §5).
// Binance spot = <ROOT>USDT
.cfg.symCanon[`BTCUSDT`ETHUSDT`SOLUSDT`DOGEUSDT`XRPUSDT]:`BTC`ETH`SOL`DOGE`XRP;
.cfg.symCanon[`AVAXUSDT`LINKUSDT`ADAUSDT`LTCUSDT]:`AVAX`LINK`ADA`LTC;
// Coinbase spot = <ROOT>-USD
.cfg.symCanon[`$("BTC-USD";"ETH-USD";"SOL-USD";"DOGE-USD";"XRP-USD")]:`BTC`ETH`SOL`DOGE`XRP;
.cfg.symCanon[`$("AVAX-USD";"LINK-USD";"ADA-USD";"LTC-USD")]:`AVAX`LINK`ADA`LTC;
// OKX spot = <ROOT>-USDT (offshore/Asia leg, added for the geography-vs-Binance test)
.cfg.symCanon[`$("BTC-USDT";"ETH-USDT";"SOL-USDT";"DOGE-USDT";"XRP-USDT")]:`BTC`ETH`SOL`DOGE`XRP;
.cfg.symCanon[`$("AVAX-USDT";"LINK-USDT";"ADA-USDT";"LTC-USDT")]:`AVAX`LINK`ADA`LTC;
// Canonicalize raw venue symbol(s) -> root (vectorised); unmapped -> passthrough.
.cfg.canon:{[s] c:.cfg.symCanon s; ?[null c; s; c]};

// --- Pairing scope (A1, refined 2026-06-07) ---------------------------------
// Which sibling legs (same root sym) may be compared. See DECISIONS.md A1.
//   `crossVenue : same inst, different venue (e.g. Coinbase-spot vs Binance-spot)
//   `crossInst  : same venue, different inst (e.g. Binance-spot vs Binance-perp)
//   `both       : either of the above, but NOT both differing (clean spot+basis)
//   `all        : any different sibling leg (fully generic — includes mixed)
.cfg.pairMode:`crossVenue;

// --- Rolling score window ---------------------------------------------------
.cfg.scoreWindow:0D00:01:00.000000000; // 1-minute trailing window for leadership_score
.cfg.timerMs:1000i;                    // .z.ts cadence (score + alert recompute)
.cfg.snapshotMins:60;                  // how often to snapshot rolling summaries to file

// --- Cumulative session score (Item 3, 2026-06-09) --------------------------
// leadership_score is a short trailing-window snapshot (good for "who leads NOW",
// but tiny-n over a quiet window). leadership_session aggregates the SAME base-rate-
// corrected co-movement evidence over the whole session, so the leader call gains
// statistical power (n grows, Wilson CI tightens, no sign-flips on single-digit n).
// sessionRetain bounds the qualifying-move history kept for it (memory guard); the
// windowed score still only looks back .cfg.scoreWindow regardless.
.cfg.sessionRetain:0D12:00:00.000000000;   // 12h of qualifying moves retained for the session score
.cfg.ciZ:1.959964;                     // z for the leadership_score Wilson CI (E16); 1.96 = 95%

// --- Regime bucketing (B6) — fixed boundaries, composite label --------------
// Spread buckets in bps and volatility buckets in bps (stdev of mid moves).
.cfg.spreadTightBps:1f;                // <= tight, <= wide else `wide
.cfg.spreadWideBps:5f;
.cfg.volLowBps:5f;                     // stdev of recent mid moves: <= low else `high

// --- Logging (lib/log.q) ----------------------------------------------------
.cfg.logLevel:`info;                   // suppress level messages below this rank
// Captured lead->lag events are appended (one CSV row each) to a durable file,
// independent of the volatile in-memory leadlag_events table (C9).
.cfg.logEventsToFile:1b;               // master switch for the event log file
.cfg.eventLogFile:"logs/leadlag_events.log";   // path (parent dir auto-created)

// --- Alerts -----------------------------------------------------------------
// Feed-integrity: flag when a pair's observed lagMs exceeds mean + k*stdev.
.cfg.integrityK:3f;
// Stale-price: a lead move with no follower within this window flags the
// non-moving sibling leg as potentially stale.
.cfg.staleWindowMs:1000f;
// Re-arm cooldown for stale-follower de-dup: the same (leg, lead, direction) condition
// is suppressed only within this window, then may fire again as a new episode. Keeps the
// alert stream live (recurring staleness stays visible) instead of saturating at the
// finite combo universe (venues x leads x directions x syms) once per session.
.cfg.staleCooldownMs:300000f;          // 5 min between repeat alerts for the same condition

// --- Feed-health diagnostics (B2, lib/health.q) ------------------------------
// Per-leg feed plumbing health, distinct from B1's move-based stale-FOLLOWER rule:
// B1 says "this leg failed to reprice around a sibling's move"; B2 says "this leg's
// FEED is unhealthy" (silent, rejecting ticks, or clock-uncalibratable) regardless of
// any move. Metrics snapshot -> feed_health (rebuilt on the timer); threshold breaches
// -> `alerts`, EDGE-TRIGGERED (fire on onset, re-arm on recovery — one alert per
// episode; B1's stale-follower de-dup re-arms on a time cooldown instead, staleCooldownMs).
.cfg.healthWindow:0D00:01:00.000000000;  // trailing window for tick/invalid-rate metrics
.cfg.healthQuietMs:30000f;     // `feedstale: leg silent (no ACCEPTED tick) longer than this.
                               // On the L2 feeds every leg ticks multiple times/sec; 30s of
                               // silence is a dead/stuck feed, not a quiet market.
.cfg.healthInvalidPct:0.05;    // `invalid: share of C11-rejected ticks in the window above this
.cfg.healthMinTicks:50;        // ...but only once the window holds at least this many ticks
                               // (don't judge a rate on a handful of samples)
.cfg.healthFallbackPct:0.2;    // `clockfallback: share of G1-fallback skew samples (diffNs=0,
                               // the eventTs:=recvTs signature) above this — the leg's exchange
                               // clock is not being estimated (cf. the 4e suspend collapse,
                               // which ran at ~100% fallback undetected)
