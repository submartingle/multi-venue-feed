// lib/skew.q — cross-venue clock-skew calibration (.skew.*)   [DORMANT / opt-in — see VERDICT]
// Item 5 / G5, 2026-06-09; RE-EVALUATED 2026-06-15 and SUPERSEDED as a lag correction.
//
// ORIGINAL CONCERN (2026-06-09): lead-lag is measured on exchange time (eventTs, G2). The
// observable recvTs-eventTs has a per-leg FLOOR (Binance spot ~133ms vs Coinbase ~58ms in the
// 2026-06-09 run). The original design read that ~75ms floor GAP as a cross-venue CLOCK skew and
// corrected it: calEventTs = eventTs + O, O = low percentile of (recvTs-eventTs); lag then measured
// on calEventTs (.cfg.lagClock=`calEventTs).
//
// WHY THAT WAS WRONG — the lag bias is NOT the floor gap. Write eventTs = T + d (d = venue clock
// offset from UTC). Lag on eventTs:
//     raw lag = eventTs_F - eventTs_L = (T_F - T_L) + (d_F - d_L) = L + (d_F - d_L)
// Transport NEVER enters eventTs (stamped before the packet leaves), so the ONLY bias in raw-eventTs
// lead-lag is the CLOCK asymmetry d_F - d_L. The observable floor is a different quantity:
//     floor_v          = minTransport_v + d_local - d_v
//     floor_F - floor_L = (minTransport_F - minTransport_L) - (d_F - d_L)   // transport asym MINUS clock asym
// Equating the lag bias to the 75ms floor gap silently ASSUMES transport is symmetric — false. And
// calEventTs = eventTs + floor maps every leg to T + minTransport_v + d_local: it removes the clock
// term but LEAVES transport, giving  calibrated lag = L + (minTransport_F - minTransport_L). It trades
// a clock-asymmetry bias for a TRANSPORT-asymmetry bias — a win ONLY if clock asym > transport asym.
//
// MEASURED 2026-06-15 (out-of-band TCP-connect RTT to the WS endpoints; 1/2 RTT = one-way minTransport):
//     Binance spot/perp -> AWS Tokyo, DIRECT : RTT ~244ms -> ~122ms one-way ~= its ENTIRE 133ms floor.
//     Coinbase / OKX    -> Cloudflare edge   : RTT ~18-25ms -> ~9-12ms; +Coinbase ~50ms level2_batch ~= 58ms floor.
// => clock asymmetry is single-digit ms; the 75ms floor gap is ~113ms TRANSPORT asymmetry minus a small d.
//    So calEventTs INJECTS ~the transport asymmetry as a NEW bias -> NET HARMFUL. This is the mechanism
//    behind the long-observed "calEventTs flips everything to Coinbase-leads" artifact (G5 amend 06-15):
//    adding Binance's big floor shoves its events ~75ms later than Coinbase's -> Coinbase spuriously leads.
//
// VERDICT: production measures lag on RAW eventTs (.cfg.lagClock=`eventTs, the default — G2). calEventTs
// is kept opt-in and DORMANT: never applied unless lagClock=`calEventTs is explicitly set. What stays
// legitimately useful here is DIAGNOSTIC ONLY: per-leg floor/jitter telemetry (health.q) and, in
// deadBandMode=`auto, a data-driven dead-band floor — neither shifts the lag clock.
//
// HONEST LIMIT (now MEASURED, not assumed): one-way arrivals cannot split a constant clock offset from
// a constant min transport (NTP needs round-trips). The 2026-06-15 RTT probe supplies that missing
// out-of-band measurement and shows transport dominates, so O = floor ~= clockOffset + minTransport is
// mostly minTransport and adding it back is the wrong correction. Per-leg keyed on (venue,inst):
// spot-estimated and perp-native clocks differ even within one venue, so the leg (not the venue) is the
// calibration unit; symbols pooled per leg for a richer sample.
//
// Lifecycle: .skew.record (hot path, append-only, called by feed.upd over every accepted
// tick) accumulates samples; .skew.calc (timer, C8) evicts to the window and recomputes
// the offsets; .skew.calClk (hot path) stamps the calibrated clock onto a moves table.

// Config resolved once at load (safe defaults if config/params.q absent).
.skew.window    :@[value; `.cfg.skewWindow;     0D00:05:00.000000000];
.skew.pctile    :@[value; `.cfg.skewPctile;     0.05];
.skew.minSamples:@[value; `.cfg.skewMinSamples; 50];
// Dead-band (slice 2): derive the lead-lag noise floor from the measured jitter when
// .cfg.deadBandMode=`auto, else use the fixed .cfg.deadBandMs. Floor never goes below
// .cfg.deadBandMs (`deadBandFloorNs`). `deadBandPctile` is the upper percentile of
// (recvTs-eventTs) that, minus the floor percentile (`pctile`), gives a leg's jitter.
.skew.deadBandMode  :@[value; `.cfg.deadBandMode;   `fixed];
.skew.deadBandPctile:@[value; `.cfg.deadBandPctile; 0.95];
.skew.deadBandFloorNs:`long$1e6*@[value; `.cfg.deadBandMs; 100f];   // safe default matches config/params.q (NOT 0 — a 0 floor silently disables the noise floor)

// Rolling buffer of per-tick (recvTs-eventTs) samples, one row per accepted tick.
// diffNs carries the long ns difference directly (cheaper than re-subtracting). Evicted
// to .skew.window on the timer (NOT on record — append-only keeps the hot path cheap).
.skew.buf:([] venue:`symbol$(); inst:`symbol$(); recvTs:`timestamp$(); diffNs:`long$());

// Resolved per-leg offsets: keyed (venue,inst) -> offset (ns long). Empty until the
// first .skew.calc with enough samples; an unknown leg falls back to 0 (no shift =
// raw eventTs), exactly like a cold start.
.skew.off:([venue:`symbol$(); inst:`symbol$()] offsetNs:`long$());

// Per-leg offset + jitter as last computed by .skew.calc (same rows that drive .skew.off
// and the auto dead-band) — kept for diagnostics consumers (lib/health.q) so they don't
// re-derive the percentiles from the buffer.
.skew.stats:([venue:`symbol$(); inst:`symbol$()] offsetNs:`long$(); jitterNs:`long$());

// Effective dead-band (ns) read by the matcher (leadlag.q) and the score's directed
// matrix (comove.q). Initialised to the floor so consumers always have a value even
// before the first .skew.calc (cold start / sims that never run the timer). In `fixed`
// mode it stays at the floor; in `auto` mode .skew.calc raises it to the measured band.
// measuredNs is the raw measured band (observable in BOTH modes for diagnostics).
.skew.measuredNs:.skew.deadBandFloorNs;
.skew.deadBandNs:.skew.deadBandFloorNs;

// Clear all state (test helper / cold restart). Dead-band resets to the floor.
.skew.reset:{[] .skew.buf:0#.skew.buf; .skew.off:0#.skew.off; .skew.stats:0#.skew.stats;
  .skew.measuredNs:.skew.deadBandFloorNs; .skew.deadBandNs:.skew.deadBandFloorNs;};

// Low percentile of a vector (nearest-rank on the sorted values). p in [0,1].
.skew.pct:{[p;v] (asc v) floor p*-1+count v};

// Record samples from a batch of accepted ticks (called by feed.upd over all valid
// ticks, before move detection — every tick is a clock-skew sample, not just qualifying
// moves). Append-only; eviction happens in .skew.calc on the timer.
.skew.record:{[d]
  if[count d; `.skew.buf insert select venue, inst, recvTs, diffNs:`long$recvTs-eventTs from d];
  };

// Recompute per-leg offsets + the data-driven dead-band (timer, C8). Evict the sample
// buffer to the window, then per leg (with at least .skew.minSamples) compute:
//   offset = low percentile of (recvTs-eventTs)        -> the systematic clock floor
//   jitter = high percentile - low percentile          -> residual timing spread
// Offsets feed calEventTs; the worst leg's jitter sizes the dead-band: a coincident pair
// can be staggered by the COMBINED residual jitter of its two legs ~ sqrt(Ja^2+Jb^2),
// conservatively sqrt2 * the worst single leg. measuredNs is recorded in both modes; the
// effective dead-band is the floor in `fixed` mode, else max(floor, measured) in `auto`.
.skew.calc:{[]
  if[not count .skew.buf; :()];
  .skew.buf:select from .skew.buf where recvTs>=(max recvTs)-.skew.window;
  // diffNs=0 exactly is the G1 FALLBACK signature (FH stamped eventTs:=recvTs because its
  // own estimate wasn't trustworthy) — not a measurement. Including fallback rows poisons
  // both statistics: the offset floor is dragged to 0 and the leg's jitter reads ~0, making
  // the UNcalibrated leg look like the cleanest in the system (observed live 2026-06-09).
  // Excluded here, a fully-falling-back leg drops below minSamples -> no offset -> calClk
  // shifts it by 0, i.e. honest cold-start behaviour instead of a fake calibration.
  g:select n:count i, lo:.skew.pct[.skew.pctile] diffNs, hi:.skew.pct[.skew.deadBandPctile] diffNs
    by venue,inst from .skew.buf where diffNs<>0;
  g:select venue, inst, offsetNs:lo, jitterNs:hi-lo from g where n>=.skew.minSamples;
  .skew.off:`venue`inst xkey select venue,inst,offsetNs from g;
  .skew.stats:`venue`inst xkey g;
  .skew.measuredNs:$[count g; `long$(sqrt 2)*max g`jitterNs; .skew.deadBandFloorNs];
  .skew.deadBandNs:$[.skew.deadBandMode=`auto; .skew.deadBandFloorNs|.skew.measuredNs; .skew.deadBandFloorNs];
  };

// Calibrated lead-lag clock for a moves table: calEventTs = eventTs + per-leg offset.
// Vectorised; an uncalibrated leg (not yet in .skew.off) gets 0 -> raw eventTs.
.skew.calClk:{[m]
  r:m lj .skew.off;                                  // attach offsetNs (null where unknown)
  r[`eventTs] + `timespan$ 0^ r`offsetNs};           // null/absent -> 0 shift
