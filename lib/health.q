// lib/health.q — per-leg feed-health diagnostics (.health.*)
// Track B / B2. Watches the feed PLUMBING, not the market: a leg that has gone silent
// (dead/stuck feed), a leg whose ticks are being rejected by the C11 validity filter,
// and a leg whose exchange clock cannot be calibrated (G1 fallback). Distinct from B1
// (lib/alert.q), which is move-based: B1 flags a leg that failed to reprice around a
// sibling's move; B2 flags a leg whose feed is broken regardless of any move — the
// absolute quote-age flavour deliberately excluded from B1 (OQ-7 question 2).
//
// Outputs, both produced by .health.calc on the timer (C8), off the hot path:
//   feed_health : per-leg metrics snapshot, REBUILT (replaced) each tick — a view.
//   alerts      : threshold breaches, EDGE-TRIGGERED — an alert fires when a condition
//                 transitions healthy->unhealthy and re-arms when it recovers, so one
//                 episode = one alert (no per-tick spam during a long outage, but a
//                 recurrence after recovery fires again). This deliberately differs
//                 from B1's once-per-session de-dup: feed faults recover and recur.
//
// Hot-path contract: feed.upd calls .health.record once per batch with the validity
// mask — one small grouped count append, no judgement. All evaluation happens here.
//
// Monitored universe = legs that have ever delivered an ACCEPTED tick (.feed.leg —
// same property as B1: a leg that never ticked at all is invisible; catching a
// configured-but-never-arrived feed is a process-level concern, not a leg metric).

// Config resolved at load.
.health.window     :@[value; `.cfg.healthWindow;      0D00:01:00.000000000];
.health.quietNs    :`long$1e6*@[value; `.cfg.healthQuietMs;    30000f];
.health.invalidPct :@[value; `.cfg.healthInvalidPct;  0.05];
.health.minTicks   :@[value; `.cfg.healthMinTicks;    50];
.health.fallbackPct:@[value; `.cfg.healthFallbackPct; 0.2];
// Alert msg symbols, fixed per type+config -> low-cardinality by construction (interned
// symbols live forever). Built here, not inline in the select (a comma inside a select
// phrase is a COLUMN separator -> 'length).
.health.msgQuiet   :`$"no_accepted_tick_",string[`long$.health.quietNs div 1000000],"ms";
.health.msgInvalid :`$"invalid_share_above_",string .health.invalidPct;
.health.msgFallback:`$"g1_fallback_share_above_",string .health.fallbackPct;

// Per-batch tick counts (accepted/rejected per leg), appended by .health.record on the
// hot path and evicted to .health.window by .health.calc on the timer (append-only on
// record, same lifecycle as .skew.buf).
.health.cnt:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  nAcc:`long$(); nRej:`long$());

// Currently-active alert conditions (the edge-trigger state): a condition row is in
// here iff it fired and has not yet recovered. Replaced wholesale by .health.calc.
.health.state:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  alertType:`symbol$(); msg:`symbol$());

// Clear all state (test helper / cold restart).
.health.reset:{[] .health.cnt:0#.health.cnt; .health.state:0#.health.state;};

// Record one ingest batch's per-leg accepted/rejected counts (called by feed.upd with
// the full batch d and its C11 validity mask, BEFORE the invalid rows are dropped —
// rejected ticks still attribute to their leg). ts = batch arrival (max recvTs), so
// offline replays evict deterministically on replayed time, not wall clock.
.health.record:{[d;valid]
  if[not count d; :()];
  bts:max d`recvTs;
  c:0!select nAcc:sum v, nRej:sum not v by sym,venue,inst
    from ([] sym:d`sym; venue:d`venue; inst:d`inst; v:valid);
  `.health.cnt insert select ts:bts, sym,venue,inst,nAcc,nRej from c;
  };

// Rebuild the feed_health snapshot and fire/clear edge-triggered alerts as of `curT`
// (timer, C8). Reads .feed.leg (last accepted tick), .health.cnt (window tick counts)
// and the .skew state (.skew.calc runs earlier on the same timer, so its buffer is
// already evicted to the skew window and .skew.stats is fresh).
.health.calc:{[curT]
  .health.cnt:select from .health.cnt where ts>=curT-.health.window;
  legs:select sym,venue,inst,lastRecvTs:recvTs from 0!.feed.leg;
  if[not count legs; :()];
  h:update lastTickAgeMs:1e-6*`long$curT-lastRecvTs from legs;
  h:h lj select nAccepted:sum nAcc, nRejected:sum nRej by sym,venue,inst from .health.cnt;
  h:update nAccepted:0^nAccepted, nRejected:0^nRejected from h;
  // invalidPct null (not 0) when the window is empty — "no data" is not "healthy"
  h:update invalidPct:?[n>0; nRejected%n; 0n],
           ticksPerMin:nAccepted%(`long$.health.window)%60e9
    from update n:nAccepted+nRejected from h;
  // link latency per (venue,inst): one-way-delay floor/jitter from .skew.stats (null while
  // uncalibrated). NOT clock skew — it is p5 / (p95-p5) of (recvTs-eventTs) = minTransport +
  // (localClock-venueClock), transport-dominated (skew.q VERDICT). A connection-health signal.
  // fallback share straight from the sample buffer (diffNs=0 = the G1 eventTs:=recvTs
  // signature, the same rows .skew.calc excludes from calibration).
  h:h lj select fallbackPct:avg 0=diffNs, nSkew:count i by venue,inst from .skew.buf;
  h:h lj .skew.stats;
  h:update oneWayFloorMs:1e-6*offsetNs, oneWayJitterMs:1e-6*jitterNs from h;
  feed_health::select ts:curT, sym,venue,inst, lastTickAgeMs, ticksPerMin,
    nAccepted, nRejected, invalidPct, oneWayFloorMs, oneWayJitterMs, fallbackPct from h;
  // --- alert conditions ------------------------------------------------------------
  cur:select sym,venue,inst, alertType:`feedstale, msg:.health.msgQuiet
      from h where lastTickAgeMs>1e-6*.health.quietNs;
  cur,:select sym,venue,inst, alertType:`invalid, msg:.health.msgInvalid
      from h where invalidPct>.health.invalidPct, (nAccepted+nRejected)>=.health.minTicks;
  // fallback share is a per-(venue,inst) property (skew pools symbols per leg) -> one
  // alert per leg with null sym, not one per symbol. Gated on the calibration's own
  // trust threshold (skewMinSamples) so a cold-start handful of fallback rows doesn't
  // read as a 100%-fallback episode.
  cur,:select sym:`$"", venue, inst, alertType:`clockfallback, msg:.health.msgFallback
      from distinct select venue,inst,fallbackPct,nSkew from h
      where fallbackPct>.health.fallbackPct, nSkew>=.skew.minSamples;
  // --- edge trigger: fire on onset, log recovery, state := current active set -------
  rowKey:{[t] flip `sym`venue`inst`alertType!t `sym`venue`inst`alertType};
  onset:cur where not rowKey[cur] in rowKey .health.state;
  reco :.health.state where not rowKey[.health.state] in rowKey cur;
  fmt:{[t] exec ", " sv/: flip string (sym;venue;inst;alertType;msg) from t};
  if[count onset;
    `alerts insert select ts:curT, sym,venue,inst,alertType,msg from onset;
    {.log.warn "feed-health ",x} each fmt onset];
  if[count reco; {.log.info "feed-health recovered ",x} each fmt reco];
  .health.state:cur;
  };
