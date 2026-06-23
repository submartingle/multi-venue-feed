// lib/imbalance.q — .imb.* streaming signed-aggressor-flow IMBALANCE-BAR detector.
//
// The TRADE-clock event source for the live trade engine (proc/live_trades.q), the streaming
// analog of the offline batch detector in sim/trade_leadlag.q. Per leg (sym,venue,inst), walk
// trades in trade_us order accumulating SIGNED aggressor notional (aggressive BUY +px*qty,
// SELL -px*qty); when the running sum crosses +thr emit an `up` (buy-pressure) event / -thr a
// `down`, resetting the accumulator (a Lopez-de-Prado imbalance-bar boundary). The per-leg
// accumulator is CARRIED across batches in .imb.state, so detection is continuous (identical to
// the offline single-pass scan by construction). Emits a moves table in the SAME shape the
// matcher (lib/leadlag.q) and scorer (lib/comove.q/score.q) consume — only direction is
// buy/sell PRESSURE rather than up/down PRICE, so all of E15/E16 is reused unchanged.
//
// thr is per-sym in USD (the SAME bar across a sym's venues — same asset/imbalance unit), set in
// config/trade_cal.q (.cfg.tradeBarUsd), fallback .cfg.tradeBarUsdDefault. Pure: no globals beyond
// .imb.* and no load side effects besides defining them.

// --- config (safe defaults; config/trade_cal.q overrides) -------------------
.cfg.tradeBarUsd       :@[value; `.cfg.tradeBarUsd;        (`symbol$())!`float$()];
.cfg.tradeBarUsdDefault:@[value; `.cfg.tradeBarUsdDefault; 300f];
.imb.thr:{[s] t:.cfg.tradeBarUsd s; $[null t; .cfg.tradeBarUsdDefault; t]};   // per-sym USD bar

// per-leg running signed-notional accumulator (carried across batches)
.imb.state:([sym:`symbol$(); venue:`symbol$(); inst:`symbol$()] acc:`float$());
.imb.reset:{[] .imb.state:0#.imb.state;};

// empty moves template (the .comove/.leadlag input shape)
.imb.mv0:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  recvTs:`timestamp$(); eventTs:`timestamp$(); moveBps:`float$(); direction:`symbol$());

// CUSUM imbalance-bar step: carry st=(acc;dir); add signed notional v; emit +/-thr crossing
// (reset acc to 0, dir +/-1), else accumulate (dir 0).
.imb.scan:{[thr;st;v] a:st[0]+v; $[a>=thr;(0f;1); a<=neg thr;(0f;-1); (a;0)]};

// detect one leg's bars. row = one (sym,venue,inst) group: list cols sgn/rTs/eTs (trades in
// trade_us order) + carried scalar acc (null for a new leg). Returns (moves; newStateRow).
.imb.leg:{[row]
  acc0:$[null row`acc; 0f; row`acc];
  r:(.imb.scan[.imb.thr row`sym])\[(acc0;0); row`sgn];     // (acc;dir) per trade; seed carries acc0
  d:`long$r[;1]; f:where d<>0;                              // fired-bar indices
  mv:([] sym:count[f]#row`sym; venue:count[f]#row`venue; inst:count[f]#row`inst;
        recvTs:(row`rTs) f; eventTs:(row`eTs) f; moveBps:count[f]#0n;
        direction:?[0<d f; `up; `down]);
  (mv; (row`sym; row`venue; row`inst; last r[;0])) };       // moves + carried accumulator

// ingest a batch of enriched trades (sym,venue,inst,recvTs,eventTs,signed). Returns the
// fired imbalance-bar moves; advances .imb.state in place.
.imb.detect:{[d]
  if[not count d; :.imb.mv0];
  d:`eventTs xasc d;                                        // trade-clock order
  legs:0!select sgn:signed, rTs:recvTs, eTs:eventTs by sym,venue,inst from d;
  legs:legs lj .imb.state;                                  // attach carried acc (null if new leg)
  out:.imb.leg each legs;
  `.imb.state upsert flip `sym`venue`inst`acc!flip out[;1]; // persist new per-leg accumulators
  raze out[;0] };
