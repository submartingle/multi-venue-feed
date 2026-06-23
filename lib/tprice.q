// lib/tprice.q — trade-PRICE winreset detector (.tprice.*)
// The price-based trade-clock event model — the complement to the signed-flow imbalance bars
// in lib/imbalance.q ("combine both", 2026-06-18). It runs the PRODUCTION winreset detector
// (lib/movedetect.q .move.wrvStep) on the TRADE-PRINT price series per leg, on the clean native
// trade clock (trade_us, real on all three spot venues). Two properties the flow model lacks:
//   - RETAINS price levels: the fired event carries the actual execution price (mid column),
//     so you can see at what price the move completed (flow imbalance is level-blind).
//   - PATH-robust to bid-ask bounce: winreset measures a >=thr swing from the trailing-window
//     EXTREME, so a lone print bouncing across the spread does not clear the bar (unlike an
//     adjacent-tick trade-return detector, which eats the bounce).
//
// Self-contained: its own per-leg deque state (.tprice.wstate), SEPARATE from the quote winreset
// state (.feed.wstate) — the two streams never cross-contaminate. Reuses the shared, tested
// .move.wrvStep, so the swing logic is identical to the quote path by construction (verified in
// sim/verify_tprice.q). Emits the standard moves shape that .comove.record / .leadlag.onMove
// already consume, so the scoring machinery is unchanged.

// --- config (config-at-load; safe defaults if config/*.q absent) ------------
// Per-sym threshold in bps + trailing window (ms). Trade prints bounce across the spread, so this
// bar is tuned independently of the quote bar (.cfg.threshold); calibration is a later step.
.cfg.tpriceThrBps       :@[value; `.cfg.tpriceThrBps;        (`symbol$())!`float$()];
.cfg.tpriceThrBpsDefault:@[value; `.cfg.tpriceThrBpsDefault; 5f];                       // bps
.cfg.tpriceWinMs        :@[value; `.cfg.tpriceWinMs;         @[value;`.cfg.moveWinMs;500f]];

.tprice.thr:{[s] t:.cfg.tpriceThrBps s; 1e-4*$[null t; .cfg.tpriceThrBpsDefault; t]};   // bps -> fraction
.tprice.winNs:`long$1e6*.cfg.tpriceWinMs;
.tprice.delta:.move.d`log;                              // log basis, as the quote winreset (B8)

// --- per-leg carried winreset deques (separate from .feed.wstate) -----------
.tprice.wstate:([sym:`symbol$(); venue:`symbol$(); inst:`symbol$()]
  mnT:(); mnV:(); mxT:(); mxV:());
.tprice.reset:{[] .tprice.wstate:0#.tprice.wstate;};

// Empty moves template (standard shape; mid carries the trade-print price).
.tprice.mv0:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  recvTs:`timestamp$(); eventTs:`timestamp$(); mid:`float$(); moveBps:`float$(); direction:`symbol$());

// Per-leg streaming step: fold this leg's new trade prints through the shared .move.wrvStep,
// seeded from the carried deques (empty/cold -> .move.wrSeed). Mirrors .feed.wrLeg exactly.
.tprice.leg:{[row]
  clk:row`clk; v:row`v;
  mnV0:row`mnV0;                                        // cold start if no real carried deque (lj-null/empty)
  seed:$[(0=count mnV0) or not 9h=type mnV0; .move.wrSeed; (row`mnT0; mnV0; row`mxT0; row`mxV0; 0; 0n)];
  r:(.move.wrvStep[.tprice.delta; .tprice.thr row`sym; .tprice.winNs])\[seed; flip(clk; v)];
  d:`long$r[;4]; f:where d<>0; lastS:r[(count r)-1];    // fires + final deque state
  `sym`venue`inst`recvTs`eventTs`mid`moveBps`direction`mnT`mnV`mxT`mxV!
    (row`sym; row`venue; row`inst; row[`rTs] f; row[`eTs] f; v f;
     10000*r[f;5]; ?[0<d f;`up;`down]; lastS 0; lastS 1; lastS 2; lastS 3) };

// Detect qualifying trade-price moves over a normalized trade batch `d`
// (cols: sym,venue,inst,recvTs,eventTs,price). eventTs = trade_us (native trade clock); the
// winreset window and the lead-lag measurement both run on it. Advances .tprice.wstate; returns
// the standard moves table (mid = the trade-print price at the completion tick).
.tprice.detect:{[d]
  if[not count d; :.tprice.mv0];
  d:`recvTs xasc d;
  d:d lj `sym`venue`inst xkey select sym,venue,inst,mnT,mnV,mxT,mxV from 0!.tprice.wstate;  // carried deques
  g:0!select clk:`long$eventTs, v:price, rTs:recvTs, eTs:eventTs,
       mnT0:first mnT, mnV0:first mnV, mxT0:first mxT, mxV0:first mxV by sym,venue,inst from d;
  res:.tprice.leg each g;                               // one row (dict) per leg
  moves:`recvTs xasc ungroup select sym,venue,inst,recvTs,eventTs,mid,moveBps,direction from res;
  `.tprice.wstate upsert `sym`venue`inst xkey select sym,venue,inst,mnT,mnV,mxT,mxV from res;  // advance deques
  moves };
