// lib/movedetect.q — .move.* generic threshold move-detection.
//
// PROBLEM this solves: the live engine (lib/feed.q) detects a "move" only when ONE
// adjacent-tick return clears the threshold. A burst of many sub-threshold steps all in
// one direction (a fast ramp) is missed entirely, even though the market clearly moved —
// and because tick granularity differs by venue (Coinbase ~50ms-batched vs OKX/Binance
// tick-by-tick), the same price path yields DIFFERENT move counts per venue, biasing the
// cross-venue lead-lag. This library provides path-based detectors that fix both.
//
// GENERIC / ASSET-CLASS AGNOSTIC: operates on a bare (t; v) series — t = ascending
// timestamps (ns), v = any numeric series (crypto mid, equity mid, FX rate, a spread, a
// yield…). The return BASIS is selectable so it ports across asset classes:
//   `rel : relative return (v2-v1)/v1      — equities/crypto/FX prices (thr in fraction)
//   `abs : absolute difference  v2-v1      — rates/spreads/ticks       (thr in those units)
//   `log : log return  log(v2/v1)          — when compounding matters
//
// FIVE detector modes (each returns the fired events as `idx`dir`size!(...)):
//   `tickwise   : adjacent-tick return >= thr            — today's engine behaviour (baseline)
//   `cumulative : CUSUM from the last fired event's value — accumulate until |Δ|>=thr, fire,
//                 reset baseline to current. Catches sub-threshold ramps; a lone >=thr jump
//                 still fires once (subsumes tickwise). Single param. (Price analog of the
//                 Part B signed-flow imbalance bars in sim/trade_leadlag.q.)
//   `windowed   : return over a trailing winNs window >= thr — "moved >=thr within x ms";
//                 ignores slow drift; consecutive same-direction exceedances are collapsed
//                 to one event.
//   `cumstale   : cumulative, but the accumulator resets (no fire) if it stays open longer
//                 than winNs — i.e. require the cumulative move to complete WITHIN x ms.
//                 Catches fast bursts AND ignores slow drift (two params: thr, winNs).
//   `winreset   : >=thr drawup/drawdown from the trailing-window EXTREME, reset only on a fire
//                 — fires AT the move's completion tick (correct lead-lag timing) and catches a
//                 swing wherever it starts in the window; no straddle loss. PRODUCTION DEFAULT
//                 (B8). Replaces cumstale (resolves OQ-10). Two params: thr, winNs.
//
// Pure functions, no globals beyond the .move.* definitions, no load side effects.

// --- return basis (the asset-class generalizer) -----------------------------
.move.d:`rel`abs`log!(
  {[a;b] (b-a)%a};            // relative
  {[a;b] b-a};                // absolute
  {[a;b] log b%a});           // log

.move.empty:`idx`dir`size!(`long$();`short$();`float$());   // no-event result

// --- tickwise: adjacent-tick return (vectorised) ----------------------------
.move.tickwise:{[delta;thr;t;v]
  if[2>count v; :.move.empty];
  dd:delta[prev v; v];                          // first elem: prev=0n -> dd 0n -> never fires
  f:where thr<=abs dd;
  `idx`dir`size!(f; `short$signum dd f; dd f)};

// --- cumulative (CUSUM from last fired value) -------------------------------
// State carried by the scan: (base; dir; size). dir!=0 marks a fire at that tick.
.move.cumStep:{[delta;thr;st;x]
  dd:delta[st 0; x];
  $[thr<=abs dd; (x; signum dd; dd); (st 0; 0; 0n)]};
.move.cumulative:{[delta;thr;t;v]
  if[2>count v; :.move.empty];
  r:(.move.cumStep[delta;thr])\[(first v; 0; 0n); v];   // sequential: baseline only moves on a fire
  d:`short$r[;1];
  f:where d<>0;
  `idx`dir`size!(f; d f; r[;2] f)};

// --- windowed: return over a trailing winNs window --------------------------
// Collapse runs of consecutive same-direction exceedances into one event so the count is
// comparable to the once-per-crossing modes.
.move.dedup:{[d] where (d<>0) & d<>0,-1_d};      // indices where a new nonzero run begins
.move.windowed:{[delta;thr;winNs;t;v]
  if[2>count v; :.move.empty];
  j:0|t bin t-winNs;                             // baseline = tick at/just before window start
  dd:delta[v j; v];
  d:`short$?[thr<=abs dd; signum dd; 0];
  e:.move.dedup d;
  `idx`dir`size!(e; d e; dd e)};

// --- cumstale: cumulative bounded to winNs ----------------------------------
// State: (base; baseTime; dir; size). Reset (no fire) if the accumulation has been open
// longer than winNs; else accumulate and fire on |Δ|>=thr (resetting base+time).
.move.staleStep:{[delta;thr;winNs;st;tx]
  t:tx 0; x:tx 1;
  $[winNs < t - st 1;            (x; t; 0; 0n);                 // open too long -> drop, reset
    thr<=abs dd:delta[st 0; x];  (x; t; signum dd; dd);         // fire, reset base+time
                                 (st 0; st 1; 0; 0n)]};         // keep accumulating
.move.cumstale:{[delta;thr;winNs;t;v]
  if[2>count v; :.move.empty];
  r:(.move.staleStep[delta;thr;winNs])\[(first v; first t; 0; 0n); flip(t;v)];
  d:`short$r[;2];
  f:where d<>0;
  `idx`dir`size!(f; d f; r[;3] f)};

// --- winreset: drawup/drawdown from the window EXTREME, reset only on a fire --
// The correct "a >= thr move happened WITHIN the last winNs" detector. At each tick, over the
// trailing-winNs window, fire when price has risen >= thr from the window's running MIN (up) or
// fallen >= thr from its running MAX (down) — so the move is measured from the relevant extreme,
// not the window endpoints. This fires AT the tick the move completes (correct lead-lag timing)
// and catches a swing wherever it starts inside the window (no straddle loss; no endpoint masking
// — cf. the discussion that retired cumstale and the net-from-start variant). Reset (discard the
// chunk) ONLY on a confirmed move: the buffer collapses to the firing tick and accumulates fresh.
// Log basis: drawup = log(x/min), drawdown = log(max/x). Implemented with monotonic min/max deques
// (mnV keeps window-min candidates, increasing; mxV keeps window-max candidates, decreasing): pushing
// a tick prunes the dominated tail, eviction drops out-of-window fronts, so the carried state is a few
// (time,value) pairs. ONE resumable step (.move.wrvStep) drives both the batch detector here and the
// live per-leg stream in feed.q (seeded with the carried deques) — identical results by construction.
// State: (mnT;mnV;mxT;mxV;dir;size). On a fire, dir!=0 and the deques collapse to the firing tick.
.move.wrvStep:{[delta;thr;winNs; st; tx]
  t:tx 0; x:tx 1; lo:t-winNs;
  k:st[1]<x; mnT:(st[0] where k),t; mnV:(st[1] where k),x; mnT@:g:where mnT>=lo; mnV@:g; // min-deque
  k:st[3]>x; mxT:(st[2] where k),t; mxV:(st[3] where k),x; mxT@:g:where mxT>=lo; mxV@:g; // max-deque
  up:delta[first mnV; x];                              // rise from window low  (>=0)
  dn:delta[x; first mxV];                              // fall from window high (>=0 magnitude)
  $[thr<=up; (enlist t;enlist x;enlist t;enlist x;  1;     up);   // fire UP   -> reset deques to this tick
    thr<=dn; (enlist t;enlist x;enlist t;enlist x; -1; neg dn);   // fire DOWN -> reset deques to this tick
             (mnT;mnV;mxT;mxV; 0; 0n)]};                           // else carry the pruned deques
.move.wrSeed:(`long$();`float$();`long$();`float$();0;0n);   // empty deques: first tick self-seeds
.move.winreset:{[delta;thr;winNs;t;v]
  if[1>count v; :.move.empty];
  r:(.move.wrvStep[delta;thr;winNs])\[.move.wrSeed; flip(t;v)];
  d:`short$r[;4];
  f:where d<>0;
  `idx`dir`size!(f; d f; r[;5] f)};              // scan ran over all ticks -> f indexes v directly

// --- dispatcher + convenience ----------------------------------------------
// cfg: dict `mode`thr`basis`winNs (winNs only used by windowed/cumstale/winreset).
.move.detect:{[cfg;t;v]
  delta:.move.d cfg`basis;
  $[cfg[`mode]=`tickwise;   .move.tickwise  [delta; cfg`thr; t; v];
    cfg[`mode]=`cumulative; .move.cumulative[delta; cfg`thr; t; v];
    cfg[`mode]=`windowed;   .move.windowed  [delta; cfg`thr; cfg`winNs; t; v];
    cfg[`mode]=`cumstale;   .move.cumstale  [delta; cfg`thr; cfg`winNs; t; v];
    cfg[`mode]=`winreset;   .move.winreset  [delta; cfg`thr; cfg`winNs; t; v];
    '"unknown .move.mode: ",string cfg`mode]};

.move.count:{[cfg;t;v] count (.move.detect[cfg;t;v])`idx};       // just the event count
