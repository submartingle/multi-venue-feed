// lib/alert.q — feed-health / execution-risk alerts (.alert.*)
// Track B / B1. Stale-follower detection (OQ-7, resolved → BRACKETED move-window): when a
// leg makes a qualifying move (the lead), any comparable sibling that made NO same-
// direction qualifying move within [t-staleWin, t+staleWin] of it is flagged `stale —
// i.e. the sibling failed to reprice around the move. Runs on the timer (C8), off the
// hot path; intended to populate the `alerts` table (C10).
//
// The window is BRACKETED (not forward-only) so a near-simultaneous co-mover — including
// the actual leader of a tightly-coupled pair — is counted as "reacting" and not flagged.
// A forward-only (t,t+staleWin] rule spuriously flags whichever leg moved first (verified
// in the slice-1 smoke test); bracketing fixes that. Detects "didn't reprice when a
// sibling moved"; it does NOT use absolute quote-age (that feed-health flavour is B2).
//
// This slice is PURE detection — returns the alert rows. The engine-wiring slice will
// insert into `alerts` + de-dup. Comparability reuses .leadlag.sib (pairMode, A1).

// Config resolved at load.
.alert.staleWin:`timespan$`long$1e6*@[value; `.cfg.staleWindowMs; 1000f];

// Empty alert template (matches schema/output.q `alerts`).
.alert.out0:([] ts:`timestamp$(); sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  alertType:`symbol$(); msg:`symbol$());

// Detect stale-follower alerts as of `asOf`, judging leads in (loTs, asOf-staleWin].
//   moves : qualifying-move history (sym,venue,inst,recvTs,direction) — .comove.hist shape
//   legs  : active comparable leg universe (sym,venue,inst) — e.g. 0!.feed.leg keys
//   loTs  : judge only leads STRICTLY AFTER this (null = no bound, full history). A lead
//           is judged exactly once — when its bracket closes — by advancing loTs; judging
//           ALL mature leads against the full history every tick is O(moves^2) and
//           collapsed the bridge at ~20k session moves (2026-06-11, two stalls).
//   asOf  : current time. Only MATURE leads (recvTs<=asOf-staleWin) are judged, so the
//           follow window has fully elapsed and a non-follow is real, not just pending.
// Returns rows shaped like `alerts`.
.alert.stale:{[moves; legs; loTs; asOf]
  if[0=count moves; :.alert.out0];
  // mature qualifying leads in the not-yet-judged window
  ld:select sym, leadVenue:venue, leadInst:inst, leadRecvTs:recvTs, direction
    from moves where recvTs<=asOf-.alert.staleWin, (null loTs)|recvTs>loTs;
  if[0=count ld; :.alert.out0];
  // expand each lead against comparable sibling legs of the same sym
  c:ej[`sym; ld; select sym, followVenue:venue, followInst:inst from legs];
  c:select from c where .leadlag.sib[leadVenue;leadInst;followVenue;followInst];
  if[0=count c; :.alert.out0];
  // a sibling is stale if it made NO same-direction move in [t-staleWin, t+staleWin]
  // (bracketed: a near-simultaneous co-mover, incl. the leader, counts as reacting).
  hasReacted:{[m;r] 0<count select from m where sym=r`sym, venue=r`followVenue,
    inst=r`followInst, direction=r`direction,
    recvTs>=r[`leadRecvTs]-.alert.staleWin, recvTs<=r[`leadRecvTs]+.alert.staleWin};
  s:c where not hasReacted[moves] each c;
  if[0=count s; :.alert.out0];
  // one alert per stale (sibling, lead-move). msg kept LOW-cardinality (leg+dir) to
  // avoid symbol-interning bloat — high-cardinality strings interned as symbols live
  // forever (CLAUDE.md s6); detailed context belongs in dedicated columns, not msg.
  select ts:asOf, sym, venue:followVenue, inst:followInst, alertType:`stale,
    msg:`$ {x,"/",y,"_",z}'[string leadVenue; string leadInst; string direction]
    from s };

// Watermark: leads at or before this are already judged (advanced each .alert.calc).
.alert.lastJudged:0Np;

// Insert new stale-follower alerts into `alerts`, de-duplicating by
// (sym,venue,inst,alertType,msg) so the same condition doesn't re-fire every timer tick.
// INCREMENTAL: each tick judges only the leads whose bracket closed since the last tick,
// against only the moves that can fall in those brackets — O(new moves) per tick, where
// the previous full-history form was O(moves^2) and ground the bridge to ~1% throughput
// at ~20k session moves.
.alert.calc:{[windowEnd]
  hi:windowEnd-.alert.staleWin;
  if[not null .alert.lastJudged; if[hi<=.alert.lastJudged; :()]];
  // moves that can react to a lead in (lastJudged, hi]: bracket reaches staleWin past
  // either end (null watermark = cold start: judge the full history once)
  m:$[null .alert.lastJudged; .comove.hist;
      select from .comove.hist where recvTs>.alert.lastJudged-.alert.staleWin];
  r:.alert.stale[m; 0!.feed.leg; .alert.lastJudged; windowEnd];
  .alert.lastJudged:hi;
  if[0=count r; :()];
  // row-wise membership on the identity columns — NOT a keyed lookup: keying all
  // columns of a table throws 'length (zero-column value part), which aborted the
  // whole .z.ts chain on first live activation (2026-06-11)
  k:`sym`venue`inst`alertType`msg;
  r:r where not (k#r) in k#alerts;
  r:distinct r;   // two same-tick lead-moves can emit identical rows (ts:asOf is shared)
  if[count r; `alerts insert r];
  };
