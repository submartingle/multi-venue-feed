// lib/leadlag.q — lead/lag follow-window matcher (.leadlag.*)
// Receives qualifying moves from feed (.leadlag.onMove), matches each move
// against recent moves on comparable sibling legs of the same root sym, and
// emits one leadlag_events row per matched (lead -> follow) pair. Move DETECTION
// lives in feed.q (B4); this module is matching + cooldown only (B5).
//
// Comparable legs are gated by .cfg.pairMode (A1): crossVenue | crossInst | both
// | all. Matching is pairwise within a comparison group; cooldown de-dups repeats
// of the same directed pair. Lag is measured on the configured clock (.cfg.lagClock,
// default eventTs — exchange time, immune to per-leg transport asymmetry; was recvTs
// under A2 before every leg had a trustworthy exchange clock). Item 2, 2026-06-09.

// Config resolved once at load (safe defaults if config/params.q absent).
.leadlag.followWin:`timespan$`long$1e6*@[value; `.cfg.followWindowMs; 500f];
.leadlag.cooldown :`timespan$`long$1e6*@[value; `.cfg.cooldownMs;    1000f];
.leadlag.bufRetain:`timespan$`long$1e6*@[value; `.cfg.bufferRetainMs;2000f];
.leadlag.pairMode :@[value; `.cfg.pairMode; `crossVenue];
.leadlag.lagClock :@[value; `.cfg.lagClock; `eventTs];
// Dead-band is read DYNAMICALLY from .skew.deadBandNs at match time (Item 5 slice 2):
// in `fixed` mode it equals .cfg.deadBandMs; in `auto` mode the timer raises it to the
// measured noise floor. .skew.q initialises it to the floor, so a value always exists.

// Stamp the chosen lead-lag clock onto a moves table as clkTs (the column all the
// time logic below operates on). recvTs/eventTs are carried for diagnostics.
//   recvTs     : local clock (A2);  eventTs : raw exchange time (G2);
//   calEventTs : eventTs + per-leg clock-skew offset (Item 5) — cross-venue.
.leadlag.stampClk:$[
  .leadlag.lagClock=`recvTs;     {[moves] update clkTs:recvTs from moves};
  .leadlag.lagClock=`calEventTs; {[moves] update clkTs:.skew.calClk moves from moves};
  {[moves] update clkTs:eventTs from moves}];

// Comparability predicate (lead leg vs follow leg, same sym already guaranteed),
// resolved once from pairMode. bv/bi = buffer (lead) venue/inst; mv/mi = move
// (follow) venue/inst. Vectorised over the buffer side.
.leadlag.sib:$[
  .leadlag.pairMode=`crossVenue; {[bv;bi;mv;mi] (bi=mi) and not bv=mv};
  .leadlag.pairMode=`crossInst;  {[bv;bi;mv;mi] (bv=mv) and not bi=mi};
  .leadlag.pairMode=`both;       {[bv;bi;mv;mi] ((bi=mi)and not bv=mv) or ((bv=mv)and not bi=mi)};
  .leadlag.pairMode=`all;        {[bv;bi;mv;mi] (not bv=mv) or not bi=mi};
  '"unknown .cfg.pairMode: ",string .leadlag.pairMode];

// Rolling buffer of recent moves (each is a potential future lead). clkTs is the
// configured lead-lag clock (recvTs/eventTs carried alongside for diagnostics).
.leadlag.buf:([] sym:`symbol$(); venue:`symbol$(); inst:`symbol$();
  clkTs:`timestamp$(); recvTs:`timestamp$(); eventTs:`timestamp$();
  moveBps:`float$(); direction:`symbol$());

// Last emission time per directed pair — drives cooldown/de-dup (B5).
.leadlag.cool:([sym:`symbol$(); leadVenue:`symbol$(); leadInst:`symbol$();
  followVenue:`symbol$(); followInst:`symbol$(); direction:`symbol$()] lastTs:`timestamp$());

// Clear engine state (test helper / cold restart).
.leadlag.reset:{[] .leadlag.buf:0#.leadlag.buf; .leadlag.cool:0#.leadlag.cool;};

// Match a single move (a row dict from onMove). Find comparable in-window leaders,
// emit surviving pairs, then add this move to the buffer.
.leadlag.match:{[m]
  curT:m`clkTs;
  db:`timespan$.skew.deadBandNs;             // effective dead-band (fixed value or live-measured)
  // clock eviction of stale buffer entries
  .leadlag.buf:select from .leadlag.buf where clkTs>=curT-.leadlag.bufRetain;
  // candidate leaders: earlier, in-window, same sym & direction, comparable leg, and
  // far enough ahead to clear the dead-band (gap in [deadBand, followWin] — a gap below
  // the noise floor is a coincident co-move, not a lead).
  leaders:select from .leadlag.buf where sym=m`sym, direction=m`direction,
    clkTs<curT, clkTs>=curT-.leadlag.followWin, (curT-clkTs)>=db,
    .leadlag.sib[venue; inst; m`venue; m`inst];
  if[count leaders;
    // one candidate event per leader (lead = buffer leg, follow = this move)
    cand:select sym, leadVenue:venue, leadInst:inst,
      followVenue:m`venue, followInst:m`inst, direction,
      leadClkTs:clkTs, leadRecvTs:recvTs, leadEventTs:eventTs, moveSizeBps:moveBps from leaders;
    // cooldown: drop directed pairs last emitted within .leadlag.cooldown
    lastTs:(.leadlag.cool select sym,leadVenue,leadInst,followVenue,followInst,direction from cand)`lastTs;
    keep:cand where (null lastTs) | (curT-lastTs)>=.leadlag.cooldown;
    if[count keep;
      ev:select ts:curT, sym, leadVenue, leadInst, followVenue, followInst, direction,
        lagMs:1e-6*`long$(curT-leadClkTs),
        moveSizeBps,
        clockSkewMs:1e-6*`long$((leadEventTs-leadRecvTs)-(m[`eventTs]-m`recvTs))
        from keep;
      `leadlag_events insert ev;
      .log.event ev;                       // durable event log (lead/lag/lagMs...)
      `.leadlag.cool upsert select sym,leadVenue,leadInst,followVenue,followInst,direction,
        lastTs:curT from keep;
     ];
   ];
  // this move becomes a potential lead for later followers
  `.leadlag.buf insert `sym`venue`inst`clkTs`recvTs`eventTs`moveBps`direction#m;
  };

// Entry point (called by feed): match a batch of qualifying moves in clock order.
.leadlag.onMove:{[moves]
  if[count moves; .leadlag.match each `clkTs xasc .leadlag.stampClk moves];
  };
