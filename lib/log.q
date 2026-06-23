// lib/log.q — logging infrastructure (.log.*)
// Two responsibilities, one namespace:
//   1. Level logging (debug/info/warn/error) to stderr for process diagnostics.
//   2. A durable leadlag-event log: appends one CSV row per captured lead->lag
//      event to a file, so events survive restarts and can be tail -f'd or
//      reloaded into q — independent of the volatile in-memory leadlag_events
//      table (C9).
// Self-contained: config is resolved at load with safe defaults, so this file
// loads and runs even if config/params.q is absent (CLAUDE.md §12).

// Resolve config once at load: .cfg.* if present (config/params.q), else a safe
// default — no runtime getter. Set .cfg.* before loading this file to override.
.log.level    :@[value; `.cfg.logLevel;        `info];
.log.toFile   :@[value; `.cfg.logEventsToFile; 1b];
.log.eventFile:@[value; `.cfg.eventLogFile;    "logs/leadlag_events.log"];

// --- Level logging ----------------------------------------------------------
// Severity ranks; messages below .cfg.logLevel are dropped.
.log.levels:`debug`info`warn`error!0 1 2 3;

// One log line: UTC timestamp (.z.p — CLAUDE.md §10), level, message.
.log.fmt:{[lvl;msg] "  " sv (string .z.p; upper string lvl; msg)};

// Core emitter -> stderr (-2) so diagnostics never pollute stdout / query output.
.log.out:{[lvl;msg]
  if[.log.levels[lvl] >= .log.levels .log.level;
     -2 .log.fmt[lvl; msg]];
  };

.log.debug:{[msg] .log.out[`debug; msg]};
.log.info :{[msg] .log.out[`info ; msg]};
.log.warn :{[msg] .log.out[`warn ; msg]};
.log.error:{[msg] .log.out[`error; msg]};

// --- Leadlag event log (file) ----------------------------------------------
// CSV columns mirror schema/output.q:leadlag_events —
//   ts, sym, direction,
//   leadVenue,leadInst     -> the LEAD market,
//   followVenue,followInst -> the LAG (following) market,
//   lagMs                  -> the time lag (recvTs[follow]-recvTs[lead], A2),
//   moveSizeBps, clockSkewMs.
.log.eventHeader:"ts,sym,direction,leadVenue,leadInst,followVenue,followInst,lagMs,moveSizeBps,clockSkewMs";

.log.eventH:0Ni;   // cached append-mode file handle; null until opened

// Open (lazily create + header) the event log and cache the handle. No-op when
// file logging is disabled. Ensures the parent directory exists first.
.log.openEventLog:{[]
  if[not .log.toFile; :()];
  p:.log.eventFile;                         // string path
  i:last where "/"=p;                       // index of final '/', null if none
  if[not null i; system "mkdir -p ",i#p];   // ensure parent dir exists
  f:hsym `$p;                               // `:path file handle
  newFile:0=@[hcount; f; 0];                // missing/empty -> needs header
  h:hopen f;                                // hopen on a file = append mode
  if[newFile; h .log.eventHeader,"\n"];
  .log.eventH:h;
  h};

// Append captured events to the log. `evs` is a leadlag_events-shaped table
// (0+ rows). Vectorised — all CSV lines built in one pass (CLAUDE.md §4), no
// per-row each. Safe no-op when disabled or empty.
.log.event:{[evs]
  if[not .log.toFile; :()];
  if[not count evs; :()];
  if[null .log.eventH; .log.openEventLog[]];
  // Format each column to strings, then join row-wise with commas.
  lines:","sv/:flip (
    string evs`ts;
    string evs`sym;
    string evs`direction;
    string evs`leadVenue;   string evs`leadInst;
    string evs`followVenue; string evs`followInst;
    string evs`lagMs;
    string evs`moveSizeBps;
    string evs`clockSkewMs);
  .log.eventH ("\n" sv lines),"\n";
  };

// Close the event log handle (call on shutdown to flush + release).
.log.closeEventLog:{[]
  if[not null .log.eventH; hclose .log.eventH; .log.eventH:0Ni];
  };

// --- Periodic snapshot of a rolling summary table -------------------------
// Appends the current contents of table `t` to logs/<name>.log as CSV (header
// on first write), so the day's evolution of an otherwise-overwritten table
// (e.g. leadership_score) is reviewable later. Low-frequency (timer-driven), so
// it opens/appends/closes per call. Rows carry their own ts column.
.log.snapshot:{[name;t]
  if[not count t; :()];
  system "mkdir -p logs";
  f:hsym `$"logs/",string[name],".log";
  if[0=@[hcount; f; 0]; f 0: enlist "," sv string cols t];   // header on new file
  h:hopen f;                                                  // append mode
  h ("\n" sv ","sv/:flip string each value flip 0!t),"\n";   // all rows, vectorised
  hclose h;
  };
