// proc/load.q — load config, schemas and libraries in dependency order.
// Shared by proc/engine.q (live) and the replay harness in sim/. No side effects
// beyond defining .cfg.* / .log.* / .feed.* / .leadlag.* / .score.* / .regime.*
// and the input/output tables. Run from the project root.
system "l config/params.q";
system "l config/regime_cal.q";
system "l config/move_cal.q";          // calibrated per-asset move thresholds (-> .cfg.moveThresholdBps)
system "l lib/log.q";
system "l schema/input.q";
system "l schema/output.q";
system "l lib/skew.q";
system "l lib/health.q";
system "l lib/movedetect.q";           // .move.* detectors (cumstale used by lib/feed.q)
system "l lib/feed.q";
system "l lib/leadlag.q";
system "l lib/comove.q";
system "l lib/score.q";
system "l lib/regime.q";
system "l lib/alert.q";
system "l lib/monitor.q";              // .monitor.snap[] — dashboard metrics
