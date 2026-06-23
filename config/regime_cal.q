// config/regime_cal.q — per-leg regime boundaries (consumed by lib/regime.q).
//
// Scheme: per-leg FIXED boundaries (spreadTight / spreadWide / volLow). Global fixed values
// cannot work across a multi-venue universe because between-leg spread levels span orders of
// magnitude (majors quote at their venue tick floor), so a global boundary would bucket by
// symbol identity rather than market state. In production these are GENERATED per leg by
// sim/regime_calibrate.q from a recent capture.
//
// Shipped EMPTY here (no fitted values) — lib/regime.q falls back to the scalar defaults in
// config/params.q (.cfg.spreadTightBps / .cfg.spreadWideBps / .cfg.volLowBps) for any leg not
// listed, so the engine runs out of the box. Populate by running the calibration tool on your
// own data.
.cfg.regimeCal:`sym`venue`inst xkey flip `sym`venue`inst`spreadTight`spreadWide`volLow!(
  `symbol$(); `symbol$(); `symbol$(); `float$(); `float$(); `float$());
