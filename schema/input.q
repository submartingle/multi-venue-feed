// schema/input.q — input feed schema
// Defines the single normalized inbound table for the propagation engine.
// All venue/instrument feeds are normalized to this shape so the analytics
// logic stays plug-and-play (asset-agnostic).

// quote_norm: normalized top-of-book stream, one row per tick per leg.
// A "leg" is the (sym;venue;inst) tuple — the unit the engine compares.
//   eventTs : exchange event time — drives lagMs by default (.cfg.lagClock=`eventTs);
//             immune to per-leg transport asymmetry. Binance spot estimates it from
//             @trade latency in the FH; perp/Coinbase native. Item 2, 2026-06-09.
//   recvTs  : local receive timestamp — single clock domain; legacy lag clock (A2),
//             selectable via .cfg.lagClock=`recvTs; also used for window eviction/diagnostics
//   sym     : root symbol (e.g. `BTC, `AAPL)
//   venue   : source (e.g. `BINANCE, `NYSE)
//   inst    : instrument type (`SPOT, `PERP, `ETF, `CASH)
//   bid/ask : top-of-book prices
//   mid     : mid-price; recomputed defensively on ingest, not trusted from upstream
// g# on sym: real-time in-memory hash lookup, maintained on append (Section 2).
quote_norm:([] eventTs:`timestamp$(); recvTs:`timestamp$(); sym:`g#`symbol$();
  venue:`symbol$(); inst:`symbol$(); bid:`float$(); ask:`float$(); mid:`float$())
