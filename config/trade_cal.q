// config/trade_cal.q — per-asset TRADE-clock imbalance-bar thresholds (USD), consumed by
// lib/imbalance.q via .cfg.tradeBarUsd. One bar = a net signed aggressor-notional surge of
// `thr` USD; the same USD bar is used across a symbol's venues (same asset / imbalance unit).
//
// In production thr = barMult x the venue median per-trade notional, measured offline on a
// recent capture. The values below are NEUTRAL PLACEHOLDERS so the engine loads and runs out of
// the box — regenerate them against your own market data. Unlisted symbols fall back to
// .cfg.tradeBarUsdDefault.
.cfg.tradeBarUsdDefault:300f;
.cfg.tradeBarUsd:`BTC`ETH`SOL`DOGE`XRP!300 250 250 100 250f;
