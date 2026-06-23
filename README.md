# Multi-Venue Lead-Lag Engine

A real-time **kdb+/q** engine that measures cross-venue **price-discovery lead/lag** — *which
venue moves first, and by how much* — and separates it from **order-flow participation**, the
venue where aggressive volume merely lands. Built on live crypto spot feeds
(Binance · Coinbase · OKX), but the pairing and scoring model is venue- and asset-agnostic: the
same code handles spot-vs-perp, venue-vs-venue, and ETF-vs-basket.

> **Public demo build.** This repository is a clean, architecture-focused snapshot: the engine,
> feed handlers, dashboard, and synthetic self-tests. Calibration files ship with neutral
> placeholder defaults and no captured market data — regenerate them against your own feeds.
> Empirical results are intentionally omitted.

---

## The core idea — three independent clocks

A naive lead-lag reading is dominated by measurement artifacts (server geography, feed
granularity, one venue's missing exchange clock). To make the signal robust, the engine runs
**three independent scorers in one process** and only trusts a leadership claim that survives
across the clocks that should agree:

| Scorer | Clock | Measures | Catches |
|---|---|---|---|
| **Quote winreset** | quote `eventTs` | whose *quote* repriced first | price discovery |
| **Trade-price winreset** | native `trade_us` | whose *print* repriced first | price discovery, clock-independent |
| **Trade-flow imbalance** | native `trade_us` | whose *aggressive flow* surged first | participation, not price |

Price discovery is confirmed only when the **quote** and **trade-price** clocks agree — the trade
clock is native on every venue, so agreement rules out a quote-timestamp artifact. The **flow**
clock is deliberately separate: the venue with the heaviest taker flow is *not* necessarily the
venue where price is made.

---

## Architecture

```
Python feed handlers          tickerplant (TP)         live bridge — proc/live_xvenue.q
 Binance / Coinbase / OKX  →   durable tplog,       →   .z.ts buffer-swap drives 3 scorers:
 normalized ticks + trades     fan-out to RDB +         quote · trade-price · trade-flow
                               bridge subscriber             │
 recal sidecar ──────────────── vol-adaptive bars ─────────►│  directed co-movement
                                                              ▼  + base-rate correction
            Python TUI  ◄──── .monitor.snap[] (IPC) ────  + binomial sign test + Wilson CI
                                                              ▼
                                               leadership_{score,session,session_t*}
```

**Library layer** (`lib/`, one namespace per file):

| File | Namespace | Role |
|---|---|---|
| `feed.q` · `movedetect.q` | `.feed` · `.move` | tick ingest + winreset move detection |
| `comove.q` · `score.q` | `.comove` · `.score` | base-rate-corrected directed co-movement + sign test + Wilson CI |
| `imbalance.q` | `.imb` | signed-flow imbalance bars (trade-flow scorer) |
| `tprice.q` | `.tprice` | trade-price winreset scorer |
| `skew.q` | `.skew` | clock dead-bands / skew handling |
| `regime.q` | `.regime` | spread × vol regime bucketing |
| `autocal.q` · `recal.q` | `.autocal` · `.recal` | vol-adaptive bar calibration |
| `health.q` · `alert.q` | `.health` · `.alert` | feed-health monitoring + alerting |
| `monitor.q` | `.monitor` | `.monitor.snap[]` — dashboard metrics over IPC |
| `log.q` | `.log` | logging + durable event log |
| `leadlag.q` | `.leadlag` | rolling-buffer follow-window matcher |

**Schema/config:** `schema/{input,output}.q`; `config/{params,move_cal,trade_cal,regime_cal}.q`.
**Processes** (`proc/`): thin orchestration — load libraries + schemas, wire the timer.
**Feed handlers** (`ingest/`): one Python process per venue, normalizing WebSocket ticks/trades
into the tickerplant.
**Dashboard** (`scripts/dashboard.py`): a `rich` terminal UI that attaches to the live engine
over IPC and renders table counts, leadership scores, per-venue feed health, the three-scorer
pipeline, leg freshness and q-workspace memory — read-only, refreshed on a timer.

---

## Design highlights

- **Honest clock frame.** Scoring is on raw venue `eventTs` / native trade time, *not* an
  arrival frame — an arrival-time frame silently encodes server geography (a Tokyo venue vs a
  US-East venue) as a fake "lead". Direction is trusted; sub-100 ms lag *magnitudes* are not, so
  feed-granularity dead-bands gate the matcher (quote 100 ms, trade 50 ms).
- **Missing-exchange-clock handling.** Some venues' spot quote streams carry no exchange
  timestamp, so that quote clock is estimated and unstable — the engine cross-checks against the
  clean native trade clock rather than trusting it.
- **Statistics, not anecdotes.** Base-rate-corrected directed co-movement removes the coincidence
  baseline; a binomial sign test plus a Wilson score interval give per-leg significance and a
  confidence band on the leadership score (robust at small sample sizes).
- **Vol-adaptive bars.** A recal sidecar re-solves each symbol's move threshold to a target event
  rate, keeping the detector calibrated across volatility regimes without a restart.
- **Production hygiene.** Tickerplant tplog as the durable record; session-detached launcher;
  bounded-memory discipline for long runs.

---

## Running

From the project root (`q` on `PATH`, `QHOME` set; Python deps: `pykx`, `rich`).

```bash
# Synthetic self-test — known ground-truth lags, self-asserting (no market data needed)
q sim/feed_sim.q

# Illustrative micro-demos of individual mechanisms (synthetic)
q sim/deadband_demo.q      # the coincidence dead-band
q sim/bias_demo.q          # tick-rate granularity bias
q sim/skew_demo.q          # clock-skew handling

# Live three-venue stack (TP + RDB + engine + feed handlers), 2x move bar
MOVE_MULT=2 ROOTS="BTC ETH SOL DOGE XRP" ./scripts/start_xvenue.sh
./scripts/start_xvenue.sh --dashboard      # also launch the live TUI

# Attach the dashboard to a running engine (port 5099); --once prints a static snapshot
python3 scripts/dashboard.py --port 5099 --refresh 10
python3 scripts/dashboard.py --port 5099 --once
```

---

## Notes

This is a portfolio / architecture demo. Calibration configs carry placeholder values; empirical
findings, captured data, and the full research history are kept in a separate private repository.
Parts of the build were developed with AI coding agents (Claude Code) under a test-and-verify
workflow.

Licensed under the MIT License — see [`LICENSE`](LICENSE).
