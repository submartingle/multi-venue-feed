
"""
Coinbase Exchange (public) spot feedhandler — cross-venue leg for multi-venue-feed.

- Default channel `level2_batch` (no auth): full order-book snapshot + batched
  `l2update` diffs. The handler maintains the book per product and emits a
  normalized BBO tick whenever top-of-book changes — continuous quote-driven BBO,
  fixing the sparsity of the trade-driven `ticker` channel (DOGE read 181 ticks/35min
  in the 4e run). `l2update` carries a native microsecond exchange `time`, so — unlike
  Binance spot @bookTicker — the BBO comes with a real exchange clock and needs NO
  latency calibration (cf. binanceFH_REST.py G1 / docs/LIVE_RUN_2026-06-09_FINDINGS.md §7).
- `--channel ticker` keeps the previous TRADE-driven path selectable (BBO refreshes
  only on matches) for rollback / A-B comparison.
- Normalizes each tick to the SAME `bookticker` tickerplant table the Binance handlers
  publish to: (exch_us; recv_us; sym; venue; inst; bid; bidsz; ask; asksz), with
  venue=COINBASE, inst=SPOT and sym = the RAW Coinbase product_id (e.g. `BTC-USD`).
  Raw venue symbols are retained as the record; the q bridge (proc/live_xvenue.q) maps
  them to canonical engine symbols (BTC-USD -> BTC) for cross-venue pairing.

CAVEAT (timing): Coinbase docs — `level2_batch` "sends batches of level2 messages every
50 milliseconds" and "the `time` field correlates to the most recent message in the batch."
So `time` is a REAL last-event timestamp (NOT a flush/grid time): a BBO change is only
OBSERVABLE at 50ms batch resolution (its detected-move time can be up to ~50ms late), but the
stamp itself carries the batch's last real event time. In practice the batch cadence is not a
clean 50ms quantum (modal near it, with jitter and occasional sub-batch gaps). Harmless under
the 100ms dead-band (direction survives); but quote-clock lag
MAGNITUDES carry this noise — use the trade clock for clean magnitudes. The unbatched `level2`
channel (per-event, no 50ms grain) requires auth; switch to it if the grain ever binds.
Ref: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels

Dependencies: pip install websockets orjson pykx  (uvloop optional)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

import orjson
import websockets

# ---------- Logging ----------

log = logging.getLogger("coinbase_feedhandler")


# ---------- Publisher (Tickerplant IPC) ----------

class TickerplantPublisher:
    """
    Publish to the tickerplant by calling .u.upd[table; rows] over IPC (PyKX),
    fire-and-forget (wait=False). Mirrors the Binance handlers' publisher so the
    ingest stack stays self-contained and uniform.
    """

    def __init__(self, host: str, port: int, enabled: bool = True):
        self.enabled = enabled
        self._q = None
        self._kx = None
        if not enabled:
            log.warning("Publisher disabled: will not publish to tickerplant.")
            return
        try:
            import pykx as kx
            self._kx = kx
            self._q = kx.SyncQConnection(host=host, port=port)
            log.info("Connected to tickerplant via PyKX at %s:%d", host, port)
        except Exception as e:
            log.exception("Failed to init PyKX publisher, falling back to logging-only: %s", e)
            self.enabled = False

    async def publish(self, table: str, rows: List[Tuple[Any, ...]]) -> None:
        if not rows:
            return
        if (not self.enabled) or (self._q is None):
            log.info("[NO-TP] would publish %d rows to %s", len(rows), table)
            return
        kx = self._kx
        try:
            self._q("{.u.upd[x;y]}", kx.SymbolAtom(table), rows, wait=False)
        except Exception:
            log.exception("Failed publishing %d rows to %s", len(rows), table)

    def close(self) -> None:
        try:
            if self._q is not None:
                self._q.close()
        except Exception:
            pass


# ---------- Level-2 book state ----------

class L2Book:
    """One product's level2 book (price -> size per side) + last emitted BBO.

    Floats are safe as price keys here: Coinbase sends canonical decimal strings,
    and equal decimal values parse to the identical float, so lookups/removals hit.
    BBO is recomputed with max()/min() per l2update message — batched at ~50ms by
    the server, so at most ~20 scans/s/product over a dict of book levels.
    """
    __slots__ = ("bids", "asks", "lastBbo")

    def __init__(self):
        self.bids: dict = {}
        self.asks: dict = {}
        self.lastBbo: Optional[Tuple[float, float, float, float]] = None

    def load_snapshot(self, bids: list, asks: list) -> None:
        self.bids = {float(p): float(s) for p, s in bids}
        self.asks = {float(p): float(s) for p, s in asks}

    def apply(self, changes: list) -> None:
        # change = [side, price, size]; size "0" deletes the level
        for side, p, s in changes:
            book = self.bids if side == "buy" else self.asks
            pf = float(p)
            sf = float(s)
            if sf == 0.0:
                book.pop(pf, None)
            else:
                book[pf] = sf

    def bbo(self) -> Optional[Tuple[float, float, float, float]]:
        if not self.bids or not self.asks:
            return None
        bb = max(self.bids)
        ba = min(self.asks)
        return (bb, self.bids[bb], ba, self.asks[ba])


# ---------- Feedhandler ----------

class CoinbaseSpotFeedHandler:
    def __init__(
        self,
        symbols: List[str],
        tp_host: str,
        tp_port: int,
        ws_url: str = "wss://ws-feed.exchange.coinbase.com",
        channel: str = "level2_batch",
        flush_interval_ms: int = 10,
        max_batch_rows: int = 10_000,
        enable_tp: bool = True,
        trades: bool = True,
    ):
        # Coinbase product ids, e.g. BTC-USD (kept verbatim as the raw venue symbol).
        self.symbols = [s.upper() for s in symbols]
        self.ws_url = ws_url
        if channel not in ("ticker", "level2_batch"):
            raise ValueError(f"unsupported channel: {channel}")
        self.channel = channel
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_rows = max_batch_rows
        self.publisher = TickerplantPublisher(tp_host, tp_port, enabled=enable_tp)
        self.trades = trades
        self._bt_batch: List[Tuple[Any, ...]] = []
        self._tr_batch: List[Tuple[Any, ...]] = []
        self._books: dict = {}  # product_id -> L2Book (level2_batch mode only)
        self._stop = asyncio.Event()

    @staticmethod
    def _now_us() -> int:
        # local receive time in epoch microseconds (single clock domain, like the
        # Binance handlers' recv_us).
        import time
        return time.time_ns() // 1000

    @staticmethod
    def _iso_to_us(s: str) -> Optional[int]:
        """Coinbase `time` (ISO8601, e.g. '2022-10-19T23:28:22.061769Z') -> epoch us.
        Returns None if absent/unparseable so the caller can fall back to recv_us."""
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return None

    def _subscribe_msg(self) -> bytes:
        # heartbeat keeps the socket alive during quiet periods
        # (Coinbase drops a connection with no subscription within 5s).
        return orjson.dumps({
            "type": "subscribe",
            "product_ids": self.symbols,
            "channels": ([self.channel, "heartbeat"] + (["matches"] if self.trades else [])),
        })

    # ----- level2_batch path: snapshot + diffs -> BBO-change ticks -----

    def _handle_snapshot(self, ev: dict, recv_us: int) -> None:
        sym = ev.get("product_id")
        if sym is None:
            return
        book = self._books.setdefault(sym, L2Book())
        book.load_snapshot(ev.get("bids") or [], ev.get("asks") or [])
        # snapshot may carry no `time`; recv_us fallback is the exact-zero skew
        # signature .skew.calc already excludes from calibration (G5 guard)
        self._emit_if_changed(book, sym, self._iso_to_us(ev.get("time")) or recv_us, recv_us)

    def _handle_l2update(self, ev: dict, recv_us: int) -> None:
        sym = ev.get("product_id")
        book = self._books.get(sym)
        if book is None:
            return  # diff before snapshot (shouldn't happen) — wait for resync
        book.apply(ev.get("changes") or [])
        self._emit_if_changed(book, sym, self._iso_to_us(ev.get("time")) or recv_us, recv_us)

    def _emit_if_changed(self, book: L2Book, sym: str, exch_us: int, recv_us: int) -> None:
        # one tick per message whose net effect moved top-of-book (price OR size,
        # matching Binance @bookTicker semantics); crossed books are emitted as-is —
        # the q engine skips them defensively on ingest (C11)
        bbo = book.bbo()
        if bbo is None or bbo == book.lastBbo:
            return
        book.lastBbo = bbo
        bid, bidsz, ask, asksz = bbo
        self._bt_batch.append((exch_us, recv_us, sym, "COINBASE", "SPOT", bid, bidsz, ask, asksz))

    # ----- ticker path (legacy, trade-driven) -----

    def _handle_ticker(self, ev: dict, recv_us: int) -> None:
        # Defensive: a ticker may omit best_bid/ask momentarily; skip if missing.
        bb = ev.get("best_bid"); ba = ev.get("best_ask")
        if bb is None or ba is None:
            return
        sym = ev.get("product_id")
        if sym is None:
            return
        exch_us = self._iso_to_us(ev.get("time")) or recv_us  # native clock; recv_us fallback
        bid = float(bb); ask = float(ba)
        bidsz = float(ev.get("best_bid_size", 0.0))
        asksz = float(ev.get("best_ask_size", 0.0))
        self._bt_batch.append((exch_us, recv_us, sym, "COINBASE", "SPOT", bid, bidsz, ask, asksz))

    # ----- matches path: trade prints -> `trades` table -----

    def _handle_match(self, ev: dict, recv_us: int) -> None:
        # `match` = one trade print. Coinbase has no separate event-vs-trade time, so
        # exch_us and trade_us both carry the match `time`. Coinbase `side` is the
        # MAKER side; the schema's side is the AGGRESSOR (Binance convention), so flip:
        # maker buy means the taker SOLD.
        sym = ev.get("product_id")
        px = ev.get("price"); qty = ev.get("size")
        if sym is None or px is None or qty is None:
            return
        t_us = self._iso_to_us(ev.get("time")) or recv_us
        side = "S" if ev.get("side") == "buy" else "B"
        self._tr_batch.append((t_us, t_us, recv_us, sym, float(px), float(qty),
                               side, int(ev.get("trade_id", 0))))

    async def run(self) -> None:
        backoff = 0.25
        try:
            while not self._stop.is_set():
                try:
                    await self._run_once()
                    backoff = 0.25
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("WS loop crashed; reconnecting after %.2fs", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 10.0)
        finally:
            try:
                self.publisher.close()
            except Exception:
                pass

    async def _run_once(self) -> None:
        log.info("Connecting WS: %s (channel=%s products=%s)",
                 self.ws_url, self.channel, ",".join(self.symbols))
        # stale books must not absorb a new connection's diffs; Coinbase re-sends
        # a full snapshot per product on subscribe
        self._books.clear()
        # full-book snapshots run to tens of MB for deep products — well past the
        # 4MB that sufficed for ticker
        async with websockets.connect(self.ws_url, max_size=64 * 1024 * 1024) as ws:
            await ws.send(self._subscribe_msg())
            flush_task = asyncio.create_task(self._flush_loop())
            try:
                async for msg in ws:
                    recv_us = self._now_us()
                    j = orjson.loads(msg)
                    t = j.get("type")
                    if t == "l2update":
                        self._handle_l2update(j, recv_us)
                    elif t == "snapshot":
                        self._handle_snapshot(j, recv_us)
                    elif t == "ticker":
                        self._handle_ticker(j, recv_us)
                    elif t == "match":
                        if self.trades:
                            self._handle_match(j, recv_us)
                    elif t == "error":
                        # e.g. if Coinbase ever moves level2_batch behind auth
                        log.error("WS error message: %s", j)
                    # subscriptions / heartbeat are ignored
            finally:
                flush_task.cancel()

    async def _flush_loop(self) -> None:
        interval = self.flush_interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            bt_rows = self._drain(self._bt_batch)
            if bt_rows:
                await self.publisher.publish("bookticker", bt_rows)
            tr_rows = self._drain(self._tr_batch)
            if tr_rows:
                await self.publisher.publish("trades", tr_rows)

    def _drain(self, buf: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        if not buf:
            return []
        n = min(len(buf), self.max_batch_rows)
        rows = buf[:n]
        del buf[:n]
        return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True, help="Coinbase products, e.g. BTC-USD ETH-USD")
    p.add_argument("--tp-host", default="127.0.0.1")
    p.add_argument("--tp-port", type=int, default=5010)
    p.add_argument("--ws-url", default="wss://ws-feed.exchange.coinbase.com")
    p.add_argument("--channel", choices=["ticker", "level2_batch"], default="level2_batch",
                   help="level2_batch (default): book-maintained quote-driven BBO; "
                        "ticker: legacy trade-driven BBO")
    p.add_argument("--flush-interval-ms", type=int, default=10)
    p.add_argument("--max-batch-rows", type=int, default=10_000)
    p.add_argument("--no-tp", action="store_true", help="Don't publish to tickerplant; just run collector.")
    p.add_argument("--no-trades", action="store_true",
                   help="Don't subscribe `matches` / publish `trades` (default: on).")
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    fh = CoinbaseSpotFeedHandler(
        symbols=args.symbols,
        tp_host=args.tp_host,
        tp_port=args.tp_port,
        ws_url=args.ws_url,
        channel=args.channel,
        flush_interval_ms=args.flush_interval_ms,
        max_batch_rows=args.max_batch_rows,
        enable_tp=(not args.no_tp),
        trades=(not args.no_trades),
    )
    await fh.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
