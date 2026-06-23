"""
binanceUMPerpFH.py

USDⓈ-M Perpetual (Binance Futures) feedhandler:
- Subscribes to <symbol>@aggTrade, <symbol>@depth@<speed>, <symbol>@markPrice@1s
- Maintains local order book sync per Binance futures docs:
    * initial snapshot from GET /fapi/v1/depth
    * enforce continuity via `pu` (previous update id)
- Publishes batches to a kdb+ tickerplant via .u.upd over IPC (PyKX)

Dependencies:
  pip install websockets httpx orjson pykx
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
import orjson
import websockets

log = logging.getLogger("binance_um_perp_fh")


# ---------- Exceptions ----------

class ResyncRequired(Exception):
    """Raised when the book stream has a gap and we must resync via snapshot."""


# ---------- Publisher (Tickerplant IPC) ----------

class TickerplantPublisher:
    """
    Publish to tickerplant by calling .u.upd[table; rows] over IPC (PyKX).
    Uses SyncQConnection with wait=False for fire-and-forget.
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
            log.exception("Failed to init PyKX publisher; falling back to logging-only: %s", e)
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


# ---------- Book Sync State (USD-M Futures) ----------

@dataclass
class BookSnapshot:
    last_update_id: int
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]


class UMBookSync:
    """
    Tracks Binance USD-M futures local order book synchronization state for ONE symbol.

    Futures diff depth messages include:
      U (first update id), u (final update id), pu (previous final update id), b, a, E, T

    Continuity rule (Binance docs):
      each new event's `pu` should equal the previous event's `u` (otherwise resync).
    """

    def __init__(self, symbol: str, rest_base: str, snapshot_limit: int = 1000):
        self.symbol = symbol.upper()
        self._rest_base = rest_base.rstrip("/")
        self._snapshot_limit = snapshot_limit

        self._lock = asyncio.Lock()
        self._buffer: Deque[dict] = deque(maxlen=20000)
        self._first_U: Optional[int] = None

        self.ready: bool = False
        self.last_update_id: Optional[int] = None  # last processed `u`
        self._pending_snapshot: Optional[BookSnapshot] = None
        self._pending_snapshot_available: bool = False

        # Optional in-memory book for sanity/monitoring
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

        self._resync_task: Optional[asyncio.Task] = None

    def buffer_event(self, ev: dict) -> None:
        if self._first_U is None:
            self._first_U = int(ev["U"])
        self._buffer.append(ev)
    async def ensure_ready(self) -> None:
        """
        Kick off (or reuse) a resync task.

        IMPORTANT: Do NOT await resync from within the WS message handler; otherwise the handler stops
        consuming depth messages and the resync can't see the bridging event -> deadlock.
        """
        async with self._lock:
            if self.ready:
                return
            if self._resync_task is None or self._resync_task.done():
                self._resync_task = asyncio.create_task(self._resync())
        return

    def take_pending_snapshot(self) -> Optional[BookSnapshot]:
        """Return the latest snapshot exactly once (for publishing to pbsnap)."""
        if self._pending_snapshot_available and self._pending_snapshot is not None:
            self._pending_snapshot_available = False
            return self._pending_snapshot
        return None

    async def _fetch_snapshot_until(self, first_U: int) -> BookSnapshot:
        """
        Fetch snapshot; if snapshot.lastUpdateId < first_U, repeat (per docs).
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(1, 20):
                url = f"{self._rest_base}/fapi/v1/depth"
                params = {"symbol": self.symbol, "limit": self._snapshot_limit}
                r = await client.get(url, params=params)
                r.raise_for_status()
                js = r.json()

                last_update_id = int(js["lastUpdateId"])
                bids = [(float(p), float(q)) for p, q in js.get("bids", [])]
                asks = [(float(p), float(q)) for p, q in js.get("asks", [])]

                if last_update_id >= first_U:
                    return BookSnapshot(last_update_id=last_update_id, bids=bids, asks=asks)

                await asyncio.sleep(min(0.05 * attempt, 1.0))

        raise RuntimeError(f"{self.symbol}: failed to get snapshot with lastUpdateId>=first_U after retries")

    async def _resync(self) -> BookSnapshot:
        # Wait for first buffered event so we have first_U
        t0 = time.time()
        while self._first_U is None:
            await asyncio.sleep(0.01)
            if time.time() - t0 > 5.0:
                raise RuntimeError(f"{self.symbol}: no depth events seen within 5s; cannot sync book")

        first_U = int(self._first_U)
        snapshot = await self._fetch_snapshot_until(first_U)
        # expose snapshot for publishing (once) without blocking the WS loop
        self._pending_snapshot = snapshot
        self._pending_snapshot_available = True

        # Discard buffered events where u <= snapshot.lastUpdateId
        while self._buffer and int(self._buffer[0]["u"]) <= snapshot.last_update_id:
            self._buffer.popleft()

        # Set local book to snapshot (BUT do not declare 'ready' yet).
        self.bids = {px: qty for px, qty in snapshot.bids if qty != 0.0}
        self.asks = {px: qty for px, qty in snapshot.asks if qty != 0.0}
        self.last_update_id = snapshot.last_update_id

        # Futures book sync requires applying the FIRST diff event that bridges the snapshot:
        # find first event where U <= lastUpdateId+1 <= u, apply it, then enforce pu continuity thereafter.
        target = snapshot.last_update_id + 1

        # Drop events that are strictly older than the snapshot boundary.
        while self._buffer and int(self._buffer[0]["u"]) < target:
            self._buffer.popleft()

        # Wait briefly for the bridging event to be present in buffer (WS may lag snapshot).
        t_start = time.time()
        bridged = False
        while not bridged:
            if not self._buffer:
                await asyncio.sleep(0.01)
            else:
                ev0 = self._buffer[0]
                U0 = int(ev0["U"])
                u0 = int(ev0["u"])

                if U0 <= target <= u0:
                    # Apply WITHOUT requiring pu match (this is the bridge event).
                    self._apply_depth_event(ev0, update_book=True, require_pu=False)
                    self._buffer.popleft()
                    bridged = True
                elif U0 > target:
                    # We missed the bridge event -> fetch a new snapshot by restarting.
                    raise ResyncRequired(
                        f"{self.symbol}: cannot bridge snapshot (U={U0} > target={target}); resync again"
                    )
                else:
                    # Safety: shouldn't happen after dropping u < target, but keep safe:
                    self._buffer.popleft()

            if time.time() - t_start > 2.0:
                raise RuntimeError(f"{self.symbol}: timed out waiting for bridge depth event after snapshot")

        # Now apply remaining buffered events WITH pu continuity.
        while self._buffer:
            ev = self._buffer.popleft()
            self._apply_depth_event(ev, update_book=True, require_pu=True)

        self.ready = True
        log.info("%s: book synced at lastUpdateId=%d (bids=%d asks=%d)",
                 self.symbol, self.last_update_id, len(self.bids), len(self.asks))
        return snapshot

    def reset_for_resync(self, seed_event: Optional[dict] = None) -> None:
        self.ready = False
        self.last_update_id = None
        self.bids.clear()
        self.asks.clear()
        self._buffer.clear()
        self._first_U = None
        self._pending_snapshot = None
        self._pending_snapshot_available = False
        if seed_event is not None:
            self.buffer_event(seed_event)

    def apply_or_raise(self, ev: dict) -> None:
        if not self.ready:
            self.buffer_event(ev)
            return
        self._apply_depth_event(ev, update_book=True)

    def _apply_depth_event(self, ev: dict, update_book: bool, require_pu: bool = True) -> None:
        assert self.last_update_id is not None

        U = int(ev["U"])
        u = int(ev["u"])
        pu = int(ev.get("pu", -1))

        # Drop stale
        if u <= self.last_update_id:
            return

        # Continuity check (Binance futures): for steady-state, pu should equal previous u.
        # For the FIRST event that bridges a REST snapshot, do not require pu match.
        if require_pu and pu != -1 and pu != self.last_update_id:
            raise ResyncRequired(f"{self.symbol}: gap detected (pu={pu} != last_u={self.last_update_id})")

        # Apply levels, then update last_update_id
        if update_book:
            for px_s, qty_s in ev.get("b", []):
                px = float(px_s)
                qty = float(qty_s)
                if qty == 0.0:
                    self.bids.pop(px, None)
                else:
                    self.bids[px] = qty

            for px_s, qty_s in ev.get("a", []):
                px = float(px_s)
                qty = float(qty_s)
                if qty == 0.0:
                    self.asks.pop(px, None)
                else:
                    self.asks[px] = qty

        self.last_update_id = u


# ---------- Feedhandler ----------

class BinanceUMPerpFeedHandler:
    def __init__(
        self,
        symbols: List[str],
        tp_host: str,
        tp_port: int,
        ws_base: str = "wss://fstream.binance.com",
        rest_base: str = "https://fapi.binance.com",
        depth_speed: str = "100ms",
        snapshot_limit: int = 1000,
        mark_speed: str = "1s",
        flush_interval_ms: int = 10,
        max_batch_rows: int = 10_000,
        enable_tp: bool = True,
        enable_trades: bool = True,
        idle_timeout_s: float = 90.0,
        enable_bupd: bool = True,
        enable_bsnap: bool = True,
        enable_mark: bool = True,
        enable_bookticker: bool = True,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")
        self.depth_speed = depth_speed
        self.snapshot_limit = snapshot_limit
        self.mark_speed = mark_speed
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_rows = max_batch_rows

        self.enable_trades = enable_trades
        self.idle_timeout_s = idle_timeout_s
        self.enable_bupd = enable_bupd
        self.enable_bsnap = enable_bsnap
        self.enable_mark = enable_mark
        # bookTicker: perp pushes best bid/ask directly with event time E -> normalized BBO
        # consumed downstream by the multi-venue-feed engine.
        self.enable_bookticker = enable_bookticker
        self.enable_book = (self.enable_bupd or self.enable_bsnap)

        self.publisher = TickerplantPublisher(tp_host, tp_port, enabled=enable_tp)

        self.books: Dict[str, UMBookSync] = {}
        if self.enable_book:
            self.books = {
                s: UMBookSync(s, rest_base=self.rest_base, snapshot_limit=self.snapshot_limit)
                for s in self.symbols
            }

        # Batches for TP
        self._trade_batch: List[Tuple[Any, ...]] = []
        self._bupd_batch: List[Tuple[Any, ...]] = []
        self._bsnap_batch: List[Tuple[Any, ...]] = []
        self._mark_batch: List[Tuple[Any, ...]] = []
        self._bt_batch: List[Tuple[Any, ...]] = []

        self._stop = asyncio.Event()

    @staticmethod
    def _now_us() -> int:
        return time.time_ns() // 1000

    @staticmethod
    def _ms_to_us(x_ms: int) -> int:
        return int(x_ms) * 1000

    # Binance routed WS endpoints (mandatory since the 2026 migration; legacy
    # unrouted URLs were decommissioned 2026-04-23). Each connection is bound to
    # ONE endpoint and only receives that endpoint's streams:
    #   /public  -> @depth, @bookTicker         (high-frequency)
    #   /market  -> @aggTrade, @markPrice, ...   (regular market data)
    # An unrouted /stream or /ws connection silently receives /public streams
    # ONLY — which is why @aggTrade/@markPrice went dark while @depth kept flowing.
    def _public_streams(self) -> List[str]:
        streams: List[str] = []
        for s in self.symbols:
            sym = s.lower()
            if self.enable_book:
                # allowed: @depth, @depth@500ms, @depth@100ms
                streams.append(f"{sym}@depth@{self.depth_speed}")
            if self.enable_bookticker:
                streams.append(f"{sym}@bookTicker")
        return streams

    def _market_streams(self) -> List[str]:
        streams: List[str] = []
        for s in self.symbols:
            sym = s.lower()
            if self.enable_trades:
                streams.append(f"{sym}@aggTrade")
            if self.enable_mark:
                streams.append(f"{sym}@markPrice@{self.mark_speed}")
        return streams

    def _routed_url(self, endpoint: str, streams: List[str]) -> str:
        """Combined-stream URL on a routed endpoint: /{endpoint}/stream?streams=..."""
        return f"{self.ws_base}/{endpoint}/stream?streams={'/'.join(streams)}"

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
        # Streams are split across two routed connections (see _public_streams /
        # _market_streams). One flush loop drains all batches. If EITHER reader
        # exits/raises, tear both down so run() reconnects the whole thing.
        public_streams = self._public_streams()
        market_streams = self._market_streams()

        flush_task = asyncio.create_task(self._flush_loop())
        readers: List[asyncio.Task] = []
        if public_streams:
            readers.append(asyncio.create_task(self._read_endpoint("public", public_streams)))
        if market_streams:
            readers.append(asyncio.create_task(self._read_endpoint("market", market_streams)))

        if not readers:
            flush_task.cancel()
            raise RuntimeError("no streams enabled")

        try:
            done, _ = await asyncio.wait(readers, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                t.result()  # re-raise the first reader failure to trigger reconnect
        finally:
            flush_task.cancel()
            for t in readers:
                t.cancel()
            # drain cancellations so failures aren't logged as "never retrieved"
            await asyncio.gather(*readers, flush_task, return_exceptions=True)

    async def _read_endpoint(self, endpoint: str, streams: List[str]) -> None:
        url = self._routed_url(endpoint, streams)
        log.info("Connecting WS [/%s]: %s", endpoint, url)

        # websockets library auto-replies to ping frames with pong frames.
        async with websockets.connect(url, max_size=16 * 1024 * 1024) as ws:
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"no WS message for {self.idle_timeout_s:.0f}s on /{endpoint} "
                        f"(silent connection) - reconnecting")
                recv_us = self._now_us()
                j = orjson.loads(msg)
                payload = j.get("data", j)

                et = payload.get("e")
                if et == "aggTrade":
                    if self.enable_trades:
                        self._handle_agg_trade(payload, recv_us)
                elif et == "depthUpdate":
                    if self.enable_book:
                        await self._handle_depth(payload, recv_us)
                elif et == "markPriceUpdate":
                    if self.enable_mark:
                        self._handle_mark(payload, recv_us)
                elif et == "bookTicker":
                    if self.enable_bookticker:
                        self._handle_bookticker(payload, recv_us)
                else:
                    continue

    def _handle_agg_trade(self, ev: dict, recv_us: int) -> None:
        """
        USD-M aggTrade fields:
          E event time (ms), s symbol, a agg id, p price, q qty, nq normal qty, f first tid, l last tid, T trade time (ms), m isBuyerMaker
        """
        sym = ev["s"]
        px = float(ev["p"])
        qty = float(ev["q"])
        nq = float(ev.get("nq", "nan")) if "nq" in ev else float("nan")
        agg_id = int(ev["a"])
        first_tid = int(ev["f"])
        last_tid = int(ev["l"])
        exch_us = self._ms_to_us(int(ev.get("E", 0)))
        trade_us = self._ms_to_us(int(ev.get("T", 0)))

        # side: if buyer is maker => aggressor sell else buy
        side = "S" if ev.get("m", False) else "B"

        # ptrades: (exch_us, trade_us, recv_us, sym, px, qty, nq, side, agg_id, first_tid, last_tid)
        self._trade_batch.append((exch_us, trade_us, recv_us, sym, px, qty, nq, side, agg_id, first_tid, last_tid))

    def _handle_bookticker(self, ev: dict, recv_us: int) -> None:
        """
        USD-M perp bookTicker fields:
          e "bookTicker", u updateId, E event time (ms), T transact time (ms),
          s symbol, b best bid px, B best bid qty, a best ask px, A best ask qty
        Perp carries a real exchange event time E (unlike spot). Row shape matches the
        `bookticker` TP table: (exch_us; recv_us; sym; venue; inst; bid; bidsz; ask; asksz)
        """
        if not self.enable_bookticker:
            return
        sym = ev["s"]
        bid = float(ev["b"]); bidsz = float(ev["B"])
        ask = float(ev["a"]); asksz = float(ev["A"])
        exch_us = self._ms_to_us(int(ev.get("E", 0)))
        self._bt_batch.append((exch_us, recv_us, sym, "BINANCE", "PERP", bid, bidsz, ask, asksz))

    async def _handle_depth(self, ev: dict, recv_us: int) -> None:
        """
        depthUpdate fields:
          E event time (ms), T tx time (ms), s symbol, U,u,pu, b bids, a asks
        """
        sym = ev["s"]
        book = self.books[sym]

        if not book.ready:
            # Buffer and kick off resync asynchronously. Do NOT await the full resync here.
            book.buffer_event(ev)
            try:
                await book.ensure_ready()
            except Exception:
                log.exception("%s: failed to start initial resync; will retry", sym)
            return

        try:
            book.apply_or_raise(ev)
        except ResyncRequired as e:
            log.warning("%s: %s -> resync", sym, e)
            book.reset_for_resync(seed_event=ev)
            try:
                await book.ensure_ready()
            except Exception:
                log.exception("%s: failed to start resync; will retry later", sym)
            return

        if self.enable_bupd:
            exch_us = self._ms_to_us(int(ev.get("E", 0)))
            tx_us = self._ms_to_us(int(ev.get("T", 0)))
            U = int(ev["U"])
            u = int(ev["u"])
            pu = int(ev.get("pu", -1))

            # Convert price levels to float pairs for cleaner typing on the q side
            bids = [(float(p), float(q)) for p, q in ev.get("b", [])]
            asks = [(float(p), float(q)) for p, q in ev.get("a", [])]

            # pbupd: (exch_us, tx_us, recv_us, sym, U, u, pu, bids, asks)
            self._bupd_batch.append((exch_us, tx_us, recv_us, sym, U, u, pu, bids, asks))

    def _handle_mark(self, ev: dict, recv_us: int) -> None:
        """
        markPriceUpdate fields:
          E event time (ms), s symbol, p mark, i index, P est settle, r funding, T next funding time (ms)
        """
        sym = ev["s"]
        exch_us = self._ms_to_us(int(ev.get("E", 0)))
        mark = float(ev["p"])
        index = float(ev["i"])
        est_settle = float(ev.get("P", "nan")) if "P" in ev else float("nan")
        funding = float(ev.get("r", "nan")) if "r" in ev else float("nan")
        next_funding_us = self._ms_to_us(int(ev.get("T", 0)))

        # pmark: (exch_us, recv_us, sym, mark, index, est_settle, funding, next_funding_us)
        self._mark_batch.append((exch_us, recv_us, sym, mark, index, est_settle, funding, next_funding_us))

    async def _flush_loop(self) -> None:
        interval = self.flush_interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval)

            # Publish any completed book snapshots (exactly once each)
            if self.enable_book and self.enable_bsnap:
                for sym, book in self.books.items():
                    snap = book.take_pending_snapshot()
                    if snap is not None:
                        self._bsnap_batch.append((self._now_us(), sym, int(snap.last_update_id), snap.bids, snap.asks))


            trade_rows = self._drain(self._trade_batch) if self.enable_trades else []
            bupd_rows = self._drain(self._bupd_batch) if self.enable_bupd else []
            bsnap_rows = self._drain(self._bsnap_batch) if self.enable_bsnap else []
            mark_rows = self._drain(self._mark_batch) if self.enable_mark else []
            bt_rows = self._drain(self._bt_batch) if self.enable_bookticker else []

            if trade_rows:
                await self.publisher.publish("ptrades", trade_rows)
            if bupd_rows:
                await self.publisher.publish("pbupd", bupd_rows)
            if bsnap_rows:
                await self.publisher.publish("pbsnap", bsnap_rows)
            if mark_rows:
                await self.publisher.publish("pmark", mark_rows)
            if bt_rows:
                await self.publisher.publish("bookticker", bt_rows)

    def _drain(self, buf: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        if not buf:
            return []
        if len(buf) > self.max_batch_rows:
            log.warning("Batch exceeded max rows (%d); truncating to last %d", len(buf), self.max_batch_rows)
            buf[:] = buf[-self.max_batch_rows :]
        out = buf[:]
        buf.clear()
        return out


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True, help="e.g. BTCUSDT ETHUSDT")
    p.add_argument("--tp-host", default="127.0.0.1")
    p.add_argument("--tp-port", type=int, default=5010)
    p.add_argument("--ws-base", default="wss://fstream.binance.com")
    p.add_argument("--rest-base", default="https://fapi.binance.com")
    p.add_argument("--depth-speed", default="100ms", choices=["100ms", "250ms", "500ms"])
    p.add_argument("--snapshot-limit", type=int, default=1000, choices=[5, 10, 20, 50, 100, 500, 1000])
    p.add_argument("--mark-speed", default="1s", choices=["1s", "3s"])
    p.add_argument("--flush-interval-ms", type=int, default=10)
    p.add_argument("--max-batch-rows", type=int, default=10_000)
    p.add_argument("--no-tp", action="store_true", help="Don't publish to tickerplant; just run collector.")
    p.add_argument("--trades-only", action="store_true", help="AggTrades only (disable pbupd/pbsnap/mark and depth subscription).")
    p.add_argument("--no-pbupd", action="store_true", help="Disable publishing pbupd updates.")
    p.add_argument("--no-pbsnap", action="store_true", help="Disable publishing pbsnap snapshots.")
    p.add_argument("--no-pmark", action="store_true", help="Disable publishing pmark stream.")
    p.add_argument("--no-bookticker", action="store_true", help="Disable publishing normalized bookTicker BBO.")
    return p.parse_args()


async def main() -> None:
    args = parse_args()

    enable_bupd = (not args.trades_only) and (not args.no_pbupd)
    enable_bsnap = (not args.trades_only) and (not args.no_pbsnap)
    enable_mark = (not args.trades_only) and (not args.no_pmark)
    enable_bookticker = (not args.trades_only) and (not args.no_bookticker)

    fh = BinanceUMPerpFeedHandler(
        symbols=args.symbols,
        tp_host=args.tp_host,
        tp_port=args.tp_port,
        ws_base=args.ws_base,
        rest_base=args.rest_base,
        depth_speed=args.depth_speed,
        snapshot_limit=args.snapshot_limit,
        mark_speed=args.mark_speed,
        flush_interval_ms=args.flush_interval_ms,
        max_batch_rows=args.max_batch_rows,
        enable_tp=(not args.no_tp),
        enable_trades=True,
        enable_bupd=enable_bupd,
        enable_bsnap=enable_bsnap,
        enable_mark=enable_mark,
        enable_bookticker=enable_bookticker,
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
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
