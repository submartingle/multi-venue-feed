
"""

A practical Binance Spot feedhandler:
- Subscribes to <symbol>@trade and <symbol>@depth@100ms (combined stream)
- Maintains local book sync state using Binance's documented snapshot+buffer+apply procedure
- Publishes batches to a kdb+ tickerplant via .u.upd over IPC (PyKX)

Notes:
- This is NOT an HFT engine. It's a production-style collector/normalizer for a kdb tick plant demo.
- Keep universe small for live. Use replay to load-test at scale.

Dependencies:
  pip install websockets httpx orjson pykx
  
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
import orjson
import websockets

# ---------- Logging ----------

log = logging.getLogger("binance_feedhandler")


# ---------- Exceptions ----------

class ResyncRequired(Exception):
    """Raised when the book stream has a gap and we must resync via snapshot."""


# ---------- Publisher (Tickerplant IPC) ----------


class TickerplantPublisher:
    """
    Publish to tickerplant by calling .u.upd[table; rows] over IPC.

    **Important (PyKX detail):**
    - `kx.AsyncQConnection` must be *awaited* to initialize; creating it in a normal __init__
      without awaiting leaves it uninitialized and raises `UninitializedConnection`.
    - For this feedhandler we don't need a response from q, so the simplest + robust approach
      is to use `kx.SyncQConnection` and send messages with `wait=False` (fire-and-forget).
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
            # Sync connection is initialized immediately; we still publish async by using wait=False on each call.
            self._q = kx.SyncQConnection(host=host, port=port)
            log.info("Connected to tickerplant via PyKX at %s:%d", host, port)
        except Exception as e:
            log.exception("Failed to init PyKX publisher, falling back to logging-only: %s", e)
            self.enabled = False

    async def publish(self, table: str, rows: List[Tuple[Any, ...]]) -> None:
        """
        Fire-and-forget publish to `.u.upd` on the q side.
        This does *not* await a response from q.
        """
        if not rows:
            return

        if (not self.enabled) or (self._q is None):
            log.info("[NO-TP] would publish %d rows to %s", len(rows), table)
            return

        kx = self._kx
        try:
            # Execute remotely: {.u.upd[x;y]}[table; rows]
            # wait=False => no response expected
            self._q("{.u.upd[x;y]}", kx.SymbolAtom(table), rows, wait=False)
        except Exception:
            log.exception("Failed publishing %d rows to %s", len(rows), table)

    def close(self) -> None:
        try:
            if self._q is not None:
                self._q.close()
        except Exception:
            pass


# ---------- Book Sync State ----------


@dataclass
class BookSnapshot:
    last_update_id: int
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]


class BookSync:
    """
    Tracks Binance's local order book synchronization state for ONE symbol.

    We ingest diff-depth events (depthUpdate), buffer them until we have a REST snapshot,
    then apply according to Binance rules:

    - Buffer events; note U of first event.
    - Get snapshot; if snapshot.lastUpdateId < first_U, repeat snapshot.
    - Drop buffered events with u <= snapshot.lastUpdateId.
    - First remaining buffered event should have snapshot.lastUpdateId within [U;u].
    - Apply events:
        * if u < local_lastUpdateId: ignore
        * if U > local_lastUpdateId + 1: missed events => resync
        * apply price levels, then set local_lastUpdateId = u
    """

    def __init__(self, symbol: str, rest_base: str, snapshot_limit: int = 1000):
        self.symbol = symbol.upper()
        self._rest_base = rest_base.rstrip("/")
        self._snapshot_limit = snapshot_limit

        self._lock = asyncio.Lock()
        self._buffer: Deque[dict] = deque(maxlen=20000)  # avoid unbounded growth
        self._first_U: Optional[int] = None

        self.ready: bool = False
        self.last_update_id: Optional[int] = None

        # Optional in-memory book (for sanity checks / monitoring)
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

        self._resync_task: Optional[asyncio.Task] = None

    def buffer_event(self, ev: dict) -> None:
        # Called from WS loop; keep it very light.
        if self._first_U is None:
            self._first_U = int(ev["U"])
        self._buffer.append(ev)

    async def ensure_ready(self) -> Optional[BookSnapshot]:
        """
        Ensure we have a snapshot + have applied buffered events.
        Returns a BookSnapshot when we (re)initialize, else None.
        """
        async with self._lock:
            if self.ready:
                return None
            if self._resync_task is None or self._resync_task.done():
                self._resync_task = asyncio.create_task(self._resync())
        return await self._resync_task

    async def _resync(self) -> BookSnapshot:
        """
        Perform snapshot+buffer reconciliation and mark book as ready.
        """
        # Wait briefly for first buffered depth event (so we have first_U)
        t0 = time.time()
        while self._first_U is None:
            await asyncio.sleep(0.01)
            if time.time() - t0 > 5.0:
                raise RuntimeError(f"{self.symbol}: no depth events seen within 5s; cannot sync book")

        first_U = int(self._first_U)

        # Retry snapshot until lastUpdateId >= first_U (per docs)
        snapshot = await self._fetch_snapshot_until(first_U)

        # Discard buffered events where u <= snapshot.lastUpdateId
        while self._buffer and int(self._buffer[0]["u"]) <= snapshot.last_update_id:
            self._buffer.popleft()

        # Now first buffered event should have lastUpdateId within [U;u] (ideally)
        if self._buffer:
            head = self._buffer[0]
            U0, u0 = int(head["U"]), int(head["u"])
            if not (U0 <= snapshot.last_update_id <= u0):
                log.warning(
                    "%s: buffer head doesn't bracket lastUpdateId. head[U,u]=[%d,%d], lastUpdateId=%d. Proceeding.",
                    self.symbol, U0, u0, snapshot.last_update_id
                )

        # Set local book to snapshot
        self.bids = {px: qty for px, qty in snapshot.bids if qty != 0.0}
        self.asks = {px: qty for px, qty in snapshot.asks if qty != 0.0}
        self.last_update_id = snapshot.last_update_id

        # Apply buffered events in order
        while self._buffer:
            ev = self._buffer.popleft()
            self._apply_depth_event(ev, update_book=True)

        self.ready = True
        log.info("%s: book synced at lastUpdateId=%d (bids=%d asks=%d)",
                 self.symbol, self.last_update_id, len(self.bids), len(self.asks))
        return snapshot

    async def _fetch_snapshot_until(self, first_U: int) -> BookSnapshot:
        """
        Fetch snapshot; if snapshot.lastUpdateId < first_U, repeat (per docs).
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(1, 20):
                url = f"{self._rest_base}/api/v3/depth"
                params = {"symbol": self.symbol, "limit": self._snapshot_limit}
                r = await client.get(url, params=params)
                r.raise_for_status()
                js = r.json()

                last_update_id = int(js["lastUpdateId"])
                bids = [(float(p), float(q)) for p, q in js.get("bids", [])]
                asks = [(float(p), float(q)) for p, q in js.get("asks", [])]

                if last_update_id >= first_U:
                    return BookSnapshot(last_update_id=last_update_id, bids=bids, asks=asks)

                # Snapshot too old; retry quickly
                await asyncio.sleep(min(0.05 * attempt, 1.0))

        raise RuntimeError(f"{self.symbol}: failed to get snapshot with lastUpdateId>=first_U after retries")

    def apply_or_raise(self, ev: dict) -> None:
        """
        Apply a depthUpdate event if ready, else buffer it.
        Might raise ResyncRequired on gaps.
        """
        if not self.ready:
            self.buffer_event(ev)
            return
        self._apply_depth_event(ev, update_book=True)

    def _apply_depth_event(self, ev: dict, update_book: bool) -> None:
        """
        Apply the Binance update-id rules, optionally mutating bids/asks dicts.
        """
        assert self.last_update_id is not None

        U = int(ev["U"])
        u = int(ev["u"])

        # Rule 1: if event's u < local last id => ignore
        if u < self.last_update_id:
            return

        # Rule 2: if event's U > local last id + 1 => gap => resync
        if U > self.last_update_id + 1:
            raise ResyncRequired(f"{self.symbol}: gap detected (U={U} > last+1={self.last_update_id+1})")

        # Rule 3: apply levels, then update local last id
        if update_book:
            for px_s, qty_s in ev.get("b", []):  # bids
                px = float(px_s)
                qty = float(qty_s)
                if qty == 0.0:
                    self.bids.pop(px, None)
                else:
                    self.bids[px] = qty

            for px_s, qty_s in ev.get("a", []):  # asks
                px = float(px_s)
                qty = float(qty_s)
                if qty == 0.0:
                    self.asks.pop(px, None)
                else:
                    self.asks[px] = qty

        self.last_update_id = u

    def reset_for_resync(self, seed_event: Optional[dict] = None) -> None:
        """
        Drop local state and begin buffering again.
        """
        self.ready = False
        self.last_update_id = None
        self.bids.clear()
        self.asks.clear()
        self._buffer.clear()
        self._first_U = None
        if seed_event is not None:
            self.buffer_event(seed_event)


# ---------- Feedhandler ----------

class BinanceSpotFeedHandler:
    def __init__(
        self,
        symbols: List[str],
        tp_host: str,
        tp_port: int,
        ws_base: str = "wss://stream.binance.com:9443",
        rest_base: str = "https://api.binance.com",
        depth_speed: str = "100ms",
        snapshot_limit: int = 1000,
        time_unit: str = "MICROSECOND",
        flush_interval_ms: int = 10,
        max_batch_rows: int = 10_000,
        enable_tp: bool = True,
        enable_trades: bool = True,
        enable_bupd: bool = True,
        enable_bsnap: bool = True,
        enable_bookticker: bool = True,
        enable_bbodepth: bool = True,
        rest_snap_interval_s: int = 3600,
        lat_window: int = 200,
        lat_min_samples: int = 20,
        lat_stale_s: int = 60,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")
        self.depth_speed = depth_speed
        self.snapshot_limit = snapshot_limit
        self.time_unit = time_unit
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_rows = max_batch_rows
        # ---- table toggles (useful for debugging) ----
        self.enable_trades = enable_trades
        self.enable_bupd = enable_bupd
        self.enable_bsnap = enable_bsnap
        # bookTicker: Binance pushes best bid/ask directly (no book maintenance needed).
        # This is the normalized BBO consumed downstream by the multi-venue-feed engine.
        self.enable_bookticker = enable_bookticker
        # Depth-derived BBO DIAGNOSTIC (bbodepth), parallel to bookticker: top of book
        # reconstructed from the maintained @depth book, stamped with the depthUpdate's
        # TRUE exchange event time E. Spot bookTicker has no E (its exch_us is the G1
        # trade-stream ESTIMATE), so matching the two streams' BBO changes measures the
        # estimate's per-tick error directly (sim/depth_bbo_audit.q). NOT consumed by
        # the engine, and NOT a better scoring clock: E is stamped at the ~100ms
        # throttle flush, a one-sided 0-100ms lateness the cross-venue sign test cannot
        # wash out (unlike the estimate's median-zero jitter).
        self.enable_bbodepth = enable_bbodepth
        # If no book-derived table is enabled, we don't need to subscribe to depth at all.
        self.enable_book = (self.enable_bupd or self.enable_bsnap or self.enable_bbodepth)

        self.rest_snap_interval_s = int(rest_snap_interval_s)


        self.publisher = TickerplantPublisher(tp_host, tp_port, enabled=enable_tp)

        self.books: Dict[str, BookSync] = {}
        if self.enable_book:
            self.books = {
                s: BookSync(s, rest_base=self.rest_base, snapshot_limit=self.snapshot_limit)
                for s in self.symbols
            }

        # Batches for TP
        self._trade_batch: List[Tuple[Any, ...]] = []
        self._bupd_batch: List[Tuple[Any, ...]] = []
        self._bsnap_batch: List[Tuple[Any, ...]] = []
        self._bt_batch: List[Tuple[Any, ...]] = []
        self._bbod_batch: List[Tuple[Any, ...]] = []
        # last known top of book per symbol for the bbodepth diagnostic; None = unknown
        # (cold start / invalidated by resync) -> re-baselined from the book before the
        # next applied depth event, WITHOUT emitting (emit means "this event changed it").
        self._depth_bbo: Dict[str, Optional[Tuple[float, float, float, float]]] = {
            s: None for s in self.symbols
        }

        # ---- spot exchange-clock estimate (calibration) ----
        # Spot @bookTicker carries NO exchange timestamp, so we cannot stamp the BBO with
        # a true event time the way perp does. The @trade stream (same combined websocket)
        # DOES carry exchange event time E, so recv_us - E measures this connection's
        # transport+exchange-emit latency. We track a rolling per-symbol median of that
        # offset and subtract it from bookTicker recv_us to recover an estimated exchange
        # time. This puts spot BBO into the same (exchange-time) clock domain as perp,
        # removing the systematic spot/perp latency asymmetry that otherwise masquerades as
        # a lead-lag signal. See docs/LIVE_RUN_2026-06-09_FINDINGS.md §6-7.
        self.lat_window = int(lat_window)            # trade samples per symbol in the median
        self.lat_min_samples = int(lat_min_samples)  # below this -> low confidence, fall back
        self.lat_stale_us = int(lat_stale_s) * 1_000_000  # ignore offset if no recent trade
        self._lat_samples: Dict[str, Deque[int]] = {s: deque(maxlen=self.lat_window) for s in self.symbols}
        self._lat_offset_us: Dict[str, Optional[int]] = {s: None for s in self.symbols}
        self._lat_last_us: Dict[str, int] = {s: 0 for s in self.symbols}

        self._stop = asyncio.Event()

    def _combined_stream_url(self) -> str:
        streams = []
        for s in self.symbols:
            sym = s.lower()
            if self.enable_trades:
                streams.append(f"{sym}@trade")
            if self.enable_book:
                streams.append(f"{sym}@depth@{self.depth_speed}")
            if self.enable_bookticker:
                streams.append(f"{sym}@bookTicker")

        stream_str = "/".join(streams)

        # Combined stream URL + timeUnit option (so timestamps can be microseconds)
        # wss://.../stream?streams=...&timeUnit=MICROSECOND
        return f"{self.ws_base}/stream?streams={stream_str}&timeUnit={self.time_unit}"

    @staticmethod
    def _now_us() -> int:
        return time.time_ns() // 1000

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
            # Ensure IPC connection is closed on shutdown
            try:
                self.publisher.close()
            except Exception:
                pass

    async def _run_once(self) -> None:
        url = self._combined_stream_url()
        log.info("Connecting WS: %s", url)

        # websockets library auto-replies to ping frames with pong frames,
        # which Binance expects (pong payload should mirror ping payload).
        async with websockets.connect(url, max_size=8 * 1024 * 1024) as ws:
            # Kick off initial resync tasks (will wait for first depth events)
            resync_tasks: List[asyncio.Task] = []
            if self.enable_book:
                resync_tasks = [asyncio.create_task(self.books[s].ensure_ready()) for s in self.symbols]

            # Publisher flush loop
            flush_task = asyncio.create_task(self._flush_loop())

            rest_snap_task = None
            if self.rest_snap_interval_s > 0 and self.enable_bsnap and self.enable_book:
                rest_snap_task = asyncio.create_task(self._periodic_rest_snapshots())

            try:
                async for msg in ws:
                    recv_us = self._now_us()
                    j = orjson.loads(msg)

                    payload = j.get("data", j)  # combined streams wrap as {"stream":..., "data":...}
                    stream = j.get("stream", "")  # e.g. "btcusdt@bookTicker"

                    et = payload.get("e")
                    if et == "trade":
                        if self.enable_trades:
                            self._handle_trade(payload, recv_us)
                    elif et == "depthUpdate":
                        if self.enable_book:
                            await self._handle_depth(payload, recv_us)
                    elif stream.endswith("@bookTicker"):
                        # Spot bookTicker has no "e" field, so dispatch by stream name.
                        if self.enable_bookticker:
                            self._handle_bookticker(payload, recv_us)
                    else:
                        # Ignore other messages
                        continue
            finally:
                flush_task.cancel()
                if rest_snap_task is not None:
                    rest_snap_task.cancel()
                for t in resync_tasks:
                    t.cancel()

    def _handle_trade(self, ev: dict, recv_us: int) -> None:
        if not self.enable_trades:
            return
        """
        Trade payload fields (spot):
          E event time, s symbol, t tradeId, p price, q qty, T trade time, m isBuyerMaker
        """
        sym = ev["s"]
        px = float(ev["p"])
        qty = float(ev["q"])
        tid = int(ev["t"])
        exch_us = int(ev.get("E", 0))
        trade_us= int(ev.get("T", ev.get("E",0)))

        # Feed the spot exchange-clock estimator: recv_us - E is this connection's
        # transport latency PLUS the local-vs-exchange clock error, either sign. Keep ALL
        # samples: median(recv-E) = median_transport + clock_error, and subtracting that
        # from recv cancels the clock error by construction (eventTs lands in the exchange
        # frame +/- transport jitter). Rejecting negative samples starves the median after
        # suspend/resume clock drift (2026-06-09: ~96% rejected -> stale -> 100% fallback)
        # and biases it high even under mild skew. Shared by _spot_exch_us.
        if exch_us > 0:
            self._lat_samples[sym].append(recv_us - exch_us)
            self._lat_last_us[sym] = recv_us
            if len(self._lat_samples[sym]) >= self.lat_min_samples:
                self._lat_offset_us[sym] = int(statistics.median(self._lat_samples[sym]))

        # side: if buyer is maker, then aggressor is sell; else buy
        side = "S" if ev.get("m", False) else "B"

        # Minimal row shape. Keep timestamps as integer microseconds for easy typing on q side.


        self._trade_batch.append((exch_us, trade_us, recv_us, sym, px, qty, side, tid))

    def _spot_exch_us(self, sym: str, recv_us: int) -> int:
        """
        Estimated spot exchange event time = recv_us - rolling median trade latency.
        Falls back to recv_us (skew 0, as before) when the estimate isn't trustworthy:
        too few samples yet, or no recent trade (offset gone stale, e.g. after a
        reconnect or in a thin-trade symbol). The offset may legitimately be NEGATIVE
        (local clock behind the exchange clock), in which case the estimate exceeds
        recv_us — do NOT clamp; downstream skew calibration (.skew.calc) expects the
        consistent exchange-frame stamp, sign included.
        """
        off = self._lat_offset_us.get(sym)
        if off is None:
            return recv_us  # cold start: not enough trade samples yet
        if recv_us - self._lat_last_us.get(sym, 0) > self.lat_stale_us:
            return recv_us  # offset stale: don't trust it
        return recv_us - off

    def _handle_bookticker(self, ev: dict, recv_us: int) -> None:
        """
        Spot bookTicker payload fields:
          u updateId, s symbol, b best bid px, B best bid qty, a best ask px, A best ask qty
        Spot bookTicker carries NO exchange event time. We estimate one via the @trade
        stream's latency (see _spot_exch_us / __init__), putting spot BBO into the same
        exchange-time clock domain as perp. Until the estimate is ready it falls back to
        recv_us (the previous behaviour). Row shape matches the `bookticker` TP table:
          (exch_us; recv_us; sym; venue; inst; bid; bidsz; ask; asksz)
        """
        if not self.enable_bookticker:
            return
        sym = ev["s"]
        bid = float(ev["b"]); bidsz = float(ev["B"])
        ask = float(ev["a"]); asksz = float(ev["A"])
        exch_us = self._spot_exch_us(sym, recv_us)
        self._bt_batch.append((exch_us, recv_us, sym, "BINANCE", "SPOT", bid, bidsz, ask, asksz))

    def _emit_depth_bbo(self, sym: str, exch_us: int, recv_us: int, u: int) -> None:
        """
        Diagnostic: append a bbodepth row whenever the maintained depth book's top of
        book changed (price OR size — @bookTicker semantics, same emit rule as the
        Coinbase L2Book), stamped with the depthUpdate's true exchange time E.
        One-sided/empty books emit nothing (no BBO to stamp). A transiently crossed
        book is emitted as-is — nothing trades off this table; the audit filters.
        Row shape matches the `bbodepth` TP table:
          (exch_us; recv_us; sym; bid; bidsz; ask; asksz; u)
        """
        book = self.books[sym]
        if not (book.bids and book.asks):
            return
        bid = max(book.bids)
        ask = min(book.asks)
        bbo = (bid, book.bids[bid], ask, book.asks[ask])
        if bbo == self._depth_bbo.get(sym):
            return
        self._depth_bbo[sym] = bbo
        self._bbod_batch.append((exch_us, recv_us, sym) + bbo + (u,))

    async def _handle_depth(self, ev: dict, recv_us: int) -> None:
        if not self.enable_book:
            return
        """
        depthUpdate fields:
          E event time, s symbol, U first update id, u final update id, b bids, a asks
        """
        sym = ev["s"]
        book = self.books[sym]

        # If not ready yet: buffer + attempt (async) resync
        if not book.ready:
            book.buffer_event(ev)
            # ensure_ready returns a snapshot when (re)initialized
            try:
                snap = await book.ensure_ready()
                if snap is not None:
                    # Publish snapshot row (event-level)
                    # bsnap: (ts_recv_us, sym, lastUpdateId, bids, asks)
                    if self.enable_bsnap:

                        self._bsnap_batch.append((
                        recv_us, sym, int(snap.last_update_id), snap.bids, snap.asks
                    ))
                    # Book replaced wholesale: invalidate the diagnostic BBO marker; it
                    # is re-baselined from the fresh book before the next applied event
                    # (snapshots themselves are not emitted — REST fetch time is not an
                    # exchange event time).
                    self._depth_bbo[sym] = None
            except Exception:
                log.exception("%s: initial book sync failed; will retry on next events", sym)
            return

        # Ready: apply, detect gaps, resync if needed
        exch_us = int(ev.get("E", 0))
        U = int(ev["U"])
        u = int(ev["u"])
        bids = ev.get("b", [])
        asks = ev.get("a", [])

        # Cold start / post-resync: baseline the diagnostic BBO marker from the book
        # BEFORE applying this event, so an emit always means "THIS event changed the
        # top" — never pre-existing state re-stamped with this event's E (which would
        # poison the timing diagnostic with rows whose E postdates the state).
        if self.enable_bbodepth and self._depth_bbo.get(sym) is None and book.bids and book.asks:
            b0 = max(book.bids)
            a0 = min(book.asks)
            self._depth_bbo[sym] = (b0, book.bids[b0], a0, book.asks[a0])

        try:
            book.apply_or_raise(ev)
        except ResyncRequired as e:
            log.warning("%s: %s -> resync", sym, e)
            book.reset_for_resync(seed_event=ev)
            # Schedule resync; snapshot row will be published when ensure_ready returns
            try:
                snap = await book.ensure_ready()
                if snap is not None:
                    if self.enable_bsnap:

                        self._bsnap_batch.append((recv_us, sym, int(snap.last_update_id), snap.bids, snap.asks))
                    # As above: resync invalidates the last-emitted depth BBO.
                    self._depth_bbo[sym] = None
            except Exception:
                log.exception("%s: resync failed; will retry later", sym)
            return

        # Publish event-level delta (compact)
        # bupd: (ts_exch_us, ts_recv_us, sym, U, u, bids, asks)
        if self.enable_bupd:

            self._bupd_batch.append((exch_us, recv_us, sym, U, u, bids, asks))

        # Diagnostic depth-derived BBO. Only when THIS event applied (after apply,
        # last_update_id == u): a stale event (u < local last id) is a no-op on the
        # book and must not stamp the current BBO with its older E.
        if self.enable_bbodepth and book.last_update_id == u:
            self._emit_depth_bbo(sym, exch_us, recv_us, u)

    async def _periodic_rest_snapshots(self) -> None:
        """Periodically fetch REST /depth snapshots and append to the *same* bsnap batch.

        This reuses the existing bsnap publish path (same batch, same flush loop, same .u.upd call),
        so types/shapes match your working live bsnap rows.

        Disable with --rest-snap-interval-s 0.
        """
        if (not self.enable_bsnap) or (not self.enable_book) or self.rest_snap_interval_s <= 0:
            return

        # A single reusable client is cheaper and reduces jitter.
        async with httpx.AsyncClient(timeout=10.0) as client:
            while True:
                await asyncio.sleep(self.rest_snap_interval_s)
                recv_us = self._now_us()
                for sym in self.symbols:
                    try:
                        url = f"{self.rest_base}/api/v3/depth"
                        params = {"symbol": sym, "limit": self.snapshot_limit}
                        r = await client.get(url, params=params)
                        r.raise_for_status()
                        js = r.json()

                        last_update_id = int(js["lastUpdateId"])
                        bids = [(float(p), float(q)) for p, q in js.get("bids", [])]
                        asks = [(float(p), float(q)) for p, q in js.get("asks", [])]

                        # bsnap row shape: (recv_us; sym; lastUpdateId; bids; asks)
                        self._bsnap_batch.append((recv_us, sym, last_update_id, bids, asks))
                        log.info("REST snapshot -> bsnap sym=%s lastUpdateId=%d", sym, last_update_id)
                    except Exception:
                        log.exception("REST snapshot fetch failed for %s", sym)

    async def _flush_loop(self) -> None:
        """
        Flush batches every flush_interval_ms or when max_batch_rows hit.
        """
        interval = self.flush_interval_ms / 1000.0
        while True:
            await asyncio.sleep(interval)

            # Drain quickly to local vars to minimize time holding references.
            trade_rows = self._drain(self._trade_batch) if self.enable_trades else []
            bupd_rows = self._drain(self._bupd_batch) if self.enable_bupd else []
            bsnap_rows = self._drain(self._bsnap_batch) if self.enable_bsnap else []
            bt_rows = self._drain(self._bt_batch) if self.enable_bookticker else []
            bbod_rows = self._drain(self._bbod_batch) if self.enable_bbodepth else []

            # Publish (fire and forget)
            if trade_rows:
                await self.publisher.publish("trades", trade_rows)
            if bupd_rows:
                await self.publisher.publish("bupd", bupd_rows)
            if bsnap_rows:
                await self.publisher.publish("bsnap", bsnap_rows)
            if bt_rows:
                await self.publisher.publish("bookticker", bt_rows)
            if bbod_rows:
                await self.publisher.publish("bbodepth", bbod_rows)

    def _drain(self, buf: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        if not buf:
            return []
        if len(buf) > self.max_batch_rows:
            # Prevent pathological growth if TP is down.
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
    p.add_argument("--ws-base", default="wss://stream.binance.com:9443")
    p.add_argument("--rest-base", default="https://api.binance.com")
    p.add_argument("--depth-speed", default="100ms", choices=["100ms", "1000ms"])
    p.add_argument("--snapshot-limit", type=int, default=1000, choices=[100, 500, 1000, 5000])
    p.add_argument("--time-unit", default="MICROSECOND", choices=["MILLISECOND", "MICROSECOND"])
    p.add_argument("--flush-interval-ms", type=int, default=10)
    p.add_argument("--max-batch-rows", type=int, default=10_000)
    p.add_argument("--no-tp", action="store_true", help="Don't publish to tickerplant; just run collector.")
    p.add_argument("--trades-only", action="store_true", help="Trades only (disable bupd/bsnap and depth subscription).")
    p.add_argument("--no-bupd", action="store_true", help="Disable publishing bupd updates.")
    p.add_argument("--no-bsnap", action="store_true", help="Disable publishing bsnap snapshots.")
    p.add_argument("--no-bookticker", action="store_true", help="Disable publishing normalized bookTicker BBO.")
    p.add_argument("--no-bbodepth", action="store_true",
                   help="Disable the depth-derived BBO diagnostic table (bbodepth).")
    p.add_argument("--rest-snap-interval-s", type=int, default=3600,
                   help="Periodically fetch REST /depth snapshot and publish to bsnap (seconds). 0 disables.")
    p.add_argument("--lat-window", type=int, default=200,
                   help="Trade samples per symbol in the rolling spot exchange-clock latency median.")
    p.add_argument("--lat-min-samples", type=int, default=20,
                   help="Min trade samples before trusting the spot latency offset (else stamp recv_us).")
    p.add_argument("--lat-stale-s", type=int, default=60,
                   help="Ignore the spot latency offset if no trade seen within this many seconds.")
    return p.parse_args()


async def main() -> None:
    args = parse_args()

    enable_bupd = (not args.trades_only) and (not args.no_bupd)
    enable_bsnap = (not args.trades_only) and (not args.no_bsnap)
    enable_bookticker = (not args.trades_only) and (not args.no_bookticker)
    enable_bbodepth = (not args.trades_only) and (not args.no_bbodepth)

    fh = BinanceSpotFeedHandler(
        symbols=args.symbols,
        tp_host=args.tp_host,
        tp_port=args.tp_port,
        ws_base=args.ws_base,
        rest_base=args.rest_base,
        depth_speed=args.depth_speed,
        snapshot_limit=args.snapshot_limit,
        time_unit=args.time_unit,
        flush_interval_ms=args.flush_interval_ms,
        max_batch_rows=args.max_batch_rows,
        enable_tp=(not args.no_tp),
        enable_trades=True,
        enable_bupd=enable_bupd,
        enable_bsnap=enable_bsnap,
        enable_bookticker=enable_bookticker,
        enable_bbodepth=enable_bbodepth,
        rest_snap_interval_s=args.rest_snap_interval_s,
        lat_window=args.lat_window,
        lat_min_samples=args.lat_min_samples,
        lat_stale_s=args.lat_stale_s,
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
