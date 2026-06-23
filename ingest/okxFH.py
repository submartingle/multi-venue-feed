
"""
OKX (public) spot feedhandler — third cross-venue leg for multi-venue-feed.

Offshore/Asia-centric CEX, added to test whether the OQ-6 lead ("Binance leads when US
participation is thin", docs/OQ6_POOLED_BYHOUR_FINDINGS.md) is a geography/flow effect
rather than a Binance-specific one. If an offshore venue ALSO leads in thin-US hours, the
story is flow, not Binance.

- Default channel `bbo-tbt` (no auth): tick-by-tick best bid/offer, pushed on every
  top-of-book change — the direct OKX analogue of Binance @bookTicker. Unlike Binance
  spot @bookTicker, the push carries a native millisecond exchange `ts`, so the BBO comes
  with a real exchange clock and needs NO latency calibration (cf. binanceFH_REST.py G1).
- `--channel books5` keeps a depth-5 snapshot path (pushed ~100ms) selectable for
  rollback / A-B; both channels carry the same `{bids,asks,ts}` shape, so we just read
  the top level. A lastBbo guard makes books5's repeated snapshots emit one tick per
  actual top-of-book change (matching @bookTicker semantics).
- Normalizes each tick to the SAME `bookticker` tickerplant table the Binance/Coinbase
  handlers publish to: (exch_us; recv_us; sym; venue; inst; bid; bidsz; ask; asksz), with
  venue=OKX, inst=SPOT and sym = the RAW OKX instId (e.g. `BTC-USDT`). Raw venue symbols
  are retained as the record; the q bridge (proc/live_xvenue.q) maps them to canonical
  engine symbols (BTC-USDT -> BTC) for cross-venue pairing.

OKX quotes USDT pairs (like Binance); Coinbase quotes USD. The existing Binance(USDT) vs
Coinbase(USD) comparison already crosses that basis, so OKX(USDT) is consistent with the
Binance leg — fine for lead-lag of mid MOVES.

WS LIBRARY (why websocket-client, not asyncio `websockets` like the other handlers): the
`websockets` library (v15, both its async and sync impls) sends a WebSocket protocol-level
ping that OKX rejects on connect with `1002 invalid latency probe frame` — OKX accepts only
an application-level "ping"/"pong" text and treats protocol pings as invalid. Verified
empirically: `websockets` dies instantly on OKX; `websocket-client` connects and streams
fine. Each FH is its own process, so OKX can be synchronous (websocket-client) without
touching the other handlers. Keep-alive is the app-level "ping" string on a timer thread;
"pong" replies are ignored.

Dependencies: pip install websocket-client orjson pykx
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from typing import Any, List, Optional, Tuple

import orjson
import websocket  # websocket-client (NOT the asyncio `websockets` lib — see module docstring)

# ---------- Logging ----------

log = logging.getLogger("okx_feedhandler")


# ---------- Publisher (Tickerplant IPC) ----------

class TickerplantPublisher:
    """
    Publish to the tickerplant by calling .u.upd[table; rows] over IPC (PyKX),
    fire-and-forget (wait=False). Synchronous (this FH is thread-based, not asyncio).
    The PyKX connection is created lazily on first publish so it lives in the SAME
    thread that uses it (the flush thread) — PyKX connections are not thread-safe.
    """

    def __init__(self, host: str, port: int, enabled: bool = True):
        self.host = host
        self.port = port
        self.enabled = enabled
        self._q = None
        self._kx = None
        if not enabled:
            log.warning("Publisher disabled: will not publish to tickerplant.")

    def _ensure(self) -> bool:
        if self._q is not None:
            return True
        try:
            import pykx as kx
            self._kx = kx
            self._q = kx.SyncQConnection(host=self.host, port=self.port)
            log.info("Connected to tickerplant via PyKX at %s:%d", self.host, self.port)
            return True
        except Exception as e:
            log.exception("Failed to init PyKX publisher, falling back to logging-only: %s", e)
            self.enabled = False
            return False

    def publish(self, table: str, rows: List[Tuple[Any, ...]]) -> None:
        if not rows:
            return
        if not self.enabled:
            log.info("[NO-TP] would publish %d rows to %s", len(rows), table)
            return
        if not self._ensure():
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


# ---------- Feedhandler ----------

class OkxSpotFeedHandler:
    PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        symbols: List[str],
        tp_host: str,
        tp_port: int,
        ws_url: str = PUBLIC_WS,
        channel: str = "bbo-tbt",
        flush_interval_ms: int = 10,
        max_batch_rows: int = 10_000,
        ping_interval_s: int = 20,
        enable_tp: bool = True,
        trades: bool = True,
    ):
        # OKX instIds, e.g. BTC-USDT (kept verbatim as the raw venue symbol).
        self.symbols = [s.upper() for s in symbols]
        self.ws_url = ws_url
        if channel not in ("bbo-tbt", "books5"):
            raise ValueError(f"unsupported channel: {channel}")
        self.channel = channel
        self.flush_interval_ms = flush_interval_ms
        self.max_batch_rows = max_batch_rows
        self.ping_interval_s = ping_interval_s
        self.publisher = TickerplantPublisher(tp_host, tp_port, enabled=enable_tp)
        self.trades = trades
        self._bt_batch: List[Tuple[Any, ...]] = []
        self._tr_batch: List[Tuple[Any, ...]] = []
        self._last: dict = {}  # instId -> last emitted (bid,bidsz,ask,asksz) for the dedup guard
        self._lock = threading.Lock()  # guards the batches (WS thread appends, flush thread drains)
        self._stop = threading.Event()
        self._ws: Optional[websocket.WebSocketApp] = None

    @staticmethod
    def _now_us() -> int:
        # local receive time in epoch microseconds (single clock domain, like the
        # Binance/Coinbase handlers' recv_us).
        return time.time_ns() // 1000

    @staticmethod
    def _ms_to_us(s: Any) -> Optional[int]:
        """OKX `ts` (epoch milliseconds, as a string) -> epoch us. Returns None if
        absent/unparseable so the caller can fall back to recv_us (the exact-zero skew
        signature .skew.calc's G5 guard already excludes from calibration)."""
        if s is None or s == "":
            return None
        try:
            return int(s) * 1000
        except (TypeError, ValueError):
            return None

    def _subscribe_msg(self) -> bytes:
        args = [{"channel": self.channel, "instId": s} for s in self.symbols]
        if self.trades:
            args += [{"channel": "trades", "instId": s} for s in self.symbols]
        return orjson.dumps({"op": "subscribe", "args": args})

    # ----- bbo-tbt / books5 path: top-of-book -> BBO-change ticks -----

    def _handle_book(self, instId: str, d: dict, recv_us: int) -> None:
        # bbo-tbt and books5 share the shape: bids/asks = [[px, sz, deprecated, #orders], ...].
        # Read the top level only; one-sided books can't form a BBO (skip — the q engine
        # also skips crossed/one-sided defensively on ingest, C11).
        bids = d.get("bids") or []
        asks = d.get("asks") or []
        if not bids or not asks:
            return
        bid = float(bids[0][0]); bidsz = float(bids[0][1])
        ask = float(asks[0][0]); asksz = float(asks[0][1])
        cur = (bid, bidsz, ask, asksz)
        # one tick per top-of-book CHANGE (price OR size) — @bookTicker semantics. bbo-tbt
        # already pushes only on change; the guard also collapses books5's repeated snaps.
        if self._last.get(instId) == cur:
            return
        self._last[instId] = cur
        exch_us = self._ms_to_us(d.get("ts")) or recv_us
        self._bt_batch.append((exch_us, recv_us, instId, "OKX", "SPOT", bid, bidsz, ask, asksz))

    # ----- trades path: trade prints -> `trades` table -----

    def _handle_trade(self, instId: str, d: dict, recv_us: int) -> None:
        # OKX `trades` channel `side` is the TAKER (aggressor) side — already the schema's
        # convention (Binance), so NO flip (unlike Coinbase, whose side is the maker).
        px = d.get("px"); qty = d.get("sz")
        if px is None or qty is None:
            return
        t_us = self._ms_to_us(d.get("ts")) or recv_us
        side = "B" if d.get("side") == "buy" else "S"
        try:
            tid = int(d.get("tradeId", 0))
        except (TypeError, ValueError):
            tid = 0
        self._tr_batch.append((t_us, t_us, recv_us, instId, float(px), float(qty), side, tid))

    def _dispatch(self, j: dict, recv_us: int) -> None:
        # data messages carry {"arg":{channel,instId}, "data":[...]}; event messages
        # ({"event":"subscribe"|"error", ...}) are acks/errors.
        ev = j.get("event")
        if ev is not None:
            if ev == "error":
                log.error("WS error message: %s", j)
            return  # subscribe/unsubscribe/channel-conn-count acks ignored
        arg = j.get("arg") or {}
        chan = arg.get("channel")
        instId = arg.get("instId")
        data = j.get("data") or []
        if chan == "trades":
            if self.trades:
                for d in data:
                    self._handle_trade(instId, d, recv_us)
        elif chan in ("bbo-tbt", "books5"):
            for d in data:
                self._handle_book(instId, d, recv_us)

    # ----- websocket-client callbacks -----

    def _on_open(self, ws) -> None:
        # a stale dedup map must not suppress the first ticks of a new connection
        self._last.clear()
        ws.send(self._subscribe_msg())
        log.info("Subscribed: channel=%s instIds=%s", self.channel, ",".join(self.symbols))

    def _on_message(self, ws, msg) -> None:
        recv_us = self._now_us()
        # app-level pong (and any stray ping) arrive as plain text, not JSON
        if msg == "pong" or msg == "ping":
            return
        try:
            j = orjson.loads(msg)
        except orjson.JSONDecodeError:
            return
        with self._lock:
            self._dispatch(j, recv_us)

    def _on_error(self, ws, err) -> None:
        log.warning("WS error: %s", err)

    def _on_close(self, ws, code, reason) -> None:
        log.info("WS closed (code=%s reason=%s)", code, reason)

    # ----- background loops -----

    def _flush_loop(self) -> None:
        # owns the PyKX connection (created lazily on first publish, in THIS thread).
        interval = self.flush_interval_ms / 1000.0
        while not self._stop.is_set():
            time.sleep(interval)
            with self._lock:
                bt_rows = self._drain(self._bt_batch)
                tr_rows = self._drain(self._tr_batch)
            if bt_rows:
                self.publisher.publish("bookticker", bt_rows)
            if tr_rows:
                self.publisher.publish("trades", tr_rows)

    def _ping_loop(self) -> None:
        # OKX keep-alive is the application-level "ping" STRING (a WS protocol ping is
        # rejected as an "invalid latency probe frame" — see module docstring).
        while not self._stop.wait(self.ping_interval_s):
            ws = self._ws
            if ws is None:
                continue
            try:
                ws.send("ping")
            except Exception:
                pass  # socket going down; the run loop will reconnect

    def _drain(self, buf: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        if not buf:
            return []
        n = min(len(buf), self.max_batch_rows)
        rows = buf[:n]
        del buf[:n]
        return rows

    def run(self) -> None:
        flush_t = threading.Thread(target=self._flush_loop, name="okx-flush", daemon=True)
        ping_t = threading.Thread(target=self._ping_loop, name="okx-ping", daemon=True)
        flush_t.start()
        ping_t.start()
        backoff = 0.25
        try:
            while not self._stop.is_set():
                log.info("Connecting WS: %s", self.ws_url)
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                # ping_interval=0 -> NO WS-protocol ping (OKX rejects those); we send the
                # app-level "ping" string from _ping_loop instead.
                self._ws.run_forever(ping_interval=0)
                self._ws = None
                if self._stop.is_set():
                    break
                log.warning("WS disconnected; reconnecting after %.2fs", backoff)
                self._stop.wait(backoff)
                backoff = min(backoff * 2.0, 10.0)
        finally:
            self._stop.set()
            self.publisher.close()

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True, help="OKX instIds, e.g. BTC-USDT ETH-USDT")
    p.add_argument("--tp-host", default="127.0.0.1")
    p.add_argument("--tp-port", type=int, default=5010)
    p.add_argument("--ws-url", default=OkxSpotFeedHandler.PUBLIC_WS)
    p.add_argument("--channel", choices=["bbo-tbt", "books5"], default="bbo-tbt",
                   help="bbo-tbt (default): tick-by-tick top of book; "
                        "books5: depth-5 snapshot ~100ms (rollback/A-B)")
    p.add_argument("--flush-interval-ms", type=int, default=10)
    p.add_argument("--max-batch-rows", type=int, default=10_000)
    p.add_argument("--no-tp", action="store_true", help="Don't publish to tickerplant; just run collector.")
    p.add_argument("--no-trades", action="store_true",
                   help="Don't subscribe `trades` / publish `trades` (default: on).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    fh = OkxSpotFeedHandler(
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
    try:
        fh.run()
    except KeyboardInterrupt:
        fh.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
