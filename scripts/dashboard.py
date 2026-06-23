#!/usr/bin/env python3
"""
Multi-venue-feed live monitor dashboard.

Connects to the live bridge q process via PyKX, calls .monitor.snap[],
and renders a live-updating terminal dashboard with system health,
data-flow statistics, leadership scores, and feed-health metrics.

Usage:
    python3 scripts/dashboard.py                          # default: localhost:5099, 60s refresh
    python3 scripts/dashboard.py --port 5099 --refresh 30
    python3 scripts/dashboard.py --host 192.168.1.10 --port 5099

Dependencies: pip install rich pykx
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# --- Rich imports ---
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

# --- Configuration -----------------------------------------------------------
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5099
DEFAULT_REFRESH = 60  # seconds


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="multi-venue-feed live dashboard")
    p.add_argument("--host", default=DEFAULT_HOST, help=f"bridge host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"bridge IPC port (default: {DEFAULT_PORT})")
    p.add_argument("--refresh", type=int, default=DEFAULT_REFRESH, help=f"refresh interval in seconds (default: {DEFAULT_REFRESH})")
    p.add_argument("--once", action="store_true", help="print a single static snapshot and exit (works in a pipe / Claude Code '!'; the full-screen live TUI needs a real terminal)")
    return p.parse_args()


# --- q connection ------------------------------------------------------------

class QConnection:
    """Lazy-connecting PyKX handle to the live bridge, with timeout."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._handle = None
        self._kx = None

    def _connect(self) -> bool:
        try:
            import pykx as kx
            self._kx = kx
            self._handle = kx.SyncQConnection(host=self._host, port=self._port, timeout=5.0)
            return True
        except Exception:
            self._handle = None
            return False

    def query(self, expr: str) -> Any:
        """Run a q expression and return the result, or raise on failure.

        On any failure of the live handle (bridge restarted / socket dropped) the
        handle is discarded so the NEXT call reconnects. Without this reset a dead
        handle is reused forever — the dashboard would stay on NO CONNECTION (and
        can block) even after the bridge comes back."""
        if self._handle is None:
            if not self._connect():
                raise ConnectionError(f"cannot connect to q at {self._host}:{self._port}")
        try:
            return self._handle(expr)
        except Exception:
            self.close()   # drop the dead handle; next query() reconnects
            raise

    def is_connected(self) -> bool:
        if self._handle is not None:
            try:
                self._handle("1")
                return True
            except Exception:
                self._handle = None
        return self._connect()

    def close(self):
        if self._handle is not None:
            try:
                self._handle.close()
            except Exception:
                pass
            self._handle = None


# --- data fetching -----------------------------------------------------------

def _is_columnar(d: dict) -> bool:
    """True if dict looks like a columnar table: all values are equal-length lists."""
    if not isinstance(d, dict) or len(d) == 0:
        return False
    vals = list(d.values())
    return all(isinstance(v, (list, tuple)) for v in vals) and \
           len(set(len(v) for v in vals)) == 1


def _transpose(d: dict) -> list:
    """Columnar {col: [vals]} → list of dicts [{col: val}, ...]."""
    keys = list(d.keys())
    return [dict(zip(keys, row)) for row in zip(*d.values())]


def _unwrap(val):
    """Recursively convert PyKX typed objects to plain Python dicts/lists/scalars."""
    # .py may be a property or a method depending on PyKX version
    if hasattr(val, 'py'):
        pv = val.py
        if callable(pv):
            return _unwrap(pv())
        return _unwrap(pv)
    if hasattr(val, 'items'):         # dict-like (PyKX Dictionary or plain dict)
        d = {str(k): _unwrap(v) for k, v in val.items()}
        if _is_columnar(d):
            return _transpose(d)
        return d
    if isinstance(val, (list, tuple)):
        return [_unwrap(v) for v in val]
    # PyKX atoms (SymbolAtom, LongAtom, etc.) — str() gives the value
    t = type(val).__name__
    if 'Atom' in t or 'Vector' in t:
        try:
            pv = val.py
            return pv() if callable(pv) else pv
        except Exception:
            return str(val)
    return val


def _row_get(row, key: str, default=None):
    """Get a value from a table row (plain dict, PyKX dict, or list)."""
    if hasattr(row, 'get'):
        return row.get(key, default)
    if hasattr(row, 'items'):
        return dict(row.items()).get(key, default)
    if isinstance(row, (list, tuple)):
        return default  # list rows need index access, not key — caller handles that
    return default


def fetch_metrics(q: QConnection) -> Optional[Dict]:
    """Call .monitor.snap[] on the bridge and return the unwrapped dict, or None."""
    try:
        raw = q.query(".monitor.snap[]")
        return _unwrap(raw)
    except Exception:
        return None


def _ok(x) -> bool:
    """True if x is a usable number: not None and not NaN.

    PyKX surfaces q null floats (0n) as Python float('nan'), and `nan is not None`
    is True — so a plain `is not None` check lets NaN through into formatting (→
    'nan%'). Use this everywhere a q-sourced float is formatted. (NaN != NaN.)"""
    return x is not None and not (isinstance(x, float) and x != x)


def fmt_age_ms(ms: float) -> str:
    """Format an age in ms to a human-readable string."""
    if not _ok(ms):
        return "—"
    if ms < 1000:
        return f"{ms:.0f}ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f}s"
    if ms < 3_600_000:
        return f"{ms / 60_000:.1f}m"
    return f"{ms / 3_600_000:.1f}h"


def fmt_uptime(ms: int) -> str:
    """Format uptime in ms to a human-readable string."""
    if ms is None:
        return "—"
    s = ms // 1000
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"


def fmt_bps(x: float) -> str:
    """Format a bps value."""
    if x is None:
        return "—"
    return f"{x:.2f}"


def fmt_count(n: int) -> str:
    """Format a count with thousands separators."""
    if n is None:
        return "—"
    return f"{n:,}"


def fmt_count_h(n) -> str:
    """Compact human-readable count: 1.2M, 4.4k, 932. Null/NaN -> em dash."""
    if not _ok(n):
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    if n >= 10_000:
        return f"{n / 1e3:.1f}k"
    return f"{n:,}"


def traffic_light(status: str) -> Text:
    """Return a colored traffic-light indicator."""
    colors = {"green": "green", "yellow": "yellow", "red": "red"}
    symbols = {"green": "●", "yellow": "●", "red": "●"}
    c = colors.get(status, "white")
    return Text(symbols.get(status, "○"), style=c)


# --- rendering ---------------------------------------------------------------

def build_header(d: Optional[Dict], status: str, status_msg: str, refresh_s: int) -> Panel:
    """Top bar: project name, time, uptime, traffic light, refresh interval."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    parts = []
    parts.append(Text("multi-venue-feed", style="bold cyan"))
    parts.append(Text("  │  "))
    parts.append(Text(now_utc, style="white"))
    parts.append(Text("  │  "))

    if d:
        # backend bridge (q process) uptime, sourced from the bridge's own start time
        uptime = fmt_uptime(d.get("uptimeMs"))
        parts.append(Text(f"bridge up: {uptime}", style="white"))
        parts.append(Text("  │  "))

    parts.append(Text(f"refresh: {refresh_s}s", style="dim white"))
    parts.append(Text("  │  "))
    parts.append(traffic_light(status))
    parts.append(Text(f" {status_msg}", style=status))

    header_text = Text.assemble(*parts)
    return Panel(header_text, box=box.HEAVY, style="cyan")


def build_data_flow(d: Optional[Dict]) -> Panel:
    """Literal table -> row-count inventory for the crucial tables, by real q name,
    grouped by pipeline stage (ingest -> moves -> events -> scores -> system)."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Table Counts", border_style="red")

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
              padding=(0, 1), pad_edge=False, expand=True)
    t.add_column("Table", style="white", no_wrap=True)
    t.add_column("Rows", justify="right", style="green", no_wrap=True)

    # (table name, snap key). A None name is a dim section divider.
    rows = [
        ("quote_norm",                "quoteNorm"),
        (".feed.leg",                 "feedLegs"),
        (None, None),
        (".comove.hist",              "comoveHist"),
        (".tr.histFlow",              "trFlowBars"),
        (".tr.histPrice",             "trPriceMoves"),
        (None, None),
        ("leadlag_events",            "leadlagEvents"),
        ("leadership_pairs",          "leadershipPairs"),
        ("leadership_score",          "leadershipScore"),
        ("leadership_session",        "sessionQuote"),
        ("leadership_session_tflow",  "sessionTflow"),
        ("leadership_session_tprice", "sessionTprice"),
        (None, None),
        ("feed_health",               "feedHealthRows"),
        ("alerts",                    "alerts"),
    ]
    for name, key in rows:
        if name is None:
            t.add_row("", "")  # thin gap between stages
            continue
        cnt = d.get(key)
        cstyle = "yellow" if (key == "alerts" and (cnt or 0) > 0) else "green"
        t.add_row(name, Text(fmt_count_h(cnt), style=cstyle))

    return Panel(t, title="Table Counts", border_style="cyan")


def build_leg_freshness(d: Optional[Dict]) -> Panel:
    """Per-leg last tick age."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Leg Freshness", border_style="red")

    legs = d.get("legFresh")
    if legs is None or (hasattr(legs, '__len__') and len(legs) == 0):
        return Panel(Text("no legs active", style="dim"), title="Leg Freshness", border_style="yellow")

    # Sort stalest-first: q returns legs in insertion (per-venue) order, so when the panel
    # clips it would otherwise only ever show the first venue's legs. Age-desc keeps the
    # legs that actually need attention visible.
    def _leg_age(row):
        a = row.get("ageMs") if isinstance(row, dict) else (row[3] if isinstance(row, (list, tuple)) and len(row) > 3 else None)
        return a if _ok(a) else -1.0
    legs = sorted(legs, key=_leg_age, reverse=True)

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan")
    t.add_column("Venue", style="white")
    t.add_column("Inst", style="white")
    t.add_column("Sym", style="white")
    t.add_column("Age", justify="right")
    t.add_column("Mid", justify="right", style="dim white")

    # legs may be list-of-lists or list-of-dicts; normalize here
    for row in legs:
        if isinstance(row, (list, tuple)):
            sym, venue, inst, age, mid = str(row[0]), str(row[1]), str(row[2]), row[3], row[4]
        elif isinstance(row, dict):
            sym = str(row.get("sym", ""))
            venue = str(row.get("venue", ""))
            inst = str(row.get("inst", ""))
            age = row.get("ageMs")
            mid = row.get("mid")
        else:
            continue

        age_str = fmt_age_ms(age) if _ok(age) else "—"
        mid_str = f"{mid:.2f}" if _ok(mid) else "—"
        age_color = "green" if (_ok(age) and age < 5000) else ("yellow" if _ok(age) and age < 30000 else "red")
        t.add_row(venue, inst, sym, Text(age_str, style=age_color), mid_str)

    return Panel(t, title="Leg Freshness", border_style="cyan")


def build_score_table(d: Optional[Dict]) -> Panel:
    """Top leadership scores: prefer 30-min window, fall back to 1-min."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Leadership Score", border_style="red")

    # Prefer the 30-min recomputed score when the 1-min is empty
    score = d.get("score30") or d.get("score")
    win_label = "30-min" if d.get("score30") and len(d.get("score30") or []) > 0 else "1-min"

    if score is None or (hasattr(score, '__len__') and len(score) == 0):
        # Try session as last resort
        score = d.get("session")
        win_label = "session"
    if score is None or (hasattr(score, '__len__') and len(score) == 0):
        return Panel(Text("no scores yet", style="dim"), title="Leadership Score", border_style="yellow")

    # Compact columns so the Regime column always fits (short headers, no wrap, tight
    # padding; inst dropped — it's always SPOT here, shown as Venue only).
    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
              padding=(0, 1), pad_edge=False, expand=False)
    t.add_column("Sym", style="white", no_wrap=True)
    t.add_column("Venue", style="white", no_wrap=True)
    t.add_column("Score", justify="right", no_wrap=True)
    t.add_column("K", justify="right", style="dim white", no_wrap=True)
    t.add_column("n", justify="right", style="white", no_wrap=True)
    t.add_column("p", justify="right", style="dim white", no_wrap=True)
    t.add_column("Regime", style="dim white", no_wrap=True)

    # sort by abs(leadlagScore) desc, take top 10
    sorted_rows = []
    for row in score:
        if isinstance(row, (list, tuple)):
            sym, venue, inst = row[0], row[1], row[2]
            s, sk, n, pv = row[3], row[4], row[5], row[6]
            rb = row[7] if len(row) > 7 else ""
            sorted_rows.append((sym, venue, inst, s or 0.0, sk or 0.0, n or 0, pv or 1.0, rb or ""))
        elif isinstance(row, dict):
            s = row.get("leadlagScore") or 0.0
            sorted_rows.append((
                row.get("sym", ""), row.get("venue", ""), row.get("inst", ""),
                float(s), float(row.get("leadlagScoreK") or 0.0),
                int(row.get("nEvents") or 0), float(row.get("pValue") or 1.0),
                str(row.get("regimeBucket") or "")))
        else:
            continue

    sorted_rows.sort(key=lambda r: abs(r[3]) if (r[3] is not None and r[3] == r[3]) else 0.0, reverse=True)
    for sym, venue, inst, s, sk, n, pv, rb in sorted_rows[:10]:
        ok = lambda x: x is not None and x == x  # catches NaN (x != x)
        score_color = "green" if ok(s) and s > 0.1 else ("red" if ok(s) and s < -0.1 else "yellow")
        sig = "*" if ok(pv) and pv < 0.05 else ""
        # compact p: scientific for tiny p so it stays narrow (e.g. 2e-06 not 0.000)
        p_str = "—" if not ok(pv) else (f"{pv:.0e}" if pv < 0.001 else f"{pv:.3f}")
        t.add_row(
            str(sym), str(venue),
            Text(f"{s:+.2f}{sig}" if ok(s) else "—", style=score_color),
            f"{sk:+.2f}" if ok(sk) else "—",
            str(n) if n is not None else "—",
            p_str,
            str(rb) if rb else "—")

    return Panel(t, title=f"Leadership Score ({win_label})", border_style="cyan")


def build_feed_health(d: Optional[Dict]) -> Panel:
    """Per-VENUE feed health. Floor(ms) and fallback% are per-venue (one socket per
    venue, shared by all its symbols), so this rolls the per-leg rows up to one row per
    venue — always shows all venues (the per-leg view would be 15 rows and clip them)
    and drops the redundant repeated floor. Per-leg freshness is in the Leg Freshness
    panel. Tick metrics aggregate across the venue's symbols: T/min summed, OldAge =
    the stalest leg, Inv% = the worst leg."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Feed Health", border_style="red")

    fh = d.get("feedHealth")
    if fh is None or (hasattr(fh, '__len__') and len(fh) == 0):
        return Panel(Text("no health data yet", style="dim"), title="Feed Health", border_style="yellow")

    agg: Dict[str, Dict] = {}
    for row in fh:
        if isinstance(row, dict):
            venue = str(row.get("venue", ""))
            age, tpm, inv = row.get("lastTickAgeMs"), row.get("ticksPerMin"), row.get("invalidPct")
            floor_ms, fb = row.get("oneWayFloorMs"), row.get("fallbackPct")
        elif isinstance(row, (list, tuple)):
            venue = str(row[1]) if len(row) > 1 else ""
            age = row[3] if len(row) > 3 else None
            tpm = row[4] if len(row) > 4 else None
            inv = row[5] if len(row) > 5 else None
            floor_ms = row[6] if len(row) > 6 else None
            fb = row[7] if len(row) > 7 else None
        else:
            continue
        a = agg.setdefault(venue, {"tpm": 0.0, "age": 0.0, "inv": 0.0, "floor": None, "fb": None})
        if _ok(tpm): a["tpm"] += tpm
        if _ok(age): a["age"] = max(a["age"], age)
        if _ok(inv): a["inv"] = max(a["inv"], inv)
        if _ok(floor_ms): a["floor"] = floor_ms       # per-venue: same for every leg
        if _ok(fb): a["fb"] = fb

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
              padding=(0, 1), pad_edge=False, expand=False)
    t.add_column("Venue", style="white", no_wrap=True)
    t.add_column("T/min", justify="right", style="white", no_wrap=True)
    t.add_column("OldAge", justify="right", no_wrap=True)
    t.add_column("Inv%", justify="right", no_wrap=True)
    t.add_column("Floor", justify="right", style="dim white", no_wrap=True)
    t.add_column("Fallbk", justify="right", no_wrap=True)

    order = ["BINANCE", "COINBASE", "OKX"]
    for venue in sorted(agg, key=lambda v: (order.index(v) if v in order else 99, v)):
        a = agg[venue]
        age, inv, fb = a["age"], a["inv"], a["fb"]
        age_color = "green" if age < 5000 else ("yellow" if age < 30000 else "red")
        inv_color = "green" if inv < 0.01 else ("yellow" if inv < 0.05 else "red")
        fb_color = "green" if (not _ok(fb) or fb < 0.05) else ("yellow" if fb < 0.2 else "red")
        t.add_row(
            venue,
            f"{a['tpm']:.0f}",
            Text(fmt_age_ms(age), style=age_color),
            Text(f"{inv * 100:.1f}%", style=inv_color),
            f"{a['floor']:.0f}ms" if _ok(a["floor"]) else "—",
            Text(f"{fb * 100:.0f}%" if _ok(fb) else "—", style=fb_color))

    return Panel(t, title="Feed Health (by venue)", border_style="cyan")


def build_alerts(d: Optional[Dict]) -> Panel:
    """Recent alerts."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Recent Alerts", border_style="red")

    alerts = d.get("recentAlerts")
    if alerts is None or (hasattr(alerts, '__len__') and len(alerts) == 0):
        return Panel(Text("no alerts", style="green"), title="Recent Alerts", border_style="green")

    # compact: time as HH:MM:SS only (full ts wraps to 2 lines), no_wrap one-line rows,
    # fewer of them — this is a glance panel, full history is in the alerts table.
    # 3 compact columns (Time HH:MM:SS, Leg, Type) — fits the narrow left panel; the
    # verbose per-alert msg is dropped for the glance (full detail is in the alerts table).
    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
              padding=(0, 1), pad_edge=False, expand=True)
    t.add_column("Time", style="dim white", no_wrap=True)
    t.add_column("Leg", style="white", no_wrap=True, overflow="ellipsis")
    t.add_column("Type", style="red", no_wrap=True, ratio=1)

    def _hms(ts):
        if hasattr(ts, "strftime"):
            return ts.strftime("%H:%M:%S")
        s = str(ts)
        s = s.split(" ", 1)[1] if " " in s else s
        return s.split(".")[0][:8]

    for row in alerts[:6]:
        if isinstance(row, (list, tuple)):
            ts, sym, venue = row[0], row[1], row[2]
            atype = row[4] if len(row) > 4 else ""
            msg = row[5] if len(row) > 5 else ""
        elif isinstance(row, dict):
            ts = row.get("ts", "")
            sym = row.get("sym", "")
            venue = row.get("venue", "")
            atype = row.get("alertType", "")
            msg = row.get("msg", "")
        else:
            continue
        t.add_row(_hms(ts), f"{venue}/{sym}", str(atype))

    return Panel(t, title=f"Alerts ({len(alerts)})", border_style="red")


# Per-clock identity colors — the system's three parallel scorers. Reused so the
# pipeline matrix reads as "three things running side by side", the defining structure.
CLOCK_STYLE = {"Quote": "cyan", "Trade-flow": "magenta", "Trade-price": "green"}


def build_scorers(d: Optional[Dict]) -> Panel:
    """Three-scorer pipeline matrix: per clock, moves detected -> legs scored, plus a
    shared ingest/system footer. Encodes the system's defining structure (3 clocks)."""
    if not d:
        return Panel(Text("no data", style="dim"), title="Scorer Pipelines", border_style="red")

    # Left-packed (not expand) so the three clocks read as a tight block, each tagged
    # with its identity dot. Columns: detected moves/bars -> scored legs. (Shared ingest
    # counts — quotes/events/alerts — already live in the Data Flow panel; not repeated.)
    t = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan",
              padding=(0, 2), pad_edge=False, expand=False)
    t.add_column("Clock", no_wrap=True)
    t.add_column("Moves", justify="right", no_wrap=True)
    t.add_column("Legs", justify="right", style="dim white", no_wrap=True)

    for name, mk, lk in (("Quote", "comoveHist", "sessionQuote"),
                         ("Trade-flow", "trFlowBars", "sessionTflow"),
                         ("Trade-price", "trPriceMoves", "sessionTprice")):
        clock = Text.assemble(("● ", CLOCK_STYLE[name]), (name, "white"))
        t.add_row(clock, fmt_count_h(d.get(mk)), fmt_count_h(d.get(lk)))

    return Panel(t, title="Scorer Pipelines", border_style="cyan")


def build_memory(d: Optional[Dict]) -> Panel:
    """q workspace from .Q.w[]: used/heap/peak/limit (bytes) + interned-symbol count.
    used = live allocated, heap = mapped from OS, peak = max heap this session,
    limit = -w cap (0 ⇒ none). syms tracks symbol-interning growth (the free-edition
    leak risk — see CLAUDE.md §6)."""
    if not d or d.get("memory") is None:
        return Panel(Text("—", style="dim"), title="Mem", border_style="cyan")

    mem = d.get("memory")
    # .Q.w[] -> dict keyed used/heap/peak/wmax/mmap/mphy/syms/symw; fall back to
    # positional [used, heap, peak, wmax, ...] if it arrived as a bare vector.
    def g(key, idx):
        if isinstance(mem, dict):
            return mem.get(key)
        return mem[idx] if hasattr(mem, "__len__") and len(mem) > idx else None
    mb = lambda b: f"{b / (1024 * 1024):.0f} MB" if _ok(b) else "—"

    wmax = g("wmax", 3)
    limit_str = mb(wmax) if (_ok(wmax) and wmax > 0) else "none"

    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1), pad_edge=False)
    t.add_column("k", style="dim white", no_wrap=True)
    t.add_column("v", justify="right", style="white", no_wrap=True)
    t.add_row("used", mb(g("used", 0)))
    t.add_row("heap", mb(g("heap", 1)))
    t.add_row("peak", mb(g("peak", 2)))
    t.add_row("limit", limit_str)
    t.add_row("syms", fmt_count_h(g("syms", 6)))

    return Panel(t, title="Mem", border_style="cyan")


# --- status assessment -------------------------------------------------------

def assess_status(d: Optional[Dict], connected: bool) -> Tuple[str, str]:
    """Return (status_color, status_message) based on metrics."""
    if not connected or d is None:
        return ("red", "NO CONNECTION")

    # check for stale legs
    legs = d.get("legFresh")
    max_age = 0.0
    if legs is not None:
        for row in legs:
            if isinstance(row, (list, tuple)):
                age = row[3] if len(row) > 3 else None
            elif isinstance(row, dict):
                age = row.get("ageMs")
            else:
                continue
            if age is not None and age > max_age:
                max_age = float(age)

    # Count feed-health alerts only (exclude legitimate cross-venue stale-follower)
    recent = d.get("recentAlerts")
    alerts_health = 0
    if recent is not None:
        for row in recent:
            atype = None
            if isinstance(row, (list, tuple)):
                atype = row[4] if len(row) > 4 else None
            elif isinstance(row, dict):
                atype = row.get("alertType")
            if atype is not None and str(atype) != "stale":
                alerts_health += 1

    if max_age > 300_000 or alerts_health > 10:
        return ("yellow", f"DEGRADED (max tick age {fmt_age_ms(max_age)}, {alerts_health} feed alerts)")
    elif max_age > 30_000:
        return ("yellow", f"WARMING (max tick age {fmt_age_ms(max_age)})")
    else:
        return ("green", "HEALTHY")


# --- main layout + loop ------------------------------------------------------

def make_layout() -> Layout:
    """Create the root layout structure."""
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body"),
    )
    # right column (leadership score + feed health) is wider than the left so the
    # score table's Regime column and the feed-health rows are never squeezed.
    layout["body"].split_row(
        Layout(name="left", ratio=2),
        Layout(name="right", ratio=3),
    )
    layout["left"].split(
        Layout(name="data_flow", ratio=5),   # Table Counts — the full inventory
        Layout(name="legs", ratio=3),         # taller so >1 leg row renders
        Layout(name="alerts", ratio=2),       # compact, downsized
    )
    layout["right"].split(
        Layout(name="score", ratio=4),
        Layout(name="feed_health", ratio=3),
        Layout(name="rbottom", ratio=3),       # taller so all 3 scorer rows render
    )
    # right-bottom: the scorer-pipeline matrix (wide) beside a compact memory strip
    layout["rbottom"].split_row(
        Layout(name="scorers", ratio=3),
        Layout(name="memory", ratio=1),
    )
    return layout


def render_layout(layout: Layout, d: Optional[Dict], status: str, status_msg: str, refresh_s: int):
    """Fill the layout with panels based on current metrics."""
    layout["header"].update(build_header(d, status, status_msg, refresh_s))
    layout["data_flow"].update(build_data_flow(d))
    layout["legs"].update(build_leg_freshness(d))
    layout["alerts"].update(build_alerts(d))
    layout["score"].update(build_score_table(d))
    layout["feed_health"].update(build_feed_health(d))
    layout["scorers"].update(build_scorers(d))
    layout["memory"].update(build_memory(d))


def main():
    args = parse_args()
    console = Console()

    q = QConnection(args.host, args.port)
    layout = make_layout()

    d = None
    connected = False
    error_count = 0
    max_errors = 3

    def refresh_metrics():
        nonlocal d, connected, error_count
        try:
            d = fetch_metrics(q)
            if d is not None:
                connected = True
                error_count = 0
            else:
                error_count += 1
                if error_count >= max_errors:
                    connected = False
        except Exception:
            error_count += 1
            if error_count >= max_errors:
                connected = False

    # initial fetch
    refresh_metrics()

    # --once: print a single static snapshot and exit (renders in a pipe / non-TTY,
    # where the full-screen Live(screen=True) TUI would draw nothing). Panels are
    # printed top-to-bottom rather than the split layout so it reads in a scrollback.
    if args.once:
        status, status_msg = assess_status(d, connected)
        for panel in (build_header(d, status, status_msg, args.refresh),
                      build_data_flow(d), build_leg_freshness(d),
                      build_score_table(d), build_feed_health(d),
                      build_scorers(d), build_alerts(d), build_memory(d)):
            console.print(panel)
        q.close()
        return

    # Live TUI needs an interactive terminal; warn if stdout isn't a TTY (it would
    # render nothing — the usual "I can't see the dashboard" cause).
    if not sys.stdout.isatty():
        console.print("[yellow]stdout is not a terminal — the live TUI needs a real "
                      "terminal. Use --once for a one-shot snapshot, or run in a "
                      "normal terminal / tmux pane.[/yellow]")
        return

    with Live(layout, console=console, refresh_per_second=1, screen=True) as live:
        while True:
            status, status_msg = assess_status(d, connected)
            render_layout(layout, d, status, status_msg, args.refresh)

            try:
                time.sleep(args.refresh)
            except KeyboardInterrupt:
                break

            refresh_metrics()

    q.close()


if __name__ == "__main__":
    main()
