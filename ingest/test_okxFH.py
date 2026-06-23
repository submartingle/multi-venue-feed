"""
Offline unit tests for okxFH.py BBO emission + trade mapping.

No network, no tickerplant (enable_tp=False). Run directly:
    python3 ingest/test_okxFH.py
Exits 1 on any failure (self-asserting, same convention as the sim/ q suite).
"""

from okxFH import OkxSpotFeedHandler


def mk_fh(channel="bbo-tbt", trades=True) -> OkxSpotFeedHandler:
    return OkxSpotFeedHandler(
        symbols=["BTC-USDT"], tp_host="127.0.0.1", tp_port=0,
        channel=channel, enable_tp=False, trades=trades,
    )


# OKX bbo-tbt / books5 data entry: bids/asks = [[px, sz, deprecated, #orders], ...], ts ms.
def book(bid="100.0", bidsz="1.5", ask="100.5", asksz="1.0", ts="1781092800123"):
    return {"asks": [[ask, asksz, "0", "2"]], "bids": [[bid, bidsz, "0", "3"]], "ts": ts}


def bbo_msg(instId="BTC-USDT", channel="bbo-tbt", **kw):
    return {"arg": {"channel": channel, "instId": instId}, "data": [book(**kw)]}


def trade_msg(instId="BTC-USDT", tradeId="555", px="100.10", sz="0.25",
              side="buy", ts="1781092800123"):
    return {"arg": {"channel": "trades", "instId": instId},
            "data": [{"instId": instId, "tradeId": tradeId, "px": px, "sz": sz,
                      "side": side, "ts": ts}]}


def last_bt(fh):
    return fh._bt_batch[-1]


# ----- BBO -----

def test_bbo_emit_with_native_ms_clock():
    fh = mk_fh()
    fh._dispatch(bbo_msg(), recv_us=999)
    assert len(fh._bt_batch) == 1
    exch_us, recv_us, sym, venue, inst, bid, bidsz, ask, asksz = last_bt(fh)
    assert (sym, venue, inst) == ("BTC-USDT", "OKX", "SPOT")
    assert (bid, bidsz, ask, asksz) == (100.0, 1.5, 100.5, 1.0)
    assert recv_us == 999
    assert exch_us == 1781092800123 * 1000  # ms -> us


def test_bbo_missing_ts_falls_back_to_recv():
    fh = mk_fh()
    fh._dispatch(bbo_msg(ts=None), recv_us=777)
    exch_us, *_ = last_bt(fh)
    assert exch_us == 777


def test_bbo_one_sided_skipped():
    fh = mk_fh()
    msg = {"arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
           "data": [{"asks": [], "bids": [["100.0", "1.0", "0", "1"]], "ts": "1781092800123"}]}
    fh._dispatch(msg, recv_us=1)
    assert fh._bt_batch == []


def test_bbo_dedup_identical_push():
    fh = mk_fh()
    fh._dispatch(bbo_msg(), recv_us=1)
    fh._dispatch(bbo_msg(), recv_us=2)  # identical top-of-book
    assert len(fh._bt_batch) == 1


def test_bbo_size_only_change_emits():
    fh = mk_fh()
    fh._dispatch(bbo_msg(), recv_us=1)
    fh._dispatch(bbo_msg(bidsz="9.9"), recv_us=2)  # same prices, size moved
    assert len(fh._bt_batch) == 2
    *_, bid, bidsz, ask, asksz = last_bt(fh)
    assert (bid, bidsz) == (100.0, 9.9)


def test_bbo_price_change_emits():
    fh = mk_fh()
    fh._dispatch(bbo_msg(), recv_us=1)
    fh._dispatch(bbo_msg(bid="100.2"), recv_us=2)
    assert len(fh._bt_batch) == 2
    *_, bid, bidsz, ask, asksz = last_bt(fh)
    assert bid == 100.2


def test_books5_top_level_only():
    # books5 carries depth-5; we read only the top level. Build a 2-level book.
    fh = mk_fh(channel="books5")
    msg = {"arg": {"channel": "books5", "instId": "BTC-USDT"},
           "data": [{"bids": [["100.0", "1.5", "0", "3"], ["99.5", "2.0", "0", "1"]],
                     "asks": [["100.5", "1.0", "0", "2"], ["101.0", "4.0", "0", "1"]],
                     "ts": "1781092800123"}]}
    fh._dispatch(msg, recv_us=1)
    *_, bid, bidsz, ask, asksz = last_bt(fh)
    assert (bid, bidsz, ask, asksz) == (100.0, 1.5, 100.5, 1.0)


def test_reconnect_clears_dedup():
    fh = mk_fh()
    fh._dispatch(bbo_msg(), recv_us=1)
    fh._last.clear()                         # what _run_once does on (re)connect
    fh._dispatch(bbo_msg(), recv_us=2)       # same BBO must re-emit on a fresh socket
    assert len(fh._bt_batch) == 2


# ----- trades -----

def test_trade_emit_taker_buy_is_B():
    fh = mk_fh()
    fh._dispatch(trade_msg(side="buy"), recv_us=42)
    exch_us, trade_us, recv_us, sym, px, qty, side, tid = fh._tr_batch[-1]
    assert exch_us == trade_us == 1781092800123 * 1000  # single OKX trade time
    assert (sym, px, qty, tid, recv_us) == ("BTC-USDT", 100.10, 0.25, 555, 42)
    assert side == "B"  # OKX side is the TAKER (aggressor) — no flip


def test_trade_taker_sell_is_S():
    fh = mk_fh()
    fh._dispatch(trade_msg(side="sell"), recv_us=1)
    *_, side, _ = fh._tr_batch[-1]
    assert side == "S"


def test_trade_incomplete_skipped():
    fh = mk_fh()
    fh._dispatch({"arg": {"channel": "trades", "instId": "BTC-USDT"},
                  "data": [{"instId": "BTC-USDT"}]}, recv_us=1)
    assert fh._tr_batch == []


def test_trades_disabled():
    fh = mk_fh(trades=False)
    fh._dispatch(trade_msg(), recv_us=1)
    assert fh._tr_batch == []


def test_multiple_data_entries_in_one_msg():
    fh = mk_fh()
    msg = {"arg": {"channel": "trades", "instId": "BTC-USDT"},
           "data": [{"instId": "BTC-USDT", "tradeId": "1", "px": "100.0", "sz": "0.1",
                     "side": "buy", "ts": "1781092800123"},
                    {"instId": "BTC-USDT", "tradeId": "2", "px": "100.1", "sz": "0.2",
                     "side": "sell", "ts": "1781092800124"}]}
    fh._dispatch(msg, recv_us=1)
    assert len(fh._tr_batch) == 2
    assert [r[6] for r in fh._tr_batch] == ["B", "S"]


# ----- plumbing -----

def test_ms_to_us_fallback():
    assert OkxSpotFeedHandler._ms_to_us(None) is None
    assert OkxSpotFeedHandler._ms_to_us("") is None
    assert OkxSpotFeedHandler._ms_to_us("not-a-number") is None
    assert OkxSpotFeedHandler._ms_to_us("1781092800123") == 1781092800123000


def test_subscribe_msg_includes_book_and_trades():
    import orjson
    fh = mk_fh()
    args = orjson.loads(fh._subscribe_msg())["args"]
    chans = {(a["channel"], a["instId"]) for a in args}
    assert ("bbo-tbt", "BTC-USDT") in chans
    assert ("trades", "BTC-USDT") in chans
    assert len(args) == 2  # one symbol x (book + trades)


def test_subscribe_msg_no_trades():
    import orjson
    fh = mk_fh(trades=False)
    args = orjson.loads(fh._subscribe_msg())["args"]
    assert all(a["channel"] != "trades" for a in args)


def test_event_messages_ignored():
    fh = mk_fh()
    fh._dispatch({"event": "subscribe", "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"}}, recv_us=1)
    fh._dispatch({"event": "error", "code": "60012", "msg": "bad request"}, recv_us=1)
    assert fh._bt_batch == [] and fh._tr_batch == []


if __name__ == "__main__":
    import sys
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError:
            failed += 1
            import traceback
            traceback.print_exc()
            print(f"FAIL {t.__name__}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
