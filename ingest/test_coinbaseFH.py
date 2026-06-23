"""
Offline unit tests for coinbaseFH.py level2_batch book maintenance + BBO emission.

No network, no tickerplant (enable_tp=False). Run directly:
    python3 ingest/test_coinbaseFH.py
Exits 1 on any failure (self-asserting, same convention as the sim/ q suite).
"""

from coinbaseFH import CoinbaseSpotFeedHandler, L2Book


def mk_fh() -> CoinbaseSpotFeedHandler:
    return CoinbaseSpotFeedHandler(
        symbols=["BTC-USD"], tp_host="127.0.0.1", tp_port=0,
        channel="level2_batch", enable_tp=False,
    )


SNAP = {
    "type": "snapshot", "product_id": "BTC-USD",
    "bids": [["100.00", "1.5"], ["99.50", "2.0"], ["99.00", "3.0"]],
    "asks": [["100.50", "1.0"], ["101.00", "4.0"]],
}


def upd(changes, time="2026-06-10T12:00:00.123456Z"):
    return {"type": "l2update", "product_id": "BTC-USD", "time": time, "changes": changes}


def last_row(fh):
    return fh._bt_batch[-1]


def test_match_emits_trade_row():
    fh = mk_fh()
    fh._handle_match({"type": "match", "product_id": "BTC-USD", "trade_id": 7714,
                      "side": "buy", "size": "0.25", "price": "100.10",
                      "time": "2026-06-10T12:00:00.123456Z"}, recv_us=999)
    exch_us, trade_us, recv_us, sym, px, qty, side, tid = fh._tr_batch[-1]
    assert exch_us == trade_us  # Coinbase has a single match time
    assert exch_us == 1781092800123456
    assert (sym, px, qty, tid, recv_us) == ("BTC-USD", 100.10, 0.25, 7714, 999)
    assert side == "S"  # Coinbase side is the MAKER side; maker buy = taker sold


def test_match_missing_time_falls_back_to_recv():
    fh = mk_fh()
    fh._handle_match({"type": "match", "product_id": "BTC-USD", "trade_id": 1,
                      "side": "sell", "size": "1", "price": "99.0"}, recv_us=555)
    exch_us, trade_us, recv_us, _, _, _, side, _ = fh._tr_batch[-1]
    assert exch_us == trade_us == recv_us == 555
    assert side == "B"


def test_match_incomplete_skipped():
    fh = mk_fh()
    fh._handle_match({"type": "match", "product_id": "BTC-USD"}, recv_us=1)
    assert fh._tr_batch == []


def test_snapshot_initial_bbo():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    assert len(fh._bt_batch) == 1
    exch_us, recv_us, sym, venue, inst, bid, bidsz, ask, asksz = last_row(fh)
    assert (sym, venue, inst) == ("BTC-USD", "COINBASE", "SPOT")
    assert (bid, bidsz, ask, asksz) == (100.00, 1.5, 100.50, 1.0)
    assert exch_us == 1_000  # snapshot carries no time -> recv_us fallback


def test_inside_book_change_no_emit():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    fh._handle_l2update(upd([["buy", "99.50", "5.0"], ["sell", "101.00", "0"]]), recv_us=2_000)
    assert len(fh._bt_batch) == 1  # top-of-book untouched


def test_best_bid_improvement_emits_with_native_time():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    fh._handle_l2update(upd([["buy", "100.10", "0.7"]]), recv_us=2_000)
    exch_us, recv_us, _, _, _, bid, bidsz, ask, asksz = last_row(fh)
    assert (bid, bidsz) == (100.10, 0.7)
    assert (ask, asksz) == (100.50, 1.0)
    assert recv_us == 2_000
    # native microsecond exchange clock parsed from the ISO `time`
    assert exch_us == 1_781_092_800_123_456


def test_top_removal_exposes_next_level():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    fh._handle_l2update(upd([["sell", "100.50", "0"]]), recv_us=2_000)
    *_, bid, bidsz, ask, asksz = last_row(fh)
    assert (ask, asksz) == (101.00, 4.0)
    assert (bid, bidsz) == (100.00, 1.5)


def test_size_only_change_at_top_emits():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    fh._handle_l2update(upd([["buy", "100.00", "9.9"]]), recv_us=2_000)
    assert len(fh._bt_batch) == 2  # @bookTicker semantics: size change is a tick
    *_, bid, bidsz, ask, asksz = last_row(fh)
    assert (bid, bidsz) == (100.00, 9.9)


def test_diff_before_snapshot_ignored():
    fh = mk_fh()
    fh._handle_l2update(upd([["buy", "100.00", "1.0"]]), recv_us=1_000)
    assert fh._bt_batch == []


def test_resync_snapshot_replaces_book():
    fh = mk_fh()
    fh._handle_snapshot(SNAP, recv_us=1_000)
    snap2 = {
        "type": "snapshot", "product_id": "BTC-USD",
        "bids": [["98.00", "1.0"]], "asks": [["98.50", "2.0"]],
    }
    fh._handle_snapshot(snap2, recv_us=2_000)
    *_, bid, bidsz, ask, asksz = last_row(fh)
    assert (bid, ask) == (98.00, 98.50)  # old 100.00 bid is gone, not merged
    book = fh._books["BTC-USD"]
    assert len(book.bids) == 1 and len(book.asks) == 1


def test_one_sided_book_no_emit():
    book = L2Book()
    book.load_snapshot([["100.00", "1.0"]], [])
    assert book.bbo() is None


def test_iso_time_fallback():
    fh = mk_fh()
    assert fh._iso_to_us("") is None
    assert fh._iso_to_us("not-a-time") is None
    fh._handle_snapshot(SNAP, recv_us=1_000)
    fh._handle_l2update(upd([["buy", "100.20", "1.0"]], time=""), recv_us=2_000)
    exch_us, *_ = last_row(fh)
    assert exch_us == 2_000  # unparseable time -> recv_us fallback


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
