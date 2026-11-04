"""Stock forecast run: CORE_SYNC=defer never opens the core DB; stock rows are synced/scored by the hourly run."""
import os, sys, json, tempfile, unittest, importlib
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_db_split as T  # stubs torch/model

LIVE = {"DATABASE_URL": "core", "SUPABASE_DATABASE_URL": "hot", "FORECASTS_LIVE": "1"}
ROW = {"symbol": "SPY", "venue": "stock", "timeframe": "1h", "model": "Kronos-small", "as_of": "2026-11-04T19:30:00Z", "factlist": {}}

class Defer(unittest.TestCase):
    def test_core_sync_never_due_in_defer(self):
        W = T.load({**LIVE, "CORE_SYNC": "defer"})
        self.assertFalse(W.core_sync_due(None, ["1h", "4h", "1d"], force=True))
        W2 = T.load({**LIVE, "CORE_SYNC": ""})
        self.assertTrue(W2.core_sync_due(None, ["1h"]))

    def test_flush_defer_writes_hot_only(self):
        W = T.load({**LIVE, "CORE_SYNC": "defer"})
        log = []
        d = tempfile.mkdtemp(); p = os.path.join(d, "shard-0.jsonl"); open(p, "w").write(json.dumps(ROW) + "\n")
        with mock.patch.object(W, "db", side_effect=AssertionError("core DB opened")), \
             mock.patch.object(W, "hot_db", side_effect=lambda *a: T.Conn("hot", log)), \
             mock.patch.object(W, "live_ready", return_value=True), \
             mock.patch.object(W, "save_live", return_value=99):
            self.assertEqual(W.flush([p]), 0)
        self.assertTrue(all(n == "hot" for n, _ in log))

    def test_flush_defer_not_live_ready_refuses_core(self):
        W = T.load({**LIVE, "CORE_SYNC": "defer"})
        d = tempfile.mkdtemp(); p = os.path.join(d, "s.jsonl"); open(p, "w").write(json.dumps(ROW) + "\n")
        with mock.patch.object(W, "db", side_effect=AssertionError("core DB opened")), \
             mock.patch.object(W, "hot_db", side_effect=lambda *a: T.Conn("hot", [])), \
             mock.patch.object(W, "live_ready", return_value=False):
            self.assertEqual(W.flush([p]), 1)

    def test_main_defer_requires_live(self):
        W = T.load({"DATABASE_URL": "core", "SUPABASE_DATABASE_URL": "hot", "FORECASTS_LIVE": "0", "CORE_SYNC": "defer"})
        with mock.patch.object(W, "db", side_effect=AssertionError("core DB opened")):
            with self.assertRaises(SystemExit): W.main()

class StockScoring(unittest.TestCase):
    def test_scheduled_symbols_accept_stocks(self):
        W = T.load({**LIVE, "SYMBOLS": "SPY,QQQ"})
        h = mock.MagicMock(); h.execute.return_value.fetchall.return_value = [("AAPL",), ("BTC-USD",), ("bad sym",)]
        self.assertEqual(W.scheduled_symbols_hot(h), ["SPY", "QQQ", "AAPL", "BTC-USD"])

    def test_realized_close_stock_reads_hot_bars(self):
        W = T.load(LIVE)
        h = mock.MagicMock(); h.execute.return_value.fetchone.return_value = (512.5,)
        self.assertEqual(W.realized_close("SPY", "stock", "2026-11-04T19:30:00Z", "1h", h), 512.5)
        q, params = h.execute.call_args[0]
        self.assertIn("from stock_bars", q); self.assertEqual(params[:2], ("SPY", "1h"))
        W.realized_close("SPY", "stock", "2026-11-04T14:30:00Z", "4h", h)
        self.assertEqual(h.execute.call_args[0][1][1], "1h"); self.assertEqual(h.execute.call_args[0][1][3], 3)  # 4th 1h bar

    def test_backfill_scores_stock_row(self):
        W = T.load(LIVE)
        c = mock.MagicMock(); h = mock.MagicMock()
        c.execute.return_value.fetchall.return_value = [(1, "SPY", "stock", "2026-11-04T19:30:00Z", 500.0, "UP", [490, 510], "1h")]
        h.execute.return_value.fetchone.return_value = (505.0,)
        with mock.patch.object(W.time, "sleep"):
            self.assertEqual(W.backfill(c, h), 1)
        ins = [a for a in c.execute.call_args_list if "insert into outcomes" in a[0][0]][0][0][1]
        self.assertEqual(ins[0], 1); self.assertTrue(ins[3]); self.assertTrue(ins[4])

    def test_sync_core_copies_all_unsynced_rows(self):
        W = T.load(LIVE)
        h = mock.MagicMock(); c = mock.MagicMock()
        cols = ["id"] + W.FCOLS + ["created_at"]
        mk = lambda i, sym, venue: [i if k == "id" else sym if k == "symbol" else venue if k == "venue" else None for k in cols]
        h.execute.return_value.fetchall.return_value = [mk(1, "BTC-USD", "primary"), mk(2, "SPY", "stock")]
        self.assertEqual(W.sync_core(h, c), 2)

if __name__ == "__main__": unittest.main()
