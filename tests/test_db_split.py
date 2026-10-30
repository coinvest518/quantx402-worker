import os, sys, types, unittest, importlib
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for m in ("torch", "kronos_paths", "model"):   # no model weights / torch needed for DB-routing tests
    st = types.ModuleType(m); sys.modules.setdefault(m, st)
sys.modules["torch"].set_num_threads = lambda n: None
sys.modules["kronos_paths"].predict_paths = None
for k in ("Kronos", "KronosTokenizer", "KronosPredictor"): setattr(sys.modules["model"], k, mock.MagicMock())

class Cur:
    def __init__(self, v): self.v = v
    def fetchone(self): return self.v[0] if self.v else None
    def fetchall(self): return self.v
class Conn:
    def __init__(self, name, log): self.name, self.log, self.closed = name, log, False
    def execute(self, q, p=None):
        self.log.append((self.name, " ".join(q.split())[:60]))
        if "returning id" in q: return Cur([(7,)])
        return Cur([])
    def close(self): self.closed = True

def load(env):
    with mock.patch.dict(os.environ, env, clear=False):
        import worker; return importlib.reload(worker)

ROW = {"as_of": "t", "factlist": {"direction": "UP"}, "prob_up": 0.6, "runtime_sec": 1}

class Split(unittest.TestCase):
    def run_main(self, env, hot_fails=False):
        W = load({"DATABASE_URL": "core", "SYMBOLS": "BTC-USD", "TIMEFRAMES": "15m", **env})
        log, opened = [], []
        def mk(name):
            def f(*a, **k): c = Conn(name, log); opened.append((name, len(log))); return c
            return f
        events = []
        with mock.patch.object(W, "db", side_effect=mk("core")), \
             mock.patch("psycopg.connect", side_effect=(OSError("down") if hot_fails else mk("hot"))), \
             mock.patch.object(W, "selected_timeframes", return_value=["15m"]), \
             mock.patch.object(W, "forecast", side_effect=lambda *a, **k: (events.append(("forecast", len(opened), k.get("bars_conn").name)), ROW)[1]), \
             mock.patch.object(W, "save", side_effect=lambda c, r: (events.append(("save", c.name)), 1)[1]), \
             mock.patch.object(W, "backfill", side_effect=lambda c, h: (events.append(("backfill", c.name, h.name)), 0)[1]):
            rc = W.main()
        return rc, log, opened, events

    def test_split_core_opened_only_after_inference(self):
        rc, log, opened, events = self.run_main({"SUPABASE_DATABASE_URL": "hot"})
        self.assertEqual(rc, 0)
        self.assertEqual(opened[0][0], "hot")
        f = [e for e in events if e[0] == "forecast"][0]
        self.assertEqual(f[1], 1, "only the always-on DB is open during inference")
        self.assertEqual(f[2], "hot")
        self.assertIn(("save", "core"), events)
        self.assertIn(("backfill", "core", "hot"), events)
        wr = [n for n, q in log if "worker_runs" in q]
        self.assertEqual(set(wr), {"hot"})
        self.assertFalse(any(n == "core" and "worker_runs" in q for n, q in log))

    def test_hot_outage_falls_back_to_core(self):
        rc, log, opened, events = self.run_main({"SUPABASE_DATABASE_URL": "hot"}, hot_fails=True)
        self.assertEqual(rc, 0)
        self.assertIn(("save", "core"), events)
        self.assertFalse(any("worker_runs" in q for n, q in log), "run log skipped when the always-on DB is down")

    def test_unset_falls_back_to_single_db(self):
        os.environ.pop("SUPABASE_DATABASE_URL", None)
        rc, log, opened, events = self.run_main({"SUPABASE_DATABASE_URL": ""})
        self.assertEqual(rc, 0)
        self.assertEqual([o[0] for o in opened], ["core"])
        self.assertEqual({n for n, q in log}, {"core"})

    def test_promote_is_two_queries(self):
        W = load({"DATABASE_URL": "core"})
        log = []; c, h = Conn("core", log), Conn("hot", log)
        h.execute = lambda q, p=None: (log.append(("hot", q)), Cur([("PEPE-USD", 6)]))[1]
        W.promote(c, h)
        self.assertTrue(any(n == "hot" and "symbol_requests" in q for n, q in log))
        self.assertTrue(any(n == "core" and "tracked_symbols" in q for n, q in log))
        self.assertFalse(any(n == "core" and "symbol_requests" in q for n, q in log))

class Retention(unittest.TestCase):
    def test_noop_when_unset(self):
        with mock.patch.dict(os.environ, {"SUPABASE_DATABASE_URL": ""}):
            import hot_retention as R; R = importlib.reload(R)
            with mock.patch("psycopg.connect") as pc:
                self.assertEqual(R.main(), 0); pc.assert_not_called()
    def test_refuses_core_host(self):
        with mock.patch.dict(os.environ, {"SUPABASE_DATABASE_URL": "postgresql://u:p@ep-x.us-east-2.aws.neon.tech/db"}):
            import hot_retention as R; R = importlib.reload(R)
            with mock.patch("psycopg.connect") as pc:
                self.assertEqual(R.main(), 2); pc.assert_not_called()
    def test_rules_keep_model_lookback(self):
        import hot_retention as R
        rules = dict((w.split(" and ")[0], w) for t, w in R.NORMAL if t == "stock_bars")
        self.assertIn("90 days", rules["timeframe = '15m'"]); self.assertIn("240 days", rules["timeframe = '1h'"])
        self.assertFalse(any("1d" in w for t, w in R.NORMAL + R.TIGHT))

if __name__ == "__main__":
    unittest.main()
