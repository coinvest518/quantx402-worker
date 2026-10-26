import os, sys, time, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.pop("DATABASE_URL", None)
import loop5m as L
from datetime import datetime, timezone

def iso(t): return datetime.fromtimestamp(t, timezone.utc).isoformat().replace("+00:00", "Z")

class TieBreak(unittest.TestCase):
    def test_simultaneous_start_younger_yields(self):
        now = time.time()
        runs = [{"id": 100, "run_started_at": iso(now - 30)}]
        self.assertTrue(L.should_yield(runs, "101", now))     # 101 is younger -> exits
        self.assertFalse(L.should_yield([{"id": 101, "run_started_at": iso(now - 30)}], "100", now))  # 100 stays
    def test_handover_new_run_stays(self):
        now = time.time()
        pred = [{"id": 100, "run_started_at": iso(now - 18400)}]   # predecessor in handover window
        self.assertFalse(L.should_yield(pred, "101", now))
    def test_no_runs_or_api_down(self):
        self.assertFalse(L.should_yield(None, "5"))
        self.assertFalse(L.should_yield([], "5"))
        self.assertFalse(L.should_yield([{"id": 1, "run_started_at": iso(time.time())}], "local-1"))

class Dispatch(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {"GH_TOKEN": "x", "GH_REPO": "o/r"}); self.env.start()
        mock.patch.object(L, "run5m_enabled", return_value=True).start()
        mock.patch.object(L, "other_active_runs", return_value=0).start()
        self.sleeps = []; mock.patch.object(L, "_sleep", self.sleeps.append).start()
    def tearDown(self): mock.patch.stopall(); self.env.stop()
    def test_retries_then_fails(self):
        with mock.patch.object(L, "_post_dispatch", return_value=False) as p:
            self.assertEqual(L.dispatch_successor(), "failed")
        self.assertEqual(p.call_count, 1 + len(L.BACKOFF)); self.assertEqual(self.sleeps, L.BACKOFF)
    def test_recovers_on_retry(self):
        with mock.patch.object(L, "_post_dispatch", side_effect=[False, True]):
            self.assertEqual(L.dispatch_successor(), "ok")
    def test_switch_off_is_skip(self):
        with mock.patch.object(L, "run5m_enabled", return_value=False): self.assertEqual(L.dispatch_successor(), "skip")
    def test_relaunch_failed_row(self):
        c = mock.MagicMock(); L.log_relaunch_failed(c, 2)
        sql, args = c.execute.call_args[0]
        self.assertIn("relaunch_failed", sql); self.assertIn("round 2", args[0])

class MainLoop(unittest.TestCase):
    """Failed dispatch at HANDOFF_AT: loop keeps cycling, logs relaunch_failed, retries periodically."""
    def test_keeps_running_and_retries(self):
        clock = [0.0]
        def fake_time(): return clock[0]
        def fake_sleep(s): clock[0] += max(s, 0)
        c = mock.MagicMock()
        cycles = []
        def cyc(pred, conn, it): cycles.append(clock[0]); clock[0] += 10
        with mock.patch.object(L, "HANDOFF_AT", 600), mock.patch.object(L, "LOOP", 3000), mock.patch.object(L, "RETRY_EVERY", 900), \
             mock.patch.object(L, "MAX_ITER", 0), mock.patch.object(L.time, "time", fake_time), mock.patch.object(L.time, "sleep", fake_sleep), \
             mock.patch.object(L, "one_cycle", cyc), mock.patch.object(L, "dispatch_successor", return_value="failed") as d, \
             mock.patch.object(L, "log_relaunch_failed") as lf, mock.patch.object(L, "ensure_conn", side_effect=lambda x: x), \
             mock.patch.object(L, "other_runs", return_value=[]), mock.patch.object(L.w, "DRY", False), mock.patch.object(L.w, "db", return_value=c), \
             mock.patch.dict(sys.modules, {"model": mock.MagicMock()}), mock.patch.dict(os.environ, {"GH_RUN_ID": ""}):
            L.main()
        self.assertGreaterEqual(d.call_count, 3); self.assertEqual(lf.call_count, d.call_count)
        self.assertTrue(any(t > 2500 for t in cycles))   # still cycling near the hard stop

class Reconnect(unittest.TestCase):
    def test_ensure_conn_reopens(self):
        import psycopg
        bad = mock.MagicMock(); bad.execute.side_effect = psycopg.OperationalError("closed")
        new = object()
        with mock.patch.object(L.w, "db", return_value=new): self.assertIs(L.ensure_conn(bad), new)
        good = mock.MagicMock(); self.assertIs(L.ensure_conn(good), good)

class CycleFailures(unittest.TestCase):
    def setUp(self):
        import psycopg; self.err = psycopg.OperationalError
        L._pending_runs.clear()
        mock.patch.object(L.w, "DRY", False).start()
    def tearDown(self): mock.patch.stopall(); L._pending_runs.clear()
    def _conn(self, run_id=7):
        c = mock.MagicMock(); c.execute.return_value.fetchone.return_value = [run_id]; return c
    def test_double_failure_never_raises(self):
        c1, c2 = self._conn(), self._conn()
        mock.patch.object(L.w, "db", return_value=c2).start()
        mock.patch.object(L, "one_cycle", side_effect=self.err("boom")).start()
        out = L.safe_cycle(None, c1, 3)
        self.assertIsNone(out)                         # reset -> reconnect next slot
        c1.close.assert_called(); c2.close.assert_called()
        sqls = [a[0][0] for a in c2.execute.call_args_list]
        self.assertTrue(any("cycle_failed" in s for s in sqls))
    def test_reconnect_failure_never_raises(self):
        c1 = self._conn(); c1.execute.side_effect = self.err("dropped")
        mock.patch.object(L.w, "db", side_effect=self.err("neon down")).start()
        self.assertIsNone(L.safe_cycle(None, c1, 0))
        # next slot with DB still down also survives
        self.assertIsNone(L.safe_cycle(None, None, 1))
    def test_worker_runs_finalized_on_failure(self):
        import worker as W
        c = self._conn(42)
        mock.patch.object(L, "SYMS", ["BTC-USD"]).start()
        mock.patch.object(L, "record_gaps", return_value=0).start()
        mock.patch.object(W, "forecast", side_effect=self.err("mid-cycle")).start()
        # forecast errors are per-symbol; make save path fail fatally via backfill+final update
        def ex(sql, *a):
            if sql.startswith("update worker_runs set finished_at = now(), ok"): raise self.err("lost")
            return mock.MagicMock(fetchone=mock.MagicMock(return_value=[42]))
        c.execute.side_effect = ex
        with self.assertRaises(self.err): L.one_cycle(None, c, 1)
        self.assertEqual(L._pending_runs, [42])
        c2 = mock.MagicMock(); self.assertEqual(L.finalize_pending(c2), 1)
        sql, args = c2.execute.call_args[0]
        self.assertIn("finished_at = coalesce", sql); self.assertEqual(args, ("cycle_failed", 42))
        self.assertEqual(L._pending_runs, [])
    def test_finalize_kept_when_db_down(self):
        L._pending_runs.append(9)
        bad = mock.MagicMock(); bad.execute.side_effect = self.err("down")
        self.assertEqual(L.finalize_pending(bad), 0); self.assertEqual(L._pending_runs, [9])
    def test_main_survives_db_outage_and_still_dispatches(self):
        clock = [0.0]
        def fake_sleep(s): clock[0] += max(s, 0)
        with mock.patch.object(L, "HANDOFF_AT", 600), mock.patch.object(L, "LOOP", 1500), mock.patch.object(L, "MAX_ITER", 0), \
             mock.patch.object(L.time, "time", lambda: clock[0]), mock.patch.object(L.time, "sleep", fake_sleep), \
             mock.patch.object(L.w, "db", side_effect=self.err("neon down")), \
             mock.patch.object(L, "dispatch_successor", return_value="ok") as d, \
             mock.patch.object(L, "other_runs", return_value=[]), \
             mock.patch.dict(sys.modules, {"model": mock.MagicMock()}), mock.patch.dict(os.environ, {"GH_RUN_ID": ""}):
            L.main()
        self.assertEqual(d.call_count, 1)

if __name__ == "__main__": unittest.main()
