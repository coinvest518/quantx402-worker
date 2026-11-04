"""Simulated outage replay against two real local DBs (OUTBOX_CORE_URL / OUTBOX_HOT_URL); skipped when unset."""
import os, json, unittest, pathlib
import outbox
C, H = os.environ.get("OUTBOX_CORE_URL"), os.environ.get("OUTBOX_HOT_URL")
DDL = os.environ.get("OUTBOX_DDL")

@unittest.skipUnless(C and H and DDL, "local test DBs not set")
class T(unittest.TestCase):
    def test_both_directions_no_dups(self):
        import psycopg
        c, h = psycopg.connect(C, autocommit=True), psycopg.connect(H, autocommit=True)
        ddl = pathlib.Path(DDL).read_text()
        for x in (c, h): x.execute("drop table if exists trades, replay_outbox"); x.execute(ddl)
        q = "insert into replay_outbox (target_db, tbl, op, conflict, row) values (%s,'trades','upsert','{id}',%s::jsonb)"
        c.execute(q, ("hot", json.dumps({"id": 1, "symbol": "COST", "status": "open"})))       # hot was down
        c.execute(q, ("hot", json.dumps({"id": 1, "symbol": "COST", "status": "closed", "pnl": 3})))
        h.execute(q, ("core", json.dumps({"id": 2, "symbol": "WMT", "status": "open", "signal_detail": {"stop_order_id": "x"}})))  # core was down
        r1 = outbox.catch_up(h, c); r2 = outbox.catch_up(h, c)
        print("catch_up 1:", r1, "catch_up 2:", r2)
        self.assertEqual(r1, {"to_core": (1, 1, 0), "to_hot": (2, 2, 0)}); self.assertEqual(r2, {"to_core": (0, 0, 0), "to_hot": (0, 0, 0)})
        self.assertEqual(h.execute("select count(*), max(status) from trades").fetchone(), (1, "closed"))
        self.assertEqual(c.execute("select count(*), max(stop_order_id) from trades").fetchone(), (1, "x"))

    def test_missing_table_noop(self):
        import psycopg
        c = psycopg.connect(C, autocommit=True); c.execute("drop table if exists replay_outbox")
        self.assertEqual(outbox.replay(c, c, "hot"), (0, 0, 0))

if __name__ == "__main__": unittest.main()
