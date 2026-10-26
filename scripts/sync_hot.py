"""Idempotent catch-up copy of the always-on tables between the core DB and the always-on DB. NOT run automatically.

  forward  (core -> always-on): run at cutover, AFTER the freeze, to copy rows written since migrate_hot.sh ran.
  reverse  (always-on -> core): rollback only; copies rows written on the always-on DB after cutover back to core.

Per table: rows whose id (or timestamp column) is greater than the target's max are copied, inserted with
ON CONFLICT DO NOTHING (tables with a primary key also re-check a small overlap window, so it is safe to re-run).
Small tables with a primary key but no id/timestamp column are copied in full with ON CONFLICT DO NOTHING.
Tables with neither are skipped and reported. Never deletes or updates anything on either side.

Usage: SRC_URL=... DST_URL=... python3 scripts/sync_hot.py forward|reverse [--dry-run]
  forward: SRC_URL=core direct url, DST_URL=always-on session url.  reverse: the other way round.
Connection strings are read from env only and never printed.
"""
import os, sys
import psycopg
from psycopg import sql as S

TABLES = ["requests", "free_trials", "skill_leads", "link_clicks", "downloads", "prediction_markets", "pm_snapshots", "news_items",
          "worker_runs", "data_quality", "market_cache", "coingecko_usage", "chat_rate_limits", "a2a_rate_limits", "stream_leases",
          "symbol_requests", "stock_bars"]
TS_COLS = ["ts", "created_at", "started_at", "published_at", "fetched_at", "first_seen", "updated_at", "window_start"]
OVERLAP = "interval '10 minutes'"

def cols(c, t):
    return [r[0] for r in c.execute("select column_name from information_schema.columns where table_schema='public' and table_name=%s order by ordinal_position", (t,))]

def has_pk(c, t):
    return bool(c.execute("select 1 from pg_constraint where conrelid = to_regclass(%s) and contype in ('p','u')", (f"public.{t}",)).fetchone())

def key_col(cs):
    if "id" in cs: return "id"
    return next((k for k in TS_COLS if k in cs), None)

def sync(src, dst, t, dry):
    sc, dc = cols(src, t), cols(dst, t)
    if not sc or not dc: return f"{t}: skipped (missing on {'source' if not sc else 'target'})"
    common = [x for x in sc if x in dc]
    pk, k = has_pk(dst, t), key_col(common)
    if k is None and not pk: return f"{t}: skipped (no id/timestamp and no unique key)"
    where, args = S.SQL("true"), []
    if k is not None:
        mx = dst.execute(S.SQL("select max({}) from {}").format(S.Identifier(k), S.Identifier(t))).fetchone()[0]
        if mx is not None:
            if k == "id": where, args = S.SQL("{} > %s").format(S.Identifier(k)), [mx]
            elif pk: where, args = S.SQL("{} > %s::timestamptz - " + OVERLAP).format(S.Identifier(k)), [mx]
            else: where, args = S.SQL("{} > %s").format(S.Identifier(k)), [mx]
    collist = S.SQL(", ").join(map(S.Identifier, common))
    n = src.execute(S.SQL("select count(*) from {} where ").format(S.Identifier(t)) + where, args).fetchone()[0]
    if dry or n == 0: return f"{t}: {n} candidate rows{' (dry run)' if dry else ''}"
    with dst.transaction():
        dst.execute(S.SQL("create temp table _s (like {} including defaults) on commit drop").format(S.Identifier(t)))
        q = S.SQL("copy (select {} from {} where ").format(collist, S.Identifier(t)) + where + S.SQL(") to stdout")
        with src.cursor().copy(q, args) if args else src.cursor().copy(q) as out, \
             dst.cursor().copy(S.SQL("copy _s ({}) from stdin").format(collist)) as inp:
            for chunk in out: inp.write(chunk)
        ins = dst.execute(S.SQL("insert into {} ({}) select {} from _s on conflict do nothing").format(S.Identifier(t), collist, collist)).rowcount
        if k == "id":   # keep the target's id sequence ahead of copied ids
            dst.execute(S.SQL("select setval(pg_get_serial_sequence(%s, 'id'), greatest((select coalesce(max(id),1) from {}), 1)) where pg_get_serial_sequence(%s, 'id') is not null")
                        .format(S.Identifier(t)), (f"public.{t}", f"public.{t}"))
    return f"{t}: {n} candidate rows, {ins} inserted"

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("forward", "reverse"): print(__doc__); return 2
    dry = "--dry-run" in sys.argv
    src_url, dst_url = os.environ.get("SRC_URL"), os.environ.get("DST_URL")
    if not src_url or not dst_url: print("set SRC_URL and DST_URL"); return 2
    if src_url == dst_url: print("refusing: SRC_URL == DST_URL"); return 2
    # Sanity: forward writes into the always-on DB, reverse writes into the core DB.
    core_is_dst = sys.argv[1] == "reverse"
    if core_is_dst != ("neon.tech" in dst_url): print("refusing: direction does not match the target host"); return 2
    with psycopg.connect(src_url, autocommit=True, prepare_threshold=None, connect_timeout=15) as src, \
         psycopg.connect(dst_url, autocommit=True, prepare_threshold=None, connect_timeout=15) as dst:
        bad = 0
        for t in TABLES:
            try: print(sync(src, dst, t, dry))
            except Exception as e: bad += 1; print(f"{t}: FAILED ({type(e).__name__})")
    return 1 if bad else 0

if __name__ == "__main__":
    sys.exit(main())
