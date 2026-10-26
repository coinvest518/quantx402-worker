"""Retention for the always-on DB: keep it well under its 500 MB plan limit (target < 400 MB).
Runs only when SUPABASE_DATABASE_URL is set, so it never trims the core DB. Env: SUPABASE_DATABASE_URL, DRY_RUN=1, HOT_CAP_MB (default 400)."""
import os, sys
E = os.environ.get
URL, DRY, CAP = E("SUPABASE_DATABASE_URL", ""), E("DRY_RUN") == "1", int(E("HOT_CAP_MB", "400"))

# (table, where-clause) at normal size, then a tighter set if the DB is still over the cap.
NORMAL = [
    ("stock_bars", "timeframe = '15m' and ts < now() - interval '90 days'"),
    ("stock_bars", "timeframe = '1h' and ts < now() - interval '240 days'"),
    ("pm_snapshots", "ts < now() - interval '30 days'"),
    ("requests", "not paid and ts < now() - interval '180 days'"),
    ("news_items", "published_at < now() - interval '30 days'"),
    ("worker_runs", "started_at < now() - interval '30 days'"),
    ("data_quality", "ts < now() - interval '90 days'"),
    ("symbol_requests", "ts < now() - interval '60 days'"),
]
TIGHT = [
    ("stock_bars", "timeframe = '15m' and ts < now() - interval '45 days'"),
    ("stock_bars", "timeframe = '1h' and ts < now() - interval '180 days'"),
    ("pm_snapshots", "ts < now() - interval '14 days'"),
    ("requests", "not paid and ts < now() - interval '60 days'"),
    ("worker_runs", "started_at < now() - interval '7 days'"),
]

def size_mb(c): return c.execute("select pg_database_size(current_database())/1048576.0").fetchone()[0]

def run(c, rules):
    for t, w in rules:
        try:
            n = c.execute(f"select count(*) from {t} where {w}").fetchone()[0] if DRY else c.execute(f"delete from {t} where {w}").rowcount
            print(f"{t}: {'would delete' if DRY else 'deleted'} {n} ({w})")
        except Exception as e: print(f"{t}: skipped ({type(e).__name__})")

def main():
    if not URL: print("SUPABASE_DATABASE_URL unset: nothing to do"); return 0
    from urllib.parse import urlsplit
    host = (urlsplit(URL).hostname or "").lower()
    if "neon.tech" in host or URL == E("DATABASE_URL", ""):
        print("refusing: SUPABASE_DATABASE_URL points at the core DB; retention only runs on the always-on DB", file=sys.stderr); return 2
    import psycopg
    with psycopg.connect(URL, autocommit=True, prepare_threshold=None, connect_timeout=15) as c:
        before = size_mb(c); print(f"size before: {before:.0f} MB (cap {CAP})")
        run(c, NORMAL)
        if not DRY and size_mb(c) > CAP:
            print("over cap after normal retention: tightening"); run(c, TIGHT)
            for t in ("stock_bars", "pm_snapshots", "requests"): c.execute(f"vacuum (analyze) {t}")
        after = size_mb(c); print(f"size after: {after:.0f} MB")
        if after > CAP: print(f"WARNING: always-on DB still {after:.0f} MB > {CAP} MB", file=sys.stderr); return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
