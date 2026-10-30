"""Catch-up copy of the always-on tables between the core DB and the always-on DB, plus a sequence bump. NOT run automatically.

  forward  (core -> always-on): copies rows written on core since the previous copy. Run before AND right after the site deploy.
  reverse  (always-on -> core): rollback only. Copies rows written on the always-on DB after cutover (ids >= BUMP) back to core.
  bump     (target only):       moves the identity/serial sequences of the TABLES below (never forecasts/outcomes) to at least BUMP (default 1e9) and past max(id), so ids
                                generated on one side never collide with ids copied from the other.

How rows are copied:
- Identity tables (detected through pg_attribute.attidentity): INSERT ... OVERRIDING SYSTEM VALUE, so source ids are kept.
- Id cursor. Forward looks only at ids below BUMP on the target, so ids that the always-on DB generates after the bump don't
  hide newer core rows. Reverse copies only ids at or above BUMP.
- Tables with a PK and a timestamp: rows with ts > target max minus 10 min, then ON CONFLICT DO NOTHING.
- Tables without a unique key: rows with ts >= target max, minus any rows already on the target (EXCEPT over the full row).
  Skipped and reported when that isn't possible.
- --upsert (use on the pre-deploy forward run and on reverse): mutable tables (MUTABLE) are copied in full with ON CONFLICT (pk) DO UPDATE, so
  updates (prices, counters, leases) carry over. Where the table has updated_at, only a newer row overwrites.
Never deletes anything. Only --upsert updates existing rows, and only in the MUTABLE tables.

Usage: SRC_URL=... DST_URL=... python3 scripts/sync_hot.py forward|reverse [--upsert] [--dry-run]
       DST_URL=... python3 scripts/sync_hot.py bump [--dry-run]
Optional: SRC_SCHEMA / DST_SCHEMA (default public, used for scratch tests), BUMP (default 1000000000).
Connection strings are read from env only and never printed.
"""
import os, sys
import psycopg
from psycopg import sql as S

TABLES = ["requests", "free_trials", "skill_leads", "link_clicks", "downloads", "prediction_markets", "pm_snapshots", "news_items",
          "worker_runs", "data_quality", "market_cache", "coingecko_usage", "chat_rate_limits", "a2a_rate_limits", "stream_leases",
          "symbol_requests", "stock_bars"]
if os.environ.get("SYNC_TABLES"): TABLES = [t for t in os.environ["SYNC_TABLES"].split(",") if t in TABLES]   # tests only: subset
MUTABLE = {"prediction_markets", "market_cache", "coingecko_usage", "chat_rate_limits", "a2a_rate_limits", "stream_leases", "worker_runs"}
TS_COLS = ["ts", "created_at", "used_at", "ingested_at", "started_at", "published_at", "fetched_at", "first_seen", "updated_at", "window_start"]
BUMP = int(os.environ.get("BUMP", "1000000000"))

def q(schema, t): return S.Identifier(schema, t)

def cols(c, sch, t):
    return [r[0] for r in c.execute("select column_name from information_schema.columns where table_schema=%s and table_name=%s order by ordinal_position", (sch, t))]

def int_id(c, sch, t):
    r = c.execute("select data_type from information_schema.columns where table_schema=%s and table_name=%s and column_name='id'", (sch, t)).fetchone()
    return bool(r) and r[0] in ("bigint", "integer", "smallint")

def identity_cols(c, sch, t):
    return [r[0] for r in c.execute("""select a.attname from pg_attribute a join pg_class k on k.oid = a.attrelid join pg_namespace n on n.oid = k.relnamespace
        where n.nspname = %s and k.relname = %s and a.attnum > 0 and not a.attisdropped and a.attidentity in ('a','d')""", (sch, t))]

def pk_cols(c, sch, t):
    r = c.execute("""select array(select a.attname from unnest(x.conkey) with ordinality u(n, o) join pg_attribute a on a.attrelid = x.conrelid and a.attnum = u.n order by u.o)
        from pg_constraint x join pg_class k on k.oid = x.conrelid join pg_namespace s on s.oid = k.relnamespace
        where s.nspname = %s and k.relname = %s and x.contype in ('p','u') order by x.contype limit 1""", (sch, t)).fetchone()
    return list(r[0]) if r else []

def key_col(cs, intid=True):
    if "id" in cs and intid: return "id"
    return next((k for k in TS_COLS if k in cs), None)

def sync(src, ss, dst, ds, t, direction, upsert, dry):
    sc, dc = cols(src, ss, t), cols(dst, ds, t)
    if not sc or not dc: return f"{t}: skipped (missing on {'source' if not sc else 'target'})", 0
    common = [x for x in sc if x in dc]
    pk, k, ident = pk_cols(dst, ds, t), key_col(common, int_id(dst, ds, t)), identity_cols(dst, ds, t)
    T, collist = q(ds, t), S.SQL(", ").join(map(S.Identifier, common))
    mode = "upsert" if (upsert and t in MUTABLE and pk) else ("insert" if pk else "except")
    if mode == "except" and k is None: return f"{t}: skipped (no unique key and no id/timestamp column)", 0
    where, args = S.SQL("true"), []
    if mode != "upsert" and k == "id":
        if direction == "forward":
            mx = dst.execute(S.SQL("select max(id) from {} where id < %s").format(T), (BUMP,)).fetchone()[0]
            where, args = S.SQL("id > %s and id < %s"), [mx if mx is not None else -1, BUMP]
        else:
            mx = dst.execute(S.SQL("select max(id) from {} where id >= %s").format(T), (BUMP,)).fetchone()[0]
            where, args = S.SQL("id > %s"), [mx if mx is not None else BUMP - 1]
    elif mode != "upsert" and k is not None:
        mx = dst.execute(S.SQL("select max({}) from {}").format(S.Identifier(k), T)).fetchone()[0]
        if mx is not None:
            where, args = (S.SQL("{} > %s::timestamptz - interval '10 minutes'") if mode == "insert" else S.SQL("{} >= %s")).format(S.Identifier(k)), [mx]
    n = src.execute(S.SQL("select count(*) from {} where ").format(q(ss, t)) + where, args).fetchone()[0]
    if dry or n == 0: return f"{t}: {n} candidate rows [{mode}]{' (dry run)' if dry else ''}", 0
    ov = S.SQL(" overriding system value") if ident else S.SQL("")
    with dst.transaction():
        dst.execute(S.SQL("create temp table _s as select {} from {} limit 0").format(collist, T))
        with src.cursor().copy(S.SQL("copy (select {} from {} where ").format(collist, q(ss, t)) + where + S.SQL(") to stdout"), args or None) as out, \
             dst.cursor().copy(S.SQL("copy _s ({}) from stdin").format(collist)) as inp:
            for chunk in out: inp.write(chunk)
        if mode == "insert":
            sql_ = S.SQL("insert into {} ({}){} select {} from _s on conflict do nothing").format(T, collist, ov, collist)
        elif mode == "upsert":
            rest = [x for x in common if x not in pk]
            set_ = S.SQL(", ").join(S.SQL("{} = excluded.{}").format(S.Identifier(x), S.Identifier(x)) for x in rest) if rest else None
            # Newer side wins where the table tracks updated_at, so a late copy never overwrites a fresher row.
            # Tables without updated_at only rewrite rows that actually differ, so a rerun reports 0.
            tgt = S.SQL("({})").format(S.SQL(", ").join(S.SQL("{}.{}").format(S.Identifier(t), S.Identifier(x)) for x in rest))
            exc = S.SQL("({})").format(S.SQL(", ").join(S.SQL("excluded.{}").format(S.Identifier(x)) for x in rest))
            guard = S.SQL(" where {}.updated_at < excluded.updated_at").format(S.Identifier(t)) if "updated_at" in rest \
                    else S.SQL(" where ") + tgt + S.SQL(" is distinct from ") + exc
            sql_ = S.SQL("insert into {} ({}){} select {} from _s on conflict ({}) ").format(T, collist, ov, collist, S.SQL(", ").join(map(S.Identifier, pk))) \
                   + (S.SQL("do update set ") + set_ + guard if set_ else S.SQL("do nothing"))
        else:
            sql_ = S.SQL("insert into {} ({}){} select {} from _s except select {} from {} where ").format(T, collist, ov, collist, collist, T) + where
            ins = dst.execute(sql_, args).rowcount
        if mode != "except": ins = dst.execute(sql_).rowcount
        dst.execute("drop table _s")
    return f"{t}: {n} candidate rows, {ins} {'upserted' if mode == 'upsert' else 'inserted'} [{mode}{', overriding system value' if ident else ''}]", ins

def bump(dst, ds, dry):
    rows = dst.execute("""select k.relname, a.attname, pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(k.relname), a.attname)
        from pg_attribute a join pg_class k on k.oid = a.attrelid join pg_namespace n on n.oid = k.relnamespace
        where n.nspname = %s and k.relkind = 'r' and a.attnum > 0 and not a.attisdropped
          and pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(k.relname), a.attname) is not null order by 1""", (ds,)).fetchall()
    for t, col, seq in rows:
        if t not in TABLES: continue   # only the always-on tables; forecasts/outcomes ids (public forecast_id) never move
        mx = dst.execute(S.SQL("select coalesce(max({}), 0) from {}").format(S.Identifier(col), q(ds, t))).fetchone()[0]
        to = max(BUMP, mx + 1)
        if not dry: dst.execute("select setval(%s, %s, false)", (seq, to))
        print(f"{t}.{col}: next id {'would be' if dry else '='} {to} (max {mx})")
    return 0

def main():
    a = sys.argv[1:]
    if not a or a[0] not in ("forward", "reverse", "bump"): print(__doc__); return 2
    dry, upsert = "--dry-run" in a, "--upsert" in a
    ss, ds = os.environ.get("SRC_SCHEMA", "public"), os.environ.get("DST_SCHEMA", "public")
    dst_url = os.environ.get("DST_URL")
    if not dst_url: print("set DST_URL"); return 2
    if a[0] == "bump":
        with psycopg.connect(dst_url, autocommit=True, prepare_threshold=None, connect_timeout=15) as dst: return bump(dst, ds, dry)
    src_url = os.environ.get("SRC_URL")
    if not src_url: print("set SRC_URL"); return 2
    if src_url == dst_url and ss == ds: print("refusing: source == target"); return 2
    if os.environ.get("SKIP_HOST_CHECK") != "1" and (a[0] == "reverse") != ("neon.tech" in dst_url):
        print("refusing: direction does not match the target host"); return 2
    with psycopg.connect(src_url, autocommit=True, prepare_threshold=None, connect_timeout=15) as src, \
         psycopg.connect(dst_url, autocommit=True, prepare_threshold=None, connect_timeout=15) as dst:
        bad = total = 0
        for t in TABLES:
            try: msg, n = sync(src, ss, dst, ds, t, a[0], upsert, dry); total += n; print(msg)
            except Exception as e: bad += 1; print(f"{t}: FAILED ({type(e).__name__}: {str(e).splitlines()[0][:120]})")
        print(f"TOTAL inserted/upserted: {total}")
    return 1 if bad else 0

if __name__ == "__main__":
    sys.exit(main())
