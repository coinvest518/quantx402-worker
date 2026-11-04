"""Catch-up for the site's replay_outbox (v14). Rows parked in one DB's outbox while the other DB was down are applied to
their target with idempotent upserts, then marked applied. Called from the hourly core sync, so it never wakes the core DB
on its own. A missing outbox table (before v14) is a no-op."""
import json, re

def _ident(s):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", s): raise ValueError(f"bad identifier {s}")
    return f'"{s}"'

def _upsert(conn, tbl, op, conflict, row):
    from psycopg.types.json import Jsonb
    cols = list(row)
    vals = [Jsonb(v) if isinstance(v, (dict, list)) else v for v in row.values()]
    upd = [c for c in cols if c not in conflict]
    tail = "do nothing" if op == "insert_ignore" or not upd else "do update set " + ", ".join(f"{_ident(c)} = excluded.{_ident(c)}" for c in upd)
    ov = "overriding system value " if "id" in cols else ""
    conn.execute(f"insert into {_ident(tbl)} ({', '.join(map(_ident, cols))}) {ov}values ({', '.join(['%s'] * len(cols))}) "
                 f"on conflict ({', '.join(map(_ident, conflict))}) {tail}", vals)

def replay(src, dst, target, limit=500):
    """Apply `target`-bound rows parked in src's outbox to dst. Returns (pending, applied, failed)."""
    try:
        if not src.execute("select to_regclass('public.replay_outbox') is not null").fetchone()[0]: return (0, 0, 0)
    except Exception: return (0, 0, 0)
    rows = src.execute("select id, tbl, op, conflict, row from replay_outbox where target_db = %s and applied_at is null order by id limit %s",
                       (target, limit)).fetchall()
    ok = bad = 0
    for oid, tbl, op, conflict, row in rows:
        try:
            _upsert(dst, tbl, op, list(conflict), row if isinstance(row, dict) else json.loads(row))
            src.execute("update replay_outbox set applied_at = now(), attempts = attempts + 1 where id = %s", (oid,)); ok += 1
        except Exception as e:
            src.execute("update replay_outbox set attempts = attempts + 1, last_error = %s where id = %s", (f"{type(e).__name__}"[:200], oid)); bad += 1
    src.execute("delete from replay_outbox where applied_at < now() - interval '7 days'")
    return (len(rows), ok, bad)

def catch_up(h, c):
    """Both directions, inside the core window: hot outbox -> core, core outbox -> hot."""
    return {"to_core": replay(h, c, "core"), "to_hot": replay(c, h, "hot")}
