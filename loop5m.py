"""5m loop runner (SPEC_5m_signal §3 Option B). Loads Kronos-small once, then every 5m close (+RUN_OFFSET s):
record missed slots, forecast SYMBOLS_5M, score outcomes, log a worker_runs heartbeat row (timeframe='5m', runner=<run id>).
Hourly: null 5m paths older than 14 days.

Stop switch: repo variable RUN_5M must be 'true'. The workflow job is skipped otherwise, and the loop re-checks it
before dispatching a successor (API read; falls back to the value seen at job start). Not 'true' => no relaunch, the
current run finishes its window and exits. Turning the chain off = set RUN_5M to anything else.

Handover: at HANDOFF_AT seconds it dispatches its successor (only if RUN_5M is true and no other run of this workflow is
already queued/in progress), keeps forecasting, and exits as soon as the successor writes its first OK heartbeat
(or at LOOP_SECONDS, whichever is first). Tie-break: a run exits (at startup and after each cycle until it has been handed over to) if another active run
has a lower run id and that older run started less than DUP_WINDOW s ago, i.e. it's a duplicate start, not a
predecessor in its handover window. The newest run always wins a real handover once it has cycled.
Dispatch failures are retried with backoff; if all fail, a data_quality(kind='relaunch_failed') row is written and
dispatch is retried every RETRY_EVERY s until the hard stop.
DB: the connection is pinged before each cycle and reopened on OperationalError/InterfaceError (cycle retried once).
If the retry fails again, or the reconnect itself fails (Neon down), the run never crashes: it logs an ERROR, tries to
write data_quality(kind='cycle_failed'), drops the connection and carries on to the next slot (reconnecting then).
worker_runs rows of failed cycles are finalized (finished_at, ok=false, error) right away or on the next good connect.
Overlap never double-writes: forecasts has a unique (symbol, timeframe, model, as_of) key plus skip_existing.
Env: DATABASE_URL, SYMBOLS_5M, LOOP_SECONDS, HANDOFF_AT, RUN_OFFSET, RUN_5M, GH_TOKEN, GH_REPO, GH_WORKFLOW, GH_RUN_ID,
BACKLOG_WARN, DRY_RUN=1, MAX_ITER."""
import os, sys, time, json
from datetime import datetime, timezone, timedelta
import requests
os.environ.setdefault("TIMEFRAMES", "5m")
import worker as w

E = os.environ.get
SYMS = [s.strip() for s in E("SYMBOLS_5M", "BTC-USD,ETH-USD,SOL-USD,XRP-USD").split(",") if s.strip()]
LOOP = int(E("LOOP_SECONDS", 19200))          # 5h20m hard stop; job timeout is 355 min
HANDOFF_AT = int(E("HANDOFF_AT", 18300))      # dispatch successor at 5h05m (setup ~5 min, then first cycle)
OFFSET = int(E("RUN_OFFSET", 60))
MAX_ITER = int(E("MAX_ITER", 0))
BACKLOG_WARN = int(E("BACKLOG_WARN", 400))
RUNNER = E("GH_RUN_ID") or f"local-{os.getpid()}"
SEC = 300
DUP_WINDOW = int(E("DUP_WINDOW", 3600))       # an older active run younger than this is a duplicate, not a predecessor
RETRY_EVERY = int(E("RETRY_EVERY", 300))      # periodic re-dispatch after a failed handover
BACKOFF = [int(x) for x in E("DISPATCH_BACKOFF", "5,15,45").split(",") if x]
_sleep = time.sleep

def _dberrs():
    import psycopg
    return (psycopg.OperationalError, psycopg.InterfaceError)

def ensure_conn(c):
    """Ping; reopen via w.db() if the server dropped the idle connection."""
    if c is None: return None
    try:
        c.execute("select 1"); return c
    except _dberrs() as e:
        print("db: connection lost, reconnecting", type(e).__name__)
        try: c.close()
        except Exception: pass
        return w.db()

_pending_runs = []   # worker_runs ids whose cycle failed before they could be finalized

def finalize_pending(c, why="cycle_failed"):
    """Close out worker_runs rows left open by failed cycles. Safe to call any time; keeps ids if the DB is down."""
    if c is None or not _pending_runs: return 0
    try:
        for rid in list(_pending_runs):
            c.execute("update worker_runs set finished_at = coalesce(finished_at, now()), ok = false, "
                      "error = coalesce(error, %s) where id = %s and finished_at is null", (why, rid))
            _pending_runs.remove(rid)
        return 1
    except Exception as e:
        print("worker_runs finalize failed", type(e).__name__, file=sys.stderr); return 0

def log_cycle_failed(c, err, slot_it):
    print(f"ERROR cycle {slot_it} failed after retry: {type(err).__name__}; continuing to next slot", file=sys.stderr)
    if c is None: return
    try:
        c.execute("insert into data_quality (timeframe, kind, note) values ('5m','cycle_failed',%s)",
                  (f"runner {RUNNER} iter {slot_it}: {type(err).__name__}",))
    except Exception as e: print("data_quality write failed", type(e).__name__, file=sys.stderr)

def _drop(c):
    if c is None: return None
    try: c.close()
    except Exception: pass
    return None

def connect(c):
    """Live connection or raise. c=None means 'reconnect' (only used when not DRY)."""
    c = w.db() if c is None else ensure_conn(c)
    finalize_pending(c)
    return c

def safe_cycle(pred, c, it):
    """Run one cycle; retry once on DB error; never raise. Returns the connection to use next (None = reconnect)."""
    if w.DRY:
        try: one_cycle(pred, None, it)
        except Exception as e: log_cycle_failed(None, e, it)
        return None
    try:
        c = connect(c); one_cycle(pred, c, it); return c
    except Exception as e:
        print("db: cycle failed", type(e).__name__, "- reconnecting and retrying once")
        c = _drop(c)
    try:
        c = connect(None); one_cycle(pred, c, it); return c
    except Exception as e:
        log_cycle_failed(c, e, it)
        finalize_pending(c)
        return _drop(c)

def next_slot(now=None):
    now = now or time.time()
    return (int(now) // SEC + 1) * SEC + OFFSET

def record_gaps(c):
    """Missed 5m bars between the newest stored 5m as_of and the bar this cycle forecasts. Called every cycle;
    a gap is only written once (dedup on gap_start), so overlapping runs don't double-log."""
    r = c.execute("select max(as_of) from forecasts where timeframe = '5m'").fetchone()[0]
    if r is None: return 0
    expected = w._floor(SEC).replace(tzinfo=timezone.utc) - timedelta(seconds=SEC)
    missed = int((expected - r).total_seconds() // SEC) - 1   # bars strictly between last stored and the one being written now
    if missed > 0:
        c.execute("""insert into data_quality (timeframe, kind, gap_start, gap_end, missed_bars, note)
            select '5m','gap',%s,%s,%s,%s where not exists
            (select 1 from data_quality where timeframe='5m' and kind='gap' and gap_start=%s)""",
                  (r + timedelta(seconds=SEC), expected - timedelta(seconds=SEC), missed, f"runner {RUNNER}", r + timedelta(seconds=SEC)))
    return max(missed, 0)

def _gh(path):
    tok, repo = E("GH_TOKEN"), E("GH_REPO")
    if not (tok and repo): return None
    try:
        r = requests.get(f"https://api.github.com/repos/{repo}/{path}", timeout=20,
                         headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"})
        return r.json() if r.status_code == 200 else None
    except Exception: return None

def run5m_enabled():
    v = _gh("actions/variables/RUN_5M")
    val = (v or {}).get("value", E("RUN_5M", ""))
    if v is None: print("stop switch: API read unavailable, using job-start value")
    return str(val).strip().lower() == "true"

def other_runs():
    """Other queued/active runs of this workflow as dicts (id, run_started_at), or None if the API is unavailable."""
    wf = E("GH_WORKFLOW", "forecast-5m.yml"); out = []
    for st in ("queued", "in_progress", "pending", "waiting"):
        d = _gh(f"actions/workflows/{wf}/runs?status={st}&per_page=20")
        if d is None: return None
        out += [x for x in d.get("workflow_runs", []) if str(x.get("id")) != str(E("GH_RUN_ID"))]
    return out

def other_active_runs():
    r = other_runs(); return None if r is None else len(r)

def _ts(x):
    v = x.get("run_started_at") or x.get("created_at")
    try: return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except Exception: return None

def should_yield(runs, my_id, now=None):
    """Deterministic tie-break: yield iff some other active run has a lower id AND started < DUP_WINDOW ago
    (a duplicate). An older run past DUP_WINDOW is a predecessor handing over, so we stay."""
    if not runs or not str(my_id or "").isdigit(): return False
    now = now or time.time()
    for x in runs:
        try: oid = int(x.get("id"))
        except Exception: continue
        t = _ts(x)
        if oid < int(my_id) and t is not None and now - t < DUP_WINDOW: return True
    return False

def _post_dispatch():
    tok, repo, wf = E("GH_TOKEN"), E("GH_REPO"), E("GH_WORKFLOW", "forecast-5m.yml")
    try:
        r = requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
                          headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"}, json={"ref": E("GH_REF", "main")}, timeout=20)
        if r.status_code == 204: return True
        print(f"handover: dispatch HTTP {r.status_code}")
    except Exception as e:
        print("handover: dispatch failed", type(e).__name__)
    return False

def dispatch_successor():
    """Returns 'ok' (dispatched), 'skip' (intentional: switch off / no token / successor already active) or 'failed'."""
    if not run5m_enabled(): print("handover: RUN_5M is not 'true', not relaunching (chain stops)"); return "skip"
    if not (E("GH_TOKEN") and E("GH_REPO")): print("handover: no GH_TOKEN/GH_REPO, not relaunching"); return "skip"
    act = other_active_runs()
    if act: print(f"handover: {act} other run(s) already active, not dispatching"); return "ok"
    for i, wait in enumerate([0] + BACKOFF):
        if wait: _sleep(wait)
        if _post_dispatch(): print("handover: successor dispatched"); return "ok"
    return "failed"

def log_relaunch_failed(c, attempt):
    print(f"ERROR handover: successor dispatch failed after retries (attempt round {attempt}); will retry every {RETRY_EVERY}s", file=sys.stderr)
    if c is None: return
    try:
        c.execute("insert into data_quality (timeframe, kind, note) values ('5m','relaunch_failed',%s)",
                  (f"runner {RUNNER} round {attempt}",))
    except Exception as e: print("data_quality write failed", type(e).__name__, file=sys.stderr)

def successor_alive(c, since):
    """True once a different runner has logged an OK 5m cycle after we dispatched."""
    return c.execute("select 1 from worker_runs where timeframe='5m' and ok and runner is distinct from %s and started_at > %s limit 1",
                     (RUNNER, since)).fetchone() is not None

def check_backlog(c):
    n = c.execute("""select count(*) from forecasts f left join outcomes o on o.forecast_id = f.id
        where o.forecast_id is null and f.timeframe = '5m' and f.horizon_end + interval '5 minutes' <= now()""").fetchone()[0]
    if n > BACKLOG_WARN:
        print(f"backfill backlog: {n} unscored 5m forecasts")
        c.execute("""insert into data_quality (timeframe, kind, missed_bars, note)
            select '5m','backlog',%s,'unscored 5m forecasts past horizon' where not exists
            (select 1 from data_quality where timeframe='5m' and kind='backlog' and ts > now() - interval '1 hour')""", (n,))
    return n

def one_cycle(pred, c, it):
    done, errors, t0 = [], [], time.time()
    run_id = None
    if c is not None:
        try: g = record_gaps(c); g and print("gap bars recorded:", g)
        except Exception as e: errors.append(f"gaps: {type(e).__name__}")
        run_id = c.execute("insert into worker_runs (timeframe, runner) values ('5m', %s) returning id", (RUNNER,)).fetchone()[0]
        _pending_runs.append(run_id)   # removed once finalized below; else closed out by finalize_pending
    for sym in SYMS:
        try:
            row = w.forecast(pred, sym, "5m", c, skip_existing=c is not None)
            if c is None: print(json.dumps({k: row[k] for k in ("symbol", "as_of", "horizon_end", "prob_up", "runtime_sec")}), "bars:", row["factlist"].get("context_bars", "?"))
            else: w.save(c, row)
            done.append(f"{sym}:5m")
        except w.AlreadyDone: done.append(f"{sym}:5m")
        except Exception as e: errors.append(f"{sym} 5m: {type(e).__name__}"); print(errors[-1], file=sys.stderr)
    if c is not None:
        try: print("outcomes filled:", w.backfill(c)); check_backlog(c)
        except Exception as e: errors.append(f"backfill: {type(e).__name__}")
        if it == 0 or datetime.now(timezone.utc).minute < 5:   # hourly retention
            try: c.execute("update forecasts set paths = '[]'::jsonb where timeframe = '5m' and as_of < now() - interval '14 days' and paths <> '[]'::jsonb")
            except Exception as e: errors.append(f"retention: {type(e).__name__}")
        c.execute("update worker_runs set finished_at = now(), ok = %s, symbols_done = %s, error = %s where id = %s", (not errors, done, "; ".join(errors)[:2000] or None, run_id))
        if run_id in _pending_runs: _pending_runs.remove(run_id)
    print(f"cycle {datetime.now(timezone.utc):%H:%M:%S}Z done={len(done)} err={len(errors)} {time.time()-t0:.1f}s")

def main():
    if E("GH_RUN_ID") and not run5m_enabled(): print("RUN_5M is not 'true', exiting"); return
    if should_yield(other_runs(), E("GH_RUN_ID")): print("tie-break: an older run started recently, exiting as duplicate"); return
    from model import Kronos, KronosTokenizer, KronosPredictor
    tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-small")
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
    c = None
    if not w.DRY:
        try: c = w.db()
        except Exception as e: print("ERROR db: unreachable at startup, will retry each cycle", type(e).__name__, file=sys.stderr)
    start, dispatched_at, it = time.time(), None, 0
    fail_round, next_retry = 0, None
    while True:
        el = time.time() - start
        if dispatched_at is None and (el >= HANDOFF_AT if next_retry is None else time.time() >= next_retry):
            res = dispatch_successor()
            if res == "ok": dispatched_at, next_retry = datetime.now(timezone.utc), None
            elif res == "skip": dispatched_at = datetime.max.replace(tzinfo=timezone.utc)
            else:
                fail_round += 1
                try: c = None if w.DRY else connect(c)
                except Exception as e: print("db: unreachable for relaunch_failed log", type(e).__name__, file=sys.stderr); c = _drop(c)
                log_relaunch_failed(c, fail_round)
                next_retry = time.time() + RETRY_EVERY
        slot = next_slot()
        if slot - start > LOOP: print("loop window over, exiting"); break
        time.sleep(max(0, slot - time.time()))
        c = safe_cycle(pred, c, it)
        it += 1
        alive = False
        if c is not None and dispatched_at and dispatched_at.year < 9999:
            try: alive = successor_alive(c, dispatched_at)
            except Exception as e: print("db: successor check failed", type(e).__name__, file=sys.stderr); c = _drop(c)
        if alive: print("handover: successor heartbeat seen, exiting"); break
        if dispatched_at is None and should_yield(other_runs(), E("GH_RUN_ID")):
            print("tie-break: older duplicate run active, exiting"); break
        if MAX_ITER and it >= MAX_ITER: break
    _drop(c)

if __name__ == "__main__":
    main()
