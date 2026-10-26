"""5m loop runner (SPEC_5m_signal §3 Option B). Loads Kronos-small once, then every 5m close (+RUN_OFFSET s):
record missed slots, forecast SYMBOLS_5M, score outcomes, log a worker_runs heartbeat row (timeframe='5m', runner=<run id>).
Hourly: null 5m paths older than 14 days.

Stop switch: repo variable RUN_5M must be 'true'. The workflow job is skipped otherwise, and the loop re-checks it
before dispatching a successor (API read; falls back to the value seen at job start). Not 'true' => no relaunch, the
current run finishes its window and exits. Turning the chain off = set RUN_5M to anything else.

Handover: at HANDOFF_AT seconds it dispatches its successor (only if RUN_5M is true and no other run of this workflow is
already queued/in progress), keeps forecasting, and exits as soon as the successor writes its first OK heartbeat
(or at LOOP_SECONDS, whichever is first). At startup a run exits if two or more other runs are already active.
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

def other_active_runs():
    wf = E("GH_WORKFLOW", "forecast-5m.yml"); n = 0
    for st in ("queued", "in_progress", "pending", "waiting"):
        d = _gh(f"actions/workflows/{wf}/runs?status={st}&per_page=20")
        if d is None: return None
        n += sum(1 for x in d.get("workflow_runs", []) if str(x.get("id")) != str(E("GH_RUN_ID")))
    return n

def dispatch_successor():
    tok, repo, wf = E("GH_TOKEN"), E("GH_REPO"), E("GH_WORKFLOW", "forecast-5m.yml")
    if not run5m_enabled(): print("handover: RUN_5M is not 'true', not relaunching (chain stops)"); return False
    if not (tok and repo): print("handover: no GH_TOKEN/GH_REPO, not relaunching"); return False
    act = other_active_runs()
    if act: print(f"handover: {act} other run(s) already active, not dispatching"); return False
    try:
        r = requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
                          headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"}, json={"ref": E("GH_REF", "main")}, timeout=20)
        print("handover: successor dispatched" if r.status_code == 204 else f"handover: dispatch HTTP {r.status_code}"); return r.status_code == 204
    except Exception as e:
        print("handover: dispatch failed", type(e).__name__); return False

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
    print(f"cycle {datetime.now(timezone.utc):%H:%M:%S}Z done={len(done)} err={len(errors)} {time.time()-t0:.1f}s")

def main():
    if E("GH_RUN_ID") and not run5m_enabled(): print("RUN_5M is not 'true', exiting"); return
    act = other_active_runs()
    if act is not None and act >= 2: print(f"{act} other runs active, exiting to avoid a duplicate chain"); return
    from model import Kronos, KronosTokenizer, KronosPredictor
    tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-small")
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
    c = None if w.DRY else w.db()
    start, dispatched_at, it = time.time(), None, 0
    while True:
        if dispatched_at is None and time.time() - start >= HANDOFF_AT:
            dispatched_at = datetime.now(timezone.utc) if dispatch_successor() else datetime.max.replace(tzinfo=timezone.utc)
        slot = next_slot()
        if slot - start > LOOP: print("loop window over, exiting"); break
        time.sleep(max(0, slot - time.time()))
        one_cycle(pred, c, it); it += 1
        if c is not None and dispatched_at and dispatched_at.year < 9999 and successor_alive(c, dispatched_at):
            print("handover: successor heartbeat seen, exiting"); break
        if MAX_ITER and it >= MAX_ITER: break
    if c is not None: c.close()

if __name__ == "__main__":
    main()
