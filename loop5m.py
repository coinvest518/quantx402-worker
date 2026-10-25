"""5m loop runner (SPEC_5m_signal §3 Option B). Loads Kronos-small once, then every 5m close (+RUN_OFFSET s):
forecast SYMBOLS_5M, score outcomes, null 5m paths older than 14 days, log a worker_runs row (timeframe='5m').
Runs for LOOP_SECONDS (default 5.5 h, under the 6 h Actions job limit). Handover: at HANDOFF_SECONDS before the
end it dispatches its successor workflow (needs GH_TOKEN with actions:write), keeps forecasting until the
successor's first slot, then exits. forecasts has a unique (symbol, timeframe, model, as_of) key and
forecast(skip_existing=True) skips bars already written, so overlap never double-writes. On start it records any
missed 5m slots since the last stored 5m forecast in data_quality.
Env: DATABASE_URL, SYMBOLS_5M, LOOP_SECONDS, HANDOFF_SECONDS, RUN_OFFSET, GH_TOKEN, GH_REPO, GH_WORKFLOW, DRY_RUN=1, MAX_ITER."""
import os, sys, time, json
from datetime import datetime, timezone, timedelta
import requests
os.environ.setdefault("TIMEFRAMES", "5m")
import worker as w

E = os.environ.get
SYMS = [s.strip() for s in E("SYMBOLS_5M", "BTC-USD,ETH-USD,SOL-USD,XRP-USD").split(",") if s.strip()]
LOOP, HANDOFF, OFFSET = int(E("LOOP_SECONDS", 19800)), int(E("HANDOFF_SECONDS", 600)), int(E("RUN_OFFSET", 60))
MAX_ITER = int(E("MAX_ITER", 0))
SEC = 300

def next_slot(now=None):
    now = now or time.time()
    return (int(now) // SEC + 1) * SEC + OFFSET

def record_gaps(c):
    """Missed 5m bars between the newest stored 5m as_of and the bar that just closed."""
    r = c.execute("select max(as_of) from forecasts where timeframe = '5m'").fetchone()[0]
    if r is None: return 0
    expected = w._floor(SEC).replace(tzinfo=timezone.utc) - timedelta(seconds=SEC)   # bar we are about to forecast is the next
    missed = int((expected - r).total_seconds() // SEC)
    if missed > 0:
        c.execute("insert into data_quality (timeframe, kind, gap_start, gap_end, missed_bars, note) values ('5m','gap',%s,%s,%s,%s)",
                  (r + timedelta(seconds=SEC), expected, missed, "runner handover/startup"))
    return max(missed, 0)

def dispatch_successor():
    tok, repo, wf = E("GH_TOKEN"), E("GH_REPO"), E("GH_WORKFLOW", "forecast-5m.yml")
    if not (tok and repo): print("handover: no GH_TOKEN/GH_REPO, relying on cron relaunch"); return False
    try:
        r = requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/dispatches",
                          headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"}, json={"ref": E("GH_REF", "main")}, timeout=20)
        print("handover: successor dispatched" if r.status_code == 204 else f"handover: dispatch HTTP {r.status_code}"); return r.status_code == 204
    except Exception as e:
        print("handover: dispatch failed", type(e).__name__); return False

def one_cycle(pred, c):
    done, errors, t0 = [], [], time.time()
    run_id = None if c is None else c.execute("insert into worker_runs (timeframe) values ('5m') returning id").fetchone()[0]
    for sym in SYMS:
        try:
            row = w.forecast(pred, sym, "5m", c, skip_existing=c is not None)
            if c is None: print(json.dumps({k: row[k] for k in ("symbol", "as_of", "horizon_end", "prob_up", "runtime_sec")}), "bars:", row["factlist"].get("context_bars", "?"))
            else: w.save(c, row)
            done.append(f"{sym}:5m")
        except w.AlreadyDone: done.append(f"{sym}:5m")
        except Exception as e: errors.append(f"{sym} 5m: {type(e).__name__}"); print(errors[-1], file=sys.stderr)
    if c is not None:
        try: print("outcomes filled:", w.backfill(c))
        except Exception as e: errors.append(f"backfill: {type(e).__name__}")
        try: c.execute("update forecasts set paths = '[]'::jsonb where timeframe = '5m' and as_of < now() - interval '14 days' and paths <> '[]'::jsonb")
        except Exception as e: errors.append(f"retention: {type(e).__name__}")
        c.execute("update worker_runs set finished_at = now(), ok = %s, symbols_done = %s, error = %s where id = %s", (not errors, done, "; ".join(errors)[:2000] or None, run_id))
    print(f"cycle {datetime.now(timezone.utc):%H:%M:%S}Z done={len(done)} err={len(errors)} {time.time()-t0:.1f}s")

def main():
    from model import Kronos, KronosTokenizer, KronosPredictor
    tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-small")
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
    c = None if w.DRY else w.db()
    if c is not None:
        try: print("gap bars recorded:", record_gaps(c))
        except Exception as e: print("gap check failed", type(e).__name__)
    start, handed, it = time.time(), False, 0
    while True:
        elapsed = time.time() - start
        if not handed and elapsed >= LOOP - HANDOFF: handed = dispatch_successor() or True
        slot = next_slot()
        if slot - start > LOOP: break
        time.sleep(max(0, slot - time.time()))
        one_cycle(pred, c); it += 1
        if MAX_ITER and it >= MAX_ITER: break
    if c is not None: c.close()

if __name__ == "__main__":
    main()
