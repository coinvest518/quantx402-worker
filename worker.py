"""Kronos worker (Render cron, '3 * * * *' UTC). One run per hourly close:
  1) forecast each symbol with Kronos-small and write `forecasts` (factlist + percentiles + 30 paths as JSONB)
  2) backfill `outcomes` for forecasts whose last forecast candle has closed
  3) retention: drop `paths` JSONB on rows older than 90 days
  4) record the run in `worker_runs`, then exit (cron is billed only while running)
Env: DATABASE_URL (Neon direct URL), SYMBOLS, LOOKBACK, HORIZON, PATHS, TEMP, TOP_P, TORCH_NUM_THREADS, DRY_RUN=1.
Timestamps (as_of, horizon_end, times_utc) are candle OPEN times in UTC, matching how Kronos stamps bars."""
import json, math, os, sys, time, traceback
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, requests, torch
sys.argv = sys.argv[:1]
from kronos_paths import predict_paths
from model import Kronos, KronosTokenizer, KronosPredictor

E = os.environ.get
SYMBOLS = [s.strip() for s in E("SYMBOLS", "BTC-USD,ETH-USD").split(",") if s.strip()]
CTX, H, N = int(E("LOOKBACK", 360)), int(E("HORIZON", 24)), int(E("PATHS", 30))
T, TOP_P, SEED = float(E("TEMP", 1.0)), float(E("TOP_P", 0.9)), int(E("SEED", 42))
torch.set_num_threads(int(E("TORCH_NUM_THREADS", 2)))
DB = E("DATABASE_URL", "")
DRY = E("DRY_RUN") == "1" or not DB
STRIKE_STEP = {"BTC": 500, "ETH": 25, "SOL": 1}
DISCLAIMER = "Probabilistic model output for research and education. Not financial advice. Past results do not guarantee future results."
iso = lambda t: pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------- data (closed candles only) ----------
def _hour_floor():
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)

def fetch_coinbase(symbol, bars):
    end, rows = _hour_floor(), []
    while len(rows) < bars + 5:
        start = end - timedelta(hours=300)
        r = requests.get(f"https://api.exchange.coinbase.com/products/{symbol}/candles",
                         params={"granularity": 3600, "start": start.isoformat(), "end": end.isoformat()}, timeout=20)
        r.raise_for_status(); rows += r.json(); end = start; time.sleep(0.25)
    df = pd.DataFrame(rows, columns=["t", "low", "high", "open", "close", "volume"]).drop_duplicates("t").sort_values("t")
    df["timestamps"] = pd.to_datetime(df["t"], unit="s")
    df = df[df["timestamps"] < _hour_floor()]
    return df.tail(bars).reset_index(drop=True)[["timestamps", "open", "high", "low", "close", "volume"]]

def fetch_binance_vision(symbol, bars):
    pair = symbol.replace("-USD", "USDT")
    r = requests.get("https://data-api.binance.vision/api/v3/klines", params={"symbol": pair, "interval": "1h", "limit": min(bars + 1, 1000)}, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame([[k[0], *map(float, k[1:6])] for k in r.json()], columns=["t", "open", "high", "low", "close", "volume"])
    df["timestamps"] = pd.to_datetime(df["t"], unit="ms")
    df = df[df["timestamps"] < _hour_floor()]
    return df.tail(bars).reset_index(drop=True)[["timestamps", "open", "high", "low", "close", "volume"]]

def candles(symbol, bars):
    try:
        df = fetch_coinbase(symbol, bars)
        if len(df) >= bars: return df, "Coinbase", "Coinbase Exchange public candles (closed bars only)"
    except Exception as e:
        print(f"{symbol}: coinbase failed ({e}), falling back to binance mirror", file=sys.stderr)
    df = fetch_binance_vision(symbol, bars)
    return df, "Binance", "Binance public market data mirror, USDT pair (closed bars only)"

# ---------- fact list (SPEC_v3 §7.1) ----------
def build(symbol, venue, source, hist, paths, y_ts, runtime):
    last = float(hist["close"].iloc[-1]); closes = paths[:, :, 3]; final = closes[:, -1]
    ret = final / last - 1
    prob_up = round(float((final > last).mean()), 3)
    direction = "UP" if prob_up >= 0.6 else "DOWN" if prob_up <= 0.4 else "NEUTRAL"
    inval = {"UP": float(np.percentile(paths[:, :, 2].min(1), 10)), "DOWN": float(np.percentile(paths[:, :, 1].max(1), 90))}.get(direction)
    step = STRIKE_STEP.get(symbol.split("-")[0], max(1, round(last * 0.005)))
    base = round(last / step) * step
    strikes = [base + k * step for k in range(-3, 4)]
    lr = np.log(hist["close"]).diff()
    hv = float(lr.rolling(H).sum().dropna().std() * 100) if len(lr) > H + 2 else None
    lo, hi = float(np.percentile(final, 10)), float(np.percentile(final, 90))
    fmt_k = lambda k: str(int(k)) if float(k).is_integer() else str(k)
    fl = {
        "symbol": symbol, "venue": venue, "asset_class": "crypto", "timeframe": "1h",
        "as_of": iso(hist["timestamps"].iloc[-1]), "horizon_bars": H, "horizon_end": iso(y_ts.iloc[-1]),
        "generated_at": iso(datetime.now(timezone.utc).replace(tzinfo=None)),
        "last_close": round(last, 2), "direction": direction, "prob_up": prob_up,
        "expected_move_pct": round(float(np.median(ret) * 100), 3),
        "band_80_pct": [round(float(np.percentile(ret, 10) * 100), 3), round(float(np.percentile(ret, 90) * 100), 3)],
        "expected_range": [round(lo, 2), round(hi, 2)],
        "band_calibrated": False,
        "calibration_note": "80% band is raw model spread, not yet checked against outcomes. Treat as uncalibrated.",
        "confidence": "MEDIUM" if abs(prob_up - 0.5) >= 0.3 else "LOW",
        "confidence_rule": "v1: single model (Kronos-small). MEDIUM if |prob_up-0.5|>=0.3, else LOW. HIGH is disabled until calibration.",
        "invalidation_level": round(inval, 2) if inval is not None else None,
        "hist_vol_over_horizon_pct": round(hv, 3) if hv is not None and math.isfinite(hv) else None,
        "p_close_above": {fmt_k(k): round(float((final > k).mean()), 3) for k in strikes},
        "model": "Kronos-small", "sampling": {"T": T, "top_p": TOP_P, "paths": N, "seed": SEED},
        "context_bars": int(len(hist)), "data_source": source, "disclaimer": DISCLAIMER,
    }
    pct = {f"p{q}": [round(float(v), 2) for v in np.percentile(closes, q, axis=0)] for q in (10, 25, 50, 75, 90)}
    pct["times_utc"] = [iso(t) for t in y_ts]
    row = dict(symbol=symbol, venue=venue, timeframe="1h", model="Kronos-small", as_of=fl["as_of"], horizon_bars=H,
               horizon_end=fl["horizon_end"], context_bars=fl["context_bars"], sampling=fl["sampling"], last_close=last,
               prob_up=prob_up, expected_move_pct=fl["expected_move_pct"], band_80_pct=fl["band_80_pct"], factlist=fl,
               percentiles=pct, runtime_sec=runtime,
               paths=[{"closes": [round(float(v), 2) for v in paths[i, :, 3]], "lows": [round(float(v), 2) for v in paths[i, :, 2]],
                       "highs": [round(float(v), 2) for v in paths[i, :, 1]]} for i in range(paths.shape[0])])
    return row

def forecast(pred, symbol):
    torch.manual_seed(SEED); np.random.seed(SEED)
    hist, venue, source = candles(symbol, CTX)
    y_ts = pd.Series(pd.date_range(hist["timestamps"].iloc[-1] + pd.Timedelta(hours=1), periods=H, freq="h"))
    t0 = time.time()
    paths = predict_paths(pred, hist[["open", "high", "low", "close", "volume"]], hist["timestamps"], y_ts, H, T, TOP_P, N)
    return build(symbol, venue, source, hist, paths, y_ts, round(time.time() - t0, 1))

# ---------- db ----------
def db():
    import psycopg
    return psycopg.connect(DB, autocommit=True)

def save(c, row):
    from psycopg.types.json import Jsonb
    cols = list(row)
    vals = {k: (Jsonb(v) if k in ("sampling", "factlist", "percentiles", "paths") else v) for k, v in row.items()}
    r = c.execute(f"insert into forecasts ({', '.join(cols)}) values ({', '.join('%(' + k + ')s' for k in cols)}) "
                  "on conflict (symbol, timeframe, model, as_of) do nothing returning id", vals).fetchone()
    return r[0] if r else None

def realized_close(symbol, venue, open_ts):
    t = pd.Timestamp(open_ts); t = t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")
    if venue == "Binance":
        r = requests.get("https://data-api.binance.vision/api/v3/klines", params={"symbol": symbol.replace("-USD", "USDT"), "interval": "1h", "startTime": int(t.timestamp() * 1000), "limit": 1}, timeout=20)
        r.raise_for_status(); k = r.json()
        return float(k[0][4]) if k and int(k[0][0]) == int(t.timestamp() * 1000) else None
    r = requests.get(f"https://api.exchange.coinbase.com/products/{symbol}/candles", params={"granularity": 3600, "start": t.isoformat(), "end": (t + pd.Timedelta(hours=1)).isoformat()}, timeout=20)
    r.raise_for_status()
    return next((float(x[4]) for x in r.json() if int(x[0]) == int(t.timestamp())), None)

def backfill(c):
    due = c.execute("""select f.id, f.symbol, f.venue, f.horizon_end, f.last_close, f.factlist->>'direction', f.factlist->'expected_range'
        from forecasts f left join outcomes o on o.forecast_id = f.id
        where o.forecast_id is null and f.horizon_end <= now() - interval '1 hour'
        order by f.horizon_end limit 200""").fetchall()
    n = 0
    for fid, sym, venue, hend, last, direction, rng in due:
        rc = realized_close(sym, venue, hend)
        if rc is None: continue
        hit = (rc > last) if direction == "UP" else (rc < last) if direction == "DOWN" else abs(rc / last - 1) < 0.005
        c.execute("insert into outcomes (forecast_id, realized_close, realized_move_pct, direction_hit, inside_band_80) values (%s,%s,%s,%s,%s) on conflict do nothing",
                  (fid, rc, round((rc / last - 1) * 100, 3), hit, rng[0] <= rc <= rng[1]))
        n += 1; time.sleep(0.2)
    return n

def main():
    hf = "NeoQuasar/"
    tok = KronosTokenizer.from_pretrained(hf + "Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained(hf + "Kronos-small")
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
    c = None if DRY else db()
    run_id = None if DRY else c.execute("insert into worker_runs default values returning id").fetchone()[0]
    done, errors = [], []
    for sym in SYMBOLS:
        try:
            row = forecast(pred, sym)
            if DRY:
                print(json.dumps(row["factlist"])); done.append(sym)
                if E("DUMP_ROWS"):  # local testing only: write full rows for db/load-rows.mjs
                    with open(E("DUMP_ROWS"), "a") as fh: fh.write(json.dumps(row) + "\n")
                continue
            fid = save(c, row); done.append(sym)
            print(f"{sym} as_of={row['as_of']} dir={row['factlist']['direction']} prob_up={row['prob_up']} runtime={row['runtime_sec']}s id={fid}")
        except Exception as e:
            errors.append(f"{sym}: {e}"); traceback.print_exc()
    if not DRY:
        try: print("outcomes filled:", backfill(c))
        except Exception as e: errors.append(f"backfill: {e}"); traceback.print_exc()
        try: c.execute("update forecasts set paths = '[]'::jsonb where as_of < now() - interval '90 days' and paths <> '[]'::jsonb")
        except Exception as e: errors.append(f"retention: {e}")
        c.execute("update worker_runs set finished_at = now(), ok = %s, symbols_done = %s, error = %s where id = %s",
                  (not errors, done, "; ".join(errors)[:2000] or None, run_id))
        c.close()
    return 1 if errors else 0

if __name__ == "__main__":
    sys.exit(main())
