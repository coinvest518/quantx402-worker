"""Kronos worker. Runs on GitHub Actions (public repo), one workflow per timeframe:
forecast-15m.yml (1,16,31,46 * * * *), forecast.yml (1h, :03 hourly), forecast-4h.yml (:20 every 4h), forecast-1d.yml (00:05 UTC).
TIMEFRAMES (default "1h") picks which timeframes to run; "auto" runs every timeframe whose candle just closed. One run per close:
  1) forecast each symbol (SYMBOLS + active auto-promoted tracked_symbols) with Kronos-small and write `forecasts`
  2) backfill `outcomes` for forecasts whose last forecast candle has closed (per-timeframe candle length)
  3) retention: drop `paths` JSONB on rows older than 90 days
  4) record the run in `worker_runs`, then exit
On-demand jobs: ondemand.py (ondemand.yml, dispatched by the site with a job_id). News: news.py (news.yml, every 20 min).
Env: DATABASE_URL (Neon direct URL), SYMBOLS, TIMEFRAMES, AUTO_PROMOTE_MAX, LOOKBACK, HORIZON, HORIZON_15M, HORIZON_4H, HORIZON_1D, PATHS, TEMP, TOP_P, TORCH_NUM_THREADS, DRY_RUN=1.
Timestamps (as_of, horizon_end, times_utc) are candle OPEN times in UTC, matching how Kronos stamps bars."""
import json, math, os, sys, time, traceback
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, requests, torch
sys.argv = sys.argv[:1]
from kronos_paths import predict_paths
from model import Kronos, KronosTokenizer, KronosPredictor

E = os.environ.get
SYMBOLS = [s.strip() for s in E("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,BNB-USD,DOGE-USD,ADA-USD,AVAX-USD,LINK-USD,SUI-USD").split(",") if s.strip()]
AUTO_MAX = int(E("AUTO_PROMOTE_MAX", "10"))   # cap on auto-promoted symbols added to the schedule
CRYPTO_RE = r"^[A-Z0-9]{2,10}-USD$"; STOCK_RE = r"^[A-Z]{1,5}(\.[A-Z])?$"
CTX, H, N = int(E("LOOKBACK", 360)), int(E("HORIZON", 24)), int(E("PATHS", 30))
T, TOP_P, SEED = float(E("TEMP", 1.0)), float(E("TOP_P", 0.9)), int(E("SEED", 42))
torch.set_num_threads(int(E("TORCH_NUM_THREADS", 2)))
DB = E("DATABASE_URL", "")
HOT_DB = E("SUPABASE_DATABASE_URL", "")   # always-on DB for stock_bars / worker_runs / symbol_requests; unset = same as DATABASE_URL
DRY = E("DRY_RUN") == "1" or not DB
STRIKE_STEP = {"BTC": 500, "ETH": 25, "SOL": 1}
DISCLAIMER = "Probabilistic model output for research and education. Not financial advice. Past results do not guarantee future results."
iso = lambda t: pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------- timeframes ----------
# sec: bar length. horizon: bars ahead. base: upstream granularity fetched (4h is resampled from 1h; the primary feed has no 4h bars).
TF = {
    "15m": {"sec": 900,   "horizon": int(E("HORIZON_15M", 16)), "base": 900},    # next 4 hours
    "1h":  {"sec": 3600,  "horizon": H,                          "base": 3600},   # next 24 hours (unchanged)
    "4h":  {"sec": 14400, "horizon": int(E("HORIZON_4H", 18)),  "base": 3600},   # next 3 days
    "1d":  {"sec": 86400, "horizon": int(E("HORIZON_1D", 7)),   "base": 86400},  # next 7 days
}
BACKUP_INTERVAL = {900: "15m", 3600: "1h", 14400: "4h", 86400: "1d"}

def due_timeframes(now=None):
    """'auto': 1h every run; 4h when a 4h candle just closed; 1d at the daily close. 15m runs on its own schedule."""
    now = now or datetime.now(timezone.utc)
    out = ["1h"]
    if now.hour % 4 == 0: out.append("4h")
    if now.hour == 0: out.append("1d")
    return out

def selected_timeframes():
    v = E("TIMEFRAMES", "1h").strip()
    tfs = due_timeframes() if v == "auto" else [t.strip() for t in v.split(",") if t.strip()]
    bad = [t for t in tfs if t not in TF]
    if bad: raise SystemExit(f"unknown TIMEFRAMES: {bad}")
    return tfs

# Stored and served labels are generic on purpose (no upstream names in any public field).
# FEED is the internal key the outcome scorer uses to read the realised close from the same feed.
VENUE = {"primary": "aggregated", "backup": "backup", "stock": "us-equities"}
SOURCE = {"primary": "aggregated public exchange candles (closed bars only)", "backup": "backup public exchange feed (closed bars only)", "stock": "our US equity candle store (closed regular-session bars)"}

# ---------- data (closed candles only) ----------
def _floor(sec, now=None):
    now = (now or datetime.now(timezone.utc)).replace(tzinfo=None)
    return datetime.fromtimestamp((int(now.replace(tzinfo=timezone.utc).timestamp()) // sec) * sec, timezone.utc).replace(tzinfo=None)

def _frame(df):
    return df[["timestamps", "open", "high", "low", "close", "volume"]].reset_index(drop=True)

def fetch_primary(symbol, bars, sec):
    end, rows = _floor(sec), []
    while len(rows) < bars + 5:
        start = end - timedelta(seconds=sec * 300)
        r = requests.get(f"https://api.exchange.coinbase.com/products/{symbol}/candles",
                         params={"granularity": sec, "start": start.isoformat(), "end": end.isoformat()}, timeout=20)
        r.raise_for_status(); got = r.json(); rows += got; end = start; time.sleep(0.25)
        if not got: break
    df = pd.DataFrame(rows, columns=["t", "low", "high", "open", "close", "volume"]).drop_duplicates("t").sort_values("t")
    df["timestamps"] = pd.to_datetime(df["t"], unit="s")
    df = df[df["timestamps"] < _floor(sec)]
    return _frame(df.tail(bars))

def fetch_backup(symbol, bars, sec):
    pair = symbol.replace("-USD", "USDT")
    r = requests.get("https://data-api.binance.vision/api/v3/klines", params={"symbol": pair, "interval": BACKUP_INTERVAL[sec], "limit": min(bars + 1, 1000)}, timeout=20)
    r.raise_for_status()
    df = pd.DataFrame([[k[0], *map(float, k[1:6])] for k in r.json()], columns=["t", "open", "high", "low", "close", "volume"])
    df["timestamps"] = pd.to_datetime(df["t"], unit="ms")
    df = df[df["timestamps"] < _floor(sec)]
    return _frame(df.tail(bars))

def resample(df, sec):
    """Aggregate closed base bars into UTC-aligned `sec` bars; drop the trailing partial bucket."""
    g = df.set_index("timestamps").resample(f"{sec}s", origin="epoch", label="left", closed="left")
    out = pd.DataFrame({"open": g["open"].first(), "high": g["high"].max(), "low": g["low"].min(), "close": g["close"].last(),
                        "volume": g["volume"].sum(), "n": g["close"].count()}).dropna()
    per = sec // int((df["timestamps"].iloc[1] - df["timestamps"].iloc[0]).total_seconds())
    out = out[out["n"] == per].drop(columns="n").reset_index()
    return _frame(out[out["timestamps"] < _floor(sec)])

STOCK_SRC = {"15m": ("15m", 1), "1h": ("1h", 1), "4h": ("1h", 4), "1d": ("1d", 1)}

def is_stock(symbol):
    import re
    return bool(re.match(STOCK_RE, symbol))

def stock_candles(c, symbol, bars, tf):
    """Equity bars from stock_bars. 4h = groups of 4 consecutive 1h session bars (matches the site's loadBars)."""
    src, g = STOCK_SRC[tf]
    rows = c.execute("select ts, o, h, l, c, v from stock_bars where symbol = %s and timeframe = %s order by ts desc limit %s", (symbol, src, bars * g)).fetchall()[::-1]
    df = pd.DataFrame(rows, columns=["timestamps", "open", "high", "low", "close", "volume"])
    if df.empty: return df, "stock"
    df["timestamps"] = pd.to_datetime(df["timestamps"], utc=True).dt.tz_localize(None)
    if g > 1:
        df = df.iloc[len(df) % g:].reset_index(drop=True); k = df.index // g
        df = pd.DataFrame({"timestamps": df.groupby(k)["timestamps"].first(), "open": df.groupby(k)["open"].first(), "high": df.groupby(k)["high"].max(),
                           "low": df.groupby(k)["low"].min(), "close": df.groupby(k)["close"].last(), "volume": df.groupby(k)["volume"].sum()})
    return _frame(df.tail(bars)), "stock"

def candles(symbol, bars, tf="1h", conn=None):
    # TEMPORARY seed helper (temp/core-on-hot only): SEED_LAG_BARS=N ends the input N bars early, so a fresh DB gets a run old enough for /basic.
    lag = int(E("SEED_LAG_BARS", "0") or 0)
    df, src = _candles(symbol, bars + lag, tf, conn)
    if lag: df = df.iloc[:-lag].tail(bars).reset_index(drop=True)
    return df, src

def _candles(symbol, bars, tf="1h", conn=None):
    if is_stock(symbol): return stock_candles(conn, symbol, bars, tf)
    cfg = TF[tf]; sec, base = cfg["sec"], cfg["base"]
    need = bars * (sec // base) + (sec // base)
    try:
        df = fetch_primary(symbol, need, base)
        if base != sec: df = resample(df, sec)
        if len(df) >= bars: return df.tail(bars).reset_index(drop=True), "primary"
    except Exception as e:
        print(f"{symbol} {tf}: primary feed failed ({type(e).__name__}), using backup feed", file=sys.stderr)
    return fetch_backup(symbol, bars, sec), "backup"

# ---------- fact list (SPEC_v3 §7.1) ----------
def px(v):
    """Price rounding that keeps sub-cent tokens meaningful: 2 decimals at >= $1, else 6 significant figures."""
    v = float(v)
    return round(v, 2) if abs(v) >= 1 or v == 0 else float(f"{v:.6g}")

def strike_step(symbol, last):
    if symbol.split("-")[0] in STRIKE_STEP: return STRIKE_STEP[symbol.split("-")[0]]
    if last >= 200: return max(1, round(last * 0.005))
    return float(f"{10 ** math.floor(math.log10(last * 0.005)):.1g}")

def build(symbol, feed, hist, paths, y_ts, runtime, tf="1h"):
    H = TF[tf]["horizon"]; venue, source = VENUE[feed], SOURCE[feed]
    last = float(hist["close"].iloc[-1]); closes = paths[:, :, 3]; final = closes[:, -1]
    ret = final / last - 1
    prob_up = round(float((final > last).mean()), 3)
    direction = "UP" if prob_up >= 0.6 else "DOWN" if prob_up <= 0.4 else "NEUTRAL"
    inval = {"UP": float(np.percentile(paths[:, :, 2].min(1), 10)), "DOWN": float(np.percentile(paths[:, :, 1].max(1), 90))}.get(direction)
    step = strike_step(symbol, last)
    base = round(last / step) * step
    strikes = [px(base + k * step) for k in range(-3, 4)]
    lr = np.log(hist["close"]).diff()
    hv = float(lr.rolling(H).sum().dropna().std() * 100) if len(lr) > H + 2 else None
    lo, hi = float(np.percentile(final, 10)), float(np.percentile(final, 90))
    fmt_k = lambda k: str(int(k)) if float(k).is_integer() else str(k)
    fl = {
        "symbol": symbol, "venue": venue, "asset_class": "equity" if feed == "stock" else "crypto", "timeframe": tf,
        "as_of": iso(hist["timestamps"].iloc[-1]), "horizon_bars": H, "horizon_end": iso(y_ts.iloc[-1]),
        "generated_at": iso(datetime.now(timezone.utc).replace(tzinfo=None)),
        "last_close": px(last), "direction": direction, "prob_up": prob_up,
        "expected_move_pct": round(float(np.median(ret) * 100), 3),
        "band_80_pct": [round(float(np.percentile(ret, 10) * 100), 3), round(float(np.percentile(ret, 90) * 100), 3)],
        "expected_range": [px(lo), px(hi)],
        "band_calibrated": False,
        "calibration_note": "80% band is raw model spread, not yet checked against outcomes. Treat as uncalibrated.",
        "confidence": "MEDIUM" if abs(prob_up - 0.5) >= 0.3 else "LOW",
        "confidence_rule": "v1: single model (Kronos-small). MEDIUM if |prob_up-0.5|>=0.3, else LOW. HIGH is disabled until calibration.",
        "invalidation_level": px(inval) if inval is not None else None,
        "hist_vol_over_horizon_pct": round(hv, 3) if hv is not None and math.isfinite(hv) else None,
        "p_close_above": {fmt_k(k): round(float((final > k).mean()), 3) for k in strikes},
        "model": "Kronos-small", "sampling": {"T": T, "top_p": TOP_P, "paths": N, "seed": SEED},
        "context_bars": int(len(hist)), "data_source": source, "disclaimer": DISCLAIMER,
    }
    pct = {f"p{q}": [px(v) for v in np.percentile(closes, q, axis=0)] for q in (10, 25, 50, 75, 90)}
    pct["times_utc"] = [iso(t) for t in y_ts]
    row = dict(symbol=symbol, venue=feed, timeframe=tf, model="Kronos-small", as_of=fl["as_of"], horizon_bars=H,
               horizon_end=fl["horizon_end"], context_bars=fl["context_bars"], sampling=fl["sampling"], last_close=last,
               prob_up=prob_up, expected_move_pct=fl["expected_move_pct"], band_80_pct=fl["band_80_pct"], factlist=fl,
               percentiles=pct, runtime_sec=runtime,
               paths=[{"closes": [px(v) for v in paths[i, :, 3]], "lows": [px(v) for v in paths[i, :, 2]],
                       "highs": [px(v) for v in paths[i, :, 1]]} for i in range(paths.shape[0])])
    return row

class AlreadyDone(Exception): pass

def forecast(pred, symbol, tf="1h", conn=None, skip_existing=False, bars_conn=None):
    torch.manual_seed(SEED); np.random.seed(SEED)
    cfg = TF[tf]; h = cfg["horizon"]
    hist, feed = candles(symbol, CTX, tf, bars_conn if bars_conn is not None else conn)
    if len(hist) < 50: raise RuntimeError(f"only {len(hist)} closed {tf} bars")
    if skip_existing and conn is not None and conn.execute("select 1 from forecasts where symbol = %s and timeframe = %s and model = 'Kronos-small' and as_of = %s",
                                         (symbol, tf, iso(hist["timestamps"].iloc[-1]))).fetchone():
        raise AlreadyDone()   # the other trigger (GitHub schedule or Vercel cron dispatch) already ran this close
    y_ts = pd.Series(pd.date_range(hist["timestamps"].iloc[-1] + pd.Timedelta(seconds=cfg["sec"]), periods=h, freq=f"{cfg['sec']}s"))
    if feed == "stock" and tf == "1d":   # sessions, not calendar days
        y_ts = pd.Series(pd.bdate_range(hist["timestamps"].iloc[-1] + pd.Timedelta(days=1), periods=h) + pd.Timedelta(hours=hist["timestamps"].iloc[-1].hour))
    t0 = time.time()
    # Sub-dollar assets (e.g. meme tokens) are rescaled so the model sees ~$100 prices; its normalisation breaks down near zero.
    last = float(hist["close"].iloc[-1]); k = 10.0 ** (2 - math.floor(math.log10(last))) if 0 < last < 1 else 1.0
    x = hist[["open", "high", "low", "close", "volume"]].copy(); x[["open", "high", "low", "close"]] *= k
    paths = predict_paths(pred, x, hist["timestamps"], y_ts, h, T, TOP_P, N)
    if k != 1.0: paths = paths.copy(); paths[:, :, :4] /= k
    return build(symbol, feed, hist, paths, y_ts, round(time.time() - t0, 1), tf)

# ---------- schedule: config list + auto-promoted symbols ----------
def promote(c, h=None):
    """Promote frequently requested on-demand crypto pairs into tracked_symbols: >=5 requests from >=2 callers in 7 days, capped.
    symbol_requests lives on the always-on DB, tracked_symbols on the core DB, so read and write are separate queries."""
    h = h if h is not None else c
    cand = h.execute("""select symbol, count(*) n from symbol_requests
          where ts > now() - interval '7 days' and endpoint = 'on-demand' and asset_class = 'crypto' and outcome in ('queued', 'served_cached')
          group by symbol having count(*) >= 5 and count(distinct caller_hash) >= 2 order by n desc limit %s""", (AUTO_MAX,)).fetchall()
    for sym, n in cand:
        c.execute("""insert into tracked_symbols (symbol, asset_class, origin, requests_7d, note)
            select %s, 'crypto', 'auto', %s, 'auto-promoted from on-demand requests'
            where (select count(*) from tracked_symbols where origin = 'auto' and active) < %s
            on conflict (symbol) do update set requests_7d = excluded.requests_7d""", (sym, n, AUTO_MAX))

def scheduled_symbols(c):
    if c is None: return SYMBOLS
    import re
    try:
        auto = [r[0] for r in c.execute("select symbol from tracked_symbols where active and asset_class = 'crypto' and origin = 'auto' order by promoted_at limit %s", (AUTO_MAX,)).fetchall()]
    except Exception: auto = []
    return SYMBOLS + [s for s in auto if s not in SYMBOLS and re.match(CRYPTO_RE, s)]

# ---------- db ----------
def db():
    import psycopg
    return psycopg.connect(DB, autocommit=True)

def hot_db(core=None):
    """Connection for the always-on tables. Falls back to the core connection (or a new core one) when unset.
    prepare_threshold=None keeps it safe behind a transaction pooler."""
    if not HOT_DB: return core if core is not None else db()
    import psycopg
    return psycopg.connect(HOT_DB, autocommit=True, prepare_threshold=None, connect_timeout=15)

def save(c, row):
    from psycopg.types.json import Jsonb
    cols = list(row)
    vals = {k: (Jsonb(v) if k in ("sampling", "factlist", "percentiles", "paths") else v) for k, v in row.items()}
    r = c.execute(f"insert into forecasts ({', '.join(cols)}) values ({', '.join('%(' + k + ')s' for k in cols)}) "
                  "on conflict (symbol, timeframe, model, as_of) do nothing returning id", vals).fetchone()
    return r[0] if r else None

def realized_close(symbol, venue, open_ts, tf="1h", c=None):
    """Close of the bar that opens at open_ts, from the same feed the forecast used. Old rows carry the legacy venue names."""
    t = pd.Timestamp(open_ts); t = t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")
    if venue == "stock":
        src, g = STOCK_SRC.get(tf, ("1h", 1))
        r = c.execute("select c from stock_bars where symbol = %s and timeframe = %s and ts >= %s order by ts limit 1 offset %s", (symbol, src, t.to_pydatetime(), g - 1)).fetchone()
        return float(r[0]) if r else None
    sec = TF.get(tf, TF["1h"])["sec"]
    if venue in ("backup", "Binance"):
        r = requests.get("https://data-api.binance.vision/api/v3/klines", params={"symbol": symbol.replace("-USD", "USDT"), "interval": BACKUP_INTERVAL[sec], "startTime": int(t.timestamp() * 1000), "limit": 1}, timeout=20)
        r.raise_for_status(); k = r.json()
        return float(k[0][4]) if k and int(k[0][0]) == int(t.timestamp() * 1000) else None
    if sec == 14400:   # 4h bar = last 1h close inside it
        return realized_close(symbol, venue, t + pd.Timedelta(hours=3), "1h")
    r = requests.get(f"https://api.exchange.coinbase.com/products/{symbol}/candles", params={"granularity": sec, "start": t.isoformat(), "end": (t + pd.Timedelta(seconds=sec)).isoformat()}, timeout=20)
    r.raise_for_status()
    return next((float(x[4]) for x in r.json() if int(x[0]) == int(t.timestamp())), None)

def backfill(c, h=None):
    h = h if h is not None else c
    due = c.execute("""select f.id, f.symbol, f.venue, f.horizon_end, f.last_close, f.factlist->>'direction', f.factlist->'expected_range', f.timeframe
        from forecasts f left join outcomes o on o.forecast_id = f.id
        where o.forecast_id is null and f.horizon_end + (case f.timeframe when '15m' then interval '15 minutes' when '4h' then interval '4 hours'
              when '1d' then interval '1 day' else interval '1 hour' end) <= now()
        order by f.horizon_end limit 200""").fetchall()
    n = 0
    for fid, sym, venue, hend, last, direction, rng, tf in due:
        rc = realized_close(sym, venue, hend, tf, h)
        if rc is None: continue
        hit = (rc > last) if direction == "UP" else (rc < last) if direction == "DOWN" else abs(rc / last - 1) < 0.005
        c.execute("insert into outcomes (forecast_id, realized_close, realized_move_pct, direction_hit, inside_band_80) values (%s,%s,%s,%s,%s) on conflict do nothing",
                  (fid, rc, round((rc / last - 1) * 100, 3), hit, rng[0] <= rc <= rng[1]))
        n += 1; time.sleep(0.2)
    return n

def mirror_tracked(c, h):
    """Copy active auto-promoted symbols to the always-on DB so the next run can schedule without waking the core DB."""
    rows = c.execute("select symbol, promoted_at from tracked_symbols where active and asset_class = 'crypto' and origin = 'auto'").fetchall()
    h.execute("create table if not exists tracked_symbols_mirror (symbol text primary key, promoted_at timestamptz)")
    with h.transaction():   # readers never see an empty mirror; a failure keeps the previous list
        h.execute("delete from tracked_symbols_mirror")
        for sym, at in rows: h.execute("insert into tracked_symbols_mirror values (%s, %s) on conflict do nothing", (sym, at))

def scheduled_symbols_hot(h):
    try:
        auto = [r[0] for r in h.execute("select symbol from tracked_symbols_mirror order by promoted_at limit %s", (AUTO_MAX,)).fetchall()]
    except Exception: auto = []
    return SYMBOLS + [s for s in auto if s not in SYMBOLS and re.match(CRYPTO_RE, s)]

def main():
    hf = "NeoQuasar/"
    tok = KronosTokenizer.from_pretrained(hf + "Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained(hf + "Kronos-small")
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
    split = bool(HOT_DB) and not DRY
    # Split mode: the core DB is opened only after inference, for one short write burst, so it can sleep between windows.
    c = None if (DRY or split) else db()
    h, hot_ok = None, False
    if not DRY:
        try: h = hot_db(c); hot_ok = True
        except Exception as e:
            # Always-on DB outage must not stop forecasting: fall back to the configured symbols and the core DB, skip the run log.
            print(f"WARNING: always-on DB unreachable ({type(e).__name__}); using configured symbols + core DB, run log skipped", file=sys.stderr)
            split = False
            c = c if c is not None else db()
            h = c
    run_id = h.execute("insert into worker_runs default values returning id").fetchone()[0] if hot_ok else None
    done, errors, pending = [], [], []
    tfs = selected_timeframes()
    print("timeframes:", ",".join(tfs))
    syms = scheduled_symbols_hot(h) if split else (scheduled_symbols(c) if (hot_ok or DRY) else list(SYMBOLS))
    shard, shards = int(E("SHARD", "0")), max(1, int(E("SHARDS", "1")))
    syms = syms[shard::shards]   # matrix jobs split symbols so each run fits its cadence; shard 0 also scores outcomes
    if c is not None and "1h" in tfs and shard == 0:
        try: promote(c, h)
        except Exception as e: print("promote skipped:", type(e).__name__)
    for sym, tf in [(s, t) for t in tfs for s in syms]:
        try:
            try: row = forecast(pred, sym, tf, c, skip_existing=c is not None, bars_conn=h)
            except AlreadyDone: print(f"{sym} {tf}: already forecast for this close, skipped"); done.append(f"{sym}:{tf}"); continue
            if DRY:
                print(json.dumps(row["factlist"])); done.append(f"{sym}:{tf}")
                if E("DUMP_ROWS"):  # local testing only: write full rows for db/load-rows.mjs
                    with open(E("DUMP_ROWS"), "a") as fh: fh.write(json.dumps(row) + "\n")
                continue
            if split: pending.append((sym, tf, row)); continue
            fid = save(c, row); done.append(f"{sym}:{tf}")
            print(f"{sym} {tf} as_of={row['as_of']} dir={row['factlist']['direction']} prob_up={row['prob_up']} runtime={row['runtime_sec']}s id={fid}")
        except Exception as e:
            errors.append(f"{sym} {tf}: {type(e).__name__}"); print(f"{sym} {tf} failed: {type(e).__name__}", file=sys.stderr)
    if not DRY:
        if split:
            c = db()
            for sym, tf, row in pending:   # inserts are idempotent (on conflict do nothing), so no pre-check read is needed
                try:
                    fid = save(c, row); done.append(f"{sym}:{tf}")
                    print(f"{sym} {tf} as_of={row['as_of']} dir={row['factlist']['direction']} prob_up={row['prob_up']} runtime={row['runtime_sec']}s id={fid}")
                except Exception as e: errors.append(f"{sym} {tf}: {type(e).__name__}")
            if "1h" in tfs and shard == 0:
                try: promote(c, h); mirror_tracked(c, h)
                except Exception as e: print("promote skipped:", type(e).__name__)
        if shard == 0:
          try: print("outcomes filled:", backfill(c, h))
          except Exception as e: errors.append(f"backfill: {type(e).__name__}")
        try: c.execute("update forecasts set paths = '[]'::jsonb where as_of < now() - interval '90 days' and paths <> '[]'::jsonb")
        except Exception as e: errors.append(f"retention: {type(e).__name__}")
        if h is not c: c.close()
        if run_id is not None:
            h.execute("update worker_runs set finished_at = now(), ok = %s, symbols_done = %s, error = %s where id = %s",
                      (not errors, done, "; ".join(errors)[:2000] or None, run_id))
        h.close()
    return 1 if errors else 0

if __name__ == "__main__":
    sys.exit(main())
