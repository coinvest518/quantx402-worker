"""Run Kronos and keep every sampled path (the stock predict() averages them).
Usage: python kronos_paths.py  -> writes outputs/*.json, *.png
"""
import inspect, json, sys, time, os
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, requests, torch
sys.path.insert(0, os.path.dirname(__file__))
import model.kronos as K
from model import Kronos, KronosTokenizer, KronosPredictor

torch.manual_seed(42); np.random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "outputs"); os.makedirs(OUT, exist_ok=True)

# --- patch: same autoregressive loop, but return all sample paths instead of their mean
src = inspect.getsource(K.auto_regressive_inference)
src = src.replace("def auto_regressive_inference(", "def auto_regressive_inference_paths(")
src = src.replace("preds = np.mean(preds, axis=1)\n", "")
exec(src, K.__dict__)

def predict_paths(pred: KronosPredictor, df, x_ts, y_ts, pred_len, T=1.0, top_p=0.9, n=30):
    df = df.copy()
    if "volume" not in df: df["volume"] = 0.0
    if "amount" not in df: df["amount"] = df["volume"] * df[["open","high","low","close"]].mean(axis=1)
    cols = ["open","high","low","close","volume","amount"]
    x = df[cols].values.astype(np.float32)
    mu, sd = x.mean(0), x.std(0)
    xn = np.clip((x - mu) / (sd + 1e-5), -pred.clip, pred.clip)
    xs = K.calc_time_stamps(x_ts).values.astype(np.float32)
    ys = K.calc_time_stamps(y_ts).values.astype(np.float32)
    t = lambda a: torch.from_numpy(a[None]).to(pred.device)
    p = K.auto_regressive_inference_paths(pred.tokenizer, pred.model, t(xn), t(xs), t(ys),
            pred.max_context, pred_len, pred.clip, T, 0, top_p, n, False)  # (1, n, L, 6)
    p = p[0, :, -pred_len:, :] * (sd + 1e-5) + mu
    return p  # (n, pred_len, 6)

def coinbase_1h(product="BTC-USD", bars=420):
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows = []
    while len(rows) < bars + 5:
        start = end - timedelta(hours=300)
        r = requests.get(f"https://api.exchange.coinbase.com/products/{product}/candles",
                         params={"granularity": 3600, "start": start.isoformat(), "end": end.isoformat()}, timeout=20)
        r.raise_for_status(); rows += r.json(); end = start; time.sleep(0.3)
    df = pd.DataFrame(rows, columns=["t","low","high","open","close","volume"]).drop_duplicates("t").sort_values("t")
    df["timestamps"] = pd.to_datetime(df["t"], unit="s")
    df = df[df["timestamps"] < datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)]  # closed bars only
    return df.tail(bars).reset_index(drop=True)[["timestamps","open","high","low","close","volume"]]

def yf_daily(sym="SPY", period="2y"):
    import yfinance as yf
    d = yf.Ticker(sym).history(period=period, interval="1d", auto_adjust=True).dropna()
    d = d[["Open","High","Low","Close","Volume"]]; d.columns = ["open","high","low","close","volume"]
    d.index = d.index.tz_localize(None); d.index.name = "timestamps"
    return d.reset_index()

def summarize(symbol, venue, timeframe, hist, paths, y_ts, model_name, T, top_p):
    last = float(hist["close"].iloc[-1]); final = paths[:, -1, 3]
    ret = final / last - 1
    pct = lambda q: float(np.percentile(final, q))
    mean_path = paths[:, :, 3].mean(0)
    s = {
        "symbol": symbol, "venue": venue, "timeframe": timeframe, "model": model_name,
        "context_bars": int(len(hist)), "horizon_bars": int(paths.shape[1]),
        "as_of": str(hist["timestamps"].iloc[-1]), "horizon_end": str(y_ts.iloc[-1]),
        "sampling": {"T": T, "top_p": top_p, "sample_count": int(paths.shape[0])},
        "last_close": round(last, 4),
        "forecast_close": {"mean": round(float(final.mean()), 4), "median": round(pct(50), 4),
                            "p10": round(pct(10), 4), "p25": round(pct(25), 4), "p75": round(pct(75), 4), "p90": round(pct(90), 4)},
        "expected_move_pct": round(float(np.median(ret) * 100), 3),
        "prob_up": round(float((final > last).mean()), 3),
        "band_80_pct": [round(float(np.percentile(ret, 10) * 100), 3), round(float(np.percentile(ret, 90) * 100), 3)],
        "path_high_p90": round(float(np.percentile(paths[:, :, 1].max(1), 90)), 4),
        "path_low_p10": round(float(np.percentile(paths[:, :, 2].min(1), 10)), 4),
        "mean_path_close": [round(float(v), 4) for v in mean_path],
    }
    return s

def plot(name, hist, paths, y_ts, s, show=60, real=None):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 4.6), dpi=130)
    fig.patch.set_facecolor("#FAF7F2"); ax.set_facecolor("#FAF7F2")
    h = hist.tail(show); ax.plot(h["timestamps"], h["close"], color="#111", lw=1.6, label="history fed to Kronos (close)")
    for i, p in enumerate(paths[:, :, 3]):
        ax.plot(y_ts, p, color="#FF5A1F", alpha=0.18, lw=0.9, label="sampled paths" if i == 0 else None)
    q10, q90 = np.percentile(paths[:, :, 3], 10, 0), np.percentile(paths[:, :, 3], 90, 0)
    ax.fill_between(y_ts, q10, q90, color="#FF5A1F", alpha=0.12, label="10-90% band")
    ax.plot(y_ts, np.median(paths[:, :, 3], 0), color="#FF5A1F", lw=2.2, label="median path")
    if real is not None: ax.plot(real["timestamps"], real["close"], color="#2563eb", lw=1.6, ls="--", label="what actually happened")
    ax.axvline(h["timestamps"].iloc[-1], color="#888", lw=0.8, ls=":")
    ax.set_title(f"Kronos forecast - {s['symbol']} {s['timeframe']} ({s['model']}, {s['sampling']['sample_count']} paths, T={s['sampling']['T']})", fontsize=10)
    ax.legend(fontsize=7, loc="upper left"); ax.grid(alpha=0.2)
    for sp in ["top", "right"]: ax.spines[sp].set_visible(False)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, name), facecolor=fig.get_facecolor()); plt.close(fig)

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "mini"
    T, TOP_P, N = 1.0, 0.9, 30
    if which == "mini":
        tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-2k"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-mini"); ctx = 2048; mname = "Kronos-mini"
    else:
        tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-small"); ctx = 512; mname = "Kronos-small"
    pred = KronosPredictor(mdl, tok, device="cpu", max_context=ctx)
    results = {}
    # 1) BTC-USD 1h live forecast (24h ahead), 360-bar context like the official demo
    btc = coinbase_1h(bars=400)
    hist = btc.tail(360).reset_index(drop=True)
    y_ts = pd.Series(pd.date_range(hist["timestamps"].iloc[-1] + pd.Timedelta(hours=1), periods=24, freq="h"))
    t0 = time.time(); paths = predict_paths(pred, hist[["open","high","low","close","volume"]], hist["timestamps"], y_ts, 24, T, TOP_P, N); dt = time.time() - t0
    s = summarize("BTC-USD", "Coinbase", "1h", hist, paths, y_ts, mname, T, TOP_P); s["runtime_sec_cpu"] = round(dt, 1)
    results["btc_live"] = s; plot(f"btc_1h_{which}.png", hist, paths, y_ts, s)
    np.save(os.path.join(OUT, f"btc_paths_{which}.npy"), paths)
    # 2) SPY daily live forecast (10 sessions ahead) - business-day timestamps
    spy = yf_daily("SPY", "3y"); hist = spy.tail(400 if ctx > 512 else 400).reset_index(drop=True)
    y_ts = pd.Series(pd.bdate_range(hist["timestamps"].iloc[-1] + pd.Timedelta(days=1), periods=10))
    t0 = time.time(); paths = predict_paths(pred, hist[["open","high","low","close","volume"]], hist["timestamps"], y_ts, 10, T, TOP_P, N); dt = time.time() - t0
    s = summarize("SPY", "NYSE Arca (data: Yahoo Finance)", "1d", hist, paths, y_ts, mname, T, TOP_P); s["runtime_sec_cpu"] = round(dt, 1)
    results["spy_live"] = s; plot(f"spy_1d_{which}.png", hist, paths, y_ts, s)
    # 3) Hold-out sanity check on BTC: forecast from 24h ago, compare with what happened (ONE example, not a benchmark)
    hist = btc.iloc[-(360 + 24):-24].reset_index(drop=True); real = btc.tail(24).reset_index(drop=True)
    y_ts = real["timestamps"]
    paths = predict_paths(pred, hist[["open","high","low","close","volume"]], hist["timestamps"], y_ts, 24, T, TOP_P, N)
    s = summarize("BTC-USD", "Coinbase", "1h", hist, paths, y_ts, mname, T, TOP_P)
    rc = float(real["close"].iloc[-1]); s["realized_close"] = rc
    s["realized_inside_80_band"] = bool(s["forecast_close"]["p10"] <= rc <= s["forecast_close"]["p90"])
    s["realized_direction_up"] = bool(rc > s["last_close"])
    results["btc_holdout"] = s; plot(f"btc_holdout_{which}.png", hist, paths, y_ts, s, real=real)
    json.dump(results, open(os.path.join(OUT, f"results_{which}.json"), "w"), indent=1, default=str)
    for k, v in results.items():
        print(k, json.dumps({kk: vv for kk, vv in v.items() if kk != "mean_path_close"}, default=str))
