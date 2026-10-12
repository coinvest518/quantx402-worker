"""Polymarket ingestion (public Gamma API, no key). Runs hourly after the Kronos forecast; never fails the job.
Upserts `prediction_markets` and writes one `pm_snapshots` row per market per hour.
  - crypto_price: daily "Bitcoin/Ethereum/Solana above ___ on <date>?" events (strike + 'above', resolve time), next 7 days
  - crypto_touch: "What price will <asset> hit ...?" events (strike + touch_above/touch_below)
  - macro: nearest Fed decision events and US CPI (inflation) events
Env: DATABASE_URL, DRY_RUN=1, PM_DAYS (default 7), PM_BUDGET_SEC (default 50)."""
import json, os, re, sys, time, traceback
from datetime import datetime, timezone, timedelta
import requests

E = os.environ.get
GAMMA = "https://gamma-api.polymarket.com"
ASSETS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}
DAYS, BUDGET = int(E("PM_DAYS", 7)), float(E("PM_BUDGET_SEC", 50))
UA = {"User-Agent": "quantx402-worker (+https://quantx402.app)"}
T0 = time.time()

def left(): return BUDGET - (time.time() - T0)

def get(path, params):
    if left() < 3: raise TimeoutError("polymarket time budget used up")
    r = requests.get(GAMMA + path, params=params, headers=UA, timeout=min(15, max(3, left())))
    r.raise_for_status(); return r.json()

def jl(v):
    if isinstance(v, list): return v
    try: return json.loads(v or "[]")
    except Exception: return []

def num(s):
    s = s.replace(",", "").strip().lower(); mult = 1
    if s.endswith("k"): mult, s = 1_000, s[:-1]
    elif s.endswith("m"): mult, s = 1_000_000, s[:-1]
    try: return float(s) * mult
    except ValueError: return None

def strike_of(m):
    t = m.get("groupItemTitle") or ""
    v = num(re.sub(r"[^\d.,km]", "", t.lower())) if re.search(r"\d", t) else None
    if v is None:
        x = re.search(r"\$\s?([\d,]+(?:\.\d+)?\s?[km]?)", m.get("question") or "", re.I)
        v = num(x.group(1)) if x else None
    return v

def row(m, ev, category, asset, comparator, strike):
    outs, prices = jl(m.get("outcomes")), [float(p) for p in jl(m.get("outcomePrices")) if p not in (None, "")]
    yes = no = None
    if len(outs) == 2 and len(prices) == 2:
        i = 0 if str(outs[0]).lower() == "yes" else 1 if str(outs[1]).lower() == "yes" else 0
        yes, no = prices[i], prices[1 - i]
    desc = m.get("description") or ev.get("description") or ""
    rs = None
    x = re.search(r"Binance\s+1 minute candle for ([A-Z]+/[A-Z]+) closing at ([\d:]+) in the ET timezone", desc)
    if x: rs = f"Binance {x.group(1)} 1m close at {x.group(2)} ET"
    return {
        "market_id": str(m["id"]), "slug": m.get("slug") or "", "event_slug": ev.get("slug"),
        "question": (m.get("question") or ev.get("title") or "")[:500], "category": category, "asset_symbol": asset,
        "strike": strike, "comparator": comparator, "resolve_time": m.get("endDate") or ev.get("endDate"),
        "resolution_source": rs, "url": f"https://polymarket.com/event/{ev.get('slug')}",
        "yes_price": yes, "no_price": no,
        "volume": float(m.get("volumeNum") or m.get("volume") or 0), "liquidity": float(m.get("liquidityNum") or m.get("liquidity") or 0),
        "active": bool(m.get("active")) and not m.get("closed"),
    }

def open_markets(ev):
    return [m for m in ev.get("markets") or [] if m.get("active") and not m.get("closed") and m.get("outcomePrices")]

def crypto_daily():
    today = datetime.now(timezone.utc).date(); slugs = {}
    for sym, name in ASSETS.items():
        for d in range(DAYS + 1):
            day = today + timedelta(days=d)
            slugs[f"{name}-above-on-{day.strftime('%B').lower()}-{day.day}-{day.year}"] = sym
    evs = get("/events", [("slug", s) for s in slugs])
    out = []
    for ev in evs:
        sym = slugs.get(ev.get("slug"))
        if not sym or ev.get("closed"): continue
        for m in open_markets(ev):
            k = strike_of(m)
            if k: out.append(row(m, ev, "crypto_price", sym, "above", k))
    return out

def crypto_touch():
    out, now = [], datetime.now(timezone.utc)
    for sym, name in ASSETS.items():
        res = get("/public-search", {"q": f"what price will {name} hit", "events_status": "active", "limit_per_type": 10})
        for ev in res.get("events") or []:
            if not str(ev.get("slug", "")).startswith(f"what-price-will-{name}-hit") or ev.get("closed"): continue
            end = ev.get("endDate")
            if end and datetime.fromisoformat(end.replace("Z", "+00:00")) > now + timedelta(days=40): continue  # skip year-long ladders
            for m in open_markets(ev):
                k = strike_of(m); q = (m.get("question") or "").lower()
                comp = "touch_below" if re.search(r"\b(dip|fall|drop|below|low)\b", q) else "touch_above"
                if k: out.append(row(m, ev, "crypto_touch", sym, comp, k))
    return out

def macro():
    out, now = [], datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    picks = [("fed-rates", lambda s: s.startswith("fed-decision-in-"), 2),
             ("cpi", lambda s: re.search(r"inflation-us-(annual|monthly)", s) is not None, 4)]
    for tag, ok, n in picks:
        evs = get("/events", {"tag_slug": tag, "active": "true", "closed": "false", "end_date_min": now, "limit": 100, "order": "endDate", "ascending": "true"})
        for ev in [e for e in evs if ok(e.get("slug", ""))][:n]:
            for m in open_markets(ev):
                out.append(row(m, ev, "macro", None, None, None))
    return out

UPSERT = """insert into prediction_markets (source, market_id, slug, event_slug, question, category, asset_symbol, strike, comparator,
  resolve_time, resolution_source, url, yes_price, volume, liquidity, active, updated_at)
  values ('polymarket', %(market_id)s, %(slug)s, %(event_slug)s, %(question)s, %(category)s, %(asset_symbol)s, %(strike)s, %(comparator)s,
  %(resolve_time)s, %(resolution_source)s, %(url)s, %(yes_price)s, %(volume)s, %(liquidity)s, %(active)s, now())
  on conflict (source, market_id) do update set slug=excluded.slug, event_slug=excluded.event_slug, question=excluded.question,
  category=excluded.category, asset_symbol=excluded.asset_symbol, strike=excluded.strike, comparator=excluded.comparator,
  resolve_time=excluded.resolve_time, resolution_source=excluded.resolution_source, url=excluded.url, yes_price=excluded.yes_price,
  volume=excluded.volume, liquidity=excluded.liquidity, active=excluded.active, updated_at=now()"""
SNAP = """insert into pm_snapshots (source, market_id, ts, yes_price, no_price, volume, liquidity)
  values ('polymarket', %(market_id)s, date_trunc('hour', now()), %(yes_price)s, %(no_price)s, %(volume)s, %(liquidity)s)
  on conflict (source, market_id, ts) do update set yes_price=excluded.yes_price, no_price=excluded.no_price, volume=excluded.volume, liquidity=excluded.liquidity"""

def run(conn=None):
    """Returns (n_markets, errors). Never raises."""
    rows, errors = [], []
    for name, fn in (("crypto_daily", crypto_daily), ("macro", macro), ("crypto_touch", crypto_touch)):
        try: got = fn(); rows += got; print(f"polymarket {name}: {len(got)} markets")
        except Exception as e: errors.append(f"polymarket {name}: {e}"); print(errors[-1], file=sys.stderr)
    seen, uniq = set(), []
    for r in rows:
        if r["market_id"] not in seen: seen.add(r["market_id"]); uniq.append(r)
    if conn is None:
        for r in uniq[:8]: print(json.dumps(r))
        return len(uniq), errors
    try:
        with conn.cursor() as cur:
            cur.executemany(UPSERT, uniq)
            cur.executemany(SNAP, uniq)
            # markets that resolved or vanished: mark inactive; keep 120 days of snapshots
            cur.execute("update prediction_markets set active = false where source='polymarket' and active and (resolve_time < now() or updated_at < now() - interval '3 hours')")
            cur.execute("delete from pm_snapshots where ts < now() - interval '120 days'")
    except Exception as e:
        errors.append(f"polymarket db: {e}"); traceback.print_exc()
    print(f"polymarket: {len(uniq)} markets upserted in {time.time() - T0:.1f}s, errors={len(errors)}")
    return len(uniq), errors

if __name__ == "__main__":
    db = E("DATABASE_URL", "")
    if E("DRY_RUN") == "1" or not db:
        run(None); sys.exit(0)
    import psycopg
    try:
        with psycopg.connect(db, autocommit=True, connect_timeout=15) as c: run(c)
    except Exception as e:
        print(f"polymarket: db connect failed: {e}", file=sys.stderr)
    sys.exit(0)  # never break the workflow
