"""Alpaca market-data bars (SIP, split/dividend-adjusted) for US stocks/ETFs into the NEW `stock_bars` table.
Stored for research/model input only; nothing on the site or API serves these prices (Alpaca data may not be redistributed).
Env: DATABASE_URL, ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY, STOCK_SYMBOLS (default SPY,QQQ), STOCK_FEED (default sip).
First run backfills (1d from 2016-01-04, 1h from 2017-01-03); later runs re-fetch from the last stored bar minus 5 days."""
import os, sys, time, json, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timedelta, timezone

E = os.environ.get
SYMBOLS = [s.strip().upper() for s in E("STOCK_SYMBOLS", "SPY,QQQ").split(",") if s.strip()]
FEED = E("STOCK_FEED", "sip")
TFS = [("1d", "1Day", "2016-01-04T00:00:00Z"), ("1h", "1Hour", "2017-01-03T00:00:00Z"), ("15m", "15Min", "2024-01-02T00:00:00Z")]
SRC = "market_feed"  # generic; never store provider names
BASE = "https://data.alpaca.markets/v2/stocks/bars"
HDR = {"APCA-API-KEY-ID": (E("ALPACA_API_KEY_ID") or "").strip(), "APCA-API-SECRET-KEY": (E("ALPACA_API_SECRET_KEY") or "").strip(), "Accept": "application/json"}

DDL = """create table if not exists stock_bars (
  symbol text not null, timeframe text not null, ts timestamptz not null,
  o double precision not null, h double precision not null, l double precision not null, c double precision not null,
  v double precision not null, vwap double precision, trade_count bigint,
  source text not null default 'market_feed', fetched_at timestamptz not null default now(),
  primary key (symbol, timeframe, ts))"""
UPSERT = """insert into stock_bars (symbol, timeframe, ts, o, h, l, c, v, vwap, trade_count, source)
  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
  on conflict (symbol, timeframe, ts) do update set o=excluded.o, h=excluded.h, l=excluded.l, c=excluded.c, v=excluded.v,
    vwap=excluded.vwap, trade_count=excluded.trade_count, source=excluded.source, fetched_at=now()"""

def get(params):
    url = BASE + "?" + urllib.parse.urlencode(params)
    for attempt in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HDR), timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                time.sleep(min(60, 2 ** attempt * 3)); continue
            raise RuntimeError(f"alpaca HTTP {e.code}: {e.read()[:200]!r}")
        except urllib.error.URLError:
            time.sleep(2 ** attempt)
    raise RuntimeError("alpaca: retries exhausted")

def main():
    if not HDR["APCA-API-KEY-ID"] or not HDR["APCA-API-SECRET-KEY"]: sys.exit("ALPACA keys missing")
    import psycopg
    con = psycopg.connect(E("DATABASE_URL"), autocommit=True)
    con.execute(DDL)
    con.execute("alter table stock_bars alter column source set default 'market_feed'")
    con.execute("update stock_bars set source=%s where source <> %s", (SRC, SRC))
    # Basic plan: SIP is available except the most recent 15 minutes.
    end = (datetime.now(timezone.utc) - timedelta(minutes=16)).strftime("%Y-%m-%dT%H:%M:%SZ")
    src = SRC
    for tf, atf, first in TFS:
        for sym in SYMBOLS:
            last = con.execute("select max(ts) from stock_bars where symbol=%s and timeframe=%s", (sym, tf)).fetchone()[0]
            start = (last - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ") if last else first
            params = {"symbols": sym, "timeframe": atf, "start": start, "end": end, "limit": 10000, "adjustment": "all", "feed": FEED, "sort": "asc"}
            n = pages = 0
            while True:
                try:
                    d = get(params)
                except RuntimeError as e:
                    # recent premium-feed window can be restricted; fall back to the free feed for it
                    if params.get("feed") != "iex" and (" 403" in str(e) or " 422" in str(e)):
                        print(f"{sym} {tf}: feed restricted, falling back for this window"); params["feed"] = "iex"; params.pop("page_token", None); continue
                    raise
                pages += 1
                rows = [(sym, tf, b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b.get("vw"), b.get("n"), src) for b in (d.get("bars") or {}).get(sym, [])]
                if rows:
                    with con.cursor() as cur: cur.executemany(UPSERT, rows)
                    n += len(rows)
                tok = d.get("next_page_token")
                if not tok: break
                params["page_token"] = tok
                time.sleep(0.4)  # stay well under 200 req/min
            print(f"{sym} {tf}: {n} bars upserted from {start} ({pages} pages)")
    for r in con.execute("select symbol, timeframe, count(*), min(ts), max(ts) from stock_bars group by 1,2 order by 1,2"):
        print("stock_bars", *r)

if __name__ == "__main__":
    main()
