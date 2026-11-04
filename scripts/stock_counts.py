"""Row counts for Kronos stock forecasts. Always-on DB always; core DB only inside the :10-:14 window (CORE=1)."""
import os, sys
from datetime import datetime, timezone
import psycopg
STOCK = "venue = 'stock'"
def q(c, sql):
    try: return c.execute(sql).fetchall()
    except Exception as e: return f"n/a ({type(e).__name__})"
h = psycopg.connect(os.environ["SUPABASE_DATABASE_URL"], autocommit=True, prepare_threshold=None, connect_timeout=15)
print("always-on forecasts (stock) by symbol/tf/synced:", q(h, f"select symbol, timeframe, synced, count(*), max(as_of) from forecasts where {STOCK} group by 1,2,3 order by 1,2,3"))
print("always-on stock_bars by tf:", q(h, "select timeframe, count(*), max(ts) from stock_bars group by 1 order by 1"))
print("always-on forecast_marks (stock syms):", q(h, "select count(*) from forecast_marks where symbol !~ '-USD$'"))
print("always-on stock_forecasts (baseline):", q(h, "select count(*) from stock_forecasts"))
print("always-on stock_outcomes (baseline):", q(h, "select count(*) from stock_outcomes"))
h.close()
m = datetime.now(timezone.utc).minute
if os.environ.get("CORE") == "1":
    if not 10 <= m <= 14: print(f"core counts skipped: minute {m} is outside :10-:14 (core DB left asleep)"); sys.exit(0)
    c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, connect_timeout=15)
    print("core forecasts (stock):", q(c, f"select timeframe, count(*), max(as_of) from forecasts where {STOCK} group by 1 order by 1"))
    print("core outcomes (stock):", q(c, f"select f.timeframe, count(*) from outcomes o join forecasts f on f.id = o.forecast_id where f.{STOCK} group by 1 order by 1"))
    print("core stock_forecasts / stock_outcomes (baseline):", q(c, "select (select count(*) from stock_forecasts), (select count(*) from stock_outcomes)"))
    c.close()
