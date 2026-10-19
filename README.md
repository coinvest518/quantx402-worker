# quantx402-worker

Forecasting jobs for [QuantX402](https://quantx402.app), powered by Kronos. Everything runs on GitHub Actions; there is no server.

| Workflow | Schedule (UTC) | What |
|---|---|---|
| `forecast-15m.yml` | `1,16,31,46 * * * *` | 15m forecasts (16 bars ahead) |
| `forecast.yml` | `3 * * * *` | 1h forecasts (24 bars ahead) + prediction-market snapshot |
| `forecast-4h.yml` | `20 */4 * * *` | 4h forecasts (18 bars ahead; 4h bars are built from closed 1h bars) |
| `forecast-1d.yml` | `5 0 * * *` | 1d forecasts (7 bars ahead) |
| `news.yml` | every 20 min | headlines -> `news_items` (symbols, tone score, dedupe key; no source fields stored) |
| `ondemand.yml` | dispatched by the site | one paid on-demand forecast (`forecast_jobs` row) |

Each forecast run: fetches the last 360 closed candles from public exchange feeds (with a backup feed), runs Kronos-small
(T=1.0, top_p=0.9, 30 paths), writes the fact list, percentiles and paths to Postgres (`DATABASE_URL` secret), scores past
forecasts whose horizon has closed (`outcomes`), and records the run in `worker_runs`.

Symbols: `SYMBOLS` (default 10 crypto pairs) plus up to `AUTO_PROMOTE_MAX` pairs auto-promoted from on-demand demand
(>=5 requests from >=2 callers in 7 days, `tracked_symbols`). On-demand stock jobs read our own `stock_bars` table.

Stored and served labels are generic: no upstream feed or publisher names are written to public fields.

Re-runs are idempotent (`unique(symbol, timeframe, model, as_of)`).

`model/` is the Kronos model code from [shiyu-coder/Kronos](https://github.com/shiyu-coder/Kronos) (MIT). This project is not affiliated with the Kronos authors.

Forecasts are probabilistic model output for research and education. Not financial advice.
