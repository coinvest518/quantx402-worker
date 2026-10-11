# quantx402-worker

Hourly forecasting job for [QuantX402](https://quantx402.app), powered by Kronos.

Every hour (GitHub Actions, `3 * * * *` UTC) it:
1. fetches the last 360 closed 1h candles for BTC-USD and ETH-USD (Coinbase public API, Binance public mirror as fallback),
2. runs Kronos-small (T=1.0, top_p=0.9, 30 sampled paths, 24 bars ahead),
3. writes the fact list, per-step percentiles and paths to Postgres (`DATABASE_URL` repo secret),
4. scores past forecasts whose horizon has closed (`outcomes`), and records the run in `worker_runs`.

Re-runs are idempotent (`unique(symbol, timeframe, model, as_of)`).

`model/` is the Kronos model code from [shiyu-coder/Kronos](https://github.com/shiyu-coder/Kronos) (MIT). This project is not affiliated with the Kronos authors.

Forecasts are probabilistic model output for research and education. Not financial advice.
