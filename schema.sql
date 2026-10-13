-- Kronos x402: Postgres schema (Neon). Apply once: psql "$DATABASE_URL" -f schema.sql
-- Timestamps: as_of / horizon_end are the OPEN times of the last input candle / last forecast candle (UTC).

create table if not exists forecasts (
  id                bigint generated always as identity primary key,
  symbol            text        not null,              -- BTC-USD
  venue             text        not null,              -- Coinbase
  timeframe         text        not null,              -- 1h
  model             text        not null,              -- Kronos-small
  as_of             timestamptz not null,
  horizon_bars      smallint    not null,
  horizon_end       timestamptz not null,
  context_bars      smallint    not null,
  sampling          jsonb       not null,              -- {T, top_p, sample_count, seed}
  last_close        double precision not null,
  prob_up           real        not null,
  expected_move_pct real        not null,
  band_80_pct       real[]      not null,              -- [p10, p90] in %
  factlist          jsonb       not null,              -- exact agent-facing response body
  percentiles       jsonb       not null,              -- {times_utc[], p10[], p25[], p50[], p75[], p90[]}
  paths             jsonb       not null,              -- [{closes[], lows[], highs[]}] x sample_count
  runtime_sec       real,
  created_at        timestamptz not null default now(),
  unique (symbol, timeframe, model, as_of)
);
create index if not exists forecasts_latest_idx on forecasts (symbol, timeframe, as_of desc);
create index if not exists forecasts_horizon_idx on forecasts (horizon_end);

create table if not exists outcomes (
  forecast_id        bigint primary key references forecasts(id) on delete cascade,
  realized_close     double precision not null,
  realized_move_pct  real    not null,
  direction_hit      boolean not null,
  inside_band_80     boolean not null,
  checked_at         timestamptz not null default now()
);

-- Privacy-safe request log for /stats: no IPs, no full user agents, no wallet addresses.
create table if not exists requests (
  id          bigint generated always as identity primary key,
  ts          timestamptz not null default now(),
  path        text        not null,
  method      text        not null default 'GET',
  status      smallint,
  ua_class    text        not null check (ua_class in ('ai_agent','ai_crawler','search_crawler','x402_client','human','other')),
  ua_name     text,
  paid        boolean     not null default false,
  amount_usd  numeric(10,4),
  network     text
);
create index if not exists requests_ts_idx on requests (ts desc);

create or replace view latest_forecasts as
  select distinct on (symbol, timeframe) id, symbol, venue, timeframe, model, as_of, horizon_end, factlist, percentiles, created_at
  from forecasts order by symbol, timeframe, as_of desc;

create or replace view stats_daily as
  select date_trunc('day', ts) as day,
         count(*) as hits,
         count(*) filter (where ua_class in ('ai_agent','ai_crawler','x402_client')) as agent_hits,
         count(*) filter (where ua_class = 'human') as human_hits,
         count(*) filter (where paid) as paid_calls,
         coalesce(sum(amount_usd) filter (where paid), 0) as revenue_usd
  from requests group by 1 order by 1;

create or replace view track_record as
  select f.symbol, f.timeframe, f.model, count(*) as scored,
         avg(o.direction_hit::int)  as hit_rate,
         avg(o.inside_band_80::int) as band_80_coverage
  from forecasts f join outcomes o on o.forecast_id = f.id
  group by 1,2,3;

-- Worker health (SPEC_v3 §8.3)
create table if not exists worker_runs (
  id            bigint generated always as identity primary key,
  started_at    timestamptz not null default now(),
  finished_at   timestamptz,
  ok            boolean,
  symbols_done  text[],
  error         text
);
create index if not exists worker_runs_started_idx on worker_runs (started_at desc);

-- v6: Polymarket public market data (Gamma API, no key). Additive only.
create table if not exists prediction_markets (
  source        text        not null default 'polymarket',
  market_id     text        not null,                 -- Gamma market id
  slug          text        not null,
  event_slug    text,
  question      text        not null,
  category      text        not null,                 -- crypto_price | crypto_touch | macro
  asset_symbol  text,                                 -- BTC | ETH | SOL | null (macro)
  strike        double precision,                     -- price level parsed from the market, if any
  comparator    text,                                 -- above | below | touch_above | touch_below | null
  resolve_time  timestamptz,
  resolution_source text,                             -- e.g. Binance BTC/USDT 1m close at 12:00 ET
  url           text        not null,
  yes_price     double precision,                     -- latest Yes price (0..1) from Gamma outcomePrices
  volume        double precision,
  liquidity     double precision,
  active        boolean     not null default true,
  updated_at    timestamptz not null default now(),
  primary key (source, market_id)
);
create index if not exists prediction_markets_asset_idx on prediction_markets (asset_symbol, resolve_time);
create index if not exists prediction_markets_cat_idx on prediction_markets (category, resolve_time);

create table if not exists pm_snapshots (
  source     text        not null default 'polymarket',
  market_id  text        not null,
  ts         timestamptz not null,                    -- ingestion hour (UTC, floored)
  yes_price  double precision,
  no_price   double precision,
  volume     double precision,
  liquidity  double precision,
  primary key (source, market_id, ts)
);
create index if not exists pm_snapshots_market_ts_idx on pm_snapshots (market_id, ts);

-- v7: US stock/ETF bars from Alpaca market data (SIP, adjustment=all). Stored for model input only; not served. Additive only.
create table if not exists stock_bars (
  symbol text not null, timeframe text not null, ts timestamptz not null,
  o double precision not null, h double precision not null, l double precision not null, c double precision not null,
  v double precision not null, vwap double precision, trade_count bigint,
  source text not null default 'alpaca_sip', fetched_at timestamptz not null default now(),
  primary key (symbol, timeframe, ts));
