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
