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

-- Ticker banner cache (SPEC_v4 item 1). One row per price source; latest snapshot shared by all visitors.
-- lease_until lets exactly one instance refresh per 60s window.
create table if not exists market_cache (
  key          text primary key,                 -- e.g. 'ticker:coinbase', 'ticker:coingecko'
  source       text        not null,
  snapshot     jsonb,                            -- {quotes:[{symbol,name,price_usd,change_24h_pct}]}
  fetched_at   timestamptz not null default 'epoch',
  lease_until  timestamptz
);

-- CoinGecko monthly call counter (Demo plan quota 10,000/mo; we hard-stop at 8,000).
create table if not exists coingecko_usage (
  month   text primary key,                        -- 'YYYY-MM' (UTC); CoinGecko resets on the 1st
  calls   integer not null default 0,
  updated_at timestamptz not null default now()
);
-- SPEC_v4 §4: count failed upstream calls too (CoinGecko bills/limits errors as well).
alter table coingecko_usage add column if not exists failed integer not null default 0;

-- Skill download leads (/skills email gate). Token is stored only as a SHA-256 hash.
create table if not exists skill_leads (
  id           bigint generated always as identity primary key,
  email        text        not null,                -- lowercased
  consent      boolean     not null,                -- "Send me QuantX402 updates"
  created_at   timestamptz not null default now(),
  token_hash   text        not null unique,         -- sha256(hex) of the signed download token
  expires_at   timestamptz not null,
  used_at      timestamptz,
  ip_hash      text                                 -- sha256(ip + secret), only for rate limiting; never the raw IP
);
create index if not exists skill_leads_email_idx on skill_leads (email, created_at desc);
create index if not exists skill_leads_ip_idx on skill_leads (ip_hash, created_at desc);

-- v5: x402 payment activity (tx hash + payer are public on-chain data; still no IPs).
alter table requests add column if not exists pay_status text;   -- payment_required | settled | rejected | settle_failed | not_charged
alter table requests add column if not exists payer text;        -- wallet that signed the payment (public on-chain)
alter table requests add column if not exists tx_hash text;      -- settlement transaction
alter table requests add column if not exists product text;      -- kronos-forecast | signal-blend
alter table requests add column if not exists pay_error text;
create index if not exists requests_pay_idx on requests (ts desc) where pay_status is not null;

-- v5: where each lead came from.
alter table skill_leads add column if not exists product text not null default 'kronos-skill';
alter table skill_leads add column if not exists source text;          -- ?ref= tag
alter table skill_leads add column if not exists utm_source text;
alter table skill_leads add column if not exists utm_medium text;
alter table skill_leads add column if not exists utm_campaign text;
alter table skill_leads add column if not exists referrer_host text;

-- v5: every download redemption attempt (ok or refused).
create table if not exists downloads (
  id        bigint generated always as identity primary key,
  ts        timestamptz not null default now(),
  lead_id   bigint references skill_leads(id) on delete set null,
  product   text not null,
  ok        boolean not null,
  reason    text                                   -- ok | expired | used | invalid | unavailable
);
create index if not exists downloads_ts_idx on downloads (ts desc);

-- v5: outbound product clicks through /go/<slug> (Gumroad etc.). No IPs.
create table if not exists link_clicks (
  id            bigint generated always as identity primary key,
  ts            timestamptz not null default now(),
  slug          text not null,
  dest_host     text,
  source        text,                              -- ?ref= tag (e.g. nav, landing-hero, guide-page)
  utm_source    text, utm_medium text, utm_campaign text,
  referrer_host text,
  ua_class      text
);
create index if not exists link_clicks_ts_idx on link_clicks (ts desc);

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

-- v7: one lifetime free call per caller, only on /api/v1/basic. caller_key = wallet (X-Wallet / ?wallet=) or salted hash of IP+UA.
-- ip_hash (salted hash of the IP alone) is unique too, so one IP gets one trial whatever wallet it sends (anti-abuse).
create table if not exists free_trials (
  caller_key     text primary key,
  key_type       text not null,                  -- wallet_evm | wallet_svm | ip_ua
  first_endpoint text not null,
  used_at        timestamptz not null default now(),
  ip_hash        text,
  ua             text
);
create unique index if not exists free_trials_ip_hash_uq on free_trials (ip_hash) where ip_hash is not null;

-- v7: /api/chat per-network-address hourly rate limit (salted hash only). New table; created lazily by lib/chat.ts too.
create table if not exists chat_rate_limits (
  ip_hash text not null, window_start timestamptz not null, count int not null default 0, primary key (ip_hash, window_start));

-- v8: paid self-serve listings, auto-approved by automated checks.
create table if not exists sponsor_submissions (
  id           bigint generated always as identity primary key,
  created_at   timestamptz not null default now(),
  status       text not null default 'pending' check (status in ('pending','approved','rejected','expired')),
  slot         text not null check (slot in ('featured','store_card','docs_banner')),
  name         text not null check (char_length(name) between 1 and 60),
  headline     text not null check (char_length(headline) between 1 and 90),
  body         text not null check (char_length(body) between 1 and 280),
  url          text not null check (url like 'https://%' and char_length(url) <= 200),
  email        text not null check (char_length(email) <= 254),
  days         int  not null check (days between 1 and 30),
  payer        text,                 -- x402 payer wallet (public on-chain)
  tx_hash      text,                 -- settle tx; required before status can leave 'pending'
  amount_usd   numeric(10,4),
  ip_hash      text,                 -- salted hash (IPv6 /64), rate limiting only
  pay_to       text not null,        -- payTo of the listed x402 service (checks: catalog listing, distinct buyers)
  resource_url text,
  checks       jsonb not null default '[]',  -- [{name,status,detail}] from the automated verification
  reviewed_by  text, reviewed_at timestamptz, reject_reason text,   -- reviewed_by='auto' (no human in the loop)
  starts_at    timestamptz, ends_at timestamptz,
  check (status <> 'approved' or (payer is not null and starts_at is not null and ends_at is not null and ends_at <= starts_at + interval '30 days'))
);
create index if not exists sponsor_submissions_live_idx on sponsor_submissions (slot, starts_at, ends_at) where status = 'approved';

-- v9: A2A task store (JSON-RPC /a2a) so tasks/get works across serverless instances; rows read back for 24h.
create table if not exists a2a_tasks (
  id         text primary key,
  task       jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists a2a_tasks_created_idx on a2a_tasks (created_at);

-- v10: /a2a per-network-address rate limit (salted hash only), 10-minute windows.
create table if not exists a2a_rate_limits (
  ip_hash text not null, window_start timestamptz not null, count int not null default 0, primary key (ip_hash, window_start));

-- v11 (builder/backend-overhaul): additive only. Nothing above is altered or dropped.
-- Stock/ETF forecasts from our own bootstrap model over stored candles (stock_bars), plus their scored outcomes.
create table if not exists stock_forecasts (
  id                bigint generated always as identity primary key,
  symbol            text not null,
  timeframe         text not null,                    -- 1h | 1d
  model             text not null,                    -- qx-bootstrap-v1
  as_of             timestamptz not null,             -- OPEN time of the last input candle
  horizon_bars      smallint not null,
  horizon_end       timestamptz,                      -- OPEN time of the bar that settles the forecast (filled when scored)
  last_close        double precision not null,
  prob_up           real not null,
  expected_move_pct real not null,
  band_80_pct       real[] not null,
  body              jsonb not null,                   -- exact response body
  created_at        timestamptz not null default now(),
  unique (symbol, timeframe, model, as_of)
);
create index if not exists stock_forecasts_latest_idx on stock_forecasts (symbol, timeframe, as_of desc);
create table if not exists stock_outcomes (
  forecast_id       bigint primary key references stock_forecasts(id) on delete cascade,
  realized_close    double precision not null,
  realized_move_pct real not null,
  direction_hit     boolean not null,
  inside_band_80    boolean not null,
  checked_at        timestamptz not null default now()
);
-- Calibration snapshots written by /api/cron/score-outcomes (reliability of prob_up buckets, Brier score, band coverage).
create table if not exists calibration_snapshots (
  id               bigint generated always as identity primary key,
  computed_at      timestamptz not null default now(),
  scope            text not null check (scope in ('crypto','stock')),
  symbol           text not null,
  timeframe        text not null,
  model            text not null,
  n                int  not null,
  hit_rate         real,
  brier            real,
  band_80_coverage real,
  band_scale_80    real,                             -- multiply the raw 80% band by this to reach 80% coverage (null if n < 30)
  reliability      jsonb not null default '[]'       -- [{bucket:[lo,hi], n, mean_prob_up, frac_up}]
);
create index if not exists calibration_snapshots_idx on calibration_snapshots (scope, symbol, timeframe, computed_at desc);
-- Push notifications: webhook subscribers get a signed "new forecast available" ping (no forecast content; fetch is paid).
create table if not exists webhook_subscriptions (
  id            text primary key,
  created_at    timestamptz not null default now(),
  url           text not null check (url like 'https://%' and char_length(url) <= 300),
  secret        text not null,                        -- HMAC-SHA256 key for X-QX-Signature
  symbols       text[] not null,
  timeframes    text[] not null,
  active        boolean not null default true,
  failures      int not null default 0,               -- consecutive; disabled at 10
  last_forecast_id bigint,
  ip_hash       text
);
create index if not exists webhook_subscriptions_active_idx on webhook_subscriptions (active);
create table if not exists webhook_deliveries (
  id           bigint generated always as identity primary key,
  ts           timestamptz not null default now(),
  sub_id       text not null references webhook_subscriptions(id) on delete cascade,
  forecast_id  bigint not null,
  status       smallint,
  ok           boolean not null,
  unique (sub_id, forecast_id)
);
-- News items for /api/v1/sentiment, filled by our ingestion pipeline. Empty until a licensed feed is connected.
create table if not exists news_items (
  id           bigint generated always as identity primary key,
  published_at timestamptz not null,
  symbols      text[] not null,
  headline     text not null,
  summary      text,
  score        real,                                  -- -1..1 from our lexicon scorer; null = not yet scored
  ingested_at  timestamptz not null default now(),
  dedupe_key   text unique
);
create index if not exists news_items_pub_idx on news_items (published_at desc);

-- v12: /api/v1/stream connection leases (per-network + global concurrency caps). Created lazily by the route too.
create table if not exists stream_leases (id text primary key, ip_hash text not null, expires_at timestamptz not null);

-- v12 (builder/multi-asset): additive only. On-demand forecasts, request log, auto-promoted symbols.
create table if not exists forecast_jobs (
  id           text primary key,                      -- odj_<random>; unguessable, doubles as the read token for the status URL
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  symbol       text not null,
  asset_class  text not null check (asset_class in ('crypto','equity')),
  timeframe    text not null,
  status       text not null default 'queued' check (status in ('queued','running','done','failed')),
  forecast_id  bigint references forecasts(id) on delete set null,
  error        text,                                  -- generic message only
  webhook_url  text check (webhook_url is null or (webhook_url like 'https://%' and char_length(webhook_url) <= 300)),
  webhook_secret text,
  webhook_status smallint,
  payer        text,
  tx_hash      text
);
create index if not exists forecast_jobs_status_idx on forecast_jobs (status, created_at);
create table if not exists symbol_requests (
  id          bigint generated always as identity primary key,
  ts          timestamptz not null default now(),
  symbol      text not null,
  asset_class text,
  timeframe   text,
  endpoint    text not null,                          -- on-demand | forecast | stocks
  outcome     text not null,                          -- served_cached | queued | no_candles | invalid | tracked_redirect
  caller_hash text                                    -- salted network hash, never the raw IP
);
create index if not exists symbol_requests_sym_idx on symbol_requests (symbol, ts desc);
create table if not exists tracked_symbols (
  symbol       text primary key,
  asset_class  text not null check (asset_class in ('crypto','equity')),
  origin       text not null check (origin in ('config','auto')),
  active       boolean not null default true,
  promoted_at  timestamptz not null default now(),
  requests_7d  int,
  note         text
);

-- v13 (builder/research-brief): /api/v1/brief (SPEC_research_brief.md §7). Additive only.
-- (calibration_snapshots scope 'brief' deferred until the weekly brief snapshot job exists; no constraint changes here)
create table if not exists briefs (
  id                  bigint generated always as identity primary key,
  created_at          timestamptz not null default now(),
  symbol              text not null,
  methodology_version text not null,
  schema_version      text not null,
  forecast_id         bigint references forecasts(id),
  forecast_as_of      timestamptz not null,
  horizon_end         timestamptz not null,           -- forecast_as_of + 1h + 24h (bar that settles)
  last_close          double precision not null,      -- internal only, never served
  prob_up_raw         real not null,                  -- internal only, for Brier
  direction           text not null check (direction in ('UP','DOWN','NEUTRAL')),
  confidence_bucket   text not null check (confidence_bucket in ('none','low','med')),
  band80_lo_pct       real not null,
  band80_hi_pct       real not null,
  news_tone           real,
  news_count          int not null default 0,
  news_conflict       boolean,
  var95_1d_pct        real,
  var99_1d_pct        real,
  es95_1d_pct         real,
  vol_ann_d30_pct     real,
  corr_btc_30d        real,
  corr_spy_30d        real,
  rationale_source    text not null,
  flags               text[] not null default '{}',
  body                jsonb not null,                 -- exact served body
  rationale_inputs    jsonb,                          -- internal: the exact JSON the rationale model may see
  unique (symbol, forecast_id, methodology_version, created_at)
);
create index if not exists briefs_symbol_time_idx on briefs (symbol, created_at desc);
create table if not exists brief_outcomes (
  brief_id           bigint primary key references briefs(id) on delete cascade,
  realized_close     double precision not null,
  realized_move_pct  real not null,
  realized_up        boolean not null,
  direction_hit      boolean,
  inside_band_80     boolean not null,
  var95_breach       boolean,
  var99_breach       boolean,
  checked_at         timestamptz not null default now()
);
-- Rationale text written by the worker's self-hosted model job; re-validated by the site at serve time.
create table if not exists brief_rationales (
  id           bigint generated always as identity primary key,
  created_at   timestamptz not null default now(),
  brief_id     bigint references briefs(id) on delete cascade,
  symbol       text not null,
  forecast_id  bigint not null,
  text         text not null check (char_length(text) <= 420),
  model        text not null,                         -- internal model id, never served
  latency_ms   int
);
create index if not exists brief_rationales_idx on brief_rationales (symbol, forecast_id, created_at desc);
