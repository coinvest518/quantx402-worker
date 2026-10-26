#!/usr/bin/env bash
# ONE-OFF: copy the always-on tables from the core DB to the new always-on DB. NOT run automatically.
# Usage (from a machine with pg_dump/psql 16+ that can reach both DBs on 5432):
#   CORE_URL=<core direct url> HOT_URL=<always-on direct/session url, not the 6543 pooler> bash scripts/migrate_hot.sh
# Safe to re-run on an empty target only. Never drops or deletes anything on the core DB.
# After it finishes and the apps run on SUPABASE_DATABASE_URL for a few days, the moved tables can be dropped from core (separate approval).
set -euo pipefail
: "${CORE_URL:?set CORE_URL}"; : "${HOT_URL:?set HOT_URL}"
HOT_TABLES="requests free_trials skill_leads link_clicks downloads prediction_markets pm_snapshots news_items worker_runs data_quality market_cache coingecko_usage chat_rate_limits a2a_rate_limits stream_leases symbol_requests"
T=""; for t in $HOT_TABLES stock_bars; do T="$T -t public.$t"; done
D=""; for t in $HOT_TABLES; do D="$D -t public.$t"; done
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
echo "1/4 schema"; pg_dump "$CORE_URL" --schema-only --no-owner --no-privileges $T > "$tmp/schema.sql"
psql "$HOT_URL" -v ON_ERROR_STOP=1 -q -f "$tmp/schema.sql"
echo "2/4 data (all rows) for small tables"; pg_dump "$CORE_URL" --data-only --no-owner $D > "$tmp/data.sql"
psql "$HOT_URL" -v ON_ERROR_STOP=1 -q -f "$tmp/data.sql"
echo "3/4 stock_bars trimmed: 15m 90 days, 1h 240 days (4h needs ~1600 1h bars), 1d all (1d model needs ~500 sessions)"
psql "$CORE_URL" -v ON_ERROR_STOP=1 -c "\copy (select * from stock_bars where timeframe = '1d' or (timeframe = '15m' and ts > now() - interval '90 days') or (timeframe = '1h' and ts > now() - interval '240 days')) to '$tmp/sb.csv' csv"
psql "$HOT_URL" -v ON_ERROR_STOP=1 -c "\copy stock_bars from '$tmp/sb.csv' csv"
psql "$HOT_URL" -v ON_ERROR_STOP=1 -q -c "create table if not exists tracked_symbols_mirror (symbol text primary key, promoted_at timestamptz)" \
  -c "analyze"
psql "$CORE_URL" -At -c "select symbol, promoted_at from tracked_symbols where active and asset_class = 'crypto' and origin = 'auto'" -F, > "$tmp/ts.csv" || true
[ -s "$tmp/ts.csv" ] && psql "$HOT_URL" -c "\copy tracked_symbols_mirror from '$tmp/ts.csv' csv"
echo "4/4 verify row counts (core vs always-on)"
for t in $HOT_TABLES stock_bars; do
  a=$(psql "$CORE_URL" -At -c "select count(*) from $t" 2>/dev/null || echo -); b=$(psql "$HOT_URL" -At -c "select count(*) from $t" 2>/dev/null || echo -)
  echo "$t core=$a hot=$b"
done
psql "$HOT_URL" -At -c "select 'always-on DB size MB: ' || round(pg_database_size(current_database())/1048576.0)"
