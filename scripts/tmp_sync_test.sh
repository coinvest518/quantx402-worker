# TEMPORARY (removed with tmp-sync-test.yml). Never prints connection strings.
set -uo pipefail
export PATH=/usr/lib/postgresql/18/bin:$PATH
S=scripts/sync_hot.py
BR=$(python3 - <<'P'
import os, re
from urllib.parse import urlsplit, urlunsplit
u = urlsplit(os.environ["CORE"]); ep = os.environ["EP"]
assert re.fullmatch(r"ep-[a-z0-9-]+", ep)
host = re.sub(r"^ep-[a-z]+-[a-z]+-[a-z0-9]+", ep, u.hostname)
assert host != u.hostname, "branch host must differ from production"
print(urlunsplit(u._replace(netloc=u.netloc.replace(u.hostname, host))))
P
)
echo "::add-mask::$BR"
python3 - <<'P'
import os
from urllib.parse import urlsplit
h = urlsplit(os.environ["HOT"]); print("always-on host suffix:", h.hostname.split(".", 1)[1], "port:", h.port, "(transaction pooler)" if h.port == 6543 else "(NOT pooler)")
P
hq(){ psql "$HOT" -qAtc "$1"; }; bq(){ psql "$BR" -qAtc "$1"; }; cq(){ psql "$CORE" -qAtc "$1"; }
echo "== 0. dry-run rehearsal against the REAL always-on public schema (no writes)"
SRC_URL="$CORE" DST_URL="$HOT" python3 $S forward --upsert --dry-run
DST_URL="$HOT" python3 $S bump --dry-run
echo "== 1. scratch schema sync_test (schema only, from core read)"
T=""; for t in ${SYNC_TABLES//,/ }; do T="$T -t public.$t"; done
pg_dump "$CORE" --schema-only --no-owner --no-privileges $T | sed -e 's/\bpublic\./sync_test./g' -e '/^\\restrict/d;/^\\unrestrict/d' > /tmp/s.sql
hq "drop schema if exists sync_test cascade; create schema sync_test" && psql "$HOT" -q -v ON_ERROR_STOP=1 -f /tmp/s.sql || { echo "schema load failed"; exit 1; }
export DST_SCHEMA=sync_test
echo "== 2. initial copy core -> sync_test (forward into empty schema)"; SRC_URL="$CORE" DST_URL="$HOT" python3 $S forward
echo "== 3. simulate missed in-flight rows: delete newest 5 requests, 3 worker_runs, 2 free_trials; make one market stale"
hq "delete from sync_test.requests where id in (select id from sync_test.requests order by id desc limit 5);
    delete from sync_test.worker_runs where id in (select id from sync_test.worker_runs order by id desc limit 3);
    delete from sync_test.free_trials where caller_key in (select caller_key from sync_test.free_trials order by used_at desc nulls last limit 2);
    update sync_test.prediction_markets set yes_price = 0.0101, updated_at = updated_at - interval '1 day' where (source, market_id) = (select source, market_id from sync_test.prediction_markets order by market_id limit 1)"
echo "== 4. forward --upsert (pre-deploy catch-up; expect requests 5, worker_runs>=3, free_trials 2, prediction_markets>=1)"
SRC_URL="$CORE" DST_URL="$HOT" python3 $S forward --upsert | grep -E "inserted|upserted|FAILED|TOTAL"
echo "== 5. forward rerun (expect TOTAL 0 apart from rows core wrote meanwhile)"; SRC_URL="$CORE" DST_URL="$HOT" python3 $S forward | grep -E "inserted [1-9]|FAILED|TOTAL"
echo "== 6. bump sync_test"; DST_URL="$HOT" python3 $S bump
echo "== 7. always-on writes after bump"
hq "insert into sync_test.requests (path,method,status,paid,ua_class) values ('/t/gha','GET',200,false,'other'),('/t/gha','GET',200,false,'other');
    insert into sync_test.worker_runs default values;
    insert into sync_test.free_trials (caller_key,key_type,first_endpoint) values ('test:gha','ip_ua','/api/v1/basic');
    update sync_test.prediction_markets set yes_price = 0.6543, updated_at = now() where (source, market_id) = (select source, market_id from sync_test.prediction_markets order by market_id limit 1)"
echo "new ids: $(hq "select string_agg(id::text, ',') from sync_test.requests where path='/t/gha'")"
echo "== 8. reverse --upsert sync_test -> Neon test branch"
SRC_SCHEMA=sync_test DST_SCHEMA=public SRC_URL="$HOT" DST_URL="$BR" python3 $S reverse --upsert | grep -E "inserted|upserted|FAILED|TOTAL"
echo "== 9. reverse rerun (expect 0)"; SRC_SCHEMA=sync_test DST_SCHEMA=public SRC_URL="$HOT" DST_URL="$BR" python3 $S reverse --upsert | grep -E "inserted [1-9]|upserted [1-9]|FAILED|TOTAL"
echo "== 10. bump Neon test branch (TABLES only)"; DST_SCHEMA=public DST_URL="$BR" python3 $S bump
echo "== 11. checks"
echo "branch requests ids>=1e9: $(bq "select count(*) from requests where id >= 1000000000") (expect 2)"
echo "branch market price: $(bq "select yes_price from prediction_markets order by market_id limit 1") (expect 0.6543)"
echo "branch forecasts id seq last_value < 1e9: $(bq "select last_value < 1000000000 from forecasts_id_seq")"
echo "core next forecasts id untouched: $(cq "select last_value < 1000000000 from forecasts_id_seq")"
for t in ${SYNC_TABLES//,/ }; do echo "$t core=$(cq "select count(*) from $t") sync_test=$(hq "select count(*) from sync_test.$t")"; done
echo "real public tables on always-on DB: $(hq "select count(*) from information_schema.tables where table_schema='public'")"
