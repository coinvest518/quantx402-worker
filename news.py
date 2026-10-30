"""News ingestion -> news_items (GitHub Actions: news.yml, every 30 min). Free public RSS/Atom feeds, no keys, no spend.
Stored rows carry NO source or brand: no URL, no publisher, no summary; headlines that name a data provider or publisher
are dropped and "Headline - Publisher" suffixes are stripped. Tone: the same lexicon as the site (lib/sentiment.ts).
Env: DATABASE_URL, DRY_RUN=1 (print instead of insert), NEWS_MAX_AGE_H (default 72), NEWS_BUDGET_SEC (default 60).
Never fails the job: exits 0 and logs per-feed errors by index only."""
import hashlib, html, json, os, re, sys, time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
import requests

E = os.environ.get
DB = E("SUPABASE_DATABASE_URL", "") or E("DATABASE_URL", "")
DRY = E("DRY_RUN") == "1" or not DB
MAX_AGE = timedelta(hours=float(E("NEWS_MAX_AGE_H", 72)))
BUDGET = float(E("NEWS_BUDGET_SEC", 60))
UA = "Mozilla/5.0 (compatible; QuantX402-news/1.0; +https://quantx402.app)"

def gnews(q):  # public news-search RSS
    return "https://news.google.com/rss/search?" + requests.compat.urlencode({"q": q + " when:1d", "hl": "en-US", "gl": "US", "ceid": "US:en"})

# (url, default symbols, strip " - Publisher" suffix). Internal config only; never stored or served.
FEEDS = [
    ("https://cointelegraph.com/rss", [], False),
    ("https://decrypt.co/feed", [], False),
    (gnews("bitcoin"), ["BTC"], True),
    (gnews("ethereum"), ["ETH"], True),
    (gnews("crypto market"), [], True),
    ("https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY&region=US&lang=en-US", ["SPY"], False),
    ("https://feeds.finance.yahoo.com/rss/2.0/headline?s=QQQ&region=US&lang=en-US", ["QQQ"], False),
    ("https://www.cnbc.com/id/100003114/device/rss/rss.html", [], False),
    (gnews("stock market S&P 500"), ["SPY"], True),
    (gnews("Nasdaq 100 tech stocks"), ["QQQ"], True),
    (gnews("solana OR XRP OR dogecoin OR cardano"), [], True),
    (gnews("avalanche avax OR chainlink OR BNB OR Sui crypto"), [], True),
    (gnews("Nvidia OR Tesla OR Apple stock"), [], True),
    (gnews("Microsoft OR Amazon OR Alphabet OR Meta stock"), [], True),
    (gnews("Dow Jones OR Russell 2000"), [], True),
    # more public crypto / finance outlets (headlines naming the outlet are still dropped)
    ("https://www.coindesk.com/arc/outboundfeeds/rss/", [], False),
    ("https://bitcoinmagazine.com/.rss/full/", ["BTC"], False),
    ("https://cryptoslate.com/feed/", [], False),
    ("https://www.theblock.co/rss.xml", [], False),
    ("https://feeds.content.dowjones.io/public/rss/mw_topstories", [], False),
    ("https://www.investing.com/rss/news_25.rss", [], False),
    ("https://seekingalpha.com/market_currents.xml", [], False),
]
# Public economic-data releases (official statistics / central bank). Tagged MACRO plus broad-market ETFs.
MACRO_FEEDS = [
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.bls.gov/feed/bls_latest.rss",
    "https://apps.bea.gov/rss/rss.xml",
]
MACRO_SYMS = ["MACRO", "SPY", "QQQ", "DIA", "IWM"]
# Public company filings (current-report atom feed per ticker). Turned into our own neutral headline.
FILING_TICKERS = {"AAPL": "Apple", "NVDA": "Nvidia", "TSLA": "Tesla", "MSFT": "Microsoft", "AMZN": "Amazon", "META": "Meta Platforms", "GOOGL": "Alphabet"}
FILING_FORMS = {"8-K": "current report", "10-Q": "quarterly report", "10-K": "annual report", "4": "insider transaction report", "144": "notice of proposed insider share sale", "S-1": "registration statement"}
FILINGS_UA = E("FILINGS_USER_AGENT", "QuantX402 research ops@quantx402.app")  # the filings site asks for a contact UA
def filings_url(t): return "https://www.sec.gov/cgi-bin/browse-edgar?" + requests.compat.urlencode({"action": "getcompany", "CIK": t, "type": "", "dateb": "", "owner": "include", "count": "10", "output": "atom"})
# Social trends: small paid search sample via a hosted scraper (internal config; never stored or served).
# Budget: SOCIAL_EVERY_H=6 -> 4 runs/day x 3 queries x 25 posts = 300 posts/day. Price: $0.005/run start + $0.0004/post
# -> about $0.14/day, $4.34 per 31-day month. Each run is hard-capped at SOCIAL_MAX_USD_PER_RUN ($0.04) -> worst case $4.96/month.
SOCIAL_TOKEN = E("APIFY_TOKEN", "")
SOCIAL_ACTOR = "memo23~x-twitter-search-scraper"
SOCIAL_EVERY_H = int(E("SOCIAL_EVERY_H", "6"))
SOCIAL_PER_QUERY = int(E("SOCIAL_PER_QUERY", "25"))
SOCIAL_MAX_USD_PER_RUN = float(E("SOCIAL_MAX_USD_PER_RUN", "0.04"))
_Q = " lang:en -filter:replies -filter:retweets -filter:links min_faves:20"
SOCIAL_QUERIES = ["(bitcoin OR ethereum OR $BTC OR $ETH)" + _Q,
                  "($SOL OR $XRP OR $DOGE OR $ADA OR $AVAX OR $LINK OR $SUI OR $BNB)" + _Q,
                  "($SPY OR $QQQ OR $NVDA OR $TSLA OR $AAPL OR $MSFT OR $AMZN OR $META OR $GOOGL)" + _Q]
SPAM = re.compile(r"giveaway|airdrop|telegram|presale|pre-sale|drop your|follow (us|me)|retweet|\bRT\b|like \+|\bcalled\b|\b\d{2,}x\b|winner|join (the|my|us|now)|\bdm\b|link in bio|free \$|\bgem\b|100x|turned \$|would have turned|worth \$[\d,]+ now|signals? group|wallet gets|whitelist|\bwl\b|\bca:|up for grabs|prophetic|like and share|competition|\bnft\b|shocking|coincidence", re.I)

I = re.I
TAGS = {
    "BTC": re.compile(r"\b(bitcoin|btc)\b", I),
    "ETH": re.compile(r"\b(ethereum|ether|eth)\b", I),
    "SOL": re.compile(r"\bsolana\b|\bSOL\b"),
    "XRP": re.compile(r"\b(xrp|ripple)\b", I),
    "BNB": re.compile(r"\bbnb\b", I),
    "DOGE": re.compile(r"\b(dogecoin|doge)\b", I),
    "ADA": re.compile(r"\bcardano\b|\bADA\b"),
    "AVAX": re.compile(r"\b(avalanche|avax)\b", I),
    "LINK": re.compile(r"\bchainlink\b|\bLINK\b"),
    "SUI": re.compile(r"\bSUI\b|\bSui (network|blockchain|token)\b"),
    "SPY": re.compile(r"(s&p ?500|\bspx\b|\bspy\b|wall street|stock market|stocks? (rise|fall|rally|slump|close))", I),
    "QQQ": re.compile(r"\b(nasdaq|qqq|tech stocks|big tech|magnificent seven)\b", I),
    "DIA": re.compile(r"\b(dow jones|the dow|dow industrials|\bDIA\b)\b", I),
    "IWM": re.compile(r"\b(russell 2000|small[- ]caps?|iwm)\b", I),
    "AAPL": re.compile(r"\b(apple|iphone|aapl)\b", I),
    "NVDA": re.compile(r"\b(nvidia|nvda)\b", I),
    "TSLA": re.compile(r"\b(tesla|tsla)\b", I),
    "MSFT": re.compile(r"\b(microsoft|msft)\b", I),
    "AMZN": re.compile(r"\b(amazon|amzn)\b", I),
    "META": re.compile(r"\b(meta platforms|facebook|instagram|zuckerberg)\b|\bMETA\b", I),
    "GOOGL": re.compile(r"\b(alphabet|googl|goog|google)\b", I),
}
# Headlines naming a data provider, venue or publisher are dropped entirely (rule: no brand names in public output).
BRANDS = re.compile(r"\b(coingecko|polymarket|alpaca|kalshi|coinbase|binance|yahoo|coindesk|cointelegraph|decrypt|cnbc|google news|reuters|bloomberg|"
                    r"marketwatch|barron'?s|investing\.com|benzinga|motley fool|seeking alpha|the block|bitcoin magazine|forbes|wsj|"
                    r"wall street journal|financial times|business insider|fortune|axios|cnn|fox business|zacks|tipranks|nasdaq\.com)\b", re.I)

POS = ["beat", "beats", "surge", "surges", "rally", "rallies", "record", "upgrade", "upgraded", "gain", "gains", "approval", "approved", "inflow", "inflows", "bullish", "soar", "soars", "rebound", "strong", "growth", "cut rates", "rate cut"]
NEG = ["miss", "misses", "plunge", "plunges", "slump", "selloff", "sell-off", "downgrade", "downgraded", "loss", "losses", "lawsuit", "hack", "hacked", "outflow", "outflows", "bearish", "crash", "crashes", "weak", "ban", "probe", "default", "recession", "rate hike"]

def tone(text):
    """Mirror of headlineScore() in the site's lib/sentiment.ts."""
    t = f" {text.lower()} "; s = 0
    for w in POS:
        if f" {w} " in t or f" {w}," in t or f" {w}." in t: s += 1
    for w in NEG:
        if f" {w} " in t or f" {w}," in t or f" {w}." in t: s -= 1
    return max(-1.0, min(1.0, s / 3))

def clean(title, strip_suffix):
    t = html.unescape(re.sub(r"<[^>]+>", "", title or "")).strip()
    t = re.sub(r"\s+", " ", t)
    if strip_suffix and " - " in t: t = t.rsplit(" - ", 1)[0].strip()
    return t

def parse_date(s):
    if not s: return None
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try: d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception: return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)

def items(xml_bytes):
    root = ET.fromstring(xml_bytes)
    for it in root.iter():
        tag = it.tag.split("}")[-1]
        if tag not in ("item", "entry"): continue
        get = lambda n: next((c.text for c in it if c.tag.split("}")[-1] == n and c.text), None)
        yield get("title"), get("pubDate") or get("published") or get("updated")

def collect(now=None, fetch=None):
    now = now or datetime.now(timezone.utc); fetch = fetch or (lambda u: requests.get(u, headers={"User-Agent": UA}, timeout=15))
    out, seen, stats, t0 = [], set(), {"feeds_ok": 0, "feeds_failed": 0, "dropped_brand": 0, "dropped_untagged": 0, "dropped_old": 0}, time.time()
    for i, (url, default, strip) in enumerate(FEEDS):
        if time.time() - t0 > BUDGET: break
        try:
            r = fetch(url); r.raise_for_status(); stats["feeds_ok"] += 1
            for title, date in items(r.content):
                h = clean(title, strip); d = parse_date(date)
                if not h or len(h) < 20 or len(h) > 300: continue
                if not d or now - d > MAX_AGE or d > now + timedelta(minutes=10): stats["dropped_old"] += 1; continue
                if BRANDS.search(h): stats["dropped_brand"] += 1; continue
                syms = sorted(set(default) | {s for s, rx in TAGS.items() if rx.search(h)})
                if not syms: stats["dropped_untagged"] += 1; continue
                key = hashlib.sha256(re.sub(r"[^a-z0-9]", "", h.lower()).encode()).hexdigest()[:32]
                if key in seen: continue
                seen.add(key)
                out.append({"published_at": d.astimezone(timezone.utc).isoformat(), "symbols": syms, "headline": h, "score": round(tone(h), 3), "dedupe_key": key, "kind": "news"})
        except Exception as e:
            stats["feeds_failed"] += 1; print(f"feed #{i} failed: {type(e).__name__}", file=sys.stderr)
    return out, stats

def _row(h, d, syms, now, kind, seen):
    if not h or len(h) < 20 or len(h) > 300 or not d or now - d > MAX_AGE or d > now + timedelta(minutes=10): return None
    if BRANDS.search(h): return None
    key = hashlib.sha256((kind + ":" + re.sub(r"[^a-z0-9]", "", h.lower())).encode()).hexdigest()[:32]
    if key in seen: return None
    seen.add(key)
    return {"published_at": d.astimezone(timezone.utc).isoformat(), "symbols": sorted(set(syms)), "headline": h, "score": round(tone(h), 3), "dedupe_key": key, "kind": kind}

def collect_extra(now=None, fetch=None):
    """Economic releases, company filings and (optionally) a small social sample. Same storage rules as headlines."""
    now = now or datetime.now(timezone.utc); seen, out, st = set(), [], {"macro": 0, "filings": 0, "social": 0, "extra_failed": 0}
    fetch = fetch or (lambda u, ua=UA: requests.get(u, headers={"User-Agent": ua}, timeout=15))
    for i, url in enumerate(MACRO_FEEDS):
        try:
            r = fetch(url); r.raise_for_status()
            for title, date in items(r.content):
                row = _row(clean(title, False), parse_date(date), MACRO_SYMS, now, "macro", seen)
                if row: out.append(row); st["macro"] += 1
        except Exception as e: st["extra_failed"] += 1; print(f"macro feed #{i} failed: {type(e).__name__}", file=sys.stderr)
    for t, name in FILING_TICKERS.items():
        try:
            r = fetch(filings_url(t), FILINGS_UA); r.raise_for_status()
            root = ET.fromstring(r.content)
            for ent in [e for e in root.iter() if e.tag.split("}")[-1] == "entry"][:5]:
                g = lambda n: next((c for c in ent.iter() if c.tag.split("}")[-1] == n), None)
                form = (g("category").get("term") if g("category") is not None else "") or ""
                if form not in FILING_FORMS: continue
                d = parse_date(g("updated").text if g("updated") is not None else None)
                row = _row(f"{name} files {form} {FILING_FORMS[form]} with US regulator", d, [t], now, "filing", seen)
                if row: row["score"] = 0.0; out.append(row); st["filings"] += 1
            time.sleep(0.15)   # stay far below the filings site's 10 requests/second guidance
        except Exception as e: st["extra_failed"] += 1; print(f"filings {t} failed: {type(e).__name__}", file=sys.stderr)
    if SOCIAL_TOKEN and (now.hour % SOCIAL_EVERY_H == 0 and now.minute < 20 or E("SOCIAL_FORCE") == "1"):
        try:
            rows, sst = social_rows(fetch_social(), now, seen); out += rows; st.update(sst)
        except Exception as e: st["extra_failed"] += 1; print(f"social step failed: {type(e).__name__}", file=sys.stderr)
    return out, st

def social_rows(posts, now, seen):
    """Hosted-scraper posts -> news_items rows labelled 'social'. Handles, links, hashtags and emoji stripped; spam and brand mentions dropped."""
    out, st = [], {"social": 0, "social_dropped": 0}
    for p in posts:
        if p.get("isRetweet") or p.get("isReply") or (p.get("lang") and p.get("lang") != "en"): st["social_dropped"] += 1; continue
        raw = html.unescape(p.get("text") or "")
        if SPAM.search(raw) or BRANDS.search(raw): st["social_dropped"] += 1; continue
        txt = re.sub(r"@\w+|https?://\S+|#", "", raw)
        txt = re.sub(r"[^\x20-\x7E\u00C0-\u024F\u2018-\u201D]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()[:280]
        syms = {k for k, rx in TAGS.items() if rx.search(txt)} | {m.upper() for m in re.findall(r"\$([A-Za-z]{2,5})\b", raw) if m.upper() in TAGS}
        if not syms: st["social_dropped"] += 1; continue
        row = _row(txt, parse_date(p.get("createdAt")), syms, now, "social", seen)
        if row: out.append(row); st["social"] += 1
        else: st["social_dropped"] += 1
    return out, st

def fetch_social(fetch=None):
    """One capped run of the hosted search scraper. Token sent as a header, never in the URL or logs."""
    f = fetch or (lambda body: requests.post(f"https://api.apify.com/v2/acts/{SOCIAL_ACTOR}/run-sync-get-dataset-items",
        params={"maxTotalChargeUsd": SOCIAL_MAX_USD_PER_RUN, "timeout": 180, "memory": 1024, "clean": "true"},
        headers={"Authorization": f"Bearer {SOCIAL_TOKEN}"}, json=body, timeout=240))
    r = f({"searchQueries": SOCIAL_QUERIES, "searchType": "Latest", "maxTweetsPerQuery": SOCIAL_PER_QUERY, "includeProfile": False, "includeAccountOrigin": False})
    r.raise_for_status()
    return [p for p in r.json() if isinstance(p, dict) and p.get("type", "tweet") == "tweet"]

def store(c, rows):
    """One batched insert; duplicates (dedupe_key) are skipped. Then 30-day retention on our own table."""
    c.execute("alter table news_items add column if not exists kind text not null default 'news'")  # additive; generic label only
    n = c.execute("""insert into news_items (published_at, symbols, headline, summary, score, dedupe_key, kind)
        select published_at, symbols, headline, null, score, dedupe_key, coalesce(kind, 'news') from jsonb_to_recordset(%s::jsonb)
          as r(published_at timestamptz, symbols text[], headline text, score double precision, dedupe_key text, kind text)
        on conflict (dedupe_key) do nothing returning 1""", (json.dumps(rows),)).fetchall()
    c.execute("delete from news_items where published_at < now() - interval '30 days'")
    return len(n)

def main():
    rows, stats = collect()
    extra, xs = collect_extra(); rows += extra; stats.update(xs)
    if DRY:
        print(json.dumps({"stats": stats, "count": len(rows), "rows": rows[:10]}, indent=1)); return 0
    import psycopg
    with psycopg.connect(DB, autocommit=True, prepare_threshold=None) as c:
        n = store(c, rows)
    print(json.dumps({**stats, "candidates": len(rows), "inserted": n}))
    return 0

if __name__ == "__main__":
    try: sys.exit(main())
    except Exception as e:
        print(f"news ingestion error: {type(e).__name__}", file=sys.stderr); sys.exit(0)
