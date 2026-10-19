"""Rationale writer for /api/v1/brief (SPEC_research_brief §6). Runs on the worker (CPU), never on the site.

Reads recent briefs that still use the template rationale, asks a small self-hosted open instruct model for a
strict-JSON rationale, applies the same guardrails as the site (lib/brief/rationale.ts), and stores passing text in
brief_rationales. The site re-checks every stored text against the current numbers before serving it, so a bad or
outdated text can only ever fall back to the template. Model and runtime names stay internal (never served or logged publicly).
"""
import json, os, re, sys, time
import psycopg

MODEL_PATH = os.environ.get("RATIONALE_MODEL_PATH", "model/rationale.gguf")
MODEL_ID = os.environ.get("RATIONALE_MODEL_ID", "open-instruct-1.5b-q4")
MAX_BRIEFS = int(os.environ.get("RATIONALE_MAX_BRIEFS", "30"))

RULES_PROMPT = "\n".join([
    "You write a short factual rationale for a market research brief. Output strict JSON only: {\"rationale\": \"...\"}.",
    "Write at most 3 sentences, 420 characters, plain English. Describe only the values given. Say the direction lean and its confidence bucket, the 80% range, the news tone and whether it conflicts, and one risk number. Do not predict, advise, or add facts. Use 'leans', 'range', 'may'.",
    "Rules: never promise or imply profit or returns; never tell anyone to buy, sell, enter, exit or hold; never claim accuracy or an edge.",
    "If news_conflict is true, say plainly that news tone disagrees with the model lean. If it is null, do not call it a conflict.",
    "Use only numbers that appear in the input JSON, written exactly as given. You may mention a headline only by quoting it exactly from the headlines list; never invent or paraphrase news.",
    "Do not name any company, website, exchange, data provider or news outlet.",
])
BANNED = [r"\bguarantee(d|s)?\b", r"\bwill\b", r"\bcertain(ly)?\b", r"\bsure\b", r"\bdefinitely\b", r"\bbuy(ing|s)?\b", r"\bsell(ing|s)?\b", r"\bshort it\b", r"\bgo long\b", r"\blong position\b",
          r"\benter\b", r"\bexit\b", r"\btake profit\b", r"\bstop loss\b", r"\bshould\b", r"\bmust\b", r"\brecommend(s|ed|ation)?\b", r"\bopportunit(y|ies)\b", r"\bmoon\b", r"\bpump\b", r"\bdump\b",
          r"\bedge\b", r"\bbeats the market\b", r"\baccura(te|cy)\b", r"\bproven\b", r"\bprofit", r"\bwin rate\b", r"\d+(\.\d+)?\s*%\s*(win|accura|hit|success)"]
BRANDS = re.compile(r"\b(coingecko|polymarket|alpaca|kalshi|coinbase|binance|yahoo|coindesk|cointelegraph|decrypt|cnbc|google|reuters|bloomberg|marketwatch|barron'?s|investing\.com|benzinga|motley fool|seeking alpha|the block|bitcoin magazine|forbes|wsj|wall street journal|financial times|business insider|fortune|axios|cnn|fox business|zacks|tipranks|nasdaq|sec\.gov|edgar|qwen|alibaba|openai|apify|x\.com|twitter)\b", re.I)
ALWAYS = [24, 80, 95, 99, 48]

def allowed(inp):
    out = list(ALWAYS)
    def walk(v):
        if isinstance(v, bool): return
        if isinstance(v, (int, float)): out.extend([v, abs(v)])
        elif isinstance(v, dict): [walk(x) for x in v.values()]
        elif isinstance(v, list): [walk(x) for x in v]
    walk({k: v for k, v in inp.items() if k != "headlines"})
    return out

def check(text, inp):
    t = (text or "").strip()
    if not t: return "empty"
    if len(t) > 420: return "too_long"
    if len(re.findall(r"[.!?](\s|$)", t)) > 3: return "too_many_sentences"
    for b in BANNED:
        if re.search(b, t, re.I): return "banned"
    if BRANDS.search(t): return "brand"
    for q in re.findall(r"[\"“]([^\"”]{6,})[\"”]", t):
        if not any(q.strip().lower() in h.lower() for h in inp["headlines"]): return "uncited_quote"
    s = re.sub(r"[\"“][^\"”]*[\"”]", " ", t); s = re.sub(r"\b1-day\b", "one-day", s, flags=re.I)
    al = allowed(inp)
    for m in re.finditer(r"-?\d+(\.\d+)?", s):
        v, d = float(m.group(0)), len((m.group(0).split(".") + [""])[1])
        if not any(abs(round(a, d) - v) < 1e-9 or abs(round(-a, d) - v) < 1e-9 for a in al): return "number"
    d = inp["forecast"]["direction"]
    up, dn = re.search(r"\b(up|upward|higher)\b", s, re.I), re.search(r"\b(down|downward|lower)\b", s, re.I)
    neu = re.search(r"\b(leans? neutral|neutral lean|lean is neutral|no clear (lean|direction)|no lean)\b", s, re.I)
    n = inp.get("news"); lbl = n["tone_label"].replace("_", " ") if n else None
    if lbl and not re.search(r"\b" + lbl + r"\b", s, re.I): return "news_tone_mismatch"
    if not lbl and re.search(r"\btone\b[^.]{0,40}\b(positive|negative|neutral)\b", s, re.I): return "news_tone_mismatch"
    if (d == "UP" and (not up or neu)) or (d == "DOWN" and (not dn or neu)) or (d == "NEUTRAL" and not neu): return "direction_mismatch"
    conf = re.search(r"\b(conflict|disagree)", s, re.I)
    if inp["news_conflict"] is not True and conf and not re.search(r"\b(no|not|without)\b[^.]{0,30}\b(conflict|disagree)", s, re.I): return "false_conflict"
    if inp["news_conflict"] is True and not conf: return "missing_conflict"
    return None

SLOT_PROMPT = RULES_PROMPT + """
Numbers: never write digits yourself. Write a placeholder from the 'slots' list instead, e.g. {band_lo} or {var95}; our code fills in the exact value.
Keep it to 2 or 3 short sentences. Mention: the lean and confidence, the 80% range ({band_lo} to {band_hi}), the news tone and conflict status, and {var95}.
Example output: {"rationale": "The model leans UP with low confidence, and its 80% range over 24h is {band_lo} to {band_hi}. News tone over 48h is slightly negative, which conflicts with the lean. The 1-day 95% VaR is {var95}."}"""

def slots(inp):
    f, r = inp["forecast"], inp.get("risk") or {}
    pct = lambda v: None if v is None else f"{v}%"
    s = {"band_lo": pct(f["band_80_pct"]["lo"]), "band_hi": pct(f["band_80_pct"]["hi"]),
         "var95": pct((r.get("var_1d_pct") or {}).get("p95")), "var99": pct((r.get("var_1d_pct") or {}).get("p99")),
         "vol30": pct((r.get("realized_vol_ann_pct") or {}).get("d30")), "mdd30": pct((r.get("max_drawdown_pct") or {}).get("d30")),
         "corr_btc": None if r.get("corr_btc_30d") is None else str(r["corr_btc_30d"])}
    tr = inp.get("track_record")
    if tr and tr.get("direction_calls"): s.update(tr_hits=str(tr["direction_hits"]), tr_calls=str(tr["direction_calls"]))
    return {k: v for k, v in s.items() if v is not None}

def user_prompt(inp):
    n = inp.get("news")
    return json.dumps({"lean": inp["forecast"]["direction"], "confidence": inp["forecast"]["confidence_bucket"],
        "news_tone": n["tone_label"].replace("_", " ") if n else "no news items",
        "news_conflict": inp["news_conflict"],
        "track_record": "recent scored direction calls: {tr_hits} right out of {tr_calls}" if inp.get("track_record", {}) and (inp.get("track_record") or {}).get("direction_calls") else "not enough scored history yet",
        "headlines": inp.get("headlines", [])[:5], "slots": ["{" + k + "}" for k in slots(inp)]})

def fill(text, inp):
    """Reject digits written by the model (outside exact headline quotes); replace placeholders with code-computed values."""
    sl = slots(inp)
    bare = re.sub(r"[\"“][^\"”]*[\"”]", " ", text)
    bare = re.sub(r"\{[a-z0-9_]+\}", "", bare)
    bare = re.sub(r"\b(1-day|24h|24 hours|48h|48 hours|80%|95%|99%|30-day|30 days|7-day|90-day)(?!\w)", " ", bare, flags=re.I)
    if re.search(r"\d", bare): return None, "model_wrote_digits"
    for k in re.findall(r"\{([a-z0-9_]+)\}", text):
        if k not in sl: return None, "unknown_slot"
    return re.sub(r"\{([a-z0-9_]+)\}", lambda m: sl[m.group(1)], text), None

_llm = None
def llm():
    global _llm
    if _llm is None:
        from llama_cpp import Llama
        _llm = Llama(model_path=MODEL_PATH, n_ctx=2048, n_threads=os.cpu_count() or 4, verbose=False)
    return _llm

def generate(inp, tries=2):
    """Returns (text or None, reason, latency_ms). Strict JSON; numbers only via code-filled slots; then the full guardrail check."""
    t0 = time.time(); last = "no_output"
    for k in range(tries):
        out = llm().create_chat_completion(messages=[{"role": "system", "content": SLOT_PROMPT}, {"role": "user", "content": user_prompt(inp)}],
            temperature=0.2, max_tokens=140, seed=42 + k, response_format={"type": "json_object"})
        raw = out["choices"][0]["message"]["content"]
        try:
            j = json.loads(raw); text = j["rationale"].strip() if isinstance(j, dict) and set(j) == {"rationale"} and isinstance(j["rationale"], str) else None
        except Exception: text = None
        if text is None: last = "not_json"; continue
        text, why = fill(text, inp)
        if text is None: last = why; continue
        why = check(text, inp)
        if why is None: return text, None, int((time.time() - t0) * 1000)
        last = why
    return None, last, int((time.time() - t0) * 1000)

def main():
    url = os.environ.get("DATABASE_URL")
    if not url: print("DATABASE_URL missing", file=sys.stderr); return 0
    if not os.path.exists(MODEL_PATH): print("rationale model file missing; skipping", file=sys.stderr); return 0
    with psycopg.connect(url, autocommit=True) as c:
        rows = c.execute("""select distinct on (b.symbol) b.id, b.symbol, b.forecast_id, b.rationale_inputs from briefs b
            where b.created_at > now() - interval '90 minutes' and b.rationale_inputs is not null
              and not exists (select 1 from brief_rationales r where r.symbol = b.symbol and r.forecast_id = b.forecast_id)
            order by b.symbol, b.created_at desc limit %s""", (MAX_BRIEFS,)).fetchall()
        ok = bad = 0
        for bid, sym, fid, inp in rows:
            text, why, ms = generate(inp)
            if text:
                c.execute("insert into brief_rationales (brief_id, symbol, forecast_id, text, model, latency_ms) values (%s,%s,%s,%s,%s,%s)", (bid, sym, fid, text, MODEL_ID, ms)); ok += 1
            else: bad += 1
            print(f"{sym} forecast={fid} {'stored' if text else 'rejected:' + str(why)} {ms}ms")
        print(f"rationales stored={ok} rejected={bad}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
