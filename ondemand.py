"""On-demand Kronos run for one forecast_jobs row (JOB_ID env). Triggered by the site via workflow_dispatch (ondemand.yml).
Loads the job, runs Kronos, saves to forecasts, marks the job done/failed, and POSTs a signed webhook if one was given."""
import hashlib, hmac, ipaddress, json, os, re, socket, sys, time, traceback
from urllib.parse import urlparse
import requests
import worker as W

def safe_url(u):
    """https only, public addresses only (no loopback/private/link-local/metadata)."""
    p = urlparse(u or "")
    if p.scheme != "https" or not p.hostname or p.username or p.password: return False
    try: addrs = {ai[4][0] for ai in socket.getaddrinfo(p.hostname, p.port or 443)}
    except OSError: return False
    return bool(addrs) and all(ipaddress.ip_address(a.split("%")[0]).is_global for a in addrs)

def deliver(url, secret, payload):
    if not safe_url(url): return 0
    body = json.dumps(payload, separators=(",", ":")); ts = str(int(time.time()))
    sig = hmac.new(secret.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
    try:
        r = requests.post(url, data=body, timeout=10, allow_redirects=False,
                          headers={"Content-Type": "application/json", "X-QX-Signature": f"t={ts},v1={sig}", "X-QX-Event": "forecast.job.completed"})
        return r.status_code
    except requests.RequestException: return 0

def main():
    jid = os.environ.get("JOB_ID", "")
    if not re.match(r"^odj_[0-9a-f]{24}$", jid): print("bad JOB_ID"); return 1
    c = W.db()
    j = c.execute("update forecast_jobs set status = 'running', updated_at = now() where id = %s and status = 'queued' returning symbol, asset_class, timeframe, webhook_url, webhook_secret", (jid,)).fetchone()
    if not j: print("job not queued"); return 0
    sym, asset, tf, hook, secret = j
    ok_sym = re.match(W.STOCK_RE if asset == "equity" else W.CRYPTO_RE, sym) and tf in W.TF
    fid, err = None, None
    try:
        if not ok_sym: raise ValueError("invalid job")
        from model import Kronos, KronosTokenizer, KronosPredictor
        tok = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-base"); mdl = Kronos.from_pretrained("NeoQuasar/Kronos-small")
        pred = KronosPredictor(mdl, tok, device="cpu", max_context=512)
        row = W.forecast(pred, sym, tf, c)
        fid = W.save(c, row) or c.execute("select id from forecasts where symbol=%s and timeframe=%s and model=%s and as_of=%s", (sym, tf, row["model"], row["as_of"])).fetchone()[0]
        print(f"{jid} {sym} {tf} done id={fid} runtime={row['runtime_sec']}s")
    except Exception as e:
        err = "Not enough clean candles for this symbol." if isinstance(e, RuntimeError) else "The forecast run failed."
        print(f"{jid} failed: {type(e).__name__}")  # no traceback: public Actions logs must not show upstream URLs
    status = "done" if fid else "failed"
    c.execute("update forecast_jobs set status = %s, forecast_id = %s, error = %s, updated_at = now() where id = %s", (status, fid, err, jid))
    if hook and secret:
        base = os.environ.get("SITE_URL", "https://quantx402.app")
        code = deliver(hook, secret, {"event": "forecast.job.completed", "job_id": jid, "status": status, "symbol": sym, "timeframe": tf,
                                      "status_url": f"{base}/api/v1/forecast/jobs/{jid}"})
        c.execute("update forecast_jobs set webhook_status = %s where id = %s", (code, jid))
    c.close()
    return 0 if fid else 1

if __name__ == "__main__":
    sys.exit(main())
