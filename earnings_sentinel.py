#!/usr/bin/env python3
"""
LAI Desk · Earnings Sentinel
============================
An always-on poller that watches SEC EDGAR for your tickers' earnings 8-Ks the
INSTANT they drop, parses the press release (revenue, guidance, margin direction,
catalysts), computes a quick good/bad verdict, fires a Discord alert, and pushes
the structured reading to your LAI worker so the dashboard's Earnings Readings
section shows it immediately — giving you an edge the moment results hit.

Why a separate process? Cloudflare cron is ~15-min granularity. This loop polls
every ~45s, so you hear about a report within a minute of it filing.

WHERE TO RUN IT (needs to be always-on):
  • a small VM / Raspberry Pi / your Mac (launchd), Railway, Render, Replit, Fly.io
  • NOT GitHub Actions (its cron floor is 5 min and timing is unreliable)

Run:
  STOCK_WATCH_TICKERS="CRDO,NVDA,AVGO" \
  EXISTING_WEBHOOK_URL="https://discord.com/api/webhooks/..." \
  INGEST_URL="https://lai-yahoo-proxy.<sub>.workers.dev/earnings-read-ingest" \
  INGEST_KEY="your-ingest-key" \
  FMP_KEY="your-fmp-key" \
  python3 earnings_sentinel.py            # loops forever
  python3 earnings_sentinel.py --once     # single pass (for testing / cron)

Environment variables
---------------------
  STOCK_WATCH_TICKERS  (required)  comma list, e.g. "CRDO,NVDA,AVGO"
  EXISTING_WEBHOOK_URL (optional)  Discord/Slack/ntfy webhook for the alert
  INGEST_URL           (optional)  worker /earnings-read-ingest endpoint
  INGEST_KEY           (optional)  shared secret = worker's INGEST_KEY var
  FMP_KEY              (optional)  Financial Modeling Prep key (for EPS/Rev actual vs est)
  POLL_SECONDS         (optional)  loop interval (default 45)
  LOOKBACK_DAYS        (optional)  only alert on 8-Ks within N days (default 3)
  STATE_FILE           (optional)  dedupe store (default ./earnings_sentinel_state.json)
  SEC_UA               (optional)  SEC requires a descriptive UA (default below)
"""

import os
import sys
import re
import json
import time
import html
import datetime as dt
from urllib import request, error

WATCH = [t.strip().upper() for t in os.environ.get("STOCK_WATCH_TICKERS", "").split(",") if t.strip()]
WEBHOOK_URL = os.environ.get("EXISTING_WEBHOOK_URL", "").strip()
INGEST_URL = os.environ.get("INGEST_URL", "").strip()
INGEST_KEY = os.environ.get("INGEST_KEY", "").strip()
FMP_KEY = os.environ.get("FMP_KEY", "").strip()
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "45"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))
STATE_FILE = os.environ.get("STATE_FILE", "earnings_sentinel_state.json")
SEC_UA = os.environ.get("SEC_UA", "LAI Desk Earnings Sentinel contact@example.com")
BROWSER_UA = "Mozilla/5.0 (compatible; LAIDesk/1.0)"

_CIK_MAP = {}


# ── tiny HTTP helpers (stdlib only) ──────────────────────────────────────────
def http_get(url, headers=None, timeout=20):
    req = request.Request(url, headers=headers or {}, method="GET")
    with request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def http_post(url, payload, headers=None, timeout=15):
    data = json.dumps(payload).encode("utf-8")
    h = {"Content-Type": "application/json", "User-Agent": BROWSER_UA}
    if headers:
        h.update(headers)
    req = request.Request(url, data=data, headers=h, method="POST")
    with request.urlopen(req, timeout=timeout) as r:
        return 200 <= r.status < 300


def fmt_rev(n):
    if n is None:
        return "?"
    n = float(n)
    return ("$%.2fB" % (n / 1e9)) if abs(n) >= 1e9 else ("$%.0fM" % (n / 1e6))


# ── SEC ──────────────────────────────────────────────────────────────────────
def load_cik_map():
    global _CIK_MAP
    if _CIK_MAP:
        return _CIK_MAP
    try:
        txt = http_get("https://www.sec.gov/files/company_tickers.json", {"User-Agent": SEC_UA})
        j = json.loads(txt)
        m = {}
        for k in j:
            e = j[k]
            if e and e.get("ticker"):
                m[e["ticker"].upper()] = str(e["cik_str"]).zfill(10)
        _CIK_MAP = m
    except Exception as exc:
        print("[warn] CIK map load failed:", exc)
    return _CIK_MAP


def get_recent_filings(cik):
    txt = http_get("https://data.sec.gov/submissions/CIK%s.json" % cik, {"User-Agent": SEC_UA, "Accept": "application/json"})
    rec = (json.loads(txt).get("filings") or {}).get("recent") or {}
    return rec


def find_earnings_8k(rec, expected_date):
    forms = rec.get("form") or []
    cutoff = (dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)).isoformat()
    for i in range(min(len(forms), 25)):
        if forms[i] != "8-K":
            continue
        fdate = (rec.get("filingDate") or [])[i] if i < len(rec.get("filingDate") or []) else ""
        if fdate and fdate < cutoff:
            continue
        items = ((rec.get("items") or [])[i] if i < len(rec.get("items") or []) else "").replace(" ", "")
        near = False
        if expected_date and fdate:
            try:
                near = abs((dt.date.fromisoformat(fdate) - dt.date.fromisoformat(expected_date)).days) <= 2
            except Exception:
                near = False
        if "2.02" in items or (near and ("9.01" in items or not items)):
            return (rec.get("accessionNumber") or [])[i], fdate
    return None, None


def fetch_pr_text(cik, accession):
    cik_n = str(int(cik))
    acc_no = accession.replace("-", "")
    base = "https://www.sec.gov/Archives/edgar/data/%s/%s" % (cik_n, acc_no)
    try:
        idx = json.loads(http_get(base + "/index.json", {"User-Agent": SEC_UA}))
        names = [it.get("name", "") for it in (idx.get("directory") or {}).get("item", [])]
        pick = next((n for n in names if re.search(r"ex.?99", n, re.I) and re.search(r"\.html?$", n, re.I)), None) \
            or next((n for n in names if re.search(r"exhibit", n, re.I) and re.search(r"\.html?$", n, re.I)), None)
        if not pick:
            return ""
        raw = http_get("%s/%s" % (base, pick), {"User-Agent": SEC_UA})
        txt = re.sub(r"<[^>]+>", " ", raw)
        txt = html.unescape(txt)
        txt = re.sub(r"[•·]", " ", txt)
        return re.sub(r"\s+", " ", txt).strip()
    except Exception as exc:
        print("[warn] PR fetch failed:", exc)
        return ""


# ── parse the press release (mirrors the worker) ─────────────────────────────
def parse_pr(txt):
    out = {"revenue": None, "eps": None, "eps_basis": "", "guidance": "", "gross_margin": None, "highlights": []}
    if not txt:
        return out
    # revenue (reported, skip guidance sentences)
    for m in re.finditer(r"(?:total\s+|net\s+)?(?:revenue|net sales)\b[^$]{0,25}\$\s?([\d.,]+)\s*(million|billion|[mb])\b", txt, re.I):
        ctx = txt[max(0, m.start() - 45): m.end()]
        if re.search(r"expect|guidance|outlook|anticipat|forecast|project|to be between|next quarter|first quarter|second quarter|third quarter|fourth quarter|full year|fiscal\s*20\d\d", ctx, re.I):
            continue
        try:
            v = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        v *= 1e9 if m.group(2).lower().startswith("b") else 1e6
        if v > 1e6:
            out["revenue"] = v
            break
    # actual reported EPS — prefer NON-GAAP diluted; guard checks the text BEFORE the
    # figure only (a trailing "compared to $X prior year" clause must not disqualify
    # the actual EPS that precedes it). Mirrors the worker exactly.
    cands = []

    def scan_eps(pattern, basis):
        for mm in re.finditer(pattern, txt, re.I):
            pre = txt[max(0, mm.start() - 60): mm.start()]
            if re.search(r"prior[\s-]year|year[\s-]ago|same period|compared to|guidance|outlook|we expect|anticipat|forecast|project", pre, re.I):
                continue
            try:
                v = float(mm.group(1))
            except ValueError:
                continue
            if 0 < v < 1000:
                cands.append((v, basis))

    scan_eps(r"non-?GAAP[^.$%]{0,70}?(?:net income|earnings|EPS|income)\s+per\s+(?:diluted\s+)?(?:common\s+)?share[^$%\d]{0,22}\$\s?(\d+\.\d{1,2})", "non-GAAP")
    scan_eps(r"(?:GAAP\s+)?(?:diluted\s+)?(?:net income|earnings)\s+per\s+(?:diluted\s+)?(?:common\s+)?share[^$%\d]{0,22}\$\s?(\d+\.\d{1,2})", "GAAP")
    if cands:
        pick = next((c for c in cands if c[1] == "non-GAAP"), None) or cands[0]
        out["eps"], out["eps_basis"] = pick[0], pick[1]
    # guidance
    move = re.search(r"\b(rais\w+|lower\w+|reaffirm\w+|reiterat\w+|increas\w+|cut)\b[^.]{0,60}\b(guidance|outlook|forecast)\b", txt, re.I)
    rng = re.search(r"\b(?:expects?|expected|sees|guidance|outlook|forecasts?|anticipates?|projects?)\b[^.]{0,130}?\$\s?[\d.,]+\s*(?:billion|million|[bm]\b)?(?:\s*(?:to|-|–|—|and)\s*\$?\s?[\d.,]+\s*(?:billion|million|[bm]\b)?)?", txt, re.I)
    s = rng.group(0) if rng else (move.group(0) if move else "")
    if s:
        direction = ""
        if move:
            w = move.group(1).lower()
            direction = " raised" if re.search(r"rais|increas", w) else " cut" if re.search(r"lower|cut", w) else " reaffirmed" if re.search(r"reaffirm|reiterat", w) else ""
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"^(outlook|guidance|forecast|guidance update)\b[\s:.\-–—]*", "", s, flags=re.I).strip()
        if len(s) > 160:
            s = s[:157] + "…"
        out["guidance"] = "\U0001F4C8 Guidance%s: %s" % (direction, s)
    # gross margin (only when compared → direction is meaningful)
    gm = re.search(r"gross margin\s*(?:of|was|were|:)?\s*([\d.]+)\s*%[^.]{0,70}?(?:compared\s+to|versus|\bvs\.?\b|up\s+from|down\s+from|from)\s*([\d.]+)\s*%", txt, re.I)
    if gm:
        try:
            cur, prev = float(gm.group(1)), float(gm.group(2))
            if 0 < cur < 100 and 0 < prev < 100 and abs(cur - prev) < 40:
                out["gross_margin"] = cur
                d = "▲ expanding" if cur > prev else "▼ compressing" if cur < prev else "flat"
                out["highlights"].append("\U0001F4CA Gross margin %s%% (%s from %s%%)" % (cur, d, prev))
        except ValueError:
            pass

    boiler = re.compile(r"any particular amount|ordinary shares\b|shares of (?:its )?common stock\b|from time to time|may (?:not\s+)?be (?:suspended|fully|modified|terminated)|no (?:obligation|assurance)|not (?:be )?obligated|in its discretion|subject to (?:market|the)|forward[\s-]looking|risks and uncertaint|no guarantee|safe harbor|may be discontinued|to acquire and|and integrate|other companies|products,? or technolog|our ability|ability to (?:acquire|integrate)|successfully (?:acquire|integrate)", re.I)

    def grab(pattern, emoji):
        m = re.search(pattern, txt, re.I)
        if not m:
            return
        snip = txt[m.start(): m.start() + 130]
        snip = re.sub(r"([.;])\s.*$", r"\1", re.sub(r"\s+", " ", snip).strip())
        if boiler.search(snip):
            return
        if len(snip) > 110:
            snip = snip[:107] + "…"
        out["highlights"].append("%s %s" % (emoji, snip))

    grab(r"\$[\d.,]+\s*(?:billion|million)\s+(?:new\s+)?(?:share\s+)?(?:repurchase|buyback)\s+(?:program|authoriz)[^.]{0,60}", "\U0001F4B0")
    grab(r"\b(?:awarded|new (?:multi-year )?contract|contract (?:win|award)|design win|order valued)\b[^.]{0,90}", "\U0001F4D1")
    grab(r"\b(?:partnership with|partnered with|strategic (?:alliance|collaboration|partnership)|collaboration with|teamed up with)\b[^.]{0,90}", "\U0001F91D")
    grab(r"\b(?:agreed to acquire|completed the acquisition of|has acquired|to acquire)\s+[A-Z][A-Za-z0-9.&'\- ]{2,50}", "\U0001F3F7")
    out["highlights"] = out["highlights"][:3]
    return out


# ── FMP actuals (EPS / Rev vs estimate) ──────────────────────────────────────
def fmp_actuals(sym):
    # Returns the next earnings date + the CONSENSUS ESTIMATE for the current report
    # period. It deliberately does NOT return a reported actual: when a report just
    # dropped FMP still shows the prior quarter, so the actual comes from the press
    # release instead. Estimates are stable and safe to use.
    res = {"eps": None, "eps_est": None, "rev": None, "rev_est": None, "exp_date": None}
    if not FMP_KEY:
        return res
    try:
        arr = json.loads(http_get("https://financialmodelingprep.com/api/v3/historical/earning_calendar/%s?apikey=%s" % (sym, FMP_KEY), {"User-Agent": BROWSER_UA}))
        rows = [x for x in arr if x.get("date")]
        if not rows:
            return res
        today = dt.date.today()
        fut = sorted([x for x in rows if x["date"] >= today.isoformat()], key=lambda x: x["date"])
        if fut:
            res["exp_date"] = fut[0]["date"]
        # estimate from the row CLOSEST to today (the just-/about-to-report period)
        near = min(rows, key=lambda x: abs((dt.date.fromisoformat(x["date"]) - today).days))
        res["eps_est"] = near.get("epsEstimated")
        res["rev_est"] = near.get("revenueEstimated")
    except Exception:
        pass
    return res


def build_verdict(eps, eps_est, rev, rev_est, eps_basis=""):
    eps_pct = ((eps - eps_est) / abs(eps_est) * 100) if (eps is not None and eps_est) else None
    rev_pct = ((rev - rev_est) / abs(rev_est) * 100) if (rev is not None and rev_est) else None
    basis_tag = (" (%s, from release)" % eps_basis) if (eps is not None and eps_basis) else ""
    bits = []
    if eps is not None:
        bits.append("EPS $%.2f%s%s%s" % (eps, (" vs $%.2f est" % eps_est) if eps_est is not None else "", (" (%+.1f%%)" % eps_pct) if eps_pct is not None else "", basis_tag))
    if rev is not None:
        bits.append("Rev %s%s%s" % (fmt_rev(rev), (" vs %s est" % fmt_rev(rev_est)) if rev_est is not None else "", (" (%+.1f%%)" % rev_pct) if rev_pct is not None else ""))
    if not bits:
        return "\U0001F4CB Actuals pending — see the filing.", eps_pct, rev_pct
    if eps_pct is not None and rev_pct is not None:
        v = "\U0001F7E2 Strong (double beat)" if eps_pct >= 0 and rev_pct >= 0 else "\U0001F534 Weak (double miss)" if eps_pct < 0 and rev_pct < 0 else "\U0001F7E1 Mixed"
    elif eps_pct is not None:
        v = "✅ EPS beat" if eps_pct >= 0 else "❌ EPS miss"
    elif rev_pct is not None:
        v = "✅ Rev beat · EPS pending" if rev_pct >= 0 else "❌ Rev miss · EPS pending"
    else:
        v = "\U0001F4CB Reported"
    return "%s — %s" % (v, " · ".join(bits)), eps_pct, rev_pct


# ── dispatch ─────────────────────────────────────────────────────────────────
def send_webhook(text):
    if not WEBHOOK_URL:
        return
    try:
        url = WEBHOOK_URL
        if "discord" in url:
            data, headers = json.dumps({"content": text[:1900]}).encode(), {"Content-Type": "application/json"}
        elif "hooks.slack.com" in url:
            data, headers = json.dumps({"text": text}).encode(), {"Content-Type": "application/json"}
        elif "ntfy.sh" in url:
            data, headers = text.encode(), {"Content-Type": "text/plain; charset=utf-8"}
        else:
            data, headers = json.dumps({"text": text, "content": text}).encode(), {"Content-Type": "application/json"}
        headers["User-Agent"] = BROWSER_UA
        req = request.Request(url, data=data, headers=headers, method="POST")
        with request.urlopen(req, timeout=15):
            pass
    except Exception as exc:
        print("[warn] webhook failed:", exc)


def push_ingest(reading):
    if not (INGEST_URL and INGEST_KEY):
        return
    try:
        sep = "&" if "?" in INGEST_URL else "?"
        http_post("%s%skey=%s" % (INGEST_URL, sep, INGEST_KEY), reading, {"x-ingest-key": INGEST_KEY})
    except Exception as exc:
        print("[warn] ingest failed:", exc)


def worker_base():
    # derive the worker root from INGEST_URL (…/earnings-read-ingest)
    return INGEST_URL.split("/earnings-read-ingest")[0] if INGEST_URL else ""


def claim_alert(tk, acc):
    """Durable, SHARED dedup via the worker's KV. Returns True only the first time a
    given (ticker, accession) is claimed — so even if this job's GitHub Actions cache
    state file is lost between the after-close and pre-open runs, we won't re-alert the
    same 8-K, and we won't collide with the worker's own alert path either.
    On any error we fall back to True (local state still guards) — better a rare dup
    than a missed earnings drop."""
    base = worker_base()
    if not base or not INGEST_KEY:
        return True
    try:
        from urllib.parse import quote
        url = "%s/earnings-claim?ticker=%s&acc=%s&key=%s" % (base, quote(tk), quote(acc), quote(INGEST_KEY))
        r = json.loads(http_get(url, {"User-Agent": BROWSER_UA}, timeout=20))
        return bool(r.get("first", True))
    except Exception:
        return True


def fetch_worker_reading(sym):
    """Ask the worker for the full reading (it has FMP + the income-statement /
    analyst-estimate / press-release fallbacks). Used when no local FMP_KEY."""
    base = worker_base()
    if not base:
        return None
    try:
        # fresh=1 forces the worker to recompute from FMP (don't serve a stale/empty cache)
        return json.loads(http_get("%s/earnings-read?ticker=%s&fresh=1" % (base, sym), {"User-Agent": BROWSER_UA}, timeout=30))
    except Exception:
        return None


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"seen": {}}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as exc:
        print("[warn] state save failed:", exc)


# ── core ─────────────────────────────────────────────────────────────────────
def process_ticker(tk, cik, state):
    fmp = fmp_actuals(tk)
    rec = get_recent_filings(cik)
    acc, fdate = find_earnings_8k(rec, fmp.get("exp_date"))
    if not acc:
        return False
    if state["seen"].get(tk) == acc:
        return False  # already handled this report (local fast-path)
    # Durable cross-run / cross-system guard: if the worker (or a prior lost-state run)
    # already claimed this exact 8-K, record it locally and stay quiet.
    if not claim_alert(tk, acc):
        state["seen"][tk] = acc
        print("[skip] %s %s — already alerted (shared claim)" % (tk, acc))
        return False

    # HIGH-SPEED PATH: parse the press release locally and build the verdict right here
    # — no slow round-trips on the critical path. The ACTUAL EPS/revenue come from the
    # release (correct for THIS report); the consensus ESTIMATE comes from FMP (stable).
    # We never use FMP's stale "actual" (it's the prior quarter until FMP catches up).
    pr = parse_pr(fetch_pr_text(cik, acc))
    eps = pr.get("eps")
    rev = pr.get("revenue")
    eps_basis = pr.get("eps_basis") or ""
    eps_est, rev_est = fmp.get("eps_est"), fmp.get("rev_est")
    guidance = pr.get("guidance") or ""
    highlights = list(pr.get("highlights") or [])
    gross_margin = pr.get("gross_margin")
    ir = ""
    verdict, eps_pct, rev_pct = build_verdict(eps, eps_est, rev, rev_est, eps_basis)
    # Fallback ONLY if the local parse found nothing — then ask the (corrected) worker.
    if eps is None and rev is None:
        wr = fetch_worker_reading(tk) or {}
        if wr:
            eps, eps_est = wr.get("eps"), wr.get("epsEst")
            rev, rev_est = wr.get("rev"), wr.get("revEst")
            guidance = guidance or wr.get("guidance") or ""
            highlights = highlights or list(wr.get("highlights") or [])
            gross_margin = gross_margin if gross_margin is not None else wr.get("grossMargin")
            ir = wr.get("irLink") or ir
            if wr.get("verdict"):
                verdict, eps_pct, rev_pct = wr["verdict"], wr.get("epsPct"), wr.get("revPct")
    if not ir:
        ir = "https://www.google.com/search?q=" + tk + "+investor+relations"

    acc_no = acc.replace("-", "")
    sec_url = "https://www.sec.gov/Archives/edgar/data/%s/%s/" % (int(cik), acc_no)
    lines = ["\U0001F4CA **%s** reported earnings (8-K)" % tk]
    if verdict:
        lines.append(verdict)
    if guidance:
        lines.append(guidance)
    lines += highlights
    lines.append("[SEC filing](<%s>)" % sec_url)
    if ir:
        lines.append("[Investor Relations](<%s>)" % ir)
    send_webhook("\n".join(lines))

    push_ingest({
        "ticker": tk, "date": fdate or "", "verdict": verdict,
        "eps": eps, "epsEst": eps_est,
        "epsPct": round(eps_pct, 1) if eps_pct is not None else None,
        "rev": rev, "revEst": rev_est,
        "revPct": round(rev_pct, 1) if rev_pct is not None else None,
        "grossMargin": gross_margin, "guidance": guidance, "highlights": highlights,
    })

    state["seen"][tk] = acc
    print("[ALERT] %s %s — %s" % (tk, fdate, verdict or "(no actuals yet)"))
    return True


def run_once():
    if not WATCH:
        print("[info] STOCK_WATCH_TICKERS not set — nothing to watch."); return
    cmap = load_cik_map()
    state = load_state()
    fired = 0
    for tk in WATCH:
        cik = cmap.get(tk)
        if not cik:
            continue
        try:
            if process_ticker(tk, cik, state):
                fired += 1
        except (error.URLError, error.HTTPError, TimeoutError, OSError) as exc:
            print("[warn] %s: %s" % (tk, exc))
        except Exception as exc:
            print("[warn] %s unexpected: %s" % (tk, exc))
        time.sleep(0.3)  # be gentle on SEC (≤10 req/s)
    save_state(state)
    return fired


def main():
    once = "--once" in sys.argv
    print("[start] Earnings Sentinel · %d tickers · poll %ss · once=%s" % (len(WATCH), POLL_SECONDS, once))
    print("[config] FMP_KEY=%s · webhook=%s · ingest=%s · worker-fallback=%s" % (
        "set" if FMP_KEY else "MISSING (will use worker fallback)",
        "set" if WEBHOOK_URL else "MISSING", "set" if (INGEST_URL and INGEST_KEY) else "MISSING",
        "available" if worker_base() else "no (set INGEST_URL)"))
    if once:
        run_once(); return
    while True:
        try:
            run_once()
        except Exception as exc:
            print("[error] loop:", exc)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
