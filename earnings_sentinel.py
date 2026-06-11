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
FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "").strip()
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
    out = {"revenue": None, "eps": None, "eps_basis": "", "guidance": "", "gross_margin": None, "highlights": [],
           "rev_yoy": None, "gm_dir": 0, "red_flags": [], "green_flags": [], "tone": 0.0}
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
            # YoY growth stated near the revenue figure ("increased 32% year-over-year",
            # "up 18% from", "a decrease of 7%") — sign-aware.
            win = txt[m.start(): m.start() + 240]
            g_up = re.search(r"(?:increas\w+|up|grew|growth|rose)\s+(?:of\s+|by\s+)?([\d.]+)\s*%", win, re.I)
            g_dn = re.search(r"(?:decreas\w+|down|declin\w+|fell)\s+(?:of\s+|by\s+)?([\d.]+)\s*%", win, re.I)
            try:
                if g_up and (not g_dn or g_up.start() < g_dn.start()):
                    out["rev_yoy"] = float(g_up.group(1))
                elif g_dn:
                    out["rev_yoy"] = -float(g_dn.group(1))
            except ValueError:
                pass
            break
    # actual reported EPS — prefer NON-GAAP diluted; guard checks the text BEFORE the
    # figure only (a trailing "compared to $X prior year" clause must not disqualify
    # the actual EPS that precedes it). Mirrors the worker exactly.
    cands = []

    def _is_guidance(idx, mlen):
        pre = txt[max(0, idx - 95): idx]
        post = txt[idx: idx + mlen + 20]
        return bool(re.search(r"we expect|in the range of|range of \$|\bguidance\b|financial outlook|we (?:are )?(?:now )?(?:reaffirm|raising|lowering|increasing)", pre, re.I) or re.search(r"\bto\s*\$\s?\d", post))

    def _is_prior(di):
        return bool(re.search(r"prior[\s-]year|year[\s-]ago|same period|compared (?:with|to)|a year earlier", txt[max(0, di - 70): di], re.I))

    def scan_eps(pattern, basis):
        for mm in re.finditer(pattern, txt, re.I):
            di = mm.start() + mm.group(0).rfind("$")
            if _is_guidance(mm.start(), len(mm.group(0))) or _is_prior(di):
                continue
            # EPS is a per-share figure — NEVER "billions"/"millions". If the captured
            # number is immediately followed by more digits (we truncated a bigger number
            # like $1.243B → "1.24") or by billion/million, it's net income, not EPS.
            tail = txt[mm.end(1): mm.end(1) + 14]
            if re.match(r"^\d", tail) or re.match(r"^\s*(?:billion|million|bn\b|mn\b)", tail, re.I):
                continue
            try:
                v = float(mm.group(1))
            except ValueError:
                continue
            if 0 < v < 1000:
                cands.append((v, basis))

    # "<basis> net income and (diluted) EPS were $X billion and $Y" → capture $Y (the
    # per-share number), not $X (net income). This is the form Medtronic/most large-caps use.
    scan_eps(r"non-?GAAP\s+(?:net\s+(?:income|loss)|earnings)\b[^.]{0,80}?\band\s+(?:non-?GAAP\s+)?(?:diluted\s+)?(?:EPS|earnings per share)[^.]{0,30}?\$\s?[\d.,]+\s*(?:billion|million)\s+and\s+\$\s?(\d+\.\d{1,2})", "non-GAAP")
    scan_eps(r"(?<!non-)GAAP\s+(?:net\s+(?:income|loss)|earnings)\b[^.]{0,80}?\band\s+(?:diluted\s+)?(?:EPS|earnings per share)[^.]{0,30}?\$\s?[\d.,]+\s*(?:billion|million)\s+and\s+\$\s?(\d+\.\d{1,2})", "GAAP")
    scan_eps(r"non-?GAAP\s+(?:diluted\s+)?(?:net income per share|earnings per share|EPS)\s+of\s+\$\s?(\d+\.\d{1,2})", "non-GAAP")
    scan_eps(r"non-?GAAP\s+net\s+(?:income|loss)\b[^.]{0,95}?\$\s?(\d+\.\d{2})\s+per\s+(?:diluted\s+)?(?:common\s+)?share", "non-GAAP")
    scan_eps(r"non-?GAAP[^.$%]{0,55}?(?:net income|earnings|EPS)\s+per\s+(?:diluted\s+)?(?:common\s+)?share[^$%\d]{0,18}\$\s?(\d+\.\d{1,2})", "non-GAAP")
    scan_eps(r"(?<!non-)GAAP\s+net\s+(?:income|loss)\b[^.]{0,95}?\$\s?(\d+\.\d{2})\s+per\s+(?:diluted\s+)?(?:common\s+)?share", "GAAP")
    scan_eps(r"(?:net income|earnings)\s+per\s+(?:diluted\s+)?(?:common\s+)?share[^$%\d]{0,18}\$\s?(\d+\.\d{1,2})", "GAAP")
    if cands:
        pick = next((c for c in cands if c[1] == "non-GAAP"), None) or cands[0]
        out["eps"], out["eps_basis"] = pick[0], pick[1]
    # guidance — must be a genuine FORWARD-looking statement (an actual guidance VERB), not
    # a results-section heading or dividend line (a bare period name appears in headings too).
    move = re.search(r"\b(rais\w+|lower\w+|reaffirm\w+|reiterat\w+|increas\w+|cut)\b[^.]{0,60}\b(guidance|outlook|forecast)\b", txt, re.I)
    VERB = r"(?:(?:currently\s+|now\s+)?(?:expects?|sees|anticipates?|projects?|forecasts?|estimates?|guides?|is guiding|reaffirms?|raises?|provides?(?:\s+its)?(?:\s+(?:guidance|outlook))?)|(?:financial\s+)?(?:guidance|outlook))"
    range_fwd = re.search(VERB + r"[^.]{0,160}?\$\s?[\d.,]+\s*(?:billion|million)?\s*(?:to|–|—|-|and)\s*\$?\s?[\d.,]+\s*(?:billion|million)?", txt, re.I)
    single = re.search(VERB + r"[^.]{0,130}?(?:non-?GAAP\s+)?(?:diluted\s+)?(?:EPS|earnings per share|revenue|net sales)[^.]{0,40}?\$\s?[\d.,]+\s*(?:billion|million)?", txt, re.I)
    s = (range_fwd.group(0) if range_fwd else (single.group(0) if single else (move.group(0) if move else "")))
    if s and re.search(r"\bdividend\b", s, re.I) and not re.search(r"(expects?|guidance|outlook|forecast|anticipat)", s, re.I):
        s = ""
    if s and re.search(r"financial results\b", s, re.I) and not re.search(r"(expects?|guidance|outlook|forecast|anticipat)", s, re.I):
        s = ""
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
                out["gm_dir"] = 1 if cur > prev else -1 if cur < prev else 0
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

    # ── RED FLAGS: the sentences that change a thesis. Each is checked for nearby
    #    negation ("no impairment", "not aware of") before it counts. ──
    def _flag(label, pattern):
        mm = re.search(pattern, txt, re.I)
        if not mm:
            return
        pre = txt[max(0, mm.start() - 18): mm.start()]
        if re.search(r"\b(?:no|not|without|nor)\s*$", pre, re.I):
            return
        out["red_flags"].append(label)
    _flag("Impairment / write-down", r"\bimpairment(?:s)?\b|\bwrite-?(?:down|off)s?\b|inventory (?:charge|reserve)")
    _flag("Restatement / material weakness", r"\brestat(?:e|ement|ing)\b|material weakness")
    _flag("Going concern", r"going concern|substantial doubt")
    _flag("Key customer loss", r"(?:loss|termination|cancellation) of (?:a |its )?(?:major|significant|key|largest) customer|customer concentration")
    _flag("Executive departure", r"\b(?:CFO|CEO|Chief (?:Financial|Executive|Operating) Officer)\b[^.]{0,50}\b(?:resign\w*|depart\w*|step(?:ping|s|ped)? down|transition\w*)\b")
    _flag("Restructuring / layoffs", r"workforce reduction|reduction in force|\blay-?offs?\b|restructuring (?:plan|charges|program)")
    _flag("Dilution / offering", r"(?:proposed|announced|commenced)[^.]{0,40}(?:public|secondary|equity) offering|convertible (?:senior )?notes offering|at-the-market (?:offering|program)")
    _flag("Covenant / liquidity stress", r"covenant (?:waiver|breach|violation|relief)|forbearance")
    _flag("Demand softness", r"(?:weaker|soft(?:er|ening)?|declin\w+|muted) (?:demand|bookings|orders)|pricing pressure|order push-?outs?")
    _flag("Delayed filing", r"unable to (?:timely )?file|late filing|\bNT 10-")
    out["red_flags"] = out["red_flags"][:4]

    # ── GREEN FLAGS: durable-positive structure beyond the headline beat ──
    def _green(label, pattern):
        if re.search(pattern, txt, re.I):
            out["green_flags"].append(label)
    _green("Record revenue", r"\brecord (?:quarterly |annual |fourth.quarter |full.year )?(?:revenue|net sales|sales)\b|all-time high")
    _green("Backlog / RPO growth", r"\b(?:backlog|remaining performance obligations?|RPO)\b[^.]{0,70}\b(?:grew|increas\w+|up|record|rose)\b")
    _green("Accelerating growth", r"accelerat\w+ (?:revenue |sales )?growth|growth accelerated")
    _green("Dividend increase", r"(?:increas\w+|rais\w+)[^.]{0,35}\bdividend\b")
    _green("Beat own guidance", r"(?:exceed\w+|above|surpass\w+) (?:the )?(?:high|top) end of (?:our|its) (?:guidance|outlook|range)")
    out["green_flags"] = out["green_flags"][:3]

    # ── MANAGEMENT TONE: crude but honest lexical read of the release language ──
    pos_w = len(re.findall(r"\b(?:pleased|proud|strong|record|momentum|robust|exceptional|outstanding|exceeded|strength|confident)\b", txt, re.I))
    neg_w = len(re.findall(r"\b(?:challenging|headwinds?|softness|cautious|difficult|disappointed|uncertainty|pressure|volatile|slowdown)\b", txt, re.I))
    out["tone"] = max(-1.0, min(1.0, (pos_w - neg_w) / 6.0))
    return out


# ── COMPOSITE READING SCORE: one number that weighs everything the release said ──
#    Base 50. EPS surprise ±30, revenue surprise ±20, guidance direction ±18,
#    margin direction ±6, red flags −10 each (cap −30), green flags +5 each (cap +15),
#    tone ±6, YoY growth bonus/penalty. Clamped 0–100, tiered for the alert headline.
def score_reading(eps_pct, rev_pct, guidance, gm_dir, red_flags, green_flags, tone, rev_yoy):
    s = 50.0
    conf = 0
    if eps_pct is not None:
        s += max(-15.0, min(15.0, eps_pct)) * 2.0; conf += 1
    if rev_pct is not None:
        s += max(-8.0, min(8.0, rev_pct)) * 2.5; conf += 1
    g = guidance or ""
    if " raised" in g:
        s += 14
    elif " cut" in g:
        s -= 18
    elif " reaffirmed" in g:
        s += 4
    elif g:
        s += 2
    s += 6 * (1 if gm_dir > 0 else -1 if gm_dir < 0 else 0)
    s -= min(30, 10 * len(red_flags or []))
    s += min(15, 5 * len(green_flags or []))
    s += 6.0 * (tone or 0.0)
    if rev_yoy is not None:
        s += 6 if rev_yoy >= 40 else 3 if rev_yoy >= 20 else (-8 if rev_yoy < 0 else 0)
    s = int(round(max(0, min(100, s))))
    tier = ("\U0001F7E2 STRONG" if s >= 72 else "✅ GOOD" if s >= 58 else
            "\U0001F7E1 MIXED" if s >= 45 else "❌ WEAK" if s >= 32 else "\U0001F534 BAD")
    if conf == 0:
        tier += " (no estimate basis)"
    return s, tier


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


def finnhub_eps_estimate(sym, ref_date):
    # Finnhub carries the proper Street consensus (FMP's calendar epsEstimated is often
    # wrong/wrong-period). Returns the consensus EPS estimate for the report period.
    if not FINNHUB_KEY:
        return None
    try:
        arr = json.loads(http_get("https://finnhub.io/api/v1/stock/earnings?symbol=%s&token=%s" % (sym, FINNHUB_KEY), {"User-Agent": BROWSER_UA}, timeout=15))
        if isinstance(arr, list) and arr:
            ref = dt.date.fromisoformat(ref_date) if ref_date else dt.date.today()
            rows = [x for x in arr if x.get("period")]
            if rows:
                best = min(rows, key=lambda x: abs((dt.date.fromisoformat(x["period"]) - ref).days))
                if best.get("estimate") is not None:
                    return float(best["estimate"])
    except Exception:
        pass
    return None


def build_verdict(eps, eps_est, rev, rev_est, eps_basis=""):
    eps_pct = ((eps - eps_est) / abs(eps_est) * 100) if (eps is not None and eps_est) else None
    rev_pct = ((rev - rev_est) / abs(rev_est) * 100) if (rev is not None and rev_est) else None
    # BASIS GUARD: estimates are non-GAAP. If the actual we parsed is GAAP, "vs est %" is
    # apples-to-oranges — show the number but suppress the EPS beat/miss verdict.
    eps_is_gaap = bool(eps_basis and re.search(r"\bGAAP\b", eps_basis) and not re.search(r"non-?GAAP", eps_basis, re.I))
    eps_mismatch = eps_is_gaap and eps_est is not None
    basis_tag = (" (%s, from release)" % eps_basis) if (eps is not None and eps_basis) else ""
    bits = []
    if eps is not None:
        est_txt = ((" vs $%.2f est%s" % (eps_est, " (non-GAAP)" if eps_mismatch else "")) if eps_est is not None else "")
        pct_txt = (" (%+.1f%%)" % eps_pct) if (eps_pct is not None and not eps_mismatch) else ""
        bits.append("EPS $%.2f%s%s%s" % (eps, est_txt, pct_txt, basis_tag))
    if rev is not None:
        bits.append("Rev %s%s%s" % (fmt_rev(rev), (" vs %s est" % fmt_rev(rev_est)) if rev_est is not None else "", (" (%+.1f%%)" % rev_pct) if rev_pct is not None else ""))
    if not bits:
        return "\U0001F4CB Actuals pending — see the filing.", eps_pct, rev_pct
    e = None if eps_mismatch else eps_pct          # only let EPS drive a verdict when basis lines up
    def _sgn(x):
        return 1 if x > 0.5 else -1 if x < -0.5 else 0
    if e is not None and rev_pct is not None:
        se, sr = _sgn(e), _sgn(rev_pct)
        v = "\U0001F7E2 Strong (double beat)" if (se >= 0 and sr >= 0 and se + sr > 0) else "\U0001F534 Weak (double miss)" if (se <= 0 and sr <= 0 and se + sr < 0) else "\U0001F7E1 Mixed"
    elif e is not None:
        v = "✅ EPS beat" if _sgn(e) > 0 else "❌ EPS miss" if _sgn(e) < 0 else "⚪ EPS in line"
    elif rev_pct is not None:
        v = "✅ Rev beat · EPS basis differs" if _sgn(rev_pct) > 0 else "❌ Rev miss · EPS basis differs" if _sgn(rev_pct) < 0 else "⚪ Rev in line"
    elif eps_mismatch:
        v = "\U0001F4CB Reported — GAAP EPS shown (non-GAAP vs est pending)"
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
    # Finnhub consensus is the authoritative estimate (FMP calendar est is unreliable)
    fn_est = finnhub_eps_estimate(tk, fdate)
    if fn_est is not None:
        eps_est = fn_est
    guidance = pr.get("guidance") or ""
    highlights = list(pr.get("highlights") or [])
    gross_margin = pr.get("gross_margin")
    ir = ""
    verdict, eps_pct, rev_pct = build_verdict(eps, eps_est, rev, rev_est, eps_basis)
    red_flags = list(pr.get("red_flags") or [])
    green_flags = list(pr.get("green_flags") or [])
    tone = pr.get("tone") or 0.0
    rev_yoy = pr.get("rev_yoy")
    score, tier = score_reading(eps_pct, rev_pct, guidance, pr.get("gm_dir") or 0, red_flags, green_flags, tone, rev_yoy)
    verdict = "%s %d/100 \u00b7 %s" % (tier, score, verdict)
    if rev_yoy is not None:
        verdict += " \u00b7 YoY %+.0f%%" % rev_yoy
    # flags lead the highlights — they are the thesis-relevant sentences
    flag_lines = ["⚠️ " + r for r in red_flags[:3]] + ["\U0001F331 " + gfl for gfl in green_flags[:2]]
    highlights = (flag_lines + highlights)[:5]
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
        "score": score, "redFlags": red_flags, "greenFlags": green_flags,
        "tone": round(tone, 2), "revYoY": rev_yoy,
    })

    state["seen"][tk] = acc
    if eps is None and rev is None:
        state.setdefault("fill", {})[tk] = {"acc": acc, "cik": cik, "date": fdate or "", "ts": time.time()}
    print("[ALERT] %s %s — %s" % (tk, fdate, verdict or "(no actuals yet)"))
    return True


_HB = {}


def backfill_pass(state):
    """Readings that went out before actuals were parseable get re-checked for up
    to 12 hours. When the press release or FMP finally yields the numbers, the
    structured reading is re-pushed so the desk's Earnings Readings show EPS."""
    fill = state.get("fill") or {}
    for tk in list(fill.keys()):
        info = fill[tk]
        if time.time() - info.get("ts", 0) > 12 * 3600:
            del fill[tk]
            continue
        try:
            pr = parse_pr(fetch_pr_text(info["cik"], info["acc"]))
            fmp = fmp_actuals(tk)
            eps = pr.get("eps")
            rev = pr.get("revenue")
            eps_est, rev_est = fmp.get("eps_est"), fmp.get("rev_est")
            fn_est = finnhub_eps_estimate(tk, info.get("date"))
            if fn_est is not None:
                eps_est = fn_est
            if eps is None and rev is None:
                continue
            verdict, eps_pct, rev_pct = build_verdict(eps, eps_est, rev, rev_est, pr.get("eps_basis") or "")
            score, tier = score_reading(eps_pct, rev_pct, pr.get("guidance") or "", pr.get("gm_dir") or 0,
                                        pr.get("red_flags") or [], pr.get("green_flags") or [],
                                        pr.get("tone") or 0.0, pr.get("rev_yoy"))
            verdict = "%s %d/100 \u00b7 %s" % (tier, score, verdict)
            push_ingest({
                "ticker": tk, "date": info.get("date") or "", "verdict": verdict,
                "eps": eps, "epsEst": eps_est,
                "epsPct": round(eps_pct, 1) if eps_pct is not None else None,
                "rev": rev, "revEst": rev_est,
                "revPct": round(rev_pct, 1) if rev_pct is not None else None,
                "grossMargin": pr.get("gross_margin"), "guidance": pr.get("guidance") or "",
                "highlights": pr.get("highlights") or [],
                "score": score, "redFlags": pr.get("red_flags") or [], "greenFlags": pr.get("green_flags") or [],
                "tone": round(pr.get("tone") or 0.0, 2), "revYoY": pr.get("rev_yoy"),
            })
            print("[backfill] %s — actuals filled (%s)" % (tk, verdict[:60]))
            del fill[tk]
        except Exception as exc:
            print("[backfill] %s retry failed: %s" % (tk, exc))


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
    try:
        backfill_pass(state)
    except Exception as exc:
        print("[warn] backfill:", exc)
    save_state(state)
    # heartbeat → worker, so the pipeline health monitor can see the sentinel is alive
    # (throttled to one ping per ~15 min in loop mode; --once runs ping every run)
    try:
        base = worker_base()
        now = time.time()
        if base and INGEST_KEY and now - _HB.get("t", 0) > 900:
            _HB["t"] = now
            http_post(base + "/health-ping", {"src": "sentinel"}, headers={"X-Ingest-Key": INGEST_KEY})
    except Exception:
        pass
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
