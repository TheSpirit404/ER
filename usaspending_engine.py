#!/usr/bin/env python3
"""
LAI Desk · Trump's Watch — Administration Capital Flows engine
==============================================================
Pulls recent large federal awards (contracts + IDVs) from the official, free
USAspending.gov Advanced Search API, maps recipient corporate names to stock
tickers, computes a relative "Revenue Impact Score", and dispatches a clean
webhook alert for every fresh mapped win.

Run it on any scheduler (cron, GitHub Actions, a small VM, Replit, etc.):
    EXISTING_WEBHOOK_URL="https://ntfy.sh/your-topic" python3 usaspending_engine.py

Environment variables
---------------------
    EXISTING_WEBHOOK_URL   (required)  chat/push gateway endpoint (ntfy/Discord/Slack)
    MIN_AWARD              (optional)  institutional floor in USD (default 5_000_000)
    LOOKBACK_DAYS          (optional)  how far back to scan (default 90)
    STATE_FILE             (optional)  dedupe store path (default ./capital_flows_state.json)
    OUTPUT_FILE            (optional)  latest results JSON for the dashboard (default ./capital_flows.json)
    INGEST_URL             (optional)  worker ingest endpoint to feed the dashboard,
                                       e.g. https://lai-yahoo-proxy.<sub>.workers.dev/usaspending-ingest
    INGEST_KEY             (optional)  shared secret matching the worker's INGEST_KEY var

Why Python? Cloudflare Workers usually can't reach api.usaspending.gov (edge-to-edge
TLS block). This script runs OUTSIDE Cloudflare, fetches cleanly, fires the webhook
alerts, and (optionally) pushes the results to the worker so the dashboard's
"Administration Capital Flows" tab shows live data.
"""

import os
import json
import re
import datetime as dt
from urllib import request, error

USASPENDING_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

MIN_AWARD = float(os.environ.get("MIN_AWARD", "5000000"))          # institutional floor: $5M
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "90"))
WEBHOOK_URL = os.environ.get("EXISTING_WEBHOOK_URL", "").strip()
STATE_FILE = os.environ.get("STATE_FILE", "capital_flows_state.json")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "capital_flows.json")
INGEST_URL = os.environ.get("INGEST_URL", "").strip()
INGEST_KEY = os.environ.get("INGEST_KEY", "").strip()

# ── Recipient corporate name → (ticker, annual revenue USD) ──────────────────
# Keyword (cleaned, uppercase) is matched as a substring of the cleaned name.
# Annual revenue powers the Revenue Impact Score (award / average daily revenue).
TICKER_MAP = [
    ("LOCKHEED MARTIN", "LMT", 71_000_000_000),
    ("GENERAL DYNAMICS", "GD", 42_300_000_000),
    ("RAYTHEON", "RTX", 68_800_000_000),
    ("RTX", "RTX", 68_800_000_000),
    ("NORTHROP GRUMMAN", "NOC", 39_000_000_000),
    ("BOEING", "BA", 77_800_000_000),
    ("L3HARRIS", "LHX", 21_300_000_000),
    ("L3 HARRIS", "LHX", 21_300_000_000),
    ("HARRIS CORP", "LHX", 21_300_000_000),
    ("LEIDOS", "LDOS", 16_700_000_000),
    ("PALANTIR", "PLTR", 2_900_000_000),
    ("CACI", "CACI", 7_700_000_000),
    ("SCIENCE APPLICATIONS", "SAIC", 7_400_000_000),
    ("SAIC", "SAIC", 7_400_000_000),
    ("BOOZ ALLEN", "BAH", 11_000_000_000),
    ("HUNTINGTON INGALLS", "HII", 11_500_000_000),
    ("MICROSOFT", "MSFT", 245_000_000_000),
    ("AMAZON", "AMZN", 620_000_000_000),
    ("GOOGLE", "GOOGL", 350_000_000_000),
    ("ALPHABET", "GOOGL", 350_000_000_000),
    ("ORACLE", "ORCL", 53_000_000_000),
    ("NVIDIA", "NVDA", 130_000_000_000),
    ("INTERNATIONAL BUSINESS MACHINES", "IBM", 62_000_000_000),
    ("IBM", "IBM", 62_000_000_000),
    ("INTEL", "INTC", 54_000_000_000),
    ("ACCENTURE", "ACN", 65_000_000_000),
    ("DELL", "DELL", 88_000_000_000),
    ("HEWLETT PACKARD ENTERPRISE", "HPE", 30_000_000_000),
    ("KRATOS", "KTOS", 1_100_000_000),
    ("AEROVIRONMENT", "AVAV", 790_000_000),
    ("ROCKET LAB", "RKLB", 440_000_000),
    ("GENERAL ELECTRIC", "GE", 68_000_000_000),
    ("GE AEROSPACE", "GE", 68_000_000_000),
    ("HONEYWELL", "HON", 37_000_000_000),
    ("CISCO", "CSCO", 54_000_000_000),
]

AGENCY_CODES = [
    (re.compile(r"defense|army|navy|air force|marine|defense logistics", re.I), "DoD"),
    (re.compile(r"homeland", re.I), "DHS"),
    (re.compile(r"energy", re.I), "DOE"),
    (re.compile(r"health|human services|\bnih\b|\bcdc\b", re.I), "HHS"),
    (re.compile(r"veterans", re.I), "VA"),
    (re.compile(r"nasa|aeronautics", re.I), "NASA"),
    (re.compile(r"\bstate\b", re.I), "STATE"),
    (re.compile(r"justice", re.I), "DOJ"),
    (re.compile(r"treasury", re.I), "TREAS"),
    (re.compile(r"transportation", re.I), "DOT"),
    (re.compile(r"general services", re.I), "GSA"),
    (re.compile(r"agriculture", re.I), "USDA"),
    (re.compile(r"commerce", re.I), "DOC"),
]


def clean_name(s: str) -> str:
    """Normalize a messy federal recipient name for dictionary lookup."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())).strip()


def map_ticker(name: str):
    """Return (ticker, annual_revenue) or None for an unmapped recipient."""
    cleaned = clean_name(name)
    if not cleaned:
        return None
    for keyword, ticker, annual_rev in TICKER_MAP:
        if keyword in cleaned:
            return ticker, annual_rev
    return None


def agency_code(name: str) -> str:
    for pattern, code in AGENCY_CODES:
        if pattern.search(name or ""):
            return code
    first = (name or "FED").split()
    return (first[0] if first else "FED")[:6].upper()


def revenue_impact_score(award_value: float, annual_revenue: float):
    """Impact = Contract Award Value / Average Daily Revenue of the company."""
    if not annual_revenue or not award_value:
        return None
    avg_daily_revenue = annual_revenue / 365.0
    return round(award_value / avg_daily_revenue, 1)   # award expressed in "days of revenue"


# ── USAspending fetch ────────────────────────────────────────────────────────
def _search(award_type_codes):
    """One USAspending spending_by_award POST for a single award-type group.
    Contract codes and IDV codes must NOT be mixed in one request (→ HTTP 422)."""
    today = dt.date.today()
    start = today - dt.timedelta(days=LOOKBACK_DAYS)
    payload = {
        "filters": {
            "award_type_codes": award_type_codes,
            "time_period": [{"start_date": start.isoformat(), "end_date": today.isoformat()}],
            "award_amounts": [{"lower_bound": MIN_AWARD}],
        },
        "fields": [
            "Award ID", "Recipient Name", "Award Amount", "Awarding Agency",
            "Awarding Sub Agency", "Description", "Start Date", "Award Type",
        ],
        "page": 1,
        "limit": 100,
        "sort": "Award Amount",
        "order": "desc",
    }
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        USASPENDING_URL,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json",
                 "User-Agent": "LAIDesk/1.0 (research)"},
        method="POST",
    )
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8")).get("results", [])


def fetch_awards():
    """Prime contracts (A-D) + IDVs (separate request, merged). IDVs are best-effort."""
    rows = _search(["A", "B", "C", "D"])
    try:
        rows += _search(["IDV_A", "IDV_B", "IDV_C", "IDV_D", "IDV_E"])
    except Exception as exc:
        print(f"[info] IDV fetch skipped: {exc}")
    return rows


# ── Webhook dispatcher (robust; never crashes the parse loop) ────────────────
def dispatch_alert(award):
    """Post a single cleanly-formatted alert. Wrapped so a flaky endpoint can't
    take down the engine's core USAspending parsing loop."""
    if not WEBHOOK_URL:
        return False
    text = (
        "🚨 LAI DESK · TRUMP'S WATCH ALERT 🚨\n"
        f"Ticker: ${award['ticker']}\n"
        f"Date: {award.get('date') or 'n/a'}\n"
        f"Agency: {award['agency']}\n"
        f"Allocation: ${award['award'] / 1e6:.1f}M\n"
        f"Description: {award['description']}\n"
        f"Calculated Revenue Impact Score: {award['impact'] if award['impact'] is not None else 'n/a'}"
    )
    try:
        url = WEBHOOK_URL
        if "ntfy.sh" in url:
            data, headers = text.encode("utf-8"), {
                "Title": f"LAI · {award['ticker']} Federal Award",
                "Tags": "rotating_light,moneybag",
                "Content-Type": "text/plain; charset=utf-8",
            }
        elif "hooks.slack.com" in url:
            data, headers = json.dumps({"text": text}).encode("utf-8"), {"Content-Type": "application/json"}
        elif "discord" in url:
            data, headers = json.dumps({"content": text[:1900]}).encode("utf-8"), {"Content-Type": "application/json"}
        else:
            data, headers = json.dumps({"text": text, "content": text}).encode("utf-8"), {"Content-Type": "application/json"}
        # Discord/Cloudflare reject the default urllib UA with 403 — send a real one.
        headers["User-Agent"] = "Mozilla/5.0 (compatible; LAIDesk/1.0)"
        req = request.Request(url, data=data, headers=headers, method="POST")
        with request.urlopen(req, timeout=15) as resp:
            return 200 <= resp.status < 300
    except (error.URLError, error.HTTPError, TimeoutError, OSError) as exc:
        print(f"[warn] webhook dispatch failed (continuing): {exc}")
        return False
    except Exception as exc:  # defensive: nothing here should kill the loop
        print(f"[warn] webhook dispatch unexpected error (continuing): {exc}")
        return False


def push_to_worker(results):
    """Feed the dashboard: POST the computed awards to the worker's KV ingest.
    Wrapped so a failure never crashes the engine."""
    if not INGEST_URL or not INGEST_KEY:
        return False
    try:
        sep = "&" if "?" in INGEST_URL else "?"
        url = f"{INGEST_URL}{sep}key={INGEST_KEY}"
        data = json.dumps({"awards": results}).encode("utf-8")
        req = request.Request(url, data=data, headers={"Content-Type": "application/json", "x-ingest-key": INGEST_KEY, "User-Agent": "Mozilla/5.0 (compatible; LAIDesk/1.0)"}, method="POST")
        with request.urlopen(req, timeout=15) as resp:
            ok = 200 <= resp.status < 300
            print(f"[ok] pushed {len(results)} awards to dashboard worker." if ok else f"[warn] ingest returned {resp.status}")
            return ok
    except Exception as exc:
        print(f"[warn] dashboard ingest failed (continuing): {exc}")
        return False


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {"alerted": {}}


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
    except OSError as exc:
        print(f"[warn] could not persist state: {exc}")


# ── Core loop ────────────────────────────────────────────────────────────────
def run():
    if not WEBHOOK_URL:
        print("[info] EXISTING_WEBHOOK_URL not set — alerts disabled (data will still be written).")
    state = load_state()
    alerted = state.get("alerted", {})

    try:
        rows = fetch_awards()
    except Exception as exc:
        print(f"[error] USAspending fetch failed: {exc}")
        return

    results = []
    new_alerts = 0
    for row in rows:
        award_value = float(row.get("Award Amount") or 0)
        if award_value < MIN_AWARD:                 # institutional filtering floor
            continue

        recipient = row.get("Recipient Name") or ""
        agency = row.get("Awarding Agency") or row.get("Awarding Sub Agency") or ""
        description = (row.get("Description") or "").strip()[:160]
        award_id = str(row.get("Award ID") or row.get("generated_internal_id") or f"{recipient}-{award_value}")

        mapping = map_ticker(recipient)
        ticker = mapping[0] if mapping else None
        impact = revenue_impact_score(award_value, mapping[1]) if mapping else None

        record = {
            "id": award_id,
            "ticker": ticker,
            "recipient": recipient,
            "agency": agency,
            "agencyCode": agency_code(agency),
            "award": award_value,
            "description": description,
            "date": row.get("Start Date") or "",
            "impact": impact,
        }
        results.append(record)

        # The moment a fresh award clears the filter AND matches a ticker → alert.
        # Only mark as alerted when the send SUCCEEDS (or alerts are disabled), so a
        # transient 403/timeout is retried on the next run instead of lost.
        if ticker and award_id not in alerted:
            if dispatch_alert(record):
                new_alerts += 1
                alerted[award_id] = dt.datetime.utcnow().isoformat()
            elif not WEBHOOK_URL:
                alerted[award_id] = dt.datetime.utcnow().isoformat()

    # mapped wins first, then by value (for the dashboard table)
    results.sort(key=lambda r: (0 if r["ticker"] else 1, -r["award"]))

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
            json.dump({"awards": results, "at": int(dt.datetime.utcnow().timestamp() * 1000)}, fh)
    except OSError as exc:
        print(f"[warn] could not write output file: {exc}")

    # Feed the dashboard's "Administration Capital Flows" tab (optional).
    push_to_worker(results)

    state["alerted"] = alerted
    save_state(state)
    mapped = sum(1 for r in results if r["ticker"])
    print(f"[ok] processed {len(results)} awards ≥ ${MIN_AWARD:,.0f} · {mapped} mapped to tickers · {new_alerts} new alerts dispatched.")


if __name__ == "__main__":
    run()
