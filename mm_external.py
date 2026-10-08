"""MAUDE Monitor V4 — external context (SEC EDGAR, ClinicalTrials.gov, payer).

One EDGAR submissions pull per ticker per run (V3 fetched the identical
JSON twice, once for filings and once for Form 4s). SEC asks for a real
contact in the User-Agent: set EDGAR_CONTACT in the environment.
"""

import json
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import quote

from mm_config import CIK_MAP, CT_SPONSOR_MAP, PAYER_NOTES, PRIVATE_TICKERS, EDGAR_CONTACT

_edgar_cache = {}


def edgar_summary(ticker):
    if ticker in PRIVATE_TICKERS:
        return {"status": "skip", "message": "Private company"}
    if ticker in _edgar_cache:
        return _edgar_cache[ticker]
    cik = CIK_MAP.get(ticker)
    if not cik:
        return {"status": "no_cik"}
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req = Request(url, headers={"User-Agent": f"MAUDE-Monitor/4.0 {EDGAR_CONTACT}"})
        with urlopen(req, timeout=15) as resp:
            f = json.loads(resp.read().decode())
        rc = f.get("filings", {}).get("recent", {})
        forms = rc.get("form", [])
        dates = rc.get("filingDate", [])
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        l90 = [(fm, d) for fm, d in zip(forms, dates) if d >= cutoff]
        fc = {}
        for fm, _ in l90:
            fc[fm] = fc.get(fm, 0) + 1
        form4 = fc.get("4", 0) + fc.get("4/A", 0)
        result = {"status": "ok", "total_90d": len(l90), "form_counts": fc,
                  "eight_k_count": fc.get("8-K", 0), "form4_count_90d": form4,
                  "message": f"{len(l90)} filings in 90d "
                             f"({fc.get('8-K', 0)} 8-Ks, {form4} Form 4s)"}
    except Exception as e:
        result = {"status": "error", "message": str(e)[:100]}
    _edgar_cache[ticker] = result
    return result


def clinical_trials(ticker):
    sp = CT_SPONSOR_MAP.get(ticker, ticker)
    try:
        url = ("https://clinicaltrials.gov/api/v2/studies?"
               f"query.spons={quote(sp)}"
               "&filter.overallStatus=RECRUITING,NOT_YET_RECRUITING,ACTIVE_NOT_RECRUITING"
               "&pageSize=20")
        req = Request(url, headers={"User-Agent": "MAUDE-Monitor/4.0"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        n = len(data.get("studies", []))
        return {"status": "ok", "count": n,
                "message": f"{n} active trials (sponsor: {sp})"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:100]}


def payer_note(ticker):
    return {"status": "ok", "message": PAYER_NOTES.get(ticker, "Unknown")}
