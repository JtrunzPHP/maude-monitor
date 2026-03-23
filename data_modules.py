"""
data_modules.py V4 — Only modules with REAL working data sources
================================================================
WORKING APIs (verified March 2026):
1. MAUDE Text NLP (api.fda.gov — mdr_text field)
2. SEC EDGAR Form 4 Insider Trading (data.sec.gov/submissions)
3. SEC EDGAR 10-Q/10-K Quality NLP (data.sec.gov/submissions)
4. ClinicalTrials.gov Pipeline (clinicaltrials.gov/api/v2)

COMPUTED (from our own data, no external API needed):
5. Recall Probability Model
6. Peer-Relative Scoring
7. Earnings Surprise Predictor
8. R-Score Backtest Engine

REMOVED (unreliable/no API):
- Google Trends (blocked from datacenter IPs)
- Short Interest (Yahoo blocks datacenter IPs)
- CMS Payer (no structured API)
- MHRA/International (no structured API)
- Reddit (blocked from datacenter IPs)
"""
import json, time, re, math
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import quote as url_quote

# SEC requires email in User-Agent
SEC_UA = "MAUDE-Monitor/3.1 research@parkmanhp.com"

def _fetch_json(url, ua=SEC_UA, timeout=20):
    """Fetch JSON with proper error reporting."""
    try:
        req = Request(url, headers={"User-Agent": ua, "Accept": "application/json"})
        with urlopen(req, timeout=timeout) as r:
            return {"_ok": True, "_data": json.loads(r.read())}
    except Exception as e:
        return {"_ok": False, "_error": str(e)[:150]}

def _fetch_text(url, ua=SEC_UA, timeout=20):
    """Fetch text/HTML content."""
    try:
        req = Request(url, headers={"User-Agent": ua})
        with urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="ignore")
    except:
        return ""

# CIK numbers (10-digit with leading zeros) from SEC EDGAR
CIKS = {
    "DXCM": "0001093557",
    "PODD": "0001145197",
    "TNDM": "0001438133",
    "ABT_LIBRE": "0000001800",
    "BBNX": "0001674632",
    "MDT_DM": "0001613103",
}

# ============================================================
# 1. MAUDE TEXT NLP — FAILURE MODE CLASSIFICATION
# ============================================================
FAILURE_MODES = {
    "sensor_accuracy": {
        "primary": ["inaccurate", "false reading", "wrong reading", "reading was off",
                     "showed low when high", "showed high when low", "not matching fingerstick"],
        "secondary": ["accuracy", "finger stick", "calibration", "mard"],
        "desc": "Sensor giving wrong glucose values — can cause wrong insulin dosing. HIGH severity.",
        "recall_risk": 0.8,
    },
    "alert_failure": {
        "primary": ["no alert", "missed alert", "no alarm", "did not alert",
                     "alarm did not sound", "speaker", "no notification", "silent"],
        "secondary": ["notification", "vibration", "urgent low", "alert not received"],
        "desc": "Device failed to warn user of dangerous glucose. Linked to deaths. CRITICAL severity.",
        "recall_risk": 0.95,
    },
    "insulin_delivery": {
        "primary": ["occlusion", "no delivery", "blockage", "under-delivery", "over-delivery",
                     "insulin not delivered", "pump failure", "air bubble"],
        "secondary": ["infusion", "bolus", "basal", "cannula", "tubing", "pod failure"],
        "desc": "Pump delivery failure — can cause DKA or severe hypo. VERY HIGH severity.",
        "recall_risk": 0.7,
    },
    "connectivity": {
        "primary": ["bluetooth", "lost connection", "no signal", "pairing failed",
                     "communication error", "disconnected", "won't connect"],
        "secondary": ["sync", "phone", "app connection", "receiver connection"],
        "desc": "Wireless connectivity failure between sensor and phone/receiver.",
        "recall_risk": 0.3,
    },
    "sensor_early_end": {
        "primary": ["sensor failed", "expired early", "terminated early", "sensor error",
                     "warm-up failed", "no readings", "sensor stopped"],
        "secondary": ["replace sensor", "sensor lasted", "only lasted"],
        "desc": "Sensor stopped before labeled wear period ended.",
        "recall_risk": 0.4,
    },
    "adhesive_skin": {
        "primary": ["adhesive", "fell off", "peeling", "skin irritation", "rash",
                     "allergic reaction", "dermatitis", "blistered"],
        "secondary": ["tape", "itching", "swelling", "redness"],
        "desc": "Adhesive failure or skin reaction. Affects compliance. MEDIUM severity.",
        "recall_risk": 0.2,
    },
    "software_app": {
        "primary": ["app crash", "software error", "display error", "screen blank",
                     "update broke", "firmware", "app not working"],
        "secondary": ["software", "update", "app", "glitch", "bug"],
        "desc": "App or firmware malfunction.",
        "recall_risk": 0.5,
    },
}

def analyze_failure_modes(search_query, start="20250101", limit=100):
    """Fetch MAUDE report texts and classify by failure mode."""
    end = datetime.now().strftime("%Y%m%d")
    url = (f"https://api.fda.gov/device/event.json?"
           f"search={url_quote(search_query, safe='+:\"[]')}+AND+date_received:[{start}+TO+{end}]"
           f"&limit={limit}")
    result = _fetch_json(url, ua="MAUDE-Monitor/3.1")
    if not result["_ok"]:
        return {"status": "error", "message": f"openFDA API error: {result['_error']}",
                "total_analyzed": 0, "modes": {}}
    records = result["_data"].get("results", [])
    if not records:
        return {"status": "no_results", "message": "No MAUDE records returned for this search query.",
                "total_analyzed": 0, "modes": {}}
    # Extract text from mdr_text field
    texts = []
    for r in records:
        mdr_texts = r.get("mdr_text", [])
        combined = " ".join(t.get("text", "") for t in mdr_texts).lower().strip()
        if len(combined) > 20:  # Skip very short/empty texts
            texts.append(combined[:2000])
    if not texts:
        return {"status": "no_text",
                "message": f"Fetched {len(records)} MAUDE records but {len(records) - len(texts)} had no narrative text. "
                           f"This is common — many records have blank mdr_text fields in the openFDA API.",
                "total_analyzed": 0, "total_fetched": len(records), "modes": {}}
    # Classify each text
    mode_counts = {cat: 0 for cat in FAILURE_MODES}
    unclassified = 0
    for text in texts:
        scores = {}
        for cat, cfg in FAILURE_MODES.items():
            s = sum(2 for kw in cfg["primary"] if kw in text) + sum(1 for kw in cfg["secondary"] if kw in text)
            if s > 0:
                scores[cat] = s
        if not scores:
            unclassified += 1
        else:
            best = max(scores, key=scores.get)
            mode_counts[best] += 1
    total = len(texts)
    modes = {}
    for cat, count in sorted(mode_counts.items(), key=lambda x: -x[1]):
        if count > 0:
            modes[cat] = {
                "count": count, "pct": round(count / total * 100, 1),
                "label": cat.replace("_", " ").title(),
                "desc": FAILURE_MODES[cat]["desc"],
                "recall_risk": FAILURE_MODES[cat]["recall_risk"],
            }
    return {
        "status": "ok", "total_analyzed": total, "total_fetched": len(records),
        "unclassified": unclassified, "modes": modes,
        "message": f"Analyzed {total} reports with narrative text (out of {len(records)} fetched). "
                   f"{unclassified} could not be classified. "
                   f"Top mode: {list(modes.keys())[0].replace('_', ' ').title() if modes else 'N/A'}.",
    }


# ============================================================
# 2. SEC EDGAR FORM 4 — INSIDER TRADING
# ============================================================
def analyze_insider_trading(ticker):
    """Fetch Form 4 insider trading filings from SEC EDGAR submissions API."""
    cik = CIKS.get(ticker)
    if not cik:
        return {"status": "no_cik", "message": f"No SEC CIK configured for ticker {ticker}."}
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    result = _fetch_json(url)
    if not result["_ok"]:
        return {"status": "error", "message": f"SEC EDGAR API error: {result['_error']}"}
    data = result["_data"]
    company_name = data.get("name", ticker)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    reporters = recent.get("reportingOwner", []) if "reportingOwner" in recent else [None] * len(forms)
    # Find Form 4 filings in last 90 days
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    form4s = []
    for i in range(min(len(forms), 500)):
        if forms[i] in ("4", "4/A") and i < len(dates) and dates[i] >= cutoff:
            form4s.append({"date": dates[i], "form": forms[i]})
    total = len(form4s)
    # Also count Form 3 (initial ownership) and Form 5 (annual)
    form3s = sum(1 for i in range(min(len(forms), 500)) if forms[i] in ("3", "3/A") and i < len(dates) and dates[i] >= cutoff)
    form5s = sum(1 for i in range(min(len(forms), 500)) if forms[i] in ("5", "5/A") and i < len(dates) and dates[i] >= cutoff)
    # Interpret
    if total == 0:
        signal = "quiet"
        msg = f"No Form 4 insider transactions for {company_name} in the last 90 days. Insider activity is quiet."
    elif total > 15:
        signal = "very_high"
        msg = (f"{total} Form 4 filings for {company_name} in 90 days — VERY HIGH insider activity. "
               f"This level typically indicates significant insider selling. "
               f"Cross-reference with R-Score: if both are elevated, this is a strong conviction signal.")
    elif total > 8:
        signal = "high"
        msg = (f"{total} Form 4 filings for {company_name} in 90 days — HIGH activity. "
               f"Could be routine RSU vesting or could indicate insider concern. Check transaction types.")
    elif total > 3:
        signal = "moderate"
        msg = f"{total} Form 4 filings for {company_name} in 90 days — moderate insider activity."
    else:
        signal = "low"
        msg = f"{total} Form 4 filings for {company_name} in 90 days — low/normal insider activity."
    if form3s > 0:
        msg += f" Also: {form3s} Form 3 (new insider) filings."
    return {
        "status": "ok", "company": company_name,
        "form4_count": total, "form3_count": form3s, "form5_count": form5s,
        "signal": signal, "period": "90 days",
        "recent_filings": form4s[:10],
        "message": msg,
    }


# ============================================================
# 3. SEC EDGAR 10-Q/10-K NLP — QUALITY LANGUAGE
# ============================================================
QUALITY_KEYWORDS = {
    "high_severity": ["recall", "warning letter", "class i recall", "class ii recall",
                      "consent decree", "fda warning", "483 observation", "corrective action",
                      "field safety corrective action", "product liability claim",
                      "class action", "securities litigation"],
    "medium_severity": ["product quality", "manufacturing defect", "adverse event",
                        "complaint rate", "complaint volume", "warranty cost", "warranty expense",
                        "replacement cost", "product return", "litigation", "lawsuit",
                        "regulatory proceeding", "investigation"],
    "low_severity": ["quality assurance", "quality control", "inspection",
                     "regulatory compliance", "post-market surveillance"],
}

def analyze_edgar_filings(ticker):
    """Fetch and analyze recent SEC filings for quality-related language."""
    cik = CIKS.get(ticker)
    if not cik:
        return {"status": "no_cik", "message": f"No SEC CIK for {ticker}."}
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    result = _fetch_json(url)
    if not result["_ok"]:
        return {"status": "error", "message": f"EDGAR error: {result['_error']}"}
    data = result["_data"]
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    # Find most recent 10-Q and 10-K filings
    filings = []
    for i in range(min(len(forms), 200)):
        if forms[i] in ("10-Q", "10-K", "10-Q/A", "10-K/A") and len(filings) < 3:
            acc_clean = accessions[i].replace("-", "")
            cik_clean = cik.lstrip("0") or "0"
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_clean}/{primary_docs[i]}"
            filings.append({"date": dates[i], "form": forms[i], "url": doc_url})
    if not filings:
        return {"status": "no_filings", "message": "No 10-Q or 10-K filings found in recent submissions."}
    # Analyze each filing's text
    results = []
    for f in filings:
        time.sleep(0.3)  # SEC rate limit: 10 req/sec
        html = _fetch_text(f["url"])
        if not html or len(html) < 1000:
            continue
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', html).lower()
        text = re.sub(r'\s+', ' ', text)[:100000]
        # Score keywords
        high_hits = [kw for kw in QUALITY_KEYWORDS["high_severity"] if kw in text]
        med_hits = [kw for kw in QUALITY_KEYWORDS["medium_severity"] if kw in text]
        low_hits = [kw for kw in QUALITY_KEYWORDS["low_severity"] if kw in text]
        score = len(high_hits) * 5 + len(med_hits) * 2 + len(low_hits) * 1
        results.append({
            "date": f["date"], "form": f["form"], "score": score,
            "high_matches": high_hits, "medium_matches": med_hits[:5],
            "total_matches": len(high_hits) + len(med_hits) + len(low_hits),
        })
    if not results:
        return {"status": "error", "message": "Could not download or parse any filing text."}
    # Trend analysis
    if len(results) >= 2:
        if results[0]["score"] > results[-1]["score"] * 1.3:
            trend = "INCREASING"
        elif results[0]["score"] < results[-1]["score"] * 0.7:
            trend = "DECREASING"
        else:
            trend = "STABLE"
    else:
        trend = "SINGLE FILING"
    latest = results[0]
    msg = f'Most recent {latest["form"]} ({latest["date"]}): quality language score {latest["score"]}. '
    if latest["high_matches"]:
        msg += f'HIGH-SEVERITY matches: {", ".join(latest["high_matches"][:5])}. '
    msg += f'Trend across {len(results)} filings: {trend}.'
    if trend == "INCREASING":
        msg += " Quality/risk language is INCREASING — management may be preparing the market for bad news."
    return {
        "status": "ok", "filings": results, "trend": trend, "message": msg,
    }


# ============================================================
# 4. CLINICALTRIALS.GOV — COMPETITIVE PIPELINE
# ============================================================
TRIAL_SEARCHES = {
    "DXCM": {"cond": "diabetes", "intr": "dexcom continuous glucose monitor"},
    "PODD": {"cond": "diabetes", "intr": "omnipod insulin pump"},
    "TNDM": {"cond": "diabetes", "intr": "tandem insulin pump"},
    "ABT_LIBRE": {"cond": "diabetes", "intr": "freestyle libre"},
    "BBNX": {"cond": "type 1 diabetes", "intr": "bionic pancreas"},
    "MDT_DM": {"cond": "diabetes", "intr": "minimed insulin pump"},
    "SQEL": {"cond": "type 1 diabetes", "intr": "twiist"},
}

def analyze_clinical_trials(ticker):
    """Search ClinicalTrials.gov v2 API for active/recruiting trials."""
    search = TRIAL_SEARCHES.get(ticker)
    if not search:
        return {"status": "no_query", "message": f"No trial search configured for {ticker}."}
    # Build URL per v2 API spec
    params = f"query.cond={url_quote(search['cond'])}&query.intr={url_quote(search['intr'])}"
    params += "&filter.overallStatus=RECRUITING|NOT_YET_RECRUITING|ACTIVE_NOT_RECRUITING"
    params += "&pageSize=10&format=json"
    url = f"https://clinicaltrials.gov/api/v2/studies?{params}"
    result = _fetch_json(url, ua="MAUDE-Monitor/3.1")
    if not result["_ok"]:
        return {"status": "error", "message": f"ClinicalTrials.gov API error: {result['_error']}"}
    data = result["_data"]
    studies = data.get("studies", [])
    total = data.get("totalCount", len(studies))
    if not studies:
        return {"status": "no_trials", "total": 0,
                "message": f"No active/recruiting trials found matching '{search['intr']}'. "
                           f"This could mean the product is mature with no active development, "
                           f"or the search terms need refinement."}
    trials = []
    for s in studies[:8]:
        proto = s.get("protocolSection", {})
        ident = proto.get("identificationModule", {})
        status = proto.get("statusModule", {})
        design = proto.get("designModule", {})
        sponsor = proto.get("sponsorCollaboratorsModule", {})
        lead = sponsor.get("leadSponsor", {})
        trials.append({
            "nct_id": ident.get("nctId", ""),
            "title": ident.get("briefTitle", "")[:150],
            "status": status.get("overallStatus", ""),
            "phase": ",".join(design.get("phases", [])) if design.get("phases") else "N/A",
            "sponsor": lead.get("name", "")[:60],
            "last_update": status.get("lastUpdatePostDateStruct", {}).get("date", ""),
        })
    msg = f"{total} active/recruiting trials found. "
    if total > 5:
        msg += "Active pipeline — significant development activity. "
    msg += "New competitor trials = future competitive threat. Paused/terminated trials = potential quality signal."
    return {
        "status": "ok", "total": total, "trials": trials, "message": msg,
    }


# ============================================================
# 5. RECALL PROBABILITY MODEL (computed)
# ============================================================
def compute_recall_probability(failure_modes_data, stats_list):
    """Estimate 6-month recall probability from failure modes and trends."""
    if not stats_list or len(stats_list) < 6:
        return {"status": "insufficient_data", "probability": None,
                "message": "Need 6+ months of MAUDE data to estimate recall probability."}
    # Base: use severity trend
    recent_sev = sum(s["severity_score"] for s in stats_list[-3:]) / 3
    prior_sev = sum(s["severity_score"] for s in stats_list[-6:-3]) / 3
    sev_ratio = recent_sev / prior_sev if prior_sev > 0 else 1.0
    base_prob = min(0.7, max(0.05, 0.1 + (sev_ratio - 1) * 0.25))
    # Adjust by failure mode risk if available
    if failure_modes_data and failure_modes_data.get("status") == "ok" and failure_modes_data.get("modes"):
        modes = failure_modes_data["modes"]
        weighted_risk = 0
        total_pct = 0
        for cat, info in modes.items():
            risk = FAILURE_MODES.get(cat, {}).get("recall_risk", 0.3)
            weighted_risk += info["pct"] * risk
            total_pct += info["pct"]
        if total_pct > 0:
            mode_risk = weighted_risk / total_pct
            base_prob = (base_prob + mode_risk) / 2  # Average of severity-based and mode-based
    # Adjust by volume trend
    recent_vol = sum(s["count"] for s in stats_list[-3:]) / 3
    prior_vol = sum(s["count"] for s in stats_list[-6:-3]) / 3
    if prior_vol > 0 and recent_vol / prior_vol > 1.3:
        base_prob *= 1.2
    # Deaths present = much higher risk
    recent_deaths = sum(s["deaths"] for s in stats_list[-3:])
    if recent_deaths > 0:
        base_prob = min(0.95, base_prob + 0.15 * recent_deaths)
    prob = min(0.95, max(0.05, base_prob))
    signal = "HIGH" if prob > 0.5 else "MODERATE" if prob > 0.25 else "LOW"
    dominant = "unknown"
    if failure_modes_data and failure_modes_data.get("modes"):
        dominant = max(failure_modes_data["modes"].items(), key=lambda x: x[1]["pct"])[0].replace("_", " ").title()
    msg = (f"{prob * 100:.0f}% estimated recall probability in the next 6 months. Signal: {signal}. "
           f"Dominant failure mode: {dominant}. "
           f"Severity trend ratio: {sev_ratio:.2f}x. Recent deaths (3mo): {recent_deaths}.")
    if prob > 0.5:
        msg += " This is an elevated risk level — monitor closely for FDA action."
    return {"status": "ok", "probability": round(prob, 2), "signal": signal,
            "dominant_mode": dominant, "sev_ratio": round(sev_ratio, 2),
            "recent_deaths": recent_deaths, "message": msg}


# ============================================================
# 6. PEER-RELATIVE SCORING (computed)
# ============================================================
def compute_peer_relative(all_r_scores):
    """Compare each company's R-Score to peer group average."""
    scores = {k: v for k, v in all_r_scores.items() if v is not None}
    if len(scores) < 2:
        return {}
    avg = sum(scores.values()) / len(scores)
    sd = (sum((v - avg) ** 2 for v in scores.values()) / len(scores)) ** 0.5
    result = {}
    for k, v in scores.items():
        diff = v - avg
        z = diff / sd if sd > 0 else 0
        if z > 1:
            signal = "WORST"
        elif z > 0.5:
            signal = "WEAK"
        elif z < -1:
            signal = "BEST"
        elif z < -0.5:
            signal = "STRONG"
        else:
            signal = "INLINE"
        msg = (f"R-Score {v:.0f} vs peer avg {avg:.0f} ({diff:+.0f}). {signal} relative to peer group. "
               f"{'Long candidate — cleanest quality profile.' if signal in ('BEST', 'STRONG') else ''}"
               f"{'Short candidate — worst quality profile in peer group.' if signal in ('WORST', 'WEAK') else ''}")
        result[k] = {"r_score": v, "peer_avg": round(avg, 1), "diff": round(diff, 1),
                     "z_vs_peers": round(z, 2), "signal": signal, "message": msg}
    return result


# ============================================================
# 7. EARNINGS SURPRISE PREDICTOR (computed)
# ============================================================
def predict_earnings_surprise(stats_list, r_score, peer_relative):
    """Predict earnings beat/miss probability from MAUDE signals."""
    if not stats_list or len(stats_list) < 6 or not r_score:
        return {"status": "insufficient_data",
                "message": "Need 6+ months of data and R-Score to predict earnings."}
    miss_score = 0
    reasons = []
    # Factor 1: R-Score level
    if r_score["total"] >= 50:
        miss_score += 3
        reasons.append(f"R-Score {r_score['total']:.0f} is elevated (above 50)")
    elif r_score["total"] >= 30:
        miss_score += 1
        reasons.append(f"R-Score {r_score['total']:.0f} in watch zone (30-50)")
    # Factor 2: Rate/$M trend
    recent_rpm = [s["rate_per_m"] for s in stats_list[-3:] if s["rate_per_m"]]
    prior_rpm = [s["rate_per_m"] for s in stats_list[-6:-3] if s["rate_per_m"]]
    if recent_rpm and prior_rpm:
        rpm_ratio = (sum(recent_rpm) / len(recent_rpm)) / (sum(prior_rpm) / len(prior_rpm))
        if rpm_ratio > 1.2:
            miss_score += 2
            reasons.append(f"Rate/$M rising {(rpm_ratio - 1) * 100:.0f}% — quality deteriorating vs revenue")
        elif rpm_ratio > 1.05:
            miss_score += 1
            reasons.append("Rate/$M slightly rising")
    # Factor 3: Slope direction
    if stats_list[-1]["slope_6m"] > 0:
        miss_score += 1
        reasons.append(f"Positive 6mo slope ({stats_list[-1]['slope_6m']:+.1f}/mo) — reports accelerating")
    # Factor 4: Peer-relative weakness
    if peer_relative and peer_relative.get("signal") in ("WORST", "WEAK"):
        miss_score += 1
        reasons.append(f"Peer-relative position: {peer_relative['signal']}")
    # Factor 5: Deaths in recent period
    recent_deaths = sum(s["deaths"] for s in stats_list[-3:])
    if recent_deaths > 0:
        miss_score += 2
        reasons.append(f"{recent_deaths} deaths reported in last 3 months")
    # Convert to probability
    miss_prob = min(0.90, miss_score / 10)
    beat_prob = 1 - miss_prob
    if miss_prob > 0.55:
        prediction = "LIKELY MISS"
    elif beat_prob > 0.55:
        prediction = "LIKELY BEAT"
    else:
        prediction = "TOSS-UP"
    msg = (f"Earnings prediction: {prediction}. Miss probability: {miss_prob * 100:.0f}%. "
           f"Based on: {'; '.join(reasons) if reasons else 'no significant risk factors'}. "
           f"Trade ahead of earnings reports using this signal.")
    return {"status": "ok", "prediction": prediction,
            "miss_probability": round(miss_prob, 2), "beat_probability": round(beat_prob, 2),
            "miss_score": miss_score, "reasons": reasons, "message": msg}


# ============================================================
# 8. R-SCORE BACKTEST ENGINE (computed)
# ============================================================
def backtest_r_score(stats_list, stock_monthly, threshold=50, forward_days=None):
    """When R-Score crossed threshold, what happened to the stock?"""
    if forward_days is None:
        forward_days = [30, 60, 90]
    if not stats_list or len(stats_list) < 12 or not stock_monthly:
        return {"status": "insufficient_data",
                "message": "Need 12+ months of MAUDE data and stock prices for backtesting."}
    # Compute R-Score at each month
    monthly_r = {}
    for i in range(6, len(stats_list)):
        window = stats_list[:i + 1]
        r = _mini_r_score(window)
        if r is not None:
            monthly_r[stats_list[i]["month"]] = r
    # Find signal months where R-Score first crosses above threshold
    sorted_months = sorted(stock_monthly.keys())
    signals = []
    prev_below = True
    for m in sorted(monthly_r.keys()):
        above = monthly_r[m] >= threshold
        if above and prev_below:
            signals.append({"month": m, "r_score": monthly_r[m]})
        prev_below = not above
    if not signals:
        return {"status": "no_signals",
                "message": f"R-Score never crossed {threshold} during the analysis period. "
                           f"This means quality has stayed within acceptable bounds. "
                           f"Max R-Score observed: {max(monthly_r.values()):.0f}. "
                           f"Try a lower threshold or this may simply be a clean company.",
                "max_r_score": round(max(monthly_r.values()), 1) if monthly_r else 0,
                "trades": []}
    # Compute forward stock returns after each signal
    trades = []
    for sig in signals:
        if sig["month"] not in sorted_months:
            continue
        sig_idx = sorted_months.index(sig["month"])
        entry_price = stock_monthly.get(sig["month"])
        if not entry_price:
            continue
        trade = {"signal_month": sig["month"], "r_score": sig["r_score"],
                 "entry_price": entry_price, "returns": {}}
        for fwd in forward_days:
            fwd_months = max(1, fwd // 30)
            target_idx = sig_idx + fwd_months
            if target_idx < len(sorted_months):
                exit_month = sorted_months[target_idx]
                exit_price = stock_monthly.get(exit_month)
                if exit_price:
                    ret = round((exit_price - entry_price) / entry_price * 100, 2)
                    trade["returns"][f"{fwd}d"] = ret
        if trade["returns"]:
            trades.append(trade)
    if not trades:
        return {"status": "no_forward_data",
                "message": f"Found {len(signals)} R-Score signals but no forward stock price data available.",
                "signals": len(signals), "trades": []}
    # Aggregate results
    results = {}
    for fwd in forward_days:
        key = f"{fwd}d"
        rets = [t["returns"][key] for t in trades if key in t["returns"]]
        if not rets:
            continue
        # For SHORT signals, a stock DECLINE is a win
        wins = sum(1 for r in rets if r < 0)
        results[key] = {
            "n": len(rets), "win_rate": round(wins / len(rets) * 100, 1),
            "avg_return": round(sum(rets) / len(rets), 2),
            "median_return": round(sorted(rets)[len(rets) // 2], 2),
            "best_trade": round(min(rets), 2),  # Most negative = best short
            "worst_trade": round(max(rets), 2),  # Most positive = worst for short
        }
    if not results:
        return {"status": "no_results", "message": "Could not compute forward returns.", "trades": trades}
    best_window = min(results.keys(), key=lambda k: results[k]["avg_return"])
    bw = results[best_window]
    msg = (f"BACKTEST: R-Score crossed {threshold} on {len(signals)} occasions. "
           f"Best short window: {best_window} — stock moved {bw['avg_return']:+.1f}% avg "
           f"(win rate: {bw['win_rate']:.0f}%, median: {bw['median_return']:+.1f}%, n={bw['n']}). "
           f"{'Signal appears predictive.' if bw['win_rate'] > 55 else 'Signal is marginal — more data needed.'}")
    return {"status": "ok", "threshold": threshold, "signals": len(signals),
            "results": results, "trades": trades, "message": msg}


def _mini_r_score(sl):
    """Quick R-Score computation for backtesting."""
    if len(sl) < 6:
        return None
    lt = sl[-1]
    zc = min(20, abs(lt["z_score"]) * 6.67)
    rs = sum(s["severity_score"] for s in sl[-3:]) / 3
    ps = sum(s["severity_score"] for s in sl[-6:-3]) / 3
    sc = min(20, max(0, (rs / ps - 1) * 40)) if ps > 0 else 10
    rr = [s["rate_per_m"] for s in sl[-3:] if s["rate_per_m"]]
    pr = [s["rate_per_m"] for s in sl[-6:-3] if s["rate_per_m"]]
    gc = min(20, max(0, ((sum(rr) / len(rr)) / (sum(pr) / len(pr)) - 1) * 80)) if rr and pr and sum(pr) / len(pr) > 0 else 10
    sp = lt["slope_6m"] / lt["avg_12m"] * 100 if lt["avg_12m"] > 0 else 0
    slc = min(20, max(0, sp * 2))
    ri = [s["rate_per_10k"] for s in sl[-3:] if s["rate_per_10k"]]
    pi = [s["rate_per_10k"] for s in sl[-6:-3] if s["rate_per_10k"]]
    ic = min(20, max(0, ((sum(ri) / len(ri)) / (sum(pi) / len(pi)) - 1) * 80)) if ri and pi and sum(pi) / len(pi) > 0 else 10
    return min(100, zc + sc + gc + slc + ic)
