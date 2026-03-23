"""
data_modules.py — Enhanced Data Collection Modules
====================================================
1. MAUDE Text NLP — failure-mode classification from mdr_text
2. Reddit Sentiment — public .json endpoints for diabetes forums
3. SEC EDGAR NLP — quality keyword scoring from 10-Q/10-K filings
4. International MHRA — lightweight UK Yellow Card data (non-blocking)

All modules are designed to fail gracefully — if a source is down,
the pipeline continues with the data it has.
"""
import json, time, re
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ============================================================
# 1. MAUDE TEXT NLP — FAILURE MODE CLASSIFICATION
# ============================================================

# Keyword dictionaries for failure-mode classification
# Each category has primary keywords (high confidence) and secondary (lower confidence)
FAILURE_MODES = {
    "sensor_accuracy": {
        "primary": ["inaccurate", "false reading", "wrong reading", "reading was off", "incorrect reading",
                     "showed low when high", "showed high when low", "discrepancy", "not matching"],
        "secondary": ["accuracy", "MARD", "finger stick", "fingerstick", "blood glucose meter",
                       "calibration", "readings do not match"],
        "description": "Sensor providing incorrect glucose values. High severity — can lead to wrong insulin dosing.",
    },
    "adhesive_skin": {
        "primary": ["adhesive", "fell off", "peeling", "tape came off", "won't stick",
                     "skin irritation", "rash", "allergic reaction", "dermatitis", "blistered"],
        "secondary": ["adhesive patch", "skin", "irritation", "fell off early", "itching",
                       "swelling", "redness around site"],
        "description": "Adhesive failure or skin reaction. Lower severity but affects compliance.",
    },
    "connectivity": {
        "primary": ["bluetooth", "lost connection", "no signal", "pairing failed", "won't connect",
                     "communication error", "lost signal", "sync failed", "disconnected"],
        "secondary": ["connection", "signal", "pairing", "sync", "phone", "app connection",
                       "receiver connection"],
        "description": "Bluetooth/wireless connectivity failures between sensor and receiver/phone.",
    },
    "alert_failure": {
        "primary": ["no alert", "missed alert", "no alarm", "did not alert", "silent failure",
                     "alarm did not sound", "no notification", "alert not received", "speaker"],
        "secondary": ["notification", "vibration", "sound", "alarm", "alert",
                       "hypoglycemia alert", "hyperglycemia alert", "urgent low"],
        "description": "Device failed to alert user to dangerous glucose levels. HIGH severity — linked to deaths.",
    },
    "insulin_delivery": {
        "primary": ["occlusion", "no delivery", "blockage", "under-delivery", "over-delivery",
                     "insulin not delivered", "air bubble", "site leak", "pump failure"],
        "secondary": ["delivery", "infusion", "bolus", "basal", "insulin delivery error",
                       "pod failure", "cannula", "tubing"],
        "description": "Insulin pump delivery failure. Very high severity — can cause DKA or severe hypo.",
    },
    "sensor_early_termination": {
        "primary": ["sensor failed", "expired early", "terminated early", "sensor error",
                     "warm-up failed", "no readings", "sensor stopped", "3 exclamation marks"],
        "secondary": ["replace sensor", "sensor ended", "sensor lasted", "sensor life",
                       "days instead of", "sensor only lasted"],
        "description": "Sensor stopped working before its labeled wear period ended.",
    },
    "software_app": {
        "primary": ["app crash", "software error", "app frozen", "display error", "screen blank",
                     "update broke", "firmware", "app not working", "data lost"],
        "secondary": ["software", "update", "app", "screen", "display", "glitch", "bug"],
        "description": "Software/app malfunction in companion mobile app or device firmware.",
    },
}

def fetch_maude_texts(search_query, start="20250101", limit=100):
    """
    Fetch individual MAUDE report texts from openFDA.
    Returns list of {month, event_type, text, mdr_key}.
    
    Note: This is rate-limited. Each call returns up to 100 records.
    For production, batch across months.
    """
    end = datetime.now().strftime("%Y%m%d")
    url = (f"https://api.fda.gov/device/event.json?"
           f"search={_quote(search_query)}+AND+date_received:[{start}+TO+{end}]"
           f"&limit={limit}")
    
    try:
        req = Request(url, headers={"User-Agent": "MAUDE-Monitor/2.2"})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  MAUDE text fetch failed: {e}")
        return []
    
    records = []
    for r in data.get("results", []):
        texts = r.get("mdr_text", [])
        combined_text = " ".join(t.get("text", "") for t in texts).lower()
        date_recv = r.get("date_received", "")
        month = f"{date_recv[:4]}-{date_recv[4:6]}" if len(date_recv) >= 6 else ""
        event_type = r.get("event_type", "")
        mdr_key = r.get("mdr_report_key", "")
        
        if combined_text and month:
            records.append({
                "month": month,
                "event_type": event_type,
                "text": combined_text[:2000],  # Truncate for memory
                "mdr_key": mdr_key,
            })
    
    return records

def classify_failure_mode(text):
    """
    Classify a MAUDE report text into failure mode categories.
    Returns dict of {category: confidence_score}.
    Primary keyword match = 2 points, secondary = 1 point.
    """
    text_lower = text.lower()
    scores = {}
    
    for category, config in FAILURE_MODES.items():
        score = 0
        for kw in config["primary"]:
            if kw in text_lower:
                score += 2
        for kw in config["secondary"]:
            if kw in text_lower:
                score += 1
        if score > 0:
            scores[category] = score
    
    return scores

def analyze_failure_modes(search_query, start="20250101", limit=100):
    """
    Fetch MAUDE texts and classify by failure mode.
    Returns summary: {category: {count, pct, examples}}.
    """
    print(f"  Fetching MAUDE report texts for NLP...")
    records = fetch_maude_texts(search_query, start, limit)
    
    if not records:
        return {"total_analyzed": 0, "modes": {}}
    
    mode_counts = {cat: {"count": 0, "examples": []} for cat in FAILURE_MODES}
    unclassified = 0
    
    for rec in records:
        scores = classify_failure_mode(rec["text"])
        if not scores:
            unclassified += 1
            continue
        # Assign to highest-scoring category
        best_cat = max(scores, key=scores.get)
        mode_counts[best_cat]["count"] += 1
        if len(mode_counts[best_cat]["examples"]) < 2:
            # Store a short excerpt as example
            mode_counts[best_cat]["examples"].append(rec["text"][:150])
    
    total = len(records)
    result = {"total_analyzed": total, "unclassified": unclassified, "modes": {}}
    for cat, data in mode_counts.items():
        if data["count"] > 0:
            result["modes"][cat] = {
                "count": data["count"],
                "pct": round(data["count"] / total * 100, 1),
                "label": cat.replace("_", " ").title(),
                "description": FAILURE_MODES[cat]["description"],
                "examples": data["examples"],
            }
    
    return result

# ============================================================
# 2. REDDIT SENTIMENT — PUBLIC .JSON ENDPOINTS
# ============================================================

REDDIT_SUBREDDITS = ["diabetes", "diabetes_t1", "dexcom", "Omnipod"]
REDDIT_SEARCH_TERMS = {
    "DXCM": ["dexcom", "G7", "G6", "stelo", "CGM accuracy"],
    "PODD": ["omnipod", "omnipod 5", "pod failure", "occlusion"],
    "TNDM": ["tandem", "t:slim", "mobi", "control-iq"],
    "ABT_LIBRE": ["libre", "freestyle libre", "libre 3"],
    "SQEL": ["twiist", "sequel pump"],
}

# Simple sentiment keywords (no external NLP library needed)
POSITIVE_WORDS = {"love", "great", "amazing", "excellent", "perfect", "happy", "reliable",
                  "accurate", "improved", "works well", "recommend", "best", "fantastic"}
NEGATIVE_WORDS = {"terrible", "horrible", "worst", "hate", "broken", "failed", "inaccurate",
                  "unreliable", "dangerous", "recall", "died", "death", "injury", "lawsuit",
                  "malfunction", "defective", "error", "problem", "issue", "complaint",
                  "frustrated", "angry", "scared", "worried", "disappointing"}

def fetch_reddit_posts(subreddit, query, limit=25, time_filter="month"):
    """Fetch posts from Reddit using public .json endpoint."""
    url = (f"https://www.reddit.com/r/{subreddit}/search.json?"
           f"q={_quote(query)}&restrict_sr=on&sort=new&t={time_filter}&limit={limit}")
    
    try:
        req = Request(url, headers={
            "User-Agent": "MAUDE-Monitor/2.2 (research; contact@example.com)",
        })
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        
        posts = []
        for child in data.get("data", {}).get("children", []):
            d = child.get("data", {})
            posts.append({
                "title": d.get("title", ""),
                "selftext": d.get("selftext", "")[:500],
                "score": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "created_utc": d.get("created_utc", 0),
                "subreddit": subreddit,
            })
        return posts
    except Exception as e:
        print(f"  Reddit fetch failed for r/{subreddit} '{query}': {e}")
        return []

def score_sentiment(text):
    """Simple keyword-based sentiment scoring. Returns float in [-1, 1]."""
    text_lower = text.lower()
    words = set(re.findall(r'\b\w+\b', text_lower))
    
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    total = pos + neg
    
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)

def analyze_reddit_sentiment(ticker):
    """
    Fetch and analyze Reddit sentiment for a ticker's products.
    Returns {post_count, avg_sentiment, neg_pct, top_concerns, sample_posts}.
    """
    terms = REDDIT_SEARCH_TERMS.get(ticker, [])
    if not terms:
        return None
    
    all_posts = []
    for sub in REDDIT_SUBREDDITS:
        for term in terms[:2]:  # Limit to avoid rate limits
            posts = fetch_reddit_posts(sub, term, limit=10, time_filter="month")
            all_posts.extend(posts)
            time.sleep(1.5)  # Rate limit: be nice to Reddit
    
    if not all_posts:
        return {"post_count": 0, "avg_sentiment": 0, "error": "No posts retrieved"}
    
    # Deduplicate by title
    seen = set()
    unique = []
    for p in all_posts:
        if p["title"] not in seen:
            seen.add(p["title"])
            unique.append(p)
    
    sentiments = []
    concerns = {}
    for p in unique:
        full_text = f"{p['title']} {p['selftext']}"
        sent = score_sentiment(full_text)
        sentiments.append(sent)
        
        # Identify concern categories
        for word in NEGATIVE_WORDS:
            if word in full_text.lower():
                concerns[word] = concerns.get(word, 0) + 1
    
    avg_sent = sum(sentiments) / len(sentiments) if sentiments else 0
    neg_count = sum(1 for s in sentiments if s < -0.1)
    
    top_concerns = sorted(concerns.items(), key=lambda x: -x[1])[:5]
    
    # Sample negative posts
    neg_samples = sorted(
        [(unique[i], sentiments[i]) for i in range(len(unique)) if sentiments[i] < -0.1],
        key=lambda x: x[1]
    )[:3]
    
    return {
        "post_count": len(unique),
        "avg_sentiment": round(avg_sent, 3),
        "negative_pct": round(neg_count / len(unique) * 100, 1) if unique else 0,
        "top_concerns": [{"word": w, "count": c} for w, c in top_concerns],
        "sample_negative": [{"title": p["title"][:100], "score": p["score"], "sentiment": s}
                           for p, s in neg_samples],
        "period": "Last 30 days",
    }

# ============================================================
# 3. SEC EDGAR NLP — QUALITY KEYWORDS IN 10-Q/10-K FILINGS
# ============================================================

# CIK numbers for our companies (from SEC EDGAR)
COMPANY_CIKS = {
    "DXCM": "0001093557",
    "PODD": "0001145197",
    "TNDM": "0001438133",
    "ABT_LIBRE": "0000001800",  # Abbott Labs
}

# Quality-related keywords to search for in filings
# Grouped by severity/category
EDGAR_KEYWORDS = {
    "product_quality": {
        "high": ["recall", "warning letter", "class I", "class II", "consent decree",
                 "FDA inspection", "483 observation", "corrective action", "field safety"],
        "medium": ["product quality", "manufacturing defect", "design defect", "adverse event",
                   "complaint rate", "complaint volume", "product liability", "warranty",
                   "warranty cost", "warranty expense", "replacement cost"],
        "low": ["quality assurance", "quality control", "inspection", "regulatory compliance"],
        "description": "Mentions of quality issues, recalls, FDA actions in filings.",
    },
    "legal_regulatory": {
        "high": ["class action", "securities litigation", "derivative action",
                 "DOJ investigation", "SEC investigation", "criminal"],
        "medium": ["litigation", "lawsuit", "legal proceeding", "product liability claim",
                   "regulatory proceeding", "government investigation"],
        "low": ["patent", "intellectual property"],
        "description": "Legal and regulatory risk language.",
    },
    "revenue_risk": {
        "high": ["revenue decline", "market share loss", "customer attrition",
                 "competitive pressure", "pricing pressure"],
        "medium": ["guidance reduction", "lowered guidance", "headwind", "uncertainty",
                   "challenging environment", "adverse impact"],
        "low": ["competition", "competitive", "market dynamics"],
        "description": "Language suggesting revenue or competitive risk.",
    },
}

def fetch_edgar_filings(cik, form_type="10-Q", count=4):
    """
    Fetch recent filing metadata from SEC EDGAR submissions API.
    Returns list of {accession, date, form_type, url}.
    Free, no API key required. Respects SEC rate limit (10 req/sec).
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        req = Request(url, headers={
            "User-Agent": "MAUDE-Monitor research@parkmanhp.com",  # SEC requires email
            "Accept": "application/json",
        })
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        
        results = []
        for i in range(len(forms)):
            if forms[i] == form_type and len(results) < count:
                acc_no = accessions[i].replace("-", "")
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_no}/{primary_docs[i]}"
                results.append({
                    "accession": accessions[i],
                    "date": dates[i],
                    "form": forms[i],
                    "url": doc_url,
                })
        return results
    except Exception as e:
        print(f"  EDGAR filing fetch failed for CIK {cik}: {e}")
        return []

def fetch_filing_text(url, max_chars=50000):
    """Fetch filing HTML and extract text content."""
    try:
        req = Request(url, headers={
            "User-Agent": "MAUDE-Monitor research@parkmanhp.com",
        })
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        
        # Strip HTML tags (simple regex — not perfect but sufficient)
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        return text[:max_chars].lower()
    except Exception as e:
        print(f"  Filing text fetch failed: {e}")
        return ""

def score_filing_quality(text):
    """
    Score a filing's text for quality-related language.
    Returns {category: {score, high_matches, medium_matches, low_matches}}.
    """
    results = {}
    for category, config in EDGAR_KEYWORDS.items():
        high_hits = [kw for kw in config["high"] if kw.lower() in text]
        med_hits = [kw for kw in config["medium"] if kw.lower() in text]
        low_hits = [kw for kw in config["low"] if kw.lower() in text]
        
        # Weighted score: high=5, medium=2, low=1
        score = len(high_hits) * 5 + len(med_hits) * 2 + len(low_hits) * 1
        
        results[category] = {
            "score": score,
            "high_matches": high_hits,
            "medium_matches": med_hits,
            "low_matches": low_hits,
            "description": config["description"],
        }
    
    return results

def analyze_edgar_filings(ticker):
    """
    Analyze recent 10-Q and 10-K filings for quality language.
    Returns {filings: [{date, form, quality_score, matches}], trend}.
    """
    cik = COMPANY_CIKS.get(ticker)
    if not cik:
        return None
    
    print(f"  Fetching EDGAR filings for {ticker} (CIK {cik})...")
    filings_10q = fetch_edgar_filings(cik, "10-Q", 4)
    filings_10k = fetch_edgar_filings(cik, "10-K", 1)
    all_filings = filings_10q + filings_10k
    time.sleep(0.5)  # Rate limit
    
    if not all_filings:
        return {"filings": [], "error": "No filings retrieved"}
    
    results = []
    for f in all_filings[:4]:  # Limit to 4 most recent
        print(f"    Analyzing {f['form']} filed {f['date']}...")
        text = fetch_filing_text(f["url"])
        time.sleep(0.3)
        
        if not text:
            continue
        
        scores = score_filing_quality(text)
        total_score = sum(s["score"] for s in scores.values())
        
        # Collect all high-severity matches
        high_matches = []
        for cat, s in scores.items():
            high_matches.extend(s["high_matches"])
        
        results.append({
            "date": f["date"],
            "form": f["form"],
            "total_score": total_score,
            "high_severity_matches": high_matches,
            "categories": {cat: s["score"] for cat, s in scores.items()},
        })
    
    # Trend: is quality language increasing?
    if len(results) >= 2:
        recent = results[0]["total_score"]
        older = sum(r["total_score"] for r in results[1:]) / len(results[1:])
        trend = "increasing" if recent > older * 1.2 else "decreasing" if recent < older * 0.8 else "stable"
    else:
        trend = "insufficient data"
    
    return {
        "filings": results,
        "trend": trend,
        "interpretation": _edgar_interpretation(results, trend, ticker),
    }

def _edgar_interpretation(results, trend, ticker):
    """Generate plain-English interpretation of EDGAR analysis."""
    if not results:
        return "No filings analyzed."
    
    latest = results[0]
    high = latest.get("high_severity_matches", [])
    
    parts = [f"Most recent {latest['form']} (filed {latest['date']}): quality language score {latest['total_score']}."]
    
    if high:
        parts.append(f"HIGH-SEVERITY matches: {', '.join(high)}.")
    
    if trend == "increasing":
        parts.append("Quality/risk language is INCREASING in recent filings — management may be preparing the market for bad news.")
    elif trend == "decreasing":
        parts.append("Quality/risk language is decreasing — may indicate improving conditions or less disclosure.")
    else:
        parts.append("Quality language volume is stable across recent filings.")
    
    return " ".join(parts)

# ============================================================
# 4. INTERNATIONAL — MHRA YELLOW CARD (UK)
# ============================================================

def fetch_mhra_data(brand_name, limit=10):
    """
    Attempt to fetch UK MHRA Yellow Card reports.
    MHRA doesn't have a clean public API, so we search their
    public Drug Analysis Prints and Medical Device reports.
    This is best-effort — returns None if unavailable.
    """
    # MHRA public search endpoint
    url = (f"https://info.mhra.gov.uk/drug-analysis-profiles/dap.aspx?"
           f"drug={_quote(brand_name)}&format=json")
    
    try:
        req = Request(url, headers={"User-Agent": "MAUDE-Monitor/2.2"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data
    except Exception:
        # MHRA endpoint may not return JSON — try alternative
        pass
    
    # Alternative: search MHRA medical device alerts
    alt_url = (f"https://www.gov.uk/drug-device-alerts.json?"
               f"filter%5Bmedical_specialism%5D=diabetes")
    try:
        req = Request(alt_url, headers={"User-Agent": "MAUDE-Monitor/2.2"})
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        
        alerts = []
        for r in data.get("results", [])[:limit]:
            title = r.get("title", "")
            if brand_name.lower() in title.lower():
                alerts.append({
                    "title": title,
                    "date": r.get("public_timestamp", ""),
                    "url": f"https://www.gov.uk{r.get('link', '')}",
                })
        return {"alerts": alerts, "source": "MHRA/GOV.UK"} if alerts else None
    except Exception as e:
        print(f"  MHRA fetch failed for {brand_name}: {e}")
        return None

def analyze_international(ticker, brand_names):
    """
    Collect international adverse event data where available.
    Currently supports: UK MHRA (best-effort).
    Returns dict or None.
    """
    results = {}
    
    for brand in brand_names:
        print(f"  Checking MHRA for '{brand}'...")
        mhra = fetch_mhra_data(brand)
        if mhra:
            results[brand] = {"mhra": mhra}
        time.sleep(0.5)
    
    if not results:
        return None
    
    return {
        "source": "UK MHRA Yellow Card / GOV.UK Medical Device Alerts",
        "disclaimer": "International data coverage is limited. MHRA does not provide a structured public API. Results are best-effort.",
        "results": results,
    }

# ============================================================
# UTILITY
# ============================================================

def _quote(s):
    """URL-encode a string, preserving openFDA operators."""
    from urllib.parse import quote
    return quote(s, safe='+:"[]')
