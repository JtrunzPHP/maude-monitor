"""MAUDE Monitor V4 — openFDA client.

Key changes vs V3:
- Quoted-phrase searches against device.brand_name (and optionally
  device.manufacturer_d_name) instead of unquoted token soup. Unquoted
  multi-token searches can over/under-count by orders of magnitude.
- Field fallback probe: if device.brand_name returns nothing for a query,
  retries bare brand_name once and remembers which field worked.
- Date counts aggregated to months client-side, with the current partial
  calendar month DROPPED and the latest full month flagged provisional
  (MAUDE reporting lag makes the newest month structurally incomplete).
- Narrative pulls paginated to MAX_NARRATIVES (V3 read only 50 reports).
- Discovery mode: for each device, prints the top exact brand names and
  product codes the query matches, so phrases/product codes get tuned
  from the API itself rather than guessed.
"""

import json
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from mm_config import OPENFDA_API_KEY, API_SLEEP, API_RETRIES, API_TIMEOUT, \
    MAX_NARRATIVES, NARRATIVE_PAGE

BASE_EVENT = "https://api.fda.gov/device/event.json"
BASE_RECALL = "https://api.fda.gov/device/recall.json"

_field_cache = {}   # device_id -> working brand field


def _encode(s):
    """Minimal encoding for openFDA query strings: spaces -> +, quotes -> %22.
    Phrases are built internally so we control the character set."""
    return s.replace(" ", "+").replace('"', "%22")


def build_search(phrases, manufacturer=None, brand_field="device.brand_name",
                 require_manufacturer=None):
    parts = [f'{brand_field}:"{p}"' for p in phrases]
    for m in (manufacturer or []):
        parts.append(f'device.manufacturer_d_name:"{m}"')
    inner = "+OR+".join(_encode(p) for p in parts)
    search = f"({inner})" if len(parts) > 1 else inner
    if require_manufacturer:
        req = "+OR+".join(_encode(f'device.manufacturer_d_name:"{m}"')
                          for m in require_manufacturer)
        req = f"({req})" if len(require_manufacturer) > 1 else req
        search = f"({search}+AND+{req})"
    return search


def device_search(device, field="device.brand_name"):
    return build_search(device["phrases"], device.get("manufacturer"), field,
                        device.get("require_manufacturer"))


def api_get(url, retries=API_RETRIES):
    if OPENFDA_API_KEY:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api_key={OPENFDA_API_KEY}"
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "MAUDE-Monitor/4.0"})
            with urlopen(req, timeout=API_TIMEOUT) as resp:
                data = json.loads(resp.read().decode())
            if "error" in data:
                err = data["error"]
                code = err.get("code", "?")
                if code == "NOT_FOUND":
                    return None
                print(f"    API ERROR: {code} - {str(err.get('message', ''))[:80]}")
                if code == "TOO_MANY_REQUESTS":
                    time.sleep(10 * (attempt + 1))
                    continue
                return None
            return data
        except HTTPError as e:
            if e.code == 404:
                return None
            body = ""
            try:
                body = e.read().decode()[:120]
            except Exception:
                pass
            print(f"    HTTP {e.code} attempt {attempt + 1}: {body}")
            time.sleep((10 if e.code == 429 else 3) * (attempt + 1))
        except Exception as e:
            print(f"    ERROR attempt {attempt + 1}: {type(e).__name__}: {str(e)[:100]}")
            time.sleep(3 * (attempt + 1))
    return None


def _daily_to_monthly(results):
    counts = {}
    for r in results:
        d = r.get("time", "")
        if len(d) >= 6:
            m = f"{d[:4]}-{d[4:6]}"
            counts[m] = counts.get(m, 0) + r.get("count", 0)
    return counts


def trim_partial_month(counts, now=None):
    """Drop the current in-progress calendar month; return (counts, meta).
    The latest remaining month is flagged provisional: MAUDE date_received
    counts for recent months keep rising for weeks as reports arrive."""
    now = now or datetime.now()
    cur = now.strftime("%Y-%m")
    trimmed = {m: v for m, v in counts.items() if m < cur}
    months = sorted(trimmed.keys())
    meta = {"dropped_partial": cur if cur in counts else None,
            "provisional_month": months[-1] if months else None}
    return trimmed, meta


def resolve_brand_field(device):
    """Probe which brand field returns data for this device's phrases."""
    did = device["id"]
    if did in _field_cache:
        return _field_cache[did]
    for field in ("device.brand_name", "brand_name"):
        search = device_search(device, field)
        url = f"{BASE_EVENT}?search={search}&limit=1"
        data = api_get(url, retries=1)
        time.sleep(API_SLEEP)
        if data and data.get("meta", {}).get("results", {}).get("total", 0) > 0:
            _field_cache[did] = field
            return field
    _field_cache[did] = "device.brand_name"
    return _field_cache[did]


def fetch_counts(device, date_field, start):
    field = resolve_brand_field(device)
    search = device_search(device, field)
    url = (f"{BASE_EVENT}?search={search}+AND+{date_field}:[{start}+TO+now]"
           f"&count={date_field}")
    data = api_get(url)
    if not data or "results" not in data:
        return {}
    return _daily_to_monthly(data["results"])


def fetch_product_code_counts(device, date_field, start):
    """Cross-check series by FDA product code, when codes are configured."""
    codes = device.get("product_codes") or []
    if not codes:
        return {}
    inner = "+OR+".join(_encode(f'device.device_report_product_code:"{c}"') for c in codes)
    search = f"({inner})" if len(codes) > 1 else inner
    url = (f"{BASE_EVENT}?search={search}+AND+{date_field}:[{start}+TO+now]"
           f"&count={date_field}")
    data = api_get(url)
    if not data or "results" not in data:
        return {}
    return _daily_to_monthly(data["results"])


def fetch_severity(device, start):
    field = resolve_brand_field(device)
    search = device_search(device, field)
    sev = {"death": {}, "injury": {}, "malfunction": {}}
    for et in sev:
        url = (f"{BASE_EVENT}?search={search}+AND+event_type:{et}"
               f"+AND+date_received:[{start}+TO+now]&count=date_received")
        data = api_get(url)
        if data and "results" in data:
            sev[et] = _daily_to_monthly(data["results"])
        time.sleep(API_SLEEP)
    return sev


def fetch_narratives(device, start, max_reports=MAX_NARRATIVES):
    """Paginated MDR narrative pull, newest first."""
    field = resolve_brand_field(device)
    search = device_search(device, field)
    texts = []
    skip = 0
    while skip < max_reports:
        url = (f"{BASE_EVENT}?search={search}+AND+date_received:[{start}+TO+now]"
               f"&sort=date_received:desc&limit={NARRATIVE_PAGE}&skip={skip}")
        data = api_get(url)
        if not data or "results" not in data or not data["results"]:
            break
        for r in data["results"]:
            for t in r.get("mdr_text", []) or []:
                nar = t.get("text", "") if isinstance(t, dict) else str(t)
                if len(nar) >= 10:
                    texts.append(nar.lower())
        if len(data["results"]) < NARRATIVE_PAGE:
            break
        skip += NARRATIVE_PAGE
        time.sleep(API_SLEEP)
    return texts


FAILURE_KEYWORDS = {
    "sensor_failure": ["sensor fail", "no reading", "sensor error", "lost signal", "signal loss", "expired early"],
    "adhesion": ["fell off", "adhesive", "peel", "detach", "came off", "not stick"],
    "connectivity": ["bluetooth", "connect", "pair", "sync", "lost connection", "disconnect"],
    "inaccurate_reading": ["inaccurate", "wrong reading", "false", "discrepan", "not match", "off by"],
    "skin_reaction": ["rash", "irritat", "red", "itch", "allerg", "skin", "welt", "blister"],
    "alarm_alert": ["alarm", "alert", "no sound", "speaker", "beep", "notification", "did not alert"],
    "battery": ["battery", "charge", "power", "dead", "drain", "won't turn on"],
    "physical_damage": ["crack", "broke", "snap", "bent", "leak", "damage"],
    "software": ["software", "app", "crash", "freeze", "update", "glitch", "display"],
    "insertion": ["insert", "needle", "pain", "bleed", "bruis", "applicat"],
    "occlusion": ["occlus", "block", "clog", "no deliv", "no insulin"],
    # cardiac ablation / PFA complications (Abbott Volt)
    "ablation_complication": ["tamponade", "perforat", "stroke", "phrenic", "esophag",
                              "hemolysis", "haemolysis", "vasospasm", "coronary spasm",
                              "pericardial", "effusion", "embol"],
}


def classify_failure_modes(narratives):
    cats = {k: 0 for k in FAILURE_KEYWORDS}
    cats["other"] = 0
    for nar in narratives:
        matched = False
        for cat, kws in FAILURE_KEYWORDS.items():
            if any(k in nar for k in kws):
                cats[cat] += 1
                matched = True
                break
        if not matched:
            cats["other"] += 1
    total = len(narratives)
    top = sorted(cats.items(), key=lambda x: -x[1])[:5]
    status = "ok" if total else "no_data"
    return {"status": status, "categories": cats, "total": total,
            "top_modes": [{"mode": k, "count": v,
                           "pct": round(v / max(total, 1) * 100, 1)} for k, v in top]}


def fetch_recalls(device):
    """FDA device recall API; feeds both the module panel and auto events."""
    inner = "+OR+".join(_encode(f'product_description:"{p}"') for p in device["phrases"])
    search = f"({inner})" if len(device["phrases"]) > 1 else inner
    url = f"{BASE_RECALL}?search={search}&sort=event_date_initiated:desc&limit=10"
    data = api_get(url)
    if not data or "results" not in data:
        return {"status": "ok", "count": 0, "recalls": [], "class1_count": 0,
                "message": "No recalls found"}
    recalls = []
    for r in data["results"][:10]:
        recalls.append({
            "reason": str(r.get("reason_for_recall", ""))[:140],
            "classification": r.get("classification", ""),
            "status": r.get("status", ""),
            "initiated": str(r.get("event_date_initiated", ""))[:10],
            "firm": str(r.get("recalling_firm", ""))[:60],
        })
    c1 = sum(1 for r in recalls if "class i" == r["classification"].strip().lower()
             or r["classification"].strip() == "Class I")
    return {"status": "ok", "count": len(data["results"]), "recalls": recalls,
            "class1_count": c1,
            "message": f"{len(data['results'])} recalls ({c1} Class I)"}


def recalls_to_events(recall_result):
    """Convert recall API rows into chart event annotations (merged with
    manual PRODUCT_EVENTS, deduped by month in the pipeline)."""
    evts = []
    for r in recall_result.get("recalls", []):
        d = r.get("initiated", "")
        if len(d) >= 7:
            cls = r.get("classification", "")
            evts.append({"date": d[:7], "label": f"Recall {cls}".strip(),
                         "type": "recall", "auto": True})
    return evts


def discover(device, start):
    """Print what a device's query actually matches: top exact brand names
    and product codes with counts. Use this to tune phrases / pin codes."""
    field = resolve_brand_field(device)
    search = device_search(device, field)
    print(f"\n{device['name']} ({device['id']}) via {field}")
    for count_field, label in (("device.brand_name.exact", "Brand names"),
                               ("device.device_report_product_code", "Product codes"),
                               ("device.manufacturer_d_name.exact", "Manufacturers")):
        url = (f"{BASE_EVENT}?search={search}+AND+date_received:[{start}+TO+now]"
               f"&count={count_field}&limit=15")
        data = api_get(url)
        print(f"  {label}:")
        if data and "results" in data:
            for r in data["results"][:15]:
                print(f"    {r.get('count', 0):>7,}  {r.get('term', '')}")
        else:
            print("    (no results)")
        time.sleep(API_SLEEP)
