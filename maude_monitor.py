#!/usr/bin/env python3
"""MAUDE Monitor V3.3 — Event-Date Smoothing, Multi-Signal Correlation, PM Case Studies.
Core fix: uses date_of_event as true signal, date_received for batch detection.
Smoothed series redistributes batch spikes across actual event months.
All modules inline. No external deps beyond stdlib + yfinance (optional).
REQUIRES: openFDA API key (free) as env var OPENFDA_API_KEY for reliable operation."""
import json,os,time,math,argparse,smtplib,csv,re
from datetime import datetime,timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as _orig_url_quote
from urllib.error import HTTPError,URLError

# Fix: preserve + signs in URL (openFDA uses + as space in search syntax)
def url_quote(s, *args, **kwargs):
    kwargs.setdefault('safe', '+')
    return _orig_url_quote(s, *args, **kwargs)

# openFDA API key (free from https://open.fda.gov/apis/authentication/)
OPENFDA_API_KEY = os.environ.get("OPENFDA_API_KEY", "")

HAS_MODULES = True

def fmt(v,d=1):
    if v is None: return "\u2014"
    if abs(v)>=1e6: return f"{v/1e6:,.{d}f}M"
    if abs(v)>=1e3: return f"{v:,.{d}f}"
    return f"{v:.{d}f}"
def fmt0(v):
    if v is None: return "\u2014"
    return f"{v:,.0f}"
def fmt2(v):
    if v is None: return "\u2014"
    return f"{v:,.2f}"

# ============================================================
# PROPER SPEARMAN WITH REAL T-DISTRIBUTION P-VALUE
# ============================================================
def _proper_spearman(x, y):
    n = len(x)
    if n < 5: return 0.0, 1.0
    def _rank(arr):
        indexed = sorted(range(len(arr)), key=lambda i: arr[i])
        ranks = [0.0]*len(arr); i = 0
        while i < len(arr):
            j = i
            while j < len(arr)-1 and arr[indexed[j]] == arr[indexed[j+1]]: j += 1
            avg_rank = (i+j)/2.0+1.0
            for k in range(i, j+1): ranks[indexed[k]] = avg_rank
            i = j+1
        return ranks
    rx, ry = _rank(x), _rank(y)
    mx, my = sum(rx)/n, sum(ry)/n
    num = sum((a-mx)*(b-my) for a,b in zip(rx,ry))
    dx = math.sqrt(sum((a-mx)**2 for a in rx))
    dy = math.sqrt(sum((b-my)**2 for b in ry))
    if dx==0 or dy==0: return 0.0, 1.0
    rho = max(-1.0, min(1.0, num/(dx*dy)))
    if abs(rho) >= 0.9999: return round(rho,4), 0.001
    t_stat = rho * math.sqrt((n-2)/(1.0-rho*rho)); df = n-2
    x_val = df/(df+t_stat*t_stat)
    def _betacf(a,b,xi):
        eps=1e-12; qab=a+b; qap=a+1.0; qam=a-1.0; c=1.0
        d=max(1.0-qab*xi/qap, eps); d=1.0/d; h=d
        for m in range(1,201):
            m2=2*m; aa=m*(b-m)*xi/((qam+m2)*(a+m2))
            d=max(1.0+aa*d, eps); c=max(1.0+aa/c, eps); d=1.0/d; h*=d*c
            aa=-(a+m)*(qab+m)*xi/((a+m2)*(qap+m2))
            d=max(1.0+aa*d, eps); c=max(1.0+aa/c, eps); d=1.0/d; h*=d*c
            if abs(d*c-1.0)<eps: break
        return h
    def _lg(z):
        if z<=0: return 0.0
        c=[76.18009172947146,-86.50532032941677,24.01409824083091,-1.231739572450155,0.1208650973866179e-2,-0.5395239384953e-5]
        y=z; tmp=z+5.5; tmp-=(z+0.5)*math.log(tmp); ser=1.000000000190015
        for j in range(6): y+=1; ser+=c[j]/y
        return -tmp+math.log(2.5066282746310005*ser/z)
    def _bi(a,b,xi):
        if xi<=0: return 0.0
        if xi>=1: return 1.0
        lb=_lg(a)+_lg(b)-_lg(a+b)
        fr=math.exp(math.log(max(xi,1e-300))*a+math.log(max(1.0-xi,1e-300))*b-lb)
        if xi<(a+1.0)/(a+b+2.0): return fr*_betacf(a,b,xi)/a
        else: return 1.0-fr*_betacf(b,a,1.0-xi)/b
    return round(rho,4), round(max(0.0001, min(1.0, _bi(df/2.0, 0.5, x_val))),4)

# ============================================================
# STATIC DATA DICTIONARIES
# ============================================================
QUARTERLY_REVENUE = {
    "DXCM":{"2023-Q1":921,"2023-Q2":871.3,"2023-Q3":975,"2023-Q4":1010,"2024-Q1":921,"2024-Q2":1004,"2024-Q3":994.2,"2024-Q4":1115,"2025-Q1":1036,"2025-Q2":1092,"2025-Q3":1174,"2025-Q4":1260,"2026-Q1":1270},
    "PODD":{"2023-Q1":412.5,"2023-Q2":432.1,"2023-Q3":476,"2023-Q4":521.5,"2024-Q1":481.5,"2024-Q2":530.4,"2024-Q3":543.9,"2024-Q4":597.7,"2025-Q1":555,"2025-Q2":655,"2025-Q3":706.3,"2025-Q4":783.8,"2026-Q1":810},
    "TNDM":{"2023-Q1":171.1,"2023-Q2":185.5,"2023-Q3":194.1,"2023-Q4":196.3,"2024-Q1":193.5,"2024-Q2":214.6,"2024-Q3":249.5,"2024-Q4":282.6,"2025-Q1":226,"2025-Q2":207.9,"2025-Q3":290.4,"2025-Q4":290.4,"2026-Q1":260},
    "ABT_LIBRE":{"2023-Q1":1100,"2023-Q2":1200,"2023-Q3":1400,"2023-Q4":1400,"2024-Q1":1500,"2024-Q2":1600,"2024-Q3":1700,"2024-Q4":1800,"2025-Q1":1700,"2025-Q2":1850,"2025-Q3":2000,"2025-Q4":2100,"2026-Q1":2200},
    "BBNX":{"2025-Q1":20,"2025-Q2":24,"2025-Q3":24.2,"2025-Q4":32.1,"2026-Q1":32},
    "MDT_DM":{"2023-Q1":570,"2023-Q2":580,"2023-Q3":600,"2023-Q4":620,"2024-Q1":620,"2024-Q2":647,"2024-Q3":691,"2024-Q4":694,"2025-Q1":728,"2025-Q2":750,"2025-Q3":770,"2025-Q4":780,"2026-Q1":800},
    "SQEL":{},
}
REVENUE_LAST_UPDATED = "2026-03-15"
INSTALLED_BASE_K = {
    "DXCM":{"2023-Q1":2000,"2023-Q2":2100,"2023-Q3":2200,"2023-Q4":2350,"2024-Q1":2500,"2024-Q2":2600,"2024-Q3":2750,"2024-Q4":2900,"2025-Q1":3000,"2025-Q2":3100,"2025-Q3":3250,"2025-Q4":3400,"2026-Q1":3550},
    "PODD":{"2023-Q1":380,"2023-Q2":400,"2023-Q3":420,"2023-Q4":440,"2024-Q1":460,"2024-Q2":480,"2024-Q3":510,"2024-Q4":540,"2025-Q1":560,"2025-Q2":590,"2025-Q3":620,"2025-Q4":660,"2026-Q1":700},
    "TNDM":{"2023-Q1":380,"2023-Q2":390,"2023-Q3":400,"2023-Q4":410,"2024-Q1":420,"2024-Q2":430,"2024-Q3":445,"2024-Q4":460,"2025-Q1":470,"2025-Q2":480,"2025-Q3":495,"2025-Q4":510,"2026-Q1":520},
    "ABT_LIBRE":{"2023-Q1":4500,"2023-Q2":4700,"2023-Q3":4900,"2023-Q4":5100,"2024-Q1":5300,"2024-Q2":5600,"2024-Q3":5900,"2024-Q4":6200,"2025-Q1":6400,"2025-Q2":6700,"2025-Q3":7000,"2025-Q4":7300,"2026-Q1":7600},
    "BBNX":{"2025-Q1":2,"2025-Q2":4,"2025-Q3":7,"2025-Q4":12,"2026-Q1":18},
    "MDT_DM":{"2023-Q1":800,"2023-Q2":820,"2023-Q3":850,"2023-Q4":880,"2024-Q1":900,"2024-Q2":930,"2024-Q3":960,"2024-Q4":1000,"2025-Q1":1050,"2025-Q2":1100,"2025-Q3":1150,"2025-Q4":1200,"2026-Q1":1250},
    "SQEL":{},
}
STOCK_MONTHLY = {
    "DXCM":{"2023-01":112.5,"2023-02":115.2,"2023-03":119.8,"2023-04":122.1,"2023-05":118.3,"2023-06":130.5,"2023-07":133.2,"2023-08":127.4,"2023-09":92.5,"2023-10":88.2,"2023-11":95.6,"2023-12":98.4,"2024-01":120.3,"2024-02":127.8,"2024-03":133.5,"2024-04":131.2,"2024-05":116.8,"2024-06":112.4,"2024-07":78.5,"2024-08":72.3,"2024-09":71.1,"2024-10":70.5,"2024-11":78.2,"2024-12":82.1,"2025-01":80.5,"2025-02":78.3,"2025-03":75.8,"2025-04":79.2,"2025-05":82.1,"2025-06":85.6,"2025-07":88.4,"2025-08":86.3,"2025-09":83.7,"2025-10":80.1,"2025-11":76.5,"2025-12":74.2},
    "PODD":{"2023-01":295.3,"2023-02":288.4,"2023-03":305.2,"2023-04":310.8,"2023-05":298.6,"2023-06":280.1,"2023-07":255.3,"2023-08":238.4,"2023-09":220.1,"2023-10":195.6,"2023-11":180.2,"2023-12":172.8,"2024-01":178.5,"2024-02":182.3,"2024-03":175.6,"2024-04":168.4,"2024-05":155.2,"2024-06":148.3,"2024-07":142.8,"2024-08":145.6,"2024-09":150.2,"2024-10":158.4,"2024-11":195.3,"2024-12":210.5,"2025-01":215.8,"2025-02":225.3,"2025-03":248.6,"2025-04":260.2,"2025-05":272.1,"2025-06":285.4,"2025-07":290.3,"2025-08":295.8,"2025-09":302.1,"2025-10":310.5,"2025-11":315.2,"2025-12":320.8},
    "TNDM":{"2023-01":48.5,"2023-02":42.3,"2023-03":38.8,"2023-04":28.5,"2023-05":26.2,"2023-06":28.8,"2023-07":30.5,"2023-08":25.6,"2023-09":23.4,"2023-10":20.8,"2023-11":22.5,"2023-12":21.3,"2024-01":22.8,"2024-02":20.5,"2024-03":19.8,"2024-04":18.5,"2024-05":17.2,"2024-06":38.5,"2024-07":42.3,"2024-08":40.8,"2024-09":38.5,"2024-10":35.2,"2024-11":40.5,"2024-12":42.8,"2025-01":44.2,"2025-02":46.5,"2025-03":48.8,"2025-04":50.2,"2025-05":52.5,"2025-06":48.3,"2025-07":45.8,"2025-08":42.5,"2025-09":40.2,"2025-10":38.5,"2025-11":36.2,"2025-12":35.5},
    "ABT":{"2023-01":108.5,"2023-02":102.3,"2023-03":98.8,"2023-04":105.2,"2023-05":103.6,"2023-06":108.4,"2023-07":105.8,"2023-08":102.5,"2023-09":98.2,"2023-10":95.8,"2023-11":105.3,"2023-12":110.2,"2024-01":112.5,"2024-02":115.8,"2024-03":118.2,"2024-04":108.5,"2024-05":105.2,"2024-06":102.8,"2024-07":107.5,"2024-08":112.3,"2024-09":115.8,"2024-10":118.2,"2024-11":120.5,"2024-12":115.2,"2025-01":118.5,"2025-02":122.3,"2025-03":125.8,"2025-04":128.2,"2025-05":130.5,"2025-06":132.8,"2025-07":135.2,"2025-08":133.5,"2025-09":130.8,"2025-10":128.2,"2025-11":126.5,"2025-12":125.8},
    "BBNX":{"2025-02":25.0,"2025-03":28.5,"2025-04":32.1,"2025-05":30.5,"2025-06":28.8,"2025-07":26.5,"2025-08":24.2,"2025-09":22.8,"2025-10":20.5,"2025-11":18.2,"2025-12":16.5},
    "MDT":{"2023-01":78.5,"2023-02":80.2,"2023-03":82.5,"2023-04":85.3,"2023-05":83.8,"2023-06":86.5,"2023-07":88.2,"2023-08":85.6,"2023-09":80.2,"2023-10":78.5,"2023-11":82.3,"2023-12":84.5,"2024-01":86.2,"2024-02":85.8,"2024-03":87.5,"2024-04":82.3,"2024-05":80.5,"2024-06":78.8,"2024-07":80.2,"2024-08":82.5,"2024-09":85.8,"2024-10":88.2,"2024-11":90.5,"2024-12":88.2,"2025-01":86.5,"2025-02":85.2,"2025-03":83.8,"2025-04":82.5,"2025-05":84.2,"2025-06":86.8,"2025-07":88.5,"2025-08":90.2,"2025-09":92.5,"2025-10":94.8,"2025-11":96.2,"2025-12":95.5},
}
_stock_source = "HARDCODED"
PRODUCT_EVENTS = {
    "DXCM_G7":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-04","label":"G7 15-Day Cleared","type":"launch"},{"date":"2025-05","label":"Class I Recall (Receiver)","type":"recall"}],
    "DXCM_G6":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-01","label":"G6 Receiver SW Recall","type":"recall"},{"date":"2025-05","label":"Class I Recall (Receiver)","type":"recall"}],
    "DXCM_ALL":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-05","label":"Class I Recall","type":"recall"}],
    "PODD_OP5":[{"date":"2024-09","label":"OP5 Gen2 Launch","type":"launch"}],"PODD_DASH":[],"PODD_ALL":[{"date":"2024-09","label":"OP5 Gen2 Launch","type":"launch"}],
    "TNDM_TSLIM":[{"date":"2024-06","label":"Mobi Launch","type":"launch"}],"TNDM_MOBI":[{"date":"2024-06","label":"Mobi Cleared","type":"launch"}],"TNDM_ALL":[{"date":"2024-06","label":"Mobi Launch","type":"launch"}],
    "ABT_LIBRE":[],"ABT_ALL":[],"BBNX_ILET":[{"date":"2025-02","label":"IPO","type":"launch"}],
    "MDT_780G":[],"MDT_ALL":[],"SQEL_TWIIST":[],
}
DEVICES = [
    {"id":"DXCM_G7","name":"Dexcom G7","search":"dexcom+g7","ticker":"DXCM","rev_key":"DXCM","company":"Dexcom","is_combined":False},
    {"id":"DXCM_G6","name":"Dexcom G6","search":"dexcom+g6","ticker":"DXCM","rev_key":"DXCM","company":"Dexcom","is_combined":False},
    {"id":"DXCM_ALL","name":"Dexcom (All CGM)","search":"dexcom","ticker":"DXCM","rev_key":"DXCM","company":"Dexcom","is_combined":True},
    {"id":"PODD_OP5","name":"Omnipod 5","search":"omnipod+5","ticker":"PODD","rev_key":"PODD","company":"Insulet","is_combined":False},
    {"id":"PODD_DASH","name":"Omnipod DASH","search":"omnipod+dash","ticker":"PODD","rev_key":"PODD","company":"Insulet","is_combined":False},
    {"id":"PODD_ALL","name":"Insulet (All Omnipod)","search":"omnipod","ticker":"PODD","rev_key":"PODD","company":"Insulet","is_combined":True},
    {"id":"TNDM_TSLIM","name":"t:slim X2","search":"tandem+t:slim","ticker":"TNDM","rev_key":"TNDM","company":"Tandem","is_combined":False},
    {"id":"TNDM_MOBI","name":"Tandem Mobi","search":"tandem+mobi","ticker":"TNDM","rev_key":"TNDM","company":"Tandem","is_combined":False},
    {"id":"TNDM_ALL","name":"Tandem (All Pumps)","search":"tandem+diabetes","ticker":"TNDM","rev_key":"TNDM","company":"Tandem","is_combined":True},
    {"id":"ABT_LIBRE","name":"Abbott FreeStyle Libre","search":"freestyle+libre","ticker":"ABT","rev_key":"ABT_LIBRE","company":"Abbott","is_combined":False},
    {"id":"ABT_ALL","name":"Abbott (All Libre)","search":"abbott+libre","ticker":"ABT","rev_key":"ABT_LIBRE","company":"Abbott","is_combined":True},
    {"id":"BBNX_ILET","name":"Beta Bionics iLet","search":"beta+bionics+ilet","ticker":"BBNX","rev_key":"BBNX","company":"Beta Bionics","is_combined":True},
    {"id":"MDT_780G","name":"Medtronic 780G","search":"medtronic+780g","ticker":"MDT","rev_key":"MDT_DM","company":"Medtronic","is_combined":False},
    {"id":"MDT_ALL","name":"Medtronic (All DM)","search":"medtronic+insulin+pump","ticker":"MDT","rev_key":"MDT_DM","company":"Medtronic","is_combined":True},
    {"id":"SQEL_TWIIST","name":"Sequel twiist","search":"sequel+twiist","ticker":"SQEL","rev_key":"SQEL","company":"Sequel","is_combined":True},
]
COMPANIES = list(dict.fromkeys(d["company"] for d in DEVICES))

# ============================================================
# LIVE STOCK PRICES VIA YFINANCE
# ============================================================
def fetch_live_stock_prices():
    try: import yfinance as yf
    except ImportError: print("  yfinance not installed"); return {}
    tickers = list(set(d["ticker"] for d in DEVICES if d["ticker"] not in ("SQEL",)))
    result = {}
    for tk in tickers:
        try:
            data = yf.download(tk, period="3y", interval="1mo", progress=False)
            if data is not None and len(data) > 0:
                monthly = {}
                for idx, row in data.iterrows():
                    m = idx.strftime("%Y-%m")
                    cv = row.get("Close") if "Close" in data.columns else row.iloc[3]
                    if hasattr(cv,'item'): cv = cv.item()
                    monthly[m] = round(float(cv), 2)
                result[tk] = monthly; print(f"    {tk}: {len(monthly)} months live")
        except Exception as e: print(f"    {tk}: error {str(e)[:60]}")
    return result

def merge_stock_data(hc, live):
    merged = {}
    for tk in set(list(hc.keys()) + list(live.keys())):
        merged[tk] = dict(hc.get(tk, {}))
        if tk in live: merged[tk].update(live[tk])
    return merged

def get_revenue_staleness():
    try:
        lu = datetime.strptime(REVENUE_LAST_UPDATED, "%Y-%m-%d")
        days = (datetime.now() - lu).days
        if days > 120: return {"stale":True,"days":days,"message":f"STALE ({days}d old)"}
        elif days > 90: return {"stale":False,"days":days,"message":f"Due ({days}d)"}
        else: return {"stale":False,"days":days,"message":f"Current ({days}d)"}
    except: return {"stale":True,"days":999,"message":"Unknown"}

# ============================================================
# OPENFDA DATA FETCHING — WITH API KEY + REAL ERROR LOGGING
# ============================================================
def _api_get(url, retries=3):
    """Fetch JSON from openFDA API with retries, API key, and real error logging."""
    # Append API key if available
    if OPENFDA_API_KEY:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api_key={OPENFDA_API_KEY}"

    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent":"MAUDE-Monitor/3.3"})
            with urlopen(req, timeout=30) as resp:
                raw = resp.read().decode()
                data = json.loads(raw)
                # Check for API error response
                if "error" in data:
                    err = data["error"]
                    print(f"      API ERROR: {err.get('code','?')} - {err.get('message','')[:80]}")
                    if err.get("code") == "TOO_MANY_REQUESTS":
                        time.sleep(10 * (attempt + 1))
                        continue
                    return None
                return data
        except HTTPError as e:
            body = ""
            try: body = e.read().decode()[:200]
            except: pass
            print(f"      HTTP {e.code} on attempt {attempt+1}: {body[:100]}")
            if e.code == 429:
                time.sleep(10 * (attempt + 1))
            elif e.code == 404:
                return None
            else:
                time.sleep(3 * (attempt + 1))
        except URLError as e:
            print(f"      URL ERROR on attempt {attempt+1}: {str(e)[:100]}")
            time.sleep(3 * (attempt + 1))
        except Exception as e:
            print(f"      ERROR on attempt {attempt+1}: {type(e).__name__}: {str(e)[:100]}")
            time.sleep(3 * (attempt + 1))
    print(f"      FAILED after {retries} attempts")
    return None

def fetch_counts(search_query, date_field, start_date):
    url = (f"https://api.fda.gov/device/event.json?"
           f"search=brand_name:{url_quote(search_query)}+AND+"
           f"{date_field}:[{start_date}+TO+now]&count={date_field}")
    data = _api_get(url)
    if not data or "results" not in data: return {}
    counts = {}
    for r in data["results"]:
        d = r.get("time","")
        if len(d) >= 6:
            m = f"{d[:4]}-{d[4:6]}"; counts[m] = counts.get(m, 0) + r.get("count", 0)
    return counts

def fetch_severity(search_query, start_date):
    sev = {"death":{},"injury":{},"malfunction":{}}
    for etype in sev:
        url = (f"https://api.fda.gov/device/event.json?"
               f"search=brand_name:{url_quote(search_query)}+AND+"
               f"event_type:{etype}+AND+date_received:[{start_date}+TO+now]"
               f"&count=date_received")
        data = _api_get(url)
        if data and "results" in data:
            for r in data["results"]:
                d = r.get("time","")
                if len(d)>=6: m=f"{d[:4]}-{d[4:6]}"; sev[etype][m] = sev[etype].get(m,0)+r.get("count",0)
        time.sleep(0.5)
    return sev

# ============================================================
# EVENT-DATE SMOOTHING ENGINE
# ============================================================
def compute_smoothed_series(recv_counts, event_counts):
    all_months = sorted(set(list(recv_counts.keys()) + list(event_counts.keys())))
    if not all_months: return {}
    smoothed = {m: event_counts.get(m, 0) for m in all_months}
    total_recv = sum(recv_counts.get(m, 0) for m in all_months)
    total_evnt = sum(event_counts.get(m, 0) for m in all_months)
    if total_recv <= 0 or total_evnt <= 0:
        return recv_counts.copy()
    for m in all_months:
        rc = recv_counts.get(m, 0)
        ec = event_counts.get(m, 0)
        excess = rc - ec
        if excess > ec * 0.5 and excess > 50:
            idx = all_months.index(m)
            lookback = min(6, idx)
            if lookback > 0:
                prior_months = all_months[idx-lookback:idx]
                prior_total = sum(smoothed.get(pm, 0) for pm in prior_months)
                if prior_total > 0:
                    for pm in prior_months:
                        weight = smoothed.get(pm, 0) / prior_total
                        smoothed[pm] = smoothed.get(pm, 0) + int(excess * weight)
                else:
                    per_month = excess // lookback
                    for pm in prior_months:
                        smoothed[pm] = smoothed.get(pm, 0) + per_month
    sm_total = sum(smoothed.values())
    if sm_total > 0 and total_recv > 0:
        scale = total_recv / sm_total
        smoothed = {m: max(0, int(v * scale)) for m, v in smoothed.items()}
    return smoothed

def detect_batch(recv_counts, event_counts, ticker=None):
    batch = {}
    for m in recv_counts:
        rc = recv_counts.get(m, 0); ec = event_counts.get(m, 0)
        if ec > 0 and rc > 2.5 * ec:
            batch[m] = "batch"
        elif ec > 0 and rc > 1.8 * ec and rc > 100:
            batch[m] = "mild_batch"
        else:
            batch[m] = None
    return batch

# ============================================================
# STATISTICS & SCORING
# ============================================================
def compute_stats(recv_counts, sev_data, ticker, smoothed=None):
    counts = smoothed if smoothed else recv_counts
    if not counts: return None
    months = sorted(counts.keys()); vals = [counts[m] for m in months]; n = len(vals)
    if n < 3: return None
    mean_val = sum(vals)/n
    std_val = math.sqrt(sum((v-mean_val)**2 for v in vals)/n) if n>1 else 1
    if std_val == 0: std_val = 1
    latest_m = months[-1]; latest_v = vals[-1]
    z_score = (latest_v - mean_val)/std_val
    recent = vals[-6:] if n>=6 else vals; nr = len(recent)
    if nr >= 3:
        x_mean=(nr-1)/2.0; y_mean=sum(recent)/nr
        num=sum((i-x_mean)*(recent[i]-y_mean) for i in range(nr))
        den=sum((i-x_mean)**2 for i in range(nr)); slope=num/den if den>0 else 0
    else: slope = 0
    last3 = months[-3:] if n>=3 else months
    deaths_3mo = sum(sev_data.get("death",{}).get(m,0) for m in last3)
    injuries_3mo = sum(sev_data.get("injury",{}).get(m,0) for m in last3)
    malfunctions_3mo = sum(sev_data.get("malfunction",{}).get(m,0) for m in last3)
    rev_key_map = {d["ticker"]:d["rev_key"] for d in DEVICES}
    rk = rev_key_map.get(ticker, ticker)
    yr,mo = latest_m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"
    qrev = QUARTERLY_REVENUE.get(rk,{}).get(q)
    rate_per_m = latest_v/(qrev/3)*1e6 if qrev and qrev>0 else None
    ib = INSTALLED_BASE_K.get(rk,{}).get(q)
    rate_per_10k = latest_v/ib*10000 if ib and ib>0 else None
    s1l=max(0,mean_val-std_val); s1h=mean_val+std_val
    s2l=max(0,mean_val-2*std_val); s2h=mean_val+2*std_val
    ma6 = {}
    for i,m in enumerate(months):
        window = vals[max(0,i-5):i+1]; ma6[m] = sum(window)/len(window)
    raw_vals = [recv_counts.get(m,0) for m in months] if smoothed else vals
    return {"months":months,"values":vals,"raw_values":raw_vals,
            "mean":mean_val,"std":std_val,"z_score":z_score,
            "latest_month":latest_m,"latest_value":latest_v,
            "slope_6mo":slope,"deaths_3mo":deaths_3mo,"injuries_3mo":injuries_3mo,
            "malfunctions_3mo":malfunctions_3mo,"rate_per_m":rate_per_m,
            "rate_per_10k":rate_per_10k,"sigma1_lo":s1l,"sigma1_hi":s1h,
            "sigma2_lo":s2l,"sigma2_hi":s2h,"ma6":ma6}

def compute_r_score(stats):
    if not stats: return None
    s = 0; z = abs(stats["z_score"])
    if z>=3: s+=20
    elif z>=2: s+=15
    elif z>=1.5: s+=10
    elif z>=1: s+=5
    sl = stats["slope_6mo"]
    if sl>100: s+=20
    elif sl>50: s+=15
    elif sl>20: s+=10
    elif sl>0: s+=5
    d = stats["deaths_3mo"]
    if d>=5: s+=20
    elif d>=2: s+=15
    elif d>=1: s+=10
    inj = stats["injuries_3mo"]
    if inj>=50: s+=20
    elif inj>=20: s+=15
    elif inj>=5: s+=10
    elif inj>=1: s+=5
    rpm = stats.get("rate_per_m")
    if rpm:
        if rpm>500: s+=20
        elif rpm>200: s+=15
        elif rpm>100: s+=10
        elif rpm>50: s+=5
    return min(100,s)

# ============================================================
# ALL MODULES (identical to what you have — not changing these)
# ============================================================
def compute_enhanced_correlation(counts, stock_prices, max_lag=6, revenue_dict=None, installed_base_dict=None):
    try:
        if not counts or not stock_prices:
            return {"status":"insufficient_data","message":"Missing data.","best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,"direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        common = sorted(set(counts.keys()) & set(stock_prices.keys()))
        if len(common)<14:
            return {"status":"insufficient_data","message":f"{len(common)} months, need 14+.","best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,"direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        mc = [counts[m] for m in common]; sp = [stock_prices[m] for m in common]
        sr = [0.0]+[(sp[i]-sp[i-1])/sp[i-1]*100 if sp[i-1]>0 else 0.0 for i in range(1,len(sp))]
        signals = {"raw_counts":mc}
        signals["count_delta"] = [0.0]+[mc[i]-mc[i-1] for i in range(1,len(mc))]
        zs = []
        for i in range(len(mc)):
            w = mc[max(0,i-5):i+1]
            if len(w)>=3: mu=sum(w)/len(w); sd=math.sqrt(sum((v-mu)**2 for v in w)/len(w)); zs.append((mc[i]-mu)/sd if sd>0 else 0.0)
            else: zs.append(0.0)
        signals["z_score"] = zs
        if revenue_dict:
            rr = []
            for m in common: yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; qr=revenue_dict.get(q); rr.append(counts[m]/(qr/3)*1e6 if qr and qr>0 else None)
            lv=None
            for i in range(len(rr)):
                if rr[i] is not None: lv=rr[i]
                elif lv is not None: rr[i]=lv
                else: rr[i]=0.0
            signals["rate_per_rev"] = rr
        if installed_base_dict:
            rb = []
            for m in common: yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; ib=installed_base_dict.get(q); rb.append(counts[m]/ib*10000 if ib and ib>0 else None)
            lv=None
            for i in range(len(rb)):
                if rb[i] is not None: lv=rb[i]
                elif lv is not None: rb[i]=lv
                else: rb[i]=0.0
            signals["rate_per_base"] = rb
        dl = signals["count_delta"]; signals["acceleration"] = [0.0,0.0]+[dl[i]-dl[i-1] for i in range(2,len(dl))]
        ob_rho,ob_p,ob_lag,ob_sig = 0,1.0,0,"raw_counts"; sa = {}; llr = {}
        for sn,sv in signals.items():
            br,bp,bl = 0,1.0,0; ld = {}
            for lag in range(0, min(max_lag+1, len(sv)-6)):
                ss = sv[:len(sv)-lag] if lag>0 else sv; rs = sr[lag:] if lag>0 else sr; ml = min(len(ss),len(rs))
                if ml<8: continue
                a,b = ss[:ml],rs[:ml]
                if all(v==a[0] for v in a) or all(v==b[0] for v in b): continue
                rho,p = _proper_spearman(a,b); ld[f"{lag}mo"] = {"rho":rho,"p":p}
                if sn=="raw_counts": llr[f"{lag}mo"]={"rho":rho,"p":p}
                if abs(rho)>abs(br): br,bp,bl = rho,p,lag
            sa[sn] = {"best_rho":br,"best_p":bp,"best_lag":bl,"significant":bp<0.05,"direction":"negative" if br<0 else "positive","lag_detail":ld}
            if abs(br)>abs(ob_rho): ob_rho,ob_p,ob_lag,ob_sig = br,bp,bl,sn
        sc = sum(1 for s in sa.values() if s["significant"]); nc = sum(1 for s in sa.values() if s["significant"] and s["direction"]=="negative")
        aar = sum(abs(s["best_rho"]) for s in sa.values())/max(len(sa),1); con = nc/max(sc,1)
        conf = min(100,int(abs(ob_rho)*40+aar*20+sc/len(sa)*20+con*20)); osig = ob_p<0.05
        msg = f"Best: \u03C1={ob_rho:+.3f} at {ob_lag}mo lag (p={ob_p:.4f}, signal={ob_sig}). "
        if osig and ob_rho<-0.2: msg+=f"MAUDE {ob_sig} predicts declines {ob_lag}mo ahead. "
        elif osig and ob_rho>0.2: msg+="Market pricing in concurrently. "
        else: msg+="No significant lead-lag. "
        msg+=f"Confidence: {conf}/100 ({sc}/{len(sa)} sig, {nc} neg)."
        return {"status":"ok","best_rho":ob_rho,"best_p":ob_p,"best_lag":ob_lag,"significant":osig,"direction":"negative" if ob_rho<0 else "positive","lag_results":llr,"message":msg,"best_signal":ob_sig,"confidence":conf,"signal_analysis":sa,"signals_tested":len(sa),"signals_significant":sc,"signals_negative":nc}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,"direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}

def analyze_failure_modes(sq, start, limit=50):
    url = f"https://api.fda.gov/device/event.json?search=brand_name:{url_quote(sq)}+AND+date_received:[{start}+TO+now]&limit={limit}"
    data = _api_get(url)
    if not data or "results" not in data: return {"status":"no_data","categories":{},"total":0}
    cats = {"sensor_failure":0,"adhesion":0,"connectivity":0,"inaccurate_reading":0,"skin_reaction":0,"alarm_alert":0,"battery":0,"physical_damage":0,"software":0,"insertion":0,"occlusion":0,"other":0}
    kw = {"sensor_failure":["sensor fail","no reading","sensor error","lost signal","signal loss","expired early"],"adhesion":["fell off","adhesive","peel","detach","came off","not stick"],"connectivity":["bluetooth","connect","pair","sync","lost connection","disconnect"],"inaccurate_reading":["inaccurate","wrong reading","false","discrepan","not match","off by"],"skin_reaction":["rash","irritat","red","itch","allerg","skin","welt","blister"],"alarm_alert":["alarm","alert","no sound","speaker","beep","notification","did not alert"],"battery":["battery","charge","power","dead","drain","won't turn on"],"physical_damage":["crack","broke","snap","bent","leak","damage"],"software":["software","app","crash","freeze","update","glitch","display"],"insertion":["insert","needle","pain","bleed","bruis","applicat"],"occlusion":["occlus","block","clog","no deliv","no insulin"]}
    total = 0
    for r in data["results"]:
        for tf in ["mdr_text"]:
            texts = r.get(tf,[])
            if isinstance(texts,list):
                for t in texts:
                    nar = t.get("text","").lower() if isinstance(t,dict) else str(t).lower()
                    if len(nar)<10: continue
                    total+=1; matched=False
                    for cat,kws in kw.items():
                        if any(k in nar for k in kws): cats[cat]+=1; matched=True; break
                    if not matched: cats["other"]+=1
    top = sorted(cats.items(), key=lambda x:-x[1])[:5]
    return {"status":"ok","categories":cats,"total":total,"top_modes":[{"mode":k,"count":v,"pct":round(v/max(total,1)*100,1)} for k,v in top]}

def analyze_edgar_filings(tk):
    if tk in ("SQEL","BBNX"): return {"status":"skip","message":"Limited EDGAR history"}
    cik_map={"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT":"0000001800","MDT":"0000064670"}
    cik=cik_map.get(tk)
    if not cik: return {"status":"no_cik"}
    try:
        url=f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req=Request(url,headers={"User-Agent":"MAUDE-Monitor/3.3 research@example.com"})
        with urlopen(req,timeout=15) as resp: filings=json.loads(resp.read().decode())
        recent=filings.get("filings",{}).get("recent",{}); forms=recent.get("form",[]); dates=recent.get("filingDate",[])
        cutoff=(datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d"); last90=[(f,d) for f,d in zip(forms,dates) if d>=cutoff]
        fc={}
        for f,d in last90: fc[f]=fc.get(f,0)+1
        return {"status":"ok","total_90d":len(last90),"form_counts":fc,"eight_k_count":fc.get("8-K",0),"message":f"{len(last90)} filings 90d ({fc.get('8-K',0)} 8-Ks)"}
    except Exception as e: return {"status":"error","message":str(e)[:100]}

def analyze_insider_trading_detailed(tk):
    if tk in ("SQEL",): return {"status":"skip","message":"Private"}
    cik_map={"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT":"0000001800","MDT":"0000064670","BBNX":"0001828723"}
    cik=cik_map.get(tk)
    if not cik: return {"status":"no_cik"}
    try:
        url=f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req=Request(url,headers={"User-Agent":"MAUDE-Monitor/3.3 research@example.com"})
        with urlopen(req,timeout=15) as resp: filings=json.loads(resp.read().decode())
        recent=filings.get("filings",{}).get("recent",{}); forms=recent.get("form",[]); dates=recent.get("filingDate",[])
        cutoff=(datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")
        f4=sum(1 for f,d in zip(forms,dates) if f in ("4","4/A") and d>=cutoff)
        return {"status":"ok","form4_count_90d":f4,"message":f"{f4} Form 4s in 90d"}
    except Exception as e: return {"status":"error","message":str(e)[:100]}

def analyze_clinical_trials(tk):
    sp_map={"DXCM":"Dexcom","PODD":"Insulet","TNDM":"Tandem+Diabetes","ABT":"Abbott","MDT":"Medtronic","BBNX":"Beta+Bionics","SQEL":"Sequel+AG"}
    sp=sp_map.get(tk,tk)
    try:
        url=f"https://clinicaltrials.gov/api/v2/studies?query.spons={url_quote(sp)}&filter.overallStatus=RECRUITING,NOT_YET_RECRUITING,ACTIVE_NOT_RECRUITING&pageSize=20"
        req=Request(url,headers={"User-Agent":"MAUDE-Monitor/3.3"})
        with urlopen(req,timeout=15) as resp: data=json.loads(resp.read().decode())
        return {"status":"ok","count":len(data.get("studies",[])),"message":f"{len(data.get('studies',[]))} active trials"}
    except Exception as e: return {"status":"error","message":str(e)[:100]}

def analyze_fda_recalls(sq, tk):
    try:
        url=f"https://api.fda.gov/device/recall.json?search=product_description:{url_quote(sq)}&sort=event_date_terminated:desc&limit=10"
        data=_api_get(url)
        if not data or "results" not in data: return {"status":"ok","count":0,"recalls":[],"message":"No recalls"}
        recalls=[{"reason":r.get("reason_for_recall","")[:120],"classification":r.get("classification",""),"status":r.get("status",""),"date":r.get("event_date_terminated","")[:10]} for r in data["results"][:5]]
        c1=sum(1 for r in recalls if r["classification"]=="Class I")
        return {"status":"ok","count":len(data["results"]),"recalls":recalls,"class1_count":c1,"message":f'{len(data["results"])} recalls ({c1} Class I)'}
    except Exception as e: return {"status":"error","message":str(e)[:100]}

def compute_recall_probability(stats, fm, edgar, tk):
    if not stats: return {"status":"insufficient_data","probability":0}
    s=0
    if stats["z_score"]>=3: s+=25
    elif stats["z_score"]>=2: s+=15
    elif stats["z_score"]>=1.5: s+=10
    if stats["deaths_3mo"]>=3: s+=25
    elif stats["deaths_3mo"]>=1: s+=15
    if stats["slope_6mo"]>50: s+=15
    elif stats["slope_6mo"]>20: s+=10
    if fm and fm.get("status")=="ok":
        cats=fm.get("categories",{})
        if cats.get("alarm_alert",0)>5: s+=15
        if cats.get("sensor_failure",0)>10: s+=10
    s=min(100,s); lev = "HIGH" if s>=70 else "MODERATE" if s>=40 else "LOW"
    return {"status":"ok","probability":s,"level":lev,"message":f"Recall prob: {lev} ({s}/100). Heuristic model."}

def compute_peer_relative(r_scores):
    if not r_scores: return {}
    sp=sorted(r_scores.items(), key=lambda x:-x[1]); result={}
    for i,(tk,score) in enumerate(sp):
        rank=i+1; total=len(sp)
        if rank==1: sig="WORST"
        elif rank<=total*0.25: sig="WEAK"
        elif rank>=total*0.75: sig="STRONG"
        elif rank==total: sig="BEST"
        else: sig="NEUTRAL"
        result[tk]={"rank":rank,"total":total,"score":score,"signal":sig,"peers":sp,"message":f"Rank {rank}/{total} ({sig})"}
    return result

def compute_earnings_predictor(stats,corr,insider,trials,fm,tk):
    if not stats: return {"status":"insufficient_data","score":0}
    s=50; factors=[]
    if stats["z_score"]>=2: s-=15; factors.append(("MAUDE z elevated",-15))
    elif stats["z_score"]<=-1: s+=10; factors.append(("MAUDE below avg",+10))
    if stats["slope_6mo"]>30: s-=10; factors.append(("Rising MAUDE trend",-10))
    elif stats["slope_6mo"]<-10: s+=5; factors.append(("Declining trend",+5))
    if corr and corr.get("significant") and corr.get("direction")=="negative": s-=10; factors.append(("Neg MAUDE-stock corr",-10))
    if stats["deaths_3mo"]>=2: s-=10; factors.append(("Recent deaths",-10))
    if stats["injuries_3mo"]>=20: s-=5; factors.append(("Elevated injuries",-5))
    if fm and fm.get("status")=="ok":
        if fm.get("categories",{}).get("alarm_alert",0)>5: s-=5; factors.append(("Alarm issues",-5))
    s=max(0,min(100,s)); out="POSITIVE" if s>=65 else "NEUTRAL" if s>=40 else "NEGATIVE"
    return {"status":"ok","score":s,"outlook":out,"factors":factors,"message":f"Outlook: {out} ({s}/100). Heuristic model."}

def compute_backtest_case_studies(counts, stock_prices, stats, tk, batch_info=None):
    try:
        if not counts or not stock_prices or not stats: return {"status":"insufficient_data","signals":[],"case_studies":[],"summary":{}}
        sm = sorted(set(counts.keys()) & set(stock_prices.keys()))
        if len(sm)<12: return {"status":"insufficient_data","signals":[],"case_studies":[],"summary":{}}
        mc={m:counts[m] for m in sm}; sp={m:stock_prices[m] for m in sm}
        zbm={}
        for i,m in enumerate(sm):
            w=[mc[sm[j]] for j in range(max(0,i-5),i+1)]
            if len(w)>=3: mu=sum(w)/len(w); sd=math.sqrt(sum((v-mu)**2 for v in w)/len(w)); zbm[m]=(mc[m]-mu)/sd if sd>0 else 0.0
            else: zbm[m]=0.0
        rcm={}; prev=None
        for m in sm: rcm[m]=(mc[m]-mc[prev])/mc[prev]*100 if prev and mc.get(prev,0)>0 else 0.0; prev=m
        ZT,ST,CD=1.5,30.0,3; sigs=[]; cases=[]; lsi=-999
        for i,m in enumerate(sm):
            if i-lsi<CD or i<6: continue
            z=zbm.get(m,0); rc=rcm.get(m,0); ib=False
            if batch_info:
                bi=batch_info.get(m)
                if bi in ("batch","mild_batch"): ib=True
            trig=None
            if z>=ZT and not ib: trig=f"Z-spike: {z:+.2f}\u03C3"
            elif rc>=ST and z>=1.0 and not ib: trig=f"MoM surge: +{rc:.0f}%"
            if not trig: continue
            lsi=i; ep=sp[m]; fwd={}
            for hn,hm in [("1mo",1),("2mo",2),("3mo",3),("6mo",6)]:
                if i+hm<len(sm): xm=sm[i+hm]; xp=sp[xm]; ret=(xp-ep)/ep*100; fwd[hn]={"exit_month":xm,"exit_price":round(xp,2),"long_ret":round(ret,2),"short_ret":round(-ret,2),"short_pnl":round(-(xp-ep)/ep*10000,2)}
            sigs.append({"month":m,"z":round(z,2),"mom":round(rc,1),"trigger":trig,"entry":round(ep,2),"fwd":fwd})
            bh=None; bsp=0
            for h,d in fwd.items():
                if d["short_pnl"]>bsp: bsp=d["short_pnl"]; bh=h
            cs={"month":m,"ticker":tk,"trigger":trig,"entry":round(ep,2),"reports":mc[m],"z":round(z,2),"best_h":bh,"best_pnl":round(bsp,2) if bh else 0,"profitable":bsp>0 if bh else False,"fwd":fwd}
            if bh and bsp>0: ed=fwd[bh]; cs["narrative"]=f"{m}: {trig}. {tk} ${ep:.2f}\u2192${ed['exit_price']:.2f} ({bh}). Short P&L: +${bsp:,.0f}/$10K."
            elif bh: ed=fwd[bh]; cs["narrative"]=f"{m}: {trig}. {tk} ${ep:.2f}. Short MISSED. Loss: ${abs(bsp):,.0f}/$10K."
            else: cs["narrative"]=f"{m}: Signal, insufficient fwd data."
            cases.append(cs)
        tot=len(sigs); prof=sum(1 for c in cases if c.get("profitable")); tp=sum(c.get("best_pnl",0) for c in cases if c.get("best_h"))
        hr=prof/tot*100 if tot>0 else 0; ap=tp/tot if tot>0 else 0; gr="STRONG" if hr>=60 else "MODERATE" if hr>=45 else "WEAK"
        return {"status":"ok","signals":sigs,"case_studies":cases,"summary":{"total":tot,"profitable":prof,"hit_rate":round(hr,1),"total_pnl":round(tp,2),"avg_pnl":round(ap,2),"grade":gr,"message":f"{gr}: {hr:.0f}% hit, {tot} signals, P&L: ${tp:+,.0f}/$10K"}}
    except Exception as e: return {"status":"error","message":str(e)[:200],"signals":[],"case_studies":[],"summary":{}}

def analyze_google_trends(tk): return {"status":"framework","message":"Requires pytrends."}
def analyze_short_interest(tk): return {"status":"framework","message":"Requires Yahoo scraping."}
def analyze_payer_coverage(tk):
    c={"DXCM":"Broad commercial+Medicare CGM","PODD":"Broad commercial+Medicare pump","TNDM":"Broad commercial+Medicare pump","ABT":"Broad commercial+Medicare CGM","MDT":"Broad commercial+Medicare pump","BBNX":"Limited (new)","SQEL":"Pre-market"}
    return {"status":"ok","message":c.get(tk,"Unknown")}
def analyze_international(tk): return {"status":"framework","message":"No structured API."}
# ============================================================
# PIPELINE — BULLETPROOF: every step wrapped, falls back to raw
# ============================================================
def run_pipeline(backfill=False, quick=False):
    start = "20230101" if backfill else ("20250901" if quick else "20230101")
    all_res, summary = {}, []
    print("ALL ENHANCED MODULES LOADED (inline)")
    print(f"\n=== Fetching live stock prices ===")
    try:
        live_stocks = fetch_live_stock_prices()
    except Exception as e:
        print(f"  STOCK ERROR: {e}"); live_stocks = {}
    global STOCK_MONTHLY, _stock_source
    STOCK_MONTHLY = merge_stock_data(STOCK_MONTHLY, live_stocks)
    _stock_source = f"LIVE ({len(live_stocks)} tickers via yfinance)" if live_stocks else "HARDCODED (install yfinance for live)"
    print(f"  {_stock_source}")
    rev_status = get_revenue_staleness()
    print(f"  Revenue: {rev_status['message']}")
    r_scores_company = {}

    for dev in DEVICES:
        did = dev["id"]; tk = dev["ticker"]; rk = dev.get("rev_key", tk)
        print(f"\n{'='*50}\n{dev['name']} ({tk})")

        recv = {}
        try:
            print(f"  Fetching date_received...")
            recv = fetch_counts(dev["search"], "date_received", start)
            print(f"    -> {len(recv)} months, total={sum(recv.values()) if recv else 0}")
        except Exception as e:
            print(f"    RECV ERROR: {e}")
        time.sleep(0.3)

        evnt = {}
        try:
            print(f"  Fetching date_of_event...")
            evnt = fetch_counts(dev["search"], "date_of_event", start)
            print(f"    -> {len(evnt)} months, total={sum(evnt.values()) if evnt else 0}")
        except Exception as e:
            print(f"    EVNT ERROR: {e}")
        time.sleep(0.3)

        sev = {"death":{},"injury":{},"malfunction":{}}
        try:
            print(f"  Fetching severity...")
            sev = fetch_severity(dev["search"], start)
            print(f"    -> deaths={sum(sev.get('death',{}).values())}, injuries={sum(sev.get('injury',{}).values())}")
        except Exception as e:
            print(f"    SEV ERROR: {e}")

        smoothed = None
        batch = {}
        if recv:
            try:
                print(f"  Computing smoothed series...")
                smoothed = compute_smoothed_series(recv, evnt)
                if smoothed and len(smoothed) > 0:
                    print(f"    -> {len(smoothed)} months smoothed")
                else:
                    print(f"    -> Smoothing returned empty, using raw")
                    smoothed = None
            except Exception as e:
                print(f"    SMOOTHING ERROR (using raw): {e}")
                smoothed = None

            try:
                batch = detect_batch(recv, evnt, ticker=tk)
            except Exception as e:
                print(f"    BATCH ERROR: {e}")
                batch = {}

        stats = None
        rscore = None
        if recv:
            try:
                print(f"  Computing stats...")
                stats = compute_stats(recv, sev, tk, smoothed=smoothed)
                if stats:
                    print(f"    -> Z={stats['z_score']:+.2f}, latest={stats['latest_value']}")
                else:
                    print(f"    -> stats returned None")
            except Exception as e:
                print(f"    STATS ERROR: {e}")
                try:
                    stats = compute_stats(recv, sev, tk, smoothed=None)
                    if stats: print(f"    -> FALLBACK stats OK: Z={stats['z_score']:+.2f}")
                except Exception as e2:
                    print(f"    FALLBACK STATS ERROR: {e2}")

        if stats:
            rscore = compute_r_score(stats)
            if rscore is not None and dev.get("is_combined"):
                r_scores_company[tk] = rscore

        modules = {"enhanced_corr":None,"failure_modes":None,"google_trends":None,
                   "insider":None,"trials":None,"short_interest":None,"edgar":None,
                   "payer":None,"international":None,"recall_prob":None,
                   "earnings_pred":None,"backtest":None,"peer_relative":None,"recalls":None}

        if HAS_MODULES and stats:
            corr_counts = smoothed if (smoothed and len(smoothed) > 0) else recv

            try:
                print(f"  Running: Enhanced multi-signal correlation...")
                modules["enhanced_corr"] = compute_enhanced_correlation(
                    corr_counts, STOCK_MONTHLY.get(tk, {}), max_lag=6,
                    revenue_dict=QUARTERLY_REVENUE.get(rk, {}),
                    installed_base_dict=INSTALLED_BASE_K.get(rk, {}))
                ec = modules["enhanced_corr"]
                if ec and ec.get("status") == "ok":
                    print(f"    -> rho={ec.get('best_rho',0):+.3f}, conf={ec.get('confidence',0)}")
            except Exception as e:
                print(f"    CORR ERROR: {e}")
                modules["enhanced_corr"] = {"status":"error","message":str(e)[:100]}

            if not did.endswith("_ALL"):
                try:
                    print(f"  Running: Failure mode classification...")
                    modules["failure_modes"] = analyze_failure_modes(dev["search"], start, limit=50)
                except Exception as e:
                    print(f"    FM ERROR: {e}")
                    modules["failure_modes"] = {"status":"error","message":str(e)[:100]}

            is_company = did.endswith("_ALL") or did in ("SQEL_TWIIST","BBNX_ILET")
            if is_company:
                for mod_name, mod_fn in [("google_trends",analyze_google_trends),
                                          ("insider",analyze_insider_trading_detailed),
                                          ("trials",analyze_clinical_trials),
                                          ("short_interest",analyze_short_interest),
                                          ("payer",analyze_payer_coverage)]:
                    try:
                        print(f"  Running: {mod_name}...")
                        modules[mod_name] = mod_fn(tk)
                    except Exception as e:
                        print(f"    {mod_name} ERROR: {e}")
                        modules[mod_name] = {"status":"error","message":str(e)[:100]}

            try:
                print(f"  Running: FDA recalls...")
                modules["recalls"] = analyze_fda_recalls(dev["search"], tk)
            except Exception as e:
                modules["recalls"] = {"status":"error","message":str(e)[:100]}

            if is_company:
                try:
                    print(f"  Running: EDGAR filings...")
                    modules["edgar"] = analyze_edgar_filings(tk)
                except Exception as e:
                    modules["edgar"] = {"status":"error","message":str(e)[:100]}

            try:
                modules["recall_prob"] = compute_recall_probability(stats, modules.get("failure_modes"), modules.get("edgar"), tk)
            except Exception as e:
                print(f"    RECALL_PROB ERROR: {e}")

            try:
                print(f"  Running: Case study backtest...")
                modules["backtest"] = compute_backtest_case_studies(
                    corr_counts, STOCK_MONTHLY.get(tk, {}), stats, tk, batch_info=batch)
                bt = modules["backtest"]
                if bt and bt.get("status") == "ok":
                    s2 = bt.get("summary",{})
                    print(f"    -> {s2.get('grade','?')}: {s2.get('hit_rate',0):.0f}% hit, {s2.get('total',0)} signals")
            except Exception as e:
                print(f"    BACKTEST ERROR: {e}")
                modules["backtest"] = {"status":"error","message":str(e)[:100]}

            try:
                modules["earnings_pred"] = compute_earnings_predictor(
                    stats, modules.get("enhanced_corr"), modules.get("insider"),
                    modules.get("trials"), modules.get("failure_modes"), tk)
            except Exception as e:
                print(f"    EARNINGS ERROR: {e}")

        signal = "NORMAL"
        if rscore is not None:
            if rscore >= 70: signal = "CRITICAL"
            elif rscore >= 50: signal = "ELEVATED"
            elif rscore >= 30: signal = "WATCH"

        all_res[did] = {"device":dev,"stats":stats,"r_score":rscore,"batch":batch,
                        "recv":recv,"evnt":evnt,"sev":sev,
                        "smoothed":smoothed if smoothed else recv,
                        "signal":signal,"modules":modules}
        summary.append({"id":did,"name":dev["name"],"ticker":tk,"signal":signal,
                        "r_score":rscore or 0,"z_score":stats["z_score"] if stats else 0})

        if stats:
            print(f"  >>> Signal: {signal} | R={rscore} | Z={stats['z_score']:+.2f}")
        else:
            print(f"  >>> NO DATA for {dev['name']}")

    if r_scores_company:
        peer_results = compute_peer_relative(r_scores_company)
        for did, res in all_res.items():
            tk = res["device"]["ticker"]
            if tk in peer_results:
                res["modules"]["peer_relative"] = peer_results[tk]

    print(f"\n=== PIPELINE SUMMARY ===")
    for s in summary:
        print(f"  {s['name']:30s} {s['signal']:10s} R={s['r_score']:3d} Z={s['z_score']:+.2f}")
    return all_res, summary


# ============================================================
# HTML HELPERS
# ============================================================
def _accordion(acc_id, title, stat_html, content):
    return (f'<div class="acc"><div class="acch" onclick="toggleAcc(\'{acc_id}\')">'
            f'<span>{title}</span>{stat_html}<span class="arr" id="arr-{acc_id}">\u25B6</span></div>'
            f'<div class="accb" id="{acc_id}" style="display:none">{content}</div></div>')


def _render_corr_content(ec, did):
    if not ec or ec.get("status") != "ok": return ""
    r = ec.get("best_rho",0); p = ec.get("best_p",1.0); lag = ec.get("best_lag",0)
    sig = ec.get("significant",False); conf = ec.get("confidence",0)
    bs = ec.get("best_signal","raw_counts"); sa = ec.get("signal_analysis",{})
    if sig and r < -0.2: ec_col = "#c0392b"
    elif sig and r > 0.2: ec_col = "#e67e22"
    else: ec_col = "var(--tx3)"
    conf_col = "#27ae60" if conf >= 60 else "#f39c12" if conf >= 35 else "#c0392b"
    c = '<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:10px">'
    c += f'<div class="si"><div class="sil">BEST \u03C1</div><div class="siv" style="color:{ec_col}">{r:+.3f}</div></div>'
    c += f'<div class="si"><div class="sil">LAG</div><div class="siv">{lag}mo</div></div>'
    c += f'<div class="si"><div class="sil">P-VALUE</div><div class="siv">{"*" if sig else ""}{p:.4f}</div></div>'
    c += f'<div class="si"><div class="sil">CONFIDENCE</div><div class="siv" style="color:{conf_col}">{conf}/100</div></div></div>'
    c += f'<div class="msub" style="margin-bottom:8px">{ec.get("message","")}</div>'
    if sa:
        sig_labels = {"raw_counts":"Raw Counts","count_delta":"MoM Change","z_score":"Z-Score",
                      "rate_per_rev":"Rate/$M Rev","rate_per_base":"Rate/10K Users","acceleration":"Acceleration"}
        c += '<table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:6px">'
        c += '<tr style="background:rgba(0,0,0,0.05)"><th style="padding:3px;text-align:left">Signal</th><th>Best \u03C1</th><th>Lag</th><th>p-value</th><th>Sig?</th></tr>'
        for sname, sdata in sa.items():
            label = sig_labels.get(sname, sname); srho = sdata.get("best_rho",0)
            slag = sdata.get("best_lag",0); sp2 = sdata.get("best_p",1.0); ssig = sdata.get("significant",False)
            rc = "#27ae60" if ssig and srho < 0 else "#c0392b" if ssig and srho > 0 else "#888"
            star = "\u2713" if ssig else "\u2014"
            bold = ' style="font-weight:600"' if sname == bs else ""
            c += f'<tr{bold}><td style="padding:3px">{"\u2192 " if sname==bs else ""}{label}</td>'
            c += f'<td style="padding:3px;color:{rc};text-align:center">{srho:+.3f}</td>'
            c += f'<td style="padding:3px;text-align:center">{slag}mo</td>'
            c += f'<td style="padding:3px;text-align:center">{sp2:.4f}</td>'
            c += f'<td style="padding:3px;text-align:center">{star}</td></tr>'
        c += '</table>'
    c += '<div class="msub" style="margin-top:8px;font-size:10px;opacity:0.7">Multi-signal: tests 6 MAUDE metrics vs stock returns at 0-6mo lags. Confidence (0-100) = strength + significance + consistency. Rate-normalized strips growth noise.</div>'
    return c


def _render_backtest_content(bt, ticker):
    if not bt or bt.get("status") != "ok": return "<div class='msub'>No case study data available.</div>"
    summary = bt.get("summary",{}); cases = bt.get("case_studies",[])
    grade_colors = {"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}
    grade = summary.get("grade","WEAK"); gc = grade_colors.get(grade,"#888")
    h = '<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:12px">'
    h += f'<div class="si"><div class="sil">GRADE</div><div class="siv" style="color:{gc}">{grade}</div></div>'
    h += f'<div class="si"><div class="sil">HIT RATE</div><div class="siv">{summary.get("hit_rate",0):.0f}%</div></div>'
    h += f'<div class="si"><div class="sil">SIGNALS</div><div class="siv">{summary.get("total",0)}</div></div>'
    pnl = summary.get("total_pnl",0); pc = "#27ae60" if pnl > 0 else "#c0392b"
    h += f'<div class="si"><div class="sil">TOTAL P&L/$10K</div><div class="siv" style="color:{pc}">${pnl:+,.0f}</div></div></div>'
    h += f'<div class="msub" style="margin-bottom:10px">{summary.get("message","")}</div>'
    if cases:
        h += '<table style="width:100%;border-collapse:collapse;font-size:11px">'
        h += '<tr style="background:rgba(0,0,0,0.05)"><th style="padding:4px;text-align:left">Date</th><th>Trigger</th><th>Entry</th><th>Best Exit</th><th>Return</th><th>P&L/$10K</th></tr>'
        for cs in cases[-8:]:
            bh = cs.get("best_h"); fwd = cs.get("fwd",{})
            if bh and bh in fwd:
                ed = fwd[bh]; ret_str = f'{ed["short_ret"]:+.1f}%'
                pv = cs.get("best_pnl",0); pnl_str = f'${pv:+,.0f}'
                rcc = "#27ae60" if pv > 0 else "#c0392b"
                exit_str = f'${ed["exit_price"]:.2f} ({bh})'
            else:
                ret_str = "\u2014"; pnl_str = "\u2014"; rcc = "#888"; exit_str = "\u2014"
            trig = cs.get("trigger","")
            trig = trig[:22]+"..." if len(trig) > 25 else trig
            h += f'<tr><td style="padding:3px">{cs.get("month","")}</td>'
            h += f'<td style="padding:3px;font-size:10px">{trig}</td>'
            h += f'<td style="padding:3px">${cs.get("entry",0):.2f}</td>'
            h += f'<td style="padding:3px">{exit_str}</td>'
            h += f'<td style="padding:3px;color:{rcc}">{ret_str}</td>'
            h += f'<td style="padding:3px;color:{rcc};font-weight:600">{pnl_str}</td></tr>'
        h += '</table>'
    h += '<div class="msub" style="margin-top:8px;font-size:10px;opacity:0.7">Strategy: Short when MAUDE z-score &gt; 1.5\u03C3 or MoM surge &gt; 30%. Batch months excluded. P&L assumes $10K notional. Past performance \u2260 future results.</div>'
    return h


# ============================================================
# HTML DASHBOARD GENERATION
# ============================================================
def generate_html(all_res, summary):
    now = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    rev_status = get_revenue_staleness()
    rev_warn = ' style="color:#c0392b;font-weight:600"' if rev_status.get("stale") else ""

    trows = ""
    for s in sorted(summary, key=lambda x: -x["r_score"]):
        res = all_res.get(s["id"],{})
        st = res.get("stats")
        if not st: continue
        sig_colors = {"CRITICAL":"#c0392b","ELEVATED":"#e67e22","WATCH":"#f1c40f","NORMAL":"#27ae60"}
        sc = sig_colors.get(s["signal"],"#888")
        ec = res.get("modules",{}).get("enhanced_corr")
        corr_str = f'{ec["best_rho"]:+.3f}{"*" if ec.get("significant") else ""}' if ec and ec.get("best_rho") else "\u2014"
        trows += (f'<tr><td>{s["name"]}</td><td>{s["ticker"]}</td><td>{st["latest_month"]}</td>'
                  f'<td>{fmt0(st["latest_value"])}</td><td>{st["z_score"]:+.2f}</td>'
                  f'<td>{s["r_score"]}</td><td>{fmt(st.get("rate_per_m"),1) if st.get("rate_per_m") else "\u2014"}</td>'
                  f'<td>{fmt(st.get("rate_per_10k"),2) if st.get("rate_per_10k") else "\u2014"}</td>'
                  f'<td>{st["slope_6mo"]:+.1f}</td><td>{st["deaths_3mo"]}</td><td>{st["injuries_3mo"]}</td>'
                  f'<td>{corr_str}</td>'
                  f'<td style="color:{sc};font-weight:700">{s["signal"]}</td></tr>')

    cd = {}; company_html = {}; tab_ids = {}
    for comp in COMPANIES:
        tab_ids[comp] = comp.lower().replace(" ","_"); company_html[comp] = ""

    for did, res in all_res.items():
        dev = res["device"]; st = res.get("stats"); batch = res.get("batch",{})
        recv = res.get("recv",{}); sev = res.get("sev",{})
        smoothed = res.get("smoothed",{}); tk = dev["ticker"]; rk = dev.get("rev_key",tk)
        all_r = res.get("modules",{})
        if not st: continue
        months = st["months"]; vals = st["values"]; raw_vals = st.get("raw_values", vals)
        bm = [m for m,v in batch.items() if v is not None and v != False and v != "None"]
        evts = PRODUCT_EVENTS.get(did,[])
        sm_vals = [smoothed.get(m,0) for m in months] if isinstance(smoothed, dict) else vals
        rate_m_vals = []
        for m in months:
            yr,mo = m.split("-"); q = f"{yr}-Q{(int(mo)-1)//3+1}"
            src = smoothed if isinstance(smoothed, dict) else recv
            qrev = QUARTERLY_REVENUE.get(rk,{}).get(q)
            rate_m_vals.append(round(src.get(m,0)/(qrev/3)*1e6, 1) if qrev and qrev > 0 else None)
        rate_10k_vals = []
        for m in months:
            yr,mo = m.split("-"); q = f"{yr}-Q{(int(mo)-1)//3+1}"
            src = smoothed if isinstance(smoothed, dict) else recv
            ib = INSTALLED_BASE_K.get(rk,{}).get(q)
            rate_10k_vals.append(round(src.get(m,0)/ib*10000, 2) if ib and ib > 0 else None)
        death_vals = [sev.get("death",{}).get(m,0) for m in months]
        injury_vals = [sev.get("injury",{}).get(m,0) for m in months]
        malf_vals = [sev.get("malfunction",{}).get(m,0) for m in months]
        z_vals = []
        for i in range(len(vals)):
            window = vals[max(0,i-5):i+1]
            if len(window) >= 3:
                mu = sum(window)/len(window)
                sd = math.sqrt(sum((v-mu)**2 for v in window)/len(window))
                z_vals.append(round((vals[i]-mu)/sd, 2) if sd > 0 else 0)
            else: z_vals.append(0)
        ma6_vals = [round(v,1) for v in st["ma6"].values()]
        stk = STOCK_MONTHLY.get(tk,{}); stk_vals = [stk.get(m) for m in months]

        cd[did] = {"l":months,"v":raw_vals,"sm":sm_vals,"bm":bm,"evts":evts,"ma6":ma6_vals,
                   "s1l":round(st["sigma1_lo"],1),"s1h":round(st["sigma1_hi"],1),
                   "s2l":round(st["sigma2_lo"],1),"s2h":round(st["sigma2_hi"],1),
                   "rm":rate_m_vals,"r10k":rate_10k_vals,
                   "deaths":death_vals,"injuries":injury_vals,"malfs":malf_vals,
                   "z":z_vals,"stk":stk_vals}

        sig_colors2 = {"CRITICAL":"#c0392b","ELEVATED":"#e67e22","WATCH":"#f1c40f","NORMAL":"#27ae60"}
        sc = sig_colors2.get(res["signal"],"#888")
        card = f'<div class="card" data-company="{dev["company"]}" data-signal="{res["signal"]}" data-view="{"combined" if dev.get("is_combined") else "individual"}">'
        card += f'<div class="ch"><span class="cn">{dev["name"]}</span><span class="cs" style="background:{sc}">{res["signal"]}</span></div>'
        card += f'<div class="sg"><div class="si"><div class="sil">R-SCORE</div><div class="siv">{res.get("r_score",0)}</div></div>'
        card += f'<div class="si"><div class="sil">Z-SCORE</div><div class="siv">{st["z_score"]:+.2f}</div></div>'
        card += f'<div class="si"><div class="sil">REPORTS</div><div class="siv">{fmt0(st["latest_value"])}</div></div>'
        card += f'<div class="si"><div class="sil">TREND</div><div class="siv">{st["slope_6mo"]:+.1f}</div></div></div>'
        card += f'<div class="cc" id="cc-{did}"><div class="cbtns">'
        for btn_id, btn_label in [("reports","Reports"),("smoothed","Smoothed"),("rate_m","Rate/$M"),
                                   ("rate_10k","Rate/10K"),("severity","Severity"),("z","Z-Score"),("stock","Stock")]:
            active = " active" if btn_id == "reports" else ""
            card += f'<button class="cb{active}" data-v="{btn_id}">{btn_label}</button>'
        card += '<button class="cb rst" data-v="reset">Reset Zoom</button></div>'
        card += f'<div class="cdesc" id="cdesc-{did}"></div>'
        card += f'<div class="cwrap"><canvas id="ch-{did}"></canvas></div></div>'

        acc_html = ""
        fm = all_r.get("failure_modes")
        if fm and isinstance(fm,dict) and fm.get("status") == "ok":
            top = fm.get("top_modes",[])
            fm_content = '<div class="sg" style="grid-template-columns:repeat(3,1fr)">'
            for t in top[:3]: fm_content += f'<div class="si"><div class="sil">{t["mode"].upper()}</div><div class="siv">{t["count"]} ({t["pct"]}%)</div></div>'
            fm_content += '</div>'
            acc_html += _accordion(f"fm-{did}","Failure Mode Classification",f'<span class="mstat">{fm.get("total",0)} analyzed</span>',fm_content)

        ecc = all_r.get("enhanced_corr")
        if ecc and isinstance(ecc,dict) and ecc.get("status") == "ok":
            ec_rho = ecc.get("best_rho",0); ec_sig = ecc.get("significant",False); ec_conf = ecc.get("confidence",0)
            if ec_sig and ec_rho < -0.2: ec_col = "#c0392b"
            elif ec_sig and ec_rho > 0.2: ec_col = "#e67e22"
            else: ec_col = "var(--tx3)"
            conf_col = "#27ae60" if ec_conf >= 60 else "#f39c12" if ec_conf >= 35 else "#c0392b"
            acc_html += _accordion(f"corr-{did}","MAUDE-Stock Correlation (Multi-Signal)",
                f'<span class="mstat" style="color:{ec_col}">\u03C1={ec_rho:+.3f}</span> <span class="mstat" style="color:{conf_col};font-size:11px">[{ec_conf}/100]</span>',
                _render_corr_content(ecc, did))

        bt = all_r.get("backtest")
        if bt and isinstance(bt,dict) and bt.get("status") == "ok":
            bts = bt.get("summary",{}); grade = bts.get("grade","?"); hit = bts.get("hit_rate",0)
            gc = {"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}.get(grade,"#888")
            acc_html += _accordion(f"bt-{did}","Trade Signal Backtest (Case Studies)",
                f'<span class="mstat" style="color:{gc}">{grade} ({hit:.0f}%)</span>',
                _render_backtest_content(bt, tk))

        ep = all_r.get("earnings_pred")
        if ep and isinstance(ep,dict) and ep.get("status") == "ok":
            epo = ep.get("outlook","?"); eps2 = ep.get("score",50)
            ep_col = "#27ae60" if epo == "POSITIVE" else "#c0392b" if epo == "NEGATIVE" else "#f39c12"
            ep_content = f'<div class="msub">{ep.get("message","")}</div>'
            for fname, fval in ep.get("factors",[]):
                fc = "#c0392b" if fval < 0 else "#27ae60"
                ep_content += f'<div style="font-size:11px;padding:2px 0"><span style="color:{fc}">{fval:+d}</span> {fname}</div>'
            ep_content += '<div class="msub" style="font-size:10px;opacity:0.7;margin-top:6px">Heuristic scoring model, not ML-trained.</div>'
            acc_html += _accordion(f"ep-{did}","Earnings Predictor (Heuristic)",f'<span class="mstat" style="color:{ep_col}">{epo} ({eps2})</span>',ep_content)

        rp = all_r.get("recall_prob")
        if rp and isinstance(rp,dict) and rp.get("status") == "ok":
            rpl = rp.get("level","?"); rpp = rp.get("probability",0)
            rp_col = "#c0392b" if rpl == "HIGH" else "#f39c12" if rpl == "MODERATE" else "#27ae60"
            acc_html += _accordion(f"rp-{did}","Recall Probability (Heuristic)",f'<span class="mstat" style="color:{rp_col}">{rpl} ({rpp})</span>',f'<div class="msub">{rp.get("message","")}</div>')

        pr = all_r.get("peer_relative")
        if pr and isinstance(pr,dict):
            prs = pr.get("signal","?"); prc2 = "#c0392b" if prs in ("WORST","WEAK") else "#27ae60" if prs in ("BEST","STRONG") else "var(--tx3)"
            pr_content = f'<div class="msub">{pr.get("message","")}</div>'
            for ptk, pscore in pr.get("peers",[]): pr_content += f'<div style="font-size:11px;{"font-weight:600" if ptk==tk else ""}">{ptk}: R={pscore}</div>'
            acc_html += _accordion(f"pr-{did}","Peer-Relative Ranking",f'<span class="mstat" style="color:{prc2}">{prs}</span>',pr_content)

        rc = all_r.get("recalls")
        if rc and isinstance(rc,dict) and rc.get("status") == "ok" and rc.get("count",0) > 0:
            rc_content = ''.join(f'<div style="padding:3px 0;border-bottom:1px solid rgba(0,0,0,0.1);font-size:11px"><strong>{r.get("classification","")}</strong> ({r.get("status","")}) \u2014 {r.get("reason","")[:100]}</div>' for r in rc.get("recalls",[])[:3])
            acc_html += _accordion(f"rc-{did}","FDA Recalls (Live)",f'<span class="mstat">{rc["count"]} found</span>',rc_content)

        for mn,ml in [("edgar","SEC Filing Activity"),("insider","Insider Trading (Form 4)"),("trials","Clinical Trials"),("payer","Payer Coverage")]:
            md = all_r.get(mn)
            if md and isinstance(md,dict) and md.get("status") == "ok":
                acc_html += _accordion(f"{mn}-{did}",ml,f'<span class="mstat">Data</span>',f'<div class="msub">{md.get("message","")}</div>')

        card += f'<div class="mods">{acc_html}</div></div>'
        company_html[dev["company"]] = company_html.get(dev["company"],"") + card

    html = f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>MAUDE Monitor V3.3</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<style>
:root{{--bg:#f8f9fa;--card:#fff;--tx1:#1a1a2e;--tx2:#444;--tx3:#888;--bdr:#e0e0e0;--acc:#f0f4f8}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--tx1);padding:12px;max-width:1400px;margin:0 auto}}
h1{{font-size:1.5em;margin-bottom:4px}} h2{{font-size:1.2em;margin:16px 0 8px}}
.hdr{{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:16px 20px;border-radius:12px;margin-bottom:16px}}
.hdr small{{opacity:0.7;font-size:12px}} .hdr .warn{{color:#f39c12;font-size:11px}}
.tabs{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px;background:var(--card);padding:8px;border-radius:8px;border:1px solid var(--bdr)}}
.tab{{padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;border:none;background:transparent;color:var(--tx2)}}
.tab:hover{{background:var(--acc)}} .tab.active{{background:#1a1a2e;color:#fff}}
.tabcontent{{display:none}} .tabcontent.active{{display:block}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:16px}}
.card{{background:var(--card);border:1px solid var(--bdr);border-radius:10px;padding:14px;box-shadow:0 1px 3px rgba(0,0,0,0.05)}}
.ch{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}}
.cn{{font-weight:700;font-size:14px}} .cs{{padding:2px 8px;border-radius:4px;color:#fff;font-size:11px;font-weight:600}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px}}
.si{{text-align:center;padding:4px;background:var(--acc);border-radius:6px}}
.sil{{font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:0.5px}}
.siv{{font-size:16px;font-weight:700;margin-top:2px}}
.cc{{margin:8px 0}} .cbtns{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:6px}}
.cb{{padding:3px 8px;border:1px solid var(--bdr);border-radius:4px;cursor:pointer;font-size:11px;background:var(--card)}}
.cb:hover{{background:var(--acc)}} .cb.active{{background:#1a1a2e;color:#fff;border-color:#1a1a2e}}
.cb.rst{{background:transparent;border-style:dashed;font-size:10px}}
.cdesc{{font-size:11px;color:var(--tx3);margin-bottom:4px;min-height:14px}}
.cwrap{{height:220px;position:relative}}
.mods{{margin-top:10px}}
.acc{{border:1px solid var(--bdr);border-radius:6px;margin-bottom:4px;overflow:hidden}}
.acch{{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;cursor:pointer;font-size:12px;font-weight:600;background:var(--acc)}}
.acch:hover{{background:#e4e8ee}}
.accb{{padding:10px;font-size:12px}}
.arr{{font-size:10px;transition:transform 0.2s}} .arr.open{{transform:rotate(90deg)}}
.mstat{{font-size:12px;font-weight:600;margin:0 8px}}
.msub{{font-size:12px;color:var(--tx2);line-height:1.5;margin:4px 0}}
.mok{{color:#27ae60}} .mwarn{{color:#f39c12}} .mcrit{{color:#c0392b}}
.filters{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;font-size:13px}}
.filters select{{padding:4px 8px;border:1px solid var(--bdr);border-radius:4px;font-size:12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}} th,td{{padding:6px 8px;text-align:left;border-bottom:1px solid var(--bdr)}}
th{{background:var(--acc);font-weight:600;font-size:11px;text-transform:uppercase}}
.disc{{margin-top:20px;padding:12px;background:var(--acc);border-radius:8px;font-size:11px;color:var(--tx3);line-height:1.6}}
.gi{{padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px}}
.gi h4{{font-size:12px;margin-bottom:4px}} .gi p{{font-size:11px;color:var(--tx2);line-height:1.4}}
</style></head><body>
<div class="hdr">
<h1>MAUDE Monitor V3.3 \u2014 Event-Date Smoothed Intelligence</h1>
<small>Updated: {now} | Stock data: {_stock_source} | {len(DEVICES)} products tracked | \u2728 Smoothing: ON</small><br>
<small{rev_warn}>Revenue data: {rev_status["message"]} (last updated {REVENUE_LAST_UPDATED})</small>
</div>
<div class="tabs">
<div class="tab active" onclick="showTab('overview')">Overview</div>
<div class="tab" onclick="showTab('guide')">Guide</div>'''
    for comp in COMPANIES: html += f'\n<div class="tab" onclick="showTab(\'{tab_ids[comp]}\')">{comp}</div>'
    html += f'''</div>
<div class="tabcontent active" id="tc-overview">
<div class="filters"><label>Company:</label><select id="fc" onchange="af()"><option value="all">All</option>'''
    for c in COMPANIES: html += f'<option value="{c}">{c}</option>'
    html += '''</select><label>Signal:</label><select id="fs" onchange="af()"><option value="all">All</option>
<option value="CRITICAL">Critical</option><option value="ELEVATED">Elevated+</option>
<option value="WATCH">Watch+</option></select>
<label>View:</label><select id="fv" onchange="af()"><option value="all">All Products</option>
<option value="combined">Company-Level Only</option><option value="individual">Individual Products Only</option></select></div>'''
    html += f'''<h2>All Products \u2014 Latest Month</h2>
<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>6mo Trend</th><th>Deaths (3mo)</th><th>Injuries (3mo)</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div></div>'''
    html += '''<div class="tabcontent" id="tc-guide"><h2>How to Read This Dashboard</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px;margin-top:8px">
<div class="gi"><h4>\u2728 Event-Date Smoothing (V3.3)</h4><p>Raw MAUDE data spikes when manufacturers batch-dump late reports. V3.3 uses date_of_event as the TRUE signal and redistributes batch excess across prior months. "Reports" = RAW. "Smoothed" = adjusted. Analytics run on smoothed when available, raw as fallback.</p></div>
<div class="gi"><h4>R-Score (0-100)</h4><p>Composite risk score. 0=safe, 100=critical. Weights: Z-score (20), trend (20), deaths (20), injuries (20), rate (20). R\u226550=investigate, R\u226570=act.</p></div>
<div class="gi"><h4>Z-Score</h4><p>Standard deviations from mean. Z\u22652=anomalous (95th percentile). Z\u22653=extreme.</p></div>
<div class="gi"><h4>Rate/$M Revenue</h4><p>Reports per $M revenue. Normalizes for business SIZE. Rising = quality deteriorating relative to revenue.</p></div>
<div class="gi"><h4>Rate/10K Users</h4><p>Reports per 10K installed base. Most precise normalization.</p></div>
<div class="gi"><h4>6mo Trend (Slope)</h4><p>Linear regression slope of last 6 months. Positive = accelerating problem.</p></div>
<div class="gi"><h4>Deaths/Injuries (3mo)</h4><p>Deaths weighted 10x in severity score. MAUDE-reported, not confirmed causal.</p></div>
<div class="gi"><h4>Batch Detection</h4><p>Orange bars. Compares date_received vs date_of_event. Excess = batch dump. Smoothed view strips this noise.</p></div>
<div class="gi"><h4>Multi-Signal Correlation</h4><p>Tests 6 MAUDE signals against stock returns at 0-6mo lags. Confidence 0-100.</p></div>
<div class="gi"><h4>Case Study Backtest</h4><p>PM-ready trade signals: date, entry, exit, P&L. Grade based on hit rate. Batch months excluded.</p></div>
</div></div>'''
    for comp in COMPANIES:
        tid = tab_ids[comp]; ch = company_html.get(comp,"<p>No data.</p>")
        html += f'\n<div class="tabcontent" id="tc-{tid}"><h2>{comp}</h2><div class="grid">{ch}</div></div>'
    html += '\n<div class="disc">V3.3: Event-date smoothing strips batch-dump noise. Research only. Not investment advice. MAUDE has 30-90 day lag. Revenue from SEC filings. Installed base from earnings calls. MDT stock = parent (~8% diabetes). BBNX from Feb 2025 IPO. Sequel is private. Heuristic scoring, not ML. Correlation \u2260 causation.</div></div>'

    js = r'''<script>
var defined_cd=__CD__;var charts={};
var chartDescs={"reports":"RAW REPORTS (date_received): Shows what FDA received each month including batch dumps. Orange = batch months. Use Smoothed for true signal.","smoothed":"SMOOTHED REPORTS (event-date adjusted): Redistributes batch excess across actual event months. Green = smoothed, dashed gray = raw reference.","rate_m":"RATE PER $M REVENUE: Reports / monthly revenue. RISING rate = quality deteriorating faster than revenue growth.","rate_10k":"RATE PER 10K USERS: Reports / installed base. Most precise normalization.","severity":"SEVERITY: Deaths (red), injuries (orange), malfunctions (yellow). Deaths weighted 10x in R-score.","z":"Z-SCORE: Monthly z-scores. Crossing +2 = anomalous. Sustained elevation = systemic issue.","stock":"STOCK OVERLAY: Green = stock price. Red = MAUDE counts. Look for MAUDE spikes preceding stock declines by 1-4 months."};
function showTab(id){document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active")});document.querySelectorAll(".tabcontent").forEach(function(t){t.classList.remove("active")});var ct=document.querySelector('.tab[onclick*="'+id+'"]');if(ct)ct.classList.add("active");var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");}
function toggleAcc(id){var el=document.getElementById(id);var arr=document.getElementById("arr-"+id);if(el.style.display==="none"){el.style.display="block";if(arr)arr.classList.add("open");}else{el.style.display="none";if(arr)arr.classList.remove("open");}}
function af(){var fc=document.getElementById("fc").value;var fs=document.getElementById("fs").value;var fv=document.getElementById("fv").value;var sigs={"all":[],"CRITICAL":["CRITICAL"],"ELEVATED":["CRITICAL","ELEVATED"],"WATCH":["CRITICAL","ELEVATED","WATCH"]};var allowed=sigs[fs]||[];document.querySelectorAll(".card").forEach(function(c){var comp=c.getAttribute("data-company");var sig=c.getAttribute("data-signal");var view=c.getAttribute("data-view");var show=true;if(fc!=="all"&&comp!==fc)show=false;if(fs!=="all"&&allowed.indexOf(sig)<0)show=false;if(fv!=="all"&&view!==fv)show=false;c.style.display=show?"":"none";});}
function init(){for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}document.querySelectorAll(".cc").forEach(function(cc){cc.querySelectorAll(".cb").forEach(function(btn){btn.addEventListener("click",function(){var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}mycc.querySelectorAll(".cb:not(.rst)").forEach(function(s){s.classList.remove("active")});this.classList.add("active");var descEl=document.getElementById("cdesc-"+did);if(descEl&&chartDescs[v]){descEl.textContent=chartDescs[v];}mk(did,defined_cd[did],v);});});});}
function mk(did,D,v){var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];var evtMs=evts.map(function(e){return e.date;});var ann={};
if(v==="reports"){var bc=D.l.map(function(m,i){return bm.indexOf(m)>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(m)>=0?"rgba(192,57,43,0.5)":"rgba(39,174,96,0.4)";});ds=[{type:"bar",label:"Reports",data:D.v,backgroundColor:bc,borderWidth:0,order:2},{type:"line",label:"6mo MA",data:D.ma6,borderColor:"#1a6b3a",borderWidth:2,pointRadius:0,fill:false,order:1}];yL="Monthly Reports";ann["s1"]={type:"box",yMin:D.s1l,yMax:D.s1h,backgroundColor:"rgba(39,174,96,0.08)",borderWidth:0};ann["s2"]={type:"box",yMin:D.s2l,yMax:D.s2h,backgroundColor:"rgba(39,174,96,0.04)",borderWidth:0};}
else if(v==="smoothed"){var sma=[];for(var i=0;i<D.sm.length;i++){var s=0,c=0;for(var j=Math.max(0,i-5);j<=i;j++){s+=D.sm[j];c++;}sma.push(Math.round(s/c));}ds=[{type:"bar",label:"Smoothed",data:D.sm,backgroundColor:"rgba(39,174,96,0.5)",borderWidth:0,order:3},{type:"line",label:"Raw (reference)",data:D.v,borderColor:"rgba(0,0,0,0.15)",borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4],order:1},{type:"line",label:"Smoothed MA",data:sma,borderColor:"#1a6b3a",borderWidth:2,pointRadius:0,fill:false,order:2}];yL="Reports (Smoothed)";}
else if(v==="rate_m"){ds=[{type:"bar",label:"Rate/$M",data:D.rm,backgroundColor:"rgba(52,152,219,0.5)",borderWidth:0}];yL="Reports per $M Revenue";}
else if(v==="rate_10k"){ds=[{type:"bar",label:"Rate/10K",data:D.r10k,backgroundColor:"rgba(155,89,182,0.5)",borderWidth:0}];yL="Reports per 10K Users";}
else if(v==="severity"){ds=[{type:"bar",label:"Deaths",data:D.deaths,backgroundColor:"rgba(192,57,43,0.7)",borderWidth:0},{type:"bar",label:"Injuries",data:D.injuries,backgroundColor:"rgba(230,126,34,0.6)",borderWidth:0},{type:"bar",label:"Malfunctions",data:D.malfs,backgroundColor:"rgba(241,196,15,0.5)",borderWidth:0}];yL="Count";}
else if(v==="z"){ds=[{type:"line",label:"Z-Score",data:D.z,borderColor:"#2980b9",borderWidth:2,pointRadius:1,fill:false}];yL="Z-Score";ann["z2"]={type:"line",yMin:2,yMax:2,borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[4,4]};ann["z0"]={type:"line",yMin:0,yMax:0,borderColor:"rgba(0,0,0,0.2)",borderWidth:1};}
else if(v==="stock"){var sv=D.stk||[];ds=[{type:"line",label:"Stock $",data:sv,borderColor:"rgba(39,174,96,0.8)",borderWidth:2,pointRadius:0,fill:false,yAxisID:"y"},{type:"bar",label:"MAUDE",data:D.sm,backgroundColor:"rgba(192,57,43,0.3)",borderWidth:0,yAxisID:"y2"}];yL="Stock Price ($)";}
var opts={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:ds.length>1,position:"top",labels:{font:{size:10}}},zoom:{zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:"x"},pan:{enabled:true,mode:"x"}}},scales:{x:{ticks:{font:{size:9},maxRotation:45}},y:{title:{display:true,text:yL,font:{size:10}},ticks:{font:{size:9}}}}};
if(v==="stock"){opts.scales["y2"]={position:"right",title:{display:true,text:"MAUDE Reports",font:{size:10}},grid:{display:false},ticks:{font:{size:9}}};}
if(Object.keys(ann).length>0){opts.plugins.annotation={annotations:ann};}
charts[did]=new Chart(ctx,{data:{labels:D.l,datasets:ds},options:opts});}
document.addEventListener("DOMContentLoaded",init);
</script>'''

    full_html = html + js + "</body></html>"
    full_html = full_html.replace("__CD__", json.dumps(cd))
    with open("docs/index.html","w") as f: f.write(full_html)
    print(f"\nDashboard written: docs/index.html ({len(full_html)//1024}KB)")


def send_alerts(summary):
    to,fr,pw = os.environ.get("MAUDE_EMAIL_TO"),os.environ.get("MAUDE_EMAIL_FROM"),os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl = [s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body = "MAUDE Monitor V3.3 Alert\n\n"
    for s in fl: body += f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg = MIMEMultipart(); msg["From"],msg["To"] = fr,to; msg["Subject"] = f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv: srv.starttls();srv.login(fr,pw);srv.send_message(msg)
    except: pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--html", action="store_true")
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--quick", action="store_true")
    a = p.parse_args()
    print(f"MAUDE Monitor V3.3 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} products | Modules: ALL (inline) | Smoothing: ON")
    r, s = run_pipeline(a.backfill, a.quick)
    generate_html(r, s)
    send_alerts(s)
    print(f"\nCOMPLETE | docs/index.html")

if __name__ == "__main__":
    main()
