#!/usr/bin/env python3
"""MAUDE Monitor V3.3 — Event-Date Smoothing, Multi-Signal Correlation, PM Case Studies.
Core fix: uses date_of_event as true signal, date_received for batch detection.
Smoothed series redistributes batch spikes across actual event months.
All modules inline. No external deps beyond stdlib + yfinance (optional)."""
import json,os,time,math,argparse,smtplib,csv,re
from datetime import datetime,timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

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
# OPENFDA DATA FETCHING
# ============================================================
def _api_get(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent":"MAUDE-Monitor/3.3"})
            with urlopen(req, timeout=30) as resp: return json.loads(resp.read().decode())
        except: 
            if attempt < retries-1: time.sleep(2*(attempt+1))
            else: return None

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
        time.sleep(0.3)
    return sev

# ============================================================
# EVENT-DATE SMOOTHING ENGINE (THE CORE FIX)
# ============================================================
def compute_smoothed_series(recv_counts, event_counts):
    """
    The KEY insight: date_received spikes when manufacturers batch-dump
    late reports (especially after recalls). date_of_event shows when
    problems ACTUALLY occurred.
    
    Algorithm:
    1. For each month, compute excess = received - event counts
    2. If excess > 0 and significant, that month has batch reporting
    3. The smoothed series uses event_date counts as the base
    4. Remaining unmatched received reports are distributed across
       the prior N months proportionally
    5. Result: a "true signal" that strips batch dumps
    
    Returns: {month: smoothed_count} dict
    """
    all_months = sorted(set(list(recv_counts.keys()) + list(event_counts.keys())))
    if not all_months: return {}
    
    # Start with event-date counts as the base truth
    smoothed = {m: event_counts.get(m, 0) for m in all_months}
    
    # Calculate total received vs total event to find unmatched reports
    total_recv = sum(recv_counts.get(m, 0) for m in all_months)
    total_evnt = sum(event_counts.get(m, 0) for m in all_months)
    
    # If event counts are very close to received, no smoothing needed
    if total_recv <= 0 or total_evnt <= 0:
        return recv_counts.copy()
    
    # For each month, identify batch excess
    for m in all_months:
        rc = recv_counts.get(m, 0)
        ec = event_counts.get(m, 0)
        excess = rc - ec
        
        if excess > ec * 0.5 and excess > 50:
            # This month has significant batch reporting
            # Distribute excess across prior 6 months proportionally
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
                    # Equal distribution if no prior data
                    per_month = excess // lookback
                    for pm in prior_months:
                        smoothed[pm] = smoothed.get(pm, 0) + per_month
    
    # Scale smoothed to match total received (preserves total volume)
    sm_total = sum(smoothed.values())
    if sm_total > 0 and total_recv > 0:
        scale = total_recv / sm_total
        smoothed = {m: max(0, int(v * scale)) for m, v in smoothed.items()}
    
    return smoothed

def detect_batch(recv_counts, event_counts, ticker=None):
    """Enhanced batch detection using recv vs event date comparison."""
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
# STATISTICS & SCORING (now accepts smoothed series)
# ============================================================
def compute_stats(recv_counts, sev_data, ticker, smoothed=None):
    """Compute stats using SMOOTHED counts if available, raw otherwise."""
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
    # Also store raw values for comparison
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
# MODULE 1: MULTI-SIGNAL CORRELATION
# ============================================================
def compute_enhanced_correlation(counts, stock_prices, max_lag=6,
                                 revenue_dict=None, installed_base_dict=None):
    try:
        if not counts or not stock_prices:
            return {"status":"insufficient_data","message":"Missing data.",
                    "best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,
                    "direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        common = sorted(set(counts.keys()) & set(stock_prices.keys()))
        if len(common)<14:
            return {"status":"insufficient_data","message":f"{len(common)} months, need 14+.",
                    "best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,
                    "direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        mc = [counts[m] for m in common]
        sp = [stock_prices[m] for m in common]
        sr = [0.0]+[(sp[i]-sp[i-1])/sp[i-1]*100 if sp[i-1]>0 else 0.0 for i in range(1,len(sp))]
        signals = {"raw_counts":mc}
        signals["count_delta"] = [0.0]+[mc[i]-mc[i-1] for i in range(1,len(mc))]
        zs = []
        for i in range(len(mc)):
            w = mc[max(0,i-5):i+1]
            if len(w)>=3:
                mu=sum(w)/len(w); sd=math.sqrt(sum((v-mu)**2 for v in w)/len(w))
                zs.append((mc[i]-mu)/sd if sd>0 else 0.0)
            else: zs.append(0.0)
        signals["z_score"] = zs
        if revenue_dict:
            rr = []
            for m in common:
                yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; qr=revenue_dict.get(q)
                rr.append(counts[m]/(qr/3)*1e6 if qr and qr>0 else None)
            lv=None
            for i in range(len(rr)):
                if rr[i] is not None: lv=rr[i]
                elif lv is not None: rr[i]=lv
                else: rr[i]=0.0
            signals["rate_per_rev"] = rr
        if installed_base_dict:
            rb = []
            for m in common:
                yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; ib=installed_base_dict.get(q)
                rb.append(counts[m]/ib*10000 if ib and ib>0 else None)
            lv=None
            for i in range(len(rb)):
                if rb[i] is not None: lv=rb[i]
                elif lv is not None: rb[i]=lv
                else: rb[i]=0.0
            signals["rate_per_base"] = rb
        dl = signals["count_delta"]
        signals["acceleration"] = [0.0,0.0]+[dl[i]-dl[i-1] for i in range(2,len(dl))]
        ob_rho,ob_p,ob_lag,ob_sig = 0,1.0,0,"raw_counts"
        sa = {}; llr = {}
        for sn,sv in signals.items():
            br,bp,bl = 0,1.0,0; ld = {}
            for lag in range(0, min(max_lag+1, len(sv)-6)):
                ss = sv[:len(sv)-lag] if lag>0 else sv
                rs = sr[lag:] if lag>0 else sr
                ml = min(len(ss),len(rs))
                if ml<8: continue
                a,b = ss[:ml],rs[:ml]
                if all(v==a[0] for v in a) or all(v==b[0] for v in b): continue
                rho,p = _proper_spearman(a,b)
                ld[f"{lag}mo"] = {"rho":rho,"p":p}
                if sn=="raw_counts": llr[f"{lag}mo"]={"rho":rho,"p":p}
                if abs(rho)>abs(br): br,bp,bl = rho,p,lag
            sa[sn] = {"best_rho":br,"best_p":bp,"best_lag":bl,
                      "significant":bp<0.05,"direction":"negative" if br<0 else "positive","lag_detail":ld}
            if abs(br)>abs(ob_rho): ob_rho,ob_p,ob_lag,ob_sig = br,bp,bl,sn
        sc = sum(1 for s in sa.values() if s["significant"])
        nc = sum(1 for s in sa.values() if s["significant"] and s["direction"]=="negative")
        aar = sum(abs(s["best_rho"]) for s in sa.values())/max(len(sa),1)
        con = nc/max(sc,1)
        conf = min(100,int(abs(ob_rho)*40+aar*20+sc/len(sa)*20+con*20))
        osig = ob_p<0.05
        msg = f"Best: \u03C1={ob_rho:+.3f} at {ob_lag}mo lag (p={ob_p:.4f}, signal={ob_sig}). "
        if osig and ob_rho<-0.2: msg+=f"MAUDE {ob_sig} predicts declines {ob_lag}mo ahead. "
        elif osig and ob_rho>0.2: msg+="Market pricing in concurrently. "
        else: msg+="No significant lead-lag. "
        msg+=f"Confidence: {conf}/100 ({sc}/{len(sa)} sig, {nc} neg)."
        return {"status":"ok","best_rho":ob_rho,"best_p":ob_p,"best_lag":ob_lag,
                "significant":osig,"direction":"negative" if ob_rho<0 else "positive",
                "lag_results":llr,"message":msg,"best_signal":ob_sig,"confidence":conf,
                "signal_analysis":sa,"signals_tested":len(sa),"signals_significant":sc,"signals_negative":nc}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"best_rho":0,"best_p":1.0,"best_lag":0,
                "significant":False,"direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}

# ============================================================
# MODULES 2-9: Failure Modes, EDGAR, Insider, Trials, Recalls, etc.
# ============================================================
def analyze_failure_modes(sq, start, limit=50):
    url = f"https://api.fda.gov/device/event.json?search=brand_name:{url_quote(sq)}+AND+date_received:[{start}+TO+now]&limit={limit}"
    data = _api_get(url)
    if not data or "results" not in data: return {"status":"no_data","categories":{},"total":0}
    cats = {"sensor_failure":0,"adhesion":0,"connectivity":0,"inaccurate_reading":0,"skin_reaction":0,
            "alarm_alert":0,"battery":0,"physical_damage":0,"software":0,"insertion":0,"occlusion":0,"other":0}
    kw = {"sensor_failure":["sensor fail","no reading","sensor error","lost signal","signal loss","expired early"],
          "adhesion":["fell off","adhesive","peel","detach","came off","not stick"],
          "connectivity":["bluetooth","connect","pair","sync","lost connection","disconnect"],
          "inaccurate_reading":["inaccurate","wrong reading","false","discrepan","not match","off by"],
          "skin_reaction":["rash","irritat","red","itch","allerg","skin","welt","blister"],
          "alarm_alert":["alarm","alert","no sound","speaker","beep","notification","did not alert"],
          "battery":["battery","charge","power","dead","drain","won't turn on"],
          "physical_damage":["crack","broke","snap","bent","leak","damage"],
          "software":["software","app","crash","freeze","update","glitch","display"],
          "insertion":["insert","needle","pain","bleed","bruis","applicat"],
          "occlusion":["occlus","block","clog","no deliv","no insulin"]}
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
    return {"status":"ok","categories":cats,"total":total,
            "top_modes":[{"mode":k,"count":v,"pct":round(v/max(total,1)*100,1)} for k,v in top]}

def analyze_edgar_filings(tk):
    if tk in ("SQEL","BBNX"): return {"status":"skip","message":"Limited EDGAR history"}
    cik_map={"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT":"0000001800","MDT":"0000064670"}
    cik=cik_map.get(tk)
    if not cik: return {"status":"no_cik"}
    try:
        url=f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req=Request(url,headers={"User-Agent":"MAUDE-Monitor/3.3 research@example.com"})
        with urlopen(req,timeout=15) as resp: filings=json.loads(resp.read().decode())
        recent=filings.get("filings",{}).get("recent",{})
        forms=recent.get("form",[]); dates=recent.get("filingDate",[])
        cutoff=(datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")
        last90=[(f,d) for f,d in zip(forms,dates) if d>=cutoff]
        fc={}
        for f,d in last90: fc[f]=fc.get(f,0)+1
        return {"status":"ok","total_90d":len(last90),"form_counts":fc,
                "eight_k_count":fc.get("8-K",0),"message":f"{len(last90)} filings 90d ({fc.get('8-K',0)} 8-Ks)"}
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
        recent=filings.get("filings",{}).get("recent",{})
        forms=recent.get("form",[]); dates=recent.get("filingDate",[])
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
        studies=data.get("studies",[])
        return {"status":"ok","count":len(studies),"message":f"{len(studies)} active trials"}
    except Exception as e: return {"status":"error","message":str(e)[:100]}

def analyze_fda_recalls(sq, tk):
    try:
        url=f"https://api.fda.gov/device/recall.json?search=product_description:{url_quote(sq)}&sort=event_date_terminated:desc&limit=10"
        data=_api_get(url)
        if not data or "results" not in data: return {"status":"ok","count":0,"recalls":[],"message":"No recalls"}
        recalls=[{"reason":r.get("reason_for_recall","")[:120],"classification":r.get("classification",""),
                  "status":r.get("status",""),"date":r.get("event_date_terminated","")[:10]} for r in data["results"][:5]]
        c1=sum(1 for r in recalls if r["classification"]=="Class I")
        return {"status":"ok","count":len(data["results"]),"recalls":recalls,"class1_count":c1,
                "message":f'{len(data["results"])} recalls ({c1} Class I)'}
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
    s=min(100,s)
    lev = "HIGH" if s>=70 else "MODERATE" if s>=40 else "LOW"
    return {"status":"ok","probability":s,"level":lev,
            "message":f"Recall prob: {lev} ({s}/100). Heuristic model."}

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
    if corr and corr.get("significant") and corr.get("direction")=="negative":
        s-=10; factors.append(("Neg MAUDE-stock corr",-10))
    if stats["deaths_3mo"]>=2: s-=10; factors.append(("Recent deaths",-10))
    if stats["injuries_3mo"]>=20: s-=5; factors.append(("Elevated injuries",-5))
    if fm and fm.get("status")=="ok":
        if fm.get("categories",{}).get("alarm_alert",0)>5: s-=5; factors.append(("Alarm issues",-5))
    s=max(0,min(100,s))
    out="POSITIVE" if s>=65 else "NEUTRAL" if s>=40 else "NEGATIVE"
    return {"status":"ok","score":s,"outlook":out,"factors":factors,
            "message":f"Outlook: {out} ({s}/100). Heuristic model."}

# ============================================================
# MODULE 10: BACKTEST CASE STUDIES (PM-READY)
# ============================================================
def compute_backtest_case_studies(counts, stock_prices, stats, tk, batch_info=None):
    try:
        if not counts or not stock_prices or not stats:
            return {"status":"insufficient_data","signals":[],"case_studies":[],"summary":{}}
        sm = sorted(set(counts.keys()) & set(stock_prices.keys()))
        if len(sm)<12: return {"status":"insufficient_data","signals":[],"case_studies":[],"summary":{}}
        mc={m:counts[m] for m in sm}; sp={m:stock_prices[m] for m in sm}
        zbm={}
        for i,m in enumerate(sm):
            w=[mc[sm[j]] for j in range(max(0,i-5),i+1)]
            if len(w)>=3:
                mu=sum(w)/len(w); sd=math.sqrt(sum((v-mu)**2 for v in w)/len(w))
                zbm[m]=(mc[m]-mu)/sd if sd>0 else 0.0
            else: zbm[m]=0.0
        rcm={}; prev=None
        for m in sm:
            rcm[m]=(mc[m]-mc[prev])/mc[prev]*100 if prev and mc.get(prev,0)>0 else 0.0
            prev=m
        ZT,ST,CD=1.5,30.0,3; sigs=[]; cases=[]; lsi=-999
        for i,m in enumerate(sm):
            if i-lsi<CD or i<6: continue
            z=zbm.get(m,0); rc=rcm.get(m,0)
            ib=False
            if batch_info:
                bi=batch_info.get(m)
                if bi in ("batch","mild_batch"): ib=True
            trig=None
            if z>=ZT and not ib: trig=f"Z-spike: {z:+.2f}\u03C3"
            elif rc>=ST and z>=1.0 and not ib: trig=f"MoM surge: +{rc:.0f}%"
            if not trig: continue
            lsi=i; ep=sp[m]; fwd={}
            for hn,hm in [("1mo",1),("2mo",2),("3mo",3),("6mo",6)]:
                if i+hm<len(sm):
                    xm=sm[i+hm]; xp=sp[xm]; ret=(xp-ep)/ep*100
                    fwd[hn]={"exit_month":xm,"exit_price":round(xp,2),"long_ret":round(ret,2),
                             "short_ret":round(-ret,2),"short_pnl":round(-(xp-ep)/ep*10000,2)}
            sigs.append({"month":m,"z":round(z,2),"mom":round(rc,1),"trigger":trig,"entry":round(ep,2),"fwd":fwd})
            bh=None; bsp=0
            for h,d in fwd.items():
                if d["short_pnl"]>bsp: bsp=d["short_pnl"]; bh=h
            cs={"month":m,"ticker":tk,"trigger":trig,"entry":round(ep,2),"reports":mc[m],"z":round(z,2),
                "best_h":bh,"best_pnl":round(bsp,2) if bh else 0,"profitable":bsp>0 if bh else False,"fwd":fwd}
            if bh and bsp>0:
                ed=fwd[bh]
                cs["narrative"]=f"{m}: {trig}. {tk} ${ep:.2f}\u2192${ed['exit_price']:.2f} ({bh}). Short P&L: +${bsp:,.0f}/$10K."
            elif bh:
                ed=fwd[bh]
                cs["narrative"]=f"{m}: {trig}. {tk} ${ep:.2f}. Short MISSED. Loss: ${abs(bsp):,.0f}/$10K."
            else: cs["narrative"]=f"{m}: Signal, insufficient fwd data."
            cases.append(cs)
        tot=len(sigs); prof=sum(1 for c in cases if c.get("profitable"))
        tp=sum(c.get("best_pnl",0) for c in cases if c.get("best_h"))
        hr=prof/tot*100 if tot>0 else 0; ap=tp/tot if tot>0 else 0
        gr="STRONG" if hr>=60 else "MODERATE" if hr>=45 else "WEAK"
        return {"status":"ok","signals":sigs,"case_studies":cases,
                "summary":{"total":tot,"profitable":prof,"hit_rate":round(hr,1),"total_pnl":round(tp,2),
                           "avg_pnl":round(ap,2),"grade":gr,
                           "message":f"{gr}: {hr:.0f}% hit, {tot} signals, P&L: ${tp:+,.0f}/$10K"}}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"signals":[],"case_studies":[],"summary":{}}

def analyze_google_trends(tk): return {"status":"framework","message":"Requires pytrends."}
def analyze_short_interest(tk): return {"status":"framework","message":"Requires Yahoo scraping."}
def analyze_payer_coverage(tk):
    c={"DXCM":"Broad commercial+Medicare CGM","PODD":"Broad commercial+Medicare pump","TNDM":"Broad commercial+Medicare pump",
       "ABT":"Broad commercial+Medicare CGM","MDT":"Broad commercial+Medicare pump","BBNX":"Limited (new)","SQEL":"Pre-market"}
    return {"status":"ok","message":c.get(tk,"Unknown")}
def analyze_international(tk): return {"status":"framework","message":"No structured API."}
    # ============================================================
# PIPELINE — Now fetches BOTH date fields and computes smoothed
# ============================================================
def run_pipeline(backfill=False, quick=False):
    start = "20230101" if backfill else ("20250901" if quick else "20230101")
    all_res, summary = {}, []
    print("ALL ENHANCED MODULES LOADED (inline)")
    print("\n=== Fetching live stock prices ===")
    live_stocks = fetch_live_stock_prices()
    global STOCK_MONTHLY, _stock_source
    STOCK_MONTHLY = merge_stock_data(STOCK_MONTHLY, live_stocks)
    _stock_source = f"LIVE ({len(live_stocks)} tickers)" if live_stocks else "HARDCODED"
    print(f"  {_stock_source}")
    rev_status = get_revenue_staleness()
    print(f"  Revenue: {rev_status['message']}")
    r_scores_company = {}

    for dev in DEVICES:
        did=dev["id"]; tk=dev["ticker"]; rk=dev.get("rev_key",tk)
        print(f"\n{'='*50}\n{dev['name']} ({tk})")

        # FETCH BOTH DATE FIELDS — this is the key to smoothing
        recv = fetch_counts(dev["search"], "date_received", start); time.sleep(0.3)
        evnt = fetch_counts(dev["search"], "date_of_event", start); time.sleep(0.3)
        sev = fetch_severity(dev["search"], start)

        # COMPUTE SMOOTHED SERIES (redistributes batch spikes)
        smoothed = compute_smoothed_series(recv, evnt)
        batch = detect_batch(recv, evnt, ticker=tk)

        # Stats computed on SMOOTHED data (not raw received)
        stats = compute_stats(recv, sev, tk, smoothed=smoothed)
        rscore = compute_r_score(stats) if stats else None
        if rscore is not None and dev.get("is_combined"):
            r_scores_company[tk] = rscore

        modules = {"enhanced_corr":None,"failure_modes":None,"google_trends":None,
                   "insider":None,"trials":None,"short_interest":None,"edgar":None,
                   "payer":None,"international":None,"recall_prob":None,
                   "earnings_pred":None,"backtest":None,"peer_relative":None,"recalls":None}

        if HAS_MODULES and stats:
            try:
                print("  Running: Multi-signal correlation (on smoothed)...")
                modules["enhanced_corr"] = compute_enhanced_correlation(
                    smoothed, STOCK_MONTHLY.get(tk,{}), max_lag=6,
                    revenue_dict=QUARTERLY_REVENUE.get(rk,{}),
                    installed_base_dict=INSTALLED_BASE_K.get(rk,{}))
            except Exception as e:
                modules["enhanced_corr"]={"status":"error","message":str(e)[:100]}

            if not did.endswith("_ALL"):
                try:
                    print("  Running: Failure modes...")
                    modules["failure_modes"]=analyze_failure_modes(dev["search"],start,50)
                except Exception as e: modules["failure_modes"]={"status":"error","message":str(e)[:100]}

            is_co = did.endswith("_ALL") or did in ("SQEL_TWIIST","BBNX_ILET")
            if is_co:
                for mn,mf in [("google_trends",analyze_google_trends),("insider",analyze_insider_trading_detailed),
                              ("trials",analyze_clinical_trials),("short_interest",analyze_short_interest),
                              ("payer",analyze_payer_coverage)]:
                    try: print(f"  Running: {mn}..."); modules[mn]=mf(tk)
                    except Exception as e: modules[mn]={"status":"error","message":str(e)[:100]}

            try: print("  Running: FDA recalls..."); modules["recalls"]=analyze_fda_recalls(dev["search"],tk)
            except Exception as e: modules["recalls"]={"status":"error","message":str(e)[:100]}

            if is_co:
                try: print("  Running: EDGAR..."); modules["edgar"]=analyze_edgar_filings(tk)
                except Exception as e: modules["edgar"]={"status":"error","message":str(e)[:100]}

            try: modules["recall_prob"]=compute_recall_probability(stats,modules.get("failure_modes"),modules.get("edgar"),tk)
            except: pass
            try:
                print("  Running: Case study backtest (on smoothed)...")
                modules["backtest"]=compute_backtest_case_studies(smoothed,STOCK_MONTHLY.get(tk,{}),stats,tk,batch)
            except Exception as e: modules["backtest"]={"status":"error","message":str(e)[:100]}
            try: modules["earnings_pred"]=compute_earnings_predictor(stats,modules.get("enhanced_corr"),modules.get("insider"),modules.get("trials"),modules.get("failure_modes"),tk)
            except: pass

        signal="NORMAL"
        if rscore is not None:
            if rscore>=70: signal="CRITICAL"
            elif rscore>=50: signal="ELEVATED"
            elif rscore>=30: signal="WATCH"

        all_res[did]={"device":dev,"stats":stats,"r_score":rscore,"batch":batch,
                      "recv":recv,"evnt":evnt,"sev":sev,"smoothed":smoothed,
                      "signal":signal,"modules":modules}
        summary.append({"id":did,"name":dev["name"],"ticker":tk,"signal":signal,
                        "r_score":rscore or 0,"z_score":stats["z_score"] if stats else 0})
        if stats: print(f"  Signal: {signal} | R={rscore} | Z={stats['z_score']:+.2f} (smoothed)")

    if r_scores_company:
        pr = compute_peer_relative(r_scores_company)
        for did,res in all_res.items():
            tk=res["device"]["ticker"]
            if tk in pr: res["modules"]["peer_relative"]=pr[tk]
    return all_res, summary

# ============================================================
# HTML HELPERS
# ============================================================
def _accordion(aid, title, stat, content):
    return (f'<div class="acc"><div class="acch" onclick="toggleAcc(\'{aid}\')">'
            f'<span>{title}</span>{stat}<span class="arr" id="arr-{aid}">\u25B6</span></div>'
            f'<div class="accb" id="{aid}" style="display:none">{content}</div></div>')

def _render_corr(ec,did):
    if not ec or ec.get("status")!="ok": return ""
    r=ec.get("best_rho",0); p=ec.get("best_p",1.0); lag=ec.get("best_lag",0)
    sig=ec.get("significant",False); conf=ec.get("confidence",0); bs=ec.get("best_signal","raw")
    sa=ec.get("signal_analysis",{})
    rc="#c0392b" if sig and r<-0.2 else "#e67e22" if sig and r>0.2 else "var(--tx3)"
    cc="#27ae60" if conf>=60 else "#f39c12" if conf>=35 else "#c0392b"
    c=f'<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:10px">'
    c+=f'<div class="si"><div class="sil">BEST \u03C1</div><div class="siv" style="color:{rc}">{r:+.3f}</div></div>'
    c+=f'<div class="si"><div class="sil">LAG</div><div class="siv">{lag}mo</div></div>'
    c+=f'<div class="si"><div class="sil">P-VALUE</div><div class="siv">{"*" if sig else ""}{p:.4f}</div></div>'
    c+=f'<div class="si"><div class="sil">CONFIDENCE</div><div class="siv" style="color:{cc}">{conf}/100</div></div></div>'
    c+=f'<div class="msub" style="margin-bottom:8px">{ec.get("message","")}</div>'
    if sa:
        sl={"raw_counts":"Raw","count_delta":"MoM \u0394","z_score":"Z-Score","rate_per_rev":"Rate/$M","rate_per_base":"Rate/10K","acceleration":"Accel."}
        c+='<table style="width:100%;border-collapse:collapse;font-size:11px"><tr style="background:rgba(0,0,0,0.05)"><th style="padding:3px;text-align:left">Signal</th><th>\u03C1</th><th>Lag</th><th>p</th><th>Sig</th></tr>'
        for sn,sd in sa.items():
            lb=sl.get(sn,sn); sr2=sd.get("best_rho",0); sg2=sd.get("significant",False)
            sc2="#27ae60" if sg2 and sr2<0 else "#c0392b" if sg2 and sr2>0 else "#888"
            bld=' style="font-weight:600"' if sn==bs else ""
            c+=f'<tr{bld}><td style="padding:3px">{"\u2192 " if sn==bs else ""}{lb}</td><td style="padding:3px;color:{sc2};text-align:center">{sr2:+.3f}</td><td style="text-align:center">{sd.get("best_lag",0)}mo</td><td style="text-align:center">{sd.get("best_p",1):.4f}</td><td style="text-align:center">{"\u2713" if sg2 else "\u2014"}</td></tr>'
        c+='</table>'
    c+='<div class="msub" style="margin-top:6px;font-size:10px;opacity:0.7">Tests 6 signals on SMOOTHED data (batch-adjusted). Rate-normalized strips growth noise.</div>'
    return c

def _render_bt(bt,tk):
    if not bt or bt.get("status")!="ok": return "<div class='msub'>No data.</div>"
    s=bt.get("summary",{}); cs=bt.get("case_studies",[])
    gc={"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}
    g=s.get("grade","WEAK"); color=gc.get(g,"#888")
    h=f'<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:12px">'
    h+=f'<div class="si"><div class="sil">GRADE</div><div class="siv" style="color:{color}">{g}</div></div>'
    h+=f'<div class="si"><div class="sil">HIT RATE</div><div class="siv">{s.get("hit_rate",0):.0f}%</div></div>'
    h+=f'<div class="si"><div class="sil">SIGNALS</div><div class="siv">{s.get("total",0)}</div></div>'
    pnl=s.get("total_pnl",0); pc="#27ae60" if pnl>0 else "#c0392b"
    h+=f'<div class="si"><div class="sil">P&L/$10K</div><div class="siv" style="color:{pc}">${pnl:+,.0f}</div></div></div>'
    h+=f'<div class="msub" style="margin-bottom:10px">{s.get("message","")}</div>'
    if cs:
        h+='<table style="width:100%;border-collapse:collapse;font-size:11px"><tr style="background:rgba(0,0,0,0.05)"><th style="padding:4px;text-align:left">Date</th><th>Trigger</th><th>Entry</th><th>Best Exit</th><th>Ret</th><th>P&L</th></tr>'
        for c in cs[-8:]:
            bh=c.get("best_h"); fwd=c.get("fwd",{})
            if bh and bh in fwd:
                ed=fwd[bh]; rs2=f'{ed["short_ret"]:+.1f}%'; pv=c.get("best_pnl",0); ps=f'${pv:+,.0f}'
                rc2="#27ae60" if pv>0 else "#c0392b"; xs=f'${ed["exit_price"]:.2f} ({bh})'
            else: rs2="\u2014"; ps="\u2014"; rc2="#888"; xs="\u2014"
            tr2=c.get("trigger",""); tr2=tr2[:20]+"..." if len(tr2)>22 else tr2
            h+=f'<tr><td style="padding:3px">{c.get("month","")}</td><td style="padding:3px;font-size:10px">{tr2}</td><td style="padding:3px">${c.get("entry",0):.2f}</td><td style="padding:3px">{xs}</td><td style="padding:3px;color:{rc2}">{rs2}</td><td style="padding:3px;color:{rc2};font-weight:600">{ps}</td></tr>'
        h+='</table>'
    h+='<div class="msub" style="margin-top:8px;font-size:10px;opacity:0.7">Short on z&gt;1.5\u03C3 or MoM&gt;30%. Batch months excluded. Uses SMOOTHED signal. Past\u2260future.</div>'
    return h

# ============================================================
# HTML DASHBOARD GENERATION
# ============================================================
def generate_html(all_res, summary):
    now=datetime.now().strftime("%Y-%m-%d %H:%M ET"); rs=get_revenue_staleness()
    rw=' style="color:#c0392b;font-weight:600"' if rs.get("stale") else ""
    trows=""
    for s in sorted(summary,key=lambda x:-x["r_score"]):
        res=all_res.get(s["id"],{}); st=res.get("stats")
        if not st: continue
        scm={"CRITICAL":"#c0392b","ELEVATED":"#e67e22","WATCH":"#f1c40f","NORMAL":"#27ae60"}
        sc=scm.get(s["signal"],"#888")
        ec=res.get("modules",{}).get("enhanced_corr")
        cs2=f'{ec["best_rho"]:+.3f}{"*" if ec.get("significant") else ""}' if ec and ec.get("best_rho") else "\u2014"
        trows+=f'<tr><td>{s["name"]}</td><td>{s["ticker"]}</td><td>{st["latest_month"]}</td><td>{fmt0(st["latest_value"])}</td><td>{st["z_score"]:+.2f}</td><td>{s["r_score"]}</td><td>{fmt(st.get("rate_per_m"),1) if st.get("rate_per_m") else "\u2014"}</td><td>{fmt(st.get("rate_per_10k"),2) if st.get("rate_per_10k") else "\u2014"}</td><td>{st["slope_6mo"]:+.1f}</td><td>{st["deaths_3mo"]}</td><td>{st["injuries_3mo"]}</td><td>{cs2}</td><td style="color:{sc};font-weight:700">{s["signal"]}</td></tr>'

    cd={}; company_html={}; tab_ids={}
    for comp in COMPANIES: tab_ids[comp]=comp.lower().replace(" ","_"); company_html[comp]=""

    for did,res in all_res.items():
        dev=res["device"]; st=res.get("stats"); batch=res.get("batch",{})
        recv=res.get("recv",{}); evnt=res.get("evnt",{}); sev=res.get("sev",{})
        smoothed=res.get("smoothed",{}); tk=dev["ticker"]; rk=dev.get("rev_key",tk)
        allm=res.get("modules",{})
        if not st: continue
        months=st["months"]; vals=st["values"]; raw_vals=st.get("raw_values",vals)

        bm=[m for m,v in batch.items() if v and v!="None"]
        evts=PRODUCT_EVENTS.get(did,[])
        # Smoothed values for chart
        sm_vals=[smoothed.get(m,0) for m in months]
        # Rate/$M on smoothed
        rm=[]
        for m in months:
            yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; qr=QUARTERLY_REVENUE.get(rk,{}).get(q)
            rm.append(round(smoothed.get(m,0)/(qr/3)*1e6,1) if qr and qr>0 else None)
        r10k=[]
        for m in months:
            yr,mo=m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"; ib=INSTALLED_BASE_K.get(rk,{}).get(q)
            r10k.append(round(smoothed.get(m,0)/ib*10000,2) if ib and ib>0 else None)
        dv=[sev.get("death",{}).get(m,0) for m in months]
        iv=[sev.get("injury",{}).get(m,0) for m in months]
        mv=[sev.get("malfunction",{}).get(m,0) for m in months]
        zv=[]
        for i in range(len(vals)):
            w=vals[max(0,i-5):i+1]
            if len(w)>=3:
                mu=sum(w)/len(w); sd=math.sqrt(sum((v-mu)**2 for v in w)/len(w))
                zv.append(round((vals[i]-mu)/sd,2) if sd>0 else 0)
            else: zv.append(0)
        ma6v=[round(v,1) for v in st["ma6"].values()]
        stk=STOCK_MONTHLY.get(tk,{}); sv=[stk.get(m) for m in months]

        cd[did]={"l":months,"v":raw_vals,"sm":sm_vals,"bm":bm,"evts":evts,"ma6":ma6v,
                 "s1l":round(st["sigma1_lo"],1),"s1h":round(st["sigma1_hi"],1),
                 "s2l":round(st["sigma2_lo"],1),"s2h":round(st["sigma2_hi"],1),
                 "rm":rm,"r10k":r10k,"deaths":dv,"injuries":iv,"malfs":mv,"z":zv,"stk":sv}

        scm2={"CRITICAL":"#c0392b","ELEVATED":"#e67e22","WATCH":"#f1c40f","NORMAL":"#27ae60"}
        sc2=scm2.get(res["signal"],"#888")
        card=f'<div class="card" data-company="{dev["company"]}" data-signal="{res["signal"]}" data-view="{"combined" if dev.get("is_combined") else "individual"}">'
        card+=f'<div class="ch"><span class="cn">{dev["name"]}</span><span class="cs" style="background:{sc2}">{res["signal"]}</span></div>'
        card+=f'<div class="sg"><div class="si"><div class="sil">R-SCORE</div><div class="siv">{res.get("r_score",0)}</div></div>'
        card+=f'<div class="si"><div class="sil">Z-SCORE</div><div class="siv">{st["z_score"]:+.2f}</div></div>'
        card+=f'<div class="si"><div class="sil">REPORTS (SM)</div><div class="siv">{fmt0(st["latest_value"])}</div></div>'
        card+=f'<div class="si"><div class="sil">TREND</div><div class="siv">{st["slope_6mo"]:+.1f}</div></div></div>'
        card+=f'<div class="cc" id="cc-{did}"><div class="cbtns">'
        for bi,bl in [("reports","Reports"),("smoothed","Smoothed"),("rate_m","Rate/$M"),("rate_10k","Rate/10K"),("severity","Severity"),("z","Z-Score"),("stock","Stock")]:
            card+=f'<button class="cb{" active" if bi=="reports" else ""}" data-v="{bi}">{bl}</button>'
        card+='<button class="cb rst" data-v="reset">Reset</button></div>'
        card+=f'<div class="cdesc" id="cdesc-{did}"></div><div class="cwrap"><canvas id="ch-{did}"></canvas></div></div>'

        acc=""
        fm=allm.get("failure_modes")
        if fm and fm.get("status")=="ok":
            top=fm.get("top_modes",[])
            fc='<div class="sg" style="grid-template-columns:repeat(3,1fr)">'
            for t in top[:3]: fc+=f'<div class="si"><div class="sil">{t["mode"].upper()}</div><div class="siv">{t["count"]} ({t["pct"]}%)</div></div>'
            fc+='</div>'
            acc+=_accordion(f"fm-{did}","Failure Modes",f'<span class="mstat">{fm.get("total",0)} analyzed</span>',fc)

        ec=allm.get("enhanced_corr")
        if ec and ec.get("status")=="ok":
            r2=ec.get("best_rho",0); sig2=ec.get("significant",False); conf2=ec.get("confidence",0)
            rc3="#c0392b" if sig2 and r2<-0.2 else "#e67e22" if sig2 and r2>0.2 else "var(--tx3)"
            cc2="#27ae60" if conf2>=60 else "#f39c12" if conf2>=35 else "#c0392b"
            acc+=_accordion(f"corr-{did}","MAUDE-Stock Correlation (Multi-Signal)",
                f'<span class="mstat" style="color:{rc3}">\u03C1={r2:+.3f}</span> <span class="mstat" style="color:{cc2};font-size:11px">[{conf2}/100]</span>',
                _render_corr(ec,did))

        bt=allm.get("backtest")
        if bt and bt.get("status")=="ok":
            bs2=bt.get("summary",{}); g2=bs2.get("grade","?"); hr2=bs2.get("hit_rate",0)
            gc2={"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}.get(g2,"#888")
            acc+=_accordion(f"bt-{did}","Trade Signal Backtest",f'<span class="mstat" style="color:{gc2}">{g2} ({hr2:.0f}%)</span>',_render_bt(bt,tk))

        ep=allm.get("earnings_pred")
        if ep and ep.get("status")=="ok":
            eo=ep.get("outlook","?"); es2=ep.get("score",50)
            ec3="#27ae60" if eo=="POSITIVE" else "#c0392b" if eo=="NEGATIVE" else "#f39c12"
            epc=f'<div class="msub">{ep.get("message","")}</div>'
            for fn,fv in ep.get("factors",[]):
                fc2="#c0392b" if fv<0 else "#27ae60"
                epc+=f'<div style="font-size:11px;padding:1px 0"><span style="color:{fc2}">{fv:+d}</span> {fn}</div>'
            acc+=_accordion(f"ep-{did}","Earnings Predictor",f'<span class="mstat" style="color:{ec3}">{eo} ({es2})</span>',epc)

        rp=allm.get("recall_prob")
        if rp and rp.get("status")=="ok":
            rl=rp.get("level","?"); rp2=rp.get("probability",0)
            rpc="#c0392b" if rl=="HIGH" else "#f39c12" if rl=="MODERATE" else "#27ae60"
            acc+=_accordion(f"rp-{did}","Recall Probability",f'<span class="mstat" style="color:{rpc}">{rl} ({rp2})</span>',f'<div class="msub">{rp.get("message","")}</div>')

        pr=allm.get("peer_relative")
        if pr and isinstance(pr,dict):
            prs=pr.get("signal","?"); prc="#c0392b" if prs in ("WORST","WEAK") else "#27ae60" if prs in ("BEST","STRONG") else "var(--tx3)"
            prc2=f'<div class="msub">{pr.get("message","")}</div>'
            for ptk,ps in pr.get("peers",[]): prc2+=f'<div style="font-size:11px;{"font-weight:600" if ptk==tk else ""}">{ptk}: R={ps}</div>'
            acc+=_accordion(f"pr-{did}","Peer Ranking",f'<span class="mstat" style="color:{prc}">{prs}</span>',prc2)

        rc4=allm.get("recalls")
        if rc4 and rc4.get("status")=="ok" and rc4.get("count",0)>0:
            rcc=''.join(f'<div style="padding:3px 0;border-bottom:1px solid rgba(0,0,0,0.1);font-size:11px"><strong>{r.get("classification","")}</strong> {r.get("reason","")[:80]}</div>' for r in rc4.get("recalls",[])[:3])
            acc+=_accordion(f"rc-{did}","FDA Recalls",f'<span class="mstat">{rc4["count"]}</span>',rcc)

        for mn,ml in [("edgar","SEC Filings"),("insider","Insider Trading"),("trials","Clinical Trials"),("payer","Payer Coverage")]:
            md=allm.get(mn)
            if md and md.get("status")=="ok":
                acc+=_accordion(f"{mn}-{did}",ml,f'<span class="mstat">Data</span>',f'<div class="msub">{md.get("message","")}</div>')

        card+=f'<div class="mods">{acc}</div></div>'
        company_html[dev["company"]]=company_html.get(dev["company"],"")+card

    # FULL HTML
    html=f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>MAUDE Monitor V3.3</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<style>
:root{{--bg:#f8f9fa;--card:#fff;--tx1:#1a1a2e;--tx2:#444;--tx3:#888;--bdr:#e0e0e0;--acc:#f0f4f8}}
*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--tx1);padding:12px;max-width:1400px;margin:0 auto}}
h1{{font-size:1.5em;margin-bottom:4px}}h2{{font-size:1.2em;margin:16px 0 8px}}
.hdr{{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#fff;padding:16px 20px;border-radius:12px;margin-bottom:16px}}.hdr small{{opacity:0.7;font-size:12px}}.hdr .warn{{color:#f39c12}}
.tabs{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px;background:var(--card);padding:8px;border-radius:8px;border:1px solid var(--bdr)}}
.tab{{padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;border:none;background:transparent;color:var(--tx2)}}.tab:hover{{background:var(--acc)}}.tab.active{{background:#1a1a2e;color:#fff}}
.tabcontent{{display:none}}.tabcontent.active{{display:block}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:16px}}
.card{{background:var(--card);border:1px solid var(--bdr);border-radius:10px;padding:14px;box-shadow:0 1px 3px rgba(0,0,0,0.05)}}
.ch{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}}.cn{{font-weight:700;font-size:14px}}.cs{{padding:2px 8px;border-radius:4px;color:#fff;font-size:11px;font-weight:600}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px}}.si{{text-align:center;padding:4px;background:var(--acc);border-radius:6px}}.sil{{font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:0.5px}}.siv{{font-size:16px;font-weight:700;margin-top:2px}}
.cc{{margin:8px 0}}.cbtns{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:6px}}.cb{{padding:3px 8px;border:1px solid var(--bdr);border-radius:4px;cursor:pointer;font-size:11px;background:var(--card)}}.cb:hover{{background:var(--acc)}}.cb.active{{background:#1a1a2e;color:#fff;border-color:#1a1a2e}}.cb.rst{{background:transparent;border-style:dashed;font-size:10px}}
.cdesc{{font-size:11px;color:var(--tx3);margin-bottom:4px;min-height:14px}}.cwrap{{height:220px;position:relative}}
.mods{{margin-top:10px}}.acc{{border:1px solid var(--bdr);border-radius:6px;margin-bottom:4px;overflow:hidden}}.acch{{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;cursor:pointer;font-size:12px;font-weight:600;background:var(--acc)}}.acch:hover{{background:#e4e8ee}}.accb{{padding:10px;font-size:12px}}.arr{{font-size:10px;transition:transform 0.2s}}.arr.open{{transform:rotate(90deg)}}.mstat{{font-size:12px;font-weight:600;margin:0 8px}}.msub{{font-size:12px;color:var(--tx2);line-height:1.5;margin:4px 0}}
.filters{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;font-size:13px}}.filters select{{padding:4px 8px;border:1px solid var(--bdr);border-radius:4px;font-size:12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{padding:6px 8px;text-align:left;border-bottom:1px solid var(--bdr)}}th{{background:var(--acc);font-weight:600;font-size:11px;text-transform:uppercase}}
.disc{{margin-top:20px;padding:12px;background:var(--acc);border-radius:8px;font-size:11px;color:var(--tx3);line-height:1.6}}
</style></head><body>
<div class="hdr"><h1>MAUDE Monitor V3.3 \u2014 Event-Date Smoothed Intelligence</h1>
<small>Updated: {now} | Stocks: {_stock_source} | {len(DEVICES)} products | \u2728 Smoothing: ON (batch-adjusted via event-date decomposition)</small><br>
<small{rw}>Revenue: {rs["message"]} (updated {REVENUE_LAST_UPDATED})</small></div>
<div class="tabs"><div class="tab active" onclick="showTab('overview')">Overview</div><div class="tab" onclick="showTab('guide')">Guide</div>'''
    for comp in COMPANIES: html+=f'<div class="tab" onclick="showTab(\'{tab_ids[comp]}\')">{comp}</div>'
    html+=f'''</div><div class="tabcontent active" id="tc-overview">
<div class="filters"><label>Company:</label><select id="fc" onchange="af()"><option value="all">All</option>'''
    for c in COMPANIES: html+=f'<option value="{c}">{c}</option>'
    html+='''</select><label>Signal:</label><select id="fs" onchange="af()"><option value="all">All</option><option value="CRITICAL">Critical</option><option value="ELEVATED">Elevated+</option><option value="WATCH">Watch+</option></select>
<label>View:</label><select id="fv" onchange="af()"><option value="all">All</option><option value="combined">Company-Level</option><option value="individual">Products</option></select></div>
<h2>All Products \u2014 Latest Month (Smoothed)</h2>'''
    html+=f'<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports(SM)</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>Trend</th><th>Deaths</th><th>Injuries</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div></div>'
    html+='''<div class="tabcontent" id="tc-guide"><h2>How to Read V3.3</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px;margin-top:8px">
<div style="padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px"><h4 style="font-size:12px;margin-bottom:4px">\u2728 Event-Date Smoothing (NEW)</h4><p style="font-size:11px;color:var(--tx2);line-height:1.4">Raw MAUDE data spikes when manufacturers batch-dump late reports (especially after recalls). V3.3 uses date_of_event as the TRUE signal and redistributes batch excess across prior months. The "Reports" chart shows RAW data. The "Smoothed" chart shows the adjusted signal. ALL analytics (Z-score, R-score, correlation, backtest) now run on smoothed data.</p></div>
<div style="padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px"><h4 style="font-size:12px;margin-bottom:4px">Multi-Signal Correlation</h4><p style="font-size:11px;color:var(--tx2);line-height:1.4">Tests 6 MAUDE signals (raw, delta, z-score, rate/$M, rate/10K, acceleration) against stock returns at 0-6mo lags on SMOOTHED data. Confidence 0-100 combines strength, significance, consistency.</p></div>
<div style="padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px"><h4 style="font-size:12px;margin-bottom:4px">Case Study Backtest</h4><p style="font-size:11px;color:var(--tx2);line-height:1.4">Specific trade signals with entry/exit/P&L. Uses smoothed z-scores. Batch months auto-excluded. Grade: STRONG (\u226560% hit), MODERATE (\u226545%), WEAK (&lt;45%).</p></div>
<div style="padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px"><h4 style="font-size:12px;margin-bottom:4px">R-Score &amp; Z-Score</h4><p style="font-size:11px;color:var(--tx2);line-height:1.4">Now computed on smoothed data. Z\u22652=anomalous. R-Score 0-100 composite risk. Both are dramatically more accurate without batch-dump noise.</p></div>
</div></div>'''
    for comp in COMPANIES:
        tid=tab_ids[comp]; ch=company_html.get(comp,"<p>No data.</p>")
        html+=f'\n<div class="tabcontent" id="tc-{tid}">{ch}</div>'
    html+='\n<div class="disc">V3.3 uses event-date smoothing to strip batch-dump noise. Research only, not investment advice. MAUDE has 30-90d lag. Correlation\u2260causation. Heuristic models, not ML.</div></div>'

    js=r'''<script>
var defined_cd=__CD__;var charts={};
var chartDescs={"reports":"RAW REPORTS (date_received): Shows what FDA actually received each month. Spikes may be batch dumps from recalls, NOT real surges. Use Smoothed view for true signal.","smoothed":"SMOOTHED REPORTS (event-date adjusted): Redistributes batch-dump excess across actual event months. THIS is the signal used for all analytics (Z-score, R-score, correlation, backtest). Green=smoothed, gray outline=raw for comparison.","rate_m":"RATE PER $M REVENUE (smoothed): Smoothed reports / monthly revenue. RISING rate = quality deteriorating faster than revenue growth.","rate_10k":"RATE PER 10K USERS (smoothed): Smoothed reports / installed base. Most precise normalization.","severity":"SEVERITY: Deaths (red), injuries (orange), malfunctions (yellow). Raw counts, not smoothed.","z":"Z-SCORE (on smoothed): Monthly z-scores computed on smoothed data. Much cleaner signal without batch noise. Z>2=anomalous.","stock":"STOCK OVERLAY: Green=price, Red=smoothed MAUDE. Look for smoothed MAUDE spikes preceding stock declines."};
function showTab(id){document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active")});document.querySelectorAll(".tabcontent").forEach(function(t){t.classList.remove("active")});var ct=document.querySelector('.tab[onclick*="'+id+'"]');if(ct)ct.classList.add("active");var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");}
function toggleAcc(id){var el=document.getElementById(id);var arr=document.getElementById("arr-"+id);if(el.style.display==="none"){el.style.display="block";if(arr)arr.classList.add("open");}else{el.style.display="none";if(arr)arr.classList.remove("open");}}
function af(){var fc=document.getElementById("fc").value;var fs=document.getElementById("fs").value;var fv=document.getElementById("fv").value;var sigs={"all":[],"CRITICAL":["CRITICAL"],"ELEVATED":["CRITICAL","ELEVATED"],"WATCH":["CRITICAL","ELEVATED","WATCH"]};var al=sigs[fs]||[];document.querySelectorAll(".card").forEach(function(c){var co=c.getAttribute("data-company");var si=c.getAttribute("data-signal");var vi=c.getAttribute("data-view");var sh=true;if(fc!=="all"&&co!==fc)sh=false;if(fs!=="all"&&al.indexOf(si)<0)sh=false;if(fv!=="all"&&vi!==fv)sh=false;c.style.display=sh?"":"none";});}
function init(){for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}document.querySelectorAll(".cc").forEach(function(cc){cc.querySelectorAll(".cb").forEach(function(btn){btn.addEventListener("click",function(){var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}mycc.querySelectorAll(".cb:not(.rst)").forEach(function(s){s.classList.remove("active")});this.classList.add("active");var de=document.getElementById("cdesc-"+did);if(de&&chartDescs[v])de.textContent=chartDescs[v];mk(did,defined_cd[did],v);});});});}
function mk(did,D,v){var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];var evtMs=evts.map(function(e){return e.date;});
if(v==="reports"){var bc=D.l.map(function(m){return bm.indexOf(m)>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(m)>=0?"rgba(192,57,43,0.5)":"rgba(39,174,96,0.4)";});ds=[{type:"bar",label:"Raw Reports",data:D.v,backgroundColor:bc,borderWidth:0,order:2},{type:"line",label:"6mo MA",data:D.ma6,borderColor:"#1a6b3a",borderWidth:2,pointRadius:0,fill:false,order:1}];yL="Reports (Raw)";}
else if(v==="smoothed"){ds=[{type:"bar",label:"Smoothed",data:D.sm,backgroundColor:"rgba(39,174,96,0.5)",borderWidth:0,order:3},{type:"line",label:"Raw (ref)",data:D.v,borderColor:"rgba(0,0,0,0.15)",borderWidth:1,pointRadius:0,fill:false,borderDash:[4,4],order:1},{type:"line",label:"Smoothed MA",data:(function(){var r=[];for(var i=0;i<D.sm.length;i++){var s=0,c=0;for(var j=Math.max(0,i-5);j<=i;j++){s+=D.sm[j];c++;}r.push(Math.round(s/c));}return r;})(),borderColor:"#1a6b3a",borderWidth:2,pointRadius:0,fill:false,order:2}];yL="Reports (Smoothed)";}
else if(v==="rate_m"){ds=[{type:"bar",label:"Rate/$M",data:D.rm,backgroundColor:"rgba(52,152,219,0.5)",borderWidth:0}];yL="Reports/$M";}
else if(v==="rate_10k"){ds=[{type:"bar",label:"Rate/10K",data:D.r10k,backgroundColor:"rgba(155,89,182,0.5)",borderWidth:0}];yL="Reports/10K";}
else if(v==="severity"){ds=[{type:"bar",label:"Deaths",data:D.deaths,backgroundColor:"rgba(192,57,43,0.7)",borderWidth:0},{type:"bar",label:"Injuries",data:D.injuries,backgroundColor:"rgba(230,126,34,0.6)",borderWidth:0},{type:"bar",label:"Malfunctions",data:D.malfs,backgroundColor:"rgba(241,196,15,0.5)",borderWidth:0}];yL="Count";}
else if(v==="z"){ds=[{type:"line",label:"Z-Score (smoothed)",data:D.z,borderColor:"#2980b9",borderWidth:2,pointRadius:1,fill:false}];yL="Z-Score";}
else if(v==="stock"){ds=[{type:"line",label:"Stock $",data:D.stk,borderColor:"rgba(39,174,96,0.8)",borderWidth:2,pointRadius:0,fill:false,yAxisID:"y"},{type:"bar",label:"MAUDE (sm)",data:D.sm,backgroundColor:"rgba(192,57,43,0.3)",borderWidth:0,yAxisID:"y2"}];yL="Stock $";}
var opts={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:ds.length>1,position:"top",labels:{font:{size:10}}},zoom:{zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:"x"},pan:{enabled:true,mode:"x"}}},scales:{x:{ticks:{font:{size:9},maxRotation:45}},y:{title:{display:true,text:yL,font:{size:10}},ticks:{font:{size:9}}}}};
if(v==="stock"){opts.scales["y2"]={position:"right",title:{display:true,text:"MAUDE (Smoothed)"},grid:{display:false},ticks:{font:{size:9}}};}
charts[did]=new Chart(ctx,{data:{labels:D.l,datasets:ds},options:opts});}
document.addEventListener("DOMContentLoaded",init);
</script>'''
    full=html+js+"</body></html>"
    full=full.replace("__CD__",json.dumps(cd))
    with open("docs/index.html","w") as f: f.write(full)
    print(f"\nDashboard: docs/index.html ({len(full)//1024}KB)")

def send_alerts(summary):
    to,fr,pw=os.environ.get("MAUDE_EMAIL_TO"),os.environ.get("MAUDE_EMAIL_FROM"),os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl=[s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body="MAUDE Monitor V3.3 Alert\n\n"
    for s in fl: body+=f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg=MIMEMultipart();msg["From"],msg["To"]=fr,to;msg["Subject"]=f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv: srv.starttls();srv.login(fr,pw);srv.send_message(msg)
    except: pass

def main():
    p=argparse.ArgumentParser();p.add_argument("--html",action="store_true");p.add_argument("--backfill",action="store_true");p.add_argument("--quick",action="store_true")
    a=p.parse_args()
    print(f"MAUDE Monitor V3.3 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} products | Smoothing: ON")
    r,s=run_pipeline(a.backfill,a.quick); generate_html(r,s); send_alerts(s)
    print(f"\nCOMPLETE | docs/index.html")

if __name__=="__main__": main()
