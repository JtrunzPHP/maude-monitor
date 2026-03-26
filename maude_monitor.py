#!/usr/bin/env python3
"""MAUDE Monitor V3.2 — Complete self-contained single file for GitHub Actions.
All modules built inline. Enhanced correlation, recall-aware smoothing, PM case studies.
No external dependencies beyond stdlib + yfinance (optional)."""
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
# PROPER SPEARMAN WITH REAL P-VALUE (replaces old approximation)
# ============================================================
def _proper_spearman_with_pvalue(x, y):
    n = len(x)
    if n < 5: return 0.0, 1.0
    def _rank(arr):
        indexed = sorted(range(len(arr)), key=lambda i: arr[i])
        ranks = [0.0]*len(arr)
        i = 0
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
    t_stat = rho * math.sqrt((n-2)/(1.0-rho*rho))
    df = n-2
    x_val = df/(df+t_stat*t_stat)
    def _betacf(a,b,x_in):
        eps=1e-12; qab=a+b; qap=a+1.0; qam=a-1.0; c=1.0
        d=max(1.0-qab*x_in/qap, eps); d=1.0/d; h=d
        for m in range(1,201):
            m2=2*m; aa=m*(b-m)*x_in/((qam+m2)*(a+m2))
            d=max(1.0+aa*d, eps); c=max(1.0+aa/c, eps); d=1.0/d; h*=d*c
            aa=-(a+m)*(qab+m)*x_in/((a+m2)*(qap+m2))
            d=max(1.0+aa*d, eps); c=max(1.0+aa/c, eps); d=1.0/d
            delta=d*c; h*=delta
            if abs(delta-1.0)<eps: break
        return h
    def _log_gamma(z):
        if z<=0: return 0.0
        c=[76.18009172947146,-86.50532032941677,24.01409824083091,-1.231739572450155,0.1208650973866179e-2,-0.5395239384953e-5]
        y=z; tmp=z+5.5; tmp-=(z+0.5)*math.log(tmp); ser=1.000000000190015
        for j in range(6): y+=1; ser+=c[j]/y
        return -tmp+math.log(2.5066282746310005*ser/z)
    def _betai(a,b,x_in):
        if x_in<=0: return 0.0
        if x_in>=1: return 1.0
        ln_beta=_log_gamma(a)+_log_gamma(b)-_log_gamma(a+b)
        front=math.exp(math.log(max(x_in,1e-300))*a+math.log(max(1.0-x_in,1e-300))*b-ln_beta)
        if x_in<(a+1.0)/(a+b+2.0): return front*_betacf(a,b,x_in)/a
        else: return 1.0-front*_betacf(b,a,1.0-x_in)/b
    p_value = max(0.0001, min(1.0, _betai(df/2.0, 0.5, x_val)))
    return round(rho,4), round(p_value,4)

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

# Known recall batch windows for smoothing
RECALL_BATCH_WINDOWS = {
    "DXCM": [
        ("2025-01","2025-04","G6 touchscreen receiver software recall (Jan 2025)"),
        ("2025-05","2025-12","Class I G6/G7/ONE/ONE+ receiver speaker recall (May 2025, 700K+ units)"),
        ("2026-01","2026-06","Continued recall batch reporting tail"),
    ],
    "PODD": [("2023-10","2024-01","Omnipod 5 occlusion alert software update")],
    "TNDM": [],
    "ABT": [],
    "BBNX": [],
    "MDT": [],
}

PRODUCT_EVENTS = {
    "DXCM_G7":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-04","label":"G7 15-Day Cleared","type":"launch"},{"date":"2025-05","label":"Class I Recall (Receiver)","type":"recall"}],
    "DXCM_G6":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-01","label":"G6 Receiver SW Recall","type":"recall"},{"date":"2025-05","label":"Class I Recall (Receiver)","type":"recall"}],
    "DXCM_ALL":[{"date":"2024-03","label":"FDA Warning Letter","type":"regulatory"},{"date":"2025-05","label":"Class I Recall","type":"recall"}],
    "PODD_OP5":[{"date":"2024-09","label":"OP5 Gen2 Launch","type":"launch"}],
    "PODD_DASH":[],
    "PODD_ALL":[{"date":"2024-09","label":"OP5 Gen2 Launch","type":"launch"}],
    "TNDM_TSLIM":[{"date":"2024-06","label":"Mobi Launch","type":"launch"}],
    "TNDM_MOBI":[{"date":"2024-06","label":"Mobi Cleared","type":"launch"}],
    "TNDM_ALL":[{"date":"2024-06","label":"Mobi Launch","type":"launch"}],
    "ABT_LIBRE":[],
    "ABT_ALL":[],
    "BBNX_ILET":[{"date":"2025-02","label":"IPO","type":"launch"}],
    "MDT_780G":[],
    "MDT_ALL":[],
    "SQEL_TWIIST":[],
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
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance not installed — using hardcoded stock data")
        return {}
    tickers = list(set(d["ticker"] for d in DEVICES if d["ticker"] not in ("SQEL",)))
    result = {}
    for tk in tickers:
        try:
            data = yf.download(tk, period="3y", interval="1mo", progress=False)
            if data is not None and len(data) > 0:
                monthly = {}
                for idx, row in data.iterrows():
                    m = idx.strftime("%Y-%m")
                    close_val = row.get("Close") if "Close" in data.columns else row.iloc[3]
                    if hasattr(close_val,'item'): close_val = close_val.item()
                    monthly[m] = round(float(close_val), 2)
                result[tk] = monthly
                print(f"    {tk}: {len(monthly)} months of live data")
        except Exception as e:
            print(f"    {tk}: yfinance error — {str(e)[:80]}")
    return result

def merge_stock_data(hardcoded, live):
    merged = {}
    for tk in set(list(hardcoded.keys()) + list(live.keys())):
        merged[tk] = dict(hardcoded.get(tk, {}))
        if tk in live:
            merged[tk].update(live[tk])
    return merged

def get_revenue_staleness():
    try:
        lu = datetime.strptime(REVENUE_LAST_UPDATED, "%Y-%m-%d")
        days = (datetime.now() - lu).days
        if days > 120: return {"stale": True, "days": days, "message": f"STALE ({days} days old) — update after earnings"}
        elif days > 90: return {"stale": False, "days": days, "message": f"Due for update ({days} days)"}
        else: return {"stale": False, "days": days, "message": f"Current ({days} days old)"}
    except: return {"stale": True, "days": 999, "message": "Unknown staleness"}

# ============================================================
# OPENFDA DATA FETCHING
# ============================================================
def _api_get(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent":"MAUDE-Monitor/3.2"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except (HTTPError, URLError, Exception) as e:
            if attempt < retries-1: time.sleep(2*(attempt+1))
            else: print(f"  API error: {str(e)[:100]}"); return None

def fetch_counts(search_query, date_field, start_date):
    url = (f"https://api.fda.gov/device/event.json?"
           f"search=brand_name:{url_quote(search_query)}+AND+"
           f"{date_field}:[{start_date}+TO+now]"
           f"&count={date_field}")
    data = _api_get(url)
    if not data or "results" not in data: return {}
    counts = {}
    for r in data["results"]:
        d = r.get("time","")
        if len(d) >= 6:
            m = f"{d[:4]}-{d[4:6]}"
            counts[m] = counts.get(m, 0) + r.get("count", 0)
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
                if len(d)>=6:
                    m=f"{d[:4]}-{d[4:6]}"
                    sev[etype][m] = sev[etype].get(m,0) + r.get("count",0)
        time.sleep(0.3)
    return sev

# ============================================================
# STATISTICS & SCORING
# ============================================================
def compute_stats(recv_counts, sev_data, ticker):
    if not recv_counts: return None
    months = sorted(recv_counts.keys())
    vals = [recv_counts[m] for m in months]
    n = len(vals)
    if n < 3: return None
    mean_val = sum(vals)/n
    std_val = math.sqrt(sum((v-mean_val)**2 for v in vals)/n) if n > 1 else 1
    if std_val == 0: std_val = 1
    latest_m = months[-1]
    latest_v = vals[-1]
    z_score = (latest_v - mean_val)/std_val
    # 6-month trend (linear regression slope)
    recent = vals[-6:] if n >= 6 else vals
    nr = len(recent)
    if nr >= 3:
        x_mean = (nr-1)/2.0
        y_mean = sum(recent)/nr
        num = sum((i-x_mean)*(recent[i]-y_mean) for i in range(nr))
        den = sum((i-x_mean)**2 for i in range(nr))
        slope = num/den if den > 0 else 0
    else: slope = 0
    # Severity counts (last 3 months)
    last3 = months[-3:] if n >= 3 else months
    deaths_3mo = sum(sev_data.get("death",{}).get(m,0) for m in last3)
    injuries_3mo = sum(sev_data.get("injury",{}).get(m,0) for m in last3)
    malfunctions_3mo = sum(sev_data.get("malfunction",{}).get(m,0) for m in last3)
    # Rate per $M revenue
    rev_key_map = {d["ticker"]: d["rev_key"] for d in DEVICES}
    rk = rev_key_map.get(ticker, ticker)
    yr, mo = latest_m.split("-")
    q = f"{yr}-Q{(int(mo)-1)//3+1}"
    qrev = QUARTERLY_REVENUE.get(rk, {}).get(q)
    rate_per_m = latest_v / (qrev/3) * 1e6 if qrev and qrev > 0 else None
    # Rate per 10K users
    ib = INSTALLED_BASE_K.get(rk, {}).get(q)
    rate_per_10k = latest_v / ib * 10000 if ib and ib > 0 else None
    # Sigma bands
    sigma1_lo = max(0, mean_val - std_val)
    sigma1_hi = mean_val + std_val
    sigma2_lo = max(0, mean_val - 2*std_val)
    sigma2_hi = mean_val + 2*std_val
    # Moving average
    ma6 = {}
    for i, m in enumerate(months):
        window = vals[max(0,i-5):i+1]
        ma6[m] = sum(window)/len(window)
    return {
        "months": months, "values": vals, "mean": mean_val, "std": std_val,
        "z_score": z_score, "latest_month": latest_m, "latest_value": latest_v,
        "slope_6mo": slope, "deaths_3mo": deaths_3mo, "injuries_3mo": injuries_3mo,
        "malfunctions_3mo": malfunctions_3mo, "rate_per_m": rate_per_m,
        "rate_per_10k": rate_per_10k, "sigma1_lo": sigma1_lo, "sigma1_hi": sigma1_hi,
        "sigma2_lo": sigma2_lo, "sigma2_hi": sigma2_hi, "ma6": ma6,
    }

def compute_r_score(stats):
    if not stats: return None
    s = 0
    z = abs(stats["z_score"])
    if z >= 3: s += 20
    elif z >= 2: s += 15
    elif z >= 1.5: s += 10
    elif z >= 1: s += 5
    sl = stats["slope_6mo"]
    if sl > 100: s += 20
    elif sl > 50: s += 15
    elif sl > 20: s += 10
    elif sl > 0: s += 5
    d = stats["deaths_3mo"]
    if d >= 5: s += 20
    elif d >= 2: s += 15
    elif d >= 1: s += 10
    inj = stats["injuries_3mo"]
    if inj >= 50: s += 20
    elif inj >= 20: s += 15
    elif inj >= 5: s += 10
    elif inj >= 1: s += 5
    rpm = stats.get("rate_per_m")
    if rpm:
        if rpm > 500: s += 20
        elif rpm > 200: s += 15
        elif rpm > 100: s += 10
        elif rpm > 50: s += 5
    return min(100, s)

# ============================================================
# BATCH DETECTION — RECALL-AWARE
# ============================================================
def detect_batch(recv_counts, event_counts, ticker=None):
    batch = {}
    for m in recv_counts:
        rc = recv_counts.get(m, 0)
        ec = event_counts.get(m, 0)
        if ec > 0 and rc > 3 * ec:
            batch[m] = "batch"
        else:
            batch[m] = None
    if ticker:
        base_ticker = ticker.split("_")[0] if "_" in ticker else ticker
        windows = RECALL_BATCH_WINDOWS.get(base_ticker, [])
        if windows:
            sorted_months = sorted(recv_counts.keys())
            first_recall_start = min(w[0] for w in windows)
            pre_recall_months = [m for m in sorted_months if m < first_recall_start]
            if len(pre_recall_months) >= 6:
                baseline_vals = sorted([recv_counts[m] for m in pre_recall_months[-6:]])
                baseline_median = baseline_vals[len(baseline_vals)//2]
            elif pre_recall_months:
                baseline_vals = [recv_counts[m] for m in pre_recall_months]
                baseline_median = sum(baseline_vals)/len(baseline_vals)
            else:
                baseline_median = None
            for win_start, win_end, desc in windows:
                for m in sorted_months:
                    if win_start <= m <= win_end:
                        rc = recv_counts.get(m, 0)
                        if baseline_median and rc > baseline_median * 1.5:
                            if batch.get(m) is None:
                                batch[m] = {"type":"recall_batch","recall_desc":desc,
                                            "organic_est":round(baseline_median),"excess":round(rc-baseline_median)}
    return batch

def get_organic_counts(recv_counts, batch_info):
    organic = {}
    for m, rc in recv_counts.items():
        bi = batch_info.get(m)
        if isinstance(bi, dict) and "organic_est" in bi:
            organic[m] = bi["organic_est"]
        elif bi == "batch":
            organic[m] = max(1, rc // 2)
        else:
            organic[m] = rc
    return organic

# ============================================================
# MODULE 1: ENHANCED MULTI-SIGNAL CORRELATION
# ============================================================
def compute_enhanced_correlation(recv_counts, stock_prices, max_lag=6,
                                 revenue_dict=None, installed_base_dict=None):
    try:
        if not recv_counts or not stock_prices:
            return {"status":"insufficient_data","message":"Missing MAUDE or stock data.",
                    "best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,
                    "direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        common = sorted(set(recv_counts.keys()) & set(stock_prices.keys()))
        if len(common) < 14:
            return {"status":"insufficient_data","message":f"Only {len(common)} overlapping months. Need 14+.",
                    "best_rho":0,"best_p":1.0,"best_lag":0,"significant":False,
                    "direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}
        mc = [recv_counts[m] for m in common]
        sp = [stock_prices[m] for m in common]
        sr = [0.0]+[(sp[i]-sp[i-1])/sp[i-1]*100 if sp[i-1]>0 else 0.0 for i in range(1,len(sp))]
        signals = {}
        signals["raw_counts"] = mc
        deltas = [0.0]+[mc[i]-mc[i-1] for i in range(1,len(mc))]
        signals["count_delta"] = deltas
        z_scores = []
        for i in range(len(mc)):
            window = mc[max(0,i-5):i+1]
            if len(window)>=3:
                mu=sum(window)/len(window); sd=math.sqrt(sum((v-mu)**2 for v in window)/len(window))
                z_scores.append((mc[i]-mu)/sd if sd>0 else 0.0)
            else: z_scores.append(0.0)
        signals["z_score"] = z_scores
        if revenue_dict:
            rate_rev = []
            for m in common:
                yr,mo = m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"
                qrev = revenue_dict.get(q)
                rate_rev.append(recv_counts[m]/(qrev/3)*1e6 if qrev and qrev>0 else None)
            last_valid = None
            for i in range(len(rate_rev)):
                if rate_rev[i] is not None: last_valid = rate_rev[i]
                elif last_valid is not None: rate_rev[i] = last_valid
                else: rate_rev[i] = 0.0
            signals["rate_per_rev"] = rate_rev
        if installed_base_dict:
            rate_base = []
            for m in common:
                yr,mo = m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"
                ib = installed_base_dict.get(q)
                rate_base.append(recv_counts[m]/ib*10000 if ib and ib>0 else None)
            last_valid = None
            for i in range(len(rate_base)):
                if rate_base[i] is not None: last_valid = rate_base[i]
                elif last_valid is not None: rate_base[i] = last_valid
                else: rate_base[i] = 0.0
            signals["rate_per_base"] = rate_base
        accel = [0.0,0.0]+[deltas[i]-deltas[i-1] for i in range(2,len(deltas))]
        signals["acceleration"] = accel
        overall_best_rho,overall_best_p,overall_best_lag,overall_best_signal = 0,1.0,0,"raw_counts"
        signal_analysis = {}; legacy_lag_results = {}
        for sig_name, sig_vals in signals.items():
            best_rho,best_p,best_lag = 0,1.0,0; lag_detail = {}
            for lag in range(0, min(max_lag+1, len(sig_vals)-6)):
                s_slice = sig_vals[:len(sig_vals)-lag] if lag>0 else sig_vals
                r_slice = sr[lag:] if lag>0 else sr
                min_len = min(len(s_slice),len(r_slice))
                if min_len<8: continue
                sv,rv = s_slice[:min_len],r_slice[:min_len]
                if all(v==sv[0] for v in sv) or all(v==rv[0] for v in rv): continue
                rho,p = _proper_spearman_with_pvalue(sv, rv)
                lag_detail[f"{lag}mo"] = {"rho":rho,"p":p}
                if sig_name=="raw_counts": legacy_lag_results[f"{lag}mo"]={"rho":rho,"p":p}
                if abs(rho)>abs(best_rho): best_rho,best_p,best_lag = rho,p,lag
            signal_analysis[sig_name] = {"best_rho":best_rho,"best_p":best_p,"best_lag":best_lag,
                "significant":best_p<0.05,"direction":"negative" if best_rho<0 else "positive","lag_detail":lag_detail}
            if abs(best_rho)>abs(overall_best_rho):
                overall_best_rho,overall_best_p,overall_best_lag,overall_best_signal = best_rho,best_p,best_lag,sig_name
        sig_count = sum(1 for s in signal_analysis.values() if s["significant"])
        neg_count = sum(1 for s in signal_analysis.values() if s["significant"] and s["direction"]=="negative")
        avg_abs_rho = sum(abs(s["best_rho"]) for s in signal_analysis.values())/max(len(signal_analysis),1)
        consistency = neg_count/max(sig_count,1)
        confidence = min(100,int(abs(overall_best_rho)*40+avg_abs_rho*20+sig_count/len(signal_analysis)*20+consistency*20))
        overall_sig = overall_best_p<0.05
        msg = f"Best: \u03C1={overall_best_rho:+.3f} at {overall_best_lag}mo lag (p={overall_best_p:.4f}, signal={overall_best_signal}). "
        if overall_sig and overall_best_rho<-0.2: msg+=f"MAUDE {overall_best_signal} spikes predict stock declines {overall_best_lag}mo later. "
        elif overall_sig and overall_best_rho>0.2: msg+="Market pricing in MAUDE data concurrently. "
        else: msg+="No significant lead-lag detected. "
        msg+=f"Confidence: {confidence}/100 ({sig_count}/{len(signal_analysis)} signals sig"
        if neg_count>0: msg+=f", {neg_count} negative"
        msg+=")."
        return {"status":"ok","best_rho":overall_best_rho,"best_p":overall_best_p,
                "best_lag":overall_best_lag,"significant":overall_sig,
                "direction":"negative" if overall_best_rho<0 else "positive",
                "lag_results":legacy_lag_results,"message":msg,
                "best_signal":overall_best_signal,"confidence":confidence,
                "signal_analysis":signal_analysis,"signals_tested":len(signal_analysis),
                "signals_significant":sig_count,"signals_negative":neg_count}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"best_rho":0,"best_p":1.0,"best_lag":0,
                "significant":False,"direction":"none","lag_results":{},"signal_analysis":{},"confidence":0}

# ============================================================
# MODULE 2: Failure Mode Classification
# ============================================================
def analyze_failure_modes(search_query, start, limit=50):
    url = (f"https://api.fda.gov/device/event.json?"
           f"search=brand_name:{url_quote(search_query)}+AND+"
           f"date_received:[{start}+TO+now]&limit={limit}")
    data = _api_get(url)
    if not data or "results" not in data:
        return {"status":"no_data","categories":{},"total":0}
    categories = {"sensor_failure":0,"adhesion":0,"connectivity":0,"inaccurate_reading":0,
                  "skin_reaction":0,"alarm_alert":0,"battery":0,"physical_damage":0,
                  "software":0,"insertion":0,"occlusion":0,"other":0}
    keywords = {"sensor_failure":["sensor fail","no reading","sensor error","lost signal","signal loss","expired early"],
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
        for txt_field in ["mdr_text","device_report_product_code"]:
            texts = r.get(txt_field,[])
            if isinstance(texts, list):
                for t in texts:
                    narrative = t.get("text","").lower() if isinstance(t,dict) else str(t).lower()
                    if len(narrative) < 10: continue
                    total += 1
                    matched = False
                    for cat, kws in keywords.items():
                        if any(kw in narrative for kw in kws):
                            categories[cat] += 1; matched = True; break
                    if not matched: categories["other"] += 1
    top = sorted(categories.items(), key=lambda x: -x[1])[:5]
    return {"status":"ok","categories":categories,"total":total,
            "top_modes":[{"mode":k,"count":v,"pct":round(v/max(total,1)*100,1)} for k,v in top]}

# ============================================================
# MODULE 3: SEC EDGAR Filing Activity
# ============================================================
def analyze_edgar_filings(ticker):
    if ticker in ("SQEL","BBNX"): return {"status":"skip","message":"Limited EDGAR history"}
    cik_map = {"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT":"0000001800","MDT":"0000064670"}
    cik = cik_map.get(ticker)
    if not cik: return {"status":"no_cik","message":f"No CIK for {ticker}"}
    url = f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt=2024-01-01&enddt=2026-12-31&forms=10-K,10-Q,8-K,DEF+14A"
    data = _api_get(f"https://efts.sec.gov/LATEST/search-index?q={cik}&forms=8-K&dateRange=custom&startdt=2025-01-01&enddt=2026-12-31")
    try:
        url2 = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req = Request(url2, headers={"User-Agent":"MAUDE-Monitor/3.2 research@example.com"})
        with urlopen(req, timeout=15) as resp:
            filings = json.loads(resp.read().decode())
        recent = filings.get("filings",{}).get("recent",{})
        forms = recent.get("form",[])
        dates = recent.get("filingDate",[])
        descs = recent.get("primaryDocDescription",[])
        last_30 = [(f,d,descs[i] if i<len(descs) else "") for i,(f,d) in enumerate(zip(forms,dates))
                    if d >= (datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")]
        form_counts = {}
        for f,d,desc in last_30:
            form_counts[f] = form_counts.get(f,0)+1
        eight_k_count = form_counts.get("8-K",0)
        return {"status":"ok","total_90d":len(last_30),"form_counts":form_counts,
                "eight_k_count":eight_k_count,
                "message":f"{len(last_30)} filings in 90 days ({eight_k_count} 8-Ks)"}
    except Exception as e:
        return {"status":"error","message":str(e)[:100]}

# ============================================================
# MODULE 4: Insider Trading (SEC EDGAR Form 4)
# ============================================================
def analyze_insider_trading_detailed(ticker):
    if ticker in ("SQEL",): return {"status":"skip","message":"Private company"}
    cik_map = {"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT":"0000001800","MDT":"0000064670","BBNX":"0001828723"}
    cik = cik_map.get(ticker)
    if not cik: return {"status":"no_cik","message":f"No CIK for {ticker}"}
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        req = Request(url, headers={"User-Agent":"MAUDE-Monitor/3.2 research@example.com"})
        with urlopen(req, timeout=15) as resp:
            filings = json.loads(resp.read().decode())
        recent = filings.get("filings",{}).get("recent",{})
        forms = recent.get("form",[])
        dates = recent.get("filingDate",[])
        cutoff = (datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")
        form4_count = sum(1 for f,d in zip(forms,dates) if f in ("4","4/A") and d >= cutoff)
        return {"status":"ok","form4_count_90d":form4_count,
                "message":f"{form4_count} Form 4 filings in 90 days"}
    except Exception as e:
        return {"status":"error","message":str(e)[:100]}

# ============================================================
# MODULE 5: Clinical Trials (ClinicalTrials.gov)
# ============================================================
def analyze_clinical_trials(ticker):
    sponsor_map = {"DXCM":"Dexcom","PODD":"Insulet","TNDM":"Tandem+Diabetes","ABT":"Abbott","MDT":"Medtronic","BBNX":"Beta+Bionics","SQEL":"Sequel+AG"}
    sponsor = sponsor_map.get(ticker, ticker)
    try:
        url = f"https://clinicaltrials.gov/api/v2/studies?query.spons={url_quote(sponsor)}&filter.overallStatus=RECRUITING,NOT_YET_RECRUITING,ACTIVE_NOT_RECRUITING&pageSize=20"
        req = Request(url, headers={"User-Agent":"MAUDE-Monitor/3.2"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        studies = data.get("studies",[])
        active = [{"title":s.get("protocolSection",{}).get("identificationModule",{}).get("briefTitle",""),
                    "status":s.get("protocolSection",{}).get("statusModule",{}).get("overallStatus",""),
                    "phase":s.get("protocolSection",{}).get("designModule",{}).get("phases",["N/A"])}
                   for s in studies[:10]]
        return {"status":"ok","count":len(studies),"active_studies":active,
                "message":f"{len(studies)} active/recruiting trials"}
    except Exception as e:
        return {"status":"error","message":str(e)[:100]}

# ============================================================
# MODULE 6: FDA Recall Lookup (Live)
# ============================================================
def analyze_fda_recalls(search_query, ticker):
    try:
        url = (f"https://api.fda.gov/device/recall.json?"
               f"search=product_description:{url_quote(search_query)}"
               f"&sort=event_date_terminated:desc&limit=10")
        data = _api_get(url)
        if not data or "results" not in data:
            return {"status":"ok","count":0,"recalls":[],"message":"No recalls found"}
        recalls = []
        for r in data["results"][:5]:
            recalls.append({"reason":r.get("reason_for_recall","")[:120],
                            "classification":r.get("classification",""),
                            "status":r.get("status",""),
                            "date":r.get("event_date_terminated","")[:10]})
        class1 = sum(1 for r in recalls if r["classification"]=="Class I")
        return {"status":"ok","count":len(data["results"]),"recalls":recalls,
                "class1_count":class1,
                "message":f'{len(data["results"])} recalls ({class1} Class I)'}
    except Exception as e:
        return {"status":"error","message":str(e)[:100]}

# ============================================================
# MODULE 7: Recall Probability (Heuristic)
# ============================================================
def compute_recall_probability(stats, failure_modes, edgar, ticker):
    if not stats: return {"status":"insufficient_data","probability":0,"message":"No stats"}
    score = 0
    if stats["z_score"] >= 3: score += 25
    elif stats["z_score"] >= 2: score += 15
    elif stats["z_score"] >= 1.5: score += 10
    if stats["deaths_3mo"] >= 3: score += 25
    elif stats["deaths_3mo"] >= 1: score += 15
    if stats["slope_6mo"] > 50: score += 15
    elif stats["slope_6mo"] > 20: score += 10
    if failure_modes and failure_modes.get("status")=="ok":
        cats = failure_modes.get("categories",{})
        if cats.get("alarm_alert",0) > 5: score += 15
        if cats.get("sensor_failure",0) > 10: score += 10
    score = min(100, score)
    if score >= 70: level = "HIGH"
    elif score >= 40: level = "MODERATE"
    else: level = "LOW"
    return {"status":"ok","probability":score,"level":level,
            "message":f"Recall probability: {level} ({score}/100). Heuristic scoring model, not ML-trained."}

# ============================================================
# MODULE 8: Peer-Relative Ranking
# ============================================================
def compute_peer_relative(r_scores):
    if not r_scores: return {}
    sorted_peers = sorted(r_scores.items(), key=lambda x: -x[1])
    result = {}
    for i,(tk,score) in enumerate(sorted_peers):
        rank = i+1
        total = len(sorted_peers)
        if rank == 1: signal = "WORST"
        elif rank <= total*0.25: signal = "WEAK"
        elif rank >= total*0.75: signal = "STRONG"
        elif rank == total: signal = "BEST"
        else: signal = "NEUTRAL"
        result[tk] = {"rank":rank,"total":total,"score":score,"signal":signal,
                       "peers":sorted_peers,"message":f"Rank {rank}/{total} ({signal})"}
    return result

# ============================================================
# MODULE 9: Earnings Predictor (Heuristic)
# ============================================================
def compute_earnings_predictor(stats, corr, insider, trials, failure_modes, ticker):
    if not stats: return {"status":"insufficient_data","score":0}
    score = 50  # Baseline neutral
    factors = []
    # MAUDE trend
    if stats["z_score"] >= 2:
        score -= 15; factors.append(("MAUDE z-score elevated",-15))
    elif stats["z_score"] <= -1:
        score += 10; factors.append(("MAUDE below average",+10))
    if stats["slope_6mo"] > 30:
        score -= 10; factors.append(("Rising MAUDE trend",-10))
    elif stats["slope_6mo"] < -10:
        score += 5; factors.append(("Declining MAUDE trend",+5))
    # Correlation signal
    if corr and corr.get("significant") and corr.get("direction")=="negative":
        score -= 10; factors.append(("Negative MAUDE-stock correlation",-10))
    # Severity
    if stats["deaths_3mo"] >= 2:
        score -= 10; factors.append(("Recent deaths reported",-10))
    if stats["injuries_3mo"] >= 20:
        score -= 5; factors.append(("Elevated injuries",-5))
    # Failure modes
    if failure_modes and failure_modes.get("status")=="ok":
        cats = failure_modes.get("categories",{})
        if cats.get("alarm_alert",0) > 5:
            score -= 5; factors.append(("Alarm/alert issues elevated",-5))
    score = max(0, min(100, score))
    if score >= 65: outlook = "POSITIVE"
    elif score >= 40: outlook = "NEUTRAL"
    else: outlook = "NEGATIVE"
    return {"status":"ok","score":score,"outlook":outlook,"factors":factors,
            "message":f"Earnings outlook: {outlook} ({score}/100). Heuristic scoring model, not ML-trained."}

# ============================================================
# MODULE 10: Backtest Case Studies (PM-Ready)
# ============================================================
def compute_backtest_case_studies(recv_counts, stock_prices, stats, ticker, batch_info=None):
    try:
        if not recv_counts or not stock_prices or not stats:
            return {"status":"insufficient_data","message":"Need MAUDE counts, stock prices, and stats.",
                    "signals":[],"case_studies":[],"summary":{}}
        sorted_months = sorted(set(recv_counts.keys()) & set(stock_prices.keys()))
        if len(sorted_months) < 12:
            return {"status":"insufficient_data","message":f"Only {len(sorted_months)} months of overlap.",
                    "signals":[],"case_studies":[],"summary":{}}
        mc = {m:recv_counts[m] for m in sorted_months}
        sp = {m:stock_prices[m] for m in sorted_months}
        z_by_month = {}
        for i,m in enumerate(sorted_months):
            window = [mc[sorted_months[j]] for j in range(max(0,i-5),i+1)]
            if len(window)>=3:
                mu=sum(window)/len(window); sd=math.sqrt(sum((v-mu)**2 for v in window)/len(window))
                z_by_month[m] = (mc[m]-mu)/sd if sd>0 else 0.0
            else: z_by_month[m] = 0.0
        rate_change = {}; prev=None
        for m in sorted_months:
            if prev is not None and mc.get(prev,0)>0:
                rate_change[m] = (mc[m]-mc[prev])/mc[prev]*100
            else: rate_change[m] = 0.0
            prev = m
        Z_THR, SURGE_THR, COOLDOWN = 1.5, 30.0, 3
        signals, case_studies = [], []; last_sig_idx = -999
        for i,m in enumerate(sorted_months):
            if i-last_sig_idx < COOLDOWN or i < 6: continue
            z = z_by_month.get(m,0); rc = rate_change.get(m,0)
            is_batch = False
            if batch_info:
                bi = batch_info.get(m)
                if bi == "batch" or (isinstance(bi,dict) and bi.get("type") in ("batch","recall_batch")):
                    is_batch = True
            trigger = None
            if z >= Z_THR and not is_batch: trigger = f"Z-score spike: {z:+.2f}\u03C3"
            elif rc >= SURGE_THR and z >= 1.0 and not is_batch: trigger = f"MoM surge: +{rc:.0f}% (z={z:+.1f}\u03C3)"
            if trigger is None: continue
            last_sig_idx = i; entry_price = sp[m]
            fwd = {}
            for hname,hmo in [("1mo",1),("2mo",2),("3mo",3),("6mo",6)]:
                if i+hmo < len(sorted_months):
                    exit_m = sorted_months[i+hmo]; exit_price = sp[exit_m]
                    ret = (exit_price-entry_price)/entry_price*100
                    short_pnl = -(exit_price-entry_price)/entry_price*10000
                    fwd[hname] = {"exit_month":exit_m,"exit_price":round(exit_price,2),
                                  "long_return_pct":round(ret,2),"short_return_pct":round(-ret,2),
                                  "short_pnl_10k":round(short_pnl,2)}
            signal = {"month":m,"z_score":round(z,2),"mom_change_pct":round(rc,1),"trigger":trigger,
                      "entry_price":round(entry_price,2),"is_batch":is_batch,"forward_returns":fwd}
            signals.append(signal)
            best_horizon,best_short_pnl = None,0
            for h,data in fwd.items():
                if data["short_pnl_10k"]>best_short_pnl: best_short_pnl=data["short_pnl_10k"]; best_horizon=h
            cs = {"signal_month":m,"ticker":ticker,"trigger":trigger,"entry_price":round(entry_price,2),
                  "maude_reports":mc[m],"z_score":round(z,2),"best_horizon":best_horizon,
                  "best_short_pnl_10k":round(best_short_pnl,2) if best_horizon else 0,
                  "profitable_short":best_short_pnl>0 if best_horizon else False,"forward_returns":fwd}
            if best_horizon and best_short_pnl>0:
                ed = fwd[best_horizon]
                cs["narrative"] = (f"{m}: MAUDE {trigger}. {ticker} at ${entry_price:.2f}. "
                    f"Short signal. Stock fell to ${ed['exit_price']:.2f} over {best_horizon} "
                    f"({ed['short_return_pct']:+.1f}%). P&L on $10K short: +${best_short_pnl:,.0f}.")
            elif best_horizon:
                ed = fwd[best_horizon]
                cs["narrative"] = (f"{m}: MAUDE {trigger}. {ticker} at ${entry_price:.2f}. "
                    f"Short signal MISSED \u2014 stock rose over {best_horizon} ({ed['long_return_pct']:+.1f}%). "
                    f"Loss on $10K short: ${abs(best_short_pnl):,.0f}.")
            else: cs["narrative"] = f"{m}: Signal fired but insufficient forward data."
            case_studies.append(cs)
        total = len(signals); profits = sum(1 for cs in case_studies if cs.get("profitable_short"))
        total_pnl = sum(cs.get("best_short_pnl_10k",0) for cs in case_studies if cs.get("best_horizon"))
        hit_rate = profits/total*100 if total>0 else 0; avg_pnl = total_pnl/total if total>0 else 0
        if hit_rate>=60: grade="STRONG"
        elif hit_rate>=45: grade="MODERATE"
        else: grade="WEAK"
        summary = {"total_signals":total,"profitable_shorts":profits,"losing_shorts":total-profits,
                   "hit_rate_pct":round(hit_rate,1),"total_pnl_10k":round(total_pnl,2),
                   "avg_pnl_per_signal_10k":round(avg_pnl,2),"grade":grade,
                   "message":f"{grade}: {hit_rate:.0f}% hit rate, {total} signals, P&L: ${total_pnl:+,.0f}/$10K"}
        return {"status":"ok","signals":signals,"case_studies":case_studies,"summary":summary,"message":summary["message"]}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"signals":[],"case_studies":[],"summary":{}}

# ============================================================
# MODULE 11-13: Framework stubs (Google Trends, Short Interest, Payer, International)
# ============================================================
def analyze_google_trends(ticker):
    return {"status":"framework","message":"Requires pytrends package. Not available on GitHub Actions free tier."}
def analyze_short_interest(ticker):
    return {"status":"framework","message":"Requires Yahoo Finance scraping. May be blocked on GitHub Actions."}
def analyze_payer_coverage(ticker):
    coverage = {"DXCM":"Broad commercial + Medicare CGM","PODD":"Broad commercial + Medicare pump",
                "TNDM":"Broad commercial + Medicare pump","ABT":"Broad commercial + Medicare CGM",
                "MDT":"Broad commercial + Medicare pump","BBNX":"Limited (new product)","SQEL":"Pre-market"}
    return {"status":"ok","message":coverage.get(ticker,"Unknown coverage status")}
def analyze_international(ticker):
    return {"status":"framework","message":"MHRA (UK) and international regulators have no structured API for automated querying."}

# ============================================================
# HTML HELPERS FOR ENHANCED MODULES
# ============================================================
def render_case_study_html(bt, ticker):
    if not bt or bt.get("status")!="ok": return "<div class='msub'>No case study data available.</div>"
    summary = bt.get("summary",{}); cases = bt.get("case_studies",[])
    gc_map = {"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}
    grade = summary.get("grade","WEAK"); gc = gc_map.get(grade,"#888")
    html = f'<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:12px">'
    html += f'<div class="si"><div class="sil">GRADE</div><div class="siv" style="color:{gc}">{grade}</div></div>'
    html += f'<div class="si"><div class="sil">HIT RATE</div><div class="siv">{summary.get("hit_rate_pct",0):.0f}%</div></div>'
    html += f'<div class="si"><div class="sil">SIGNALS</div><div class="siv">{summary.get("total_signals",0)}</div></div>'
    pnl = summary.get("total_pnl_10k",0); pc = "#27ae60" if pnl>0 else "#c0392b"
    html += f'<div class="si"><div class="sil">TOTAL P&L/$10K</div><div class="siv" style="color:{pc}">${pnl:+,.0f}</div></div></div>'
    html += f'<div class="msub" style="margin-bottom:10px">{summary.get("message","")}</div>'
    if cases:
        html += '<table style="width:100%;border-collapse:collapse;font-size:11px">'
        html += '<tr style="background:rgba(0,0,0,0.05)"><th style="padding:4px;text-align:left">Date</th><th>Trigger</th><th>Entry</th><th>Best Exit</th><th>Return</th><th>P&L/$10K</th></tr>'
        for cs in cases[-8:]:
            bh = cs.get("best_horizon"); fwd = cs.get("forward_returns",{})
            if bh and bh in fwd:
                ed = fwd[bh]; ret_str = f'{ed["short_return_pct"]:+.1f}%'
                pv = cs.get("best_short_pnl_10k",0); pnl_str = f'${pv:+,.0f}'
                rc = "#27ae60" if pv>0 else "#c0392b"; exit_str = f'${ed["exit_price"]:.2f} ({bh})'
            else: ret_str="\u2014"; pnl_str="\u2014"; rc="#888"; exit_str="\u2014"
            trig = cs.get("trigger",""); trig = trig[:22]+"..." if len(trig)>25 else trig
            html += f'<tr><td style="padding:3px">{cs.get("signal_month","")}</td><td style="padding:3px;font-size:10px">{trig}</td>'
            html += f'<td style="padding:3px">${cs.get("entry_price",0):.2f}</td><td style="padding:3px">{exit_str}</td>'
            html += f'<td style="padding:3px;color:{rc}">{ret_str}</td><td style="padding:3px;color:{rc};font-weight:600">{pnl_str}</td></tr>'
        html += '</table>'
    html += '<div class="msub" style="margin-top:8px;font-size:10px;opacity:0.7">Strategy: Short when MAUDE z-score &gt; 1.5\u03C3 or MoM surge &gt; 30%. Batch/recall months excluded. P&L assumes $10K notional. Past performance \u2260 future results.</div>'
    return html

def render_corr_accordion_content(ec, tk):
    if not ec or not isinstance(ec,dict): return ""
    ec_rho=ec.get("best_rho",0); ec_p=ec.get("best_p",1.0); ec_lag=ec.get("best_lag",0)
    ec_sig=ec.get("significant",False); ec_conf=ec.get("confidence",0)
    best_signal=ec.get("best_signal","raw_counts"); sig_analysis=ec.get("signal_analysis",{})
    if ec_sig and ec_rho<-0.2: ec_col="#c0392b"
    elif ec_sig and ec_rho>0.2: ec_col="#e67e22"
    else: ec_col="var(--tx3)"
    conf_col="#27ae60" if ec_conf>=60 else "#f39c12" if ec_conf>=35 else "#c0392b"
    c = '<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:10px">'
    c += f'<div class="si"><div class="sil">BEST \u03C1</div><div class="siv" style="color:{ec_col}">{ec_rho:+.3f}</div></div>'
    c += f'<div class="si"><div class="sil">LAG</div><div class="siv">{ec_lag}mo</div></div>'
    c += f'<div class="si"><div class="sil">P-VALUE</div><div class="siv">{"*" if ec_sig else ""}{ec_p:.4f}</div></div>'
    c += f'<div class="si"><div class="sil">CONFIDENCE</div><div class="siv" style="color:{conf_col}">{ec_conf}/100</div></div></div>'
    c += f'<div class="msub" style="margin-bottom:8px">{ec.get("message","")}</div>'
    if sig_analysis:
        sig_labels={"raw_counts":"Raw Counts","count_delta":"MoM Change","z_score":"Z-Score",
                    "rate_per_rev":"Rate/$M Rev","rate_per_base":"Rate/10K Users","acceleration":"Acceleration"}
        c += '<table style="width:100%;border-collapse:collapse;font-size:11px;margin-top:6px">'
        c += '<tr style="background:rgba(0,0,0,0.05)"><th style="padding:3px;text-align:left">Signal</th><th>Best \u03C1</th><th>Lag</th><th>p-value</th><th>Sig?</th></tr>'
        for sname,sdata in sig_analysis.items():
            label=sig_labels.get(sname,sname); srho=sdata.get("best_rho",0); slag=sdata.get("best_lag",0)
            sp=sdata.get("best_p",1.0); ssig=sdata.get("significant",False)
            rc="#27ae60" if ssig and srho<0 else "#c0392b" if ssig and srho>0 else "#888"
            star="\u2713" if ssig else "\u2014"
            bold=' style="font-weight:600"' if sname==best_signal else ""
            c += f'<tr{bold}><td style="padding:3px">{"\u2192 " if sname==best_signal else ""}{label}</td>'
            c += f'<td style="padding:3px;color:{rc};text-align:center">{srho:+.3f}</td>'
            c += f'<td style="padding:3px;text-align:center">{slag}mo</td>'
            c += f'<td style="padding:3px;text-align:center">{sp:.4f}</td>'
            c += f'<td style="padding:3px;text-align:center">{star}</td></tr>'
        c += '</table>'
    c += '<div class="msub" style="margin-top:8px;font-size:10px;opacity:0.7">Multi-signal analysis tests 6 MAUDE metrics against stock returns at 0-6mo lags. Confidence (0-100) combines strength, significance &amp; directional consistency. Rate-normalized signals strip growth noise for cleaner correlation.</div>'
    return c
    # ============================================================
# PIPELINE
# ============================================================
def run_pipeline(backfill=False, quick=False):
    start = "20230101" if backfill else ("20250901" if quick else "20230101")
    all_res, summary = {}, []
    if HAS_MODULES: print("ALL ENHANCED MODULES LOADED (inline)")
    else: print("BASIC mode - no enhanced modules")

    print("\n=== Fetching live stock prices ===")
    live_stocks = fetch_live_stock_prices()
    global STOCK_MONTHLY, _stock_source
    STOCK_MONTHLY = merge_stock_data(STOCK_MONTHLY, live_stocks)
    _stock_source = f"LIVE ({len(live_stocks)} tickers via yfinance)" if live_stocks else "HARDCODED (install yfinance for live)"
    print(f"  {_stock_source}")
    rev_status = get_revenue_staleness()
    print(f"  Revenue: {rev_status['message']}")

    # Collect R-scores for peer ranking
    r_scores_by_company = {}

    for dev in DEVICES:
        did = dev["id"]; tk = dev["ticker"]; rk = dev.get("rev_key", tk)
        print(f"\n{'='*50}\n{dev['name']} ({tk})")

        recv = fetch_counts(dev["search"], "date_received", start); time.sleep(0.3)
        evnt = fetch_counts(dev["search"], "date_of_event", start); time.sleep(0.3)
        sev = fetch_severity(dev["search"], start)

        # RECALL-AWARE batch detection
        batch = detect_batch(recv, evnt, ticker=tk)

        stats = compute_stats(recv, sev, tk)
        rscore = compute_r_score(stats) if stats else None
        if rscore is not None and dev.get("is_combined"):
            r_scores_by_company[tk] = rscore

        modules = {"enhanced_corr":None,"failure_modes":None,"google_trends":None,
                   "insider":None,"trials":None,"short_interest":None,"edgar":None,
                   "payer":None,"international":None,"recall_prob":None,
                   "earnings_pred":None,"backtest":None,"peer_relative":None,"recalls":None}

        if HAS_MODULES and stats:
            # Organic counts for correlation (strips recall batch noise)
            organic = get_organic_counts(recv, batch)

            try:
                print("  Running: Enhanced multi-signal correlation...")
                modules["enhanced_corr"] = compute_enhanced_correlation(
                    organic, STOCK_MONTHLY.get(tk, {}), max_lag=6,
                    revenue_dict=QUARTERLY_REVENUE.get(rk, {}),
                    installed_base_dict=INSTALLED_BASE_K.get(rk, {}))
            except Exception as e:
                modules["enhanced_corr"] = {"status":"error","message":str(e)[:100]}

            if not did.endswith("_ALL"):
                try:
                    print("  Running: Failure mode classification...")
                    modules["failure_modes"] = analyze_failure_modes(dev["search"], start, limit=50)
                except Exception as e:
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
                        modules[mod_name] = {"status":"error","message":str(e)[:100]}

            try:
                print("  Running: FDA recalls...")
                modules["recalls"] = analyze_fda_recalls(dev["search"], tk)
            except Exception as e:
                modules["recalls"] = {"status":"error","message":str(e)[:100]}

            if is_company:
                try:
                    print("  Running: EDGAR filings...")
                    modules["edgar"] = analyze_edgar_filings(tk)
                except Exception as e:
                    modules["edgar"] = {"status":"error","message":str(e)[:100]}

            try:
                print("  Running: Recall probability...")
                modules["recall_prob"] = compute_recall_probability(stats, modules.get("failure_modes"), modules.get("edgar"), tk)
            except Exception as e:
                modules["recall_prob"] = {"status":"error","message":str(e)[:100]}

            try:
                print("  Running: Case study backtest...")
                modules["backtest"] = compute_backtest_case_studies(
                    recv, STOCK_MONTHLY.get(tk, {}), stats, tk, batch_info=batch)
            except Exception as e:
                modules["backtest"] = {"status":"error","message":str(e)[:100]}

            try:
                print("  Running: Earnings predictor...")
                modules["earnings_pred"] = compute_earnings_predictor(
                    stats, modules.get("enhanced_corr"), modules.get("insider"),
                    modules.get("trials"), modules.get("failure_modes"), tk)
            except Exception as e:
                modules["earnings_pred"] = {"status":"error","message":str(e)[:100]}

        # Build signal
        signal = "NORMAL"
        if rscore is not None:
            if rscore >= 70: signal = "CRITICAL"
            elif rscore >= 50: signal = "ELEVATED"
            elif rscore >= 30: signal = "WATCH"

        all_res[did] = {"device":dev,"stats":stats,"r_score":rscore,"batch":batch,
                        "recv":recv,"evnt":evnt,"sev":sev,"signal":signal,"modules":modules}

        s_entry = {"id":did,"name":dev["name"],"ticker":tk,"signal":signal,
                   "r_score":rscore or 0,"z_score":stats["z_score"] if stats else 0}
        summary.append(s_entry)
        print(f"  Signal: {signal} | R={rscore} | Z={stats['z_score']:+.2f}" if stats else f"  No data")

    # Peer-relative ranking
    if r_scores_by_company:
        peer_results = compute_peer_relative(r_scores_by_company)
        for did, res in all_res.items():
            tk = res["device"]["ticker"]
            if tk in peer_results:
                res["modules"]["peer_relative"] = peer_results[tk]

    return all_res, summary

# ============================================================
# HTML DASHBOARD GENERATION
# ============================================================
def _accordion(acc_id, title, stat_html, content):
    return (f'<div class="acc"><div class="acch" onclick="toggleAcc(\'{acc_id}\')">'
            f'<span>{title}</span>{stat_html}<span class="arr" id="arr-{acc_id}">\u25B6</span></div>'
            f'<div class="accb" id="{acc_id}" style="display:none">{content}</div></div>')

def generate_html(all_res, summary):
    now = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    rev_status = get_revenue_staleness()
    rev_warn = ' style="color:#c0392b;font-weight:600"' if rev_status.get("stale") else ""

    # Summary table rows
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

    # Build chart data and company HTML
    cd = {}  # chart data per device
    company_html = {}
    tab_ids = {}
    for comp in COMPANIES:
        tab_ids[comp] = comp.lower().replace(" ","_")
        company_html[comp] = ""

    for did, res in all_res.items():
        dev = res["device"]; st = res.get("stats"); batch = res.get("batch",{})
        recv = res.get("recv",{}); sev = res.get("sev",{})
        tk = dev["ticker"]; rk = dev.get("rev_key",tk)
        all_r = res.get("modules",{})

        if not st: continue
        months = st["months"]; vals = st["values"]

        # Chart data
        bm = [m for m,v in batch.items() if v is not None and v != False and
              (v=="batch" or v=="recall_batch" or isinstance(v,dict))]
        evts = PRODUCT_EVENTS.get(did,[])
        # Rate per $M
        rate_m_vals = []
        for m in months:
            yr,mo = m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"
            qrev = QUARTERLY_REVENUE.get(rk,{}).get(q)
            rate_m_vals.append(round(recv.get(m,0)/(qrev/3)*1e6,1) if qrev and qrev>0 else None)
        # Rate per 10K
        rate_10k_vals = []
        for m in months:
            yr,mo = m.split("-"); q=f"{yr}-Q{(int(mo)-1)//3+1}"
            ib = INSTALLED_BASE_K.get(rk,{}).get(q)
            rate_10k_vals.append(round(recv.get(m,0)/ib*10000,2) if ib and ib>0 else None)
        # Severity data
        death_vals = [sev.get("death",{}).get(m,0) for m in months]
        injury_vals = [sev.get("injury",{}).get(m,0) for m in months]
        malf_vals = [sev.get("malfunction",{}).get(m,0) for m in months]
        # Z-score series
        z_vals = []
        for i in range(len(vals)):
            window = vals[max(0,i-5):i+1]
            if len(window)>=3:
                mu=sum(window)/len(window); sd=math.sqrt(sum((v-mu)**2 for v in window)/len(window))
                z_vals.append(round((vals[i]-mu)/sd,2) if sd>0 else 0)
            else: z_vals.append(0)
        # MA6
        ma6_vals = [round(v,1) for v in st["ma6"].values()]
        # Stock data
        stk = STOCK_MONTHLY.get(tk,{})
        stk_vals = [stk.get(m) for m in months]

        cd[did] = {"l":months,"v":vals,"bm":bm,"evts":evts,"ma6":ma6_vals,
                   "s1l":round(st["sigma1_lo"],1),"s1h":round(st["sigma1_hi"],1),
                   "s2l":round(st["sigma2_lo"],1),"s2h":round(st["sigma2_hi"],1),
                   "rm":rate_m_vals,"r10k":rate_10k_vals,
                   "deaths":death_vals,"injuries":injury_vals,"malfs":malf_vals,
                   "z":z_vals,"stk":stk_vals}

        # Card HTML
        sig_colors = {"CRITICAL":"#c0392b","ELEVATED":"#e67e22","WATCH":"#f1c40f","NORMAL":"#27ae60"}
        sc = sig_colors.get(res["signal"],"#888")

        card = f'<div class="card" data-company="{dev["company"]}" data-signal="{res["signal"]}" data-view="{"combined" if dev.get("is_combined") else "individual"}">'
        card += f'<div class="ch"><span class="cn">{dev["name"]}</span><span class="cs" style="background:{sc}">{res["signal"]}</span></div>'
        card += f'<div class="sg"><div class="si"><div class="sil">R-SCORE</div><div class="siv">{res.get("r_score",0)}</div></div>'
        card += f'<div class="si"><div class="sil">Z-SCORE</div><div class="siv">{st["z_score"]:+.2f}</div></div>'
        card += f'<div class="si"><div class="sil">REPORTS</div><div class="siv">{fmt0(st["latest_value"])}</div></div>'
        card += f'<div class="si"><div class="sil">TREND</div><div class="siv">{st["slope_6mo"]:+.1f}</div></div></div>'

        # Chart container
        card += f'<div class="cc" id="cc-{did}">'
        card += '<div class="cbtns">'
        for btn_id, btn_label in [("reports","Reports"),("rate_m","Rate/$M"),("rate_10k","Rate/10K"),
                                   ("severity","Severity"),("z","Z-Score"),("stock","Stock")]:
            active = " active" if btn_id == "reports" else ""
            card += f'<button class="cb{active}" data-v="{btn_id}">{btn_label}</button>'
        card += '<button class="cb rst" data-v="reset">Reset Zoom</button></div>'
        card += f'<div class="cdesc" id="cdesc-{did}"></div>'
        card += f'<div class="cwrap"><canvas id="ch-{did}"></canvas></div></div>'

        # Module accordions
        acc_html = ""

        # 1. Failure Modes
        fm = all_r.get("failure_modes")
        if fm and isinstance(fm,dict) and fm.get("status")=="ok":
            top = fm.get("top_modes",[])
            fm_stat = f'<span class="mstat">{fm.get("total",0)} analyzed</span>'
            fm_content = '<div class="sg" style="grid-template-columns:repeat(3,1fr)">'
            for t in top[:3]:
                fm_content += f'<div class="si"><div class="sil">{t["mode"].upper()}</div><div class="siv">{t["count"]} ({t["pct"]}%)</div></div>'
            fm_content += '</div>'
            acc_html += _accordion(f"fm-{did}","Failure Mode Classification",fm_stat,fm_content)

        # 2. ENHANCED CORRELATION (multi-signal)
        ec = all_r.get("enhanced_corr")
        if ec and isinstance(ec,dict) and ec.get("status")=="ok":
            ec_rho = ec.get("best_rho",0); ec_sig = ec.get("significant",False)
            ec_conf = ec.get("confidence",0)
            if ec_sig and ec_rho<-0.2: ec_col="#c0392b"
            elif ec_sig and ec_rho>0.2: ec_col="#e67e22"
            else: ec_col="var(--tx3)"
            conf_col="#27ae60" if ec_conf>=60 else "#f39c12" if ec_conf>=35 else "#c0392b"
            ec_stat = f'<span class="mstat" style="color:{ec_col}">\u03C1={ec_rho:+.3f}</span>'
            ec_stat += f' <span class="mstat" style="color:{conf_col};font-size:11px">[{ec_conf}/100]</span>'
            ec_content = render_corr_accordion_content(ec, did)
            acc_html += _accordion(f"corr-{did}","MAUDE-Stock Correlation (Multi-Signal)",ec_stat,ec_content)

        # 3. Case Study Backtest
        bt = all_r.get("backtest")
        if bt and isinstance(bt,dict) and bt.get("status")=="ok":
            bts = bt.get("summary",{})
            grade = bts.get("grade","?"); hit = bts.get("hit_rate_pct",0)
            gc_map = {"STRONG":"#27ae60","MODERATE":"#f39c12","WEAK":"#c0392b"}
            gc = gc_map.get(grade,"#888")
            bt_stat = f'<span class="mstat" style="color:{gc}">{grade} ({hit:.0f}%)</span>'
            bt_content = render_case_study_html(bt, tk)
            acc_html += _accordion(f"bt-{did}","Trade Signal Backtest (Case Studies)",bt_stat,bt_content)

        # 4. Earnings Predictor
        ep = all_r.get("earnings_pred")
        if ep and isinstance(ep,dict) and ep.get("status")=="ok":
            epo = ep.get("outlook","?"); eps_score = ep.get("score",50)
            ep_col = "#27ae60" if epo=="POSITIVE" else "#c0392b" if epo=="NEGATIVE" else "#f39c12"
            ep_stat = f'<span class="mstat" style="color:{ep_col}">{epo} ({eps_score})</span>'
            ep_content = f'<div class="msub">{ep.get("message","")}</div>'
            factors = ep.get("factors",[])
            if factors:
                ep_content += '<div style="font-size:11px;margin-top:6px">'
                for fname,fval in factors:
                    fc = "#c0392b" if fval<0 else "#27ae60"
                    ep_content += f'<div style="padding:2px 0"><span style="color:{fc}">{fval:+d}</span> {fname}</div>'
                ep_content += '</div>'
            ep_content += '<div class="msub" style="font-size:10px;opacity:0.7;margin-top:6px">Heuristic scoring model, not ML-trained.</div>'
            acc_html += _accordion(f"ep-{did}","Earnings Predictor (Heuristic)",ep_stat,ep_content)

        # 5. Recall Probability
        rp = all_r.get("recall_prob")
        if rp and isinstance(rp,dict) and rp.get("status")=="ok":
            rpl = rp.get("level","?"); rpp = rp.get("probability",0)
            rp_col = "#c0392b" if rpl=="HIGH" else "#f39c12" if rpl=="MODERATE" else "#27ae60"
            rp_stat = f'<span class="mstat" style="color:{rp_col}">{rpl} ({rpp})</span>'
            acc_html += _accordion(f"rp-{did}","Recall Probability (Heuristic)",rp_stat,
                                   f'<div class="msub">{rp.get("message","")}</div>')

        # 6. Peer-Relative
        pr = all_r.get("peer_relative")
        if pr and isinstance(pr,dict):
            prs = pr.get("signal","?")
            prc = "#c0392b" if prs in ("WORST","WEAK") else "#27ae60" if prs in ("BEST","STRONG") else "var(--tx3)"
            pr_stat = f'<span class="mstat" style="color:{prc}">{prs} ({pr.get("rank","?")}/{pr.get("total","?")})</span>'
            pr_content = f'<div class="msub">{pr.get("message","")}</div>'
            peers = pr.get("peers",[])
            if peers:
                pr_content += '<div style="font-size:11px;margin-top:6px">'
                for ptk,pscore in peers:
                    pw = "font-weight:600" if ptk==tk else ""
                    pr_content += f'<div style="padding:2px 0;{pw}">{ptk}: R={pscore}</div>'
                pr_content += '</div>'
            acc_html += _accordion(f"pr-{did}","Peer-Relative Ranking",pr_stat,pr_content)

        # 7. FDA Recalls
        rc = all_r.get("recalls")
        if rc and isinstance(rc,dict) and rc.get("status")=="ok" and rc.get("count",0)>0:
            rc_stat = f'<span class="mstat">{rc["count"]} found</span>'
            rc_content = '<div style="font-size:11px">'
            for r in rc.get("recalls",[])[:3]:
                rc_content += f'<div style="padding:3px 0;border-bottom:1px solid rgba(0,0,0,0.1)"><strong>{r.get("classification","")}</strong> ({r.get("status","")}) — {r.get("reason","")[:100]}</div>'
            rc_content += '</div>'
            acc_html += _accordion(f"rc-{did}","FDA Recalls (Live)",rc_stat,rc_content)

        # 8. EDGAR
        ed = all_r.get("edgar")
        if ed and isinstance(ed,dict) and ed.get("status")=="ok":
            acc_html += _accordion(f"ed-{did}","SEC Filing Activity",
                f'<span class="mstat">{ed.get("total_90d",0)} (90d)</span>',
                f'<div class="msub">{ed.get("message","")}</div>')

        # 9. Insider Trading
        ins = all_r.get("insider")
        if ins and isinstance(ins,dict) and ins.get("status")=="ok":
            acc_html += _accordion(f"ins-{did}","Insider Trading (Form 4)",
                f'<span class="mstat">{ins.get("form4_count_90d",0)} filings</span>',
                f'<div class="msub">{ins.get("message","")}</div>')

        # 10. Clinical Trials
        ct = all_r.get("trials")
        if ct and isinstance(ct,dict) and ct.get("status")=="ok":
            acc_html += _accordion(f"ct-{did}","Clinical Trials",
                f'<span class="mstat">{ct.get("count",0)} active</span>',
                f'<div class="msub">{ct.get("message","")}</div>')

        # 11. Payer Coverage
        pay = all_r.get("payer")
        if pay and isinstance(pay,dict) and pay.get("status")=="ok":
            acc_html += _accordion(f"pay-{did}","Payer Coverage",
                f'<span class="mstat">Info</span>',
                f'<div class="msub">{pay.get("message","")}</div>')

        # Framework modules: only show if they have real data (not just stubs)
        for mod_name, mod_label in [("google_trends","Google Trends"),("short_interest","Short Interest"),("international","International (MHRA)")]:
            md = all_r.get(mod_name)
            if md and isinstance(md,dict) and md.get("status") not in (None,"framework","skip","error"):
                acc_html += _accordion(f"{mod_name}-{did}",mod_label,
                    f'<span class="mstat">Data</span>',
                    f'<div class="msub">{md.get("message","")}</div>')

        card += f'<div class="mods">{acc_html}</div></div>'
        company_html[dev["company"]] = company_html.get(dev["company"],"") + card

    # ============================================================
    # FULL HTML OUTPUT
    # ============================================================
    html = f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>MAUDE Monitor V3.2</title>
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
.filters{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;font-size:13px}}
.filters select{{padding:4px 8px;border:1px solid var(--bdr);border-radius:4px;font-size:12px}}
table{{width:100%;border-collapse:collapse;font-size:12px}} th,td{{padding:6px 8px;text-align:left;border-bottom:1px solid var(--bdr)}}
th{{background:var(--acc);font-weight:600;font-size:11px;text-transform:uppercase}}
.disc{{margin-top:20px;padding:12px;background:var(--acc);border-radius:8px;font-size:11px;color:var(--tx3);line-height:1.6}}
.gi{{padding:8px;background:var(--card);border:1px solid var(--bdr);border-radius:6px}}
.gi h4{{font-size:12px;margin-bottom:4px}} .gi p{{font-size:11px;color:var(--tx2);line-height:1.4}}
</style></head><body>
<div class="hdr">
<h1>MAUDE Monitor V3.2 \u2014 Medical Device Adverse Event Intelligence</h1>
<small>Updated: {now} | Stock data: {_stock_source} | {len(DEVICES)} products tracked</small><br>
<small{rev_warn}>Revenue data: {rev_status["message"]} (last updated {REVENUE_LAST_UPDATED})</small>
</div>
<div class="tabs">
<div class="tab active" onclick="showTab('overview')">Overview</div>
<div class="tab" onclick="showTab('guide')">Guide</div>'''

    for comp in COMPANIES:
        tid = tab_ids[comp]
        html += f'\n<div class="tab" onclick="showTab(\'{tid}\')">{comp}</div>'

    html += f'''</div>
<div class="tabcontent active" id="tc-overview">
<div class="filters"><label>Company:</label><select id="fc" onchange="af()"><option value="all">All</option>'''
    for c in COMPANIES: html += f'<option value="{c}">{c}</option>'
    html += '''</select><label>Signal:</label><select id="fs" onchange="af()"><option value="all">All</option>
<option value="CRITICAL">Critical</option><option value="ELEVATED">Elevated+</option>
<option value="WATCH">Watch+</option></select>
<label>View:</label><select id="fv" onchange="af()"><option value="all">All</option>
<option value="combined">Company-Level</option><option value="individual">Products Only</option></select></div>
<h2>All Products \u2014 Latest Month</h2>
<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>Trend</th><th>Deaths</th><th>Injuries</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div></div>'''

    html += '''<div class="tabcontent" id="tc-guide">
<h2>How to Read This Dashboard</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px;margin-top:8px">
<div class="gi"><h4>R-Score (0-100)</h4><p>Composite risk score. 0=safe, 100=critical. Weights: Z-score (20), trend (20), deaths (20), injuries (20), rate (20). R\u226550=investigate, R\u226570=act.</p></div>
<div class="gi"><h4>Z-Score</h4><p>Standard deviations from mean. Z\u22652=anomalous (95th percentile). Z\u22653=extreme. Negative=below average reporting.</p></div>
<div class="gi"><h4>Rate/$M Revenue</h4><p>Reports per $M revenue. Normalizes for business size. RISING rate=quality deteriorating faster than revenue growth.</p></div>
<div class="gi"><h4>Rate/10K Users</h4><p>Reports per 10K installed base. Most precise normalization. Sources: earnings calls, 10-K filings.</p></div>
<div class="gi"><h4>6mo Trend (Slope)</h4><p>Linear regression slope of last 6 months. Positive=accelerating reports. +50 means ~50 more reports/month.</p></div>
<div class="gi"><h4>Batch Detection</h4><p>Orange bars on charts. Original: received&gt;3x event count. NEW: recall-aware detection flags known recall batch windows (e.g. DXCM 700K receiver recall). Prevents false signals.</p></div>
<div class="gi"><h4>Multi-Signal Correlation</h4><p>Tests 6 MAUDE signals (raw, delta, z-score, rate/$M, rate/10K, acceleration) against stock returns at 0-6mo lags. Confidence 0-100. Rate-normalized signals strip installed base growth noise.</p></div>
<div class="gi"><h4>Case Study Backtest</h4><p>Shows specific trade signals: date, entry price, exit price, P&L. Grade (STRONG/MODERATE/WEAK) based on hit rate. Batch/recall months auto-excluded.</p></div>
</div></div>'''

    for comp in COMPANIES:
        tid = tab_ids[comp]; ch = company_html.get(comp,"<p>No data.</p>")
        html += f'\n<div class="tabcontent" id="tc-{tid}">{ch}</div>'

    html += '\n<div class="disc">Research only. Not investment advice. MAUDE has 30-90 day reporting lag. Revenue/installed base manually updated (check header). Stock via yfinance when available. R-Score and Earnings Predictor are heuristic, not ML. Recall batch windows are manually curated. Correlation is not causation.</div></div>'

    # JAVASCRIPT
    js = r'''<script>
var defined_cd=__CD__;var charts={};
var chartDescs={"reports":"REPORTS + SIGMA BANDS: Bars = monthly MAUDE reports. Shaded bands = 1\u03C3/2\u03C3 bounds. Beyond 2\u03C3 = anomalous. Orange = batch/recall dump. Red = regulatory event. Line = 6mo moving average.","rate_m":"RATE PER $M REVENUE: Reports / monthly revenue. RISING rate = quality deteriorating faster than business growth. This metric predicted the PODD selloff 4-5 months early.","rate_10k":"RATE PER 10K USERS: Reports / installed base. Most precise normalization. New products with few users show HIGH rates (correct \u2014 that IS a high failure rate).","severity":"SEVERITY BREAKDOWN: Deaths (red), injuries (orange), malfunctions (yellow). Deaths weighted 10x in R-score. MAUDE-reported, not confirmed causal.","z":"Z-SCORE TREND: Monthly z-scores over time. Crossing +2 = anomalous. Sustained elevation = systemic issue. Useful for spotting when a product enters/exits a problem period.","stock":"STOCK OVERLAY: Green = stock price, Red = MAUDE count. Look for: MAUDE spikes (red UP) preceding stock declines (green DOWN) by 1-4 months. That lead time is your alpha window."};
function showTab(id){document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active")});document.querySelectorAll(".tabcontent").forEach(function(t){t.classList.remove("active")});var ct=document.querySelector('.tab[onclick*="'+id+'"]');if(ct)ct.classList.add("active");var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");}
function toggleAcc(id){var el=document.getElementById(id);var arr=document.getElementById("arr-"+id);if(el.style.display==="none"){el.style.display="block";if(arr)arr.classList.add("open");}else{el.style.display="none";if(arr)arr.classList.remove("open");}}
function af(){var fc=document.getElementById("fc").value;var fs=document.getElementById("fs").value;var fv=document.getElementById("fv").value;var sigs={"all":[],"CRITICAL":["CRITICAL"],"ELEVATED":["CRITICAL","ELEVATED"],"WATCH":["CRITICAL","ELEVATED","WATCH"]};var allowed=sigs[fs]||[];document.querySelectorAll(".card").forEach(function(c){var comp=c.getAttribute("data-company");var sig=c.getAttribute("data-signal");var view=c.getAttribute("data-view");var show=true;if(fc!=="all"&&comp!==fc)show=false;if(fs!=="all"&&allowed.indexOf(sig)<0)show=false;if(fv!=="all"&&view!==fv)show=false;c.style.display=show?"":"none";});}
function init(){for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}document.querySelectorAll(".cc").forEach(function(cc){cc.querySelectorAll(".cb").forEach(function(btn){btn.addEventListener("click",function(){var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}mycc.querySelectorAll(".cb:not(.rst)").forEach(function(s){s.classList.remove("active")});this.classList.add("active");var descEl=document.getElementById("cdesc-"+did);if(descEl&&chartDescs[v]){descEl.textContent=chartDescs[v];}mk(did,defined_cd[did],v);});});});}
function mk(did,D,v){var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];var evtMs=evts.map(function(e){return e.date;});
if(v==="reports"){var bc=D.l.map(function(m,i){return bm.indexOf(m)>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(m)>=0?"rgba(192,57,43,0.5)":"rgba(39,174,96,0.4)";});ds=[{type:"bar",label:"Reports",data:D.v,backgroundColor:bc,borderWidth:0,order:2},{type:"line",label:"6mo MA",data:D.ma6,borderColor:"#1a6b3a",borderWidth:2,pointRadius:0,fill:false,order:1}];yL="Reports";
var ann={};ann["s1"]={type:"box",yMin:D.s1l,yMax:D.s1h,backgroundColor:"rgba(39,174,96,0.08)",borderWidth:0};ann["s2"]={type:"box",yMin:D.s2l,yMax:D.s2h,backgroundColor:"rgba(39,174,96,0.04)",borderWidth:0};
}else if(v==="rate_m"){ds=[{type:"bar",label:"Rate/$M",data:D.rm,backgroundColor:"rgba(52,152,219,0.5)",borderWidth:0}];yL="Reports/$M";}
else if(v==="rate_10k"){ds=[{type:"bar",label:"Rate/10K",data:D.r10k,backgroundColor:"rgba(155,89,182,0.5)",borderWidth:0}];yL="Reports/10K";}
else if(v==="severity"){ds=[{type:"bar",label:"Deaths",data:D.deaths,backgroundColor:"rgba(192,57,43,0.7)",borderWidth:0},{type:"bar",label:"Injuries",data:D.injuries,backgroundColor:"rgba(230,126,34,0.6)",borderWidth:0},{type:"bar",label:"Malfunctions",data:D.malfs,backgroundColor:"rgba(241,196,15,0.5)",borderWidth:0}];yL="Count";}
else if(v==="z"){ds=[{type:"line",label:"Z-Score",data:D.z,borderColor:"#2980b9",borderWidth:2,pointRadius:1,fill:false}];yL="Z-Score";var ann={};ann["z2"]={type:"line",yMin:2,yMax:2,borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[4,4]};ann["z0"]={type:"line",yMin:0,yMax:0,borderColor:"rgba(0,0,0,0.2)",borderWidth:1};}
else if(v==="stock"){var sv=D.stk||[];ds=[{type:"line",label:"Stock $",data:sv,borderColor:"rgba(39,174,96,0.8)",borderWidth:2,pointRadius:0,fill:false,yAxisID:"y"},{type:"bar",label:"MAUDE",data:D.v,backgroundColor:"rgba(192,57,43,0.3)",borderWidth:0,yAxisID:"y2"}];yL="Stock Price ($)";}
var opts={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:ds.length>1,position:"top",labels:{font:{size:10}}},zoom:{zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:"x"},pan:{enabled:true,mode:"x"}}},scales:{x:{ticks:{font:{size:9},maxRotation:45}},y:{title:{display:true,text:yL,font:{size:10}},ticks:{font:{size:9}}}}};
if(v==="stock"){opts.scales["y2"]={position:"right",title:{display:true,text:"MAUDE Reports",font:{size:10}},grid:{display:false},ticks:{font:{size:9}}};}
if(typeof ann!=="undefined"&&v==="reports"){opts.plugins.annotation={annotations:ann};}
if(typeof ann!=="undefined"&&v==="z"){opts.plugins.annotation={annotations:ann};}
charts[did]=new Chart(ctx,{data:{labels:D.l,datasets:ds},options:opts});}
document.addEventListener("DOMContentLoaded",init);
</script>'''

    full_html = html + js + "</body></html>"
    full_html = full_html.replace("__CD__", json.dumps(cd))
    with open("docs/index.html","w") as f:
        f.write(full_html)
    print(f"\nDashboard written: docs/index.html ({len(full_html)//1024}KB)")

# ============================================================
# EMAIL ALERTS
# ============================================================
def send_alerts(summary):
    to = os.environ.get("MAUDE_EMAIL_TO")
    fr = os.environ.get("MAUDE_EMAIL_FROM")
    pw = os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl = [s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body = "MAUDE Monitor V3.2 Alert\n\n"
    for s in fl:
        body += f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg = MIMEMultipart()
    msg["From"], msg["To"] = fr, to
    msg["Subject"] = f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv:
            srv.starttls(); srv.login(fr,pw); srv.send_message(msg)
    except: pass

# ============================================================
# MAIN
# ============================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--html", action="store_true")
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--quick", action="store_true")
    a = p.parse_args()
    print(f"MAUDE Monitor V3.2 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} products | Modules: ALL (inline)")
    r, s = run_pipeline(a.backfill, a.quick)
    generate_html(r, s)
    send_alerts(s)
    print(f"\nCOMPLETE | docs/index.html")

if __name__ == "__main__":
    main()
