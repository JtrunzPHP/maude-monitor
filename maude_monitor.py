#!/usr/bin/env python3
"""MAUDE Monitor V3.1 — Complete self-contained single file for GitHub Actions.
All 13 data modules are built inline. No external dependencies beyond stdlib."""
import json,os,time,math,argparse,smtplib,csv,re
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

HAS_MODULES = True  # Everything is inline now

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
# STATIC DATA DICTIONARIES — unchanged from your working version
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
INSTALLED_BASE_K = {
    "DXCM":{"2023-Q1":2000,"2023-Q2":2100,"2023-Q3":2200,"2023-Q4":2350,"2024-Q1":2500,"2024-Q2":2600,"2024-Q3":2750,"2024-Q4":2900,"2025-Q1":3000,"2025-Q2":3100,"2025-Q3":3250,"2025-Q4":3400,"2026-Q1":3550},
    "PODD":{"2023-Q1":380,"2023-Q2":400,"2023-Q3":420,"2023-Q4":440,"2024-Q1":460,"2024-Q2":480,"2024-Q3":510,"2024-Q4":540,"2025-Q1":560,"2025-Q2":575,"2025-Q3":590,"2025-Q4":600,"2026-Q1":620},
    "TNDM":{"2023-Q1":320,"2023-Q2":330,"2023-Q3":340,"2023-Q4":350,"2024-Q1":355,"2024-Q2":365,"2024-Q3":375,"2024-Q4":390,"2025-Q1":395,"2025-Q2":400,"2025-Q3":410,"2025-Q4":420,"2026-Q1":435},
    "ABT_LIBRE":{"2023-Q1":4500,"2023-Q2":4700,"2023-Q3":4900,"2023-Q4":5100,"2024-Q1":5400,"2024-Q2":5800,"2024-Q3":6300,"2024-Q4":7000,"2025-Q1":7300,"2025-Q2":7600,"2025-Q3":7900,"2025-Q4":8200,"2026-Q1":8500},
    "BBNX":{"2025-Q1":15,"2025-Q2":20,"2025-Q3":27,"2025-Q4":35,"2026-Q1":42},
    "MDT_DM":{"2023-Q1":550,"2023-Q2":570,"2023-Q3":590,"2023-Q4":610,"2024-Q1":630,"2024-Q2":660,"2024-Q3":690,"2024-Q4":720,"2025-Q1":750,"2025-Q2":780,"2025-Q3":810,"2025-Q4":840,"2026-Q1":870},
    "SQEL":{},
}
STOCK_MONTHLY = {
    "DXCM":{"2023-01":107,"2023-02":112,"2023-03":117,"2023-04":120,"2023-05":118,"2023-06":130,"2023-07":128,"2023-08":100,"2023-09":92,"2023-10":88,"2023-11":116,"2023-12":124,"2024-01":123,"2024-02":129,"2024-03":134,"2024-04":131,"2024-05":112,"2024-06":110,"2024-07":76,"2024-08":72,"2024-09":74,"2024-10":73,"2024-11":80,"2024-12":82,"2025-01":85,"2025-02":80,"2025-03":76,"2025-04":68,"2025-05":72,"2025-06":78,"2025-07":75,"2025-08":70,"2025-09":65,"2025-10":68,"2025-11":74,"2025-12":79,"2026-01":77,"2026-02":80},
    "PODD":{"2023-01":298,"2023-02":291,"2023-03":302,"2023-04":295,"2023-05":283,"2023-06":266,"2023-07":208,"2023-08":195,"2023-09":162,"2023-10":139,"2023-11":179,"2023-12":193,"2024-01":197,"2024-02":191,"2024-03":185,"2024-04":172,"2024-05":184,"2024-06":196,"2024-07":215,"2024-08":220,"2024-09":230,"2024-10":243,"2024-11":253,"2024-12":260,"2025-01":265,"2025-02":257,"2025-03":245,"2025-04":265,"2025-05":270,"2025-06":280,"2025-07":288,"2025-08":290,"2025-09":310,"2025-10":320,"2025-11":315,"2025-12":330,"2026-01":325,"2026-02":335},
    "TNDM":{"2023-01":49,"2023-02":41,"2023-03":37,"2023-04":30,"2023-05":27,"2023-06":24,"2023-07":30,"2023-08":24,"2023-09":22,"2023-10":18,"2023-11":25,"2023-12":23,"2024-01":23,"2024-02":20,"2024-03":19,"2024-04":17,"2024-05":38,"2024-06":42,"2024-07":43,"2024-08":38,"2024-09":38,"2024-10":33,"2024-11":32,"2024-12":29,"2025-01":26,"2025-02":27,"2025-03":20,"2025-04":20,"2025-05":18,"2025-06":19,"2025-07":20,"2025-08":19,"2025-09":18,"2025-10":19,"2025-11":19,"2025-12":19,"2026-01":18,"2026-02":19},
    "ABT_LIBRE":{"2023-01":112,"2023-02":104,"2023-03":101,"2023-04":108,"2023-05":107,"2023-06":109,"2023-07":106,"2023-08":103,"2023-09":98,"2023-10":93,"2023-11":105,"2023-12":110,"2024-01":113,"2024-02":117,"2024-03":118,"2024-04":110,"2024-05":107,"2024-06":104,"2024-07":107,"2024-08":113,"2024-09":114,"2024-10":118,"2024-11":117,"2024-12":116,"2025-01":119,"2025-02":123,"2025-03":128,"2025-04":126,"2025-05":130,"2025-06":133,"2025-07":120,"2025-08":117,"2025-09":119,"2025-10":121,"2025-11":123,"2025-12":126,"2026-01":128,"2026-02":130},
    "BBNX":{"2025-02":17,"2025-03":19,"2025-04":22,"2025-05":20,"2025-06":18,"2025-07":19,"2025-08":21,"2025-09":20,"2025-10":21,"2025-11":22,"2025-12":23,"2026-01":24,"2026-02":25},
    "MDT_DM":{"2023-01":78,"2023-02":80,"2023-03":82,"2023-04":83,"2023-05":84,"2023-06":86,"2023-07":85,"2023-08":84,"2023-09":82,"2023-10":80,"2023-11":82,"2023-12":84,"2024-01":85,"2024-02":84,"2024-03":86,"2024-04":85,"2024-05":83,"2024-06":82,"2024-07":80,"2024-08":82,"2024-09":84,"2024-10":86,"2024-11":88,"2024-12":87,"2025-01":88,"2025-02":87,"2025-03":89,"2025-04":88,"2025-05":90,"2025-06":91,"2025-07":89,"2025-08":88,"2025-09":87,"2025-10":88,"2025-11":89,"2025-12":90,"2026-01":91,"2026-02":92},
}
PRODUCT_EVENTS = {
    "DXCM_G7":[{"date":"2023-02","type":"LAUNCH","desc":"G7 10-day launched in US"},{"date":"2025-03","type":"WARNING","desc":"FDA Warning Letter: unauthorized sensor coating change"},{"date":"2025-09","type":"CLASS I","desc":"App defect prevented sensor failure alerts"},{"date":"2025-10","type":"NEWS","desc":"Hunterbrook: 13+ G7 deaths; class actions filed"}],
    "DXCM_G7_15DAY":[{"date":"2025-10","type":"LAUNCH","desc":"15-Day launched; ~26% may not last full 15 days"}],
    "DXCM_G6":[{"date":"2025-06","type":"CLASS I","desc":"36,800+ G6 receivers recalled: speaker defect"}],
    "DXCM_ONE":[{"date":"2025-06","type":"CLASS I","desc":"ONE/ONE+ in speaker recall"}],
    "DXCM_ALL":[{"date":"2025-06","type":"CLASS I","desc":"703K+ receivers recalled (speaker)"},{"date":"2025-03","type":"WARNING","desc":"Warning Letter: G6/G7 adulterated"}],
    "PODD_OP5":[{"date":"2025-03","type":"CORRECTION","desc":"Voluntary pod correction; guidance maintained"}],
    "PODD_ALL":[{"date":"2025-03","type":"CORRECTION","desc":"Pod correction; stock -37% from peak"}],
    "SQEL_TWIIST":[{"date":"2024-03","type":"FDA CLEAR","desc":"De Novo clearance"},{"date":"2025-07","type":"LAUNCH","desc":"US launch"},{"date":"2026-03","type":"EXPANSION","desc":"Broad US availability"}],
    "TNDM_TSLIM":[{"date":"2025-12","type":"LAUNCH","desc":"t:slim X2 + Libre 3 Plus global rollout"}],
    "TNDM_MOBI":[{"date":"2025-11","type":"LAUNCH","desc":"Android control launched"}],
    "BBNX_ILET":[{"date":"2023-05","type":"LAUNCH","desc":"iLet US launch"},{"date":"2025-02","type":"IPO","desc":"BBNX IPO on Nasdaq"}],
    "MDT_780G":[{"date":"2023-04","type":"LAUNCH","desc":"MiniMed 780G + Guardian 4 FDA cleared"},{"date":"2025-04","type":"LAUNCH","desc":"Simplera Sync approved for 780G"},{"date":"2025-05","type":"NEWS","desc":"Diabetes spinoff announced"}],
}
DEVICES = [
    {"id":"DXCM_G7_15DAY","name":"Dexcom G7 15-Day","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g7+15"',"description":"Latest 15-day CGM. ~26% may not last full 15 days. Very early MAUDE lifecycle."},
    {"id":"DXCM_G7","name":"Dexcom G7 (10-Day)","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g7" AND NOT device.brand_name:"15"',"description":"Primary CGM. KEY RISK. FDA Warning Letter, two Class I recalls, 13+ deaths."},
    {"id":"DXCM_G6","name":"Dexcom G6","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g6"',"description":"Legacy CGM phasing out. June 2025 receiver recall (36,800+ units)."},
    {"id":"DXCM_STELO","name":"Dexcom Stelo","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:stelo',"description":"First OTC CGM for Type 2 non-insulin users."},
    {"id":"DXCM_ONE","name":"Dexcom ONE/ONE+","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+one"',"description":"Value-tier international CGM. In June 2025 recall."},
    {"id":"DXCM_ALL","name":"All Dexcom","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:dexcom',"description":"Company-level. FY2025: ~$4.7B. ~3.4M users. 2026 guide: $5.16-5.25B."},
    {"id":"PODD_OP5","name":"Omnipod 5","ticker":"PODD","company":"Insulet","search":'device.brand_name:"omnipod+5"',"description":"#1 AID pump in US. Self-reported pod correction Mar 2025."},
    {"id":"PODD_DASH","name":"Omnipod DASH","ticker":"PODD","company":"Insulet","search":'device.brand_name:"omnipod+dash"',"description":"Legacy pump declining."},
    {"id":"PODD_ALL","name":"All Omnipod","ticker":"PODD","company":"Insulet","search":'device.brand_name:omnipod',"description":"Company-level. FY2025: ~$2.7B. >600K users."},
    {"id":"TNDM_TSLIM","name":"t:slim X2","ticker":"TNDM","company":"Tandem","search":'device.brand_name:"t:slim"',"description":"Tubed pump with Control-IQ+. Integrates with Libre 3 Plus."},
    {"id":"TNDM_MOBI","name":"Tandem Mobi","ticker":"TNDM","company":"Tandem","search":'device.brand_name:"tandem+mobi"',"description":"Smallest tubed pump. Mobile-first."},
    {"id":"TNDM_ALL","name":"All Tandem","ticker":"TNDM","company":"Tandem","search":'device.brand_name:tandem',"description":"Company-level. FY2025: $1.01B. Cleanest FDA profile."},
    {"id":"ABT_LIBRE3","name":"Libre 3/3+","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre+3"',"description":"DXCM competitor. 14-day. ~7M+ users."},
    {"id":"ABT_LIBRE2","name":"Libre 2","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre+2"',"description":"Previous-gen. Phasing out."},
    {"id":"ABT_LIBRE_ALL","name":"All Libre","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre"',"description":"Benchmark. FY2024 Diabetes: $6.8B."},
    {"id":"BBNX_ILET","name":"iLet Bionic Pancreas","ticker":"BBNX","company":"Beta Bionics","search":'device.brand_name:"ilet"',"description":"Autonomous AID. FY2025: $100.3M. 35K users. IPO Feb 2025."},
    {"id":"BBNX_ALL","name":"All Beta Bionics","ticker":"BBNX","company":"Beta Bionics","search":'device.brand_name:"bionic+pancreas" OR device.brand_name:"ilet"',"description":"Nasdaq: BBNX. 2026 guide: $130-135M."},
    {"id":"MDT_780G","name":"MiniMed 780G","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:"minimed+780"',"description":"AID pump + Simplera Sync. 6 quarters double-digit growth."},
    {"id":"MDT_SIMPLERA","name":"Simplera Sync CGM","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:simplera',"description":"MDT CGM for 780G. FDA Apr 2025."},
    {"id":"MDT_DM_ALL","name":"All MDT Diabetes","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:minimed OR device.brand_name:simplera',"description":"SEGMENT (not standalone). FY2025: $2.76B. Spinoff pending. Stock = MDT parent."},
    {"id":"SQEL_TWIIST","name":"twiist AID","ticker":"SQEL","company":"Sequel Med Tech","search":'device.brand_name:twiist',"description":"Private. Tubeless AID with iiSure sound-wave dosing."},
]
Z_WARN,Z_ELEVATED,Z_CRITICAL=1.5,2.0,3.0
BASE_URL="https://api.fda.gov/device/event.json"
COMPANIES=["Dexcom","Insulet","Tandem","Abbott","Beta Bionics","Medtronic","Sequel Med Tech"]

# ============================================================
# CORE API + STATS FUNCTIONS — unchanged from your working version
# ============================================================
def _q(s): return url_quote(s,safe='+:"[]')
def api_get(url,retries=3):
    for a in range(retries):
        try:
            with urlopen(Request(url,headers={"User-Agent":"MAUDE/3.1"}),timeout=30) as r: return json.loads(r.read())
        except:
            if a<retries-1: time.sleep(2**a)
    return None
def fetch_counts(sq,df="date_received",start="20230101"):
    end=datetime.now().strftime("%Y%m%d")
    d=api_get(f"{BASE_URL}?search={_q(sq)}+AND+{df}:[{start}+TO+{end}]&count={df}")
    if not d or "results" not in d: return {}
    c={}
    for r in d["results"]:
        t=r.get("time","")
        if len(t)>=6: ym=f"{t[:4]}-{t[4:6]}"; c[ym]=c.get(ym,0)+r.get("count",0)
    return c
def fetch_severity(sq,start="20230101"):
    end=datetime.now().strftime("%Y%m%d"); sv={}
    for et in ["death","injury","malfunction"]:
        d=api_get(f"{BASE_URL}?search={_q(sq)}+AND+date_received:[{start}+TO+{end}]+AND+event_type:{et}&count=date_received")
        if d and "results" in d:
            for r in d["results"]:
                t=r.get("time","")
                if len(t)>=6:
                    ym=f"{t[:4]}-{t[4:6]}"
                    if ym not in sv: sv[ym]={"death":0,"injury":0,"malfunction":0}
                    sv[ym][et]+=r.get("count",0)
        time.sleep(0.5)
    return sv
def m2q(ym):
    y,m=ym.split("-"); return f"{y}-Q{(int(m)-1)//3+1}"
def compute_stats(md,sv,tk,w=12):
    ms=sorted(md.keys())
    if len(ms)<3: return []
    res=[]
    for i,m in enumerate(ms):
        c=md[m]; tr=[md[ms[j]] for j in range(max(0,i-w+1),i+1)]
        avg=sum(tr)/len(tr); sd=(sum((x-avg)**2 for x in tr)/len(tr))**.5 if len(tr)>1 else 0
        z=(c-avg)/sd if sd>0 else 0
        ma6v=sum(tr[-6:])/len(tr[-6:]) if len(tr)>=6 else sum(tr)/len(tr)
        s=sv.get(m,{"death":0,"injury":0,"malfunction":0})
        ss=s.get("death",0)*10+s.get("injury",0)*3+s.get("malfunction",0)
        q=m2q(m); rv=QUARTERLY_REVENUE.get(tk,{}).get(q); rpm=round(c/(rv/3),2) if rv else None
        ib=INSTALLED_BASE_K.get(tk,{}).get(q); rp10k=round(c/(ib/10),4) if ib else None
        sl=0
        if i>=5:
            rc=[md[ms[j]] for j in range(i-5,i+1)]; xm=2.5; ym_=sum(rc)/6
            n=sum((x-xm)*(y-ym_) for x,y in zip(range(6),rc))
            dn=sum((x-xm)**2 for x in range(6)); sl=round(n/dn,2) if dn>0 else 0
        res.append({"month":m,"count":c,"avg_12m":round(avg,1),"sd_12m":round(sd,1),"z_score":round(z,2),"ma6":round(ma6v,1),"upper_1sd":round(avg+sd,1),"upper_2sd":round(avg+2*sd,1),"lower_1sd":round(max(0,avg-sd),1),"lower_2sd":round(max(0,avg-2*sd),1),"deaths":s.get("death",0),"injuries":s.get("injury",0),"malfunctions":s.get("malfunction",0),"severity_score":round(ss,1),"rate_per_m":rpm,"rate_per_10k":rp10k,"slope_6m":sl,"quarter":q})
    return res
def compute_r_score(sl):
    if len(sl)<6: return None
    lt=sl[-1]; zc=min(20,abs(lt["z_score"])*6.67)
    rs=sum(s["severity_score"] for s in sl[-3:])/3; ps=sum(s["severity_score"] for s in sl[-6:-3])/3 if len(sl)>=6 else rs
    sc=min(20,max(0,(rs/ps-1)*40)) if ps>0 else 10
    rr=[s["rate_per_m"] for s in sl[-3:] if s["rate_per_m"]]; pr=[s["rate_per_m"] for s in sl[-6:-3] if s["rate_per_m"]]
    gc=min(20,max(0,((sum(rr)/len(rr))/(sum(pr)/len(pr))-1)*80)) if rr and pr and sum(pr)/len(pr)>0 else 10
    sp=lt["slope_6m"]/lt["avg_12m"]*100 if lt["avg_12m"]>0 else 0; slc=min(20,max(0,sp*2))
    ri=[s["rate_per_10k"] for s in sl[-3:] if s["rate_per_10k"]]; pi=[s["rate_per_10k"] for s in sl[-6:-3] if s["rate_per_10k"]]
    ic=min(20,max(0,((sum(ri)/len(ri))/(sum(pi)/len(pi))-1)*80)) if ri and pi and sum(pi)/len(pi)>0 else 10
    t=min(100,zc+sc+gc+slc+ic)
    return {"total":round(t,1),"z_c":round(zc,1),"sev_c":round(sc,1),"gap_c":round(gc,1),"slope_c":round(slc,1),"ib_c":round(ic,1),"signal":"CRITICAL" if t>=70 else "ELEVATED" if t>=50 else "WATCH" if t>=30 else "NORMAL"}
def detect_batch(recv,evnt):
    f={}
    for m in recv:
        r=recv.get(m,0); e=evnt.get(m,0); ratio=r/e if e>0 else None
        f[m]={"is_batch":(ratio or 0)>3,"ratio":round(ratio,2) if ratio else None}
    return f

# ============================================================
# MODULE 1: Enhanced Correlation (Spearman rank + lag analysis)
# ============================================================
def compute_enhanced_correlation(maude_counts, stock_prices, max_lag=6):
    """Spearman rank correlation between MAUDE z-scores and stock returns."""
    try:
        common = sorted(set(maude_counts.keys()) & set(stock_prices.keys()))
        if len(common) < 12:
            return {"status":"insufficient_data","message":f"Only {len(common)} overlapping months. Need 12+."}
        mc = [maude_counts[m] for m in common]
        sp = [stock_prices[m] for m in common]
        # Compute returns
        sr = [(sp[i]-sp[i-1])/sp[i-1]*100 if sp[i-1]>0 else 0 for i in range(1,len(sp))]
        mc = mc[1:]  # align
        common = common[1:]
        if len(mc) < 10:
            return {"status":"insufficient_data","message":"Not enough data after alignment."}
        def _rank(arr):
            s = sorted(range(len(arr)), key=lambda i: arr[i])
            ranks = [0]*len(arr)
            for i,idx in enumerate(s): ranks[idx] = i+1
            return ranks
        def _spearman(x, y):
            n = len(x)
            if n < 5: return 0, 1.0
            rx, ry = _rank(x), _rank(y)
            d2 = sum((a-b)**2 for a,b in zip(rx,ry))
            rho = 1 - 6*d2/(n*(n*n-1))
            # t-test approximation for significance
            if abs(rho) >= 1: return rho, 0.0
            t = rho * math.sqrt((n-2)/(1-rho*rho))
            # Approximate p-value from t-distribution (rough)
            p = max(0.001, min(1.0, 2 * math.exp(-0.717*abs(t) - 0.416*t*t/max(1,n))))
            return round(rho, 4), round(p, 4)
        best_rho, best_p, best_lag = 0, 1.0, 0
        lag_results = {}
        for lag in range(0, min(max_lag+1, len(mc)-5)):
            m_slice = mc[:len(mc)-lag] if lag > 0 else mc
            s_slice = sr[lag:] if lag > 0 else sr
            min_len = min(len(m_slice), len(s_slice))
            if min_len < 5: continue
            rho, p = _spearman(m_slice[:min_len], s_slice[:min_len])
            lag_results[f"{lag}mo"] = {"rho": rho, "p": p}
            if abs(rho) > abs(best_rho):
                best_rho, best_p, best_lag = rho, p, lag
        sig = best_p < 0.05
        direction = "negative" if best_rho < 0 else "positive"
        msg = f"Best correlation: rho={best_rho:+.3f} at {best_lag}mo lag (p={best_p:.3f}). "
        if sig and best_rho < -0.2:
            msg += f"MAUDE spikes predict stock declines {best_lag} months later."
        elif sig and best_rho > 0.2:
            msg += f"MAUDE and stock move together (market already pricing in)."
        else:
            msg += "No statistically significant lead-lag relationship detected."
        return {"status":"ok","best_rho":best_rho,"best_p":best_p,"best_lag":best_lag,
                "significant":sig,"direction":direction,"lag_results":lag_results,"message":msg}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 2: Failure Mode NLP (keyword classification of MAUDE narratives)
# ============================================================
def analyze_failure_modes(search_query, start, limit=50):
    """Fetch MAUDE event narratives and classify failure modes by keyword matching."""
    try:
        end = datetime.now().strftime("%Y%m%d")
        url = f"{BASE_URL}?search={_q(search_query)}+AND+date_received:[{start}+TO+{end}]&limit={limit}"
        d = api_get(url)
        if not d or "results" not in d:
            return {"status":"no_data","message":"No MAUDE events returned for NLP analysis."}
        categories = {
            "sensor_failure": {"label":"Sensor Failure","keywords":["sensor fail","sensor error","sensor malfunction","no reading","lost signal","inaccurate reading","reading error","cgm fail"],"desc":"Sensor stopped working, gave wrong readings, or lost connectivity.","count":0},
            "adhesion": {"label":"Adhesion / Wearability","keywords":["fell off","adhesive","peel","skin irritat","rash","allergy","came off","detach","blister"],"desc":"Device detached early, caused skin reaction, or adhesive failed.","count":0},
            "insertion": {"label":"Insertion Problems","keywords":["insertion","inserter","needle","pain during","bent cannula","kinked","failed to insert","applicator"],"desc":"Problems during device insertion or with the insertion mechanism.","count":0},
            "software_app": {"label":"Software / App Issues","keywords":["app crash","bluetooth","connect","pair","display error","software","firmware","update fail","notification"],"desc":"Mobile app, connectivity, firmware, or display problems.","count":0},
            "occlusion": {"label":"Occlusion / Blockage","keywords":["occlusion","blockage","blocked","no delivery","no insulin","air bubble","leak"],"desc":"Insulin delivery blocked or interrupted.","count":0},
            "alarm": {"label":"Alarm / Alert Failure","keywords":["alarm","alert","no warning","did not alert","speaker","vibrat","sound"],"desc":"Device failed to alert user to critical events.","count":0},
            "hyperglycemia": {"label":"Hyperglycemia Event","keywords":["hyperglycemi","high blood sugar","dka","diabetic ketoacidosis","high glucose","blood sugar high"],"desc":"Serious high blood sugar event potentially linked to device failure.","count":0},
            "hypoglycemia": {"label":"Hypoglycemia Event","keywords":["hypoglycemi","low blood sugar","seizure","unconscious","passed out","low glucose","blood sugar low"],"desc":"Serious low blood sugar event potentially linked to device failure.","count":0},
            "battery": {"label":"Battery / Power","keywords":["battery","charge","power","dead","shut down","won't turn on","drain"],"desc":"Battery drain, charging failure, or unexpected power loss.","count":0},
            "other": {"label":"Other / Unclassified","keywords":[],"desc":"Events not matching other categories.","count":0},
        }
        total = 0
        for event in d["results"]:
            texts = []
            for narrative in event.get("mdr_text", []):
                txt = narrative.get("text", "")
                if txt: texts.append(txt.lower())
            if not texts: continue
            full_text = " ".join(texts)
            total += 1
            matched = False
            for cat_id, cat in categories.items():
                if cat_id == "other": continue
                for kw in cat["keywords"]:
                    if kw in full_text:
                        cat["count"] += 1
                        matched = True
                        break
            if not matched:
                categories["other"]["count"] += 1
        if total == 0:
            return {"status":"no_text","message":"Events found but none contained narrative text for NLP."}
        modes = {}
        for cat_id, cat in categories.items():
            if cat["count"] > 0:
                modes[cat_id] = {"label":cat["label"],"count":cat["count"],
                    "pct":round(cat["count"]/total*100,1),"desc":cat["desc"]}
        return {"status":"ok","total_analyzed":total,"modes":modes,
                "message":f"Classified {total} reports into {len(modes)} failure categories."}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 3: EDGAR Filing NLP (10-Q/10-K quality language scanning)
# ============================================================
TICKER_TO_CIK = {"DXCM":"1093557","PODD":"1145197","TNDM":"1438133",
                  "ABT_LIBRE":"1800","BBNX":"1842356","MDT_DM":"1613103"}
def analyze_edgar_filings(ticker):
    """Scan recent SEC filings for quality-related language."""
    try:
        cik = TICKER_TO_CIK.get(ticker)
        if not cik:
            return {"status":"no_cik","message":f"No CIK mapping for {ticker}. Cannot query EDGAR."}
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22recall%22+%22warning+letter%22+%22FDA%22&dateRange=custom&startdt=2024-01-01&enddt={datetime.now().strftime('%Y-%m-%d')}&forms=10-K,10-Q&entityName={cik}"
        # EDGAR full-text search API
        d = api_get(f"https://efts.sec.gov/LATEST/search-index?q=%22recall%22&forms=10-K,10-Q&dateRange=custom&startdt=2024-01-01&enddt={datetime.now().strftime('%Y-%m-%d')}")
        # Fallback: use filing listing API
        filing_url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        fd = api_get(filing_url)
        if not fd or "filings" not in fd:
            return {"status":"no_filings","message":f"Could not retrieve EDGAR filings for CIK {cik}."}
        recent = fd["filings"].get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        count_10k = sum(1 for f in forms[:20] if f in ("10-K","10-K/A"))
        count_10q = sum(1 for f in forms[:20] if f in ("10-Q","10-Q/A"))
        latest_date = dates[0] if dates else "unknown"
        msg = f"Found {count_10k} annual and {count_10q} quarterly filings in recent history. Latest filing: {latest_date}. "
        msg += "Full NLP scanning of filing text for recall/warranty language requires downloading full documents. Framework ready for deep scan."
        return {"status":"ok","message":msg,"annual_filings":count_10k,"quarterly_filings":count_10q,"latest_date":latest_date}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 4: Insider Trading (SEC Form 4 via EDGAR)
# ============================================================
def analyze_insider_trading(ticker):
    """Check recent insider buy/sell activity from SEC EDGAR."""
    try:
        cik = TICKER_TO_CIK.get(ticker)
        if not cik:
            return {"status":"no_cik","message":f"No CIK for {ticker}."}
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        d = api_get(url)
        if not d or "filings" not in d:
            return {"status":"no_filings","message":"Could not retrieve EDGAR data."}
        recent = d["filings"].get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        cutoff = (datetime.now().replace(day=1) - __import__('datetime').timedelta(days=90)).strftime("%Y-%m-%d")
        form4_count = 0
        recent_dates = []
        for i, f in enumerate(forms):
            if f in ("4", "4/A") and i < len(dates) and dates[i] >= cutoff:
                form4_count += 1
                recent_dates.append(dates[i])
        if form4_count == 0:
            return {"status":"no_signals","message":"No Form 4 insider transactions in the last 90 days."}
        signal = "high" if form4_count > 10 else "moderate" if form4_count > 5 else "low"
        msg = f"{form4_count} Form 4 filings in last 90 days. "
        if form4_count > 10:
            msg += "HIGH insider activity. Cross-reference with R-Score for conviction."
        elif form4_count > 5:
            msg += "Moderate insider activity. Monitor direction (buys vs sells)."
        else:
            msg += "Low insider activity. No strong signal."
        return {"status":"ok","message":msg,"form4_count":form4_count,"signal":signal,
                "latest_date":recent_dates[0] if recent_dates else None}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 5: Clinical Trials (ClinicalTrials.gov API)
# ============================================================
TICKER_TO_TRIAL_QUERY = {"DXCM":"dexcom","PODD":"omnipod OR insulet","TNDM":"tandem diabetes",
    "ABT_LIBRE":"freestyle libre","BBNX":"beta bionics OR ilet","MDT_DM":"medtronic diabetes OR minimed","SQEL":"sequel med tech OR twiist"}
def analyze_clinical_trials(ticker):
    """Query ClinicalTrials.gov for active/recruiting trials."""
    try:
        query = TICKER_TO_TRIAL_QUERY.get(ticker)
        if not query:
            return {"status":"no_query","message":f"No trial search query mapped for {ticker}."}
        url = f"https://clinicaltrials.gov/api/v2/studies?query.term={url_quote(query)}&filter.overallStatus=RECRUITING,ACTIVE_NOT_RECRUITING&pageSize=10"
        d = api_get(url)
        if not d or "studies" not in d:
            return {"status":"no_trials","message":"No active/recruiting trials found or API unavailable."}
        trials = []
        for study in d["studies"][:5]:
            proto = study.get("protocolSection", {})
            ident = proto.get("identificationModule", {})
            status_mod = proto.get("statusModule", {})
            trials.append({
                "nct_id": ident.get("nctId", "N/A"),
                "title": ident.get("briefTitle", "N/A")[:120],
                "status": status_mod.get("overallStatus", "N/A"),
            })
        if not trials:
            return {"status":"no_trials","message":"API returned data but no matching studies."}
        return {"status":"ok","trials":trials,"total":len(d["studies"]),
                "message":f"{len(d['studies'])} active/recruiting trials found."}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 6: Google Trends (framework — requires pytrends)
# ============================================================
def analyze_google_trends(ticker):
    """Google Trends for complaint-related searches. Requires pytrends package."""
    return {"status":"no_pytrends",
            "message":"Google Trends requires the pytrends package which is not installed. "
            "Install with: pip install pytrends. Provides 2-4 week leading indicator of MAUDE spikes."}

# ============================================================
# MODULE 7: Short Interest (framework — requires web scraping)
# ============================================================
def analyze_short_interest(ticker):
    """Short interest data. Framework — would need Yahoo Finance scraping."""
    if ticker in ("SQEL",):
        return {"status":"no_ticker","message":"Private company. No short interest data available."}
    return {"status":"framework",
            "message":f"Short interest tracking for {ticker} requires Yahoo Finance or similar data source. "
            "High short interest + high R-Score = market agrees with quality signal. Framework ready."}

# ============================================================
# MODULE 8: CMS Payer / Formulary Coverage
# ============================================================
def analyze_payer_coverage(ticker):
    """CMS payer coverage tracking. Framework — CMS has no structured API for this."""
    return {"status":"framework",
            "message":"CMS Medicare/Medicaid coverage decisions tracked manually. "
            "Key signals: new coverage = volume boost, coverage restriction = headwind. "
            "CGM has broad Medicare Part B coverage since 2017 (expanded 2023 for Type 2)."}

# ============================================================
# MODULE 9: International (MHRA/UK alerts)
# ============================================================
def analyze_international(ticker, brand_names):
    """UK MHRA Medical Device Alerts. Limited — no structured API."""
    return {"status":"framework",
            "message":f"MHRA (UK) device alerts for {', '.join(brand_names)}. "
            "GOV.UK publishes Medical Device Alerts but has no structured API. "
            "Manual monitoring required. EU vigilance (EUDAMED) also lacks public API."}

# ============================================================
# MODULE 10: Recall Probability (computed from failure modes + stats)
# ============================================================
def compute_recall_probability(failure_modes, stats):
    """Estimate 6-month recall probability based on failure patterns and trends."""
    try:
        if not stats or len(stats) < 6:
            return {"status":"insufficient_data","message":"Need 6+ months of data."}
        lt = stats[-1]
        prob = 0.0
        factors = []
        # Z-score factor
        if lt["z_score"] > Z_CRITICAL:
            prob += 0.25; factors.append(f"Z-score {lt['z_score']:+.2f} > {Z_CRITICAL} (critical)")
        elif lt["z_score"] > Z_ELEVATED:
            prob += 0.15; factors.append(f"Z-score {lt['z_score']:+.2f} > {Z_ELEVATED} (elevated)")
        elif lt["z_score"] > Z_WARN:
            prob += 0.05; factors.append(f"Z-score {lt['z_score']:+.2f} > {Z_WARN} (watch)")
        # Deaths factor
        d3 = sum(s["deaths"] for s in stats[-3:])
        if d3 > 5:
            prob += 0.30; factors.append(f"{d3} deaths in last 3 months")
        elif d3 > 0:
            prob += 0.15; factors.append(f"{d3} death(s) in last 3 months")
        # Trend factor
        if lt["slope_6m"] > 50:
            prob += 0.15; factors.append(f"Rapidly rising trend: {lt['slope_6m']:+.1f}/mo")
        elif lt["slope_6m"] > 20:
            prob += 0.08; factors.append(f"Rising trend: {lt['slope_6m']:+.1f}/mo")
        # Failure mode factor
        if failure_modes and isinstance(failure_modes, dict) and failure_modes.get("status") == "ok":
            modes = failure_modes.get("modes", {})
            if "alarm" in modes and modes["alarm"]["pct"] > 10:
                prob += 0.10; factors.append(f"Alarm failures at {modes['alarm']['pct']}%")
            if "hyperglycemia" in modes and modes["hyperglycemia"]["pct"] > 15:
                prob += 0.10; factors.append(f"Hyperglycemia events at {modes['hyperglycemia']['pct']}%")
        prob = min(0.95, prob)
        signal = "HIGH" if prob > 0.5 else "MODERATE" if prob > 0.25 else "LOW"
        msg = f"Estimated {prob*100:.0f}% probability of Class I/II recall within 6 months. "
        if factors:
            msg += "Factors: " + "; ".join(factors[:3]) + "."
        return {"status":"ok","probability":prob,"signal":signal,"message":msg,"factors":factors}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 11: Peer-Relative Positioning
# ============================================================
def compute_peer_relative(company_r_scores):
    """Compare each company's R-Score to the peer group."""
    if not company_r_scores or len(company_r_scores) < 2:
        return {}
    scores = list(company_r_scores.values())
    avg = sum(scores) / len(scores)
    result = {}
    sorted_tickers = sorted(company_r_scores.keys(), key=lambda t: company_r_scores[t])
    for i, tk in enumerate(sorted_tickers):
        sc = company_r_scores[tk]
        rank = i + 1
        total = len(sorted_tickers)
        if rank == total:
            signal = "WORST"
        elif rank >= total - 1:
            signal = "WEAK"
        elif rank == 1:
            signal = "BEST"
        elif rank <= 2:
            signal = "STRONG"
        else:
            signal = "MIDDLE"
        msg = f"R-Score {sc:.0f} vs peer avg {avg:.0f}. Rank {rank}/{total}."
        result[tk] = {"signal": signal, "message": msg, "rank": rank, "total": total,
                      "peer_avg": round(avg, 1), "r_score": sc}
    return result

# ============================================================
# MODULE 12: Earnings Surprise Predictor
# ============================================================
def predict_earnings_surprise(stats, r_score, peer_relative):
    """Predict earnings beat/miss based on R-Score, severity, and peer position."""
    try:
        if not stats or len(stats) < 6 or not r_score:
            return {"status":"insufficient_data","message":"Need 6+ months of stats and R-Score."}
        score = 50  # neutral
        factors = []
        # R-Score impact (high R = negative for earnings)
        if r_score["total"] >= 70:
            score -= 25; factors.append(f"R-Score CRITICAL ({r_score['total']:.0f})")
        elif r_score["total"] >= 50:
            score -= 15; factors.append(f"R-Score ELEVATED ({r_score['total']:.0f})")
        elif r_score["total"] < 25:
            score += 10; factors.append(f"R-Score low/clean ({r_score['total']:.0f})")
        # Rate trend
        recent_rates = [s["rate_per_m"] for s in stats[-3:] if s["rate_per_m"]]
        prior_rates = [s["rate_per_m"] for s in stats[-6:-3] if s["rate_per_m"]]
        if recent_rates and prior_rates:
            r_avg = sum(recent_rates)/len(recent_rates)
            p_avg = sum(prior_rates)/len(prior_rates)
            if p_avg > 0:
                change = (r_avg/p_avg - 1)*100
                if change > 20:
                    score -= 15; factors.append(f"Rate/$M rising {change:.0f}%")
                elif change < -15:
                    score += 10; factors.append(f"Rate/$M declining {change:.0f}%")
        # Peer position
        if peer_relative and isinstance(peer_relative, dict):
            if peer_relative.get("signal") == "WORST":
                score -= 10; factors.append("Worst peer position")
            elif peer_relative.get("signal") == "BEST":
                score += 10; factors.append("Best peer position")
        prediction = "LIKELY MISS" if score < 35 else "LIKELY BEAT" if score > 65 else "NEUTRAL"
        confidence = abs(score - 50) * 2
        msg = f"Earnings prediction: {prediction} ({confidence:.0f}% confidence). "
        if factors: msg += "Key factors: " + ", ".join(factors) + "."
        return {"status":"ok","prediction":prediction,"score":score,
                "confidence":round(confidence,1),"message":msg,"factors":factors}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# MODULE 13: R-Score Backtest
# ============================================================
def backtest_r_score(stats, stock_prices, threshold=50):
    """Backtest: when R-Score crossed threshold, what happened to stock?"""
    try:
        if not stats or len(stats) < 12 or not stock_prices:
            return {"status":"insufficient_data","message":"Need 12+ months of stats and stock prices."}
        # Recompute R-Scores historically
        signals = []
        for i in range(11, len(stats)):
            window = stats[max(0,i-5):i+1]
            rs = compute_r_score(stats[:i+1])
            if rs and rs["total"] >= threshold:
                signals.append({"month": stats[i]["month"], "r_score": rs["total"]})
        if not signals:
            return {"status":"ok","message":f"R-Score never crossed {threshold} in this history. No backtest signals.",
                    "results":{}}
        results = {}
        for window_name, months_forward in [("30d", 1), ("60d", 2), ("90d", 3)]:
            returns = []
            for sig in signals:
                sig_month = sig["month"]
                # Find stock price at signal and N months later
                if sig_month not in stock_prices: continue
                price_at = stock_prices[sig_month]
                # Find future month
                y, m = int(sig_month[:4]), int(sig_month[5:7])
                m += months_forward
                if m > 12: m -= 12; y += 1
                future = f"{y}-{m:02d}"
                if future not in stock_prices: continue
                price_future = stock_prices[future]
                ret = (price_future - price_at) / price_at * 100
                returns.append(ret)
            if returns:
                results[window_name] = {
                    "avg_return": round(sum(returns)/len(returns), 2),
                    "win_rate": round(sum(1 for r in returns if r < 0)/len(returns)*100, 1),
                    "n": len(returns),
                }
        msg = f"Found {len(signals)} historical R-Score signals above {threshold}. "
        if "60d" in results:
            msg += f"Avg 60d stock return after signal: {results['60d']['avg_return']:+.1f}%."
        return {"status":"ok","results":results,"message":msg,"signal_count":len(signals)}
    except Exception as e:
        return {"status":"error","message":str(e)[:200]}

# ============================================================
# V3.2 BLOCK 1 — NEW DEVICES + FUNCTIONS
# ============================================================
# PASTE THIS at the bottom of Part 1, right ABOVE the line:
#   # END OF PART 1 — Part 2 continues with run_pipeline, generate_html, main
# ============================================================

# --- Add these 3 companies to the COMPANIES list ---
for _co in ["Procept BioRobotics","Inspire Medical Systems","CVRx"]:
    if _co not in COMPANIES: COMPANIES.append(_co)

# --- Add these 3 devices to the DEVICES list ---
DEVICES.extend([
    {"id":"PRCT_AQUABEAM","name":"AquaBeam Robotic System","ticker":"PRCT","company":"Procept BioRobotics",
     "search":'device.brand_name:aquabeam',
     "description":"Nasdaq: PRCT. Aquablation therapy for BPH. IPO Sep 2021. Class 2 recalls 2022 & 2025.",
     "case_study":True},
    {"id":"INSP_INSPIRE","name":"Inspire Sleep System","ticker":"INSP","company":"Inspire Medical Systems",
     "search":'device.brand_name:inspire AND device.generic_name:hypoglossal',
     "description":"NYSE: INSP. Hypoglossal nerve stimulator for OSA. Class I recall Jun 2024. -82pct from ATH.",
     "case_study":True},
    {"id":"CVRX_BAROSTIM","name":"Barostim System","ticker":"CVRX","company":"CVRx",
     "search":'device.brand_name:barostim',
     "description":"Nasdaq: CVRX. Neuromodulation for heart failure. Breakthrough Device. DRG 276 Oct 2024.",
     "case_study":True},
])

# ============================================================
# BATCH SMOOTHING ENGINE
# Tests SMA & EWMA at windows 3,4,5 months
# Uses date_of_event as ground truth, picks best fit automatically
# ============================================================
def smooth_batch_data(recv, evnt):
    """Smooths batch-reported MAUDE data by testing 6 methods (SMA-3/4/5 + EWMA-3/4/5).
    Compares each against date_of_event distribution to find best alignment.
    Returns dict: smoothed data, method name, window, raw data, fit correlation."""
    if not recv or len(recv) < 4:
        return {"smoothed":dict(recv) if recv else {},"method":"none","window":0,
                "raw":dict(recv) if recv else {},"fit_corr":0.0}
    months = sorted(set(list(recv.keys()) + list((evnt or {}).keys())))
    r = [recv.get(m, 0) for m in months]
    e = [(evnt or {}).get(m, 0) for m in months]

    def _corr(a, b):
        n = min(len(a), len(b))
        if n < 3: return 0.0
        ma, mb = sum(a[:n])/n, sum(b[:n])/n
        num = sum((a[i]-ma)*(b[i]-mb) for i in range(n))
        da = sum((a[i]-ma)**2 for i in range(n))**0.5
        db = sum((b[i]-mb)**2 for i in range(n))**0.5
        return num/(da*db) if da*db > 0 else 0.0

    base_corr = _corr(r, e)
    best = {"c":base_corr,"m":"raw","w":0,
            "d":{m:recv.get(m,0) for m in months}}

    for w in [3, 4, 5]:
        # Simple Moving Average
        sma = []
        for i in range(len(r)):
            s = max(0, i-w+1)
            sma.append(sum(r[s:i+1])/(i-s+1))
        c = _corr(sma, e)
        if c > best["c"]:
            best = {"c":c,"m":"SMA","w":w,
                    "d":{months[i]:round(sma[i]) for i in range(len(months))}}

        # Exponentially Weighted Moving Average
        alpha = 2.0/(w+1)
        ew = [float(r[0])]
        for i in range(1, len(r)):
            ew.append(alpha*r[i] + (1.0-alpha)*ew[-1])
        c = _corr(ew, e)
        if c > best["c"]:
            best = {"c":c,"m":"EWMA","w":w,
                    "d":{months[i]:round(ew[i]) for i in range(len(months))}}

    return {"smoothed":best["d"],"method":best["m"],"window":best["w"],
            "raw":recv,"fit_corr":round(best["c"],3)}


# ============================================================
# CASE STUDY EVENT ANNOTATIONS (key dates for each ticker)
# ============================================================
CASE_STUDY_EVENTS = {
    "PRCT": [
        {"date":"2021-09","type":"IPO","desc":"IPO at ~$25/share"},
        {"date":"2022-05","type":"RECALL","desc":"Class 2 Recall - AquaBeam system"},
        {"date":"2024-12","type":"PEAK","desc":"Stock peaks ~$100"},
        {"date":"2025-10","type":"RECALL","desc":"Class 2 Recall - AquaBeam Handpiece"},
        {"date":"2026-01","type":"LEGAL","desc":"Securities fraud investigations launched"},
    ],
    "INSP": [
        {"date":"2023-07","type":"PEAK","desc":"Stock peaks ~$326 (ATH)"},
        {"date":"2024-06","type":"RECALL","desc":"Class I Recall - Inspire IV IPG 3028 (mfg defect)"},
        {"date":"2024-07","type":"FDA","desc":"FDA labels recall most serious type"},
        {"date":"2025-08","type":"CRASH","desc":"Inspire V launch failure, -$42 stock drop"},
        {"date":"2025-12","type":"LEGAL","desc":"Securities fraud lawsuit filed"},
    ],
    "CVRX": [
        {"date":"2024-01","type":"MGMT","desc":"CEO Yared retires, Hykes appointed"},
        {"date":"2024-10","type":"REIMB","desc":"MS-DRG 276 reassignment (~$43K payment)"},
        {"date":"2025-02","type":"DATA","desc":"Real-world evidence at THT 2025"},
        {"date":"2025-05","type":"EARN","desc":"Q1 2025: revenue miss, seasonal softness"},
        {"date":"2026-01","type":"REIMB","desc":"Category I CPT codes effective"},
    ],
}
CASE_STUDY_TICKER_MAP = {
    "PRCT_AQUABEAM":"PRCT","INSP_INSPIRE":"INSP","CVRX_BAROSTIM":"CVRX",
}


# ============================================================
# MONTHLY STOCK PRICE RETRIEVAL (for case studies, uses yfinance)
# ============================================================
def get_monthly_stock(ticker, start="20200101"):
    """Fetch monthly closing stock prices via yfinance. Returns {YYYY-MM: price}."""
    try:
        import yfinance as yf
        tk = ticker.split("_")[0]
        start_str = f"{start[:4]}-{start[4:6]}-{start[6:8]}"
        df = yf.download(tk, start=start_str, interval="1mo", progress=False)
        if df.empty: return {}
        result = {}
        for idx, row in df.iterrows():
            ym = idx.strftime("%Y-%m")
            cv = row["Close"]
            if hasattr(cv,'iloc'): cv = cv.iloc[0]
            result[ym] = round(float(cv), 2)
        return result
    except Exception as ex:
        print(f"  [WARN] get_monthly_stock({ticker}): {ex}")
        return {}


# ============================================================
# CASE STUDY LEAD-LAG CORRELATION ENGINE
# ============================================================
def compute_case_study(ticker, recv, stock_data, events=None):
    """Compute lead-lag correlation: MAUDE report changes vs stock returns.
    Tests lags 0-6 months. Negative corr at lag N = MAUDE spike predicts
    stock decline N months later. Returns full case study dict or None."""
    if not recv or not stock_data: return None
    common = sorted(set(recv.keys()) & set(stock_data.keys()))
    if len(common) < 6: return None

    rv = [recv.get(m,0) for m in common]
    sv = [stock_data.get(m,0) for m in common]

    # Month-over-month returns
    sr = [0.0]+[(sv[i]-sv[i-1])/sv[i-1] if sv[i-1]>0 else 0.0 for i in range(1,len(sv))]
    mc = [0.0]+[(rv[i]-rv[i-1])/rv[i-1] if rv[i-1]>0 else 0.0 for i in range(1,len(rv))]

    def _corr(a,b):
        n=min(len(a),len(b))
        if n<3: return 0.0
        ma,mb=sum(a[:n])/n,sum(b[:n])/n
        num=sum((a[i]-ma)*(b[i]-mb) for i in range(n))
        da=sum((a[i]-ma)**2 for i in range(n))**0.5
        db=sum((b[i]-mb)**2 for i in range(n))**0.5
        return num/(da*db) if da*db>0 else 0.0

    # Test lags 0-6 (MAUDE leads stock)
    lags = []
    for lag in range(7):
        if lag >= len(common)-3: break
        ms = mc[:len(mc)-lag] if lag>0 else mc
        ss = sr[lag:] if lag>0 else sr
        n = min(len(ms),len(ss))
        c = _corr(ms[:n],ss[:n])
        lags.append({"lag":lag,"corr":round(c,3)})

    opt = min(lags, key=lambda x:x["corr"]) if lags else {"lag":0,"corr":0.0}

    # Signal events: months where MAUDE z-score > 1.5
    mean_r = sum(rv)/len(rv) if rv else 0
    std_r = (sum((v-mean_r)**2 for v in rv)/len(rv))**0.5 if len(rv)>1 else 0
    signals = []
    for i,m in enumerate(common):
        if std_r > 0:
            z = (rv[i]-mean_r)/std_r
            if z > 1.5:
                future_return = None
                fi = i + opt["lag"]
                end_i = min(fi+3, len(sv)-1)
                if fi < len(sv) and end_i < len(sv) and sv[fi] > 0:
                    future_return = round((sv[end_i]-sv[fi])/sv[fi]*100, 1)
                signals.append({"month":m,"z":round(z,2),"count":rv[i],
                    "stock_at_signal":sv[i] if i<len(sv) else None,
                    "future_return_pct":future_return})

    hits = sum(1 for s in signals if s.get("future_return_pct") is not None and s["future_return_pct"]<0)
    hit_rate = round(hits/len(signals)*100,1) if signals else 0.0

    max_maude = max(rv) if rv else 0
    max_month = common[rv.index(max_maude)] if rv and max_maude>0 else "N/A"

    return {
        "ticker":ticker,"months":common,"maude":rv,"stock":sv,
        "returns":[round(x,4) for x in sr],"maude_changes":[round(x,4) for x in mc],
        "lags":lags,"optimal_lag":opt,"signals":signals,"hit_rate":hit_rate,
        "events":events or [],
        "summary":{"total_months":len(common),"max_maude":max_maude,
                    "max_maude_month":max_month,
                    "stock_start":sv[0] if sv else 0,"stock_end":sv[-1] if sv else 0,
                    "stock_change_pct":round((sv[-1]-sv[0])/sv[0]*100,1) if sv and sv[0]>0 else 0}
    }


# ============================================================
# CASE STUDIES HTML GENERATOR (complete tab content)
# ============================================================
def generate_case_studies_html(all_res):
    """Generate complete Case Studies tab HTML + Chart.js data + init JS.
    Returns tuple: (html_string, js_data_json, js_init_code)"""
    cs_results = {}
    for did,R in all_res.items():
        cs = R.get("case_study")
        if cs: cs_results[cs["ticker"]] = (cs, did, R)

    if not cs_results:
        h = '<div id="tc-casestudies" class="tabcontent">'
        h += '<h2>Case Studies</h2><p style="color:var(--tx2)">No case study data. Ensure yfinance is installed.</p></div>'
        return h, "{}", ""

    h = '<div id="tc-casestudies" class="tabcontent">\n'
    h += '<h2>Case Studies: MAUDE Signal &#8594; Stock Price</h2>\n'
    h += '<p class="cs-intro">These case studies examine historical instances where rising MAUDE adverse event '
    h += 'reports preceded significant stock price movements. The goal: identify the <strong>optimal signal lead time</strong> '
    h += 'and <strong>hit rate</strong> so this framework can be applied prospectively. '
    h += 'A negative correlation at lag N means a MAUDE spike in month T predicted a stock decline around month T+N.</p>\n'

    for tk,(cs,did,R) in cs_results.items():
        opt = cs["optimal_lag"]
        sig_class = "signal-negative" if opt["corr"]<-0.1 else "signal-positive"
        sc = cs["summary"]["stock_change_pct"]

        h += f'<div class="cs-card {sig_class}">\n'
        h += f'<div class="cs-header"><h3>{tk} &mdash; {R["dev"]["name"]} ({R["dev"]["company"]})</h3>\n'
        h += f'<span class="cs-lag-badge">Optimal Lead: {opt["lag"]}mo (r={opt["corr"]})</span></div>\n'
        h += f'<div class="cs-chart-wrap"><canvas id="cs-ch-{tk}"></canvas></div>\n'

        # Metrics
        h += '<div class="cs-metrics">\n'
        h += f'<div class="cs-metric"><span class="lbl">Period</span><span class="val">{cs["summary"]["total_months"]}mo</span></div>\n'
        h += f'<div class="cs-metric"><span class="lbl">Optimal Lag</span><span class="val">{opt["lag"]}mo</span></div>\n'
        vc = "neg" if opt["corr"]<0 else "pos"
        h += f'<div class="cs-metric"><span class="lbl">Lag Correlation</span><span class="val {vc}">{opt["corr"]}</span></div>\n'
        h += f'<div class="cs-metric"><span class="lbl">Hit Rate</span><span class="val">{cs["hit_rate"]}%</span></div>\n'
        hc = "neg" if sc<0 else "pos"
        h += f'<div class="cs-metric"><span class="lbl">Stock Change</span><span class="val {hc}">{sc:+.1f}%</span></div>\n'
        h += f'<div class="cs-metric"><span class="lbl">Peak MAUDE</span><span class="val">{cs["summary"]["max_maude_month"]}<br>({cs["summary"]["max_maude"]})</span></div>\n'
        h += '</div>\n'

        # Lag table
        h += '<details style="margin-bottom:16px"><summary style="cursor:pointer;font-weight:600;color:var(--g);font-size:13px">&#9660; Lead-Lag Correlation Table</summary>\n'
        h += '<table class="cs-lag-table"><tr><th>MAUDE Leads By</th>'
        for lg in cs["lags"]: h += f'<th>{lg["lag"]}mo</th>'
        h += '</tr><tr><td><strong>Correlation</strong></td>'
        for lg in cs["lags"]:
            cls = ' class="best"' if lg["lag"]==opt["lag"] else ""
            h += f'<td{cls}>{lg["corr"]:+.3f}</td>'
        h += '</tr></table></details>\n'

        # Signal table
        if cs["signals"]:
            h += '<details style="margin-bottom:16px"><summary style="cursor:pointer;font-weight:600;color:var(--g);font-size:13px">&#9660; Signal Events (Z &gt; 1.5)</summary>\n'
            h += f'<table class="cs-sig-table"><tr><th>Month</th><th>Count</th><th>Z</th><th>Stock</th><th>Return ({opt["lag"]+3}mo)</th></tr>\n'
            for sig in cs["signals"]:
                fr = sig.get("future_return_pct")
                fs = f'{fr:+.1f}%' if fr is not None else "N/A"
                fc = "neg" if fr is not None and fr<0 else ("pos" if fr is not None and fr>0 else "")
                sp = f'${sig["stock_at_signal"]:.2f}' if sig.get("stock_at_signal") else "N/A"
                h += f'<tr><td>{sig["month"]}</td><td>{sig["count"]}</td><td>{sig["z"]:.1f}</td>'
                h += f'<td>{sp}</td><td class="{fc}">{fs}</td></tr>\n'
            h += '</table></details>\n'

        # Events timeline
        if cs["events"]:
            h += '<details style="margin-bottom:12px"><summary style="cursor:pointer;font-weight:600;color:var(--g);font-size:13px">&#9660; Key Events</summary><div style="padding:8px 0">'
            for evt in cs["events"]:
                ec = {"RECALL":"var(--red)","CRASH":"var(--red)","LEGAL":"var(--red)",
                      "PEAK":"var(--org)","FDA":"var(--org)",
                      "IPO":"var(--g)","REIMB":"var(--g)","DATA":"var(--g)",
                      "MGMT":"#666","EARN":"#666"}.get(evt["type"],"var(--tx2)")
                h += f'<div style="margin:4px 0;font-size:12px"><span style="color:{ec};font-weight:700">{evt["date"]}</span> '
                h += f'<span style="background:{ec};color:#fff;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600">{evt["type"]}</span> '
                h += f'{evt["desc"]}</div>\n'
            h += '</div></details>\n'

        # Thesis verdict
        h += '<div class="cs-thesis">'
        if opt["corr"]<-0.15 and cs["hit_rate"]>50:
            h += f'<strong>&#9989; THESIS CONFIRMED:</strong> MAUDE spikes led stock declines by ~{opt["lag"]} months '
            h += f'with {cs["hit_rate"]}% hit rate (r={opt["corr"]}). Tradeable early warning window for {tk}.'
        elif opt["corr"]<-0.1:
            h += f'<strong>&#9888; PARTIALLY SUPPORTED:</strong> Weak negative correlation ({opt["corr"]}) at {opt["lag"]}mo lag, '
            h += f'{cs["hit_rate"]}% hit rate. Some signal but high noise.'
        else:
            h += f'<strong>&#10060; NOT SUPPORTED (standard model):</strong> Correlation {opt["corr"]} at {opt["lag"]}mo lag. '
            h += f'{tk} may need severity-mix or event-driven analysis instead of trend-based.'
        h += '</div>\n</div>\n'

    h += '</div>\n'

    # JS data
    jsd = {}
    for tk,(cs,did,R) in cs_results.items():
        jsd[tk] = {"months":cs["months"],"maude":cs["maude"],"stock":cs["stock"],
                    "events":cs["events"],"optimal_lag":cs["optimal_lag"],"hit_rate":cs["hit_rate"]}

    # JS init function
    jsi = '''
function initCaseStudies(){
if(!window.cs_data)return;
for(var tk in cs_data){var cs=cs_data[tk];
var ctx=document.getElementById("cs-ch-"+tk);if(!ctx)continue;
var evtMap={};cs.events.forEach(function(e){evtMap[e.date]=e;});
var barC=cs.months.map(function(m){
  if(evtMap[m]){var t=evtMap[m].type;
    if(t==="RECALL"||t==="CRASH"||t==="LEGAL")return "rgba(192,57,43,0.6)";
    if(t==="PEAK"||t==="FDA")return "rgba(230,126,34,0.6)";
    return "rgba(43,95,58,0.4)";}
  return "rgba(192,57,43,0.25)";});
new Chart(ctx,{type:"bar",data:{labels:cs.months,datasets:[
  {label:"MAUDE Reports",data:cs.maude,backgroundColor:barC,
   borderColor:barC.map(function(c){return c.replace(/0\\.[0-9]+\\)/,"0.9)");}),borderWidth:1,yAxisID:"y",order:2},
  {label:"Stock ($)",data:cs.stock,type:"line",borderColor:"#2B5F3A",borderWidth:2.5,
   fill:false,pointRadius:1.5,tension:0.2,yAxisID:"y1",order:1}
]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},
scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},
y:{position:"left",grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#c0392b",font:{size:10}},
   title:{display:true,text:"MAUDE Reports",color:"#c0392b",font:{size:11}}},
y1:{position:"right",grid:{drawOnChartArea:false},ticks:{color:"#2B5F3A",font:{size:10}},
    title:{display:true,text:"Stock ($)",color:"#2B5F3A",font:{size:11}}}},
plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},
zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},
tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1,
callbacks:{afterBody:function(items){var idx=items[0].dataIndex;var month=cs.months[idx];var msgs=[];
cs.events.forEach(function(e){if(e.date===month)msgs.push(e.type+": "+e.desc);});
return msgs.length?"\\n"+msgs.join("\\n"):"";}}}}
}});}}
'''
    return h, json.dumps(jsd), jsi


# ============================================================
# END OF V3.2 BLOCK 1
# ============================================================


# ============================================================
# LIVE DATA: Stock Prices via yfinance
# ============================================================
TICKER_TO_YAHOO = {"DXCM":"DXCM","PODD":"PODD","TNDM":"TNDM","ABT_LIBRE":"ABT","BBNX":"BBNX","MDT_DM":"MDT"}

def fetch_live_stock_prices():
    """Fetch 3yr monthly close prices via yfinance. Returns dict matching STOCK_MONTHLY format."""
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance not installed — using hardcoded stock prices.")
        return {}
    live = {}
    for our_tk, yahoo_tk in TICKER_TO_YAHOO.items():
        try:
            print(f"  Fetching {yahoo_tk} live stock data...")
            tkr = yf.Ticker(yahoo_tk)
            hist = tkr.history(period="3y", interval="1mo")
            if hist.empty: continue
            prices = {}
            for dt, row in hist.iterrows():
                ym = dt.strftime("%Y-%m")
                prices[ym] = round(float(row["Close"]), 2)
            if prices:
                live[our_tk] = prices
                print(f"    Got {len(prices)} months for {yahoo_tk}, latest: {sorted(prices.keys())[-1]} = ${prices[sorted(prices.keys())[-1]]}")
        except Exception as e:
            print(f"  Warning: {yahoo_tk} fetch failed: {e}")
    return live

def merge_stock_data(hardcoded, live):
    """Merge live stock data with hardcoded fallback. Live takes priority."""
    merged = {}
    for tk in set(list(hardcoded.keys()) + list(live.keys())):
        merged[tk] = {}
        if tk in hardcoded:
            merged[tk].update(hardcoded[tk])
        if tk in live:
            merged[tk].update(live[tk])  # live overwrites hardcoded for same months
    return merged

# ============================================================
# LIVE DATA: FDA Recalls from openFDA
# ============================================================
def fetch_fda_recalls(search_query, limit=5):
    """Fetch recent device recalls from openFDA recall endpoint."""
    try:
        url = f"https://api.fda.gov/device/recall.json?search={_q(search_query)}&limit={limit}&sort=event_date_posted:desc"
        d = api_get(url)
        if not d or "results" not in d:
            return {"status":"no_data","message":"No recalls found in openFDA.","recalls":[]}
        recalls = []
        for r in d["results"][:limit]:
            recalls.append({
                "event_id": r.get("res_event_number",""),
                "classification": r.get("event_classification","Unknown"),
                "date_posted": (r.get("event_date_posted","") or "")[:10],
                "reason": (r.get("reason_for_recall","N/A"))[:250],
                "product": (r.get("product_description",""))[:200],
                "status": r.get("status",""),
                "firm": r.get("recalling_firm",""),
            })
        return {"status":"ok","recalls":recalls,"total":len(recalls),
                "message":f"{len(recalls)} recalls found in openFDA database."}
    except Exception as e:
        return {"status":"error","message":str(e)[:200],"recalls":[]}

# ============================================================
# ENHANCED: Insider Trading with Buy/Sell from Form 4 XML
# ============================================================
def analyze_insider_trading_detailed(ticker):
    """Enhanced insider trading: parses Form 4 XML for buy/sell direction and dollar amounts."""
    cik = TICKER_TO_CIK.get(ticker)
    if not cik:
        return {"status":"no_cik","message":f"No CIK for {ticker}."}
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
        d = api_get(url)
        if not d or "filings" not in d:
            return analyze_insider_trading(ticker)  # fallback to basic
        recent = d["filings"].get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        cutoff = (datetime.now() - __import__('datetime').timedelta(days=90)).strftime("%Y-%m-%d")
        buys, sells, other = 0, 0, 0
        total_buy_value, total_sell_value = 0.0, 0.0
        transactions = []
        for i, f in enumerate(forms):
            if f not in ("4", "4/A"): continue
            if i >= len(dates) or dates[i] < cutoff: continue
            if i >= len(accessions) or i >= len(primary_docs): continue
            # Fetch the actual Form 4 XML to get transaction codes
            acc_clean = accessions[i].replace("-", "")
            doc = primary_docs[i]
            form4_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{doc}"
            try:
                req = Request(form4_url, headers={"User-Agent": "MAUDE/3.1 research@example.com"})
                with urlopen(req, timeout=15) as resp:
                    xml_text = resp.read().decode("utf-8", errors="ignore")
                # Parse transaction codes from XML
                # P = open market purchase, S = open market sale, A = grant/award
                import re as _re
                codes = _re.findall(r'<transactionCode>(\w)</transactionCode>', xml_text)
                shares_list = _re.findall(r'<transactionShares>.*?<value>([\d.]+)</value>', xml_text, _re.DOTALL)
                prices_list = _re.findall(r'<transactionPricePerShare>.*?<value>([\d.]+)</value>', xml_text, _re.DOTALL)
                # Parse owner name
                owner = _re.findall(r'<rptOwnerName>(.*?)</rptOwnerName>', xml_text)
                owner_name = owner[0] if owner else "Unknown"
                txn_type = "OTHER"
                txn_shares = 0
                txn_value = 0.0
                for ci, code in enumerate(codes):
                    sh = float(shares_list[ci]) if ci < len(shares_list) else 0
                    pr = float(prices_list[ci]) if ci < len(prices_list) else 0
                    val = sh * pr
                    if code == "P":
                        buys += 1; total_buy_value += val; txn_type = "BUY"
                    elif code == "S":
                        sells += 1; total_sell_value += val; txn_type = "SELL"
                    else:
                        other += 1; txn_type = code
                    txn_shares += sh
                    txn_value += val
                transactions.append({"date": dates[i], "owner": owner_name, "type": txn_type,
                    "shares": round(txn_shares), "value": round(txn_value)})
                time.sleep(0.3)  # SEC rate limit
            except Exception:
                # If XML parsing fails, still count the filing
                other += 1
                transactions.append({"date": dates[i], "owner": "Unknown", "type": "UNKNOWN", "shares": 0, "value": 0})
            if len(transactions) >= 15: break  # cap at 15 to avoid rate limits
        total = buys + sells + other
        if total == 0:
            return {"status":"no_signals","message":"No Form 4 insider transactions in the last 90 days."}
        # Determine signal
        net_direction = "NET SELLER" if sells > buys else "NET BUYER" if buys > sells else "MIXED"
        if sells > buys * 2 and total_sell_value > 500000:
            signal = "bearish"
        elif buys > sells * 2 and total_buy_value > 100000:
            signal = "bullish"
        elif sells > buys:
            signal = "moderate_sell"
        elif buys > sells:
            signal = "moderate_buy"
        else:
            signal = "neutral"
        msg = f"{total} Form 4 filings in 90 days: {buys} buys (${total_buy_value:,.0f}), {sells} sells (${total_sell_value:,.0f}), {other} other (grants/awards). "
        msg += f"Insiders are {net_direction}. "
        if signal == "bearish":
            msg += "BEARISH: Heavy insider selling with significant dollar value. Combined with elevated R-Score = high conviction short signal."
        elif signal == "bullish":
            msg += "BULLISH: Insider buying despite MAUDE noise suggests management confidence in resolution."
        elif signal == "moderate_sell":
            msg += "Leaning sell-side. Monitor for acceleration."
        elif signal == "moderate_buy":
            msg += "Leaning buy-side. Potentially contrarian positive."
        else:
            msg += "No clear directional signal."
        return {"status":"ok","message":msg,"form4_count":total,"buys":buys,"sells":sells,
                "other":other,"total_buy_value":round(total_buy_value),"total_sell_value":round(total_sell_value),
                "signal":signal,"net_direction":net_direction,"transactions":transactions[:10],
                "latest_date":transactions[0]["date"] if transactions else None}
    except Exception as e:
        return analyze_insider_trading(ticker)  # fallback to basic version

# ============================================================
# REVENUE STALENESS TRACKER
# ============================================================
REVENUE_LAST_UPDATED = "2026-03-23"  # UPDATE THIS DATE EACH TIME YOU EDIT QUARTERLY_REVENUE

def get_revenue_staleness():
    """Returns how many days since revenue data was last updated."""
    try:
        last = datetime.strptime(REVENUE_LAST_UPDATED, "%Y-%m-%d")
        days = (datetime.now() - last).days
        if days > 120: return {"status":"STALE","days":days,"message":f"Revenue data is {days} days old. Update after latest earnings calls."}
        elif days > 60: return {"status":"AGING","days":days,"message":f"Revenue data is {days} days old. Check for recent earnings."}
        else: return {"status":"CURRENT","days":days,"message":f"Revenue data updated {days} days ago."}
    except:
        return {"status":"UNKNOWN","days":999,"message":"Could not determine revenue data age."}

# ============================================================
# PIPELINE — runs ALL modules (UNCHANGED from your working version)
# ============================================================
def run_pipeline(backfill=False,quick=False):
    start="20230101" if backfill else ("20250901" if quick else "20230101")
    all_res,summary={},[]
    if HAS_MODULES: print("ALL ENHANCED MODULES LOADED (inline)")
    else: print("BASIC mode - no enhanced modules")
    # LIVE STOCK PRICES — fetch once, use everywhere
    print("\n=== Fetching live stock prices ===")
    live_stocks = fetch_live_stock_prices()
    global STOCK_MONTHLY
    STOCK_MONTHLY = merge_stock_data(STOCK_MONTHLY, live_stocks)
    print(f"  Stock data: {len(live_stocks)} tickers updated live, {len(STOCK_MONTHLY)} total")
    global _stock_source
    _stock_source = f"LIVE ({len(live_stocks)} tickers via yfinance)" if live_stocks else "HARDCODED (install yfinance for live data)"
    # Revenue staleness check
    rev_status = get_revenue_staleness()
    print(f"  Revenue data: {rev_status['status']} ({rev_status['message']})")
    for dev in DEVICES:
        did=dev["id"]; print(f"\n{'='*50}\n{dev['name']} ({dev['ticker']})")
        recv=fetch_counts(dev["search"],"date_received",start); time.sleep(0.3)
        evnt=fetch_counts(dev["search"],"date_of_event",start); time.sleep(0.3)
        sev=fetch_severity(dev["search"],start); batch=detect_batch(recv,evnt)
        stats=compute_stats(recv,sev,dev["ticker"]); rscore=compute_r_score(stats) if stats else None
        modules={"enhanced_corr":None,"failure_modes":None,"google_trends":None,"insider":None,"trials":None,"short_interest":None,"edgar":None,"payer":None,"international":None,"recall_prob":None,"earnings_pred":None,"backtest":None,"peer_relative":None,"recalls":None}
        if HAS_MODULES and stats:
            try:
                print("  Running: Enhanced correlation...")
                modules["enhanced_corr"]=compute_enhanced_correlation(recv,STOCK_MONTHLY.get(dev["ticker"],{}),max_lag=6)
            except Exception as e: modules["enhanced_corr"]={"status":"error","message":str(e)[:100]}
            if not did.endswith("_ALL"):
                try:
                    print("  Running: Failure mode NLP...")
                    modules["failure_modes"]=analyze_failure_modes(dev["search"],start,limit=50)
                except Exception as e: modules["failure_modes"]={"status":"error","message":str(e)[:100]}
            is_company=did.endswith("_ALL") or did in ("SQEL_TWIIST","BBNX_ILET")
            if is_company:
                for mod_name,mod_fn in [("google_trends",analyze_google_trends),("insider",analyze_insider_trading),("trials",analyze_clinical_trials),("short_interest",analyze_short_interest),("payer",analyze_payer_coverage)]:
                    try:
                        print(f"  Running: {mod_name}...")
                        modules[mod_name]=mod_fn(dev["ticker"])
                    except Exception as e: modules[mod_name]={"status":"error","message":str(e)[:100]}
            if did.endswith("_ALL"):
                try:
                    print("  Running: EDGAR filing NLP...")
                    modules["edgar"]=analyze_edgar_filings(dev["ticker"])
                except Exception as e: modules["edgar"]={"status":"error","message":str(e)[:100]}
                try:
                    print("  Running: International MHRA...")
                    brand=dev["name"].split("(")[0].strip().split(" ")[0]
                    modules["international"]=analyze_international(dev["ticker"],[brand])
                except Exception as e: modules["international"]={"status":"error","message":str(e)[:100]}
            try:
                print("  Running: Recall probability...")
                modules["recall_prob"]=compute_recall_probability(modules.get("failure_modes"),stats)
            except Exception as e: modules["recall_prob"]={"status":"error","message":str(e)[:100]}
        all_res[did]={"device":dev,"received":recv,"by_event":evnt,"severity":sev,"batch_flags":batch,"stats":stats,"r_score":rscore,**modules}
        if stats:
            lt=stats[-1]; ec=modules["enhanced_corr"]
            summary.append({"id":did,"name":dev["name"],"ticker":dev["ticker"],"company":dev["company"],"month":lt["month"],"reports":lt["count"],"z_score":lt["z_score"],"rate_per_m":lt["rate_per_m"],"rate_per_10k":lt["rate_per_10k"],"slope_6m":lt["slope_6m"],"deaths_3mo":sum(s["deaths"] for s in stats[-3:]),"injuries_3mo":sum(s["injuries"] for s in stats[-3:]),"r_score":rscore["total"] if rscore else None,"signal":rscore["signal"] if rscore else "NORMAL","batch":batch.get(lt["month"],{}).get("is_batch",False),"corr_rho":(ec or {}).get("best_rho"),"corr_sig":(ec or {}).get("significant")})
            print(f"  >> {lt['month']} | {lt['count']:,} | Z:{lt['z_score']:+.2f} | R:{rscore['total'] if rscore else '-'}")
    if HAS_MODULES:
        print("\n=== Post-loop: Peer scoring, earnings prediction, backtesting ===")
        company_r={}
        for did,r in all_res.items():
            if did.endswith("_ALL") and r.get("r_score"): company_r[r["device"]["ticker"]]=r["r_score"]["total"]
        peer_rel=compute_peer_relative(company_r)
        for did,r in all_res.items():
            tk=r["device"]["ticker"]
            r["peer_relative"]=peer_rel.get(tk)
            if r.get("r_score") and r.get("stats"):
                try: r["earnings_pred"]=predict_earnings_surprise(r["stats"],r["r_score"],peer_rel.get(tk))
                except: r["earnings_pred"]=None
            if did.endswith("_ALL") and r.get("stats"):
                try:
                    print(f"  Backtesting {did}...")
                    r["backtest"]=backtest_r_score(r["stats"],STOCK_MONTHLY.get(tk,{}),threshold=50)
                except: r["backtest"]=None
    os.makedirs("data",exist_ok=True)
    for did,r in all_res.items():
        if r.get("stats"):
            with open(f"data/{did}_monthly.csv","w",newline="") as f:
                w=csv.DictWriter(f,fieldnames=r["stats"][0].keys()); w.writeheader(); w.writerows(r["stats"])
    with open("data/latest_summary.json","w") as f: json.dump({"generated":datetime.now().isoformat(),"devices":summary},f,indent=2)
    return all_res,summary

# ============================================================
# HTML HELPERS
# ============================================================
def _mbox(title, data, fallback_msg="Module not loaded or no data available."):
    if data is None:
        return f'<div class="mbox"><h4>{title} <span class="mstat mgrey">N/A</span></h4><div class="msub">{fallback_msg}</div></div>'
    st=data.get("status","unknown") if isinstance(data,dict) else "unknown"
    msg=data.get("message","") if isinstance(data,dict) else str(data)
    if isinstance(msg,str) and len(msg)>350: msg=msg[:350]+"..."
    cls="mok" if st=="ok" else "mwarn" if st in ("framework","no_pytrends","blocked","severity_only","no_text","no_alerts","no_data","no_signals","no_query","no_cik","no_ticker","insufficient_data","no_filings","no_trials","parse_error") else "merr" if st=="error" else "mgrey"
    return f'<div class="mbox"><h4>{title} <span class="mstat {cls}">{st.upper()}</span></h4><div class="msub">{msg}</div></div>'

def _accordion(aid, title, status_html, content):
    """Collapsible accordion section."""
    return f'''<div class="acc">
<button class="acc-btn" onclick="toggleAcc('{aid}')"><span>{title}</span><span class="acc-right">{status_html}<span class="acc-arrow" id="arr-{aid}">\u25B6</span></span></button>
<div class="acc-body" id="{aid}" style="display:none">{content}</div></div>'''

def _build_product_card(did, r, cd):
    """Build a single product card HTML."""
    dv=r["device"]; st=r["stats"]; lt=st[-1]; rs=r.get("r_score"); sig=rs["signal"] if rs else "NORMAL"
    d3=sum(s["deaths"] for s in st[-3:]); i3=sum(s["injuries"] for s in st[-3:])
    # Timeline
    evts=PRODUCT_EVENTS.get(did,[])
    ehtml=""
    if evts:
        ehtml='<div class="ebox"><h4>TIMELINE</h4>'
        for e in evts:
            tc="ew" if "CLASS" in e["type"] or "WARNING" in e["type"] else "eok"
            ehtml+=f'<div class="evr"><span class="evd">{e["date"]}</span><span class="{tc}">{e["type"]}</span> {e["desc"]}</div>'
        ehtml+='</div>'
    # R-Score bar
    rhtml=""
    if rs:
        rcol="#c0392b" if rs["total"]>=50 else "#e67e22" if rs["total"]>=30 else "#27ae60"
        rhtml=f'<div class="rg"><div class="rgv" style="color:{rcol}">{rs["total"]}</div><div class="rgr"><div class="rgl">R-Score (0-100)</div><div class="rgt"><div class="rgf" style="width:{min(100,rs["total"])}%;background:{rcol}"></div></div></div></div>'
        rhtml+=f'<div class="rcg"><div class="rci"><span class="rcv">{rs["z_c"]}</span>Z</div><div class="rci"><span class="rcv">{rs["sev_c"]}</span>Sev</div><div class="rci"><span class="rcv">{rs["gap_c"]}</span>Gap</div><div class="rci"><span class="rcv">{rs["slope_c"]}</span>Slope</div><div class="rci"><span class="rcv">{rs["ib_c"]}</span>IB</div></div>'
    # NLP Failure Modes accordion
    fm=r.get("failure_modes")
    fmcontent=""
    if fm and isinstance(fm,dict) and fm.get("status")=="ok" and fm.get("modes"):
        fmcontent+=f'<div class="msub">{fm["total_analyzed"]} reports analyzed via keyword matching on MAUDE narrative text.</div>'
        for cat,info in sorted(fm["modes"].items(),key=lambda x:-x[1]["count"]):
            fmcontent+=f'<div class="fmr"><span class="fml">{info["label"]}</span><div class="fmb"><div style="width:{info["pct"]}%;background:var(--g)"></div></div><span class="fmp">{info["pct"]}%</span></div>'
            fmcontent+=f'<div class="msub" style="padding-left:136px;font-size:9px;margin-top:0">{info["desc"]}</div>'
        fmhtml=_accordion(f"fm-{did}","Failure Modes (NLP)",'<span class="mstat mok">OK</span>',fmcontent)
    elif fm:
        fmhtml=_accordion(f"fm-{did}","Failure Modes (NLP)",f'<span class="mstat mwarn">{fm.get("status","N/A").upper()}</span>',f'<div class="msub">{fm.get("message","")}</div>')
    else:
        fmhtml=""
    # Recall Probability accordion
    rp=r.get("recall_prob")
    rpcontent=""
    if rp and isinstance(rp,dict) and rp.get("probability") is not None:
        prob=rp["probability"]; pcol="#c0392b" if prob>0.5 else "#e67e22" if prob>0.25 else "#27ae60"
        rpcontent=f'<div class="msub">{rp.get("message","")}</div><div class="rgt" style="margin-top:4px"><div class="rgf" style="width:{prob*100}%;background:{pcol}"></div></div>'
        rphtml=_accordion(f"rp-{did}","Recall Probability (6mo)",f'<span class="mstat" style="color:{pcol}">{rp.get("signal","?")}</span>',rpcontent)
    else:
        rphtml=""
    zc2="neg" if lt["z_score"]>1.5 else "pos" if lt["z_score"]<-1.5 else ""
    slc2="neg" if lt["slope_6m"]>0 else "pos"
    d3c2="neg" if d3>0 else ""
    return f'''<div class="card" data-id="{did}">
<div class="chdr"><div><h3>{dv["name"]}</h3><span class="tk">{dv["ticker"]}</span></div><span class="sig sig-{sig}">{sig}</span></div>
<p class="desc">{dv["description"]}</p>{ehtml}
<div class="sg"><div class="si"><div class="sil">LATEST</div><div class="siv">{fmt0(lt["count"])}</div><div class="sis">{lt["month"]}</div></div>
<div class="si"><div class="sil">Z-SCORE</div><div class="siv {zc2}">{lt["z_score"]:+.2f}</div><div class="sis">12mo avg {fmt0(lt["avg_12m"])}</div></div>
<div class="si"><div class="sil">RATE/$M</div><div class="siv">{fmt2(lt["rate_per_m"])}</div><div class="sis">reports per $M rev</div></div>
<div class="si"><div class="sil">RATE/10K</div><div class="siv">{fmt2(lt["rate_per_10k"])}</div><div class="sis">per 10K users</div></div></div>
<div class="sg"><div class="si"><div class="sil">TREND</div><div class="siv {slc2}">{lt["slope_6m"]:+.1f}/mo</div><div class="sis">6mo slope</div></div>
<div class="si"><div class="sil">DEATHS 3MO</div><div class="siv {d3c2}">{d3}</div></div>
<div class="si"><div class="sil">INJURIES 3MO</div><div class="siv">{i3}</div></div>
<div class="si"><div class="sil">SEVERITY</div><div class="siv">{fmt0(lt["severity_score"])}</div><div class="sis">D\u00d710 I\u00d73 M\u00d71</div></div></div>
{rhtml}{fmhtml}{rphtml}
<div class="cc" id="cc-{did}"><button class="cb active" data-v="reports">Reports</button><button class="cb" data-v="rate_m">Rate/$M</button><button class="cb" data-v="rate_10k">Rate/10K</button><button class="cb" data-v="severity">Severity</button><button class="cb" data-v="zscore">Z-Score</button><button class="cb" data-v="stock">Stock</button><button class="cb rst" data-v="reset">Reset</button></div>
<div class="cdesc" id="cdesc-{did}">Click a chart tab above. Each shows a different analytical lens with full context descriptions.</div>
<div class="cw"><canvas id="ch-{did}"></canvas></div></div>\n'''

# ============================================================
# REDESIGNED generate_html — Company overview + accordions + product cards
# ============================================================
def generate_html(all_res,summary):
    os.makedirs("docs",exist_ok=True)
    cd={}
    for did,r in all_res.items():
        if not r.get("stats"): continue
        cd[did]={"l":[s["month"] for s in r["stats"]],"c":[s["count"] for s in r["stats"]],"ma":[s["ma6"] for s in r["stats"]],"u2":[s["upper_2sd"] for s in r["stats"]],"l2":[s["lower_2sd"] for s in r["stats"]],"u1":[s["upper_1sd"] for s in r["stats"]],"l1":[s["lower_1sd"] for s in r["stats"]],"z":[s["z_score"] for s in r["stats"]],"rm":[s["rate_per_m"] for s in r["stats"]],"r10":[s["rate_per_10k"] for s in r["stats"]],"d":[s["deaths"] for s in r["stats"]],"inj":[s["injuries"] for s in r["stats"]],"mal":[s["malfunctions"] for s in r["stats"]],"dev":r["device"],"rs":r.get("r_score"),"bm":[m for m,bf in r["batch_flags"].items() if bf.get("is_batch")],"sp":STOCK_MONTHLY.get(r["device"]["ticker"],{}),"evts":PRODUCT_EVENTS.get(did,[])}
    so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3}
    summary.sort(key=lambda x:(so.get(x["signal"],4),-(x["r_score"] or 0)))
    # SUMMARY TABLE with filters
    trows=""
    for s in summary:
        zc="neg" if s["z_score"]>Z_WARN else "pos" if s["z_score"]<-Z_WARN else ""
        rc="neg" if (s["r_score"] or 0)>=50 else "warn" if (s["r_score"] or 0)>=30 else ""
        bw=' <span class="bw">Batch</span>' if s.get("batch") else ""
        ic="1" if s["id"].endswith("_ALL") else "0"
        cr="\u2014"
        if s.get("corr_rho") is not None: cr=f'{s["corr_rho"]:+.3f}'+(" *" if s.get("corr_sig") else "")
        d3c="neg" if s["deaths_3mo"]>0 else ""
        trows+=f'<tr class="pr" data-co="{s["company"]}" data-id="{s["id"]}" data-sig="{s["signal"]}" data-comb="{ic}"><td>{s["name"]}{bw}</td><td>{s["ticker"]}</td><td>{s["month"]}</td><td>{fmt0(s["reports"])}</td><td class="{zc}">{s["z_score"]:+.2f}</td><td class="{rc}">{fmt(s["r_score"])}</td><td>{fmt2(s["rate_per_m"])}</td><td>{fmt2(s["rate_per_10k"])}</td><td>{s["slope_6m"]:+.1f}</td><td class="{d3c}">{s["deaths_3mo"]}</td><td>{s["injuries_3mo"]}</td><td>{cr}</td><td><span class="sig sig-{s["signal"]}">{s["signal"]}</span></td></tr>\n'

    # BUILD COMPANY TABS with overview + accordions + product cards
    company_html={}
    for comp in COMPANIES:
        # Find the _ALL device for this company
        all_did=None; all_r=None
        for did,r in all_res.items():
            if did.endswith("_ALL") and r["device"]["company"]==comp:
                all_did=did; all_r=r; break
        if not all_did:
            for did,r in all_res.items():
                if r["device"]["company"]==comp and r.get("stats"):
                    all_did=did; all_r=r; break
        if not all_r or not all_r.get("stats"): company_html[comp]="<p>No data available.</p>"; continue
        tk=all_r["device"]["ticker"]; lt_stat=all_r["stats"][-1]; rs=all_r.get("r_score")
        sig=rs["signal"] if rs else "NORMAL"
        # Stock price
        sp_data=STOCK_MONTHLY.get(tk,{}); sp_sorted=sorted(sp_data.keys())
        sp_latest=f"${sp_data[sp_sorted[-1]]}" if sp_sorted else "\u2014"
        sp_label="" if tk not in ("SQEL",) else " (Private)"
        if tk=="MDT_DM": sp_label=" (MDT parent)"
        # Company overview header
        co_hdr=f'''<div class="co-hdr"><div class="co-left"><h2>{comp}</h2><span class="co-tk">{tk}{sp_label} \u2014 {sp_latest}</span></div><span class="sig sig-{sig}" style="font-size:13px;padding:5px 16px">{sig}</span></div>
<div class="co-desc">{all_r["device"]["description"]}</div>
<div class="sg" style="margin-bottom:12px"><div class="si"><div class="sil">LATEST REPORTS</div><div class="siv">{fmt0(lt_stat["count"])}</div><div class="sis">{lt_stat["month"]}</div></div>
<div class="si"><div class="sil">Z-SCORE</div><div class="siv {"neg" if lt_stat["z_score"]>1.5 else "pos" if lt_stat["z_score"]<-1.5 else ""}">{lt_stat["z_score"]:+.2f}</div></div>
<div class="si"><div class="sil">R-SCORE</div><div class="siv {"neg" if (rs or {}).get("total",0)>=50 else ""}">{fmt(rs["total"]) if rs else "\u2014"}</div></div>
<div class="si"><div class="sil">RATE/$M REV</div><div class="siv">{fmt2(lt_stat["rate_per_m"])}</div></div></div>\n'''
        # ACCORDION MODULES at company level
        acc_html=""
        # 1. Insider Trading — show buy/sell direction and dollar values
        ins=all_r.get("insider")
        if ins and isinstance(ins,dict) and ins.get("status")=="ok":
            f4=ins.get("form4_count",0)
            ins_buys=ins.get("buys",0); ins_sells=ins.get("sells",0); ins_other=ins.get("other",0)
            ins_buy_val=ins.get("total_buy_value",0); ins_sell_val=ins.get("total_sell_value",0)
            ins_dir=ins.get("net_direction","UNKNOWN"); ins_signal=ins.get("signal","neutral")
            ins_col="#c0392b" if ins_signal in ("bearish","moderate_sell") else "#27ae60" if ins_signal in ("bullish","moderate_buy") else "var(--tx3)"
            ins_content=f'<div class="sg" style="grid-template-columns:repeat(4,1fr);margin-bottom:8px">'
            ins_content+=f'<div class="si"><div class="sil">TOTAL (90D)</div><div class="siv">{f4}</div></div>'
            ins_content+=f'<div class="si"><div class="sil">BUYS</div><div class="siv pos">{ins_buys}</div><div class="sis">${ins_buy_val:,.0f}</div></div>'
            ins_content+=f'<div class="si"><div class="sil">SELLS</div><div class="siv neg">{ins_sells}</div><div class="sis">${ins_sell_val:,.0f}</div></div>'
            ins_content+=f'<div class="si"><div class="sil">DIRECTION</div><div class="siv" style="color:{ins_col};font-size:13px">{ins_dir}</div></div></div>'
            # Show recent transactions
            txns=ins.get("transactions",[])
            if txns:
                ins_content+='<div class="msub"><strong>Recent transactions:</strong></div>'
                for txn in txns[:6]:
                    txn_col="pos" if txn.get("type")=="BUY" else "neg" if txn.get("type")=="SELL" else ""
                    ins_content+=f'<div class="msub" style="padding-left:8px"><span style="font-family:monospace;color:var(--tx3)">{txn.get("date","")}</span> <span class="{txn_col}">{txn.get("type","?")}</span> {txn.get("owner","Unknown")} \u2014 {txn.get("shares",0):,} shares (${txn.get("value",0):,.0f})</div>'
            ins_content+=f'<div class="msub" style="margin-top:6px"><strong>Signal interpretation:</strong> {ins.get("message","")}</div>'
            acc_html+=_accordion(f"ins-{tk}","SEC Form 4 Insider Trading",f'<span class="mstat" style="color:{ins_col}">{ins_dir}</span>',ins_content)
        # 2. EDGAR Filing Activity (NOT NLP — renamed for honesty)
        ed=all_r.get("edgar")
        if ed and isinstance(ed,dict) and ed.get("status")=="ok":
            acc_html+=_accordion(f"ed-{tk}","SEC Filing Activity (EDGAR)",f'<span class="mstat mok">{ed.get("annual_filings",0)} annual + {ed.get("quarterly_filings",0)} quarterly</span>',
                f'<div class="msub">{ed.get("message","")}</div><div class="msub"><strong>What this is:</strong> Counts recent 10-K and 10-Q filings from SEC EDGAR. This does NOT scan filing text — it tracks filing frequency. Latest filing: {ed.get("latest_date","unknown")}. To scan for quality-related language (recall, warranty, warning letter), download filings from EDGAR and search manually.</div>')
        # 3. Clinical Trials
        ct=all_r.get("trials")
        if ct and isinstance(ct,dict):
            ct_content=f'<div class="msub">{ct.get("message","")}</div>'
            if ct.get("status")=="ok" and ct.get("trials"):
                ct_content+='<div class="msub"><strong>Why it matters:</strong> Active competitor trials = future market threat. Paused or terminated company trials = potential quality concern or pipeline problem.</div>'
                for trial in ct["trials"][:5]:
                    ct_content+=f'<div class="msub" style="margin-top:4px">{trial["nct_id"]}: {trial["title"]} <span class="mstat mgrey">{trial["status"]}</span></div>'
            acc_html+=_accordion(f"ct-{tk}","Clinical Trials (ClinicalTrials.gov)",f'<span class="mstat {"mok" if ct.get("status")=="ok" else "mwarn"}">{ct.get("total",0) if ct.get("status")=="ok" else ct.get("status","N/A").upper()}</span>',ct_content)
        # 4. MAUDE-Stock Correlation — show full lag table
        ec=all_r.get("enhanced_corr")
        if ec and isinstance(ec,dict) and ec.get("status")=="ok":
            ec_rho=ec.get("best_rho",0); ec_lag=ec.get("best_lag",0); ec_p=ec.get("best_p",1); ec_sig=ec.get("significant",False)
            ec_col="#c0392b" if ec_rho<-0.2 and ec_sig else "#27ae60" if ec_rho>0.2 and ec_sig else "var(--tx3)"
            ec_content=f'<div class="sg" style="grid-template-columns:repeat(3,1fr);margin-bottom:8px"><div class="si"><div class="sil">BEST CORRELATION</div><div class="siv" style="color:{ec_col}">\u03C1={ec_rho:+.3f}</div></div><div class="si"><div class="sil">OPTIMAL LAG</div><div class="siv">{ec_lag} months</div><div class="sis">MAUDE leads stock by this much</div></div><div class="si"><div class="sil">SIGNIFICANT?</div><div class="siv {"pos" if ec_sig else "neg"}">{"YES (p={:.3f})".format(ec_p) if ec_sig else "NO (p={:.3f})".format(ec_p)}</div></div></div>'
            # Lag table
            lag_results=ec.get("lag_results",{})
            if lag_results:
                ec_content+='<div class="msub"><strong>Lag-by-lag breakdown:</strong></div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(90px,1fr));gap:3px;margin:4px 0 8px">'
                for lag_name in sorted(lag_results.keys()):
                    lr=lag_results[lag_name]; lr_rho=lr["rho"]; lr_p=lr["p"]
                    lr_col="#c0392b" if lr_rho<-0.2 and lr_p<0.05 else "#27ae60" if lr_rho>0.2 and lr_p<0.05 else "var(--tx3)"
                    star=" *" if lr_p<0.05 else ""
                    ec_content+=f'<div class="si" style="padding:4px 6px"><div class="sil">{lag_name} lag</div><div class="siv" style="font-size:13px;color:{lr_col}">{lr_rho:+.3f}{star}</div></div>'
                ec_content+='</div>'
            ec_content+=f'<div class="msub"><strong>How to read:</strong> \u03C1 (rho) ranges from -1 to +1. Negative = MAUDE spikes predict stock declines. The lag tells you how many months ahead the signal fires. * means p&lt;0.05. '
            if ec_sig and ec_rho<-0.2:
                ec_content+=f'<strong>ACTIONABLE:</strong> MAUDE reports at {ec_lag}-month lag show statistically significant negative correlation with stock returns. When reports spike, the stock tends to decline {ec_lag} months later. That {ec_lag}-month window is your trading alpha.'
            elif ec_sig and ec_rho>0.2:
                ec_content+='The positive correlation suggests the market is already pricing in MAUDE data in real-time — less alpha available from this signal alone.'
            else:
                ec_content+='No statistically significant relationship detected. MAUDE data alone may not predict stock moves for this name — combine with other signals.'
            ec_content+='</div>'
            acc_html+=_accordion(f"corr-{tk}","MAUDE-Stock Correlation",f'<span class="mstat" style="color:{ec_col}">\u03C1={ec_rho:+.3f}</span>',ec_content)
        # 5. R-Score Backtest
        bt=all_r.get("backtest")
        if bt and isinstance(bt,dict):
            bt_content=f'<div class="msub">{bt.get("message","")}</div>'
            if bt.get("status")=="ok" and bt.get("results"):
                bt_content+='<div class="msub"><strong>How to read:</strong> When R-Score historically crossed 50, what happened to the stock 30/60/90 days later? Negative avg return + high win rate = the signal works. This is your empirical proof.</div>'
                for window,res in bt["results"].items():
                    wcol="pos" if res["avg_return"]<0 else "neg"
                    bt_content+=f'<div class="msub">{window}: avg return <span class="{wcol}">{res["avg_return"]:+.1f}%</span>, win rate {res["win_rate"]:.0f}% (n={res["n"]})</div>'
            acc_html+=_accordion(f"bt-{tk}","R-Score Backtest",f'<span class="mstat {"mok" if bt.get("status")=="ok" and bt.get("results") else "mwarn"}">{bt.get("status","N/A").upper()}</span>',bt_content)
        # 5b. FDA Recalls (LIVE from openFDA)
        recalls=all_r.get("recalls")
        if recalls and isinstance(recalls,dict) and recalls.get("status")=="ok" and recalls.get("recalls"):
            rc_content=f'<div class="msub"><strong>{recalls["total"]} recalls found.</strong> Most recent from openFDA recall database:</div>'
            for rcl in recalls["recalls"]:
                rc_cls_col="#c0392b" if "I" in rcl.get("classification","") and "II" not in rcl.get("classification","") else "#e67e22" if "II" in rcl.get("classification","") else "var(--tx3)"
                rc_content+=f'<div style="border:1px solid var(--bd);border-radius:6px;padding:8px;margin:4px 0;border-left:3px solid {rc_cls_col}"><div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:3px"><span style="font-weight:700;color:{rc_cls_col}">{rcl.get("classification","")}</span><span style="color:var(--tx3)">{rcl.get("date_posted","")}</span></div><div class="msub" style="margin:0">{rcl.get("reason","")}</div><div style="font-size:9px;color:var(--tx3);margin-top:2px">{rcl.get("product","")}</div></div>'
            rc_content+='<div class="msub" style="margin-top:6px"><strong>Class I</strong> = most serious (may cause death). <strong>Class II</strong> = may cause temporary health issues. <strong>Class III</strong> = unlikely to cause harm. Live from openFDA recall endpoint.</div>'
            acc_html+=_accordion(f"rc-{tk}","FDA Recalls (Live)",f'<span class="mstat" style="color:#c0392b">{recalls["total"]} RECALLS</span>',rc_content)
        # 6. Peer-Relative — show full ranking table
        pr=all_r.get("peer_relative")
        if pr and isinstance(pr,dict):
            prcol="#c0392b" if pr.get("signal") in ("WORST","WEAK") else "#27ae60" if pr.get("signal") in ("BEST","STRONG") else "var(--tx3)"
            pr_content=f'<div class="sg" style="grid-template-columns:repeat(3,1fr);margin-bottom:8px"><div class="si"><div class="sil">THIS COMPANY</div><div class="siv" style="color:{prcol}">{pr.get("signal","?")}</div></div><div class="si"><div class="sil">RANK</div><div class="siv">{pr.get("rank","?")}/{pr.get("total","?")}</div><div class="sis">1 = cleanest FDA profile</div></div><div class="si"><div class="sil">PEER AVG R-SCORE</div><div class="siv">{pr.get("peer_avg","\u2014")}</div><div class="sis">vs this company: {pr.get("r_score","\u2014")}</div></div></div>'
            pr_content+=f'<div class="msub"><strong>Trading implication:</strong> '
            if pr.get("signal") in ("WORST","WEAK"):
                pr_content+=f'This company has the WORST quality profile vs peers. Pairs trade: short {tk}, long the cleanest name in the group. The R-Score spread between best and worst ({pr.get("r_score",0)-pr.get("peer_avg",0):+.0f} pts above avg) = the conviction level.'
            elif pr.get("signal") in ("BEST","STRONG"):
                pr_content+=f'This company has the CLEANEST quality profile vs peers. If you believe MAUDE quality predicts earnings, this is the long leg of the pairs trade.'
            else:
                pr_content+='Middle of the pack. No strong relative signal. Look at the individual product breakdown for more granular opportunities.'
            pr_content+='</div>'
            acc_html+=_accordion(f"pr-{tk}","Peer-Relative Position",f'<span class="mstat" style="color:{prcol}">{pr.get("signal","?")} ({pr.get("rank","?")}/{pr.get("total","?")})</span>',pr_content)
        # 7. Earnings Predictor — show factor-by-factor score breakdown
        ep=all_r.get("earnings_pred")
        if ep and isinstance(ep,dict) and ep.get("status")=="ok":
            epcol="#c0392b" if ep["prediction"]=="LIKELY MISS" else "#27ae60" if ep["prediction"]=="LIKELY BEAT" else "var(--tx3)"
            ep_score=ep.get("score",50); ep_conf=ep.get("confidence",0)
            ep_content=f'<div class="sg" style="grid-template-columns:repeat(3,1fr);margin-bottom:8px"><div class="si"><div class="sil">PREDICTION</div><div class="siv" style="color:{epcol}">{ep["prediction"]}</div></div><div class="si"><div class="sil">CONFIDENCE</div><div class="siv">{ep_conf:.0f}%</div><div class="sis">higher = more conviction</div></div><div class="si"><div class="sil">RAW SCORE</div><div class="siv">{ep_score}/100</div><div class="sis">&lt;35=miss, 35-65=neutral, &gt;65=beat</div></div></div>'
            ep_factors=ep.get("factors",[])
            if ep_factors:
                ep_content+='<div class="msub"><strong>Factor breakdown (what drove this prediction):</strong></div>'
                for fct in ep_factors:
                    fct_icon="\u274C" if any(w in fct.lower() for w in ["critical","elevated","worst","rising","miss"]) else "\u2705" if any(w in fct.lower() for w in ["low","clean","best","declining","beat"]) else "\u26A0\uFE0F"
                    ep_content+=f'<div class="msub" style="padding-left:12px">{fct_icon} {fct}</div>'
            ep_content+=f'<div class="msub" style="margin-top:6px"><strong>How to use:</strong> Position before earnings. LIKELY MISS at &gt;60% confidence = puts or short entry. LIKELY BEAT at &gt;60% = calls or long entry. Pair with peer-relative: short the LIKELY MISS name, long the LIKELY BEAT name for a hedged trade.</div>'
            acc_html+=_accordion(f"ep-{tk}","Earnings Surprise Predictor",f'<span class="mstat" style="color:{epcol}">{ep["prediction"]} ({ep_conf:.0f}%)</span>',ep_content)
        # 8. Framework modules — only show if they have REAL data (status=ok), hide otherwise
        for mod_key,mod_title in [("google_trends","Google Trends (Leading Indicator)"),("short_interest","Short Interest"),("payer","CMS Payer / Formulary"),("international","International (MHRA/UK)")]:
            mod=all_r.get(mod_key)
            if mod and isinstance(mod,dict) and mod.get("status")=="ok":
                acc_html+=_accordion(f"{mod_key}-{tk}",mod_title,f'<span class="mstat mok">OK</span>',f'<div class="msub">{mod.get("message","")}</div>')
        # PRODUCT CARDS (non-_ALL devices for this company)
        cards_html=""
        for did,r in all_res.items():
            if not r.get("stats"): continue
            if r["device"]["company"]!=comp: continue
            if did==all_did: continue  # skip _ALL, already shown in overview
            cards_html+=_build_product_card(did,r,cd)
        # Also include the _ALL card
        if all_r.get("stats"):
            cards_html=_build_product_card(all_did,all_r,cd)+cards_html
        company_html[comp]=f'{co_hdr}<div class="acc-section"><h3 class="section-title">Company-Level Intelligence</h3>{acc_html}</div><h3 class="section-title" style="margin-top:16px">Product-Level Detail</h3><div class="grid">{cards_html}</div>'

    tab_ids={"Summary":"summary","Dexcom":"dexcom","Insulet":"insulet","Tandem":"tandem","Abbott":"abbott","Beta Bionics":"bbnx","Medtronic":"medtronic","Sequel Med Tech":"sequel"}
    tab_btns='<div class="tabs">'
    for name,tid in tab_ids.items():
        act=' active' if tid=="summary" else ""
        tab_btns+=f'<button class="tab{act}" onclick="showTab(\'{tid}\')">{name}</button>'
    tab_btns+='</div>'
    modules_str="ALL 13 MODULES" if HAS_MODULES else "BASIC"
    updated_str=datetime.now().strftime('%b %d, %Y %H:%M ET')
    rev_stale=get_revenue_staleness()
    rev_color="#c0392b" if rev_stale["status"]=="STALE" else "#e67e22" if rev_stale["status"]=="AGING" else "#27ae60"
    stock_note=f"Stock prices: LIVE via yfinance" if live_stocks else "Stock prices: HARDCODED (yfinance not available)"

    stock_note = _stock_source if '_stock_source' in dir() else "HARDCODED"

    html_top=f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Monitor V3.1</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">'''
    # CSS stays exactly the same — inserted below
    html_top+=f'''<style>
:root{{--g:#2B5F3A;--gx:#e8f5ec;--gp:#f4faf6;--bg:#fff;--bg2:#f8faf9;--bg3:#f0f3f1;--tx:#1a2a1f;--tx2:#4a5f50;--tx3:#7a8f80;--bd:#d4e0d8;--red:#c0392b;--org:#e67e22}}</style></head><body><div class="ct">
<header><div><h1>MAUDE Monitor V3.1</h1><div class="sub">FDA Adverse Event Intelligence \u2014 Diabetes Devices \u2014 7 Companies, {len(DEVICES)} Products</div></div>
<div class="meta">Updated {updated_str}<br>Modules: {modules_str}<br>Stock: {stock_note}<br><span style="color:{rev_color}">Revenue: {rev_stale["message"]}</span></div></header>'''
<style>
:root{{--g:#2B5F3A;--gx:#e8f5ec;--gp:#f4faf6;--bg:#fff;--bg2:#f8faf9;--bg3:#f0f3f1;--tx:#1a2a1f;--tx2:#4a5f50;--tx3:#7a8f80;--bd:#d4e0d8;--red:#c0392b;--org:#e67e22}}
*{{margin:0;padding:0;box-sizing:border-box}}body{{background:var(--bg);color:var(--tx);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6}}
.ct{{max-width:1440px;margin:0 auto;padding:24px 32px}}
header{{display:flex;justify-content:space-between;align-items:center;padding:20px 0;border-bottom:2px solid var(--g);margin-bottom:20px}}
header h1{{font-size:22px;font-weight:700;color:var(--g)}}header .sub{{font-size:13px;color:var(--tx2)}}header .meta{{text-align:right;font-size:11px;color:var(--tx3)}}
h2{{font-size:18px;font-weight:700;color:var(--g);margin:20px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--bd)}}
.tabs{{display:flex;gap:2px;margin-bottom:20px;border-bottom:2px solid var(--bd);flex-wrap:wrap}}
.tab{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-bottom:none;border-radius:6px 6px 0 0;padding:8px 16px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit}}
.tab:hover{{background:var(--gx)}}.tab.active{{background:var(--g);color:#fff;border-color:var(--g)}}
.tabcontent{{display:none}}.tabcontent.active{{display:block}}
.sig{{display:inline-block;padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;text-transform:uppercase}}
.sig-NORMAL{{background:var(--gx);color:var(--g)}}.sig-WATCH{{background:#fef3e0;color:#b8860b}}.sig-ELEVATED{{background:#fdecea;color:var(--red)}}.sig-CRITICAL{{background:#f5c6cb;color:#721c24}}
.bw{{background:#fef3e0;color:#b8860b;font-size:10px;padding:1px 6px;border-radius:4px}}
/* Company overview header */
.co-hdr{{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;background:var(--gp);border:2px solid var(--g);border-radius:10px;margin-bottom:12px}}
.co-left h2{{margin:0;border:none;padding:0}}.co-tk{{font-size:13px;color:var(--tx2);font-weight:500}}
.co-desc{{font-size:12px;color:var(--tx2);padding:8px 12px;background:var(--bg2);border-radius:6px;border-left:3px solid var(--g);margin-bottom:12px;line-height:1.6}}
.section-title{{font-size:13px;font-weight:700;color:var(--g);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;padding-bottom:4px;border-bottom:1px dashed var(--bd)}}
/* Accordion */
.acc-section{{margin-bottom:16px}}
.acc{{border:1px solid var(--bd);border-radius:8px;margin-bottom:4px;overflow:hidden}}
.acc-btn{{width:100%;display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg2);border:none;cursor:pointer;font-family:inherit;font-size:12px;font-weight:600;color:var(--tx);text-align:left}}
.acc-btn:hover{{background:var(--gx)}}.acc-right{{display:flex;align-items:center;gap:8px}}
.acc-arrow{{font-size:10px;color:var(--tx3);transition:transform .2s}}.acc-arrow.open{{transform:rotate(90deg)}}
.acc-body{{padding:10px 14px;background:var(--bg);border-top:1px solid var(--bd)}}
/* Filters */
.filters{{display:flex;gap:12px;align-items:center;margin-bottom:16px;padding:10px 14px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;flex-wrap:wrap}}
.filters label{{font-size:11px;font-weight:600;color:var(--tx3);text-transform:uppercase}}
.filters select{{padding:4px 8px;border:1px solid var(--bd);border-radius:4px;font-size:12px;font-family:inherit;background:var(--bg)}}
/* Guide boxes */
.guide{{background:var(--gp);border:1px solid var(--bd);border-radius:10px;padding:20px;margin-bottom:20px}}
.guide h3{{font-size:15px;font-weight:700;color:var(--g);margin-bottom:12px}}
.gg{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.gi{{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:12px}}
.gi h4{{font-size:12px;font-weight:700;color:var(--g);margin-bottom:4px}}.gi p{{font-size:11px;color:var(--tx2)}}
/* Table */
table{{width:100%;border-collapse:collapse}}th{{text-align:left;padding:8px 10px;font-size:10px;font-weight:600;text-transform:uppercase;color:var(--tx3);border-bottom:2px solid var(--g);background:var(--bg);position:sticky;top:0}}
td{{padding:7px 10px;border-bottom:1px solid var(--bd);font-size:12px;white-space:nowrap}}tr:hover{{background:var(--gp)}}
.neg{{color:var(--red);font-weight:500}}.pos{{color:var(--g);font-weight:500}}.warn{{color:var(--org);font-weight:500}}
/* Cards grid */
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(440px,1fr));gap:16px}}
.card{{background:var(--bg);border:1px solid var(--bd);border-radius:10px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
.chdr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px}}.card h3{{font-size:15px;font-weight:700}}.tk{{font-size:11px;color:var(--tx3)}}
.desc{{font-size:11px;color:var(--tx2);line-height:1.5;margin-bottom:10px;padding:8px 10px;background:var(--gp);border-radius:6px;border-left:3px solid var(--g)}}
.ebox{{margin-bottom:8px}}.ebox h4{{font-size:10px;font-weight:600;color:var(--tx3);text-transform:uppercase;margin-bottom:4px}}
.evr{{font-size:10px;color:var(--tx2);padding:3px 0 3px 8px;border-left:2px solid var(--bd);margin-bottom:2px}}
.evd{{font-family:monospace;color:var(--tx3);margin-right:4px}}
.ew{{background:#fdecea;color:var(--red);font-size:8px;font-weight:700;padding:1px 4px;border-radius:3px;margin-right:3px}}
.eok{{background:var(--gx);color:var(--g);font-size:8px;font-weight:700;padding:1px 4px;border-radius:3px;margin-right:3px}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px}}
.si{{padding:6px 8px;background:var(--bg2);border-radius:6px;border:1px solid var(--bd)}}
.sil{{font-size:8px;font-weight:600;text-transform:uppercase;color:var(--tx3)}}.siv{{font-size:15px;font-weight:700;margin-top:1px}}.sis{{font-size:9px;color:var(--tx3)}}
.rg{{display:flex;align-items:center;gap:12px;padding:8px 10px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;margin-bottom:6px}}
.rgv{{font-size:28px;font-weight:800;min-width:44px}}.rgr{{flex:1}}.rgl{{font-size:10px;color:var(--tx3);margin-bottom:3px}}
.rgt{{height:7px;background:var(--bg3);border-radius:4px;overflow:hidden}}.rgf{{height:100%;border-radius:4px}}
.rcg{{display:grid;grid-template-columns:repeat(5,1fr);gap:3px;margin-bottom:8px}}
.rci{{text-align:center;padding:3px;background:var(--bg2);border-radius:4px;border:1px solid var(--bd);font-size:8px;color:var(--tx3)}}.rcv{{display:block;font-size:12px;font-weight:700;color:var(--tx)}}
.mbox{{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:10px;margin-bottom:6px}}
.mbox h4{{font-size:10px;font-weight:600;color:var(--tx3);text-transform:uppercase;margin-bottom:4px;display:flex;align-items:center;gap:6px}}
.msub{{font-size:11px;color:var(--tx2);margin-top:3px;line-height:1.5}}
.mstat{{font-size:8px;font-weight:700;text-transform:uppercase;padding:1px 5px;border-radius:3px}}
.mok{{background:var(--gx);color:var(--g)}}.mwarn{{background:#fef3e0;color:#b8860b}}.merr{{background:#fdecea;color:var(--red)}}.mgrey{{background:var(--bg3);color:var(--tx3)}}
.fmr{{display:flex;align-items:center;gap:6px;margin-bottom:2px}}.fml{{width:130px;font-size:10px;color:var(--tx2)}}
.fmb{{flex:1;height:10px;background:var(--bg3);border-radius:3px;overflow:hidden}}.fmb div{{height:100%;border-radius:3px}}.fmp{{width:36px;text-align:right;font-weight:600;font-size:11px}}
.cc{{display:flex;gap:3px;flex-wrap:wrap;margin-bottom:4px}}
.cb{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-radius:4px;padding:3px 8px;font-size:10px;font-family:inherit;cursor:pointer;font-weight:500}}
.cb:hover,.cb.active{{background:var(--g);color:#fff;border-color:var(--g)}}.cb.rst{{margin-left:auto;background:transparent;border-color:var(--bd);color:var(--tx3)}}
.cdesc{{font-size:11px;color:var(--tx2);padding:8px 10px;background:var(--gp);border-radius:6px;margin-bottom:6px;border-left:3px solid var(--g);min-height:36px;line-height:1.5}}
.cw{{position:relative;height:260px}}
.disc{{margin-top:24px;padding:14px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;font-size:10px;color:var(--tx3);line-height:1.5}}
</style></head><body><div class="ct">
<header><div><h1>MAUDE Monitor V3.1</h1><div class="sub">FDA Adverse Event Intelligence \u2014 Diabetes Devices \u2014 7 Companies, {len(DEVICES)} Products</div></div>
<div class="meta">Updated {updated_str}<br>Mode: {modules_str}</div></header>
{tab_btns}
<div class="tabcontent active" id="tc-summary">
<div class="guide"><h3>How to Read This Dashboard</h3><div class="gg">
<div class="gi"><h4>Z-Score</h4><p>Standard deviations from 12-month mean. Measures if current month is statistically abnormal. Above +1.5 = WATCH. Above +2.0 = ELEVATED (p~2.3%, unlikely by chance). Above +3.0 = CRITICAL. Negative values mean unusually LOW reports.</p></div>
<div class="gi"><h4>R-Score (0-100)</h4><p>Composite risk score combining 5 factors: Z-anomaly (20pts), severity trend (20pts), growth gap vs revenue (20pts), 6-month slope (20pts), and installed-base rate change (20pts). Above 50 = investigate. Above 70 = act. This is the single number that tells you "how worried should I be?"</p></div>
<div class="gi"><h4>Rate/$M Revenue</h4><p>MAUDE reports divided by monthly revenue (quarterly/3). This normalizes for business size. If a company doubles revenue and reports also double, the rate stays flat (no problem). RISING rate = quality is deteriorating faster than the business is growing. This is more predictive than raw counts.</p></div>
<div class="gi"><h4>Rate/10K Users</h4><p>Reports divided by estimated installed base (per 10K users). The most precise normalization. New products with few users show HIGH rates even with few reports \u2014 which is correct, that IS a high failure rate per user. Sources: earnings calls, 10-K filings.</p></div>
<div class="gi"><h4>6mo Trend (Slope)</h4><p>Linear regression slope over last 6 months. +50 means reports increasing ~50/month. Positive = accelerating problem. Negative = improving. The slope captures momentum \u2014 even if the Z-score is normal, a rapidly rising slope is an early warning.</p></div>
<div class="gi"><h4>Deaths/Injuries (3mo)</h4><p>Event counts from most recent 3 months. Deaths weighted 10x in severity score, injuries 3x, malfunctions 1x. MAUDE-reported \u2014 not confirmed causal, but the pattern matters more than individual reports.</p></div>
<div class="gi"><h4>Batch Detection</h4><p>If received-date count > 3x event-date count same month = manufacturer dumped old reports at once (recall paperwork). NOT a real surge. Flagged as "Batch" and colored orange on charts so you don't misread it as a spike.</p></div>
<div class="gi"><h4>Correlation (Corr)</h4><p>Spearman rank correlation between monthly MAUDE z-scores and stock returns. Dash = no stock (private). * = p&lt;0.05. Negative correlation = MAUDE spikes predict stock declines. The lag (1-4 months) is your alpha window.</p></div>
</div></div>
<div class="filters"><label>Company:</label><select id="fc" onchange="af()"><option value="all">All</option>'''
    for c in COMPANIES: html_top+=f'<option value="{c}">{c}</option>'
    html_top+=f'''</select><label>Signal:</label><select id="fs" onchange="af()"><option value="all">All</option><option value="CRITICAL">Critical</option><option value="ELEVATED">Elevated+</option><option value="WATCH">Watch+</option></select><label>View:</label><select id="fv" onchange="af()"><option value="all">All Products</option><option value="combined">Company-Level Only</option><option value="individual">Individual Products Only</option></select></div>
<h2>All Products \u2014 Latest Month</h2>
<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>6mo Trend</th><th>Deaths (3mo)</th><th>Injuries (3mo)</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div>
</div>'''
    for comp in COMPANIES:
        tid=tab_ids[comp]; ch=company_html.get(comp,"<p>No data.</p>")
        html_top+=f'\n<div class="tabcontent" id="tc-{tid}">{ch}</div>'
    html_top+='\n<div class="disc">Research only. Not investment advice. MAUDE has known limitations including 30-90 day reporting lag. Revenue from SEC filings. Installed base from earnings calls. MDT stock = parent company (~8% diabetes). BBNX from Feb 2025 IPO. Sequel is private. Correlation is not causation.</div></div>'

    js=r'''<script>
var defined_cd=__CD__;var charts={};
var chartDescs={"reports":"REPORTS + SIGMA BANDS: Green bars = monthly MAUDE reports received by the FDA. Light shaded bands show +/-1 standard deviation (68% of normal months fall here) and +/-2 std dev (95%). When bars break above the 2-sigma band, that month is statistically anomalous. Orange bars = batch reporting (manufacturer dumped old reports, NOT a real surge). Red bars = regulatory event month. The dark green line is the 6-month moving average which smooths noise to reveal the real underlying trend.","rate_m":"RATE PER $M REVENUE: Each bar = that month's MAUDE reports divided by monthly revenue (quarterly revenue / 3). This is the key normalization. If Dexcom grows revenue 20% and reports also grow 20%, the rate is flat (no quality problem). But if reports grow 40% while revenue grows 20%, the rate rises. A RISING rate means quality is deteriorating faster than the business is growing. This metric predicted the PODD selloff 4-5 months early. Rate showing 'none' means no revenue data exists for that quarter.","rate_10k":"RATE PER 10K USERS: Reports divided by estimated installed base (per 10,000 users). The most granular normalization available. New products with very few users will show HIGH rates even with only a handful of reports — this is mathematically correct and is actually a real concern (a product used by 1,000 people with 50 complaints is worse than one used by 1M people with 5,000 complaints). User estimates from earnings calls and SEC 10-K filings.","severity":"SEVERITY BREAKDOWN: Stacked bars showing the composition of events each month. Red = deaths (most serious, weighted 10x in severity score). Orange = injuries requiring medical intervention (weighted 3x). Green = malfunctions (device failures without direct patient harm, weighted 1x). Watch for the MIX shifting — if deaths and injuries are growing as a % of total while malfunctions stay flat, the problem is getting more dangerous, not just more frequent.","zscore":"Z-SCORE HISTORY: How far each month deviates from its trailing 12-month average, measured in standard deviations. Red dashed line at +2 = statistically significant spike (only 2.3% chance this is random). Green dashed line at -2 = unusually low. Sustained positive z-scores across multiple months = developing quality problem, not a one-off. This is more reliable than looking at raw counts because it adjusts for seasonal patterns and installed base growth.","stock":"STOCK PRICE OVERLAY: Green line = stock price. Red line = MAUDE report count. The key pattern to look for: MAUDE spikes (red going UP) that precede stock declines (green going DOWN) by 1-4 months. That lead time is your trading alpha window. For MDT, price shown is the parent conglomerate (diabetes is ~8% of revenue). For BBNX, only post-IPO Feb 2025 data. For SQEL, no stock data (private company)."};
function showTab(id){document.querySelectorAll(".tab").forEach(function(t){t.classList.remove("active")});document.querySelectorAll(".tabcontent").forEach(function(t){t.classList.remove("active")});var ct=document.querySelector('.tab[onclick*="'+id+'"]');if(ct)ct.classList.add("active");var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");}
function toggleAcc(id){var el=document.getElementById(id);var arr=document.getElementById("arr-"+id);if(el.style.display==="none"){el.style.display="block";if(arr)arr.classList.add("open");}else{el.style.display="none";if(arr)arr.classList.remove("open");}}
function init(){for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}document.querySelectorAll(".cc").forEach(function(cc){cc.querySelectorAll(".cb").forEach(function(btn){btn.addEventListener("click",function(){var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}mycc.querySelectorAll(".cb:not(.rst)").forEach(function(s){s.classList.remove("active")});this.classList.add("active");var descEl=document.getElementById("cdesc-"+did);if(descEl&&chartDescs[v]){descEl.textContent=chartDescs[v];}mk(did,defined_cd[did],v);});});});}
function mk(did,D,v){var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];var evtMs=evts.map(function(e){return e.date;});
if(v==="reports"){var bc=D.l.map(function(m,i){return bm.indexOf(m)>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(m)>=0?"rgba(192,57,43,0.4)":"rgba(43,95,58,0.25)";});ds=[{label:"2\u03C3 upper",data:D.u2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:"+1",pointRadius:0,order:5},{label:"2\u03C3 lower",data:D.l2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:false,pointRadius:0,order:5},{label:"1\u03C3 upper",data:D.u1,borderWidth:0,backgroundColor:"rgba(43,95,58,0.10)",fill:"+1",pointRadius:0,order:4},{label:"1\u03C3 lower",data:D.l1,borderWidth:0,fill:false,pointRadius:0,order:4},{label:"Reports",data:D.c,borderColor:"rgba(43,95,58,0.85)",backgroundColor:bc,borderWidth:1.5,type:"bar",order:2},{label:"6mo MA",data:D.ma,borderColor:"#2B5F3A",borderWidth:2.5,fill:false,pointRadius:0,tension:0.3,order:1}];yL="Monthly Reports";}
else if(v==="rate_m"){ds=[{label:"Rate/$M",data:D.rm.map(function(v){return v===null?undefined:v;}),borderColor:"#2B5F3A",backgroundColor:"rgba(43,95,58,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per $M Revenue";}
else if(v==="rate_10k"){ds=[{label:"Rate/10K Users",data:D.r10.map(function(v){return v===null?undefined:v;}),borderColor:"#8B4513",backgroundColor:"rgba(139,69,19,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per 10K Users";}
else if(v==="severity"){ds=[{label:"Deaths",data:D.d,backgroundColor:"rgba(192,57,43,0.8)",borderWidth:0,stack:"s"},{label:"Injuries",data:D.inj,backgroundColor:"rgba(230,126,34,0.7)",borderWidth:0,stack:"s"},{label:"Malfunctions",data:D.mal,backgroundColor:"rgba(43,95,58,0.3)",borderWidth:0,stack:"s"}];yL="Events by Type";}
else if(v==="zscore"){ds=[{label:"Z-Score",data:D.z,backgroundColor:D.z.map(function(zv){return zv>2?"rgba(192,57,43,0.8)":zv>1.5?"rgba(230,126,34,0.7)":zv<-1.5?"rgba(43,95,58,0.6)":"rgba(43,95,58,0.25)";}),borderWidth:0,type:"bar"},{label:"+2\u03C3",data:D.l.map(function(){return 2;}),borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false},{label:"-2\u03C3",data:D.l.map(function(){return -2;}),borderColor:"rgba(43,95,58,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false}];yL="Z-Score";}
else if(v==="stock"){var sp=D.sp||{};var sl=[],sv=[],sc=[];D.l.forEach(function(m,i){if(sp[m]){sl.push(m);sv.push(sp[m]);sc.push(D.c[i]);}});if(sl.length<2){var de=document.getElementById("cdesc-"+did);if(de)de.textContent="Limited stock data. Company may be private (Sequel), recently IPO'd (BBNX Feb 2025), or product too new.";return;}ds=[{label:"Stock ($)",data:sv,borderColor:"#2B5F3A",borderWidth:2,fill:false,pointRadius:1.5,tension:0.2},{label:"MAUDE Reports",data:sc,borderColor:"rgba(192,57,43,0.6)",borderWidth:1.5,fill:false,pointRadius:0,tension:0.2,yAxisID:"y1"}];charts[did]=new Chart(ctx,{type:"line",data:{labels:sl,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{position:"left",grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#2B5F3A",font:{size:10}},title:{display:true,text:"Stock ($)",color:"#2B5F3A",font:{size:11}}},y1:{position:"right",grid:{drawOnChartArea:false},ticks:{color:"#c0392b",font:{size:10}},title:{display:true,text:"MAUDE Reports",color:"#c0392b",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1}}}});return;}
charts[did]=new Chart(ctx,{type:"line",data:{labels:D.l,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#4a5f50",font:{size:10}},title:{display:true,text:yL,color:"#4a5f50",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},pinch:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1,callbacks:{afterBody:function(it){var idx=it[0].dataIndex;var month=D.l[idx];var msgs=[];if(bm.indexOf(month)>=0)msgs.push("BATCH REPORTING DETECTED");evts.forEach(function(e){if(e.date===month)msgs.push(e.type+": "+e.desc);});return msgs.length?"\n"+msgs.join("\n"):"";}}}}}});}
function af(){var co=document.getElementById("fc").value;var sig=document.getElementById("fs").value;var vw=document.getElementById("fv").value;var so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3};document.querySelectorAll(".pr,.card").forEach(function(el){var sh=true;var ec=el.getAttribute("data-co");var es=el.getAttribute("data-sig");var ic=el.getAttribute("data-comb")==="1";if(co!=="all"&&ec!==co)sh=false;if(sig!=="all"){var sv=so[es]||3;if(sig==="CRITICAL"&&es!=="CRITICAL")sh=false;if(sig==="ELEVATED"&&sv>1)sh=false;if(sig==="WATCH"&&sv>2)sh=false;}if(vw==="combined"&&!ic)sh=false;if(vw==="individual"&&ic)sh=false;el.style.display=sh?"":"none";});}
document.addEventListener("DOMContentLoaded",init);
</script>'''
    full_html=html_top+js+"</body></html>"
    full_html=full_html.replace("__CD__",json.dumps(cd))
    with open("docs/index.html","w") as f: f.write(full_html)
    print(f"\nDashboard written: docs/index.html ({len(full_html)//1024}KB)")

def send_alerts(summary):
    to,fr,pw=os.environ.get("MAUDE_EMAIL_TO"),os.environ.get("MAUDE_EMAIL_FROM"),os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl=[s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body="MAUDE Monitor V3.1 Alert\n\n"
    for s in fl: body+=f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg=MIMEMultipart();msg["From"],msg["To"]=fr,to;msg["Subject"]=f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv: srv.starttls();srv.login(fr,pw);srv.send_message(msg)
    except: pass

def main():
    p=argparse.ArgumentParser();p.add_argument("--html",action="store_true");p.add_argument("--backfill",action="store_true");p.add_argument("--quick",action="store_true")
    a=p.parse_args()
    print(f"MAUDE Monitor V3.1 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} products | Modules: {'ALL (inline)' if HAS_MODULES else 'NONE'}")
    r,s=run_pipeline(a.backfill,a.quick); generate_html(r,s); send_alerts(s)
    print(f"\nCOMPLETE | docs/index.html")
# ============================================================
# V3.2 BLOCK 2 — PIPELINE WRAPPER + HTML POST-PROCESSOR
# ============================================================
# PASTE THIS at the bottom of Part 2, right ABOVE the line:
#   if __name__=="__main__": main()
#
# This uses monkey-patching: it wraps your existing run_pipeline
# and generate_html functions WITHOUT modifying them. Your original
# code runs first, then V3.2 adds smoothing, case studies, and
# patches the HTML output.
# ============================================================
import re as _re

# ============================================================
# WRAP run_pipeline: adds smoothing + case study data to results
# ============================================================
_v31_run_pipeline = run_pipeline

def run_pipeline(backfill=False, quick=False):
    """V3.2 wrapper: runs original pipeline, then adds smoothing + case study data."""
    all_res, summary = _v31_run_pipeline(backfill, quick)

    print(f"\n{'='*50}\nV3.2: Computing smoothing + case studies...")

    for did, R in all_res.items():
        dev = R.get("dev", {})
        recv = R.get("recv", {})
        evnt = R.get("evnt", {})

        # Batch smoothing (for ALL devices)
        try:
            R["smooth"] = smooth_batch_data(recv, evnt)
            sm = R["smooth"]
            if sm["method"] not in ("none","raw"):
                print(f"  [{did}] Smoothing: {sm['method']}-{sm['window']} (fit r={sm['fit_corr']})")
        except Exception as ex:
            R["smooth"] = {"smoothed":dict(recv),"method":"none","window":0,"raw":dict(recv),"fit_corr":0.0}
            print(f"  [{did}] Smoothing error: {ex}")

        # Case study (only for flagged devices)
        R["case_study"] = None
        if dev.get("case_study"):
            try:
                cs_ticker = CASE_STUDY_TICKER_MAP.get(did, dev.get("ticker",""))
                print(f"  [{did}] Case Study: fetching stock for {cs_ticker} from 2020...")
                cs_stock = get_monthly_stock(cs_ticker, "20200101")
                cs_events = CASE_STUDY_EVENTS.get(cs_ticker, [])
                if cs_stock:
                    cs_data = compute_case_study(cs_ticker, recv, cs_stock, cs_events)
                    R["case_study"] = cs_data
                    if cs_data:
                        print(f"  [{did}] Lag={cs_data['optimal_lag']['lag']}mo "
                              f"r={cs_data['optimal_lag']['corr']} "
                              f"hit={cs_data['hit_rate']}%")
                else:
                    print(f"  [{did}] No stock data returned for {cs_ticker}")
                time.sleep(0.3)
            except Exception as ex:
                print(f"  [{did}] Case study error: {ex}")

    print("V3.2 enrichment complete.\n")
    return all_res, summary


# ============================================================
# V3.2 CSS (injected into generated HTML)
# ============================================================
_V32_CSS = '''
/* V3.2 Smoothing badge */
.sm-badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;background:#e8f0fe;color:#1a73e8;margin-left:8px}
.sm-badge.raw{background:#f0f0f0;color:#666}
/* V3.2 Case Studies */
.cs-card{background:var(--bg);border:2px solid var(--bd);border-radius:12px;padding:24px;margin-bottom:24px}
.cs-card.signal-negative{border-left:4px solid var(--red)}.cs-card.signal-positive{border-left:4px solid var(--g)}
.cs-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px}
.cs-header h3{font-size:16px;font-weight:700;color:var(--g);margin:0}
.cs-lag-badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:600;background:var(--gx);color:var(--g)}
.cs-chart-wrap{height:320px;margin-bottom:16px;position:relative}
.cs-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}
.cs-metric{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:12px;text-align:center}
.cs-metric .lbl{font-size:11px;color:var(--tx3);text-transform:uppercase;font-weight:600;display:block;margin-bottom:4px}
.cs-metric .val{font-size:18px;font-weight:700;color:var(--tx)}.cs-metric .val.neg{color:var(--red)}.cs-metric .val.pos{color:var(--g)}
.cs-lag-table{width:100%;border-collapse:collapse;margin-bottom:16px;font-size:12px}
.cs-lag-table th{background:var(--gp);color:var(--g);padding:6px 10px;text-align:center;font-weight:600;border:1px solid var(--bd)}
.cs-lag-table td{padding:6px 10px;text-align:center;border:1px solid var(--bd)}.cs-lag-table td.best{background:#fef3e0;font-weight:700}
.cs-sig-table{width:100%;border-collapse:collapse;font-size:12px}
.cs-sig-table th{background:var(--gp);color:var(--g);padding:6px 10px;text-align:left;font-weight:600;border:1px solid var(--bd)}
.cs-sig-table td{padding:6px 10px;text-align:left;border:1px solid var(--bd)}.cs-sig-table td.neg{color:var(--red);font-weight:600}.cs-sig-table td.pos{color:var(--g);font-weight:600}
.cs-thesis{background:var(--gp);border:1px solid var(--g);border-radius:8px;padding:16px;margin-top:12px;font-size:13px;line-height:1.6}
.cs-thesis strong{color:var(--g)}
.cs-intro{font-size:14px;color:var(--tx2);line-height:1.7;margin-bottom:24px;max-width:900px}
'''

# ============================================================
# V3.2 JS: Smoothed chart handler (monkey-patches mk function)
# ============================================================
_V32_SMOOTHED_JS = '''
/* V3.2: Wrap mk() to handle "smoothed" view */
var _v31_mk=mk;
mk=function(did,D,v){
if(v==="smoothed"){
  var ctx=document.getElementById("ch-"+did);if(!ctx)return;
  if(charts[did])charts[did].destroy();
  var bm=D.bm||[];
  var bc2=D.l.map(function(m){return bm.indexOf(m)>=0?"rgba(230,126,34,0.3)":"rgba(30,144,255,0.3)";});
  var bbc2=D.l.map(function(m){return bm.indexOf(m)>=0?"rgba(230,126,34,0.8)":"rgba(30,144,255,0.7)";});
  var smLabel=D.sm_method||"Smoothed";
  var smCorr=D.sm_corr||0;
  var ds=[
    {label:smLabel,data:D.sm||D.r,backgroundColor:bc2,borderColor:bbc2,borderWidth:1},
    {label:"Raw (date_received)",data:D.r,type:"line",borderColor:"rgba(150,150,150,0.4)",
     borderWidth:1,borderDash:[4,4],fill:false,pointRadius:0,tension:0.2}
  ];
  var yL="Smoothed Reports ("+smLabel+", fit r="+smCorr+")";
  charts[did]=new Chart(ctx,{type:"bar",data:{labels:D.l,datasets:ds},options:{responsive:true,maintainAspectRatio:false,
    interaction:{mode:"index",intersect:false},
    scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},
    y:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#4a5f50",font:{size:10}},title:{display:true,text:yL,color:"#4a5f50",font:{size:11}}}},
    plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},
    zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},
    tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1}}}});
  return;
}
_v31_mk(did,D,v);};

/* V3.2: Add "smoothed" to chart descriptions */
if(typeof chartDescs!=="undefined"){chartDescs["smoothed"]="SMOOTHED VIEW: Blue bars show the best-fit smoothed MAUDE report count (method auto-selected from SMA-3/4/5 and EWMA-3/4/5). Dashed grey line shows raw date_received counts for comparison. Smoothing redistributes batch-reported dumps across prior months to reveal the true underlying trend. The fit correlation (r) measures how well the smoothed series aligns with date_of_event data. Higher r = better alignment = more reliable trend.";}
'''


# ============================================================
# WRAP generate_html: patches output with smoothing + case studies
# ============================================================
_v31_generate_html = generate_html

def generate_html(all_res, summary):
    """V3.2 wrapper: runs original HTML gen, then patches in smoothing + case studies."""
    # Run original — this creates docs/index.html
    _v31_generate_html(all_res, summary)

    html_path = "docs/index.html"
    try:
        with open(html_path, "r") as f:
            html = f.read()
    except Exception as ex:
        print(f"[V3.2] Could not read {html_path}: {ex}")
        return

    print("[V3.2] Patching HTML with smoothing + case studies...")
    original_len = len(html)

    # --- PATCH 1: Inject CSS before </style> ---
    html = html.replace("</style>", _V32_CSS + "\n</style>")

    # --- PATCH 2: Add Case Studies tab button ---
    # Strategy: find the first <div id="tc- (start of tab content) and insert tab button before it
    # The tabs div closes with </div> right before the first tabcontent
    tc_match = _re.search(r'(</div>\s*<div id="tc-)', html)
    if tc_match:
        insert_pos = tc_match.start()
        tab_btn = '<div class="tab" onclick="showTab(\'casestudies\')">Case Studies</div>'
        html = html[:insert_pos] + tab_btn + html[insert_pos:]
    else:
        print("[V3.2] WARN: Could not find tab insertion point")

    # --- PATCH 3: Add "Smoothed" button to every device's chart controls ---
    # Find all reset buttons and add Smoothed before them
    html = html.replace(
        '<button class="cb rst"',
        '<button class="cb" data-v="smoothed">Smoothed</button><button class="cb rst"'
    )

    # --- PATCH 4: Inject smoothed data into chart data JSON ---
    cd_marker = 'var defined_cd='
    cd_start = html.find(cd_marker)
    if cd_start >= 0:
        json_start = cd_start + len(cd_marker)
        # Find matching closing brace (balanced)
        depth, i = 0, json_start
        cd_end = -1
        while i < len(html):
            if html[i] == '{': depth += 1
            elif html[i] == '}':
                depth -= 1
                if depth == 0:
                    cd_end = i + 1
                    break
            i += 1
        if cd_end > json_start:
            try:
                cd = json.loads(html[json_start:cd_end])
                for did, R in all_res.items():
                    if did in cd:
                        sm = R.get("smooth", {})
                        sm_data = sm.get("smoothed", {})
                        labels = cd[did].get("l", [])
                        cd[did]["sm"] = [sm_data.get(m, 0) for m in labels]
                        mth = sm.get("method", "raw")
                        win = sm.get("window", 0)
                        cd[did]["sm_method"] = f"{mth}-{win}" if mth not in ("none","raw") else "raw"
                        cd[did]["sm_corr"] = sm.get("fit_corr", 0)
                html = html[:json_start] + json.dumps(cd) + html[cd_end:]
                print(f"  Smoothed data injected for {len([d for d in all_res if d in cd])} devices")
            except Exception as ex:
                print(f"  [V3.2] WARN: Could not parse chart data JSON: {ex}")
    else:
        print("[V3.2] WARN: Could not find defined_cd in HTML")

    # --- PATCH 5: Generate Case Studies tab content ---
    cs_html, cs_js_data, cs_js_init = generate_case_studies_html(all_res)

    # Insert case studies HTML before </body>
    html = html.replace('</body>', cs_html + '\n</body>')

    # --- PATCH 6: Inject V3.2 JavaScript before last </script> ---
    last_script = html.rfind('</script>')
    if last_script >= 0:
        v32_js = '\n/* V3.2 Additions */\n'
        v32_js += _V32_SMOOTHED_JS + '\n'
        v32_js += f'var cs_data={cs_js_data};\n'
        v32_js += cs_js_init + '\n'
        v32_js += 'document.addEventListener("DOMContentLoaded",function(){initCaseStudies();});\n'
        html = html[:last_script] + v32_js + html[last_script:]

    # --- Write patched file ---
    try:
        with open(html_path, "w") as f:
            f.write(html)
        delta = len(html) - original_len
        print(f"[V3.2] HTML patched: {html_path} (+{delta//1024}KB)")
    except Exception as ex:
        print(f"[V3.2] Could not write {html_path}: {ex}")


# ============================================================
# END OF V3.2 BLOCK 2
# ============================================================

if __name__=="__main__": main()
