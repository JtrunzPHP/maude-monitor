#!/usr/bin/env python3
"""MAUDE Monitor V3.1 — Complete with all 13 data modules integrated."""
import json,os,time,math,argparse,smtplib,csv
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

try:
    from stats_engine import compute_enhanced_correlation, _self_test as stats_selftest
    from data_modules import (
        analyze_failure_modes, analyze_edgar_filings, analyze_international,
        analyze_google_trends, analyze_insider_trading, analyze_clinical_trials,
        analyze_short_interest, analyze_payer_coverage, compute_recall_probability,
        compute_peer_relative, predict_earnings_surprise, backtest_r_score
    )
    HAS_MODULES = True
except ImportError as e:
    HAS_MODULES = False
    print(f"Module import failed: {e}")

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
# PIPELINE — runs ALL modules
# ============================================================
def run_pipeline(backfill=False,quick=False):
    start="20230101" if backfill else ("20250901" if quick else "20230101")
    all_res,summary={},[]
    if HAS_MODULES: print("ALL ENHANCED MODULES LOADED"); stats_selftest()
    else: print("BASIC mode - no enhanced modules")
    for dev in DEVICES:
        did=dev["id"]; print(f"\n{'='*50}\n{dev['name']} ({dev['ticker']})")
        recv=fetch_counts(dev["search"],"date_received",start); time.sleep(0.3)
        evnt=fetch_counts(dev["search"],"date_of_event",start); time.sleep(0.3)
        sev=fetch_severity(dev["search"],start); batch=detect_batch(recv,evnt)
        stats=compute_stats(recv,sev,dev["ticker"]); rscore=compute_r_score(stats) if stats else None
        # Initialize ALL module results
        modules={"enhanced_corr":None,"failure_modes":None,"google_trends":None,"insider":None,"trials":None,"short_interest":None,"edgar":None,"payer":None,"international":None,"recall_prob":None,"earnings_pred":None,"backtest":None,"peer_relative":None}
        if HAS_MODULES and stats:
            # Correlation
            try:
                print("  Running: Enhanced correlation...")
                modules["enhanced_corr"]=compute_enhanced_correlation(recv,STOCK_MONTHLY.get(dev["ticker"],{}),max_lag=6)
            except Exception as e: modules["enhanced_corr"]={"status":"error","message":str(e)[:100]}
            # NLP failure modes (individual products only)
            if not did.endswith("_ALL"):
                try:
                    print("  Running: Failure mode NLP...")
                    modules["failure_modes"]=analyze_failure_modes(dev["search"],start,limit=50)
                except Exception as e: modules["failure_modes"]={"status":"error","message":str(e)[:100]}
            # Company-level OR key individual products
            is_company=did.endswith("_ALL") or did in ("SQEL_TWIIST","BBNX_ILET")
            if is_company:
                try:
                    print("  Running: Google Trends...")
                    modules["google_trends"]=analyze_google_trends(dev["ticker"])
                except Exception as e: modules["google_trends"]={"status":"error","message":str(e)[:100]}
                try:
                    print("  Running: Insider trading (Form 4)...")
                    modules["insider"]=analyze_insider_trading(dev["ticker"])
                except Exception as e: modules["insider"]={"status":"error","message":str(e)[:100]}
                try:
                    print("  Running: Clinical trials...")
                    modules["trials"]=analyze_clinical_trials(dev["ticker"])
                except Exception as e: modules["trials"]={"status":"error","message":str(e)[:100]}
                try:
                    print("  Running: Short interest...")
                    modules["short_interest"]=analyze_short_interest(dev["ticker"])
                except Exception as e: modules["short_interest"]={"status":"error","message":str(e)[:100]}
                try:
                    print("  Running: CMS payer coverage...")
                    modules["payer"]=analyze_payer_coverage(dev["ticker"])
                except Exception as e: modules["payer"]={"status":"error","message":str(e)[:100]}
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
            # Recall probability
            try:
                print("  Running: Recall probability...")
                modules["recall_prob"]=compute_recall_probability(modules.get("failure_modes"),stats)
            except Exception as e: modules["recall_prob"]={"status":"error","message":str(e)[:100]}
        all_res[did]={"device":dev,"received":recv,"by_event":evnt,"severity":sev,"batch_flags":batch,"stats":stats,"r_score":rscore,**modules}
        if stats:
            lt=stats[-1]; ec=modules["enhanced_corr"]
            summary.append({"id":did,"name":dev["name"],"ticker":dev["ticker"],"company":dev["company"],"month":lt["month"],"reports":lt["count"],"z_score":lt["z_score"],"rate_per_m":lt["rate_per_m"],"rate_per_10k":lt["rate_per_10k"],"slope_6m":lt["slope_6m"],"deaths_3mo":sum(s["deaths"] for s in stats[-3:]),"injuries_3mo":sum(s["injuries"] for s in stats[-3:]),"r_score":rscore["total"] if rscore else None,"signal":rscore["signal"] if rscore else "NORMAL","batch":batch.get(lt["month"],{}).get("is_batch",False),"corr_rho":(ec or {}).get("best_rho"),"corr_sig":(ec or {}).get("significant")})
            print(f"  >> {lt['month']} | {lt['count']:,} | Z:{lt['z_score']:+.2f} | R:{rscore['total'] if rscore else '-'}")
    # POST-LOOP: peer-relative, earnings, backtest
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
# HTML — builds card sections for ALL 13 modules
# ============================================================
def _mbox(title, data, fallback_msg="Module not loaded or no data available."):
    """Always-visible module box with status indicator."""
    if data is None:
        return f'<div class="mbox"><h4>{title} <span class="mstat mgrey">N/A</span></h4><div class="msub">{fallback_msg}</div></div>'
    st=data.get("status","unknown") if isinstance(data,dict) else "unknown"
    msg=data.get("message","") if isinstance(data,dict) else str(data)
    if isinstance(msg,str) and len(msg)>350: msg=msg[:350]+"..."
    cls="mok" if st=="ok" else "mwarn" if st in ("framework","no_pytrends","blocked","severity_only","no_text","no_alerts","no_data","no_signals","no_query","no_cik","no_ticker","insufficient_data","no_filings","no_trials","parse_error") else "merr" if st=="error" else "mgrey"
    return f'<div class="mbox"><h4>{title} <span class="mstat {cls}">{st.upper()}</span></h4><div class="msub">{msg}</div></div>'

def generate_html(all_res,summary):
    os.makedirs("docs",exist_ok=True)
    cd={}
    for did,r in all_res.items():
        if not r.get("stats"): continue
        cd[did]={"l":[s["month"] for s in r["stats"]],"c":[s["count"] for s in r["stats"]],"ma":[s["ma6"] for s in r["stats"]],"u2":[s["upper_2sd"] for s in r["stats"]],"l2":[s["lower_2sd"] for s in r["stats"]],"u1":[s["upper_1sd"] for s in r["stats"]],"l1":[s["lower_1sd"] for s in r["stats"]],"z":[s["z_score"] for s in r["stats"]],"rm":[s["rate_per_m"] for s in r["stats"]],"r10":[s["rate_per_10k"] for s in r["stats"]],"d":[s["deaths"] for s in r["stats"]],"inj":[s["injuries"] for s in r["stats"]],"mal":[s["malfunctions"] for s in r["stats"]],"dev":r["device"],"rs":r.get("r_score"),"bm":[m for m,bf in r["batch_flags"].items() if bf.get("is_batch")],"sp":STOCK_MONTHLY.get(r["device"]["ticker"],{}),"evts":PRODUCT_EVENTS.get(did,[])}
    so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3}
    summary.sort(key=lambda x:(so.get(x["signal"],4),-(x["r_score"] or 0)))
    trows=""
    for s in summary:
        zc="neg" if s["z_score"]>Z_WARN else "pos" if s["z_score"]<-Z_WARN else ""
        rc="neg" if (s["r_score"] or 0)>=50 else "warn" if (s["r_score"] or 0)>=30 else ""
        bw=' <span class="bw">Batch</span>' if s.get("batch") else ""
        ic="1" if s["id"].endswith("_ALL") else "0"
        cr="\u2014"
        if s.get("corr_rho") is not None: cr=f'{s["corr_rho"]:+.3f}'; cr+=(" *" if s.get("corr_sig") else "")
        d3c="neg" if s["deaths_3mo"]>0 else ""
        trows+=f'<tr class="pr" data-co="{s["company"]}" data-id="{s["id"]}" data-sig="{s["signal"]}" data-comb="{ic}"><td>{s["name"]}{bw}</td><td>{s["ticker"]}</td><td>{s["month"]}</td><td>{fmt0(s["reports"])}</td><td class="{zc}">{s["z_score"]:+.2f}</td><td class="{rc}">{fmt(s["r_score"])}</td><td>{fmt2(s["rate_per_m"])}</td><td>{fmt2(s["rate_per_10k"])}</td><td>{s["slope_6m"]:+.1f}</td><td class="{d3c}">{s["deaths_3mo"]}</td><td>{s["injuries_3mo"]}</td><td>{cr}</td><td><span class="sig sig-{s["signal"]}">{s["signal"]}</span></td></tr>\n'

    # BUILD ALL CARDS with ALL module sections
    company_cards={c:"" for c in COMPANIES}
    for did,r in all_res.items():
        if not r.get("stats"): continue
        dv=r["device"]; st=r["stats"]; lt=st[-1]; rs=r.get("r_score"); co=dv["company"]
        sig=rs["signal"] if rs else "NORMAL"
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
        # R-Score
        rhtml=""
        if rs:
            rcol="#c0392b" if rs["total"]>=50 else "#e67e22" if rs["total"]>=30 else "#27ae60"
            rhtml=f'<div class="rg"><div class="rgv" style="color:{rcol}">{rs["total"]}</div><div class="rgr"><div class="rgl">R-Score (0-100)</div><div class="rgt"><div class="rgf" style="width:{min(100,rs["total"])}%;background:{rcol}"></div></div></div></div>'
            rhtml+=f'<div class="rcg"><div class="rci"><span class="rcv">{rs["z_c"]}</span>Z</div><div class="rci"><span class="rcv">{rs["sev_c"]}</span>Sev</div><div class="rci"><span class="rcv">{rs["gap_c"]}</span>Gap</div><div class="rci"><span class="rcv">{rs["slope_c"]}</span>Slope</div><div class="rci"><span class="rcv">{rs["ib_c"]}</span>IB</div></div>'
        # NLP Failure Modes
        fm=r.get("failure_modes")
        fmhtml=""
        if fm and isinstance(fm,dict) and fm.get("status")=="ok" and fm.get("modes"):
            fmhtml='<div class="mbox"><h4>FAILURE MODES (NLP) <span class="mstat mok">OK</span></h4>'
            fmhtml+=f'<div class="msub">{fm["total_analyzed"]} reports analyzed. Classified by keyword matching on MAUDE narrative text.</div>'
            for cat,info in sorted(fm["modes"].items(),key=lambda x:-x[1]["count"]):
                fmhtml+=f'<div class="fmr"><span class="fml">{info["label"]}</span><div class="fmb"><div style="width:{info["pct"]}%;background:var(--g)"></div></div><span class="fmp">{info["pct"]}%</span></div>'
                fmhtml+=f'<div class="msub" style="padding-left:136px;font-size:9px;margin-top:0">{info["desc"]}</div>'
            fmhtml+='</div>'
        elif fm:
            fmhtml=_mbox("FAILURE MODES (NLP)",fm)
        else:
            fmhtml=_mbox("FAILURE MODES (NLP)",None,"Only runs for individual products, not company-level combined views.")
        # Recall Probability
        rp=r.get("recall_prob")
        rphtml=""
        if rp and isinstance(rp,dict) and rp.get("probability") is not None:
            prob=rp["probability"]; pcol="#c0392b" if prob>0.5 else "#e67e22" if prob>0.25 else "#27ae60"
            rphtml=f'<div class="mbox"><h4>RECALL PROBABILITY (6MO) <span class="mstat" style="color:{pcol}">{rp.get("signal","?")}</span></h4><div class="msub">{rp.get("message","")}</div><div class="rgt" style="margin-top:4px"><div class="rgf" style="width:{prob*100}%;background:{pcol}"></div></div></div>'
        else: rphtml=_mbox("RECALL PROBABILITY (6MO)",rp)
        # Google Trends
        gt=r.get("google_trends")
        gthtml=""
        if gt and isinstance(gt,dict) and gt.get("status")=="ok" and gt.get("trends"):
            gthtml='<div class="mbox"><h4>GOOGLE TRENDS <span class="mstat mok">OK</span></h4><div class="msub">Complaint-related search interest for this company\'s products. Rising = patients are Googling problems before filing MAUDE reports (2-4 week lead).</div>'
            for t in gt["trends"]:
                chcol="neg" if t["change_pct"]>20 else "pos" if t["change_pct"]<-20 else ""
                gthtml+=f'<div class="msub">"{t["query"]}": interest {t["recent_interest"]:.0f} vs prior {t["prior_interest"]:.0f} (<span class="{chcol}">{t["change_pct"]:+.0f}%</span>)</div>'
            gthtml+=f'<div class="msub"><strong>Signal: {gt["signal"].upper()}</strong> - {gt.get("message","")}</div></div>'
        else: gthtml=_mbox("GOOGLE TRENDS",gt,"Google Trends provides the earliest signal. Requires pytrends package.")
        # Insider Trading
        inshtml=_mbox("SEC FORM 4 INSIDER TRADING",r.get("insider"),"Tracks insider buys/sells from SEC EDGAR. High selling + high R-Score = strong conviction short signal.")
        # Clinical Trials
        ct=r.get("trials")
        cthtml=""
        if ct and isinstance(ct,dict) and ct.get("status")=="ok" and ct.get("trials"):
            cthtml='<div class="mbox"><h4>ACTIVE CLINICAL TRIALS <span class="mstat mok">OK</span></h4><div class="msub">Active/recruiting trials from ClinicalTrials.gov. Competitor trials = future threat. Paused trials = potential quality signal.</div>'
            for trial in ct["trials"][:3]:
                cthtml+=f'<div class="msub">{trial["nct_id"]}: {trial["title"]} <span class="mstat mgrey">{trial["status"]}</span></div>'
            cthtml+='</div>'
        else: cthtml=_mbox("CLINICAL TRIALS",ct,"From ClinicalTrials.gov API. Tracks competitive pipeline.")
        # Short Interest
        si=r.get("short_interest")
        sihtml=""
        if si and isinstance(si,dict) and si.get("status")=="ok":
            sicol="#c0392b" if si.get("signal")=="high" else "#e67e22" if si.get("signal")=="moderate" else "#27ae60"
            sihtml=f'<div class="mbox"><h4>SHORT INTEREST <span class="mstat" style="color:{sicol}">{si.get("signal","?").upper()}</span></h4><div class="msub">{si.get("message","")}</div></div>'
        else: sihtml=_mbox("SHORT INTEREST",si,"From Yahoo Finance. High short interest + high R-Score = market agrees with our signal.")
        # EDGAR NLP
        edhtml=_mbox("SEC FILING NLP (EDGAR)",r.get("edgar"),"Scans 10-Q/10-K filings for quality-related language (recall, warning letter, warranty cost). Rising trend = management preparing market.")
        # Payer Coverage
        payhtml=_mbox("CMS PAYER / FORMULARY",r.get("payer"),"Tracks Medicare/Medicaid coverage decisions. CMS does not provide a structured API.")
        # Peer-Relative
        pr=r.get("peer_relative")
        prhtml=""
        if pr and isinstance(pr,dict):
            prcol="#c0392b" if pr.get("signal") in ("WORST","WEAK") else "#27ae60" if pr.get("signal") in ("BEST","STRONG") else "var(--tx3)"
            prhtml=f'<div class="mbox"><h4>PEER-RELATIVE POSITION <span class="mstat" style="color:{prcol}">{pr.get("signal","?")}</span></h4><div class="msub">{pr.get("message","")} Long the cleanest name, short the dirtiest.</div></div>'
        else: prhtml=_mbox("PEER-RELATIVE POSITION",None,"Compares R-Score to peer group. Computed after all products are processed.")
        # Earnings Predictor
        ep=r.get("earnings_pred")
        ephtml=""
        if ep and isinstance(ep,dict) and ep.get("status")=="ok":
            epcol="#c0392b" if ep["prediction"]=="LIKELY MISS" else "#27ae60" if ep["prediction"]=="LIKELY BEAT" else "var(--tx3)"
            ephtml=f'<div class="mbox"><h4>EARNINGS SURPRISE PREDICTOR <span class="mstat" style="color:{epcol}">{ep["prediction"]}</span></h4><div class="msub">{ep.get("message","")} Trade ahead of earnings reports using this signal.</div></div>'
        else: ephtml=_mbox("EARNINGS SURPRISE PREDICTOR",ep,"Predicts beat/miss based on R-Score, severity trend, and peer position.")
        # Backtest
        bt=r.get("backtest")
        bthtml=""
        if bt and isinstance(bt,dict) and bt.get("status")=="ok" and bt.get("results"):
            bthtml='<div class="mbox"><h4>R-SCORE BACKTEST <span class="mstat mok">OK</span></h4>'
            bthtml+=f'<div class="msub">{bt.get("message","")} This proves (or disproves) the signal historically.</div>'
            for window,res in bt["results"].items():
                wcol="pos" if res["avg_return"]<0 else "neg"
                bthtml+=f'<div class="msub">{window} window: avg return <span class="{wcol}">{res["avg_return"]:+.1f}%</span>, win rate {res["win_rate"]:.0f}% (n={res["n"]})</div>'
            bthtml+='</div>'
        else: bthtml=_mbox("R-SCORE BACKTEST",bt,"Historical test: when R-Score crossed 50, what happened to stock? Only runs for company-level views.")
        # Enhanced Correlation
        echtml=_mbox("MAUDE-STOCK CORRELATION",r.get("enhanced_corr"),"Spearman rank correlation + Granger causality. Requires stats_engine.py. Shows lead time between MAUDE signal and stock reaction.")
        # International
        intlhtml=_mbox("INTERNATIONAL (MHRA/UK)",r.get("international"),"UK Medical Device Alerts from GOV.UK. Limited coverage — MHRA has no structured API.")

        zc2="neg" if lt["z_score"]>1.5 else "pos" if lt["z_score"]<-1.5 else ""
        slc2="neg" if lt["slope_6m"]>0 else "pos"
        d3c2="neg" if d3>0 else ""
        card=f'''<div class="card" data-id="{did}">
<div class="chdr"><div><h3>{dv["name"]}</h3><span class="tk">{dv["ticker"]}{"  (Private)" if dv["ticker"]=="SQEL" else " (Segment)" if dv["ticker"]=="MDT_DM" else ""}</span></div><span class="sig sig-{sig}">{sig}</span></div>
<p class="desc">{dv["description"]}</p>{ehtml}
<div class="sg"><div class="si"><div class="sil">LATEST</div><div class="siv">{fmt0(lt["count"])}</div><div class="sis">{lt["month"]}</div></div>
<div class="si"><div class="sil">Z-SCORE</div><div class="siv {zc2}">{lt["z_score"]:+.2f}</div><div class="sis">12mo avg {fmt0(lt["avg_12m"])}</div></div>
<div class="si"><div class="sil">RATE/$M</div><div class="siv">{fmt2(lt["rate_per_m"])}</div><div class="sis">per $M quarterly rev</div></div>
<div class="si"><div class="sil">RATE/10K USERS</div><div class="siv">{fmt2(lt["rate_per_10k"])}</div><div class="sis">per 10K installed base</div></div></div>
<div class="sg"><div class="si"><div class="sil">6MO TREND</div><div class="siv {slc2}">{lt["slope_6m"]:+.1f}/mo</div><div class="sis">regression slope</div></div>
<div class="si"><div class="sil">DEATHS (3MO)</div><div class="siv {d3c2}">{d3}</div></div>
<div class="si"><div class="sil">INJURIES (3MO)</div><div class="siv">{i3}</div></div>
<div class="si"><div class="sil">SEVERITY</div><div class="siv">{fmt0(lt["severity_score"])}</div><div class="sis">D x10 I x3 M x1</div></div></div>
{rhtml}
{fmhtml}{rphtml}{gthtml}{inshtml}{cthtml}{sihtml}{edhtml}{prhtml}{ephtml}{bthtml}{payhtml}{echtml}{intlhtml}
<div class="cc" id="cc-{did}"><button class="cb active" data-v="reports">Reports</button><button class="cb" data-v="rate_m">Rate/$M</button><button class="cb" data-v="rate_10k">Rate/10K</button><button class="cb" data-v="severity">Severity</button><button class="cb" data-v="zscore">Z-Score</button><button class="cb" data-v="stock">Stock</button><button class="cb rst" data-v="reset">Reset</button></div>
<div class="cdesc" id="cdesc-{did}">Select a chart view above. Each tab shows a different analytical lens with context.</div>
<div class="cw"><canvas id="ch-{did}"></canvas></div></div>\n'''
        if co in company_cards: company_cards[co]+=card

    tab_ids={"Summary":"summary","Dexcom":"dexcom","Insulet":"insulet","Tandem":"tandem","Abbott":"abbott","Beta Bionics":"bbnx","Medtronic":"medtronic","Sequel Med Tech":"sequel"}
    tab_btns='<div class="tabs">'
    for name,tid in tab_ids.items():
        act=' active' if tid=="summary" else ""
        tab_btns+=f'<button class="tab{act}" onclick="showTab(\'{tid}\')">{name}</button>'
    tab_btns+='</div>'
    modules_str="ALL MODULES (Stats+NLP+Trends+Insider+Trials+ShortInt+EDGAR+Payer+Recall+Peer+Earnings+Backtest+Intl)" if HAS_MODULES else "BASIC (no modules)"
    updated_str=datetime.now().strftime('%b %d, %Y %H:%M ET')

    html_top=f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Monitor V3.1</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{{--g:#2B5F3A;--gx:#e8f5ec;--gp:#f4faf6;--bg:#fff;--bg2:#f8faf9;--bg3:#f0f3f1;--tx:#1a2a1f;--tx2:#4a5f50;--tx3:#7a8f80;--bd:#d4e0d8;--red:#c0392b;--org:#e67e22}}
*{{margin:0;padding:0;box-sizing:border-box}}body{{background:var(--bg);color:var(--tx);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6}}
.ct{{max-width:1440px;margin:0 auto;padding:24px 32px}}
header{{display:flex;justify-content:space-between;align-items:center;padding:20px 0;border-bottom:2px solid var(--g);margin-bottom:20px}}
header h1{{font-size:22px;font-weight:700;color:var(--g)}}header .sub{{font-size:13px;color:var(--tx2)}}header .meta{{text-align:right;font-size:11px;color:var(--tx3)}}
h2{{font-size:18px;font-weight:700;color:var(--g);margin:20px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--bd)}}
.tabs{{display:flex;gap:2px;margin-bottom:20px;border-bottom:2px solid var(--bd)}}
.tab{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-bottom:none;border-radius:6px 6px 0 0;padding:8px 16px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit}}
.tab:hover{{background:var(--gx)}}.tab.active{{background:var(--g);color:#fff;border-color:var(--g)}}
.tabcontent{{display:none}}.tabcontent.active{{display:block}}
.sig{{display:inline-block;padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;text-transform:uppercase}}
.sig-NORMAL{{background:var(--gx);color:var(--g)}}.sig-WATCH{{background:#fef3e0;color:#b8860b}}.sig-ELEVATED{{background:#fdecea;color:var(--red)}}.sig-CRITICAL{{background:#f5c6cb;color:#721c24}}
.bw{{background:#fef3e0;color:#b8860b;font-size:10px;padding:1px 6px;border-radius:4px}}
.guide{{background:var(--gp);border:1px solid var(--bd);border-radius:10px;padding:20px;margin-bottom:20px}}
.guide h3{{font-size:15px;font-weight:700;color:var(--g);margin-bottom:12px}}
.gg{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.gi{{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:12px}}
.gi h4{{font-size:12px;font-weight:700;color:var(--g);margin-bottom:4px}}.gi p{{font-size:11px;color:var(--tx2)}}
table{{width:100%;border-collapse:collapse}}th{{text-align:left;padding:8px 10px;font-size:10px;font-weight:600;text-transform:uppercase;color:var(--tx3);border-bottom:2px solid var(--g);background:var(--bg);position:sticky;top:0}}
td{{padding:7px 10px;border-bottom:1px solid var(--bd);font-size:12px;white-space:nowrap}}tr:hover{{background:var(--gp)}}
.neg{{color:var(--red);font-weight:500}}.pos{{color:var(--g);font-weight:500}}.warn{{color:var(--org);font-weight:500}}
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
<div class="gi"><h4>Z-Score</h4><p>Std deviations from 12-month mean. Above +2.0 = unusual (p~2.3%). WATCH 1.5, ELEVATED 2.0, CRITICAL 3.0.</p></div>
<div class="gi"><h4>R-Score (0-100)</h4><p>Composite: Z-anomaly + severity trend + growth gap + slope + installed-base rate. Above 50 = investigate. Above 70 = act.</p></div>
<div class="gi"><h4>Rate/$M Revenue</h4><p>Reports / monthly revenue. Normalizes for business SIZE. Rising = quality deteriorating relative to revenue.</p></div>
<div class="gi"><h4>Rate/10K Users</h4><p>Reports / installed base. More precise than Rate/$M. Sources: earnings calls, 10-K filings.</p></div>
<div class="gi"><h4>6mo Trend (Slope)</h4><p>Linear regression slope of last 6 months. +50 means reports increasing ~50/month. Positive = accelerating problem.</p></div>
<div class="gi"><h4>Deaths/Injuries (3mo)</h4><p>Event counts from most recent 3 months. Deaths weighted 10x in severity score. MAUDE-reported, not confirmed causal.</p></div>
<div class="gi"><h4>Batch Detection</h4><p>If received > 3x event count same month = retrospective dump (recall paperwork), not real surge. Flagged as "Batch".</p></div>
<div class="gi"><h4>Correlation (Corr)</h4><p>Spearman rank between MAUDE z-scores and stock returns. Dash = no stock (private). * = p&lt;0.05. Negative = MAUDE predicts decline.</p></div>
</div></div>
<h2>All Products \u2014 Latest Month</h2>
<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>6mo Trend</th><th>Deaths (3mo)</th><th>Injuries (3mo)</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div>
</div>'''
    for comp in COMPANIES:
        tid=tab_ids[comp]; cards_html=company_cards.get(comp,"<p>No data.</p>")
        html_top+=f'\n<div class="tabcontent" id="tc-{tid}"><h2>{comp}</h2><div class="grid">{cards_html}</div></div>'
    html_top+='\n<div class="disc">Research only. Not investment advice. MAUDE has known limitations. Revenue from SEC filings. Installed base from earnings calls. MDT stock = parent company (~8% diabetes). BBNX from Feb 2025 IPO. Sequel is private. Correlation is not causation. Google Trends requires pytrends. Short interest from Yahoo Finance (may be blocked). CMS payer tracking is framework-level.</div></div>'

    # JAVASCRIPT (completely separate, no f-string)
    js=r'''<script>
var defined_cd=__CD__;var charts={};
var chartDescs={"reports":"REPORTS + SIGMA BANDS: Bars = monthly MAUDE reports. Light green band = +/-1 std dev (68% normal). Outer = +/-2 std dev (95%). Beyond 2-sigma = statistically anomalous. Orange bars = batch reporting (retrospective dump). Red = regulatory event month. The 6-month moving average (dark line) smooths noise to reveal the underlying trend.","rate_m":"RATE PER $M REVENUE: Reports divided by monthly revenue (quarterly/3). Normalizes for business growth. If company doubles revenue, reports should double too. RISING rate = quality deteriorating faster than revenue growing. Different from Rate/10K when average selling price changes (e.g. OTC products).","rate_10k":"RATE PER 10K USERS: Reports divided by estimated installed base (per 10K users). The most precise normalization. New products with few users show HIGH rates even with few reports (which is correct - that IS a high failure rate). User estimates from earnings calls and SEC filings.","severity":"SEVERITY BREAKDOWN: Deaths (red) = most serious, weighted 10x. Injuries (orange) = hospitalizations/ER, weighted 3x. Malfunctions (green) = device failures without direct patient harm, weighted 1x. A shift toward more deaths/injuries vs malfunctions = worsening signal.","zscore":"Z-SCORE HISTORY: How far each month deviates from the 12-month mean in standard deviations. Above +2 (red dashed line) = statistically significant spike. Below -2 (green) = unusually low. Sustained positive z-scores = developing quality problem.","stock":"STOCK PRICE OVERLAY: Green = stock price. Red = MAUDE reports. Look for MAUDE spikes (red up) that precede stock declines (green down) by 1-4 months. That lead time is the alpha window. For MDT, price is the parent conglomerate."};
function showTab(id){var tabs=document.querySelectorAll(".tab");for(var i=0;i<tabs.length;i++){tabs[i].classList.remove("active");}var tcs=document.querySelectorAll(".tabcontent");for(var i=0;i<tcs.length;i++){tcs[i].classList.remove("active");}var clickedTab=document.querySelector('.tab[onclick*="'+id+'"]');if(clickedTab)clickedTab.classList.add("active");var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");}
function init(){for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}var allcc=document.querySelectorAll(".cc");for(var ci=0;ci<allcc.length;ci++){var btns=allcc[ci].querySelectorAll(".cb");for(var bi=0;bi<btns.length;bi++){btns[bi].addEventListener("click",function(){var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}var siblings=mycc.querySelectorAll(".cb:not(.rst)");for(var si=0;si<siblings.length;si++){siblings[si].classList.remove("active");}this.classList.add("active");var descEl=document.getElementById("cdesc-"+did);if(descEl&&chartDescs[v]){descEl.textContent=chartDescs[v];}mk(did,defined_cd[did],v);});}}}
function mk(did,D,v){var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];var evtMs=[];for(var ei=0;ei<evts.length;ei++){evtMs.push(evts[ei].date);}
if(v==="reports"){var bc=[];for(var bi=0;bi<D.l.length;bi++){bc.push(bm.indexOf(D.l[bi])>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(D.l[bi])>=0?"rgba(192,57,43,0.4)":"rgba(43,95,58,0.25)");}ds=[{label:"2s upper",data:D.u2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:"+1",pointRadius:0,order:5},{label:"2s lower",data:D.l2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:false,pointRadius:0,order:5},{label:"1s upper",data:D.u1,borderWidth:0,backgroundColor:"rgba(43,95,58,0.10)",fill:"+1",pointRadius:0,order:4},{label:"1s lower",data:D.l1,borderWidth:0,fill:false,pointRadius:0,order:4},{label:"Reports",data:D.c,borderColor:"rgba(43,95,58,0.85)",backgroundColor:bc,borderWidth:1.5,type:"bar",order:2},{label:"6mo MA",data:D.ma,borderColor:"#2B5F3A",borderWidth:2.5,fill:false,pointRadius:0,tension:0.3,order:1}];yL="Monthly Reports";}
else if(v==="rate_m"){var rd=[];for(var i=0;i<D.rm.length;i++){rd.push(D.rm[i]===null?undefined:D.rm[i]);}ds=[{label:"Rate/$M",data:rd,borderColor:"#2B5F3A",backgroundColor:"rgba(43,95,58,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per $M Revenue";}
else if(v==="rate_10k"){var r10=[];for(var i=0;i<D.r10.length;i++){r10.push(D.r10[i]===null?undefined:D.r10[i]);}ds=[{label:"Rate/10K Users",data:r10,borderColor:"#8B4513",backgroundColor:"rgba(139,69,19,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per 10K Users";}
else if(v==="severity"){ds=[{label:"Deaths",data:D.d,backgroundColor:"rgba(192,57,43,0.8)",borderWidth:0,stack:"s"},{label:"Injuries",data:D.inj,backgroundColor:"rgba(230,126,34,0.7)",borderWidth:0,stack:"s"},{label:"Malfunctions",data:D.mal,backgroundColor:"rgba(43,95,58,0.3)",borderWidth:0,stack:"s"}];yL="Events by Type";}
else if(v==="zscore"){var zc=[];for(var i=0;i<D.z.length;i++){var zv=D.z[i];zc.push(zv>2?"rgba(192,57,43,0.8)":zv>1.5?"rgba(230,126,34,0.7)":zv<-1.5?"rgba(43,95,58,0.6)":"rgba(43,95,58,0.25)");}var t2=[];var nt2=[];for(var i=0;i<D.l.length;i++){t2.push(2);nt2.push(-2);}ds=[{label:"Z-Score",data:D.z,backgroundColor:zc,borderWidth:0,type:"bar"},{label:"+2s",data:t2,borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false},{label:"-2s",data:nt2,borderColor:"rgba(43,95,58,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false}];yL="Z-Score";}
else if(v==="stock"){var sp=D.sp||{};var sl=[];var sv=[];var sc=[];for(var i=0;i<D.l.length;i++){if(sp[D.l[i]]){sl.push(D.l[i]);sv.push(sp[D.l[i]]);sc.push(D.c[i]);}}if(sl.length<2){var descEl=document.getElementById("cdesc-"+did);if(descEl)descEl.textContent="Limited stock data. Company may be private (Sequel), recently IPO'd (BBNX Feb 2025), or product too new for sufficient price history.";return;}ds=[{label:"Stock ($)",data:sv,borderColor:"#2B5F3A",borderWidth:2,fill:false,pointRadius:1.5,tension:0.2},{label:"MAUDE Reports",data:sc,borderColor:"rgba(192,57,43,0.6)",borderWidth:1.5,fill:false,pointRadius:0,tension:0.2,yAxisID:"y1"}];charts[did]=new Chart(ctx,{type:"line",data:{labels:sl,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{position:"left",grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#2B5F3A",font:{size:10}},title:{display:true,text:"Stock ($)",color:"#2B5F3A",font:{size:11}}},y1:{position:"right",grid:{drawOnChartArea:false},ticks:{color:"#c0392b",font:{size:10}},title:{display:true,text:"MAUDE Reports",color:"#c0392b",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1}}}});return;}
charts[did]=new Chart(ctx,{type:"line",data:{labels:D.l,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#4a5f50",font:{size:10}},title:{display:true,text:yL,color:"#4a5f50",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},pinch:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1,callbacks:{afterBody:function(it){var idx=it[0].dataIndex;var month=D.l[idx];var msgs=[];if(bm.indexOf(month)>=0){msgs.push("BATCH REPORTING DETECTED");}for(var ee=0;ee<evts.length;ee++){if(evts[ee].date===month){msgs.push(evts[ee].type+": "+evts[ee].desc);}}return msgs.length?"\n"+msgs.join("\n"):"";}}}}}});}
function af(){var co=document.getElementById("fc").value;var sig=document.getElementById("fs").value;var vw=document.getElementById("fv").value;var so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3};var els=document.querySelectorAll(".pr,.card");for(var i=0;i<els.length;i++){var el=els[i];var sh=true;var ec=el.getAttribute("data-co");var es=el.getAttribute("data-sig");var ic=el.getAttribute("data-comb")==="1";if(co!=="all"&&ec!==co)sh=false;if(sig!=="all"){var sv=so[es]||3;if(sig==="CRITICAL"&&es!=="CRITICAL")sh=false;if(sig==="ELEVATED"&&sv>1)sh=false;if(sig==="WATCH"&&sv>2)sh=false;}if(vw==="combined"&&!ic)sh=false;if(vw==="individual"&&ic)sh=false;el.style.display=sh?"":"none";}}
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
    print(f"MAUDE Monitor V3.1 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} products | Modules: {'ALL' if HAS_MODULES else 'NONE'}")
    r,s=run_pipeline(a.backfill,a.quick); generate_html(r,s); send_alerts(s)
    print(f"\nCOMPLETE | docs/index.html")

if __name__=="__main__": main()
