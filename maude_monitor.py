#!/usr/bin/env python3
"""MAUDE Monitor V3.0 — FDA Adverse Event Intelligence. Tabbed layout, 7 companies."""
import json,os,time,math,argparse,smtplib,csv
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

try:
    from stats_engine import compute_enhanced_correlation, _self_test as stats_selftest
    from data_modules import (analyze_failure_modes, analyze_reddit_sentiment, analyze_edgar_filings, analyze_international)
    HAS_MODULES = True
except ImportError:
    HAS_MODULES = False

def fmt(v,d=1):
    if v is None: return "—"
    if abs(v)>=1e6: return f"{v/1e6:,.{d}f}M"
    if abs(v)>=1e3: return f"{v:,.{d}f}"
    return f"{v:.{d}f}"
def fmt0(v):
    if v is None: return "—"
    return f"{v:,.0f}"
def fmt2(v):
    if v is None: return "—"
    return f"{v:,.2f}"

# ============================================================
# ALL DATA FROM SEC FILINGS / EARNINGS CALLS
# Revenue in $M per calendar quarter
# ============================================================
QUARTERLY_REVENUE = {
    "DXCM":{"2023-Q1":921,"2023-Q2":871.3,"2023-Q3":975,"2023-Q4":1010,"2024-Q1":921,"2024-Q2":1004,"2024-Q3":994.2,"2024-Q4":1115,"2025-Q1":1036,"2025-Q2":1092,"2025-Q3":1174,"2025-Q4":1260,"2026-Q1":1270},
    "PODD":{"2023-Q1":412.5,"2023-Q2":432.1,"2023-Q3":476,"2023-Q4":521.5,"2024-Q1":481.5,"2024-Q2":530.4,"2024-Q3":543.9,"2024-Q4":597.7,"2025-Q1":555,"2025-Q2":655,"2025-Q3":706.3,"2025-Q4":783.8,"2026-Q1":810},
    "TNDM":{"2023-Q1":171.1,"2023-Q2":185.5,"2023-Q3":194.1,"2023-Q4":196.3,"2024-Q1":193.5,"2024-Q2":214.6,"2024-Q3":249.5,"2024-Q4":282.6,"2025-Q1":226,"2025-Q2":207.9,"2025-Q3":290.4,"2025-Q4":290.4,"2026-Q1":260},
    "ABT_LIBRE":{"2023-Q1":1100,"2023-Q2":1200,"2023-Q3":1400,"2023-Q4":1400,"2024-Q1":1500,"2024-Q2":1600,"2024-Q3":1700,"2024-Q4":1800,"2025-Q1":1700,"2025-Q2":1850,"2025-Q3":2000,"2025-Q4":2100,"2026-Q1":2200},
    # BBNX: Beta Bionics 10-K FY2025 total $100.3M. Q4=$32.1M. 2026 guide $130-135M.
    "BBNX":{"2025-Q1":20,"2025-Q2":24,"2025-Q3":24.2,"2025-Q4":32.1,"2026-Q1":32},
    # MDT Diabetes segment. FY ends April. Mapped to cal quarters approximately.
    # Note: MDT is a $33.5B conglomerate; diabetes is ~$2.76B segment. Spinoff announced May 2025.
    "MDT_DM":{"2023-Q1":570,"2023-Q2":580,"2023-Q3":600,"2023-Q4":620,"2024-Q1":620,"2024-Q2":647,"2024-Q3":691,"2024-Q4":694,"2025-Q1":728,"2025-Q2":750,"2025-Q3":770,"2025-Q4":780,"2026-Q1":800},
    "SQEL":{},
}
# Installed base in thousands. Sources in comments.
INSTALLED_BASE_K = {
    "DXCM":{"2023-Q1":2000,"2023-Q2":2100,"2023-Q3":2200,"2023-Q4":2350,"2024-Q1":2500,"2024-Q2":2600,"2024-Q3":2750,"2024-Q4":2900,"2025-Q1":3000,"2025-Q2":3100,"2025-Q3":3250,"2025-Q4":3400,"2026-Q1":3550},# DXCM 10-K: 2.8-2.9M end 2024
    "PODD":{"2023-Q1":380,"2023-Q2":400,"2023-Q3":420,"2023-Q4":440,"2024-Q1":460,"2024-Q2":480,"2024-Q3":510,"2024-Q4":540,"2025-Q1":560,"2025-Q2":575,"2025-Q3":590,"2025-Q4":600,"2026-Q1":620},# PODD Q4 2025 call: >600K
    "TNDM":{"2023-Q1":320,"2023-Q2":330,"2023-Q3":340,"2023-Q4":350,"2024-Q1":355,"2024-Q2":365,"2024-Q3":375,"2024-Q4":390,"2025-Q1":395,"2025-Q2":400,"2025-Q3":410,"2025-Q4":420,"2026-Q1":435},# Est from pump shipments
    "ABT_LIBRE":{"2023-Q1":4500,"2023-Q2":4700,"2023-Q3":4900,"2023-Q4":5100,"2024-Q1":5400,"2024-Q2":5800,"2024-Q3":6300,"2024-Q4":7000,"2025-Q1":7300,"2025-Q2":7600,"2025-Q3":7900,"2025-Q4":8200,"2026-Q1":8500},# Abbott newsroom: ~7M end 2024
    "BBNX":{"2025-Q1":15,"2025-Q2":20,"2025-Q3":27,"2025-Q4":35,"2026-Q1":42},# BBNX 10-K: 35,011 at end 2025
    "MDT_DM":{"2023-Q1":550,"2023-Q2":570,"2023-Q3":590,"2023-Q4":610,"2024-Q1":630,"2024-Q2":660,"2024-Q3":690,"2024-Q4":720,"2025-Q1":750,"2025-Q2":780,"2025-Q3":810,"2025-Q4":840,"2026-Q1":870},# Est from 780G adoption data
    "SQEL":{},
}
STOCK_MONTHLY = {
    "DXCM":{"2023-01":107,"2023-02":112,"2023-03":117,"2023-04":120,"2023-05":118,"2023-06":130,"2023-07":128,"2023-08":100,"2023-09":92,"2023-10":88,"2023-11":116,"2023-12":124,"2024-01":123,"2024-02":129,"2024-03":134,"2024-04":131,"2024-05":112,"2024-06":110,"2024-07":76,"2024-08":72,"2024-09":74,"2024-10":73,"2024-11":80,"2024-12":82,"2025-01":85,"2025-02":80,"2025-03":76,"2025-04":68,"2025-05":72,"2025-06":78,"2025-07":75,"2025-08":70,"2025-09":65,"2025-10":68,"2025-11":74,"2025-12":79,"2026-01":77,"2026-02":80},
    "PODD":{"2023-01":298,"2023-02":291,"2023-03":302,"2023-04":295,"2023-05":283,"2023-06":266,"2023-07":208,"2023-08":195,"2023-09":162,"2023-10":139,"2023-11":179,"2023-12":193,"2024-01":197,"2024-02":191,"2024-03":185,"2024-04":172,"2024-05":184,"2024-06":196,"2024-07":215,"2024-08":220,"2024-09":230,"2024-10":243,"2024-11":253,"2024-12":260,"2025-01":265,"2025-02":257,"2025-03":245,"2025-04":265,"2025-05":270,"2025-06":280,"2025-07":288,"2025-08":290,"2025-09":310,"2025-10":320,"2025-11":315,"2025-12":330,"2026-01":325,"2026-02":335},
    "TNDM":{"2023-01":49,"2023-02":41,"2023-03":37,"2023-04":30,"2023-05":27,"2023-06":24,"2023-07":30,"2023-08":24,"2023-09":22,"2023-10":18,"2023-11":25,"2023-12":23,"2024-01":23,"2024-02":20,"2024-03":19,"2024-04":17,"2024-05":38,"2024-06":42,"2024-07":43,"2024-08":38,"2024-09":38,"2024-10":33,"2024-11":32,"2024-12":29,"2025-01":26,"2025-02":27,"2025-03":20,"2025-04":20,"2025-05":18,"2025-06":19,"2025-07":20,"2025-08":19,"2025-09":18,"2025-10":19,"2025-11":19,"2025-12":19,"2026-01":18,"2026-02":19},
    "ABT_LIBRE":{"2023-01":112,"2023-02":104,"2023-03":101,"2023-04":108,"2023-05":107,"2023-06":109,"2023-07":106,"2023-08":103,"2023-09":98,"2023-10":93,"2023-11":105,"2023-12":110,"2024-01":113,"2024-02":117,"2024-03":118,"2024-04":110,"2024-05":107,"2024-06":104,"2024-07":107,"2024-08":113,"2024-09":114,"2024-10":118,"2024-11":117,"2024-12":116,"2025-01":119,"2025-02":123,"2025-03":128,"2025-04":126,"2025-05":130,"2025-06":133,"2025-07":120,"2025-08":117,"2025-09":119,"2025-10":121,"2025-11":123,"2025-12":126,"2026-01":128,"2026-02":130},
    "BBNX":{"2025-02":17,"2025-03":19,"2025-04":22,"2025-05":20,"2025-06":18,"2025-07":19,"2025-08":21,"2025-09":20,"2025-10":21,"2025-11":22,"2025-12":23,"2026-01":24,"2026-02":25},# IPO Feb 2025
    "MDT_DM":{"2023-01":78,"2023-02":80,"2023-03":82,"2023-04":83,"2023-05":84,"2023-06":86,"2023-07":85,"2023-08":84,"2023-09":82,"2023-10":80,"2023-11":82,"2023-12":84,"2024-01":85,"2024-02":84,"2024-03":86,"2024-04":85,"2024-05":83,"2024-06":82,"2024-07":80,"2024-08":82,"2024-09":84,"2024-10":86,"2024-11":88,"2024-12":87,"2025-01":88,"2025-02":87,"2025-03":89,"2025-04":88,"2025-05":90,"2025-06":91,"2025-07":89,"2025-08":88,"2025-09":87,"2025-10":88,"2025-11":89,"2025-12":90,"2026-01":91,"2026-02":92},# MDT parent stock (diabetes is ~8% of rev)
}
PRODUCT_EVENTS = {
    "DXCM_G7":[{"date":"2023-02","type":"LAUNCH","desc":"G7 10-day sensor launched in US"},{"date":"2025-03","type":"WARNING","desc":"FDA Warning Letter: unauthorized sensor coating change"},{"date":"2025-09","type":"CLASS I","desc":"App software defect prevented sensor failure alerts"},{"date":"2025-10","type":"NEWS","desc":"Hunterbrook: 13+ G7 deaths since 2023; class actions filed"}],
    "DXCM_G7_15DAY":[{"date":"2025-10","type":"LAUNCH","desc":"15-Day sensor launched; ~26% may not last full 15 days"}],
    "DXCM_G6":[{"date":"2025-06","type":"CLASS I","desc":"36,800+ G6 receivers recalled: speaker defect"}],
    "DXCM_ONE":[{"date":"2025-06","type":"CLASS I","desc":"ONE/ONE+ receivers in speaker recall"}],
    "DXCM_ALL":[{"date":"2025-06","type":"CLASS I","desc":"703K+ receivers recalled (speaker malfunction)"},{"date":"2025-03","type":"WARNING","desc":"FDA Warning Letter: G6/G7 CGMs adulterated"}],
    "PODD_OP5":[{"date":"2025-03","type":"CORRECTION","desc":"Voluntary pod correction (self-identified); guidance maintained"}],
    "PODD_ALL":[{"date":"2025-03","type":"CORRECTION","desc":"Pod correction; stock -37% from peak but recovered"}],
    "SQEL_TWIIST":[{"date":"2024-03","type":"FDA CLEAR","desc":"De Novo clearance"},{"date":"2025-07","type":"LAUNCH","desc":"US commercial launch"},{"date":"2026-03","type":"EXPANSION","desc":"Broad US availability"}],
    "TNDM_TSLIM":[{"date":"2025-12","type":"LAUNCH","desc":"t:slim X2 + Libre 3 Plus integration global rollout"}],
    "TNDM_MOBI":[{"date":"2025-11","type":"LAUNCH","desc":"Android mobile control launched"}],
    "BBNX_ILET":[{"date":"2023-05","type":"LAUNCH","desc":"iLet Bionic Pancreas US commercial launch"},{"date":"2025-02","type":"IPO","desc":"Beta Bionics IPO on Nasdaq (BBNX)"}],
    "MDT_780G":[{"date":"2023-04","type":"LAUNCH","desc":"MiniMed 780G + Guardian 4 FDA cleared for US"},{"date":"2025-04","type":"LAUNCH","desc":"Simplera Sync CGM approved for use with 780G"},{"date":"2025-05","type":"NEWS","desc":"Medtronic announces diabetes spinoff into standalone company"}],
}
DEVICES = [
    # DEXCOM
    {"id":"DXCM_G7_15DAY","name":"Dexcom G7 15-Day","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g7+15"',"description":"Latest 15-day CGM. ~26% may not last full 15 days. Very early MAUDE lifecycle."},
    {"id":"DXCM_G7","name":"Dexcom G7 (10-Day)","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g7" AND NOT device.brand_name:"15"',"description":"Primary CGM. KEY RISK. FDA Warning Letter, two Class I recalls, 13+ deaths."},
    {"id":"DXCM_G6","name":"Dexcom G6","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+g6"',"description":"Legacy CGM phasing out. June 2025 receiver recall (36,800+ units)."},
    {"id":"DXCM_STELO","name":"Dexcom Stelo","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:stelo',"description":"First OTC CGM for Type 2 non-insulin users."},
    {"id":"DXCM_ONE","name":"Dexcom ONE/ONE+","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:"dexcom+one"',"description":"Value-tier international CGM. In June 2025 recall."},
    {"id":"DXCM_ALL","name":"All Dexcom","ticker":"DXCM","company":"Dexcom","search":'device.brand_name:dexcom',"description":"Company-level. FY2025: ~$4.7B. ~3.4M users (10-K). 2026 guide: $5.16-5.25B."},
    # INSULET
    {"id":"PODD_OP5","name":"Omnipod 5","ticker":"PODD","company":"Insulet","search":'device.brand_name:"omnipod+5"',"description":"#1 AID pump in US. Self-reported pod correction Mar 2025."},
    {"id":"PODD_DASH","name":"Omnipod DASH","ticker":"PODD","company":"Insulet","search":'device.brand_name:"omnipod+dash"',"description":"Legacy pump declining. Users moving to OP5."},
    {"id":"PODD_ALL","name":"All Omnipod","ticker":"PODD","company":"Insulet","search":'device.brand_name:omnipod',"description":"Company-level. FY2025: ~$2.7B. >600K users (Q4 call)."},
    # TANDEM
    {"id":"TNDM_TSLIM","name":"t:slim X2","ticker":"TNDM","company":"Tandem","search":'device.brand_name:"t:slim"',"description":"Tubed pump with Control-IQ+. Integrates with Libre 3 Plus."},
    {"id":"TNDM_MOBI","name":"Tandem Mobi","ticker":"TNDM","company":"Tandem","search":'device.brand_name:"tandem+mobi"',"description":"Smallest tubed pump. Mobile-first."},
    {"id":"TNDM_ALL","name":"All Tandem","ticker":"TNDM","company":"Tandem","search":'device.brand_name:tandem',"description":"Company-level. FY2025: $1.01B. Cleanest FDA profile."},
    # ABBOTT
    {"id":"ABT_LIBRE3","name":"Libre 3/3+","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre+3"',"description":"DXCM competitor. 14-day. ~7M+ users. Q3 2025: $2.0B quarterly."},
    {"id":"ABT_LIBRE2","name":"Libre 2","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre+2"',"description":"Previous-gen. Being phased out."},
    {"id":"ABT_LIBRE_ALL","name":"All Libre","ticker":"ABT_LIBRE","company":"Abbott","search":'device.brand_name:"freestyle+libre"',"description":"Competitive benchmark. FY2024 Diabetes: $6.8B (~7M users, Abbott newsroom)."},
    # BETA BIONICS
    {"id":"BBNX_ILET","name":"iLet Bionic Pancreas","ticker":"BBNX","company":"Beta Bionics","search":'device.brand_name:"ilet"',"description":"Autonomous AID pump. No carb counting. FY2025: $100.3M (+54%). 35K users (10-K). IPO Feb 2025."},
    {"id":"BBNX_ALL","name":"All Beta Bionics","ticker":"BBNX","company":"Beta Bionics","search":'device.brand_name:"bionic+pancreas" OR device.brand_name:"ilet"',"description":"Company-level. Nasdaq: BBNX. 2026 guide: $130-135M. Developing bihormonal (insulin+glucagon) version."},
    # MEDTRONIC (Diabetes segment — spinoff announced May 2025)
    {"id":"MDT_780G","name":"MiniMed 780G","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:"minimed+780"',"description":"AID pump + Simplera Sync CGM. 6 consecutive quarters of double-digit organic growth."},
    {"id":"MDT_SIMPLERA","name":"Simplera Sync CGM","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:simplera',"description":"Medtronic CGM for 780G. FDA approved Apr 2025. Competes with Dexcom G7 and Libre 3."},
    {"id":"MDT_DM_ALL","name":"All Medtronic Diabetes","ticker":"MDT_DM","company":"Medtronic","search":'device.brand_name:minimed OR device.brand_name:simplera OR device.brand_name:"medtronic+insulin"',"description":"Segment-level (NOT standalone). FY2025 diabetes rev: $2.76B. Spinoff into standalone company announced May 2025. Stock = MDT parent (~8% of revenue)."},
    # SEQUEL
    {"id":"SQEL_TWIIST","name":"twiist AID","ticker":"SQEL","company":"Sequel Med Tech","search":'device.brand_name:twiist',"description":"NEW ENTRANT (private). Tubeless AID, iiSure sound-wave dosing, Libre 3+ and Eversense 365 compatible."},
]

Z_WARN,Z_ELEVATED,Z_CRITICAL = 1.5,2.0,3.0
BASE_URL = "https://api.fda.gov/device/event.json"
COMPANIES = ["Dexcom","Insulet","Tandem","Abbott","Beta Bionics","Medtronic","Sequel Med Tech"]

def _q(s): return url_quote(s, safe='+:"[]')
def api_get(url, retries=3):
    for a in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent":"MAUDE/3.0"}), timeout=30) as r: return json.loads(r.read())
        except:
            if a<retries-1: time.sleep(2**a)
    return None
def fetch_counts(sq, df="date_received", start="20230101"):
    end=datetime.now().strftime("%Y%m%d")
    d=api_get(f"{BASE_URL}?search={_q(sq)}+AND+{df}:[{start}+TO+{end}]&count={df}")
    if not d or "results" not in d: return {}
    c={}
    for r in d["results"]:
        t=r.get("time","")
        if len(t)>=6: ym=f"{t[:4]}-{t[4:6]}"; c[ym]=c.get(ym,0)+r.get("count",0)
    return c
def fetch_severity(sq, start="20230101"):
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

def run_pipeline(backfill=False,quick=False):
    start="20230101" if backfill else ("20250901" if quick else "20230101")
    all_res,summary={},[]
    if HAS_MODULES: print("Enhanced modules loaded"); stats_selftest()
    else: print("BASIC mode")
    for dev in DEVICES:
        did=dev["id"]; print(f"\n{'='*50}\n{dev['name']} ({dev['ticker']})")
        recv=fetch_counts(dev["search"],"date_received",start); time.sleep(0.3)
        evnt=fetch_counts(dev["search"],"date_of_event",start); time.sleep(0.3)
        sev=fetch_severity(dev["search"],start); batch=detect_batch(recv,evnt)
        stats=compute_stats(recv,sev,dev["ticker"]); rscore=compute_r_score(stats) if stats else None
        ec,fm,rd,ed,intl=None,None,None,None,None
        if HAS_MODULES and stats:
            try: ec=compute_enhanced_correlation(recv,STOCK_MONTHLY.get(dev["ticker"],{}),max_lag=6)
            except: pass
            if not did.endswith("_ALL"):
                try: fm=analyze_failure_modes(dev["search"],start,limit=50)
                except: pass
            if did.endswith("_ALL") or did in ("SQEL_TWIIST","BBNX_ILET"):
                try: rd=analyze_reddit_sentiment(dev["ticker"])
                except: pass
            if did.endswith("_ALL"):
                try: ed=analyze_edgar_filings(dev["ticker"])
                except: pass
        all_res[did]={"device":dev,"received":recv,"by_event":evnt,"severity":sev,"batch_flags":batch,"stats":stats,"r_score":rscore,"enhanced_corr":ec,"failure_modes":fm,"reddit":rd,"edgar":ed}
        if stats:
            lt=stats[-1]
            summary.append({"id":did,"name":dev["name"],"ticker":dev["ticker"],"company":dev["company"],"month":lt["month"],"reports":lt["count"],"z_score":lt["z_score"],"rate_per_m":lt["rate_per_m"],"rate_per_10k":lt["rate_per_10k"],"slope_6m":lt["slope_6m"],"deaths_3mo":sum(s["deaths"] for s in stats[-3:]),"injuries_3mo":sum(s["injuries"] for s in stats[-3:]),"r_score":rscore["total"] if rscore else None,"signal":rscore["signal"] if rscore else "NORMAL","batch":batch.get(lt["month"],{}).get("is_batch",False),"corr_rho":ec["best_rho"] if ec else None,"corr_sig":ec["significant"] if ec else None})
            print(f"  {lt['month']} | {lt['count']:,} | Z:{lt['z_score']:+.2f} | R:{rscore['total'] if rscore else '-'}")
    os.makedirs("data",exist_ok=True)
    for did,r in all_res.items():
        if r["stats"]:
            with open(f"data/{did}_monthly.csv","w",newline="") as f:
                w=csv.DictWriter(f,fieldnames=r["stats"][0].keys()); w.writeheader(); w.writerows(r["stats"])
    with open("data/latest_summary.json","w") as f: json.dump({"generated":datetime.now().isoformat(),"devices":summary},f,indent=2)
    return all_res,summary

def generate_html(all_res,summary):
    os.makedirs("docs",exist_ok=True)
    cd={}
    for did,r in all_res.items():
        if not r["stats"]: continue
        cd[did]={"l":[s["month"] for s in r["stats"]],"c":[s["count"] for s in r["stats"]],"ma":[s["ma6"] for s in r["stats"]],"u2":[s["upper_2sd"] for s in r["stats"]],"l2":[s["lower_2sd"] for s in r["stats"]],"u1":[s["upper_1sd"] for s in r["stats"]],"l1":[s["lower_1sd"] for s in r["stats"]],"z":[s["z_score"] for s in r["stats"]],"rm":[s["rate_per_m"] for s in r["stats"]],"r10":[s["rate_per_10k"] for s in r["stats"]],"d":[s["deaths"] for s in r["stats"]],"inj":[s["injuries"] for s in r["stats"]],"mal":[s["malfunctions"] for s in r["stats"]],"dev":r["device"],"rs":r["r_score"],"bm":[m for m,bf in r["batch_flags"].items() if bf.get("is_batch")],"sp":STOCK_MONTHLY.get(r["device"]["ticker"],{}),"evts":PRODUCT_EVENTS.get(did,[])}
    so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3}
    summary.sort(key=lambda x:(so.get(x["signal"],4),-(x["r_score"] or 0)))

    # TABLE ROWS
    trows=""
    for s in summary:
        zc="neg" if s["z_score"]>Z_WARN else "pos" if s["z_score"]<-Z_WARN else ""
        rc="neg" if (s["r_score"] or 0)>=50 else "warn" if (s["r_score"] or 0)>=30 else ""
        bw=' <span class="bw">Batch</span>' if s.get("batch") else ""
        ic="1" if s["id"].endswith("_ALL") else "0"
        cr="—"
        if s.get("corr_rho") is not None:
            cr=f'{s["corr_rho"]:+.3f}'
            if s.get("corr_sig"): cr+=" *"
        d3c="neg" if s["deaths_3mo"]>0 else ""
        trows+=f'<tr class="pr" data-co="{s["company"]}" data-id="{s["id"]}" data-sig="{s["signal"]}" data-comb="{ic}"><td>{s["name"]}{bw}</td><td>{s["ticker"]}</td><td>{s["month"]}</td><td>{fmt0(s["reports"])}</td><td class="{zc}">{s["z_score"]:+.2f}</td><td class="{rc}">{fmt(s["r_score"])}</td><td>{fmt2(s["rate_per_m"])}</td><td>{fmt2(s["rate_per_10k"])}</td><td>{s["slope_6m"]:+.1f}</td><td class="{d3c}">{s["deaths_3mo"]}</td><td>{s["injuries_3mo"]}</td><td>{cr}</td><td><span class="sig sig-{s["signal"]}">{s["signal"]}</span></td></tr>\n'

    # PRODUCT CARDS (grouped by company for tabs)
    company_cards={}
    for comp in COMPANIES:
        company_cards[comp]=""
    for did,r in all_res.items():
        if not r["stats"]: continue
        dv=r["device"]; st=r["stats"]; lt=st[-1]; rs=r["r_score"]; co=dv["company"]
        sig=rs["signal"] if rs else "NORMAL"
        d3=sum(s["deaths"] for s in st[-3:]); i3=sum(s["injuries"] for s in st[-3:])
        evts=PRODUCT_EVENTS.get(did,[])
        ehtml=""
        if evts:
            ehtml='<div class="ebox"><h4>Timeline</h4>'
            for e in evts:
                tc="ew" if "CLASS" in e["type"] or "WARNING" in e["type"] else "eok" if "LAUNCH" in e["type"] or "FDA" in e["type"] or "EXPANSION" in e["type"] or "IPO" in e["type"] else "en"
                ehtml+=f'<div class="evt"><span class="evd">{e["date"]}</span><span class="evt {tc}">{e["type"]}</span> {e["desc"]}</div>'
            ehtml+='</div>'
        rhtml=""
        if rs:
            rcol="#c0392b" if rs["total"]>=50 else "#e67e22" if rs["total"]>=30 else "#27ae60"
            rhtml=f'<div class="rg"><div class="rgv" style="color:{rcol}">{rs["total"]}</div><div class="rgr"><div class="rgl">R-Score (0-100)</div><div class="rgt"><div class="rgf" style="width:{min(100,rs["total"])}%;background:{rcol}"></div></div></div></div>'
            rhtml+=f'<div class="rcg"><div class="rci"><span class="rcv">{rs["z_c"]}</span>Z</div><div class="rci"><span class="rcv">{rs["sev_c"]}</span>Sev</div><div class="rci"><span class="rcv">{rs["gap_c"]}</span>Gap</div><div class="rci"><span class="rcv">{rs["slope_c"]}</span>Slope</div><div class="rci"><span class="rcv">{rs["ib_c"]}</span>IB</div></div>'
        fmhtml=""
        fm=r.get("failure_modes")
        if fm and fm.get("modes"):
            fmhtml='<div class="mbox"><h4>Failure Modes (NLP)</h4>'
            for cat,info in sorted(fm["modes"].items(),key=lambda x:-x[1]["count"]):
                fmhtml+=f'<div class="fmr"><span class="fml">{info["label"]}</span><div class="fmb"><div style="width:{info["pct"]}%;background:var(--g)"></div></div><span class="fmp">{info["pct"]}%</span></div>'
            fmhtml+='</div>'
        rdhtml=""
        rd=r.get("reddit")
        if rd and rd.get("post_count",0)>0:
            scol="#c0392b" if rd["avg_sentiment"]<-0.1 else "#27ae60" if rd["avg_sentiment"]>0.1 else "var(--tx3)"
            rdhtml=f'<div class="mbox"><h4>Reddit Sentiment</h4><div class="sg3"><div class="si"><div class="sil">Posts</div><div class="siv">{rd["post_count"]}</div></div><div class="si"><div class="sil">Sentiment</div><div class="siv" style="color:{scol}">{rd["avg_sentiment"]:+.2f}</div></div><div class="si"><div class="sil">Neg%</div><div class="siv">{rd["negative_pct"]}%</div></div></div></div>'
        echtml=""
        ec=r.get("enhanced_corr")
        if ec: echtml=f'<div class="mbox"><h4>MAUDE-Stock Correlation</h4><div class="msub">{ec.get("interpretation","")[:300]}</div></div>'
        zc2="neg" if lt["z_score"]>1.5 else "pos" if lt["z_score"]<-1.5 else ""
        slc2="neg" if lt["slope_6m"]>0 else "pos"
        d3c2="neg" if d3>0 else ""
        card=f'''<div class="card" data-id="{did}">
<div class="chdr"><div><h3>{dv["name"]}</h3><span class="tk">{dv["ticker"]}{"  (Private)" if dv["ticker"]=="SQEL" else " (Segment)" if dv["ticker"]=="MDT_DM" else ""}</span></div><span class="sig sig-{sig}">{sig}</span></div>
<p class="desc">{dv["description"]}</p>{ehtml}
<div class="sg"><div class="si"><div class="sil">Latest</div><div class="siv">{fmt0(lt["count"])}</div><div class="sis">{lt["month"]}</div></div>
<div class="si"><div class="sil">Z-Score</div><div class="siv {zc2}">{lt["z_score"]:+.2f}</div><div class="sis">12mo avg {fmt0(lt["avg_12m"])}</div></div>
<div class="si"><div class="sil">Rate/$M</div><div class="siv">{fmt2(lt["rate_per_m"])}</div><div class="sis">per $M quarterly rev</div></div>
<div class="si"><div class="sil">Rate/10K Users</div><div class="siv">{fmt2(lt["rate_per_10k"])}</div><div class="sis">per 10K installed base</div></div></div>
<div class="sg"><div class="si"><div class="sil">6mo Trend</div><div class="siv {slc2}">{lt["slope_6m"]:+.1f}/mo</div><div class="sis">regression slope</div></div>
<div class="si"><div class="sil">Deaths (3mo)</div><div class="siv {d3c2}">{d3}</div></div>
<div class="si"><div class="sil">Injuries (3mo)</div><div class="siv">{i3}</div></div>
<div class="si"><div class="sil">Severity</div><div class="siv">{fmt0(lt["severity_score"])}</div><div class="sis">D x10 I x3 M x1</div></div></div>
{rhtml}{fmhtml}{rdhtml}{echtml}
<div class="cc" id="cc-{did}"><button class="cb active" data-v="reports">Reports</button><button class="cb" data-v="rate_m">Rate/$M</button><button class="cb" data-v="rate_10k">Rate/10K</button><button class="cb" data-v="severity">Severity</button><button class="cb" data-v="zscore">Z-Score</button><button class="cb" data-v="stock">Stock</button><button class="cb rst" data-v="reset">Reset</button></div>
<div class="cdesc" id="cdesc-{did}">Click a chart tab above to see data with context.</div>
<div class="cw"><canvas id="ch-{did}"></canvas></div></div>\n'''
        if co in company_cards: company_cards[co]+=card

    # TAB BUTTONS
    tab_ids={"Summary":"summary","Dexcom":"dexcom","Insulet":"insulet","Tandem":"tandem","Abbott":"abbott","Beta Bionics":"bbnx","Medtronic":"medtronic","Sequel Med Tech":"sequel"}
    tab_btns='<div class="tabs">'
    for name,tid in tab_ids.items():
        act=' active' if tid=="summary" else ""
        tab_btns+=f'<button class="tab{act}" onclick="showTab(\'{tid}\')">{name}</button>'
    tab_btns+='</div>'

    modules_str="Stats+NLP+Reddit+EDGAR" if HAS_MODULES else "Basic"
    updated_str=datetime.now().strftime('%b %d, %Y %H:%M ET')

    html_top=f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Monitor V3</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{{--g:#2B5F3A;--gx:#e8f5ec;--gp:#f4faf6;--bg:#fff;--bg2:#f8faf9;--bg3:#f0f3f1;--tx:#1a2a1f;--tx2:#4a5f50;--tx3:#7a8f80;--bd:#d4e0d8;--red:#c0392b;--org:#e67e22}}
*{{margin:0;padding:0;box-sizing:border-box}}body{{background:var(--bg);color:var(--tx);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6}}
.ct{{max-width:1440px;margin:0 auto;padding:24px 32px}}
header{{display:flex;justify-content:space-between;align-items:center;padding:20px 0;border-bottom:2px solid var(--g);margin-bottom:20px}}
header h1{{font-size:22px;font-weight:700;color:var(--g)}}header .sub{{font-size:13px;color:var(--tx2)}}header .meta{{text-align:right;font-size:12px;color:var(--tx3)}}
h2{{font-size:18px;font-weight:700;color:var(--g);margin:20px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--bd)}}
.tabs{{display:flex;gap:2px;margin-bottom:20px;border-bottom:2px solid var(--bd);padding-bottom:0}}
.tab{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-bottom:none;border-radius:6px 6px 0 0;padding:8px 16px;font-size:13px;font-weight:500;cursor:pointer;font-family:inherit}}
.tab:hover{{background:var(--gx)}}
.tab.active{{background:var(--g);color:#fff;border-color:var(--g)}}
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
.chdr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px}}.card h3{{font-size:15px;font-weight:700}}
.tk{{font-size:11px;color:var(--tx3)}}
.desc{{font-size:11px;color:var(--tx2);line-height:1.5;margin-bottom:10px;padding:8px 10px;background:var(--gp);border-radius:6px;border-left:3px solid var(--g)}}
.ebox{{margin-bottom:8px}}.ebox h4,.mbox h4{{font-size:10px;font-weight:600;color:var(--tx3);text-transform:uppercase;margin-bottom:4px}}
.evt{{font-size:10px;color:var(--tx2);padding:3px 0 3px 8px;border-left:2px solid var(--bd);margin-bottom:2px}}
.evd{{font-family:monospace;color:var(--tx3);margin-right:4px}}
.ew{{background:#fdecea;color:var(--red);font-size:8px;font-weight:700;padding:1px 4px;border-radius:3px;margin-right:3px}}
.eok{{background:var(--gx);color:var(--g);font-size:8px;font-weight:700;padding:1px 4px;border-radius:3px;margin-right:3px}}
.en{{background:#eef2f7;color:#2980b9;font-size:8px;font-weight:700;padding:1px 4px;border-radius:3px;margin-right:3px}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:8px}}
.sg3{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:6px}}
.si{{padding:6px 8px;background:var(--bg2);border-radius:6px;border:1px solid var(--bd)}}
.sil{{font-size:8px;font-weight:600;text-transform:uppercase;color:var(--tx3)}}.siv{{font-size:15px;font-weight:700;margin-top:1px}}.sis{{font-size:9px;color:var(--tx3)}}
.rg{{display:flex;align-items:center;gap:12px;padding:8px 10px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;margin-bottom:6px}}
.rgv{{font-size:28px;font-weight:800;min-width:44px}}.rgr{{flex:1}}.rgl{{font-size:10px;color:var(--tx3);margin-bottom:3px}}
.rgt{{height:7px;background:var(--bg3);border-radius:4px;overflow:hidden}}.rgf{{height:100%;border-radius:4px}}
.rcg{{display:grid;grid-template-columns:repeat(5,1fr);gap:3px;margin-bottom:8px}}
.rci{{text-align:center;padding:3px;background:var(--bg2);border-radius:4px;border:1px solid var(--bd);font-size:8px;color:var(--tx3)}}
.rcv{{display:block;font-size:12px;font-weight:700;color:var(--tx)}}
.mbox{{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:10px;margin-bottom:8px}}
.msub{{font-size:11px;color:var(--tx2);margin-top:3px}}
.fmr{{display:flex;align-items:center;gap:6px;margin-bottom:2px}}.fml{{width:130px;font-size:10px;color:var(--tx2)}}
.fmb{{flex:1;height:10px;background:var(--bg3);border-radius:3px;overflow:hidden}}.fmb div{{height:100%;border-radius:3px}}
.fmp{{width:36px;text-align:right;font-weight:600;font-size:11px}}
.cc{{display:flex;gap:3px;flex-wrap:wrap;margin-bottom:4px}}
.cb{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-radius:4px;padding:3px 8px;font-size:10px;font-family:inherit;cursor:pointer;font-weight:500}}
.cb:hover,.cb.active{{background:var(--g);color:#fff;border-color:var(--g)}}
.cb.rst{{margin-left:auto;background:transparent;border-color:var(--bd);color:var(--tx3)}}
.cdesc{{font-size:11px;color:var(--tx2);padding:8px 10px;background:var(--gp);border-radius:6px;margin-bottom:6px;border-left:3px solid var(--g);min-height:40px}}
.cw{{position:relative;height:260px}}
.disc{{margin-top:24px;padding:14px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;font-size:10px;color:var(--tx3);line-height:1.5}}
</style></head><body><div class="ct">
<header><div><h1>MAUDE Monitor</h1><div class="sub">FDA Adverse Event Intelligence — Diabetes Devices</div></div>
<div class="meta">7 Companies | {len(DEVICES)} Products<br>Updated {updated_str}<br>Mode: {modules_str}</div></header>
{tab_btns}
<div class="tabcontent active" id="tc-summary">
<div class="guide"><h3>How to Read This Dashboard</h3><div class="gg">
<div class="gi"><h4>Z-Score</h4><p>Standard deviations from 12-month mean. Above +2.0 = statistically unusual (p~2.3%). WATCH at 1.5, ELEVATED at 2.0, CRITICAL at 3.0. Negative = fewer reports than normal (improving quality OR declining usage).</p></div>
<div class="gi"><h4>R-Score (0-100)</h4><p>Composite risk combining 5 signals (each 0-20): Z-anomaly, severity acceleration, growth gap, 6-month trend slope, and installed-base-adjusted rate. Above 50 = investigate. Above 70 = act. Below 30 = clean.</p></div>
<div class="gi"><h4>Rate/$M Revenue</h4><p>Monthly MAUDE reports divided by monthly revenue (quarterly rev / 3). Normalizes for business SIZE. Rising = quality deteriorating relative to revenue. Differs from Rate/10K when ASP changes (e.g., cheap OTC product like Stelo lowers revenue per user).</p></div>
<div class="gi"><h4>Rate/10K Users</h4><p>Monthly reports divided by (estimated active users / 10,000). Normalizes for INSTALLED BASE. More precise than Rate/$M. A new product with 1K users and 50 reports = 500/10K (catastrophic). A mature product with 3M users and 5K reports = 16.7/10K (may be normal). Sources: earnings calls, 10-K filings.</p></div>
<div class="gi"><h4>6mo Trend (Slope)</h4><p>Linear regression slope of the last 6 months of report counts. Slope of +50 means reports are increasing by ~50/month on average. Positive slope = accelerating problem. Negative = improving or declining base.</p></div>
<div class="gi"><h4>Deaths / Injuries</h4><p>Event counts from the most recent 3-month period. Deaths weighted 10x and injuries 3x in the severity score. These are MAUDE-reported events — not all confirmed as device-caused.</p></div>
<div class="gi"><h4>Batch Detection</h4><p>If date_received is more than 3x date_of_event count for the same month, that month is flagged Batch. This means FDA received a retrospective dump (common after recalls), NOT a real-time surge. Products without this flag have consistent reporting patterns.</p></div>
<div class="gi"><h4>Correlation (Corr)</h4><p>Spearman rank correlation between MAUDE z-scores and stock log-returns at optimal lag (1-6 months). Shows "—" when no stock data exists (private companies) or insufficient history. A * means statistically significant at p&lt;0.05. Negative correlation = rising MAUDE predicts stock decline. Requires stats_engine.py module.</p></div>
</div></div>
<h2>All Products — Latest Month</h2>
<div style="overflow-x:auto"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>6mo Trend</th><th>Deaths (3mo)</th><th>Injuries (3mo)</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div>
</div>'''

    # Company tabs
    for comp in COMPANIES:
        tid=tab_ids[comp]
        cards_html=company_cards.get(comp,"<p>No data available for this company.</p>")
        html_top+=f'\n<div class="tabcontent" id="tc-{tid}"><h2>{comp}</h2><div class="grid">{cards_html}</div></div>'

    html_top+='\n<div class="disc">Disclaimer: Research only. Not investment advice. MAUDE has known limitations. Revenue from SEC filings. Installed base from earnings calls. Stock prices approximate monthly closes. MDT stock reflects parent company (~8% diabetes revenue); spinoff pending. BBNX stock from Feb 2025 (IPO). Sequel Med Tech is private. Correlation is not causation.</div></div>'

    # JAVASCRIPT — completely separate, no f-string
    js=r'''<script>
var defined_cd=__CD__;
var charts={};
var chartDescs={
"reports":"REPORTS + SIGMA BANDS: Each bar shows total MAUDE adverse event reports received by the FDA that month. The shaded bands show the normal range: light green = +/-1 standard deviation (68% of months fall here), outer boundary = +/-2 standard deviations (95% fall here). Bars beyond the 2-sigma band are statistically anomalous. Orange bars = batch reporting detected (retrospective filing dump). Red bars = regulatory event that month.",
"rate_m":"RATE PER $M REVENUE: Monthly MAUDE reports divided by monthly revenue (quarterly revenue / 3). This normalizes for company growth. If a company doubles revenue, reports should roughly double too. A RISING rate means quality is deteriorating faster than the business is growing. This differs from Rate/10K when average selling price changes.",
"rate_10k":"RATE PER 10,000 USERS: Monthly MAUDE reports divided by estimated active installed base (in units of 10,000). This is the most precise normalization because it accounts for actual users, not revenue. A new product with few users will show high Rate/10K even with low absolute reports. User estimates sourced from earnings calls and 10-K filings.",
"severity":"SEVERITY BREAKDOWN: Stacked bars showing the mix of event types each month. Deaths (red) are the most serious. Injuries (orange) include hospitalizations, ER visits, and serious health consequences. Malfunctions (green) are device failures that did not directly harm a patient. A shift toward more deaths/injuries relative to malfunctions is a worsening signal.",
"zscore":"Z-SCORE HISTORY: Each bar shows how far that month's report count deviated from the trailing 12-month average, measured in standard deviations. Bars above the +2 line (red dashed) are statistically unusual spikes. Bars below -2 (green dashed) indicate unusually LOW reporting. Sustained positive Z-scores suggest a developing quality problem.",
"stock":"STOCK PRICE OVERLAY: Green line = monthly stock close (left axis). Red line = monthly MAUDE report count (right axis). Look for patterns where MAUDE spikes (red rising) precede stock declines (green falling) by 1-4 months. That lead time is the potential alpha window. For segment stocks (MDT), price reflects the parent conglomerate."
};
function showTab(id){
  var tabs=document.querySelectorAll(".tab");for(var i=0;i<tabs.length;i++){tabs[i].classList.remove("active");}
  var tcs=document.querySelectorAll(".tabcontent");for(var i=0;i<tcs.length;i++){tcs[i].classList.remove("active");}
  var clickedTab=document.querySelector('.tab[onclick*="'+id+'"]');if(clickedTab)clickedTab.classList.add("active");
  var tc=document.getElementById("tc-"+id);if(tc)tc.classList.add("active");
}
function init(){
  for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}
  var allcc=document.querySelectorAll(".cc");
  for(var ci=0;ci<allcc.length;ci++){
    var btns=allcc[ci].querySelectorAll(".cb");
    for(var bi=0;bi<btns.length;bi++){
      btns[bi].addEventListener("click",function(){
        var mycc=this.parentNode;var did=mycc.id.replace("cc-","");var v=this.getAttribute("data-v");
        if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}
        var siblings=mycc.querySelectorAll(".cb:not(.rst)");
        for(var si=0;si<siblings.length;si++){siblings[si].classList.remove("active");}
        this.classList.add("active");
        var descEl=document.getElementById("cdesc-"+did);
        if(descEl&&chartDescs[v]){descEl.textContent=chartDescs[v];}
        mk(did,defined_cd[did],v);
      });
    }
  }
}
function mk(did,D,v){
  var ctx=document.getElementById("ch-"+did);if(!ctx)return;if(charts[did])charts[did].destroy();
  var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];
  var evtMs=[];for(var ei=0;ei<evts.length;ei++){evtMs.push(evts[ei].date);}
  if(v==="reports"){
    var bc=[];for(var bi=0;bi<D.l.length;bi++){bc.push(bm.indexOf(D.l[bi])>=0?"rgba(230,126,34,0.5)":evtMs.indexOf(D.l[bi])>=0?"rgba(192,57,43,0.4)":"rgba(43,95,58,0.25)");}
    ds=[{label:"2s upper",data:D.u2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:"+1",pointRadius:0,order:5},{label:"2s lower",data:D.l2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:false,pointRadius:0,order:5},{label:"1s upper",data:D.u1,borderWidth:0,backgroundColor:"rgba(43,95,58,0.10)",fill:"+1",pointRadius:0,order:4},{label:"1s lower",data:D.l1,borderWidth:0,fill:false,pointRadius:0,order:4},{label:"Reports",data:D.c,borderColor:"rgba(43,95,58,0.85)",backgroundColor:bc,borderWidth:1.5,type:"bar",order:2},{label:"6mo MA",data:D.ma,borderColor:"#2B5F3A",borderWidth:2.5,fill:false,pointRadius:0,tension:0.3,order:1}];yL="Monthly Reports";
  }else if(v==="rate_m"){
    var rd=[];for(var i=0;i<D.rm.length;i++){rd.push(D.rm[i]===null?undefined:D.rm[i]);}
    ds=[{label:"Rate/$M",data:rd,borderColor:"#2B5F3A",backgroundColor:"rgba(43,95,58,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per $M Revenue";
  }else if(v==="rate_10k"){
    var r10=[];for(var i=0;i<D.r10.length;i++){r10.push(D.r10[i]===null?undefined:D.r10[i]);}
    ds=[{label:"Rate/10K Users",data:r10,borderColor:"#8B4513",backgroundColor:"rgba(139,69,19,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per 10K Users";
  }else if(v==="severity"){
    ds=[{label:"Deaths",data:D.d,backgroundColor:"rgba(192,57,43,0.8)",borderWidth:0,stack:"s"},{label:"Injuries",data:D.inj,backgroundColor:"rgba(230,126,34,0.7)",borderWidth:0,stack:"s"},{label:"Malfunctions",data:D.mal,backgroundColor:"rgba(43,95,58,0.3)",borderWidth:0,stack:"s"}];yL="Events by Type";
  }else if(v==="zscore"){
    var zc=[];for(var i=0;i<D.z.length;i++){var zv=D.z[i];zc.push(zv>2?"rgba(192,57,43,0.8)":zv>1.5?"rgba(230,126,34,0.7)":zv<-1.5?"rgba(43,95,58,0.6)":"rgba(43,95,58,0.25)");}
    var t2=[];var nt2=[];for(var i=0;i<D.l.length;i++){t2.push(2);nt2.push(-2);}
    ds=[{label:"Z-Score",data:D.z,backgroundColor:zc,borderWidth:0,type:"bar"},{label:"+2s",data:t2,borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false},{label:"-2s",data:nt2,borderColor:"rgba(43,95,58,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false}];yL="Z-Score";
  }else if(v==="stock"){
    var sp=D.sp||{};var sl=[];var sv=[];var sc=[];
    for(var i=0;i<D.l.length;i++){if(sp[D.l[i]]){sl.push(D.l[i]);sv.push(sp[D.l[i]]);sc.push(D.c[i]);}}
    if(sl.length<2){var descEl=document.getElementById("cdesc-"+did);if(descEl)descEl.textContent="Limited stock data available for this product. Stock may not exist yet (private company) or product launched recently.";return;}
    ds=[{label:"Stock ($)",data:sv,borderColor:"#2B5F3A",borderWidth:2,fill:false,pointRadius:1.5,tension:0.2},{label:"MAUDE Reports",data:sc,borderColor:"rgba(192,57,43,0.6)",borderWidth:1.5,fill:false,pointRadius:0,tension:0.2,yAxisID:"y1"}];
    charts[did]=new Chart(ctx,{type:"line",data:{labels:sl,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{position:"left",grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#2B5F3A",font:{size:10}},title:{display:true,text:"Stock ($)",color:"#2B5F3A",font:{size:11}}},y1:{position:"right",grid:{drawOnChartArea:false},ticks:{color:"#c0392b",font:{size:10}},title:{display:true,text:"MAUDE Reports",color:"#c0392b",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1}}}});return;
  }
  charts[did]=new Chart(ctx,{type:"line",data:{labels:D.l,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},y:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#4a5f50",font:{size:10}},title:{display:true,text:yL,color:"#4a5f50",font:{size:11}}}},plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},pinch:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1,callbacks:{afterBody:function(it){var idx=it[0].dataIndex;var month=D.l[idx];var msgs=[];if(bm.indexOf(month)>=0){msgs.push("BATCH REPORTING DETECTED");}for(var ee=0;ee<evts.length;ee++){if(evts[ee].date===month){msgs.push(evts[ee].type+": "+evts[ee].desc);}}return msgs.length?"\n"+msgs.join("\n"):"";}}}}}}); 
}
document.addEventListener("DOMContentLoaded",init);
</script>'''

    full_html=html_top+js+"</body></html>"
    full_html=full_html.replace("__CD__",json.dumps(cd))
    with open("docs/index.html","w") as f: f.write(full_html)
    print(f"\nDashboard: docs/index.html ({len(full_html)//1024}KB)")

def send_alerts(summary):
    to,fr,pw=os.environ.get("MAUDE_EMAIL_TO"),os.environ.get("MAUDE_EMAIL_FROM"),os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl=[s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body="MAUDE Monitor Alert\n\n"
    for s in fl: body+=f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg=MIMEMultipart();msg["From"],msg["To"]=fr,to;msg["Subject"]=f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv: srv.starttls();srv.login(fr,pw);srv.send_message(msg)
    except: pass

def main():
    p=argparse.ArgumentParser();p.add_argument("--html",action="store_true");p.add_argument("--backfill",action="store_true");p.add_argument("--quick",action="store_true")
    a=p.parse_args()
    print(f"MAUDE Monitor V3.0 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} devices | Modules: {'YES' if HAS_MODULES else 'NO'}")
    r,s=run_pipeline(a.backfill,a.quick); generate_html(r,s); send_alerts(s)
    print(f"\nCOMPLETE | data/latest_summary.json | docs/index.html")

if __name__=="__main__": main()
