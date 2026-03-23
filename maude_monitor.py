#!/usr/bin/env python3
"""
MAUDE Monitor V2.3 — FDA Adverse Event Intelligence
Full pipeline with enhanced modules support.
"""
import json,os,time,math,argparse,smtplib,csv,re
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

try:
    from stats_engine import compute_enhanced_correlation, _self_test as stats_selftest
    from data_modules import (analyze_failure_modes, analyze_reddit_sentiment,
                              analyze_edgar_filings, analyze_international)
    HAS_MODULES = True
except ImportError:
    HAS_MODULES = False

def fmt(v, d=1):
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
def fmtpct(v):
    if v is None: return "—"
    return f"{v:+.2f}"

QUARTERLY_REVENUE = {
    "DXCM": {"2023-Q1":921,"2023-Q2":871.3,"2023-Q3":975,"2023-Q4":1010,"2024-Q1":921,"2024-Q2":1004,"2024-Q3":994.2,"2024-Q4":1115,"2025-Q1":1036,"2025-Q2":1092,"2025-Q3":1174,"2025-Q4":1260,"2026-Q1":1270},
    "PODD": {"2023-Q1":412.5,"2023-Q2":432.1,"2023-Q3":476,"2023-Q4":521.5,"2024-Q1":481.5,"2024-Q2":530.4,"2024-Q3":543.9,"2024-Q4":597.7,"2025-Q1":555,"2025-Q2":655,"2025-Q3":706.3,"2025-Q4":783.8,"2026-Q1":810},
    "TNDM": {"2023-Q1":171.1,"2023-Q2":185.5,"2023-Q3":194.1,"2023-Q4":196.3,"2024-Q1":193.5,"2024-Q2":214.6,"2024-Q3":249.5,"2024-Q4":282.6,"2025-Q1":226,"2025-Q2":207.9,"2025-Q3":290.4,"2025-Q4":290.4,"2026-Q1":260},
    "ABT_LIBRE": {"2023-Q1":1100,"2023-Q2":1200,"2023-Q3":1400,"2023-Q4":1400,"2024-Q1":1500,"2024-Q2":1600,"2024-Q3":1700,"2024-Q4":1800,"2025-Q1":1700,"2025-Q2":1850,"2025-Q3":2000,"2025-Q4":2100,"2026-Q1":2200},
    "SQEL": {},
}
INSTALLED_BASE_K = {
    "DXCM": {"2023-Q1":2000,"2023-Q2":2100,"2023-Q3":2200,"2023-Q4":2350,"2024-Q1":2500,"2024-Q2":2600,"2024-Q3":2750,"2024-Q4":2900,"2025-Q1":3000,"2025-Q2":3100,"2025-Q3":3250,"2025-Q4":3400,"2026-Q1":3550},
    "PODD": {"2023-Q1":380,"2023-Q2":400,"2023-Q3":420,"2023-Q4":440,"2024-Q1":460,"2024-Q2":480,"2024-Q3":510,"2024-Q4":540,"2025-Q1":560,"2025-Q2":575,"2025-Q3":590,"2025-Q4":600,"2026-Q1":620},
    "TNDM": {"2023-Q1":320,"2023-Q2":330,"2023-Q3":340,"2023-Q4":350,"2024-Q1":355,"2024-Q2":365,"2024-Q3":375,"2024-Q4":390,"2025-Q1":395,"2025-Q2":400,"2025-Q3":410,"2025-Q4":420,"2026-Q1":435},
    "ABT_LIBRE": {"2023-Q1":4500,"2023-Q2":4700,"2023-Q3":4900,"2023-Q4":5100,"2024-Q1":5400,"2024-Q2":5800,"2024-Q3":6300,"2024-Q4":7000,"2025-Q1":7300,"2025-Q2":7600,"2025-Q3":7900,"2025-Q4":8200,"2026-Q1":8500},
    "SQEL": {},
}
STOCK_MONTHLY = {
    "DXCM":{"2023-01":107,"2023-02":112,"2023-03":117,"2023-04":120,"2023-05":118,"2023-06":130,"2023-07":128,"2023-08":100,"2023-09":92,"2023-10":88,"2023-11":116,"2023-12":124,"2024-01":123,"2024-02":129,"2024-03":134,"2024-04":131,"2024-05":112,"2024-06":110,"2024-07":76,"2024-08":72,"2024-09":74,"2024-10":73,"2024-11":80,"2024-12":82,"2025-01":85,"2025-02":80,"2025-03":76,"2025-04":68,"2025-05":72,"2025-06":78,"2025-07":75,"2025-08":70,"2025-09":65,"2025-10":68,"2025-11":74,"2025-12":79},
    "PODD":{"2023-01":298,"2023-02":291,"2023-03":302,"2023-04":295,"2023-05":283,"2023-06":266,"2023-07":208,"2023-08":195,"2023-09":162,"2023-10":139,"2023-11":179,"2023-12":193,"2024-01":197,"2024-02":191,"2024-03":185,"2024-04":172,"2024-05":184,"2024-06":196,"2024-07":215,"2024-08":220,"2024-09":230,"2024-10":243,"2024-11":253,"2024-12":260,"2025-01":265,"2025-02":257,"2025-03":245,"2025-04":265,"2025-05":270,"2025-06":280,"2025-07":288,"2025-08":290,"2025-09":310,"2025-10":320,"2025-11":315,"2025-12":330},
    "TNDM":{"2023-01":49,"2023-02":41,"2023-03":37,"2023-04":30,"2023-05":27,"2023-06":24,"2023-07":30,"2023-08":24,"2023-09":22,"2023-10":18,"2023-11":25,"2023-12":23,"2024-01":23,"2024-02":20,"2024-03":19,"2024-04":17,"2024-05":38,"2024-06":42,"2024-07":43,"2024-08":38,"2024-09":38,"2024-10":33,"2024-11":32,"2024-12":29,"2025-01":26,"2025-02":27,"2025-03":20,"2025-04":20,"2025-05":18,"2025-06":19,"2025-07":20,"2025-08":19,"2025-09":18,"2025-10":19,"2025-11":19,"2025-12":19},
    "ABT_LIBRE":{"2023-01":112,"2023-02":104,"2023-03":101,"2023-04":108,"2023-05":107,"2023-06":109,"2023-07":106,"2023-08":103,"2023-09":98,"2023-10":93,"2023-11":105,"2023-12":110,"2024-01":113,"2024-02":117,"2024-03":118,"2024-04":110,"2024-05":107,"2024-06":104,"2024-07":107,"2024-08":113,"2024-09":114,"2024-10":118,"2024-11":117,"2024-12":116,"2025-01":119,"2025-02":123,"2025-03":128,"2025-04":126,"2025-05":130,"2025-06":133,"2025-07":120,"2025-08":117,"2025-09":119,"2025-10":121,"2025-11":123,"2025-12":126},
}
PRODUCT_EVENTS = {
    "DXCM_G7":[{"date":"2023-02","type":"LAUNCH","desc":"G7 10-day sensor launched in US"},{"date":"2025-03","type":"WARNING","desc":"FDA Warning Letter — unauthorized sensor coating change"},{"date":"2025-09","type":"CLASS I","desc":"Class I Recall — app software defect prevented sensor failure alerts"},{"date":"2025-10","type":"NEWS","desc":"Hunterbrook: 13+ G7 user deaths since 2023; class action lawsuits filed"}],
    "DXCM_G7_15DAY":[{"date":"2025-10","type":"LAUNCH","desc":"G7 15-Day initial launch; ~26% may not last full 15 days per labeling"}],
    "DXCM_G6":[{"date":"2025-06","type":"CLASS I","desc":"36,800+ G6 receivers recalled — speaker defect causing missed alerts"}],
    "DXCM_ONE":[{"date":"2025-06","type":"CLASS I","desc":"ONE/ONE+ receivers included in speaker defect recall"}],
    "DXCM_ALL":[{"date":"2025-06","type":"CLASS I","desc":"703,687+ receivers recalled across G7/G6/ONE — speaker malfunction"},{"date":"2025-03","type":"WARNING","desc":"FDA Warning Letter — facilities found G6/G7 CGMs adulterated"}],
    "PODD_OP5":[{"date":"2025-03","type":"CORRECTION","desc":"Voluntary pod correction — self-identified by Insulet; guidance maintained"}],
    "PODD_ALL":[{"date":"2025-03","type":"CORRECTION","desc":"Pod correction; stock down ~37% from peak but recovered"}],
    "SQEL_TWIIST":[{"date":"2024-03","type":"FDA CLEAR","desc":"FDA De Novo clearance"},{"date":"2025-07","type":"LAUNCH","desc":"US commercial launch"},{"date":"2026-03","type":"EXPANSION","desc":"Broad US availability announced"}],
    "TNDM_TSLIM":[{"date":"2025-12","type":"LAUNCH","desc":"Global rollout of t:slim X2 + Abbott Libre 3 Plus integration"}],
    "TNDM_MOBI":[{"date":"2025-11","type":"LAUNCH","desc":"Android mobile control launched"}],
}
DEVICES = [
    {"id":"DXCM_G7_15DAY","name":"Dexcom G7 15-Day","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:"dexcom+g7+15"',"description":"Latest 15-day wear CGM. ~26% may not last full 15 days per labeling. Very early MAUDE lifecycle."},
    {"id":"DXCM_G7","name":"Dexcom G7 (10-Day)","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:"dexcom+g7" AND NOT device.brand_name:"15"',"description":"Primary CGM. KEY RISK PRODUCT. FDA Warning Letter (Mar 2025), two Class I recalls, 13+ deaths reported."},
    {"id":"DXCM_G6","name":"Dexcom G6","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:"dexcom+g6"',"description":"Legacy CGM being phased out. June 2025 receiver recall (36,800+ units)."},
    {"id":"DXCM_STELO","name":"Dexcom Stelo","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:stelo',"description":"First OTC CGM for Type 2 non-insulin users."},
    {"id":"DXCM_ONE","name":"Dexcom ONE/ONE+","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:"dexcom+one"',"description":"Value-tier international CGM. Included in June 2025 recall."},
    {"id":"DXCM_ALL","name":"All Dexcom (combined)","ticker":"DXCM","company":"Dexcom","group_id":"DXCM","search":'device.brand_name:dexcom',"description":"Company-level. FY2025: ~$4.7B. ~3.4M users. 2026 guide: $5.16-5.25B."},
    {"id":"PODD_OP5","name":"Omnipod 5","ticker":"PODD","company":"Insulet","group_id":"PODD","search":'device.brand_name:"omnipod+5"',"description":"#1 AID pump in US new starts. Self-reported pod correction Mar 2025."},
    {"id":"PODD_DASH","name":"Omnipod DASH","ticker":"PODD","company":"Insulet","group_id":"PODD","search":'device.brand_name:"omnipod+dash"',"description":"Legacy tubeless pump. Declining as users move to Omnipod 5."},
    {"id":"PODD_ALL","name":"All Omnipod (combined)","ticker":"PODD","company":"Insulet","group_id":"PODD","search":'device.brand_name:omnipod',"description":"Company-level. FY2025: ~$2.7B (31% growth). >600K users."},
    {"id":"TNDM_TSLIM","name":"Tandem t:slim X2","ticker":"TNDM","company":"Tandem","group_id":"TNDM","search":'device.brand_name:"t:slim"',"description":"Tubed pump with Control-IQ+. Now integrates with Abbott Libre 3 Plus."},
    {"id":"TNDM_MOBI","name":"Tandem Mobi","ticker":"TNDM","company":"Tandem","group_id":"TNDM","search":'device.brand_name:"tandem+mobi"',"description":"Smallest tubed pump. Mobile-first. Pharmacy sales nearly doubled Q3-Q4 2025."},
    {"id":"TNDM_ALL","name":"All Tandem (combined)","ticker":"TNDM","company":"Tandem","group_id":"TNDM","search":'device.brand_name:tandem',"description":"Company-level. FY2025: $1.01B. Cleanest FDA profile of the three."},
    {"id":"ABT_LIBRE3","name":"FreeStyle Libre 3/3+","ticker":"ABT_LIBRE","company":"Abbott","group_id":"ABT_LIBRE","search":'device.brand_name:"freestyle+libre+3"',"description":"DXCM competitor. 14-day wear. ~7M+ users. Q3 2025: $2.0B quarterly."},
    {"id":"ABT_LIBRE2","name":"FreeStyle Libre 2","ticker":"ABT_LIBRE","company":"Abbott","group_id":"ABT_LIBRE","search":'device.brand_name:"freestyle+libre+2"',"description":"Previous-gen Libre. Being phased out internationally."},
    {"id":"ABT_LIBRE_ALL","name":"All FreeStyle Libre","ticker":"ABT_LIBRE","company":"Abbott","group_id":"ABT_LIBRE","search":'device.brand_name:"freestyle+libre"',"description":"Competitive benchmark. FY2024 Diabetes: $6.8B."},
    {"id":"SQEL_TWIIST","name":"twiist AID System","ticker":"SQEL","company":"Sequel Med Tech","group_id":"SQEL","search":'device.brand_name:twiist',"description":"NEW ENTRANT (private). Tubeless AID with iiSure sound-wave dosing."},
]
Z_WARN,Z_ELEVATED,Z_CRITICAL = 1.5,2.0,3.0
BASE_URL = "https://api.fda.gov/device/event.json"

def _q(s): return url_quote(s, safe='+:"[]')
def api_get(url, retries=3):
    for a in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent":"MAUDE/2.3"}), timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if a < retries-1: time.sleep(2**a)
            else: print(f"  API FAIL: {e}"); return None
def fetch_counts(sq, df="date_received", start="20230101"):
    end = datetime.now().strftime("%Y%m%d")
    d = api_get(f"{BASE_URL}?search={_q(sq)}+AND+{df}:[{start}+TO+{end}]&count={df}")
    if not d or "results" not in d: return {}
    c = {}
    for r in d["results"]:
        t = r.get("time","")
        if len(t)>=6: ym=f"{t[:4]}-{t[4:6]}"; c[ym]=c.get(ym,0)+r.get("count",0)
    return c
def fetch_severity(sq, start="20230101"):
    end = datetime.now().strftime("%Y%m%d")
    sv = {}
    for et in ["death","injury","malfunction"]:
        d = api_get(f"{BASE_URL}?search={_q(sq)}+AND+date_received:[{start}+TO+{end}]+AND+event_type:{et}&count=date_received")
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
def compute_stats(md, sv, tk, w=12):
    ms = sorted(md.keys())
    if len(ms)<3: return []
    res = []
    for i,m in enumerate(ms):
        c=md[m]; tr=[md[ms[j]] for j in range(max(0,i-w+1),i+1)]
        avg=sum(tr)/len(tr); sd=(sum((x-avg)**2 for x in tr)/len(tr))**.5 if len(tr)>1 else 0
        z=(c-avg)/sd if sd>0 else 0
        ma6v=sum(tr[-6:])/len(tr[-6:]) if len(tr)>=6 else sum(tr)/len(tr)
        s=sv.get(m,{"death":0,"injury":0,"malfunction":0})
        ss=s.get("death",0)*10+s.get("injury",0)*3+s.get("malfunction",0)
        q=m2q(m)
        rv=QUARTERLY_REVENUE.get(tk,{}).get(q); rpm=round(c/(rv/3),2) if rv else None
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
    lt=sl[-1]
    zc=min(20,abs(lt["z_score"])*6.67)
    rs=sum(s["severity_score"] for s in sl[-3:])/3
    ps=sum(s["severity_score"] for s in sl[-6:-3])/3 if len(sl)>=6 else rs
    sc=min(20,max(0,(rs/ps-1)*40)) if ps>0 else 10
    rr=[s["rate_per_m"] for s in sl[-3:] if s["rate_per_m"]]
    pr=[s["rate_per_m"] for s in sl[-6:-3] if s["rate_per_m"]]
    gc=min(20,max(0,((sum(rr)/len(rr))/(sum(pr)/len(pr))-1)*80)) if rr and pr and sum(pr)/len(pr)>0 else 10
    sp=lt["slope_6m"]/lt["avg_12m"]*100 if lt["avg_12m"]>0 else 0
    slc=min(20,max(0,sp*2))
    ri=[s["rate_per_10k"] for s in sl[-3:] if s["rate_per_10k"]]
    pi=[s["rate_per_10k"] for s in sl[-6:-3] if s["rate_per_10k"]]
    ic=min(20,max(0,((sum(ri)/len(ri))/(sum(pi)/len(pi))-1)*80)) if ri and pi and sum(pi)/len(pi)>0 else 10
    t=min(100,zc+sc+gc+slc+ic)
    return {"total":round(t,1),"z_c":round(zc,1),"sev_c":round(sc,1),"gap_c":round(gc,1),"slope_c":round(slc,1),"ib_c":round(ic,1),"signal":"CRITICAL" if t>=70 else "ELEVATED" if t>=50 else "WATCH" if t>=30 else "NORMAL"}
def detect_batch(recv, evnt):
    f={}
    for m in recv:
        r=recv.get(m,0); e=evnt.get(m,0); ratio=r/e if e>0 else None
        f[m]={"is_batch":(ratio or 0)>3,"ratio":round(ratio,2) if ratio else None}
    return f

def run_pipeline(backfill=False, quick=False):
    start = "20230101" if backfill else ("20250901" if quick else "20230101")
    all_res, summary = {}, []
    if HAS_MODULES:
        print("Enhanced modules loaded"); stats_selftest()
    else:
        print("Running BASIC mode (no stats_engine/data_modules)")
    for dev in DEVICES:
        did = dev["id"]
        print(f"\n{'='*50}\n{dev['name']} ({dev['ticker']})")
        recv = fetch_counts(dev["search"], "date_received", start); time.sleep(0.3)
        evnt = fetch_counts(dev["search"], "date_of_event", start); time.sleep(0.3)
        sev = fetch_severity(dev["search"], start)
        batch = detect_batch(recv, evnt)
        stats = compute_stats(recv, sev, dev["ticker"])
        rscore = compute_r_score(stats) if stats else None
        enhanced_corr, failure_modes, reddit_data, edgar_data, intl_data = None, None, None, None, None
        if HAS_MODULES and stats:
            try: enhanced_corr = compute_enhanced_correlation(recv, STOCK_MONTHLY.get(dev["ticker"],{}), max_lag=6)
            except Exception as e: print(f"  Corr error: {e}")
            if not did.endswith("_ALL"):
                try: failure_modes = analyze_failure_modes(dev["search"], start, limit=50)
                except Exception as e: print(f"  NLP error: {e}")
            if did.endswith("_ALL") or did == "SQEL_TWIIST":
                try: reddit_data = analyze_reddit_sentiment(dev["ticker"])
                except Exception as e: print(f"  Reddit error: {e}")
            if did.endswith("_ALL"):
                try: edgar_data = analyze_edgar_filings(dev["ticker"])
                except Exception as e: print(f"  EDGAR error: {e}")
                try: intl_data = analyze_international(dev["ticker"], [dev["name"].split("(")[0].strip().split(" ")[0]])
                except Exception as e: print(f"  Intl error: {e}")
        all_res[did] = {"device":dev,"received":recv,"by_event":evnt,"severity":sev,"batch_flags":batch,"stats":stats,"r_score":rscore,"enhanced_corr":enhanced_corr,"failure_modes":failure_modes,"reddit":reddit_data,"edgar":edgar_data,"international":intl_data}
        if stats:
            lt = stats[-1]; ec = enhanced_corr
            summary.append({"id":did,"name":dev["name"],"ticker":dev["ticker"],"company":dev["company"],"month":lt["month"],"reports":lt["count"],"z_score":lt["z_score"],"rate_per_m":lt["rate_per_m"],"rate_per_10k":lt["rate_per_10k"],"slope_6m":lt["slope_6m"],"deaths_3mo":sum(s["deaths"] for s in stats[-3:]),"injuries_3mo":sum(s["injuries"] for s in stats[-3:]),"r_score":rscore["total"] if rscore else None,"signal":rscore["signal"] if rscore else "NORMAL","batch":batch.get(lt["month"],{}).get("is_batch",False),"corr_rho":ec["best_rho"] if ec else None,"corr_p":ec["best_p"] if ec else None,"corr_sig":ec["significant"] if ec else None})
            print(f"  {lt['month']} | {lt['count']:,} rpts | Z:{lt['z_score']:+.2f} | R:{rscore['total'] if rscore else 'n/a'}")
    os.makedirs("data", exist_ok=True)
    for did, r in all_res.items():
        if r["stats"]:
            with open(f"data/{did}_monthly.csv","w",newline="") as f:
                w=csv.DictWriter(f, fieldnames=r["stats"][0].keys()); w.writeheader(); w.writerows(r["stats"])
    with open("data/latest_summary.json","w") as f:
        json.dump({"generated":datetime.now().isoformat(),"devices":summary}, f, indent=2)
    return all_res, summary

def generate_html(all_res, summary):
    os.makedirs("docs", exist_ok=True)
    cd = {}
    for did, r in all_res.items():
        if not r["stats"]: continue
        cd[did] = {"l":[s["month"] for s in r["stats"]],"c":[s["count"] for s in r["stats"]],"ma":[s["ma6"] for s in r["stats"]],"u2":[s["upper_2sd"] for s in r["stats"]],"l2":[s["lower_2sd"] for s in r["stats"]],"u1":[s["upper_1sd"] for s in r["stats"]],"l1":[s["lower_1sd"] for s in r["stats"]],"z":[s["z_score"] for s in r["stats"]],"rm":[s["rate_per_m"] for s in r["stats"]],"r10":[s["rate_per_10k"] for s in r["stats"]],"d":[s["deaths"] for s in r["stats"]],"inj":[s["injuries"] for s in r["stats"]],"mal":[s["malfunctions"] for s in r["stats"]],"dev":r["device"],"rs":r["r_score"],"bm":[m for m,bf in r["batch_flags"].items() if bf.get("is_batch")],"sp":STOCK_MONTHLY.get(r["device"]["ticker"],{}),"evts":PRODUCT_EVENTS.get(did,[])}
    so = {"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3}
    summary.sort(key=lambda x:(so.get(x["signal"],4),-(x["r_score"] or 0)))
    companies = sorted(set(d["company"] for d in DEVICES))

    # TABLE ROWS
    trows = ""
    for s in summary:
        zc="neg" if s["z_score"]>Z_WARN else "pos" if s["z_score"]<-Z_WARN else ""
        rc="neg" if (s["r_score"] or 0)>=50 else "warn" if (s["r_score"] or 0)>=30 else ""
        bw=' <span class="bw">⚠ Batch</span>' if s.get("batch") else ""
        co=next((d["company"] for d in DEVICES if d["id"]==s["id"]),"")
        ic="1" if s["id"].endswith("_ALL") else "0"
        cd_str = "—"
        cd_cls = ""
        if s.get("corr_rho") is not None:
            cd_str = f'{s["corr_rho"]:+.3f}'
            if s.get("corr_sig"): cd_cls = "neg" if s["corr_rho"]<0 else "pos"; cd_str += " *"
        d3c = "neg" if s["deaths_3mo"]>0 else ""
        trows += f'<tr class="pr" data-co="{co}" data-id="{s["id"]}" data-sig="{s["signal"]}" data-comb="{ic}"><td>{s["name"]}{bw}</td><td>{s["ticker"]}</td><td>{s["month"]}</td><td>{fmt0(s["reports"])}</td><td class="{zc}">{fmtpct(s["z_score"])}</td><td class="{rc}">{fmt(s["r_score"])}</td><td>{fmt2(s["rate_per_m"])}</td><td>{fmt2(s["rate_per_10k"])}</td><td>{s["slope_6m"]:+.1f}</td><td class="{d3c}">{s["deaths_3mo"]}</td><td>{s["injuries_3mo"]}</td><td class="{cd_cls}">{cd_str}</td><td><span class="sig sig-{s["signal"]}">{s["signal"]}</span></td></tr>\n'

    # PRODUCT CARDS
    cards = ""
    for did, r in all_res.items():
        if not r["stats"]: continue
        dv=r["device"]; st=r["stats"]; lt=st[-1]; rs=r["r_score"]; co=dv["company"]
        ic="1" if did.endswith("_ALL") else "0"
        sig=rs["signal"] if rs else "NORMAL"
        d3=sum(s["deaths"] for s in st[-3:]); i3=sum(s["injuries"] for s in st[-3:])
        evts=PRODUCT_EVENTS.get(did,[])
        ehtml=""
        if evts:
            ehtml='<div class="evt-box"><h4>Timeline</h4>'
            for e in evts:
                tcls = "evt-warn" if "CLASS" in e["type"] or "WARNING" in e["type"] else "evt-ok" if "LAUNCH" in e["type"] or "FDA" in e["type"] or "EXPANSION" in e["type"] else "evt-note"
                ehtml+=f'<div class="evt"><span class="evt-d">{e["date"]}</span><span class="evt-t {tcls}">{e["type"]}</span> {e["desc"]}</div>'
            ehtml+='</div>'
        rhtml=""
        if rs:
            rcol="#c0392b" if rs["total"]>=50 else "#e67e22" if rs["total"]>=30 else "#27ae60"
            rhtml=f'<div class="rg"><div class="rg-v" style="color:{rcol}">{rs["total"]}</div><div class="rg-r"><div class="rg-lbl">R-Score (0-100)</div><div class="rg-trk"><div class="rg-fill" style="width:{min(100,rs["total"])}%;background:{rcol}"></div></div></div></div>'
            rhtml+=f'<div class="rcg"><div class="rci"><span class="rcv">{rs["z_c"]}</span>Z</div><div class="rci"><span class="rcv">{rs["sev_c"]}</span>Sev</div><div class="rci"><span class="rcv">{rs["gap_c"]}</span>Gap</div><div class="rci"><span class="rcv">{rs["slope_c"]}</span>Slope</div><div class="rci"><span class="rcv">{rs["ib_c"]}</span>IB</div></div>'
        fmhtml=""
        fm=r.get("failure_modes")
        if fm and fm.get("modes"):
            fmhtml='<div class="mod-box"><h4>Failure Modes</h4>'
            for cat,info in sorted(fm["modes"].items(), key=lambda x:-x[1]["count"]):
                fmhtml+=f'<div class="fm-row"><span class="fm-lbl">{info["label"]}</span><div class="fm-bar"><div style="width:{info["pct"]}%;background:var(--g)"></div></div><span class="fm-pct">{info["pct"]}%</span></div>'
            fmhtml+='</div>'
        rdhtml=""
        rd=r.get("reddit")
        if rd and rd.get("post_count",0)>0:
            scol="#c0392b" if rd["avg_sentiment"]<-0.1 else "#27ae60" if rd["avg_sentiment"]>0.1 else "var(--tx3)"
            rdhtml=f'<div class="mod-box"><h4>Reddit Sentiment</h4><div class="sg" style="grid-template-columns:repeat(3,1fr)"><div class="si"><div class="si-l">Posts</div><div class="si-v">{rd["post_count"]}</div></div><div class="si"><div class="si-l">Sentiment</div><div class="si-v" style="color:{scol}">{rd["avg_sentiment"]:+.2f}</div></div><div class="si"><div class="si-l">Neg %</div><div class="si-v">{rd["negative_pct"]}%</div></div></div></div>'
        edhtml=""
        ed=r.get("edgar")
        if ed and ed.get("filings"):
            edhtml=f'<div class="mod-box"><h4>EDGAR NLP</h4><div class="mod-sub">Trend: <strong>{ed.get("trend","—").upper()}</strong></div><div class="mod-sub">{ed.get("interpretation","")[:200]}</div></div>'
        echtml=""
        ec=r.get("enhanced_corr")
        if ec:
            stxt="Significant" if ec.get("significant") else "Not significant"
            echtml=f'<div class="mod-box"><h4>MAUDE-Stock Correlation</h4><div class="mod-sub">{ec.get("interpretation","")[:300]}</div></div>'
        zc2 = "neg" if lt["z_score"]>1.5 else "pos" if lt["z_score"]<-1.5 else ""
        slc2 = "neg" if lt["slope_6m"]>0 else "pos"
        d3c2 = "neg" if d3>0 else ""
        cards+=f'''<div class="card" data-co="{co}" data-id="{did}" data-sig="{sig}" data-comb="{ic}">
<div class="card-hdr"><div><h3>{dv["name"]}</h3><span class="tk">{dv["ticker"]}{"  (Private)" if dv["ticker"]=="SQEL" else ""}</span></div><span class="sig sig-{sig}">{sig}</span></div>
<p class="desc">{dv["description"]}</p>{ehtml}
<div class="sg"><div class="si"><div class="si-l">Latest</div><div class="si-v">{fmt0(lt["count"])}</div><div class="si-s">{lt["month"]}</div></div>
<div class="si"><div class="si-l">Z-Score</div><div class="si-v {zc2}">{fmtpct(lt["z_score"])}</div><div class="si-s">avg {fmt0(lt["avg_12m"])} +/- {fmt0(lt["sd_12m"])}</div></div>
<div class="si"><div class="si-l">Rate/$M</div><div class="si-v">{fmt2(lt["rate_per_m"])}</div></div>
<div class="si"><div class="si-l">Rate/10K</div><div class="si-v">{fmt2(lt["rate_per_10k"])}</div></div></div>
<div class="sg"><div class="si"><div class="si-l">6mo Trend</div><div class="si-v {slc2}">{lt["slope_6m"]:+.1f}/mo</div></div>
<div class="si"><div class="si-l">Deaths (3mo)</div><div class="si-v {d3c2}">{d3}</div></div>
<div class="si"><div class="si-l">Injuries (3mo)</div><div class="si-v">{i3}</div></div>
<div class="si"><div class="si-l">Severity</div><div class="si-v">{fmt0(lt["severity_score"])}</div></div></div>
{rhtml}{fmhtml}{rdhtml}{edhtml}{echtml}
<div class="cc" id="cc-{did}"><button class="cb active" data-v="reports">Reports</button><button class="cb" data-v="rate_m">Rate/$M</button><button class="cb" data-v="rate_10k">Rate/10K</button><button class="cb" data-v="severity">Severity</button><button class="cb" data-v="zscore">Z-Score</button><button class="cb" data-v="stock">Stock</button><button class="cb rst" data-v="reset">Reset</button></div>
<div class="cw"><canvas id="ch-{did}"></canvas></div></div>\n'''

    company_options = "".join(f'<option value="{c}">{c}</option>' for c in companies)
    modules_str = "Stats + NLP + Reddit + EDGAR" if HAS_MODULES else "Basic mode"
    updated_str = datetime.now().strftime('%b %d, %Y %H:%M ET')

    # HTML part (f-string, NO javascript)
    html_top = f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Monitor</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{{--g:#2B5F3A;--gl:#3a8a52;--gx:#e8f5ec;--gp:#f4faf6;--bg:#fff;--bg2:#f8faf9;--bg3:#f0f3f1;--tx:#1a2a1f;--tx2:#4a5f50;--tx3:#7a8f80;--bd:#d4e0d8;--red:#c0392b;--org:#e67e22}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--tx);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6}}
.ct{{max-width:1440px;margin:0 auto;padding:24px 32px}}
header{{display:flex;justify-content:space-between;align-items:center;padding:20px 0;border-bottom:2px solid var(--g);margin-bottom:28px}}
header h1{{font-size:22px;font-weight:700;color:var(--g)}} header .sub{{font-size:13px;color:var(--tx2)}}
header .meta{{text-align:right;font-size:12px;color:var(--tx3)}}
h2{{font-size:18px;font-weight:700;color:var(--g);margin:28px 0 16px;padding-bottom:8px;border-bottom:1px solid var(--bd)}}
.sig{{display:inline-block;padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;text-transform:uppercase}}
.sig-NORMAL{{background:var(--gx);color:var(--g)}} .sig-WATCH{{background:#fef3e0;color:#b8860b}}
.sig-ELEVATED{{background:#fdecea;color:var(--red)}} .sig-CRITICAL{{background:#f5c6cb;color:#721c24}}
.bw{{background:#fef3e0;color:#b8860b;font-size:10px;padding:1px 6px;border-radius:4px}}
.guide{{background:var(--gp);border:1px solid var(--bd);border-radius:10px;padding:24px;margin-bottom:24px}}
.guide h3{{font-size:16px;font-weight:700;color:var(--g);margin-bottom:16px}}
.guide-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
.guide-item{{background:var(--bg);border:1px solid var(--bd);border-radius:8px;padding:16px}}
.guide-item h4{{font-size:13px;font-weight:700;color:var(--g);margin-bottom:6px}}
.guide-item p{{font-size:12px;color:var(--tx2)}}
.flt{{display:flex;gap:12px;flex-wrap:wrap;padding:14px 16px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;margin-bottom:20px}}
.fg label{{font-size:10px;font-weight:600;text-transform:uppercase;color:var(--tx3);display:block;margin-bottom:3px}}
.fg select{{background:var(--bg);color:var(--tx);border:1px solid var(--bd);border-radius:6px;padding:6px 10px;font-size:13px;font-family:inherit}}
table{{width:100%;border-collapse:collapse}} th{{text-align:left;padding:10px 12px;font-size:11px;font-weight:600;text-transform:uppercase;color:var(--tx3);border-bottom:2px solid var(--g);background:var(--bg);position:sticky;top:0}}
td{{padding:9px 12px;border-bottom:1px solid var(--bd);font-size:13px;white-space:nowrap}} tr:hover{{background:var(--gp)}}
.neg{{color:var(--red);font-weight:500}} .pos{{color:var(--g);font-weight:500}} .warn{{color:var(--org);font-weight:500}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(460px,1fr));gap:20px;margin-top:16px}}
.card{{background:var(--bg);border:1px solid var(--bd);border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.04)}}
.card-hdr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}} .card h3{{font-size:16px;font-weight:700}}
.tk{{font-size:12px;color:var(--tx3)}}
.desc{{font-size:12px;color:var(--tx2);line-height:1.6;margin-bottom:12px;padding:10px 12px;background:var(--gp);border-radius:6px;border-left:3px solid var(--g)}}
.evt-box{{margin-bottom:10px}} .evt-box h4,.mod-box h4{{font-size:11px;font-weight:600;color:var(--tx3);text-transform:uppercase;margin-bottom:6px}}
.evt{{font-size:11px;color:var(--tx2);padding:4px 0 4px 10px;border-left:2px solid var(--bd);margin-bottom:3px}}
.evt-d{{font-family:monospace;color:var(--tx3);margin-right:6px}}
.evt-t{{font-size:9px;font-weight:700;text-transform:uppercase;padding:1px 5px;border-radius:3px;margin-right:4px}}
.evt-warn{{background:#fdecea;color:var(--red)}} .evt-ok{{background:var(--gx);color:var(--g)}} .evt-note{{background:#eef2f7;color:#2980b9}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px}}
.si{{padding:8px 10px;background:var(--bg2);border-radius:6px;border:1px solid var(--bd)}}
.si-l{{font-size:9px;font-weight:600;text-transform:uppercase;color:var(--tx3)}}
.si-v{{font-size:17px;font-weight:700;margin-top:2px}} .si-s{{font-size:10px;color:var(--tx3)}}
.rg{{display:flex;align-items:center;gap:14px;padding:10px 12px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;margin-bottom:8px}}
.rg-v{{font-size:30px;font-weight:800;min-width:48px}} .rg-r{{flex:1}} .rg-lbl{{font-size:11px;color:var(--tx3);margin-bottom:4px}}
.rg-trk{{height:8px;background:var(--bg3);border-radius:4px;overflow:hidden}} .rg-fill{{height:100%;border-radius:4px}}
.rcg{{display:grid;grid-template-columns:repeat(5,1fr);gap:4px;margin-bottom:10px}}
.rci{{text-align:center;padding:4px;background:var(--bg2);border-radius:4px;border:1px solid var(--bd);font-size:9px;color:var(--tx3)}}
.rcv{{display:block;font-size:13px;font-weight:700;color:var(--tx)}}
.mod-box{{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;padding:12px;margin-bottom:10px}}
.mod-sub{{font-size:12px;color:var(--tx2);margin-top:4px}}
.fm-row{{display:flex;align-items:center;gap:8px;margin-bottom:3px}}
.fm-lbl{{width:150px;font-size:11px;color:var(--tx2)}} .fm-bar{{flex:1;height:12px;background:var(--bg3);border-radius:3px;overflow:hidden}}
.fm-bar div{{height:100%;border-radius:3px}} .fm-pct{{width:40px;text-align:right;font-weight:600;font-size:12px}}
.cc{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px}}
.cb{{background:var(--bg2);color:var(--tx2);border:1px solid var(--bd);border-radius:5px;padding:4px 10px;font-size:11px;font-family:inherit;cursor:pointer;font-weight:500}}
.cb:hover,.cb.active{{background:var(--g);color:#fff;border-color:var(--g)}}
.cb.rst{{margin-left:auto;background:transparent;border-color:var(--bd);color:var(--tx3)}} .cb.rst:hover{{background:var(--bg3)}}
.cw{{position:relative;height:280px}}
.disc{{margin-top:28px;padding:16px;background:var(--bg2);border:1px solid var(--bd);border-radius:8px;font-size:11px;color:var(--tx3);line-height:1.6}}
</style></head><body><div class="ct">
<header><div><h1>MAUDE Monitor</h1><div class="sub">FDA Adverse Event Intelligence</div></div>
<div class="meta">Updated {updated_str}<br>Enhanced: {modules_str}</div></header>
<div class="guide"><h3>Understanding the Metrics</h3><div class="guide-grid">
<div class="guide-item"><h4>Z-Score</h4><p>Standard deviations from 12-month mean. Z > +2.0 = statistically unusual. WATCH at 1.5, ELEVATED at 2.0, CRITICAL at 3.0.</p></div>
<div class="guide-item"><h4>R-Score (0-100)</h4><p>Composite risk: Z-anomaly + severity trend + growth gap + 6mo slope + installed-base rate. Above 50 = investigate. Above 70 = act.</p></div>
<div class="guide-item"><h4>Rate/$M Revenue</h4><p>Monthly reports / (quarterly rev / 3). Rising = quality deteriorating relative to business size.</p></div>
<div class="guide-item"><h4>Rate/10K Users</h4><p>Monthly reports / (est. users / 10K). Normalizes for installed base. More precise than Rate/$M.</p></div>
<div class="guide-item"><h4>Batch Detection</h4><p>If date_received > 3x date_of_event for same month = retrospective filing dump, not real-time surge. Flagged as Batch.</p></div>
<div class="guide-item"><h4>Sigma Bands</h4><p>Light band = +/-1 sigma (68% normal). Outer = +/-2 sigma (95%). Bars beyond 2 sigma are statistically anomalous.</p></div>
<div class="guide-item"><h4>MAUDE-Stock Correlation</h4><p>Spearman rank correlation between MAUDE z-scores and stock log-returns at 1-6 month lags. * = significant at p less than 0.05.</p></div>
<div class="guide-item"><h4>Growth Gap</h4><p>Reports growing faster than revenue? Positive gap = quality deteriorating faster than business growing. Most predictive earnings signal.</p></div>
</div></div>
<div class="flt">
<div class="fg"><label>Company</label><select id="fc" onchange="af()"><option value="all">All</option>{company_options}</select></div>
<div class="fg"><label>Signal</label><select id="fs" onchange="af()"><option value="all">All</option><option value="CRITICAL">Critical</option><option value="ELEVATED">Elevated+</option><option value="WATCH">Watch+</option></select></div>
<div class="fg"><label>View</label><select id="fv" onchange="af()"><option value="individual">Individual</option><option value="combined">Company Level</option></select></div></div>
<h2>Summary</h2>
<div style="overflow-x:auto;margin-bottom:24px"><table><thead><tr><th>Product</th><th>Ticker</th><th>Month</th><th>Reports</th><th>Z-Score</th><th>R-Score</th><th>Rate/$M</th><th>Rate/10K</th><th>Slope</th><th>Deaths</th><th>Injuries</th><th>Corr</th><th>Signal</th></tr></thead><tbody>{trows}</tbody></table></div>
<h2>Product Detail</h2>
<div class="grid">{cards}</div>
<div class="disc">Disclaimer: For research only. Not investment advice. MAUDE is passive surveillance with known limitations. Revenue from SEC filings; installed base from earnings calls. Sequel Med Tech is private. Correlation is not causation.</div>
</div>'''

    # JAVASCRIPT — plain string, NO f-string (avoids all brace conflicts)
    js_code = '''<script>
var defined_cd = __CHART_DATA_PLACEHOLDER__;
var charts = {};
function init(){
  for(var d in defined_cd){if(defined_cd.hasOwnProperty(d)){mk(d,defined_cd[d],"reports");}}
  var allcc = document.querySelectorAll(".cc");
  for(var ci=0;ci<allcc.length;ci++){
    var cc=allcc[ci];
    var btns=cc.querySelectorAll(".cb");
    for(var bi=0;bi<btns.length;bi++){
      btns[bi].addEventListener("click",function(){
        var mycc=this.parentNode;
        var did=mycc.id.replace("cc-","");
        var v=this.getAttribute("data-v");
        if(v==="reset"){if(charts[did])charts[did].resetZoom();return;}
        var siblings=mycc.querySelectorAll(".cb:not(.rst)");
        for(var si=0;si<siblings.length;si++){siblings[si].classList.remove("active");}
        this.classList.add("active");
        mk(did,defined_cd[did],v);
      });
    }
  }
}
function mk(did,D,v){
  var ctx=document.getElementById("ch-"+did);if(!ctx)return;
  if(charts[did])charts[did].destroy();
  var ds=[],yL="",bm=D.bm||[],evts=D.evts||[];
  var evtMs=[];for(var ei=0;ei<evts.length;ei++){evtMs.push(evts[ei].date);}
  if(v==="reports"){
    var barColors=[];
    for(var bi=0;bi<D.l.length;bi++){
      if(bm.indexOf(D.l[bi])>=0){barColors.push("rgba(230,126,34,0.5)");}
      else if(evtMs.indexOf(D.l[bi])>=0){barColors.push("rgba(192,57,43,0.4)");}
      else{barColors.push("rgba(43,95,58,0.25)");}
    }
    ds=[
      {label:"2s upper",data:D.u2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:"+1",pointRadius:0,order:5},
      {label:"2s lower",data:D.l2,borderWidth:0,backgroundColor:"rgba(43,95,58,0.06)",fill:false,pointRadius:0,order:5},
      {label:"1s upper",data:D.u1,borderWidth:0,backgroundColor:"rgba(43,95,58,0.10)",fill:"+1",pointRadius:0,order:4},
      {label:"1s lower",data:D.l1,borderWidth:0,fill:false,pointRadius:0,order:4},
      {label:"Reports",data:D.c,borderColor:"rgba(43,95,58,0.85)",backgroundColor:barColors,borderWidth:1.5,type:"bar",order:2},
      {label:"6mo MA",data:D.ma,borderColor:"#2B5F3A",borderWidth:2.5,fill:false,pointRadius:0,tension:0.3,order:1}
    ];yL="Monthly Reports";
  }else if(v==="rate_m"){
    var rmData=[];for(var ri=0;ri<D.rm.length;ri++){rmData.push(D.rm[ri]===null?undefined:D.rm[ri]);}
    ds=[{label:"Rate/$M",data:rmData,borderColor:"#2B5F3A",backgroundColor:"rgba(43,95,58,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per $M Revenue";
  }else if(v==="rate_10k"){
    var r10Data=[];for(var ri=0;ri<D.r10.length;ri++){r10Data.push(D.r10[ri]===null?undefined:D.r10[ri]);}
    ds=[{label:"Rate/10K",data:r10Data,borderColor:"#2B5F3A",backgroundColor:"rgba(43,95,58,0.2)",borderWidth:1.5,type:"bar"}];yL="Reports per 10K Users";
  }else if(v==="severity"){
    ds=[{label:"Deaths",data:D.d,backgroundColor:"rgba(192,57,43,0.8)",borderWidth:0,stack:"s"},
        {label:"Injuries",data:D.inj,backgroundColor:"rgba(230,126,34,0.7)",borderWidth:0,stack:"s"},
        {label:"Malfunctions",data:D.mal,backgroundColor:"rgba(43,95,58,0.3)",borderWidth:0,stack:"s"}];yL="Events by Type";
  }else if(v==="zscore"){
    var zClr=[];for(var zi=0;zi<D.z.length;zi++){var zv=D.z[zi];zClr.push(zv>2?"rgba(192,57,43,0.8)":zv>1.5?"rgba(230,126,34,0.7)":zv<-1.5?"rgba(43,95,58,0.6)":"rgba(43,95,58,0.25)");}
    var twoLine=[];var negTwoLine=[];for(var ti=0;ti<D.l.length;ti++){twoLine.push(2);negTwoLine.push(-2);}
    ds=[{label:"Z-Score",data:D.z,backgroundColor:zClr,borderWidth:0,type:"bar"},
        {label:"+2s",data:twoLine,borderColor:"rgba(192,57,43,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false},
        {label:"-2s",data:negTwoLine,borderColor:"rgba(43,95,58,0.5)",borderWidth:1,borderDash:[6,3],pointRadius:0,fill:false}];yL="Z-Score";
  }else if(v==="stock"){
    var sp=D.sp||{};var sl=[];var sv=[];var sc=[];
    for(var si=0;si<D.l.length;si++){if(sp[D.l[si]]){sl.push(D.l[si]);sv.push(sp[D.l[si]]);sc.push(D.c[si]);}}
    ds=[{label:"Stock ($)",data:sv,borderColor:"#2B5F3A",borderWidth:2,fill:false,pointRadius:1.5,tension:0.2},
        {label:"MAUDE Reports",data:sc,borderColor:"rgba(192,57,43,0.6)",borderWidth:1.5,fill:false,pointRadius:0,tension:0.2,yAxisID:"y1"}];
    charts[did]=new Chart(ctx,{type:"line",data:{labels:sl,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},
      scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},
        y:{position:"left",grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#2B5F3A",font:{size:10}},title:{display:true,text:"Stock ($)",color:"#2B5F3A",font:{size:11}}},
        y1:{position:"right",grid:{drawOnChartArea:false},ticks:{color:"#c0392b",font:{size:10}},title:{display:true,text:"MAUDE Reports",color:"#c0392b",font:{size:11}}}},
      plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},
        tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1}}}});return;
  }
  charts[did]=new Chart(ctx,{type:"line",data:{labels:D.l,datasets:ds},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},
    scales:{x:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#7a8f80",maxRotation:45,font:{size:10}}},
      y:{grid:{color:"rgba(0,0,0,.05)"},ticks:{color:"#4a5f50",font:{size:10}},title:{display:true,text:yL,color:"#4a5f50",font:{size:11}}}},
    plugins:{legend:{labels:{color:"#4a5f50",boxWidth:12,font:{size:10}}},zoom:{pan:{enabled:true,mode:"x"},zoom:{wheel:{enabled:true},pinch:{enabled:true},drag:{enabled:true,backgroundColor:"rgba(43,95,58,0.08)"},mode:"x"}},
      tooltip:{backgroundColor:"#fff",titleColor:"#1a2a1f",bodyColor:"#4a5f50",borderColor:"#d4e0d8",borderWidth:1,
        callbacks:{afterBody:function(it){var idx=it[0].dataIndex;var month=D.l[idx];var msgs=[];
          if(bm.indexOf(month)>=0){msgs.push("BATCH REPORTING DETECTED");}
          for(var ee=0;ee<evts.length;ee++){if(evts[ee].date===month){msgs.push(evts[ee].type+": "+evts[ee].desc);}}
          return msgs.length?"\\n"+msgs.join("\\n"):"";}}}}}}); 
}
function af(){
  var co=document.getElementById("fc").value;
  var sig=document.getElementById("fs").value;
  var vw=document.getElementById("fv").value;
  var so={"CRITICAL":0,"ELEVATED":1,"WATCH":2,"NORMAL":3};
  var els=document.querySelectorAll(".pr,.card");
  for(var i=0;i<els.length;i++){
    var el=els[i];var sh=true;
    var ec=el.getAttribute("data-co");var es=el.getAttribute("data-sig");var ic=el.getAttribute("data-comb")==="1";
    if(co!=="all"&&ec!==co)sh=false;
    if(sig!=="all"){var sv=so[es]||3;if(sig==="CRITICAL"&&es!=="CRITICAL")sh=false;if(sig==="ELEVATED"&&sv>1)sh=false;if(sig==="WATCH"&&sv>2)sh=false;}
    if(vw==="combined"&&!ic)sh=false;if(vw==="individual"&&ic)sh=false;
    el.style.display=sh?"":"none";
  }
}
document.addEventListener("DOMContentLoaded",init);
</script></body></html>'''

    # Combine and inject data
    full_html = html_top + js_code
    full_html = full_html.replace("__CHART_DATA_PLACEHOLDER__", json.dumps(cd))
    with open("docs/index.html", "w") as f:
        f.write(full_html)
    print(f"\nDashboard: docs/index.html ({len(full_html)//1024}KB)")

def send_alerts(summary):
    to,fr,pw=os.environ.get("MAUDE_EMAIL_TO"),os.environ.get("MAUDE_EMAIL_FROM"),os.environ.get("MAUDE_SMTP_PASSWORD")
    if not all([to,fr,pw]): return
    fl=[s for s in summary if s["signal"] in ("ELEVATED","CRITICAL")]
    if not fl: return
    body="MAUDE Monitor Alert\n\n"
    for s in fl: body+=f"  {s['name']} ({s['ticker']}): {s['signal']} | R={s['r_score']} | Z={s['z_score']:+.2f}\n"
    msg=MIMEMultipart(); msg["From"],msg["To"]=fr,to
    msg["Subject"]=f"MAUDE Alert: {len(fl)} flagged"
    msg.attach(MIMEText(body,"plain"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as srv: srv.starttls();srv.login(fr,pw);srv.send_message(msg)
    except Exception as e: print(f"Email failed: {e}")

def main():
    p=argparse.ArgumentParser(description="MAUDE Monitor V2.3")
    p.add_argument("--html",action="store_true")
    p.add_argument("--backfill",action="store_true")
    p.add_argument("--quick",action="store_true")
    a=p.parse_args()
    print(f"MAUDE Monitor V2.3 | {datetime.now():%Y-%m-%d %H:%M} | {len(DEVICES)} devices | Modules: {'YES' if HAS_MODULES else 'NO'}")
    r,s = run_pipeline(a.backfill, a.quick)
    generate_html(r, s)
    send_alerts(s)
    print(f"\nCOMPLETE | data/latest_summary.json | docs/index.html")

if __name__=="__main__": main()
