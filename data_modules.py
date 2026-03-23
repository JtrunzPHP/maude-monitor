"""
data_modules.py — All Enhanced Data Sources for MAUDE Monitor V3
=================================================================
1. MAUDE Text NLP (failure-mode classification)
2. Google Trends (early complaint signal)
3. SEC EDGAR Form 4 (insider trading)
4. SEC EDGAR 10-Q/10-K NLP (quality language)
5. ClinicalTrials.gov (competitive pipeline)
6. Short Interest (market positioning)
7. CMS Payer/Formulary (coverage tracking)
8. International MHRA (UK signal)

Every function returns a dict with a "status" key so the dashboard
ALWAYS shows whether data was fetched, blocked, or unavailable.
"""
import json,time,re,math
from datetime import datetime,timedelta
from urllib.request import urlopen,Request
from urllib.parse import quote as url_quote
from urllib.error import HTTPError,URLError

def _fetch(url, timeout=15, ua="MAUDE-Monitor/3.0"):
    try:
        req=Request(url,headers={"User-Agent":ua,"Accept":"application/json"})
        with urlopen(req,timeout=timeout) as r: return json.loads(r.read())
    except Exception as e:
        return {"_error":str(e)}

def _fetch_text(url, timeout=15, ua="MAUDE-Monitor/3.0 research@example.com"):
    try:
        req=Request(url,headers={"User-Agent":ua})
        with urlopen(req,timeout=timeout) as r: return r.read().decode("utf-8",errors="ignore")
    except Exception as e:
        return ""

# ============================================================
# 1. MAUDE TEXT NLP
# ============================================================
FAILURE_MODES = {
    "sensor_accuracy":{"primary":["inaccurate","false reading","wrong reading","reading was off","incorrect reading","showed low when high","showed high when low"],"secondary":["accuracy","MARD","finger stick","calibration"],"desc":"Sensor giving wrong glucose values. Can cause wrong insulin dosing. HIGH severity.","recall_risk":0.8},
    "adhesive_skin":{"primary":["adhesive","fell off","peeling","skin irritation","rash","allergic reaction","dermatitis","blistered"],"secondary":["tape","itching","swelling","redness"],"desc":"Adhesive failure or skin reaction. Affects compliance. MEDIUM severity.","recall_risk":0.2},
    "connectivity":{"primary":["bluetooth","lost connection","no signal","pairing failed","communication error","disconnected"],"secondary":["sync","phone","app connection","receiver"],"desc":"Wireless connectivity failure between sensor and phone/receiver.","recall_risk":0.3},
    "alert_failure":{"primary":["no alert","missed alert","no alarm","did not alert","silent failure","alarm did not sound","speaker"],"secondary":["notification","vibration","urgent low","alert not received"],"desc":"Failed to warn user of dangerous glucose. Linked to deaths. CRITICAL severity.","recall_risk":0.95},
    "insulin_delivery":{"primary":["occlusion","no delivery","blockage","under-delivery","over-delivery","insulin not delivered","pump failure"],"secondary":["infusion","bolus","basal","cannula","tubing","pod failure"],"desc":"Pump delivery failure. Can cause DKA or severe hypo. VERY HIGH severity.","recall_risk":0.7},
    "sensor_early_end":{"primary":["sensor failed","expired early","terminated early","sensor error","warm-up failed","no readings","sensor stopped"],"secondary":["replace sensor","sensor lasted","only lasted"],"desc":"Sensor stopped before labeled wear period ended.","recall_risk":0.4},
    "software_app":{"primary":["app crash","software error","display error","screen blank","update broke","firmware"],"secondary":["software","update","app","glitch"],"desc":"App or firmware malfunction.","recall_risk":0.5},
}

def analyze_failure_modes(search_query, start="20250101", limit=100):
    end=datetime.now().strftime("%Y%m%d")
    url=f"https://api.fda.gov/device/event.json?search={url_quote(search_query,safe='+:\"[]')}+AND+date_received:[{start}+TO+{end}]&limit={limit}"
    data=_fetch(url)
    if "_error" in data or "results" not in data:
        return {"status":"error","message":data.get("_error","No results"),"total_analyzed":0,"modes":{}}
    records=[]; total_with_text=0
    for r in data.get("results",[]):
        texts=r.get("mdr_text",[])
        combined=" ".join(t.get("text","") for t in texts).lower().strip()
        if combined: total_with_text+=1; records.append(combined[:2000])
    if not records:
        return {"status":"no_text","message":f"Fetched {len(data.get('results',[]))} records but none had narrative text. This is common — many MAUDE records have blank mdr_text fields in the API.","total_analyzed":0,"modes":{}}
    mode_counts={cat:{"count":0} for cat in FAILURE_MODES}
    unclassified=0
    for text in records:
        scores={}
        for cat,cfg in FAILURE_MODES.items():
            s=sum(2 for kw in cfg["primary"] if kw in text)+sum(1 for kw in cfg["secondary"] if kw in text)
            if s>0: scores[cat]=s
        if not scores: unclassified+=1; continue
        best=max(scores,key=scores.get); mode_counts[best]["count"]+=1
    total=len(records)
    result={"status":"ok","total_analyzed":total,"records_with_text":total_with_text,"unclassified":unclassified,"modes":{}}
    for cat,d in mode_counts.items():
        if d["count"]>0:
            result["modes"][cat]={"count":d["count"],"pct":round(d["count"]/total*100,1),"label":cat.replace("_"," ").title(),"desc":FAILURE_MODES[cat]["desc"],"recall_risk":FAILURE_MODES[cat]["recall_risk"]}
    return result

# ============================================================
# 2. GOOGLE TRENDS
# ============================================================
TRENDS_QUERIES = {
    "DXCM":["dexcom g7 problems","dexcom recall","dexcom inaccurate","dexcom lawsuit"],
    "PODD":["omnipod 5 problems","omnipod failure","omnipod occlusion"],
    "TNDM":["tandem pump problems","tslim issues","control iq problems"],
    "ABT_LIBRE":["libre 3 problems","freestyle libre inaccurate"],
    "BBNX":["ilet problems","bionic pancreas issues"],
    "MDT_DM":["minimed 780g problems","medtronic pump recall"],
    "SQEL":["twiist pump problems"],
}

def analyze_google_trends(ticker):
    """Fetch Google Trends data. Uses pytrends if available, else reports unavailable."""
    queries=TRENDS_QUERIES.get(ticker,[])
    if not queries:
        return {"status":"no_queries","message":"No trend queries configured for this ticker."}
    try:
        from pytrends.request import TrendReq
        pytrends=TrendReq(hl='en-US',tz=300)
        # Get interest over last 12 months for top 2 queries
        kw_list=queries[:2]
        pytrends.build_payload(kw_list,cat=0,timeframe='today 12-m',geo='US')
        interest=pytrends.interest_over_time()
        if interest.empty:
            return {"status":"no_data","message":"Google Trends returned no data for these queries."}
        # Compute recent vs prior trend
        recent=interest.tail(4).mean()  # last month
        prior=interest.head(len(interest)-4).mean()  # everything before
        trends=[]
        for kw in kw_list:
            r_val=recent.get(kw,0); p_val=prior.get(kw,0)
            change=((r_val-p_val)/p_val*100) if p_val>0 else 0
            trends.append({"query":kw,"recent_interest":round(float(r_val),1),"prior_interest":round(float(p_val),1),"change_pct":round(float(change),1)})
        # Overall signal
        avg_change=sum(t["change_pct"] for t in trends)/len(trends)
        signal="rising" if avg_change>20 else "falling" if avg_change<-20 else "stable"
        return {"status":"ok","trends":trends,"signal":signal,"avg_change":round(avg_change,1),"message":f"Complaint-related search interest is {signal} ({avg_change:+.0f}% vs prior period)."}
    except ImportError:
        return {"status":"no_pytrends","message":"pytrends not installed. Add 'pip install pytrends' to workflow. Google Trends provides the earliest signal (2-4 weeks ahead of MAUDE)."}
    except Exception as e:
        return {"status":"error","message":f"Google Trends blocked or errored: {str(e)[:100]}"}

# ============================================================
# 3. SEC FORM 4 INSIDER TRADING
# ============================================================
COMPANY_CIKS = {"DXCM":"0001093557","PODD":"0001145197","TNDM":"0001438133","ABT_LIBRE":"0000001800","BBNX":"0001674632","MDT_DM":"0001613103"}

def analyze_insider_trading(ticker):
    cik=COMPANY_CIKS.get(ticker)
    if not cik: return {"status":"no_cik","message":"No CIK for this ticker."}
    url=f"https://data.sec.gov/submissions/CIK{cik}.json"
    data=_fetch(url,ua="MAUDE-Monitor research@parkmanhp.com")
    if "_error" in data: return {"status":"error","message":f"EDGAR error: {data['_error'][:100]}"}
    recent=data.get("filings",{}).get("recent",{})
    forms=recent.get("form",[]); dates=recent.get("filingDate",[]); urls=recent.get("primaryDocument",[])
    # Find Form 4s in last 90 days
    cutoff=(datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")
    form4s=[]
    for i in range(min(len(forms),200)):
        if forms[i]=="4" and dates[i]>=cutoff:
            form4s.append({"date":dates[i],"form":forms[i]})
    # Classify as buys vs sells (Form 4 detail requires parsing XML, so we approximate by count)
    total=len(form4s)
    if total==0:
        return {"status":"ok","total_filings":0,"period":"90 days","message":"No Form 4 insider transactions in the last 90 days.","signal":"neutral"}
    # High Form 4 activity often = selling
    signal="high_activity" if total>10 else "moderate" if total>4 else "low"
    msg=f"{total} Form 4 filings in last 90 days. "
    if total>10: msg+="HIGH insider trading activity — often indicates selling. Cross-reference with R-Score."
    elif total>4: msg+="Moderate insider activity."
    else: msg+="Low insider activity — normal."
    return {"status":"ok","total_filings":total,"period":"90 days","signal":signal,"message":msg,"filings":form4s[:5]}

# ============================================================
# 4. SEC EDGAR 10-Q/10-K NLP
# ============================================================
EDGAR_KEYWORDS = {
    "product_quality":{"high":["recall","warning letter","class i","class ii","consent decree","fda inspection","483 observation","corrective action"],"medium":["product quality","manufacturing defect","adverse event","complaint rate","warranty cost","warranty expense","replacement cost"]},
    "legal_risk":{"high":["class action","securities litigation","derivative action","doj investigation"],"medium":["litigation","lawsuit","legal proceeding","product liability"]},
    "revenue_risk":{"high":["revenue decline","market share loss","customer attrition","competitive pressure"],"medium":["guidance reduction","lowered guidance","headwind","adverse impact"]},
}

def analyze_edgar_filings(ticker):
    cik=COMPANY_CIKS.get(ticker)
    if not cik: return {"status":"no_cik","message":"No CIK."}
    url=f"https://data.sec.gov/submissions/CIK{cik}.json"
    data=_fetch(url,ua="MAUDE-Monitor research@parkmanhp.com")
    if "_error" in data: return {"status":"error","message":f"EDGAR: {data['_error'][:100]}"}
    recent=data.get("filings",{}).get("recent",{})
    forms=recent.get("form",[]); dates=recent.get("filingDate",[]); accessions=recent.get("accessionNumber",[]); docs=recent.get("primaryDocument",[])
    filings=[]
    for i in range(min(len(forms),100)):
        if forms[i] in ("10-Q","10-K") and len(filings)<3:
            acc=accessions[i].replace("-","")
            furl=f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc}/{docs[i]}"
            filings.append({"date":dates[i],"form":forms[i],"url":furl})
    if not filings: return {"status":"no_filings","message":"No 10-Q/10-K found."}
    results=[]
    for f in filings:
        time.sleep(0.3)
        text=_fetch_text(f["url"],ua="MAUDE-Monitor research@parkmanhp.com")
        if not text: continue
        text_lower=re.sub(r'<[^>]+>',' ',text).lower()[:80000]
        scores={}; all_high=[]
        for cat,kws in EDGAR_KEYWORDS.items():
            h=[kw for kw in kws["high"] if kw in text_lower]; m=[kw for kw in kws["medium"] if kw in text_lower]
            scores[cat]=len(h)*5+len(m)*2; all_high.extend(h)
        total_score=sum(scores.values())
        results.append({"date":f["date"],"form":f["form"],"score":total_score,"high_matches":list(set(all_high)),"categories":scores})
    if not results: return {"status":"error","message":"Could not parse filing text."}
    trend="increasing" if len(results)>=2 and results[0]["score"]>results[-1]["score"]*1.2 else "decreasing" if len(results)>=2 and results[0]["score"]<results[-1]["score"]*0.8 else "stable"
    latest=results[0]
    msg=f'{latest["form"]} ({latest["date"]}): quality score {latest["score"]}.'
    if latest["high_matches"]: msg+=f' HIGH matches: {", ".join(latest["high_matches"][:5])}.'
    msg+=f" Trend: {trend}."
    return {"status":"ok","filings":results,"trend":trend,"message":msg}

# ============================================================
# 5. CLINICALTRIALS.GOV
# ============================================================
TRIAL_QUERIES = {
    "DXCM":"continuous glucose monitor dexcom","PODD":"omnipod insulin pump","TNDM":"tandem insulin pump",
    "ABT_LIBRE":"freestyle libre","BBNX":"bionic pancreas ilet","MDT_DM":"minimed insulin pump","SQEL":"twiist insulin",
}

def analyze_clinical_trials(ticker):
    query=TRIAL_QUERIES.get(ticker)
    if not query: return {"status":"no_query","message":"No trial query configured."}
    url=f"https://clinicaltrials.gov/api/v2/studies?query.term={url_quote(query)}&filter.overallStatus=RECRUITING,NOT_YET_RECRUITING,ACTIVE_NOT_RECRUITING&pageSize=5&sort=LastUpdatePostDate:desc"
    data=_fetch(url)
    if "_error" in data: return {"status":"error","message":f"ClinicalTrials.gov: {data['_error'][:100]}"}
    studies=data.get("studies",[])
    if not studies: return {"status":"no_trials","message":"No active/recruiting trials found."}
    trials=[]
    for s in studies[:5]:
        proto=s.get("protocolSection",{})
        ident=proto.get("identificationModule",{})
        status=proto.get("statusModule",{})
        trials.append({"nct_id":ident.get("nctId",""),"title":ident.get("briefTitle","")[:120],"status":status.get("overallStatus",""),"last_update":status.get("lastUpdatePostDateStruct",{}).get("date","")})
    return {"status":"ok","count":len(trials),"trials":trials,"message":f"{len(trials)} active/recruiting trials found. New trials = competitive pipeline activity."}

# ============================================================
# 6. SHORT INTEREST (Yahoo Finance key stats page)
# ============================================================
YAHOO_TICKERS = {"DXCM":"DXCM","PODD":"PODD","TNDM":"TNDM","ABT_LIBRE":"ABT","BBNX":"BBNX","MDT_DM":"MDT"}

def analyze_short_interest(ticker):
    yt=YAHOO_TICKERS.get(ticker)
    if not yt: return {"status":"no_ticker","message":"No Yahoo ticker."}
    url=f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{yt}?modules=defaultKeyStatistics"
    data=_fetch(url,ua="Mozilla/5.0")
    if "_error" in data:
        return {"status":"blocked","message":"Yahoo Finance blocked this request. Short interest data unavailable from this IP."}
    try:
        stats=data["quoteSummary"]["result"][0]["defaultKeyStatistics"]
        si_pct=stats.get("shortPercentOfFloat",{}).get("raw",None)
        si_shares=stats.get("sharesShort",{}).get("raw",None)
        si_ratio=stats.get("shortRatio",{}).get("raw",None)
        if si_pct is None: return {"status":"no_data","message":"Short interest data not available."}
        signal="high" if si_pct>0.10 else "moderate" if si_pct>0.05 else "low"
        msg=f"Short interest: {si_pct*100:.1f}% of float ({si_shares:,.0f} shares). Days to cover: {si_ratio:.1f}. "
        if si_pct>0.10: msg+="HIGH short interest — market is bearish. If R-Score also elevated, consensus building."
        elif si_pct>0.05: msg+="Moderate — some skepticism but not crowded."
        else: msg+="Low — market not positioned for downside."
        return {"status":"ok","short_pct":round(si_pct*100,2),"short_shares":si_shares,"days_to_cover":round(si_ratio,1),"signal":signal,"message":msg}
    except: return {"status":"parse_error","message":"Could not parse Yahoo Finance response."}

# ============================================================
# 7. CMS PAYER/FORMULARY TRACKING
# ============================================================
def analyze_payer_coverage(ticker):
    """Check CMS.gov for recent coverage decisions related to CGM/insulin pumps."""
    # CMS doesn't have a clean API for this. We search their decision memo page.
    queries={"DXCM":"continuous glucose monitor","PODD":"insulin pump","TNDM":"insulin pump","ABT_LIBRE":"continuous glucose monitor","BBNX":"insulin pump","MDT_DM":"insulin pump"}
    q=queries.get(ticker)
    if not q: return {"status":"no_query","message":"No CMS query configured."}
    url=f"https://www.cms.gov/medicare-coverage-database/search.aspx?searchTerm={url_quote(q)}&format=json"
    # CMS site doesn't return JSON reliably, so this is framework-level
    return {"status":"framework","message":"CMS coverage tracking is a framework placeholder. CMS does not provide a structured API for coverage decisions. For CGMs, the key coverage expansion was the April 2023 Medicare ruling allowing CGM coverage without insulin use. Monitor cms.gov/medicare-coverage-database manually for changes."}

# ============================================================
# 8. INTERNATIONAL (MHRA/UK)
# ============================================================
def analyze_international(ticker, brand_names):
    results={}
    for brand in brand_names[:2]:
        url=f"https://www.gov.uk/drug-device-alerts.json?keywords={url_quote(brand)}"
        data=_fetch(url)
        if "_error" not in data:
            alerts=[]
            for r in data.get("results",[])[:3]:
                if brand.lower() in r.get("title","").lower():
                    alerts.append({"title":r.get("title","")[:120],"date":r.get("public_timestamp","")[:10]})
            if alerts: results[brand]=alerts
    if not results:
        return {"status":"no_alerts","message":"No UK MHRA alerts found for this company's products. International signal is limited — MHRA does not have a structured API."}
    return {"status":"ok","alerts":results,"message":f"Found MHRA alerts for: {', '.join(results.keys())}"}

# ============================================================
# 9. RECALL PROBABILITY MODEL
# ============================================================
def compute_recall_probability(failure_modes_data, stats_list):
    """Estimate probability of a future recall based on failure mode mix and trends."""
    if not failure_modes_data or failure_modes_data.get("status")!="ok" or not failure_modes_data.get("modes"):
        if not stats_list or len(stats_list)<6:
            return {"status":"insufficient_data","message":"Need failure mode data and 6+ months of stats.","probability":None}
        # Fallback: use severity trend alone
        recent_sev=sum(s["severity_score"] for s in stats_list[-3:])/3
        prior_sev=sum(s["severity_score"] for s in stats_list[-6:-3])/3
        ratio=recent_sev/prior_sev if prior_sev>0 else 1
        prob=min(0.8,max(0.05,0.1+(ratio-1)*0.3))
        return {"status":"severity_only","probability":round(prob,2),"message":f"Based on severity trend only (ratio {ratio:.2f}). {prob*100:.0f}% estimated recall probability in next 6 months."}
    # Weight by recall_risk of dominant failure modes
    modes=failure_modes_data["modes"]
    weighted_risk=0; total_pct=0
    for cat,info in modes.items():
        risk=FAILURE_MODES.get(cat,{}).get("recall_risk",0.3)
        weighted_risk+=info["pct"]*risk; total_pct+=info["pct"]
    if total_pct>0: weighted_risk/=total_pct
    # Adjust by trend
    if stats_list and len(stats_list)>=6:
        recent=sum(s["count"] for s in stats_list[-3:])/3
        prior=sum(s["count"] for s in stats_list[-6:-3])/3
        trend_mult=min(2,recent/prior) if prior>0 else 1
        weighted_risk*=trend_mult
    prob=min(0.95,max(0.05,weighted_risk))
    dominant=max(modes.items(),key=lambda x:x[1]["pct"])[0] if modes else "unknown"
    signal="HIGH" if prob>0.5 else "MODERATE" if prob>0.25 else "LOW"
    return {"status":"ok","probability":round(prob,2),"signal":signal,"dominant_mode":dominant.replace("_"," ").title(),"message":f"{prob*100:.0f}% estimated recall probability (next 6mo). Dominant failure mode: {dominant.replace('_',' ')}. Signal: {signal}."}

# ============================================================
# 10. PEER-RELATIVE SCORING
# ============================================================
def compute_peer_relative(all_r_scores):
    """Compare each company's R-Score to the peer group average."""
    scores={k:v for k,v in all_r_scores.items() if v is not None}
    if len(scores)<2: return {}
    avg=sum(scores.values())/len(scores)
    sd=(sum((v-avg)**2 for v in scores.values())/len(scores))**.5
    result={}
    for k,v in scores.items():
        diff=v-avg
        z_vs_peers=diff/sd if sd>0 else 0
        signal="WORST" if z_vs_peers>1 else "WEAK" if z_vs_peers>0.5 else "BEST" if z_vs_peers<-1 else "STRONG" if z_vs_peers<-0.5 else "INLINE"
        result[k]={"r_score":v,"peer_avg":round(avg,1),"diff":round(diff,1),"z_vs_peers":round(z_vs_peers,2),"signal":signal,"message":f"R-Score {v:.0f} vs peer avg {avg:.0f} ({diff:+.0f}). {signal} relative to peer group."}
    return result

# ============================================================
# 11. EARNINGS SURPRISE PREDICTOR
# ============================================================
def predict_earnings_surprise(stats_list, r_score, peer_relative):
    """Predict beat/miss probability based on MAUDE signals."""
    if not stats_list or len(stats_list)<6 or not r_score:
        return {"status":"insufficient_data","message":"Need 6+ months of data and R-Score."}
    # Factors that predict a miss:
    miss_score=0
    # 1. High R-Score
    if r_score["total"]>=50: miss_score+=3
    elif r_score["total"]>=30: miss_score+=1
    # 2. Rising Rate/$M (quality getting worse relative to revenue)
    recent_rpm=[s["rate_per_m"] for s in stats_list[-3:] if s["rate_per_m"]]
    prior_rpm=[s["rate_per_m"] for s in stats_list[-6:-3] if s["rate_per_m"]]
    if recent_rpm and prior_rpm:
        rpm_change=(sum(recent_rpm)/len(recent_rpm))/(sum(prior_rpm)/len(prior_rpm))
        if rpm_change>1.2: miss_score+=2
        elif rpm_change>1.05: miss_score+=1
    # 3. Positive slope (accelerating problems)
    if stats_list[-1]["slope_6m"]>0: miss_score+=1
    # 4. Peer-relative weakness
    if peer_relative and peer_relative.get("signal") in ("WORST","WEAK"): miss_score+=1
    # 5. Deaths in recent period
    recent_deaths=sum(s["deaths"] for s in stats_list[-3:])
    if recent_deaths>0: miss_score+=2
    # Convert to probability
    max_score=9
    miss_prob=min(0.85,miss_score/max_score)
    beat_prob=1-miss_prob
    if miss_prob>0.55: prediction="LIKELY MISS"
    elif beat_prob>0.55: prediction="LIKELY BEAT"
    else: prediction="TOSS-UP"
    return {"status":"ok","miss_probability":round(miss_prob,2),"beat_probability":round(beat_prob,2),"prediction":prediction,"miss_score":miss_score,"max_score":max_score,"message":f"Earnings prediction: {prediction}. Miss probability: {miss_prob*100:.0f}%. Based on R-Score ({r_score['total']:.0f}), quality trend, severity, and peer position."}

# ============================================================
# BACKTEST ENGINE
# ============================================================
def backtest_r_score(stats_list, stock_monthly, threshold=50, forward_days_list=[30,60,90]):
    """
    Backtest: when R-Score crossed threshold, what happened to stock?
    Returns hit rates, avg returns, and individual trades.
    """
    if not stats_list or len(stats_list)<12 or not stock_monthly:
        return {"status":"insufficient_data","message":"Need 12+ months of MAUDE data and stock prices."}
    # Compute R-Scores for each month
    monthly_r={}
    for i in range(6,len(stats_list)):
        window=stats_list[:i+1]
        r=_quick_r_score(window)
        if r is not None: monthly_r[stats_list[i]["month"]]=r
    # Find signal months (R-Score crosses threshold)
    sorted_months=sorted(stock_monthly.keys())
    signals=[]
    prev_below=True
    for m in sorted(monthly_r.keys()):
        above=monthly_r[m]>=threshold
        if above and prev_below:  # Crossed upward
            signals.append(m)
        prev_below=not above
    if not signals:
        return {"status":"no_signals","message":f"R-Score never crossed {threshold} in this period. Try a lower threshold.","trades":[]}
    # For each signal, compute forward returns
    trades=[]
    for sig_month in signals:
        sig_idx=sorted_months.index(sig_month) if sig_month in sorted_months else -1
        if sig_idx<0: continue
        entry_price=stock_monthly.get(sig_month)
        if not entry_price: continue
        trade={"signal_month":sig_month,"entry_price":entry_price,"r_score":monthly_r[sig_month],"returns":{}}
        for fwd in forward_days_list:
            fwd_months=fwd//30
            target_idx=sig_idx+fwd_months
            if target_idx<len(sorted_months):
                exit_price=stock_monthly.get(sorted_months[target_idx])
                if exit_price: trade["returns"][f"{fwd}d"]=round((exit_price-entry_price)/entry_price*100,2)
        trades.append(trade)
    # Aggregate stats
    results={}
    for fwd in forward_days_list:
        key=f"{fwd}d"
        rets=[t["returns"].get(key) for t in trades if key in t["returns"]]
        if not rets: continue
        wins=sum(1 for r in rets if r<0)  # We're looking for stock DECLINES after high R-Score
        results[key]={"n":len(rets),"win_rate":round(wins/len(rets)*100,1),"avg_return":round(sum(rets)/len(rets),2),"best":round(min(rets),2),"worst":round(max(rets),2)}
    if not results:
        return {"status":"no_forward_data","message":"Signal months found but no forward stock data available.","trades":trades}
    best_window=min(results.keys(),key=lambda k:results[k]["avg_return"])
    bw=results[best_window]
    msg=f"Backtest: R-Score crossed {threshold} on {len(signals)} occasions. "
    msg+=f"Best window: {best_window} — stock declined {abs(bw['avg_return']):.1f}% avg (win rate: {bw['win_rate']:.0f}%, n={bw['n']})."
    return {"status":"ok","threshold":threshold,"signals":len(signals),"results":results,"trades":trades,"message":msg}

def _quick_r_score(sl):
    if len(sl)<6: return None
    lt=sl[-1]; zc=min(20,abs(lt["z_score"])*6.67)
    rs=sum(s["severity_score"] for s in sl[-3:])/3; ps=sum(s["severity_score"] for s in sl[-6:-3])/3
    sc=min(20,max(0,(rs/ps-1)*40)) if ps>0 else 10
    rr=[s["rate_per_m"] for s in sl[-3:] if s["rate_per_m"]]; pr=[s["rate_per_m"] for s in sl[-6:-3] if s["rate_per_m"]]
    gc=min(20,max(0,((sum(rr)/len(rr))/(sum(pr)/len(pr))-1)*80)) if rr and pr and sum(pr)/len(pr)>0 else 10
    sp=lt["slope_6m"]/lt["avg_12m"]*100 if lt["avg_12m"]>0 else 0; slc=min(20,max(0,sp*2))
    ri=[s["rate_per_10k"] for s in sl[-3:] if s["rate_per_10k"]]; pi=[s["rate_per_10k"] for s in sl[-6:-3] if s["rate_per_10k"]]
    ic=min(20,max(0,((sum(ri)/len(ri))/(sum(pi)/len(pi))-1)*80)) if ri and pi and sum(pi)/len(pi)>0 else 10
    return min(100,zc+sc+gc+slc+ic)
