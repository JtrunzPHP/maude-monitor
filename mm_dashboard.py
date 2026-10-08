"""MAUDE Monitor V4 — dashboard generator.

Writes a single self-contained HTML page (docs/index.html for GitHub
Pages). All analytics arrive as an embedded JSON payload; charts render
lazily via IntersectionObserver so 17 devices stay fast. Chart.js and the
zoom plugin load from cdnjs.
"""

import json
import os

from mm_config import OUTPUT_HTML, PAYLOAD_FILE, COMPANIES, FDR_Q


def _device_payload(did, res):
    dev = res["device"]
    st = res.get("stats")
    out = {"id": did, "name": dev["name"], "company": dev["company"],
           "ticker": dev["ticker"], "combined": dev["is_combined"],
           "signal": res["signal"], "r": res.get("r_score"),
           "r_detail": res.get("r_detail"),
           "new_since": res.get("new_since_last_run"),
           "changed_months": res.get("changed_months", []),
           "rev_verified": res.get("rev_verified", False),
           "base_verified": res.get("base_verified", False),
           "events": res.get("events", []),
           "brand_field": res.get("brand_field", "")}
    if st:
        months = st["months"]
        sev = res.get("sev", {})
        out["stats"] = {
            "months": months, "values": st["values"],
            "event_values": st["event_values"], "ma6": st["ma6"],
            "z_raw": st["z_raw"], "z_adj": st["z_adj"],
            "rate_m": st["rate_m_series"], "rate_10k": st["rate_10k_series"],
            "s1lo": st["sigma1_lo"], "s1hi": st["sigma1_hi"],
            "s2lo": st["sigma2_lo"], "s2hi": st["sigma2_hi"],
            "latest_month": st["latest_month"], "latest_value": st["latest_value"],
            "provisional": st["provisional"], "slope": st["slope_6mo"],
            "z": st["z_score"], "z_t": st["z_score_adj"],
            "deaths3": st["deaths_3mo"], "inj3": st["injuries_3mo"],
            "rpm": st["rate_per_m"], "r10k": st["rate_per_10k"],
            "batch": st["batch_months"],
            "deaths": [sev.get("death", {}).get(m, 0) for m in months],
            "injuries": [sev.get("injury", {}).get(m, 0) for m in months],
            "malfs": [sev.get("malfunction", {}).get(m, 0) for m in months],
        }
        stock = res.get("stock_series", {})
        out["stats"]["stock"] = [stock.get(m) for m in months]
        sw = (res.get("modules", {}).get("severity_weighted") or {})
        out["stats"]["sw"] = ([sw.get("scores", {}).get(m, 0) for m in months]
                              if sw.get("status") == "ok" else [])
    mods = {}
    for key in ("car", "correlation", "prr", "cascade", "severity_weighted",
                "cross_company", "peer_relative", "failure_modes",
                "earnings_pred", "recall_prob", "recalls", "edgar",
                "trials", "payer"):
        m = res.get("modules", {}).get(key)
        if m:
            mods[key] = m
    out["modules"] = mods
    return out


def build_payload(all_res, meta, stock_by_ticker):
    devices = []
    for did, res in all_res.items():
        if res.get("stats"):
            res["stock_series"] = stock_by_ticker.get(res["device"]["ticker"], {})
        devices.append(_device_payload(did, res))
    order = {"CRITICAL": 0, "ELEVATED": 1, "WATCH": 2, "NORMAL": 3}
    devices.sort(key=lambda d: (order.get(d["signal"], 9), -(d["r"] or 0)))
    return {"meta": meta, "companies": COMPANIES, "fdr_q": FDR_Q,
            "devices": devices}


def write_dashboard(all_res, meta, stock_by_ticker):
    payload = build_payload(all_res, meta, stock_by_ticker)
    pj = json.dumps(payload, separators=(",", ":")).replace("<", "\\u003c")
    html = TEMPLATE.replace("__PAYLOAD__", pj)
    os.makedirs(os.path.dirname(OUTPUT_HTML) or ".", exist_ok=True)
    with open(OUTPUT_HTML, "w") as f:
        f.write(html)
    os.makedirs(os.path.dirname(PAYLOAD_FILE) or ".", exist_ok=True)
    with open(PAYLOAD_FILE, "w") as f:
        json.dump(payload, f)
    print(f"\nDashboard written to {OUTPUT_HTML} "
          f"({os.path.getsize(OUTPUT_HTML) / 1024:.0f} KB)")
    return OUTPUT_HTML


TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>MAUDE Monitor</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<style>
:root{
  --bg:#eef0ee; --panel:#fbfbf9; --panel2:#f2f3f0; --line:#d8dcd6;
  --tx:#1d2420; --tx2:#4a554e; --tx3:#7a857d;
  --crit:#b3272e; --elev:#bc5f04; --watch:#8f7a10; --norm:#2c6e49;
  --acc:#1d5a66; --band:rgba(29,90,102,.10); --band2:rgba(29,90,102,.05);
  font-variant-numeric: tabular-nums;
}
:root[data-theme="dark"]{
  --bg:#171a18; --panel:#1f2321; --panel2:#262b28; --line:#333a35;
  --tx:#e6e9e5; --tx2:#aab3ac; --tx3:#77817a;
  --crit:#e05c62; --elev:#e58a3a; --watch:#c7b23c; --norm:#5bbd8a;
  --acc:#63b3c4; --band:rgba(99,179,196,.12); --band2:rgba(99,179,196,.06);
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    --bg:#171a18; --panel:#1f2321; --panel2:#262b28; --line:#333a35;
    --tx:#e6e9e5; --tx2:#aab3ac; --tx3:#77817a;
    --crit:#e05c62; --elev:#e58a3a; --watch:#c7b23c; --norm:#5bbd8a;
    --acc:#63b3c4; --band:rgba(99,179,196,.12); --band2:rgba(99,179,196,.06);
  }
}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{background:var(--bg);color:var(--tx);
  font:14px/1.5 "Instrument Sans",system-ui,-apple-system,sans-serif;
  padding:env(safe-area-inset-top,0) 0 env(safe-area-inset-bottom,0)}
.wrap{max-width:1180px;margin:0 auto;padding:18px 16px 60px}
header{display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 18px;margin-bottom:6px}
h1{font-size:21px;font-weight:700;margin:0;letter-spacing:-.01em}
h1 span{color:var(--tx3);font-weight:500}
.meta{color:var(--tx2);font-size:12.5px}
#themeBtn{margin-left:auto;border:1px solid var(--line);background:var(--panel);
  color:var(--tx2);border-radius:6px;padding:4px 10px;cursor:pointer;font:inherit;font-size:12px}
.integrity{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 16px}
.chip{background:var(--panel);border:1px solid var(--line);border-radius:6px;
  padding:5px 10px;font-size:12px;color:var(--tx2)}
.chip b{color:var(--tx);font-weight:600}
.chip.bad{border-color:var(--crit);color:var(--crit)}
.chip.warn{border-color:var(--elev);color:var(--elev)}
.filters{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 14px;align-items:center}
.fbtn{border:1px solid var(--line);background:var(--panel);color:var(--tx2);
  border-radius:16px;padding:4px 12px;cursor:pointer;font:inherit;font-size:12.5px}
.fbtn.on{background:var(--acc);border-color:var(--acc);color:#fff}
.fsep{width:1px;height:20px;background:var(--line);margin:0 4px}
table.summary{width:100%;border-collapse:collapse;background:var(--panel);
  border:1px solid var(--line);border-radius:8px;overflow:hidden;font-size:12.5px;
  margin-bottom:22px;display:block;overflow-x:auto;white-space:nowrap}
table.summary thead th{position:sticky;top:0;background:var(--panel2);color:var(--tx2);
  font-weight:600;text-align:left;padding:7px 10px;cursor:pointer;user-select:none}
table.summary td{padding:6px 10px;border-top:1px solid var(--line)}
table.summary tbody tr:hover{background:var(--panel2)}
.sig{font-weight:700}
.sig.CRITICAL{color:var(--crit)} .sig.ELEVATED{color:var(--elev)}
.sig.WATCH{color:var(--watch)} .sig.NORMAL{color:var(--norm)}
.pill{display:inline-block;border-radius:5px;padding:2px 8px;font-size:11.5px;
  font-weight:700;color:#fff}
.pill.CRITICAL{background:var(--crit)} .pill.ELEVATED{background:var(--elev)}
.pill.WATCH{background:var(--watch)} .pill.NORMAL{background:var(--norm)}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
  margin-bottom:16px;padding:14px 16px}
.ch{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.cn{font-size:16px;font-weight:700}
.ct{color:var(--tx3);font-size:12.5px}
.newbadge{font-size:11.5px;color:var(--acc);border:1px solid var(--acc);
  border-radius:5px;padding:1px 7px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(96px,1fr));
  gap:8px;margin:12px 0}
.tile{background:var(--panel2);border-radius:7px;padding:7px 10px}
.tl{font-size:10.5px;color:var(--tx3)}
.tv{font-size:15px;font-weight:650}
.tv small{font-size:11px;color:var(--tx3);font-weight:500}
.views{display:flex;flex-wrap:wrap;gap:5px;margin:4px 0 8px}
.vb{border:1px solid var(--line);background:var(--panel2);color:var(--tx2);
  border-radius:5px;padding:3px 9px;cursor:pointer;font:inherit;font-size:11.5px}
.vb.on{background:var(--acc);border-color:var(--acc);color:#fff}
.vnote{font-size:11.5px;color:var(--tx3);margin:0 0 6px;min-height:15px}
.vnote .dag{color:var(--elev)}
.cwrap{position:relative;height:270px}
details{border:1px solid var(--line);border-radius:7px;margin-top:8px;
  background:var(--panel2)}
details>summary{cursor:pointer;padding:7px 12px;font-size:12.5px;font-weight:600;
  color:var(--tx2);list-style:none;display:flex;align-items:center;gap:8px}
details>summary::before{content:"+";color:var(--tx3);font-weight:700;width:12px}
details[open]>summary::before{content:"\2013"}
details>summary .mstat{margin-left:auto;font-weight:600;font-size:12px}
.dbody{padding:4px 12px 12px;font-size:12.5px;color:var(--tx2)}
.dbody table{width:100%;border-collapse:collapse;font-size:11.5px;margin-top:6px}
.dbody th{text-align:left;color:var(--tx3);font-weight:600;padding:3px 6px;
  border-bottom:1px solid var(--line)}
.dbody td{padding:3px 6px;border-bottom:1px solid var(--line)}
.dbody .fine{font-size:10.5px;color:var(--tx3);margin-top:7px}
.pos{color:var(--norm)} .neg{color:var(--crit)} .warnc{color:var(--elev)}
.minor{color:var(--tx3)}
footer{color:var(--tx3);font-size:11.5px;margin-top:28px;line-height:1.6}
.key{display:flex;flex-wrap:wrap;gap:8px 14px;align-items:center;margin:0 0 12px;
  font-size:12.5px;color:var(--tx2)}
.key .pill{font-size:11px}
details.guide{background:var(--panel);margin:0 0 16px}
details.guide>summary{font-size:13.5px;color:var(--tx)}
.guide .dbody{color:var(--tx2);line-height:1.55;max-width:860px}
.guide h4{margin:12px 0 4px;font-size:13px;color:var(--tx)}
.guide p{margin:0 0 6px}
.guide dl{margin:0;display:grid;grid-template-columns:max-content 1fr;gap:3px 12px}
.guide dt{font-weight:600;color:var(--tx)}
.guide dd{margin:0}
[title]{cursor:help}
.why td.pts{font-weight:650;text-align:right}
.why tfoot td{font-weight:700;color:var(--tx)}
.intro{margin:4px 0 6px;color:var(--tx3);font-size:11.5px}
.td{font-size:10px;color:var(--tx3);margin-top:2px;line-height:1.3}
details>summary{flex-wrap:wrap}
details>summary .sub{flex-basis:100%;font-weight:400;font-size:11.5px;color:var(--tx3);
  padding-left:20px;margin-top:-2px}
.plain{background:var(--panel2);border-left:3px solid var(--acc);border-radius:6px;
  padding:9px 12px;margin:10px 0 4px;font-size:13px;line-height:1.5;color:var(--tx)}
.plain b{font-weight:650}
@media (max-width:640px){.cwrap{height:230px}.wrap{padding:12px 10px 40px}}
</style>
</head>
<body>
<div class="wrap">
<header>
  <h1>MAUDE Monitor <span>V4</span></h1>
  <div class="meta" id="runMeta"></div>
  <button id="themeBtn">Theme</button>
</header>
<div class="integrity" id="integrity"></div>
<div class="key" id="key"></div>
<details class="guide" id="guide"><summary>How to read this dashboard</summary><div class="dbody" id="guideBody"></div></details>
<div class="filters" id="filters"></div>
<table class="summary"><thead><tr id="sumHead"></tr></thead><tbody id="sumBody"></tbody></table>
<div id="cards"></div>
<footer id="foot"></footer>
</div>

<script type="application/json" id="payload">__PAYLOAD__</script>
<script>
"use strict";
const P = JSON.parse(document.getElementById("payload").textContent);
const $ = (s, el) => (el || document).querySelector(s);
const el = (tag, cls, html) => { const e = document.createElement(tag);
  if (cls) e.className = cls; if (html !== undefined) e.innerHTML = html; return e; };
const fmt0 = v => v == null ? "\u2014" : Number(v).toLocaleString();
const fmt1 = v => v == null ? "\u2014" : Number(v).toLocaleString(undefined,{maximumFractionDigits:1});
const fmt2 = v => v == null ? "\u2014" : Number(v).toFixed(2);
const sgn = v => v == null ? "\u2014" : (v >= 0 ? "+" : "") + Number(v).toFixed(2);
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g,
  c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

/* theme */
const themeBtn = $("#themeBtn");
themeBtn.onclick = () => {
  const r = document.documentElement;
  const dark = (r.dataset.theme || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light")) === "dark";
  r.dataset.theme = dark ? "light" : "dark";
  charts.forEach(c => { restyle(c); c.update(); });
};

/* header + integrity chips */
const M = P.meta;
$("#runMeta").textContent = `Run ${M.run_at} \u00b7 ${M.mode} \u00b7 since ${M.start_date}`;
(function(){
  const box = $("#integrity");
  const f = M.fundamentals || {};
  const fundBad = f.pct < 100;
  box.appendChild(el("div", "chip" + (fundBad ? " warn" : ""),
    `Fundamentals verified: <b>${f.verified}/${f.total}</b>` +
    (fundBad ? " \u2014 Rate/$M and Rate/10K flagged until sourced" : "")));
  const srcs = M.stock_sources || {};
  const missing = Object.keys(srcs).filter(k => srcs[k] === "unavailable");
  box.appendChild(el("div", "chip" + (missing.length ? " bad" : ""),
    missing.length ? `Market data missing: <b>${missing.join(", ")}</b>`
                   : `Market data: <b>live</b> (yfinance/stooq)`));
  box.appendChild(el("div", "chip" + (M.benchmark_ok ? "" : " bad"),
    `CAR benchmark: <b>${M.benchmark_primary}</b> (vs ${M.benchmark_secondary})`));
  if (!M.api_key_set) box.appendChild(el("div", "chip warn", "openFDA API key not set"));
  const rs = M.revenue_staleness || {};
  if (rs.stale) box.appendChild(el("div", "chip warn", `Fundamentals table: ${esc(rs.message)}`));
  if (M.prev_run) box.appendChild(el("div", "chip", `Prev run: <b>${esc(M.prev_run.slice(0,16))}</b>`));
  if ((M.transitions || []).length)
    box.appendChild(el("div", "chip warn", `${M.transitions.length} signal transition(s) this run`));
  const prov = P.devices.find(d => d.stats && d.stats.provisional);
  if (prov) box.appendChild(el("div", "chip",
    `Latest month <b>${prov.stats.latest_month}</b> is provisional (MAUDE reporting lag)`));
})();


/* definitions used for tooltips, the guide, and the summary table */
const DEFS = {
  signal: "Overall risk level from the R-score: CRITICAL 70+, ELEVATED 50-69, WATCH 30-49, NORMAL below 30. Open a card's \"Why this signal\" panel to see the point breakdown.",
  r: "R-score, 0-100. Five factors worth up to 20 points each: trend-adjusted z, 6-month slope, deaths (3mo), injuries (3mo), reports per $M revenue. Higher = more concerning.",
  z_t: "Trend-adjusted z: how far the latest month sits above or below this device's own trailing growth trend, in standard deviations. +2 means roughly a 1-in-40 month relative to the trend. This is the primary anomaly score; it does not flag a device just for growing.",
  z: "Raw trailing z: latest month vs the simple average of the prior 12 months. Rises with installed-base growth, so read it alongside the trend-adjusted z.",
  latest: "Reports FDA received in the latest complete calendar month. The current in-progress month is excluded.",
  new: "Change in total reports since the previous run of this monitor. Includes FDA backfilling older months, not only fresh events.",
  rpm: "Reports per $1M of quarterly revenue. Dagger = the revenue figures have not yet been verified against filings; do not cite.",
  r10k: "Reports per 10,000 installed-base users or systems. Dagger = installed-base figures not yet verified; do not cite.",
  slope: "Slope of a straight line fit through the last 6 months of report counts: the average change in monthly reports, per month.",
  deaths3: "Reports with event type Death in the last 3 complete months.",
  inj3: "Reports with event type Injury in the last 3 complete months.",
  rho: "Strongest Spearman rank correlation between a MAUDE signal and forward stock returns, across lags of 0-6 months. Negative at a positive lag means rising reports preceded falling prices. * = survives false-discovery correction (q < " + P.fdr_q + ").",
  latest_month: "Latest complete calendar month in the data. (prov.) = provisional: MAUDE counts for the most recent month keep rising for weeks as late reports arrive.",
  ticker: "Stock ticker used for price data and the CAR backtest.",
  name: "Device or company row. Company rows combine all brand names for that company and drive the cross-company modules."
};
const TILE_SHORT = { "R-score": "0-100 risk composite", "z trend-adj": "latest month vs own trend, in sd",
  "z raw": "latest month vs prior 12-mo avg", "Reports": "received by FDA this month",
  "6mo slope": "change in monthly reports, per month", "Deaths / Inj 3mo": "event-type counts, last 3 months",
  "Rate/10K": "reports per 10K installed base" };
const TILE_DEFS = { "R-score": DEFS.r, "z trend-adj": DEFS.z_t, "z raw": DEFS.z,
  "Reports": DEFS.latest, "6mo slope": DEFS.slope,
  "Deaths / Inj 3mo": "Death and Injury event-type reports in the last 3 complete months.",
  "Rate/10K": DEFS.r10k };

/* signal key */
(function(){
  const k = $("#key");
  k.appendChild(el("span", null, "Signal key (R-score):"));
  [["CRITICAL","70+"],["ELEVATED","50-69"],["WATCH","30-49"],["NORMAL","below 30"]].forEach(([sg, r]) => {
    const sp = el("span", null, `<span class="pill ${sg}">${sg}</span> ${r}`);
    sp.title = DEFS.signal; k.appendChild(sp); });
  k.appendChild(el("span", "minor", "\u2020 = built on unverified fundamentals \u00b7 * on a month = provisional \u00b7 hover any column or tile for its definition"));
})();

/* guide */
$("#guideBody").innerHTML = `
<p>This monitor pulls every adverse event report that device makers and users file with the FDA (the MAUDE database), counts them by month for each device we follow, and asks three questions: is the latest month unusual for this device, is the mix of events getting more serious, and has that pattern historically preceded a move in the stock?</p>
<h4>What the signal means</h4>
<p>Each device gets an R-score from 0 to 100 built from five factors, each worth up to 20 points: how far the latest month sits above the device's own trend, how steep the 6-month trend is, deaths in the last 3 months, injuries in the last 3 months, and reports per $M of revenue. CRITICAL is 70 or more, ELEVATED 50 to 69, WATCH 30 to 49, NORMAL below 30. Every card's first panel, "Why this signal", shows the exact points each factor earned and the rule it tripped, so nobody has to take the label on faith.</p>
<h4>How to read a card</h4>
<dl>
<dt>Reports chart</dt><dd>Bars are monthly reports by the date FDA received them; the dark line is the 6-month moving average; the shaded band is one standard deviation around the device's clean-month mean. Grey shaded columns are batch-filing months, where the manufacturer dumped a backlog of summary reports at once; those months are shown but excluded from every statistic so they do not create false alarms. Dashed verticals are recalls, warning letters, and launches.</dd>
<dt>Event-date view</dt><dd>The same reports counted by when the incident actually happened rather than when FDA received the paperwork. A big gap between the two in one month is how batch filings are detected.</dd>
<dt>Z-scores view</dt><dd>Red line is the trend-adjusted z (the one that matters); grey is the raw z; the dashed line at 1.5 is the backtest trigger.</dd>
<dt>Stock view</dt><dd>Reports against the share price so you can eyeball whether spikes led price moves. The CAR panel does this properly.</dd>
</dl>
<h4>The module panels</h4>
<dl>
<dt>Why this signal</dt><dd>Point-by-point breakdown of the R-score.</dd>
<dt>CAR event study</dt><dd>Backtest: every past month the trend-adjusted z crossed 1.5 (or reports jumped 30%+ with z above 1) is treated as a short signal, and the stock's return over the next 3 months is measured against the IHI medtech ETF. Hit rate = share of signals where the stock lagged IHI. The 3-month horizon is fixed in advance so the result is not cherry-picked.</dd>
<dt>MAUDE to stock correlation</dt><dd>Tests whether six versions of the report series (raw counts, changes, z-scores, acceleration, rates) correlate with stock returns 0 to 6 months later. Because that is roughly 40 tests, the p-values are corrected for multiple comparisons; only correlations with q below ${P.fdr_q} count as real.</dd>
<dt>Recall cascade position</dt><dd>Where the device sits on the typical path from rising reports, to deaths, to warning letter, to recall, to Class I recall. Scored 0 to 100 from which steps have already happened.</dd>
<dt>PRR disproportionality</dt><dd>The FDA's own pharmacovigilance measure. For each failure mode (sensor failure, adhesion, alarm, battery, etc.) it asks whether this device's narratives mention it far more often than the other monitored devices do. PRR of 2 means twice as often; with chi-squared above 4 that is a flag.</dd>
<dt>Severity-weighted score</dt><dd>Deaths count 100, injuries 10, malfunctions 1, summed per month and z-scored, so a shift toward serious events shows even when total counts are flat.</dd>
<dt>Cross-company ranking</dt><dd>Company rows ranked against each other on z, slope and (once verified) rate per 10K; rank 1 is the worst and is the short screen.</dd>
<dt>Failure modes</dt><dd>Keyword classification of the most recent few hundred report narratives.</dd>
<dt>Earnings setup and recall risk</dt><dd>Heuristic screening scores, labeled as such. They summarize the factors above into one number for triage; they are not calibrated probabilities.</dd>
<dt>New since last run</dt><dd>How many reports appeared since the monitor last ran, with the months that changed.</dd>
</dl>
<h4>Caveats that matter</h4>
<p>MAUDE reports are unverified submissions; a report does not establish that the device caused the event, and counts are not incidence rates. The newest month is always incomplete and is marked provisional. Metrics marked with a dagger rest on revenue and installed-base figures that have not yet been checked against filings and should not be quoted until they are. A run in quick mode only loads one year of history and will show "insufficient data" on the backtest and correlation modules; the standard run loads from January 2023.</p>`;

/* filters */
const state = { company: "All", signal: "All", view: "all" };
(function(){
  const bar = $("#filters");
  const mk = (label, group, val) => {
    const b = el("button", "fbtn" + ((state[group] === val) ? " on" : ""), esc(label));
    b.onclick = () => { state[group] = val;
      bar.querySelectorAll(`[data-g="${group}"]`).forEach(x => x.classList.remove("on"));
      b.classList.add("on"); applyFilters(); };
    b.dataset.g = group; return b;
  };
  bar.appendChild(mk("All companies", "company", "All"));
  P.companies.forEach(c => bar.appendChild(mk(c, "company", c)));
  bar.appendChild(el("div", "fsep"));
  ["All", "CRITICAL", "ELEVATED", "WATCH", "NORMAL"].forEach(s =>
    bar.appendChild(mk(s === "All" ? "All signals" : s, "signal", s)));
  bar.appendChild(el("div", "fsep"));
  [["all","Devices + company rows"],["individual","Devices only"],["combined","Company rows only"]]
    .forEach(([v, l]) => bar.appendChild(mk(l, "view", v)));
})();
function applyFilters(){
  document.querySelectorAll(".card").forEach(c => {
    const okC = state.company === "All" || c.dataset.company === state.company;
    const okS = state.signal === "All" || c.dataset.signal === state.signal;
    const okV = state.view === "all" || c.dataset.view === state.view;
    c.style.display = (okC && okS && okV) ? "" : "none";
  });
  document.querySelectorAll("#sumBody tr").forEach(r => {
    const okC = state.company === "All" || r.dataset.company === state.company;
    const okS = state.signal === "All" || r.dataset.signal === state.signal;
    const okV = state.view === "all" || r.dataset.view === state.view;
    r.style.display = (okC && okS && okV) ? "" : "none";
  });
}

/* summary table */
const COLS = [
  ["name","Device"],["ticker","Tkr"],["latest_month","Month"],["latest","Reports"],
  ["new","\u0394 run"],["z_t","z (trend-adj)"],["z","z (raw)"],["r","R"],
  ["rpm","Rate/$M\u2020"],["r10k","Rate/10K\u2020"],["slope","Slope"],
  ["deaths3","D 3mo"],["inj3","I 3mo"],["rho","Best \u03c1"],["signal","Signal"]];
let sortKey = "r", sortDir = -1;
function rowData(d){
  const s = d.stats || {};
  const corr = (d.modules || {}).correlation || {};
  return { name: d.name, ticker: d.ticker, latest_month: s.latest_month || "",
    latest: s.latest_value, new: d.new_since, z_t: s.z_t, z: s.z, r: d.r,
    rpm: s.rpm, r10k: s.r10k, slope: s.slope, deaths3: s.deaths3, inj3: s.inj3,
    rho: corr.status === "ok" ? corr.best_rho : null,
    rho_sig: corr.status === "ok" && corr.significant, signal: d.signal };
}
function renderSummary(){
  const head = $("#sumHead"); head.innerHTML = "";
  COLS.forEach(([k, label]) => {
    const th = el("th", null, esc(label) + (sortKey === k ? (sortDir < 0 ? " \u25be" : " \u25b4") : ""));
    if (DEFS[k]) th.title = DEFS[k];
    th.onclick = () => { sortDir = (sortKey === k) ? -sortDir : -1; sortKey = k; renderSummary(); };
    head.appendChild(th);
  });
  const rows = P.devices.filter(d => d.stats).map(d => ({ d, r: rowData(d) }));
  rows.sort((a, b) => {
    const x = a.r[sortKey], y = b.r[sortKey];
    if (x == null && y == null) return 0;
    if (x == null) return 1; if (y == null) return -1;
    return (x < y ? -1 : x > y ? 1 : 0) * -sortDir;
  });
  const body = $("#sumBody"); body.innerHTML = "";
  rows.forEach(({ d, r }) => {
    const tr = el("tr");
    tr.dataset.company = d.company; tr.dataset.signal = d.signal;
    tr.dataset.view = d.combined ? "combined" : "individual";
    const rho = r.rho == null ? "\u2014" : sgn(r.rho).replace("+0","+0") + (r.rho_sig ? "*" : "");
    tr.innerHTML =
      `<td>${esc(r.name)}${d.stats.provisional ? ' <span class="minor">(prov.)</span>' : ""}</td>` +
      `<td>${esc(r.ticker)}</td><td>${esc(r.latest_month)}</td><td>${fmt0(r.latest)}</td>` +
      `<td>${r.new == null ? "\u2014" : (r.new >= 0 ? "+" : "") + fmt0(r.new)}</td>` +
      `<td>${sgn(r.z_t)}</td><td>${sgn(r.z)}</td><td>${r.r == null ? "\u2014" : r.r}</td>` +
      `<td>${fmt1(r.rpm)}</td><td>${fmt2(r.r10k)}</td><td>${sgn(r.slope)}</td>` +
      `<td>${fmt0(r.deaths3)}</td><td>${fmt0(r.inj3)}</td><td>${rho}</td>` +
      `<td class="sig ${d.signal}">${d.signal}</td>`;
    tr.onclick = () => { const c = document.getElementById("card-" + d.id);
      if (c) c.scrollIntoView({ behavior: "smooth", block: "start" }); };
    body.appendChild(tr);
  });
  applyFilters();
}
renderSummary();

/* ----- module panels ----- */
const DESC = {
  "Why this signal": "How the R-score was built. Each row is one factor, the value observed, the rule it tripped, and the points earned.",
  "CAR event study": "Backtest of past report spikes as short signals: stock return over the next 3 months minus the IHI medtech ETF return.",
  "MAUDE \u2192 stock correlation": "Does any version of the report series correlate with stock returns 0 to 6 months later? Corrected for the ~40 tests run.",
  "Recall cascade position": "How far along the typical path from rising reports to recall this device has progressed.",
  "PRR disproportionality": "Which failure modes this device's narratives mention disproportionately vs the other monitored devices.",
  "Severity-weighted score": "Deaths x100 + injuries x10 + malfunctions, per month, z-scored. Catches a shift toward serious events.",
  "Cross-company ranking": "Company rows ranked against each other. Rank 1 is the worst (short screen).",
  "Peer R-score rank": "Company rows ranked by R-score alone.",
  "Failure modes": "Keyword classification of the most recent narratives into failure categories.",
  "Earnings setup (heuristic)": "Triage score starting at 50, adjusted by the factors listed. Not a forecast.",
  "Recall risk (heuristic)": "Triage score from z, deaths, slope, and alarm/sensor failure counts. Not a calibrated probability.",
  "FDA recalls": "Recalls from the FDA device recall database matching this product.",
  "New since last run": "Reports added since the monitor last ran, by month.",
  "Company context": "SEC filing activity in the last 90 days, active clinical trials, and payer coverage notes."
};
function panel(title, stat, statCls, bodyHtml, open){
  const key = Object.keys(DESC).find(k => title.indexOf(k) === 0);
  const sub = key ? `<span class="sub">${DESC[key]}</span>` : "";
  return `<details${open ? " open" : ""}><summary>${esc(title)}<span class="mstat ${statCls || ""}">${stat || ""}</span>${sub}</summary><div class="dbody">${bodyHtml}</div></details>`;
}
function plainSummary(d){
  const r = d.r_detail, s = d.stats; if (!r || !s) return "";
  const hits = r.components.filter(c => c.points > 0);
  const zeros = r.components.filter(c => c.points === 0);
  const sigWord = { CRITICAL: "the highest alert level", ELEVATED: "elevated",
    WATCH: "on watch", NORMAL: "normal" }[d.signal];
  let t = `<b>${esc(d.name)} is ${d.signal}</b> (R-score ${r.score} of 100, ${sigWord}). `;
  if (!hits.length) t += "None of the five risk factors tripped a threshold this month.";
  else {
    t += "Points came from " + hits.map(c => {
      const f = c.factor.replace(" (unverified inputs)", "");
      return `${f.charAt(0).toLowerCase() + f.slice(1)} at ${esc(c.value)} (${c.points} pts, rule: ${esc(c.rule)})`;
    }).join("; ") + ".";
  }
  if (zeros.length) t += " Contributing nothing: " + zeros.map(c => c.factor.replace(" (unverified inputs)", "").toLowerCase()).join(", ") + ".";
  if (s.provisional) t += ` The latest month (${esc(s.latest_month)}) is still filling in, so these readings can move.`;
  if ((s.batch || []).length) t += ` ${s.batch.length} batch-filing month${s.batch.length > 1 ? "s were" : " was"} excluded from the statistics (grey columns on the chart).`;
  return `<div class="plain">${t}</div>`;
}
function whyPanel(d){
  const r = d.r_detail; if (!r) return "";
  let b = `<table class="why"><tr><th>Factor</th><th>Value</th><th>Rule tripped</th><th>Scale</th><th>Pts</th></tr>`;
  r.components.forEach(c => {
    b += `<tr><td>${esc(c.factor)}</td><td>${esc(c.value)}</td><td>${esc(c.rule)}</td>` +
         `<td class="minor">${esc(c.scale)}</td><td class="pts ${c.points >= 15 ? "neg" : c.points >= 10 ? "warnc" : ""}">${c.points}</td></tr>`;
  });
  b += `<tfoot><tr><td colspan="4">R-score (capped at 100) → <span class="sig ${d.signal}">${d.signal}</span></td><td class="pts">${r.score}</td></tr></tfoot></table>`;
  b += `<div class="fine">${esc(r.thresholds)}. The z and slope factors use only this device's own history; deaths and injuries are absolute counts, so large-volume devices score higher on those two by construction.</div>`;
  return panel("Why this signal", `${d.signal} · R ${r.score}`, "", b, true);
}
function gradeCls(g){ return g && g.indexOf("STRONG") === 0 ? "pos" : g === "MODERATE" ? "warnc" : "neg"; }
function carPanel(d){
  const car = d.modules.car; if (!car) return "";
  if (car.status !== "ok") return panel("CAR event study", "insufficient data", "minor",
    `<div>Needs at least 12 months where both report counts and ${esc(d.ticker)} prices exist. ` +
    (M.mode === "quick" ? "This was a quick-mode run (history since " + esc(M.start_date) + "); run the workflow in standard mode to load history from 2023." :
     "Either market data for this ticker is unavailable this run (see header) or the device has too little report history.") + `</div>`);
  const s = car.summary, h = car.primary_horizon;
  let b = `<div>${esc(s.message)}</div><table><tr><th>Date</th><th>Trigger</th><th>Entry</th>` +
          `<th>CAR ${esc(h)}</th><th>vs ${esc(M.benchmark_primary)}/${esc(M.benchmark_secondary)}</th><th>P&amp;L/$10K</th></tr>`;
  car.case_studies.slice(-8).forEach(c => {
    const p = c.primary;
    if (p && p.abnormal_ret != null) {
      const cls = p.short_pnl > 0 ? "pos" : "neg";
      b += `<tr><td>${esc(c.month)}</td><td>${esc(c.trigger)}</td><td>$${c.entry}</td>` +
           `<td class="${cls}">${sgn(p.abnormal_ret)}%</td>` +
           `<td class="minor">${sgn(p.bench_ret)}% / ${sgn(p.bench2_ret)}%</td>` +
           `<td class="${cls}">$${p.short_pnl >= 0 ? "+" : ""}${fmt0(p.short_pnl)}</td></tr>`;
    } else {
      b += `<tr><td>${esc(c.month)}</td><td>${esc(c.trigger)}</td><td>$${c.entry}</td>` +
           `<td class="minor" colspan="3">no ${esc(h)} forward window yet</td></tr>`;
    }
  });
  b += "</table>";
  const ex = car.exploratory || {};
  const keys = Object.keys(ex);
  if (keys.length) {
    b += `<table><tr><th>Horizon (exploratory, in-sample)</th><th>n</th><th>Hit</th><th>Mean CAR</th><th>P&amp;L</th></tr>`;
    keys.forEach(k => { const e = ex[k];
      b += `<tr><td>${esc(k)}${k === h ? " (pre-specified)" : ""}</td><td>${e.n}</td>` +
           `<td>${e.hit_rate}%</td><td>${sgn(e.mean_car)}%</td><td>$${e.total_pnl >= 0 ? "+" : ""}${fmt0(e.total_pnl)}</td></tr>`; });
    b += "</table>";
  }
  b += `<div class="fine">Headline grade uses only the pre-specified ${esc(h)} horizon (no ex-post horizon selection). CAR = stock return minus ${esc(M.benchmark_primary)} return. Entries: trend-adjusted z \u2265 1.5 or MoM \u2265 30% with z \u2265 1; batch months excluded; 3-month cooldown between signals.</div>`;
  return panel("CAR event study (" + h + ", sector-adjusted)",
    `${esc(s.grade)} \u00b7 ${s.hit_rate}% \u00b7 n=${s.total}`, gradeCls(s.grade), b);
}
function corrPanel(d){
  const c = d.modules.correlation; if (!c) return "";
  if (c.status !== "ok") return panel("MAUDE \u2192 stock correlation", "insufficient data", "minor",
    `<div>${esc(c.message || "")} Needs 14+ overlapping months of reports and prices. ` +
    (M.mode === "quick" ? "Quick-mode run; use the standard run for full history." : "") + `</div>`);
  let b = `<div>${esc(c.message)}</div><table><tr><th>Signal</th><th>\u03c1</th><th>Lag</th><th>p</th><th>q (FDR)</th><th>Sig</th></tr>`;
  Object.entries(c.signal_analysis).forEach(([sn, s]) => {
    const cls = s.significant ? (s.best_rho < 0 ? "pos" : "neg") : "minor";
    const bold = sn === c.best_signal ? ' style="font-weight:650"' : "";
    b += `<tr${bold}><td>${esc(sn)}</td><td class="${cls}">${sgn(s.best_rho)}</td>` +
         `<td>${s.best_lag}mo</td><td>${s.best_p.toFixed(4)}</td><td>${s.best_q.toFixed(4)}</td>` +
         `<td>${s.significant ? "\u2713" : "\u2014"}</td></tr>`;
  });
  b += "</table>";
  if ((c.excluded_signals || []).length)
    b += `<div class="fine">Excluded: ${esc(c.excluded_signals.join("; "))}.</div>`;
  b += `<div class="fine">Significance = Benjamini-Hochberg q &lt; ${P.fdr_q} across all ${c.tests_run} signal \u00d7 lag tests. Negative \u03c1 at positive lag means reports lead stock declines.</div>`;
  const stat = c.significant ? `\u03c1=${sgn(c.best_rho)} q=${c.best_q.toFixed(3)}` : "none survive FDR";
  return panel("MAUDE \u2192 stock correlation (FDR-corrected)", stat,
    c.significant ? (c.best_rho < 0 ? "pos" : "warnc") : "minor", b);
}
function prrPanel(d){
  const p = d.modules.prr; if (!p) return "";
  if (p.status !== "ok") return panel("PRR disproportionality", "n/a", "minor",
    `<div>${esc(p.message || "Insufficient narratives")}</div>`);
  let b = `<div>${esc(p.message)}</div>`;
  if ((p.prr_signals || []).length) {
    b += `<table><tr><th>Failure mode</th><th>n</th><th>PRR</th><th>\u03c7\u00b2</th><th>Signal</th></tr>`;
    p.prr_signals.slice(0, 6).forEach(s => {
      b += `<tr><td>${esc(s.mode)}</td><td>${s.count}</td>` +
           `<td class="${s.signal ? "neg" : "minor"}">${s.prr.toFixed(1)}</td>` +
           `<td>${s.chi2.toFixed(1)}</td><td>${s.signal ? "\u2713" : "\u2014"}</td></tr>`;
    });
    b += "</table>";
  }
  b += `<div class="fine">PRR \u2265 2 with \u03c7\u00b2 \u2265 4 flags a disproportionate failure mode vs ${fmt0(p.comparator_n)} narratives from the other monitored devices (overlapping company rows excluded from the comparator).</div>`;
  const cls = p.level === "ALERT" ? "neg" : p.level === "WATCH" ? "warnc" : "pos";
  return panel("PRR disproportionality", `${esc(p.level)} (${p.sig_count})`, cls, b);
}
function cascadePanel(d){
  const c = d.modules.cascade; if (!c || c.status !== "ok") return "";
  let b = `<div>${esc(c.message)}</div>`;
  (c.steps || []).forEach(s => {
    const cls = ["CRITICAL","ISSUED","CONFIRMED","ACTIVE"].includes(s.status) ? "neg"
      : s.status === "WATCH" ? "warnc" : "pos";
    b += `<div><span class="${cls}">\u25cf ${esc(s.status)}</span> ${esc(s.step)}: ${esc(s.detail)}</div>`;
  });
  const cls = c.score >= 70 ? "neg" : c.score >= 40 ? "warnc" : "pos";
  return panel("Recall cascade position", `${esc(c.phase)} (${c.score})`, cls, b);
}
function swPanel(d){
  const s = d.modules.severity_weighted; if (!s || s.status !== "ok") return "";
  const cls = s.level === "CRITICAL" ? "neg" : s.level === "ELEVATED" ? "warnc" : "pos";
  return panel("Severity-weighted score (D\u00d7100 + I\u00d710 + M)",
    `${esc(s.level)} (${fmt0(s.latest)})`, cls, `<div>${esc(s.message)}</div>`);
}
function xcPanel(d){
  const x = d.modules.cross_company; if (!x || x.status !== "ok") return "";
  let b = `<div>${esc(x.message)}</div><table><tr><th>#</th><th>Company</th><th>Rate/10K\u2020</th><th>Slope</th><th>z adj</th><th>Comp.</th></tr>`;
  (x.ranking || []).forEach((p, i) => {
    const bold = p.ticker === d.ticker ? ' style="font-weight:650"' : "";
    b += `<tr${bold}><td>${i + 1}</td><td>${esc(p.name)}</td>` +
         `<td>${p.rate == null ? "\u2014" : fmt2(p.rate) + (p.rate_verified ? "" : "\u2020")}</td>` +
         `<td>${sgn(p.slope)}</td><td>${sgn(p.z)}</td><td>${p.composite.toFixed(3)}</td></tr>`;
  });
  b += `</table><div class="fine">${x.composite_uses_rate ? "" : "Composite ranks on trend-adjusted z and slope only; Rate/10K excluded while installed-base series are unverified."}</div>`;
  const cls = /WORST|WEAK/.test(x.signal) ? "neg" : /BEST|STRONG/.test(x.signal) ? "pos" : "minor";
  return panel("Cross-company ranking", esc(x.signal), cls, b);
}
function fmPanel(d){
  const f = d.modules.failure_modes; if (!f || f.status !== "ok") return "";
  let b = `<table><tr><th>Mode</th><th>n</th><th>%</th></tr>`;
  (f.top_modes || []).forEach(t => {
    b += `<tr><td>${esc(t.mode)}</td><td>${t.count}</td><td>${t.pct}%</td></tr>`; });
  b += `</table><div class="fine">Keyword classification over the ${fmt0(f.total)} most recent narratives.</div>`;
  return panel("Failure modes", `${fmt0(f.total)} narratives`, "minor", b);
}
function epPanel(d){
  const e = d.modules.earnings_pred; if (!e || e.status !== "ok") return "";
  let b = `<div>${esc(e.message)}</div>`;
  (e.factors || []).forEach(([name, v]) =>
    b += `<div><span class="${v < 0 ? "neg" : "pos"}">${v > 0 ? "+" : ""}${v}</span> ${esc(name)}</div>`);
  const cls = e.outlook === "POSITIVE" ? "pos" : e.outlook === "NEGATIVE" ? "neg" : "warnc";
  return panel("Earnings setup (heuristic)", `${esc(e.outlook)} (${e.score})`, cls, b);
}
function rpPanel(d){
  const r = d.modules.recall_prob; if (!r || r.status !== "ok") return "";
  const cls = r.level === "HIGH" ? "neg" : r.level === "MODERATE" ? "warnc" : "pos";
  return panel("Recall risk (heuristic)", `${esc(r.level)} (${r.probability})`, cls,
    `<div>${esc(r.message)}</div>`);
}
function recallsPanel(d){
  const r = d.modules.recalls; if (!r || r.status !== "ok" || !r.count) return "";
  let b = `<table><tr><th>Initiated</th><th>Class</th><th>Status</th><th>Reason</th></tr>`;
  (r.recalls || []).forEach(x => {
    b += `<tr><td>${esc(x.initiated)}</td><td>${esc(x.classification)}</td>` +
         `<td>${esc(x.status)}</td><td>${esc(x.reason)}</td></tr>`; });
  b += `</table><div class="fine">FDA device recall database, matched on product description.</div>`;
  return panel("FDA recalls", esc(r.message), r.class1_count ? "neg" : "minor", b);
}
function contextPanel(d){
  const e = d.modules.edgar, t = d.modules.trials, pay = d.modules.payer;
  if (!e && !t && !pay) return "";
  let b = "";
  if (e) b += `<div>EDGAR: ${esc(e.message || e.status)}</div>`;
  if (t) b += `<div>ClinicalTrials.gov: ${esc(t.message || t.status)}</div>`;
  if (pay) b += `<div>Payer: ${esc(pay.message || "")}</div>`;
  return panel("Company context", "", "", b);
}
function peerPanel(d){
  const p = d.modules.peer_relative; if (!p) return "";
  const cls = /WORST|WEAK/.test(p.signal) ? "neg" : /BEST|STRONG/.test(p.signal) ? "pos" : "minor";
  return panel("Peer R-score rank", esc(p.message), cls,
    `<table><tr><th>Ticker</th><th>R</th></tr>` +
    p.peers.map(x => `<tr${x[0] === d.ticker ? ' style="font-weight:650"' : ""}><td>${esc(x[0])}</td><td>${x[1]}</td></tr>`).join("") +
    `</table>`);
}
function diffPanel(d){
  if (d.new_since == null) return "";
  let b = `<div>${d.new_since >= 0 ? "+" : ""}${fmt0(d.new_since)} reports vs previous run` +
          (M.prev_run ? ` (${esc(M.prev_run.slice(0,16))})` : "") + `.</div>`;
  if ((d.changed_months || []).length) {
    b += `<table><tr><th>Month</th><th>\u0394</th></tr>`;
    d.changed_months.forEach(c =>
      b += `<tr><td>${esc(c.month)}</td><td>${c.delta >= 0 ? "+" : ""}${fmt0(c.delta)}</td></tr>`);
    b += `</table><div class="fine">Month-level changes include MAUDE backfilling older periods, not just fresh reports.</div>`;
  }
  return panel("New since last run", (d.new_since >= 0 ? "+" : "") + fmt0(d.new_since),
    d.new_since > 0 ? "warnc" : "minor", b);
}

/* ----- cards ----- */
const VIEWS = [["reports","Reports"],["eventdate","Event-date"],["rate10k","Rate/10K\u2020"],
  ["ratem","Rate/$M\u2020"],["severity","Severity"],["z","Z-scores"],["stock","Stock"]];
const cardsBox = $("#cards");
P.devices.forEach(d => {
  const card = el("div", "card");
  card.id = "card-" + d.id;
  card.dataset.company = d.company; card.dataset.signal = d.signal;
  card.dataset.view = d.combined ? "combined" : "individual";
  const s = d.stats;
  let h = `<div class="ch"><span class="cn">${esc(d.name)}</span>` +
          `<span class="ct">${esc(d.ticker)}${d.combined ? " \u00b7 company row" : ""}</span>` +
          `<span class="pill ${d.signal}">${d.signal}</span>` +
          (d.new_since ? `<span class="newbadge">${d.new_since >= 0 ? "+" : ""}${fmt0(d.new_since)} since last run</span>` : "") +
          `</div>`;
  if (!s) { card.innerHTML = h + `<div class="vnote">No MAUDE data returned for this query (field tried: ${esc(d.brand_field)}).</div>`;
    cardsBox.appendChild(card); return; }
  h += `<div class="tiles">` +
    `<div class="tile" title="${esc(TILE_DEFS["R-score"])}"><div class="tl">R-score</div><div class="tv">${d.r == null ? "\u2014" : d.r}</div><div class="td">${TILE_SHORT["R-score"]}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["z trend-adj"])}"><div class="tl">z trend-adj</div><div class="tv">${sgn(s.z_t)}</div><div class="td">${TILE_SHORT["z trend-adj"]}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["z raw"])}"><div class="tl">z raw</div><div class="tv">${sgn(s.z)}</div><div class="td">${TILE_SHORT["z raw"]}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["Reports"] + (s.provisional ? " " + DEFS.latest_month : ""))}"><div class="tl">Reports ${esc(s.latest_month)}${s.provisional ? " (prov.)" : ""}</div><div class="tv">${fmt0(s.latest_value)}</div><div class="td">${TILE_SHORT["Reports"]}${s.provisional ? ", still filling in" : ""}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["6mo slope"])}"><div class="tl">6mo slope</div><div class="tv">${sgn(s.slope)}</div><div class="td">${TILE_SHORT["6mo slope"]}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["Deaths / Inj 3mo"])}"><div class="tl">Deaths / Inj 3mo</div><div class="tv">${fmt0(s.deaths3)} / ${fmt0(s.inj3)}</div><div class="td">${TILE_SHORT["Deaths / Inj 3mo"]}</div></div>` +
    `<div class="tile" title="${esc(TILE_DEFS["Rate/10K"])}"><div class="tl">Rate/10K\u2020</div><div class="tv">${fmt2(s.r10k)}${d.base_verified ? "" : " <small>unv.</small>"}</div><div class="td">${TILE_SHORT["Rate/10K"]}${d.base_verified ? "" : " (inputs unverified)"}</div></div>` +
    `</div>`;
  h += `<div class="views">` + VIEWS.map(([v, l], i) =>
    `<button class="vb${i === 0 ? " on" : ""}" data-v="${v}">${l}</button>`).join("") +
    `<button class="vb" data-v="__reset">Reset zoom</button></div>`;
  h += `<div class="vnote" id="note-${d.id}"></div><div class="cwrap"><canvas id="cv-${d.id}"></canvas></div>`;
  h += plainSummary(d) + whyPanel(d) + carPanel(d) + corrPanel(d) + cascadePanel(d) + prrPanel(d) + swPanel(d) +
       xcPanel(d) + peerPanel(d) + fmPanel(d) + epPanel(d) + rpPanel(d) +
       recallsPanel(d) + diffPanel(d) + contextPanel(d);
  card.innerHTML = h;
  cardsBox.appendChild(card);
  card.querySelectorAll(".vb").forEach(b => b.onclick = () => {
    if (b.dataset.v === "__reset") { const c = chartMap[d.id]; if (c) c.resetZoom(); return; }
    card.querySelectorAll(".vb").forEach(x => { if (x.dataset.v !== "__reset") x.classList.remove("on"); });
    b.classList.add("on");
    setView(d, b.dataset.v);
  });
});

/* ----- charts ----- */
const charts = [], chartMap = {}, viewMap = {};
const eventsPlugin = {
  id: "mmEvents",
  afterDraw(chart){
    const d = chart.$mm; if (!d) return;
    const { ctx, chartArea, scales } = chart;
    const x = scales.x; if (!x) return;
    ctx.save();
    (d.stats.batch || []).forEach(m => {
      const i = d.stats.months.indexOf(m); if (i < 0) return;
      const px = x.getPixelForValue(i), w = Math.max(6, x.getPixelForValue(1) - x.getPixelForValue(0));
      ctx.fillStyle = "rgba(150,150,150,.14)";
      ctx.fillRect(px - w / 2, chartArea.top, w, chartArea.bottom - chartArea.top);
    });
    (d.events || []).forEach(ev => {
      const i = d.stats.months.indexOf(ev.date); if (i < 0) return;
      const px = x.getPixelForValue(i);
      ctx.strokeStyle = ev.type === "recall" ? css("--crit") :
        ev.type === "regulatory" ? css("--elev") : css("--acc");
      ctx.setLineDash([4, 3]); ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(px, chartArea.top); ctx.lineTo(px, chartArea.bottom); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = ctx.strokeStyle;
      ctx.font = "10px Instrument Sans, sans-serif";
      ctx.save(); ctx.translate(px + 3, chartArea.top + 4); ctx.rotate(Math.PI / 2);
      ctx.fillText(ev.label.slice(0, 26), 0, 0); ctx.restore();
    });
    ctx.restore();
  }
};
Chart.register(eventsPlugin);

function baseOpts(){
  return { responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { labels: { color: css("--tx2"), boxWidth: 12, font: { size: 11 } } },
      tooltip: { callbacks: {} },
      zoom: { zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: "x" },
              pan: { enabled: true, mode: "x" } }
    },
    scales: {
      x: { type: "category", ticks: { color: css("--tx3"), maxRotation: 45, font: { size: 10 } },
           grid: { color: "transparent" } },
      y: { ticks: { color: css("--tx3"), font: { size: 10 } },
           grid: { color: css("--line") }, beginAtZero: true }
    }};
}
function restyle(chart){
  const o = chart.options;
  o.plugins.legend.labels.color = css("--tx2");
  o.scales.x.ticks.color = css("--tx3");
  o.scales.y.ticks.color = css("--tx3");
  o.scales.y.grid.color = css("--line");
  if (o.scales.y1) { o.scales.y1.ticks.color = css("--tx3"); }
}
function datasetsFor(d, view){
  const s = d.stats, n = s.months.length;
  const note = $("#note-" + d.id);
  const unv = " \u2020 built on unverified fundamentals; verify in mm_fundamentals.py before citing";
  let sets = [], y1 = null, noteTxt = "";
  if (view === "reports") {
    sets = [
      { type: "line", label: "+1\u03c3 band", data: Array(n).fill(s.s1hi), borderWidth: 0,
        pointRadius: 0, backgroundColor: css("--band"), fill: "+1", order: 9 },
      { type: "line", label: "\u22121\u03c3", data: Array(n).fill(s.s1lo), borderWidth: 0,
        pointRadius: 0, order: 9 },
      { type: "bar", label: "Reports (date received)", data: s.values,
        backgroundColor: css("--acc") + "99", order: 5 },
      { type: "line", label: "6mo MA", data: s.ma6, borderColor: css("--tx2"),
        borderWidth: 1.5, pointRadius: 0, tension: .2, order: 1 }];
    noteTxt = "Shaded columns = batch-reporting months (excluded from z baselines and backtests). Dashed verticals = events.";
  } else if (view === "eventdate") {
    sets = [
      { type: "bar", label: "Reports by event date", data: s.event_values,
        backgroundColor: css("--elev") + "88", order: 5 },
      { type: "line", label: "By date received", data: s.values, borderColor: css("--acc"),
        borderWidth: 1.5, pointRadius: 0, tension: .2 }];
    noteTxt = "Event-date series shows when incidents occurred; gaps vs date-received reveal batch filings. Recent event-date months fill in slowly.";
  } else if (view === "rate10k") {
    sets = [{ type: "line", label: "Reports per 10K installed base", data: s.rate_10k,
      borderColor: css("--acc"), borderWidth: 2, pointRadius: 0, tension: .2, spanGaps: true }];
    noteTxt = (d.base_verified ? "Installed-base series verified." : "") +
      (d.base_verified ? "" : unv);
  } else if (view === "ratem") {
    sets = [{ type: "line", label: "Reports per $M quarterly revenue", data: s.rate_m,
      borderColor: css("--acc"), borderWidth: 2, pointRadius: 0, tension: .2, spanGaps: true }];
    noteTxt = d.rev_verified ? "Revenue series verified." : unv;
  } else if (view === "severity") {
    sets = [
      { type: "bar", label: "Deaths", data: s.deaths, backgroundColor: css("--crit"), stack: "s" },
      { type: "bar", label: "Injuries", data: s.injuries, backgroundColor: css("--elev"), stack: "s" },
      { type: "bar", label: "Malfunctions", data: s.malfs, backgroundColor: css("--acc") + "88", stack: "s" }];
    noteTxt = "Stacked monthly counts by MAUDE event type.";
  } else if (view === "z") {
    sets = [
      { type: "line", label: "Trend-adjusted z", data: s.z_adj, borderColor: css("--crit"),
        borderWidth: 2, pointRadius: 0, tension: .2 },
      { type: "line", label: "Raw trailing z", data: s.z_raw, borderColor: css("--tx3"),
        borderWidth: 1.2, pointRadius: 0, tension: .2 },
      { type: "line", label: "+1.5\u03c3 trigger", data: Array(n).fill(1.5),
        borderColor: css("--elev"), borderDash: [5, 4], borderWidth: 1, pointRadius: 0 }];
    noteTxt = "Trend-adjusted z measures the residual vs the device's own trailing growth trend; raw z flags growth itself.";
  } else if (view === "stock") {
    const anyStock = (s.stock || []).some(v => v != null);
    sets = [
      { type: "bar", label: "Reports", data: s.values, backgroundColor: css("--acc") + "55",
        yAxisID: "y", order: 5 },
      { type: "line", label: d.ticker + " price", data: s.stock, borderColor: css("--crit"),
        borderWidth: 2, pointRadius: 0, tension: .2, yAxisID: "y1", spanGaps: true }];
    y1 = { position: "right", ticks: { color: css("--tx3"), font: { size: 10 } },
           grid: { drawOnChartArea: false } };
    noteTxt = anyStock ? "Live prices (" + esc((M.stock_sources || {})[d.ticker] || "") + ")."
                       : "No market data available for " + esc(d.ticker) + " this run.";
  }
  note.innerHTML = noteTxt.indexOf("\u2020") >= 0
    ? noteTxt.replace("\u2020", '<span class="dag">\u2020</span>') : noteTxt;
  return { sets, y1 };
}
function setView(d, view){
  viewMap[d.id] = view;
  const chart = chartMap[d.id]; if (!chart) return;
  const { sets, y1 } = datasetsFor(d, view);
  chart.data.datasets = sets;
  chart.options.scales.y.beginAtZero = view !== "z";
  if (y1) chart.options.scales.y1 = y1; else delete chart.options.scales.y1;
  chart.resetZoom(); chart.update();
}
function initChart(d){
  const cv = document.getElementById("cv-" + d.id);
  if (!cv || chartMap[d.id] || !d.stats) return;
  const labels = d.stats.months.map((m, i) =>
    (d.stats.provisional && i === d.stats.months.length - 1) ? m + "*" : m);
  const chart = new Chart(cv.getContext("2d"), {
    data: { labels, datasets: [] }, options: baseOpts() });
  chart.$mm = d;
  chartMap[d.id] = chart; charts.push(chart);
  setView(d, viewMap[d.id] || "reports");
}
const io = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (e.isIntersecting) {
      const id = e.target.id.replace("cv-", "");
      const d = P.devices.find(x => x.id === id);
      if (d) initChart(d);
      io.unobserve(e.target);
    }
  });
}, { rootMargin: "250px" });
document.querySelectorAll("canvas[id^='cv-']").forEach(c => io.observe(c));

/* footer */
$("#foot").innerHTML =
  `Data: FDA MAUDE via openFDA (adverse event reports are unverified submissions and ` +
  `do not establish causation; counts shift as FDA backfills). \u2020 marks metrics built on ` +
  `fundamentals not yet verified against filings. Latest month marked * is provisional due ` +
  `to reporting lag. Internal research tool \u2014 not investment advice.`;
</script>
</body>
</html>
"""
