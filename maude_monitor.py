"""
MAUDE Multi-Device Failure Rate Monitor (v2)
=============================================
Granular product-level monitoring with statistical significance,
revenue-normalized failure rates, and leading indicator analysis.

Usage:
    python maude_monitor.py --html
"""

import json
import csv
import os
import sys
import time
import argparse
import smtplib
import statistics
import math
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from urllib.request import urlopen, Request


# ============================================================
# CONFIGURATION
# ============================================================
CONFIG = {
    "sigma_threshold": 2.0,
    "lookback_years": 3,
    "data_dir": "data",
    "output_dir": "docs",
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
}

# Each entry is a specific product line searchable in MAUDE
# group_id ties sub-products to a parent company for comparison
DEVICES = [
    # --- INSULET / PODD ---
    {
        "id": "OMNIPOD5",
        "name": "Omnipod 5",
        "ticker": "PODD",
        "group_id": "PODD",
        "search": 'device.brand_name:"omnipod 5"',
        "recall_firm": "insulet",
        "quarterly_revenue": {
            "2023-Q1": 320, "2023-Q2": 340, "2023-Q3": 360, "2023-Q4": 345,
            "2024-Q1": 410, "2024-Q2": 440, "2024-Q3": 470, "2024-Q4": 455,
            "2025-Q1": 520, "2025-Q2": 580, "2025-Q3": 650, "2025-Q4": 720,
        },
    },
    {
        "id": "OMNIPOD_DASH",
        "name": "Omnipod DASH",
        "ticker": "PODD",
        "group_id": "PODD",
        "search": 'device.brand_name:"omnipod dash"',
        "recall_firm": "insulet",
        "quarterly_revenue": {
            "2023-Q1": 80, "2023-Q2": 78, "2023-Q3": 72, "2023-Q4": 66,
            "2024-Q1": 68, "2024-Q2": 65, "2024-Q3": 64, "2024-Q4": 61,
            "2025-Q1": 60, "2025-Q2": 60, "2025-Q3": 58, "2025-Q4": 62,
        },
    },
    {
        "id": "OMNIPOD_ALL",
        "name": "All Omnipod (combined)",
        "ticker": "PODD",
        "group_id": "PODD",
        "search": "device.brand_name:omnipod",
        "recall_firm": "insulet",
        "quarterly_revenue": {
            "2023-Q1": 400, "2023-Q2": 418, "2023-Q3": 432, "2023-Q4": 411,
            "2024-Q1": 478, "2024-Q2": 505, "2024-Q3": 534, "2024-Q4": 516,
            "2025-Q1": 580, "2025-Q2": 640, "2025-Q3": 708, "2025-Q4": 782,
        },
    },
    # --- DEXCOM / DXCM ---
    {
        "id": "DEXCOM_G7",
        "name": "Dexcom G7",
        "ticker": "DXCM",
        "group_id": "DXCM",
        "search": 'device.brand_name:"dexcom g7"',
        "recall_firm": "dexcom",
        "quarterly_revenue": {
            "2023-Q1": 200, "2023-Q2": 350, "2023-Q3": 500, "2023-Q4": 600,
            "2024-Q1": 550, "2024-Q2": 650, "2024-Q3": 650, "2024-Q4": 750,
            "2025-Q1": 750, "2025-Q2": 800, "2025-Q3": 850, "2025-Q4": 900,
        },
    },
    {
        "id": "DEXCOM_G6",
        "name": "Dexcom G6",
        "ticker": "DXCM",
        "group_id": "DXCM",
        "search": 'device.brand_name:"dexcom g6"',
        "recall_firm": "dexcom",
        "quarterly_revenue": {
            "2023-Q1": 521, "2023-Q2": 521, "2023-Q3": 475, "2023-Q4": 410,
            "2024-Q1": 340, "2024-Q2": 354, "2024-Q3": 344, "2024-Q4": 363,
            "2025-Q1": 300, "2025-Q2": 300, "2025-Q3": 300, "2025-Q4": 300,
        },
    },
    {
        "id": "DEXCOM_ALL",
        "name": "All Dexcom (combined)",
        "ticker": "DXCM",
        "group_id": "DXCM",
        "search": "device.brand_name:dexcom",
        "recall_firm": "dexcom",
        "quarterly_revenue": {
            "2023-Q1": 721, "2023-Q2": 871, "2023-Q3": 975, "2023-Q4": 1010,
            "2024-Q1": 890, "2024-Q2": 1004, "2024-Q3": 994, "2024-Q4": 1113,
            "2025-Q1": 1050, "2025-Q2": 1100, "2025-Q3": 1150, "2025-Q4": 1200,
        },
    },
    # --- TANDEM / TNDM ---
    {
        "id": "TSLIM_X2",
        "name": "Tandem t:slim X2",
        "ticker": "TNDM",
        "group_id": "TNDM",
        "search": 'device.brand_name:"t:slim"',
        "recall_firm": "tandem",
        "quarterly_revenue": {
            "2023-Q1": 130, "2023-Q2": 150, "2023-Q3": 155, "2023-Q4": 185,
            "2024-Q1": 140, "2024-Q2": 160, "2024-Q3": 165, "2024-Q4": 200,
            "2025-Q1": 155, "2025-Q2": 175, "2025-Q3": 180, "2025-Q4": 205,
        },
    },
    {
        "id": "MOBI",
        "name": "Tandem Mobi",
        "ticker": "TNDM",
        "group_id": "TNDM",
        "search": 'device.brand_name:"mobi"',
        "recall_firm": "tandem",
        "quarterly_revenue": {
            "2024-Q1": 31, "2024-Q2": 35, "2024-Q3": 35, "2024-Q4": 35,
            "2025-Q1": 30, "2025-Q2": 35, "2025-Q3": 35, "2025-Q4": 35,
        },
    },
]


# ============================================================
# API HELPERS
# ============================================================
def api_fetch(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "MAUDE-Monitor/2.0"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 3 * (attempt + 1)
            print(f"  Retry {attempt + 1}/{retries} (wait {wait}s): {e}")
            time.sleep(wait)
    return None


def fetch_ae_counts(device, start_date, end_date):
    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    url = (
        f"https://api.fda.gov/device/event.json"
        f"?search={device['search']}+AND+date_received:[{start}+TO+{end}]"
        f"&count=date_received"
    )
    print(f"  Fetching: {device['id']} ({start}-{end})")
    data = api_fetch(url)
    return data.get("results", []) if data else []


def fetch_event_types(device, months_back=3):
    end = datetime.now()
    start = end - timedelta(days=months_back * 30)
    url = (
        f"https://api.fda.gov/device/event.json"
        f"?search={device['search']}+AND+date_received:"
        f"[{start.strftime('%Y%m%d')}+TO+{end.strftime('%Y%m%d')}]"
        f"&count=event_type.exact"
    )
    try:
        data = api_fetch(url)
        return {
            r["term"]: r["count"]
            for r in (data.get("results", []) if data else [])
        }
    except Exception:
        return {}


# ============================================================
# DATA PROCESSING
# ============================================================
def aggregate_monthly(daily_counts):
    months = {}
    for entry in daily_counts:
        key = entry["time"][:6]
        label = f"{key[:4]}-{key[4:6]}"
        if label not in months:
            months[label] = 0
        months[label] += entry["count"]
    return dict(sorted(months.items()))


def get_quarter(month_key):
    y, m = month_key.split("-")
    q = (int(m) - 1) // 3 + 1
    return f"{y}-Q{q}"


def compute_stats(values):
    if len(values) < 2:
        return {"mean": values[0] if values else 0, "sd": 0}
    return {
        "mean": round(statistics.mean(values)),
        "sd": round(statistics.stdev(values)),
    }


def compute_moving_average(data, window=6):
    result = []
    for i in range(len(data)):
        sl = data[max(0, i - window + 1) : i + 1]
        result.append(round(sum(sl) / len(sl)))
    return result


def z_score(value, mean, sd):
    return round((value - mean) / sd, 2) if sd > 0 else 0


def trend_slope(values, window=6):
    """Compute linear regression slope over last N values."""
    recent = values[-window:] if len(values) >= window else values
    n = len(recent)
    if n < 3:
        return 0
    x_mean = (n - 1) / 2
    y_mean = sum(recent) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(recent))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return round(num / den, 1) if den != 0 else 0


def rate_vs_revenue_divergence(rates, counts, revenues_monthly):
    """
    Compute the growth rate of reports vs growth rate of revenue
    over the trailing 12 months. Positive = reports growing faster.
    """
    if len(counts) < 12 or len(revenues_monthly) < 12:
        return None
    first_half_reports = sum(counts[-12:-6])
    second_half_reports = sum(counts[-6:])
    first_half_rev = sum(revenues_monthly[-12:-6])
    second_half_rev = sum(revenues_monthly[-6:])
    if first_half_reports == 0 or first_half_rev == 0:
        return None
    report_growth = (second_half_reports / first_half_reports - 1) * 100
    rev_growth = (second_half_rev / first_half_rev - 1) * 100
    return round(report_growth - rev_growth, 1)


# ============================================================
# DEVICE REFRESH
# ============================================================
def refresh_device(device):
    data_dir = Path(CONFIG["data_dir"])
    data_dir.mkdir(exist_ok=True)
    csv_path = data_dir / f"{device['id']}_monthly.csv"

    end = datetime.now()
    start = end.replace(year=end.year - CONFIG["lookback_years"])
    daily = fetch_ae_counts(device, start, end)

    if not daily:
        print(f"  No data returned for {device['id']}")
        return None

    monthly = aggregate_monthly(daily)
    months = list(monthly.keys())
    counts = list(monthly.values())

    stats_1yr = (
        compute_stats(counts[-12:]) if len(counts) >= 12 else compute_stats(counts)
    )
    stats_all = compute_stats(counts)
    ma = compute_moving_average(counts, 6)
    event_mix = fetch_event_types(device)

    # Build enriched rows
    rows = []
    revenues_monthly = []
    rate_values = []
    for i, (month, count) in enumerate(zip(months, counts)):
        q = get_quarter(month)
        q_rev = device["quarterly_revenue"].get(q)
        mo_rev = round(q_rev / 3, 1) if q_rev else None
        rate = round(count / mo_rev, 1) if mo_rev and mo_rev > 0 else None
        z = z_score(count, stats_1yr["mean"], stats_1yr["sd"])
        revenues_monthly.append(mo_rev or 0)
        if rate is not None:
            rate_values.append(rate)

        rows.append({
            "month": month,
            "reports": count,
            "ma_6mo": ma[i],
            "avg_1yr": stats_1yr["mean"],
            "sd_1yr": stats_1yr["sd"],
            "z_score": z,
            "rate_per_m": rate,
            "qtr_revenue": q_rev,
            "mo_revenue": mo_rev,
        })

    # Write CSV
    fieldnames = [
        "month", "reports", "ma_6mo", "avg_1yr", "sd_1yr",
        "z_score", "rate_per_m", "qtr_revenue", "mo_revenue",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} months to {csv_path}")

    # Analytics
    latest = rows[-1] if rows else {}
    rates = [r["rate_per_m"] for r in rows if r["rate_per_m"] is not None]
    rate_avg = round(sum(rates) / len(rates), 1) if rates else 0
    recent_3_rates = rates[-3:] if len(rates) >= 3 else rates
    rate_elevated = len(recent_3_rates) == 3 and all(
        r > rate_avg * 1.2 for r in recent_3_rates
    )
    report_slope = trend_slope(counts)
    rate_slope = trend_slope(rate_values) if len(rate_values) >= 3 else 0
    divergence = rate_vs_revenue_divergence(rates, counts, revenues_monthly)

    # Rate z-score (is the current rate anomalous vs its own history?)
    rate_stats = compute_stats(rates) if len(rates) >= 6 else {"mean": 0, "sd": 0}
    current_rate = rates[-1] if rates else 0
    rate_z = z_score(current_rate, rate_stats["mean"], rate_stats["sd"])

    # Event mix severity score (% injury + death)
    ev_total = sum(event_mix.values()) if event_mix else 0
    severity_pct = (
        round((event_mix.get("Injury", 0) + event_mix.get("Death", 0))
              / ev_total * 100, 1)
        if ev_total > 0 else 0
    )

    return {
        "device": device,
        "latest": latest,
        "stats_1yr": stats_1yr,
        "stats_all": stats_all,
        "event_mix": event_mix,
        "rate_avg": rate_avg,
        "rate_elevated": rate_elevated,
        "rate_z": rate_z,
        "rate_stats": rate_stats,
        "report_slope": report_slope,
        "rate_slope": rate_slope,
        "divergence": divergence,
        "severity_pct": severity_pct,
        "months_count": len(rows),
        "total_reports": sum(counts),
        "alert": (
            abs(latest.get("z_score", 0)) >= CONFIG["sigma_threshold"]
            or rate_elevated
            or (divergence is not None and divergence > 15)
        ),
    }


# ============================================================
# ALERT EMAIL
# ============================================================
def send_alert_email(summaries):
    email_to = os.environ.get("MAUDE_EMAIL_TO")
    email_from = os.environ.get("MAUDE_EMAIL_FROM")
    smtp_password = os.environ.get("MAUDE_SMTP_PASSWORD")

    if not all([email_to, email_from, smtp_password]):
        print("  Email not configured — printing alerts to console:")
        for s in summaries:
            if s and s["alert"]:
                d = s["device"]
                l = s["latest"]
                print(f"    {d['name']} ({d['ticker']}): "
                      f"{l.get('reports', '?')} reports, "
                      f"Z={l.get('z_score', '?')}, "
                      f"Divergence={s.get('divergence', 'N/A')}")
        return

    alerts = [s for s in summaries if s and s["alert"]]
    if not alerts:
        return

    subject = f"MAUDE Alert: {', '.join(s['device']['name'] for s in alerts)}"
    body = (
        "<div style='font-family:-apple-system,Arial,sans-serif;max-width:640px;'>"
        "<h2 style='color:#dc2626;'>MAUDE Failure Rate Alert</h2>"
        f"<p style='color:#6b7280;font-size:13px;'>"
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ET</p>"
    )
    for s in alerts:
        d = s["device"]
        l = s["latest"]
        z = l.get("z_score", 0)
        color = "#dc2626" if abs(z) >= 3 else "#d97706" if abs(z) >= 2 else "#2563eb"
        body += (
            "<div style='background:#f9fafb;border:1px solid #e5e7eb;"
            "border-radius:8px;padding:16px;margin:12px 0;'>"
            f"<h3 style='margin:0 0 8px;'>{d['name']} ({d['ticker']})</h3>"
            "<table style='width:100%;font-size:13px;'>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Latest</td>"
            f"<td style='font-weight:600;padding:4px 8px;'>"
            f"{l.get('month','?')}: {l.get('reports',0):,}</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Z-score</td>"
            f"<td style='font-weight:600;color:{color};padding:4px 8px;'>"
            f"{'+'if z>0 else''}{z}s</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Rate Z</td>"
            f"<td style='padding:4px 8px;'>"
            f"{'+'if s['rate_z']>0 else''}{s['rate_z']}s</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Divergence</td>"
            f"<td style='padding:4px 8px;'>"
            f"{'+' if s.get('divergence',0) and s['divergence']>0 else''}"
            f"{s.get('divergence','N/A')}pp</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Severity</td>"
            f"<td style='padding:4px 8px;'>{s['severity_pct']}% injury+death</td></tr>"
            "</table></div>"
        )
    body += "</div>"

    recipients = [e.strip() for e in email_to.split(",") if e.strip()]
    for recipient in recipients:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = recipient
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP(CONFIG["smtp_host"], CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(email_from, smtp_password)
            server.send_message(msg)
    print(f"  Alert email sent to {email_to}")


# ============================================================
# HTML DASHBOARD
# ============================================================
def generate_dashboard(summaries):
    output_dir = Path(CONFIG["output_dir"])
    output_dir.mkdir(exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    # Group by ticker
    groups = {}
    for s in summaries:
        if s is None:
            continue
        gid = s["device"]["group_id"]
        if gid not in groups:
            groups[gid] = []
        groups[gid].append(s)

    cards_html = ""
    chart_data_js = "const CHART_DATA = {\n"

    for s in summaries:
        if s is None:
            continue
        d = s["device"]
        l = s["latest"]
        z = l.get("z_score", 0)
        rz = s["rate_z"]

        signal = (
            "CRITICAL" if abs(z) >= 3 or abs(rz) >= 3
            else "ELEVATED" if abs(z) >= 2 or abs(rz) >= 2
            else "WATCH" if abs(z) >= 1.5 or abs(rz) >= 1.5
            else "NORMAL"
        )
        sig_colors = {
            "CRITICAL": "#dc2626", "ELEVATED": "#d97706",
            "WATCH": "#f59e0b", "NORMAL": "#10b981"
        }
        sc = sig_colors[signal]

        csv_path = Path(CONFIG["data_dir"]) / f"{d['id']}_monthly.csv"
        months, counts, mas, rates, revs = [], [], [], [], []
        if csv_path.exists():
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    months.append(row["month"])
                    counts.append(int(row["reports"]))
                    mas.append(int(row["ma_6mo"]))
                    rates.append(float(row["rate_per_m"]) if row["rate_per_m"] else 0)
                    revs.append(float(row["mo_revenue"]) if row["mo_revenue"] else 0)

        chart_data_js += (
            f'  "{d["id"]}": {{'
            f'"months":{json.dumps(months)},'
            f'"counts":{json.dumps(counts)},'
            f'"ma":{json.dumps(mas)},'
            f'"rates":{json.dumps(rates)},'
            f'"revs":{json.dumps(revs)},'
            f'"avg1yr":{s["stats_1yr"]["mean"]},'
            f'"upper2s":{s["stats_1yr"]["mean"] + 2 * s["stats_1yr"]["sd"]},'
            f'"rateAvg":{s["rate_avg"]}'
            f"}},\n"
        )

        ev = s.get("event_mix", {})
        ev_total = sum(ev.values()) if ev else 0
        inj_pct = (f"{ev.get('Injury',0)/ev_total*100:.1f}%" if ev_total > 0 else "N/A")
        dth_ct = ev.get("Death", 0)
        div_val = s.get("divergence")
        div_str = f"{'+'if div_val and div_val>0 else''}{div_val}pp" if div_val is not None else "N/A"
        div_color = "#dc2626" if div_val and div_val > 15 else "#d97706" if div_val and div_val > 5 else "#10b981" if div_val is not None else "#6b7280"

        cards_html += f"""
        <div class="card">
          <div class="card-header">
            <div>
              <h3>{d['name']}</h3>
              <span class="ticker">{d['ticker']}</span>
            </div>
            <span class="signal" style="background:{sc}15;color:{sc};
              border:1px solid {sc}40;">{signal}</span>
          </div>
          <div class="metrics">
            <div class="metric">
              <span class="label">Latest month</span>
              <span class="value">{l.get('reports',0):,}</span>
              <span class="sub">{l.get('month','')}</span>
            </div>
            <div class="metric">
              <span class="label">Report Z-score</span>
              <span class="value" style="color:{sc}">
                {"+"if z>0 else""}{z}s</span>
              <span class="sub">1yr avg: {s['stats_1yr']['mean']:,}
                (sd={s['stats_1yr']['sd']})</span>
            </div>
            <div class="metric">
              <span class="label">Rate/$M Z-score</span>
              <span class="value" style="color:{'#dc2626'if abs(rz)>=2 else '#d97706'if abs(rz)>=1 else '#10b981'}">
                {"+"if rz>0 else""}{rz}s</span>
              <span class="sub">rate: {l.get('rate_per_m','-')} |
                avg: {s['rate_avg']}</span>
            </div>
            <div class="metric">
              <span class="label">Report vs Rev growth</span>
              <span class="value" style="color:{div_color}">{div_str}</span>
              <span class="sub">{'Reports outpacing revenue' if div_val and div_val > 5
                else 'In line' if div_val is not None else 'Insufficient data'}</span>
            </div>
            <div class="metric">
              <span class="label">6mo trend</span>
              <span class="value">{"+" if s['report_slope']>0 else""}{s['report_slope']}/mo</span>
              <span class="sub">rate slope: {"+"if s['rate_slope']>0 else""}{s['rate_slope']}</span>
            </div>
            <div class="metric">
              <span class="label">Severity mix</span>
              <span class="value">{s['severity_pct']}%</span>
              <span class="sub">injury+death (3mo) |
                {dth_ct} deaths</span>
            </div>
          </div>
          <div class="chart-container"><canvas id="chart-{d['id']}"></canvas></div>
          <div class="chart-toggle">
            <button class="active"
              onclick="drawChart('{d['id']}','counts',this)">Reports</button>
            <button onclick="drawChart('{d['id']}','rates',this)">Rate/$M</button>
            <button onclick="drawChart('{d['id']}','growth',this)">Growth gap</button>
          </div>
        </div>"""

    chart_data_js += "};\n"

    # Comparison table HTML
    comp_rows = ""
    for s in sorted(
        [x for x in summaries if x],
        key=lambda x: abs(x["latest"].get("z_score", 0)),
        reverse=True,
    ):
        d = s["device"]
        l = s["latest"]
        z = l.get("z_score", 0)
        rz = s["rate_z"]
        div = s.get("divergence")
        sig = (
            "CRITICAL" if abs(z) >= 3 or abs(rz) >= 3
            else "ELEVATED" if abs(z) >= 2 or abs(rz) >= 2
            else "WATCH" if abs(z) >= 1.5
            else "OK"
        )
        sc = {"CRITICAL":"#dc2626","ELEVATED":"#d97706","WATCH":"#f59e0b","OK":"#10b981"}[sig]
        comp_rows += (
            f"<tr>"
            f"<td style='font-weight:600'>{d['name']}</td>"
            f"<td>{d['ticker']}</td>"
            f"<td>{l.get('month','')}</td>"
            f"<td style='text-align:right'>{l.get('reports',0):,}</td>"
            f"<td style='text-align:right;color:{sc};font-weight:600'>"
            f"{'+'if z>0 else''}{z}</td>"
            f"<td style='text-align:right'>{l.get('rate_per_m','-')}</td>"
            f"<td style='text-align:right;color:{'#dc2626'if abs(rz)>=2 else '#10b981'}'>"
            f"{'+'if rz>0 else''}{rz}</td>"
            f"<td style='text-align:right;color:{'#dc2626'if div and div>15 else '#10b981'}'>"
            f"{'+'if div and div>0 else''}{div if div is not None else '-'}{'pp'if div is not None else ''}</td>"
            f"<td style='text-align:right'>{s['severity_pct']}%</td>"
            f"<td><span style='color:{sc};font-weight:600'>{sig}</span></td>"
            f"</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Failure Rate Monitor</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    background:#f8f9fa;color:#1a1a1a;padding:20px}}
  .wrap{{max-width:1200px;margin:0 auto}}
  h1{{font-size:22px;font-weight:600}}
  .sub{{font-size:13px;color:#6b7280;margin:4px 0 20px}}
  .comp-table{{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:24px;
    background:#fff;border-radius:8px;overflow:hidden;border:1px solid #e5e7eb}}
  .comp-table th{{background:#f9fafb;padding:8px 10px;text-align:left;
    font-weight:600;color:#6b7280;border-bottom:1px solid #e5e7eb;font-size:10px;
    text-transform:uppercase;letter-spacing:.3px}}
  .comp-table td{{padding:8px 10px;border-bottom:1px solid #f0f0f0}}
  .comp-table tr:hover{{background:#f9fafb}}
  .section-title{{font-size:16px;font-weight:600;margin:24px 0 12px;
    padding-bottom:8px;border-bottom:1px solid #e5e7eb}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));
    gap:14px}}
  .card{{background:#fff;border-radius:10px;border:1px solid #e5e7eb;padding:16px}}
  .card-header{{display:flex;justify-content:space-between;align-items:center;
    margin-bottom:12px}}
  .card-header h3{{font-size:15px;font-weight:600}}
  .ticker{{font-size:11px;color:#6b7280;margin-left:6px}}
  .signal{{font-size:10px;font-weight:600;padding:3px 10px;border-radius:20px}}
  .metrics{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;
    margin-bottom:12px}}
  .metric{{background:#f9fafb;border-radius:6px;padding:6px 8px}}
  .metric .label{{font-size:9px;text-transform:uppercase;color:#9ca3af;
    letter-spacing:.3px}}
  .metric .value{{font-size:16px;font-weight:600;display:block;margin-top:1px}}
  .metric .sub{{font-size:10px;color:#9ca3af;line-height:1.3}}
  .chart-container{{height:180px;margin-bottom:6px}}
  .chart-toggle{{display:flex;gap:3px}}
  .chart-toggle button{{font-size:10px;padding:3px 10px;border:1px solid #e5e7eb;
    border-radius:5px;background:#fff;cursor:pointer;font-family:inherit}}
  .chart-toggle button.active{{background:#1a1a1a;color:#fff;border-color:#1a1a1a}}
  .footer{{font-size:11px;color:#9ca3af;margin-top:24px;line-height:1.6}}
  .legend{{display:flex;gap:16px;flex-wrap:wrap;margin:8px 0 20px;font-size:11px;
    color:#6b7280}}
  .legend span{{display:flex;align-items:center;gap:4px}}
  .dot{{width:8px;height:8px;border-radius:50%;display:inline-block}}
</style>
</head><body>
<div class="wrap">
  <h1>MAUDE failure rate monitor</h1>
  <p class="sub">openFDA adverse events | Updated {now} |
    Threshold: {CONFIG['sigma_threshold']}s |
    {len([s for s in summaries if s])} products tracked</p>

  <div class="legend">
    <span><span class="dot" style="background:#10b981"></span> Normal (&lt;1.5s)</span>
    <span><span class="dot" style="background:#f59e0b"></span> Watch (1.5-2s)</span>
    <span><span class="dot" style="background:#d97706"></span> Elevated (2-3s)</span>
    <span><span class="dot" style="background:#dc2626"></span> Critical (&gt;3s)</span>
  </div>

  <table class="comp-table">
    <thead><tr>
      <th>Product</th><th>Ticker</th><th>Month</th>
      <th style="text-align:right">Reports</th>
      <th style="text-align:right">Z-score</th>
      <th style="text-align:right">Rate/$M</th>
      <th style="text-align:right">Rate Z</th>
      <th style="text-align:right">Divergence</th>
      <th style="text-align:right">Severity</th>
      <th>Signal</th>
    </tr></thead>
    <tbody>{comp_rows}</tbody>
  </table>

  <div class="section-title">Product detail</div>
  <div class="grid">{cards_html}</div>

  <div class="footer">
    <p><strong>Methodology:</strong> Z-scores compare latest month to trailing
    12-month average. Rate/$M = monthly reports / (quarterly revenue / 3).
    Rate Z-score compares current rate to its own history. Divergence =
    report growth rate minus revenue growth rate over trailing 12 months
    (positive = reports outpacing revenue = quality signal). Severity =
    % of injury + death events in last 3 months.</p>
    <p><strong>Leading indicators (ranked):</strong> (1) Rate/$M Z-score
    &gt;2s for 3+ months, (2) Divergence &gt;15pp, (3) Raw report Z-score
    &gt;2s, (4) Severity % rising, (5) New recall filings.</p>
    <p>Data: openFDA MAUDE device adverse events API. Reports are lagging
    indicators (30-90 day delay). Auto-updated via GitHub Actions.</p>
  </div>
</div>

<script>
{chart_data_js}
const charts={{}};
function drawChart(id,mode,btn){{
  if(btn){{btn.parentElement.querySelectorAll('button')
    .forEach(b=>b.classList.remove('active'));btn.classList.add('active')}}
  const d=CHART_DATA[id];if(!d)return;
  const ctx=document.getElementById('chart-'+id);
  if(charts[id])charts[id].destroy();

  let datasets,yLabel;
  if(mode==='rates'){{
    datasets=[
      {{type:'bar',label:'Rate/$M',data:d.rates,
        backgroundColor:'rgba(249,115,22,0.3)',borderColor:'rgb(249,115,22)',
        borderWidth:1,borderRadius:2}},
    ];
    yLabel='Reports per $M revenue';
  }}else if(mode==='growth'){{
    // Compute cumulative growth index for reports and revenue
    const repBase=d.counts[0]||1;
    const revBase=d.revs[0]||1;
    const repIdx=d.counts.map(v=>Math.round((v/repBase)*100));
    const revIdx=d.revs.map(v=>v>0?Math.round((v/revBase)*100):null);
    datasets=[
      {{type:'line',label:'Report growth index',data:repIdx,
        borderColor:'rgb(220,38,38)',borderWidth:2,pointRadius:0,tension:0.3}},
      {{type:'line',label:'Revenue growth index',data:revIdx,
        borderColor:'rgb(16,185,129)',borderWidth:2,pointRadius:0,tension:0.3}},
    ];
    yLabel='Growth index (base=100)';
  }}else{{
    datasets=[
      {{type:'bar',label:'Reports',data:d.counts,
        backgroundColor:'rgba(59,130,246,0.25)',borderColor:'rgb(59,130,246)',
        borderWidth:1,borderRadius:2}},
      {{type:'line',label:'6mo MA',data:d.ma,
        borderColor:'rgb(37,99,235)',borderWidth:2,pointRadius:0,tension:0.3}},
    ];
    yLabel='Monthly reports';
  }}

  charts[id]=new Chart(ctx,{{type:'bar',
    data:{{labels:d.months,datasets}},
    options:{{responsive:true,maintainAspectRatio:false,
      interaction:{{intersect:false,mode:'index'}},
      scales:{{
        x:{{ticks:{{maxTicksLimit:10,font:{{size:9}}}}}},
        y:{{title:{{display:true,text:yLabel,font:{{size:10}}}},
          ticks:{{font:{{size:9}}}}}}
      }},
      plugins:{{legend:{{labels:{{font:{{size:9}}}}}}}}
    }}}});
}}
Object.keys(CHART_DATA).forEach(id=>drawChart(id,'counts',null));
</script>
</body></html>"""

    html_path = output_dir / "index.html"
    with open(html_path, "w") as f:
        f.write(html)
    print(f"  Dashboard written to {html_path}")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="MAUDE Monitor v2")
    parser.add_argument("--device", help="Refresh single device by ID")
    parser.add_argument("--html", action="store_true", help="Generate dashboard")
    parser.add_argument("--no-email", action="store_true", help="Skip email")
    args = parser.parse_args()

    devices = DEVICES
    if args.device:
        devices = [d for d in DEVICES if d["id"] == args.device.upper()]
        if not devices:
            print(f"Unknown device: {args.device}. "
                  f"Available: {', '.join(d['id'] for d in DEVICES)}")
            sys.exit(1)

    print(f"MAUDE Monitor v2 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Devices: {', '.join(d['id'] for d in devices)}")
    print()

    summaries = []
    for device in devices:
        print(f"[{device['id']}] {device['name']} ({device['ticker']})")
        try:
            summary = refresh_device(device)
            summaries.append(summary)
            if summary:
                l = summary["latest"]
                z = l.get("z_score", 0)
                rz = summary["rate_z"]
                div = summary.get("divergence", "N/A")
                tag = "[!]" if summary["alert"] else "[ok]"
                print(f"  {tag} {l.get('month','?')}: "
                      f"{l.get('reports',0):,} reports | "
                      f"Z={z} | RateZ={rz} | "
                      f"Div={div} | Sev={summary['severity_pct']}%")
        except Exception as e:
            print(f"  [x] Error: {e}")
            summaries.append(None)
        print()

    valid = [s for s in summaries if s is not None]
    alerts = [s for s in valid if s["alert"]]

    if alerts and not args.no_email:
        print(f"Alerts: {', '.join(s['device']['id'] for s in alerts)}")
        send_alert_email(valid)
    else:
        print("No alerts triggered.")

    if args.html or True:
        generate_dashboard(valid)

    data_dir = Path(CONFIG["data_dir"])
    data_dir.mkdir(exist_ok=True)
    summary_path = data_dir / "latest_summary.json"
    summary_data = []
    for s in valid:
        summary_data.append({
            "id": s["device"]["id"],
            "ticker": s["device"]["ticker"],
            "name": s["device"]["name"],
            "latest_month": s["latest"].get("month"),
            "reports": s["latest"].get("reports"),
            "z_score": s["latest"].get("z_score"),
            "rate": s["latest"].get("rate_per_m"),
            "rate_z": s["rate_z"],
            "divergence": s.get("divergence"),
            "severity_pct": s["severity_pct"],
            "alert": s["alert"],
            "updated": datetime.now().isoformat(),
        })
    with open(summary_path, "w") as f:
        json.dump(summary_data, f, indent=2)

    print(f"\nDone. Summary: {summary_path}")


if __name__ == "__main__":
    main()
