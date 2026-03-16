"""
MAUDE Multi-Device Failure Rate Monitor
========================================
Fetches adverse event data from openFDA, computes z-scores and failure rates,
generates alerts, and outputs CSV + HTML dashboard.

Usage:
    python maude_monitor.py                    # Full refresh, all devices
    python maude_monitor.py --device PODD      # Single device
    python maude_monitor.py --html             # Generate HTML dashboard
"""

import json
import csv
import os
import sys
import time
import argparse
import smtplib
import statistics
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

DEVICES = [
    {
        "id": "PODD",
        "name": "Insulet Omnipod",
        "ticker": "PODD",
        "search": "device.brand_name:omnipod",
        "recall_firm": "insulet",
        "quarterly_revenue": {
            "2023-Q1": 400, "2023-Q2": 418, "2023-Q3": 432, "2023-Q4": 411,
            "2024-Q1": 478, "2024-Q2": 505, "2024-Q3": 534, "2024-Q4": 516,
            "2025-Q1": 580, "2025-Q2": 640, "2025-Q3": 708, "2025-Q4": 782,
        },
    },
    {
        "id": "DXCM",
        "name": "Dexcom CGM",
        "ticker": "DXCM",
        "search": "device.brand_name:dexcom",
        "recall_firm": "dexcom",
        "quarterly_revenue": {
            "2023-Q1": 721, "2023-Q2": 871, "2023-Q3": 975, "2023-Q4": 1010,
            "2024-Q1": 890, "2024-Q2": 1004, "2024-Q3": 994, "2024-Q4": 1113,
            "2025-Q1": 1050, "2025-Q2": 1100, "2025-Q3": 1150, "2025-Q4": 1200,
        },
    },
    {
        "id": "TNDM",
        "name": "Tandem Diabetes",
        "ticker": "TNDM",
        "search": "device.brand_name:tandem",
        "recall_firm": "tandem",
        "quarterly_revenue": {
            "2023-Q1": 154, "2023-Q2": 178, "2023-Q3": 187, "2023-Q4": 220,
            "2024-Q1": 171, "2024-Q2": 195, "2024-Q3": 200, "2024-Q4": 235,
            "2025-Q1": 185, "2025-Q2": 210, "2025-Q3": 215, "2025-Q4": 240,
        },
    },
]


# ============================================================
# API HELPERS
# ============================================================
def api_fetch(url, retries=3):
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "MAUDE-Monitor/1.0"})
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"  Retry {attempt + 1}/{retries}: {e}")
            time.sleep(2 * (attempt + 1))
    return None


def fetch_ae_counts(device, start_date, end_date):
    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    url = (
        f"https://api.fda.gov/device/event.json"
        f"?search={device['search']}+AND+date_received:[{start}+TO+{end}]"
        f"&count=date_received"
    )
    print(f"  Fetching AE counts: {device['id']} ({start}-{end})")
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


def fetch_recalls(device):
    url = (
        f"https://api.fda.gov/device/recall.json"
        f'?search=recalling_firm:"{device["recall_firm"]}"'
        f"&limit=10&sort=event_date_posted:desc"
    )
    try:
        data = api_fetch(url)
        return data.get("results", []) if data else []
    except Exception:
        return []


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

    rows = []
    for i, (month, count) in enumerate(zip(months, counts)):
        q = get_quarter(month)
        q_rev = device["quarterly_revenue"].get(q)
        rate = round(count / (q_rev / 3), 1) if q_rev else None
        z = z_score(count, stats_1yr["mean"], stats_1yr["sd"])
        rows.append({
            "month": month,
            "reports": count,
            "ma_6mo": ma[i],
            "avg_1yr": stats_1yr["mean"],
            "sd_1yr": stats_1yr["sd"],
            "z_score": z,
            "rate_per_m": rate,
            "qtr_revenue": q_rev,
        })

    fieldnames = [
        "month", "reports", "ma_6mo", "avg_1yr",
        "sd_1yr", "z_score", "rate_per_m", "qtr_revenue",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} months to {csv_path}")

    latest = rows[-1] if rows else {}
    rates = [r["rate_per_m"] for r in rows if r["rate_per_m"] is not None]
    rate_avg = round(sum(rates) / len(rates), 1) if rates else 0
    recent_3_rates = rates[-3:] if len(rates) >= 3 else rates
    rate_elevated = len(recent_3_rates) == 3 and all(
        r > rate_avg * 1.2 for r in recent_3_rates
    )

    return {
        "device": device,
        "latest": latest,
        "stats_1yr": stats_1yr,
        "stats_all": stats_all,
        "event_mix": event_mix,
        "rate_avg": rate_avg,
        "rate_elevated": rate_elevated,
        "months_count": len(rows),
        "total_reports": sum(counts),
        "alert": abs(latest.get("z_score", 0)) >= CONFIG["sigma_threshold"]
        or rate_elevated,
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
                print(
                    f"    {d['name']} ({d['ticker']}): "
                    f"{l.get('reports', '?')} reports, "
                    f"Z={l.get('z_score', '?')}"
                )
        return

    alerts = [s for s in summaries if s and s["alert"]]
    if not alerts:
        return

    subject = (
        f"MAUDE Alert: {', '.join(s['device']['name'] for s in alerts)}"
    )

    body = (
        "<div style='font-family:-apple-system,Arial,sans-serif;"
        "max-width:640px;'>"
        "<h2 style='color:#dc2626;'>MAUDE Failure Rate Alert</h2>"
        "<p style='color:#6b7280;font-size:13px;'>"
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} ET</p>"
    )

    for s in alerts:
        d = s["device"]
        l = s["latest"]
        z = l.get("z_score", 0)
        color = (
            "#dc2626" if abs(z) >= 3
            else "#d97706" if abs(z) >= 2
            else "#2563eb"
        )
        body += (
            "<div style='background:#f9fafb;border:1px solid #e5e7eb;"
            "border-radius:8px;padding:16px;margin:12px 0;'>"
            f"<h3 style='margin:0 0 8px;'>{d['name']} "
            f"<span style='color:#6b7280;'>({d['ticker']})</span></h3>"
            "<table style='width:100%;font-size:13px;'>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Latest month</td>"
            f"<td style='font-weight:600;padding:4px 8px;'>"
            f"{l.get('month','?')}: {l.get('reports',0):,} reports</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>1yr average</td>"
            f"<td style='padding:4px 8px;'>"
            f"{s['stats_1yr']['mean']:,}/mo "
            f"(sd={s['stats_1yr']['sd']})</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Z-score</td>"
            f"<td style='font-weight:600;color:{color};padding:4px 8px;'>"
            f"{'+'if z>0 else''}{z}s</td></tr>"
            f"<tr><td style='color:#6b7280;padding:4px 8px;'>Rate</td>"
            f"<td style='padding:4px 8px;'>"
            f"{l.get('rate_per_m','N/A')} per $M "
            f"(avg: {s['rate_avg']})</td></tr>"
            "</table>"
        )
        if s["rate_elevated"]:
            body += (
                "<p style='color:#dc2626;font-size:12px;margin:8px 0 0;'>"
                "Rate elevated 3+ months above average</p>"
            )
        body += "</div>"

    body += (
        "<hr style='border:none;border-top:1px solid #e5e7eb;margin:16px 0;'>"
        f"<p style='font-size:11px;color:#9ca3af;'>"
        f"Threshold: {CONFIG['sigma_threshold']}s | Data: openFDA MAUDE</p>"
        "</div>"
    )

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

    cards_html = ""
    chart_data_js = "const CHART_DATA = {\n"

    for s in summaries:
        if s is None:
            continue
        d = s["device"]
        l = s["latest"]
        z = l.get("z_score", 0)
        signal = (
            "CRITICAL" if abs(z) >= 3
            else "ELEVATED" if abs(z) >= 2
            else "WATCH" if abs(z) >= 1
            else "NORMAL"
        )
        sig_color = (
            "#dc2626" if abs(z) >= 3
            else "#d97706" if abs(z) >= 2
            else "#f59e0b" if abs(z) >= 1
            else "#10b981"
        )

        csv_path = Path(CONFIG["data_dir"]) / f"{d['id']}_monthly.csv"
        months, counts, mas, rates = [], [], [], []
        if csv_path.exists():
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    months.append(row["month"])
                    counts.append(int(row["reports"]))
                    mas.append(int(row["ma_6mo"]))
                    rates.append(
                        float(row["rate_per_m"]) if row["rate_per_m"] else 0
                    )

        chart_data_js += (
            f'  "{d["id"]}": {{'
            f'"months":{json.dumps(months)},'
            f'"counts":{json.dumps(counts)},'
            f'"ma":{json.dumps(mas)},'
            f'"rates":{json.dumps(rates)},'
            f'"avg1yr":{s["stats_1yr"]["mean"]}'
            f"}},\n"
        )

        ev = s.get("event_mix", {})
        ev_total = sum(ev.values()) if ev else 0
        inj_pct = (
            f"{ev.get('Injury', 0) / ev_total * 100:.1f}%"
            if ev_total > 0
            else "N/A"
        )

        cards_html += f"""
        <div class="card">
          <div class="card-header">
            <div>
              <h3>{d['name']}</h3>
              <span class="ticker">{d['ticker']}</span>
            </div>
            <span class="signal" style="background:{sig_color}20;
              color:{sig_color};border:1px solid {sig_color}40;">
              {signal}</span>
          </div>
          <div class="metrics">
            <div class="metric">
              <span class="label">Latest</span>
              <span class="value">{l.get('reports', 0):,}</span>
              <span class="sub">{l.get('month', '')}</span>
            </div>
            <div class="metric">
              <span class="label">1yr avg</span>
              <span class="value">{s['stats_1yr']['mean']:,}</span>
              <span class="sub">sd={s['stats_1yr']['sd']}</span>
            </div>
            <div class="metric">
              <span class="label">Z-score</span>
              <span class="value" style="color:{sig_color}">
                {"+" if z > 0 else ""}{z}s</span>
              <span class="sub">vs 1yr</span>
            </div>
            <div class="metric">
              <span class="label">Rate/$M</span>
              <span class="value">{l.get('rate_per_m', '-')}</span>
              <span class="sub">avg {s['rate_avg']}</span>
            </div>
            <div class="metric">
              <span class="label">Injury %</span>
              <span class="value">{inj_pct}</span>
              <span class="sub">last 3mo</span>
            </div>
            <div class="metric">
              <span class="label">Total</span>
              <span class="value">{s['total_reports']:,}</span>
              <span class="sub">{s['months_count']}mo</span>
            </div>
          </div>
          <div class="chart-container">
            <canvas id="chart-{d['id']}"></canvas>
          </div>
          <div class="chart-toggle">
            <button class="active"
              onclick="drawChart('{d['id']}','counts',this)">
              Reports</button>
            <button
              onclick="drawChart('{d['id']}','rates',this)">
              Rate/$M</button>
          </div>
        </div>"""

    chart_data_js += "};\n"

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MAUDE Failure Rate Monitor</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    background:#f8f9fa;color:#1a1a1a;padding:24px}}
  .header{{max-width:1000px;margin:0 auto 24px}}
  .header h1{{font-size:22px;font-weight:600}}
  .header p{{font-size:13px;color:#6b7280;margin-top:4px}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));
    gap:16px;max-width:1000px;margin:0 auto}}
  .card{{background:#fff;border-radius:12px;border:1px solid #e5e7eb;padding:20px}}
  .card-header{{display:flex;justify-content:space-between;align-items:center;
    margin-bottom:16px}}
  .card-header h3{{font-size:16px;font-weight:600}}
  .ticker{{font-size:12px;color:#6b7280}}
  .signal{{font-size:11px;font-weight:600;padding:4px 12px;border-radius:20px}}
  .metrics{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;
    margin-bottom:16px}}
  .metric{{background:#f9fafb;border-radius:8px;padding:8px 10px}}
  .metric .label{{font-size:10px;text-transform:uppercase;color:#9ca3af;
    letter-spacing:.3px}}
  .metric .value{{font-size:18px;font-weight:600;display:block;margin-top:2px}}
  .metric .sub{{font-size:11px;color:#9ca3af}}
  .chart-container{{height:200px;margin-bottom:8px}}
  .chart-toggle{{display:flex;gap:4px}}
  .chart-toggle button{{font-size:11px;padding:4px 12px;border:1px solid #e5e7eb;
    border-radius:6px;background:#fff;cursor:pointer;font-family:inherit}}
  .chart-toggle button.active{{background:#1a1a1a;color:#fff;border-color:#1a1a1a}}
  .footer{{max-width:1000px;margin:24px auto 0;font-size:11px;color:#9ca3af}}
</style>
</head><body>
<div class="header">
  <h1>MAUDE failure rate monitor</h1>
  <p>openFDA adverse events | Updated {now} |
     Threshold: {CONFIG['sigma_threshold']}s</p>
</div>
<div class="grid">{cards_html}</div>
<div class="footer">
  <p>Data: openFDA MAUDE device adverse events API.
  Reports are lagging indicators (30-90 day delay).
  Failure rate = monthly reports / (quarterly revenue / 3).
  Z-scores vs trailing 12-month average.
  Auto-updated via GitHub Actions.</p>
</div>
<script>
{chart_data_js}
const charts={{}};
function drawChart(deviceId,mode,btn){{
  if(btn){{btn.parentElement.querySelectorAll('button')
    .forEach(b=>b.classList.remove('active'));btn.classList.add('active')}}
  const d=CHART_DATA[deviceId];if(!d)return;
  const ctx=document.getElementById('chart-'+deviceId);
  if(charts[deviceId])charts[deviceId].destroy();
  const values=mode==='rates'?d.rates:d.counts;
  const datasets=[{{type:'bar',
    label:mode==='rates'?'Rate/$M':'Reports',data:values,
    backgroundColor:mode==='rates'?'rgba(249,115,22,0.3)':'rgba(59,130,246,0.3)',
    borderColor:mode==='rates'?'rgb(249,115,22)':'rgb(59,130,246)',
    borderWidth:1,borderRadius:2}}];
  if(mode!=='rates'){{datasets.push({{type:'line',label:'6mo MA',data:d.ma,
    borderColor:'rgb(37,99,235)',borderWidth:2,pointRadius:0,tension:0.3}})}}
  charts[deviceId]=new Chart(ctx,{{type:'bar',
    data:{{labels:d.months,datasets}},
    options:{{responsive:true,maintainAspectRatio:false,
      interaction:{{intersect:false,mode:'index'}},
      scales:{{x:{{ticks:{{maxTicksLimit:12,font:{{size:10}}}}}},
        y:{{ticks:{{font:{{size:10}}}}}}}},
      plugins:{{legend:{{labels:{{font:{{size:10}}}}}}}}
    }}}})}}
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
    parser = argparse.ArgumentParser(description="MAUDE Failure Rate Monitor")
    parser.add_argument("--device", help="Refresh single device by ID")
    parser.add_argument("--alert-only", action="store_true",
                        help="Check alerts without refreshing")
    parser.add_argument("--html", action="store_true",
                        help="Generate HTML dashboard")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email alerts")
    args = parser.parse_args()

    devices = DEVICES
    if args.device:
        devices = [d for d in DEVICES if d["id"] == args.device.upper()]
        if not devices:
            print(f"Unknown device: {args.device}. "
                  f"Available: {', '.join(d['id'] for d in DEVICES)}")
            sys.exit(1)

    print(f"MAUDE Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Devices: {', '.join(d['id'] for d in devices)}")
    print()

    summaries = []
    for device in devices:
        print(f"[{device['id']}] {device['name']}")
        try:
            summary = refresh_device(device)
            summaries.append(summary)
            if summary:
                l = summary["latest"]
                tag = "[!] ALERT" if summary["alert"] else "[ok]"
                print(f"  {tag} - {l.get('month','?')}: "
                      f"{l.get('reports',0):,} reports, "
                      f"Z={l.get('z_score',0)}, "
                      f"Rate={l.get('rate_per_m','N/A')}")
        except Exception as e:
            print(f"  [x] Error: {e}")
            summaries.append(None)
        print()

    valid = [s for s in summaries if s is not None]
    alerts = [s for s in valid if s["alert"]]

    if alerts and not args.no_email:
        print(f"Sending alerts for: "
              f"{', '.join(s['device']['id'] for s in alerts)}")
        send_alert_email(valid)
    elif not alerts:
        print("No alerts triggered.")

    if args.html or not args.alert_only:
        generate_dashboard(valid)

    data_dir = Path(CONFIG["data_dir"])
    data_dir.mkdir(exist_ok=True)
    summary_path = data_dir / "latest_summary.json"
    summary_data = []
    for s in valid:
        summary_data.append({
            "id": s["device"]["id"],
            "ticker": s["device"]["ticker"],
            "latest_month": s["latest"].get("month"),
            "reports": s["latest"].get("reports"),
            "z_score": s["latest"].get("z_score"),
            "rate": s["latest"].get("rate_per_m"),
            "alert": s["alert"],
            "updated": datetime.now().isoformat(),
        })
    with open(summary_path, "w") as f:
        json.dump(summary_data, f, indent=2)

    print(f"\nDone. Summary: {summary_path}")


if __name__ == "__main__":
    main()
