"""MAUDE Monitor V4 — run-over-run diffing and signal-transition alerts.

Diffing: every run writes a per-device snapshot (total reports + monthly
counts) under data/snapshots/, keyed latest.json plus dated archives.
The next run diffs against it and surfaces new reports per device on the
dashboard and in alerts.

Alerts: fire only on signal STATE TRANSITIONS (e.g. WATCH -> ELEVATED),
not on every run, using data/signal_state.json. Email (SMTP_* env vars)
and Slack (SLACK_WEBHOOK_URL) are both optional and silently skipped
when not configured.
"""

import json
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from urllib.request import urlopen, Request

from email.mime.multipart import MIMEMultipart

from mm_config import SNAPSHOT_DIR, STATE_FILE, SLACK_WEBHOOK_URL, \
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_TO, ALERT_FROM, \
    DASHBOARD_URL, DIGEST_MODE, RESEND_API_KEY, RESEND_FROM

SIGNAL_RANK = {"NORMAL": 0, "WATCH": 1, "ELEVATED": 2, "CRITICAL": 3}


# ----------------------------------------------------------------------
# Snapshots / diffing
# ----------------------------------------------------------------------
def _latest_path():
    return os.path.join(SNAPSHOT_DIR, "latest.json")


def load_previous_snapshot():
    try:
        with open(_latest_path()) as f:
            return json.load(f)
    except Exception:
        return None


def load_snapshot_days_ago(days=7, tolerance=3):
    """Closest dated snapshot to `days` ago (within tolerance), or None."""
    try:
        files = [f for f in os.listdir(SNAPSHOT_DIR) if f.endswith(".json") and f != "latest.json"]
    except Exception:
        return None
    target = datetime.now().date() - timedelta(days=days)
    best, best_gap = None, None
    for f in files:
        try:
            d = datetime.strptime(f[:-5], "%Y-%m-%d").date()
        except ValueError:
            continue
        gap = abs((d - target).days)
        if gap <= tolerance and (best_gap is None or gap < best_gap):
            best, best_gap = f, gap
    if not best:
        return None
    try:
        with open(os.path.join(SNAPSHOT_DIR, best)) as fh:
            snap = json.load(fh)
        snap["_file_date"] = best[:-5]
        return snap
    except Exception:
        return None


def write_snapshot(all_res, fda_last_updated=None):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {"run_at": datetime.now().isoformat(timespec="seconds"),
            "fda_last_updated": fda_last_updated, "devices": {}}
    for did, res in all_res.items():
        recv = res.get("recv", {})
        recalls = (res.get("modules", {}).get("recalls") or {}).get("recalls", [])
        snap["devices"][did] = {"total": sum(recv.values()), "by_month": recv,
                                "r_score": res.get("r_score"), "signal": res.get("signal"),
                                "recall_keys": [f"{r.get('initiated','')}|{r.get('reason','')[:40]}"
                                                for r in recalls]}
    with open(_latest_path(), "w") as f:
        json.dump(snap, f)
    dated = os.path.join(SNAPSHOT_DIR, datetime.now().strftime("%Y-%m-%d") + ".json")
    with open(dated, "w") as f:
        json.dump(snap, f)
    _prune_snapshots(keep=30)
    return snap


def _prune_snapshots(keep=30):
    try:
        dated = sorted(f for f in os.listdir(SNAPSHOT_DIR)
                       if f.endswith(".json") and f != "latest.json")
        for f in dated[:-keep]:
            os.remove(os.path.join(SNAPSHOT_DIR, f))
    except Exception:
        pass


def diff_snapshot(all_res, prev, week=None):
    """Returns {device_id: {new_reports, prev_run, changed_months, new_recalls,
    wow_new_reports, wow_date}}."""
    out = {}
    if not prev:
        return out
    pdev = prev.get("devices", {})
    wdev = (week or {}).get("devices", {})
    wdate = (week or {}).get("_file_date")
    for did, res in all_res.items():
        recv = res.get("recv", {})
        old = pdev.get(did)
        if not old:
            continue
        new_total = sum(recv.values()) - old.get("total", 0)
        changed = []
        old_m = old.get("by_month", {})
        for m in sorted(recv):
            delta = recv[m] - old_m.get(m, 0)
            if delta != 0:
                changed.append({"month": m, "delta": delta})
        prev_keys = set(old.get("recall_keys", []))
        recalls = (res.get("modules", {}).get("recalls") or {}).get("recalls", [])
        new_recalls = [r for r in recalls
                       if f"{r.get('initiated','')}|{r.get('reason','')[:40]}" not in prev_keys]
        wow = None
        if wdev.get(did):
            wow = sum(recv.values()) - wdev[did].get("total", 0)
        out[did] = {"new_reports": new_total, "prev_run": prev.get("run_at", ""),
                    "changed_months": changed[-6:], "new_recalls": new_recalls,
                    "wow_new_reports": wow, "wow_date": wdate,
                    "prev_r": (wdev.get(did) or old).get("r_score"),
                    "prev_signal": (wdev.get(did) or old).get("signal")}
    return out


# ----------------------------------------------------------------------
# Signal state + transitions
# ----------------------------------------------------------------------
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(signals):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"updated": datetime.now().isoformat(timespec="seconds"),
                   "signals": signals}, f, indent=1)


def detect_transitions(prev_state, current_signals):
    prev = (prev_state or {}).get("signals", {})
    transitions = []
    for did, sig in current_signals.items():
        old = prev.get(did)
        if old is None or old == sig:
            continue
        transitions.append({"device": did, "from": old, "to": sig,
                            "upgraded": SIGNAL_RANK.get(sig, 0) > SIGNAL_RANK.get(old, 0)})
    return transitions


# ----------------------------------------------------------------------
# Delivery
# ----------------------------------------------------------------------
def _format_alert(transitions, diffs, all_res):
    lines = ["MAUDE Monitor signal transitions:"]
    for t in transitions:
        res = all_res.get(t["device"], {})
        name = res.get("device", {}).get("name", t["device"])
        arrow = "UP" if t["upgraded"] else "down"
        d = diffs.get(t["device"], {})
        extra = f" | {d['new_reports']:+,} reports since last run" if d else ""
        st = res.get("stats") or {}
        z = st.get("z_score_adj")
        ztxt = f" | trend-adj z {z:+.2f}" if z is not None else ""
        lines.append(f"  [{arrow}] {name}: {t['from']} -> {t['to']}{ztxt}{extra}")
    return "\n".join(lines)


def send_alerts(transitions, diffs, all_res):
    if not transitions:
        return {"sent": False, "reason": "no transitions"}
    body = _format_alert(transitions, diffs, all_res)
    upgrades = [t for t in transitions if t["upgraded"]]
    subject = (f"MAUDE Monitor: {len(upgrades)} signal upgrade(s)"
               if upgrades else "MAUDE Monitor: signal changes")
    sent = []
    if SLACK_WEBHOOK_URL:
        try:
            payload = json.dumps({"text": f"*{subject}*\n```{body}```"}).encode()
            req = Request(SLACK_WEBHOOK_URL, data=payload,
                          headers={"Content-Type": "application/json"})
            urlopen(req, timeout=15).read()
            sent.append("slack")
        except Exception as e:
            print(f"  Slack alert failed: {str(e)[:80]}")
    print(body)
    return {"sent": bool(sent), "channels": sent, "body": body}


# ----------------------------------------------------------------------
# Daily digest email
# ----------------------------------------------------------------------
SIG_COLOR = {"CRITICAL": "#b3272e", "ELEVATED": "#bc5f04", "WATCH": "#8f7a10", "NORMAL": "#2c6e49"}
SIG_PLAIN = {"CRITICAL": "critical (R-score 70+)", "ELEVATED": "elevated (50-69)",
             "WATCH": "on watch (30-49)", "NORMAL": "normal (below 30)"}


def _esc(t):
    return (str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _pill(sig):
    return (f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
            f'font-size:11px;font-weight:700;color:#ffffff;background:{SIG_COLOR.get(sig, "#666")}">'
            f'{sig}</span>')


def _fmt_delta(v):
    if v is None:
        return "n/a"
    return f"{v:+,}"


def _pct(v):
    if v is None:
        return "n/a"
    return f"{v:+.0f}%"


def _mult(v):
    return "n/a" if v is None else f"{v:.1f}x"


def _mon(m):
    try:
        return datetime.strptime(m, "%Y-%m").strftime("%b %Y")
    except Exception:
        return m or ""


def _row(did, res, diffs, meta):
    st = res["stats"]
    d = diffs.get(did, {})
    mods = res.get("modules", {})
    car = mods.get("car") or {}
    cs = car.get("summary", {}) if car.get("status") == "ok" else {}
    fm = mods.get("failure_modes") or {}
    top_modes = [(t["mode"].replace("_", " "), t["pct"]) for t in fm.get("top_modes", [])
                 if t["mode"] != "other"][:3] if fm.get("status") == "ok" else []
    prr = mods.get("prr") or {}
    prr_flags = [x["mode"].replace("_", " ") for x in prr.get("prr_signals", []) if x.get("signal")] \
        if prr.get("status") == "ok" else []
    rc = mods.get("recalls") or {}
    casc = mods.get("cascade") or {}
    tk = res["device"]["ticker"]
    earn = (meta.get("next_earnings") or {}).get(tk)
    days_to_earn = None
    if earn:
        try:
            days_to_earn = (datetime.strptime(earn, "%Y-%m-%d").date() - datetime.now().date()).days
        except ValueError:
            pass
    r3 = st.get("reports_3mo") or 0
    return {"id": did, "name": res["device"]["name"], "ticker": tk, "company": res["device"]["company"],
            "signal": res["signal"], "r": res.get("r_score") or 0, "prev_r": res.get("prev_r"),
            "prev_signal": res.get("prev_signal"),
            "z": st["z_score_adj"], "latest": st["latest_value"], "month": st["latest_month"],
            "provisional": st["provisional"], "deaths3": st["deaths_3mo"], "inj3": st["injuries_3mo"],
            "new": d.get("new_reports"), "wow": d.get("wow_new_reports"),
            "new_recalls": d.get("new_recalls", []),
            "combined": res["device"]["is_combined"], "detail": res.get("r_detail"),
            "prev_month": st.get("prev_month"), "prev_value": st.get("prev_value"),
            "yago_month": st.get("yago_month"), "yago_value": st.get("yago_value"),
            "mom": st.get("mom_pct"), "yoy": st.get("yoy_pct"), "vs_med": st.get("vs_median12"),
            "median12": st.get("median12"), "surge": st.get("surge"), "small_base": st.get("small_base"),
            "batch": st["latest_month"] in (st.get("batch_months") or []),
            "dpk3": st.get("deaths_per_k_3mo"), "dpk12": st.get("deaths_per_k_base"),
            "ipk3": st.get("inj_per_k_3mo"), "ipk12": st.get("inj_per_k_base"),
            "sev_base_ok": r3 >= 300,
            "car_n": cs.get("total", 0), "car_hit": cs.get("hit_rate"), "car_mean": cs.get("mean_car"),
            "car_pnl": cs.get("total_pnl"), "car_grade": cs.get("grade"),
            "horizon": car.get("primary_horizon", "3mo"),
            "earnings": earn, "days_to_earn": days_to_earn,
            "top_modes": top_modes, "prr_flags": prr_flags,
            "recall_count": rc.get("count", 0), "class1": rc.get("class1_count", 0),
            "cascade": casc.get("phase"),
            "perf": (meta.get("stock_perf") or {}).get(tk, {})}


def _history_line(r, bench):
    if not r["car_n"] or r["car_n"] < 2 or r["car_hit"] is None:
        return ""
    return (f"History: after {r['car_n']} prior spikes of this kind, {r['ticker']} lagged {bench} over the next "
            f"{r['horizon']} {r['car_hit']:.0f}% of the time, averaging {r['car_mean']:+.1f}% vs {bench}.")


def _stock_line(r, bench):
    pf = r.get("perf") or {}
    if pf.get("1mo_rel") is None and pf.get("3mo_rel") is None:
        return ""
    bits = []
    if pf.get("1mo") is not None:
        bits.append(f"{pf['1mo']:+.1f}% over 1 month ({pf['1mo_rel']:+.1f}% vs {bench})")
    if pf.get("3mo") is not None:
        bits.append(f"{pf['3mo']:+.1f}% over 3 months ({pf['3mo_rel']:+.1f}% vs {bench})")
    return f"Stock already: {r['ticker']} " + ", ".join(bits) + "."


def _severity_line(r):
    if not r["sev_base_ok"] or r["dpk3"] is None or not r["dpk12"]:
        return f"{r['deaths3']} deaths reported in the last 3 months (base too small for a rate)." if r["deaths3"] else ""
    ratio = r["dpk3"] / r["dpk12"]
    direction = ("more severe" if ratio >= 1.25 else "less severe" if ratio <= 0.75 else "about as severe")
    return (f"Mix is {direction} than usual: {r['dpk3']:.2f} deaths per 1,000 reports over the last 3 months "
            f"vs {r['dpk12']:.2f} over the prior 12 ({ratio:.1f}x), {r['deaths3']} deaths in the window.")


def _plain(r, bench):
    parts = [f"{r['name']} ({r['ticker']}): {r['latest']:,} reports in {_mon(r['month'])}"]
    ctx = []
    if r["prev_value"] is not None:
        ctx.append(f"{_pct(r['mom'])} vs {_mon(r['prev_month'])} ({r['prev_value']:,})")
    if r["yago_value"] is not None:
        ctx.append(f"{_pct(r['yoy'])} vs {_mon(r['yago_month'])} ({r['yago_value']:,})")
    if r["vs_med"] is not None:
        ctx.append(f"{_mult(r['vs_med'])} the trailing 12-month median")
    if ctx:
        parts[0] += ", " + "; ".join(ctx)
    parts[0] += "."
    if r["small_base"]:
        parts.append("Small base: the percentages exaggerate what are small absolute counts.")
    if r["batch"]:
        parts.append("This month is flagged as a batch filing (reports received far exceed events that occurred), "
                     "so it reads as paperwork catching up rather than a new wave of incidents; it is excluded from the statistics.")
    elif r["surge"]:
        parts.append("The batch test cannot yet tell a retrospective filing from live events for the newest month, "
                     "so this is either a bulk filing, which usually follows a recall or corrective action, or a genuine "
                     "wave of incidents; the narratives and the recall record settle it.")
    sl = _severity_line(r)
    if sl:
        parts.append(sl)
    if r["top_modes"]:
        parts.append("Top complaint types: " + ", ".join(f"{m} {p:.0f}%" for m, p in r["top_modes"])
                     + (f"; disproportionate vs peers: {', '.join(r['prr_flags'])}" if r["prr_flags"] else "") + ".")
    reg = []
    if r["class1"]:
        reg.append(f"{r['class1']} Class I recall{'s' if r['class1'] > 1 else ''} on file")
    elif r["recall_count"]:
        reg.append(f"{r['recall_count']} recall{'s' if r['recall_count'] > 1 else ''} on file, none Class I")
    if r["cascade"] and r["cascade"] != "NO CASCADE":
        reg.append(f"cascade position {r['cascade'].lower()}")
    if reg:
        parts.append("Regulatory: " + "; ".join(reg) + ".")
    h = _history_line(r, bench)
    if h:
        parts.append(h)
    sk = _stock_line(r, bench)
    if sk:
        parts.append(sk)
    if r["earnings"]:
        when = f" ({r['days_to_earn']} days)" if r["days_to_earn"] is not None and r["days_to_earn"] >= 0 else ""
        parts.append(f"Next {r['ticker']} earnings: {r['earnings']}{when}.")
    return " ".join(parts)


def _dedupe(items):
    """One item per ticker+kind: prefer the specific device row over the
    company row, keep the higher R if both are devices."""
    best = {}
    for it in items:
        k = (it["r"]["ticker"], it["kind"])
        cur = best.get(k)
        if cur is None:
            best[k] = it
            continue
        a, b = it["r"], cur["r"]
        if (not a["combined"] and b["combined"]) or (a["combined"] == b["combined"] and a["r"] > b["r"]):
            best[k] = it
    out = list(best.values())
    seen_ticker = set()
    final = []
    for it in sorted(out, key=lambda i: (not i["hot"], -i["r"]["r"])):
        key = it["r"]["ticker"]
        if key in seen_ticker:
            it["head"] += " (also)"
        seen_ticker.add(key)
        final.append(it)
    return final


def build_digest(all_res, meta, diffs, transitions):
    """Returns (subject, html, text, noteworthy) for the digest email."""
    date_label = datetime.now().strftime("%a %b %d, %Y")
    bench = meta.get("benchmark_primary", "IHI")
    refreshed = meta.get("data_refreshed")
    fda_lu = meta.get("fda_last_updated") or "unknown"
    rows = [_row(did, res, diffs, meta) for did, res in all_res.items() if res.get("stats")]
    order = {"CRITICAL": 0, "ELEVATED": 1, "WATCH": 2, "NORMAL": 3}
    rows.sort(key=lambda r: (order.get(r["signal"], 9), -r["r"]))
    by_id = {r["id"]: r for r in rows}
    tmap = {t["device"]: t for t in transitions}

    # ---------- items ----------
    items = []
    for t in transitions:
        r = by_id.get(t["device"])
        if not r:
            continue
        verb = "UPGRADED" if t["upgraded"] else "downgraded"
        items.append({"kind": "signal", "hot": t["upgraded"], "r": r,
                      "head": f"{r['name']} {verb} {t['from']} \u2192 {t['to']} (R {r['r']})"})
    for r in rows:
        for rc in r["new_recalls"]:
            items.append({"kind": "recall", "hot": True, "r": r,
                          "head": f"New FDA recall: {r['name']}, {rc.get('classification','')} initiated "
                                  f"{rc.get('initiated','')}. {rc.get('reason','')}"})
    for r in rows:
        if r["sev_base_ok"] and r["dpk3"] and r["dpk12"] and r["dpk3"] / r["dpk12"] >= 1.75 and r["deaths3"] >= 3:
            items.append({"kind": "severity", "hot": True, "r": r,
                          "head": f"Severity shift: {r['name']} deaths per 1,000 reports at "
                                  f"{r['dpk3'] / r['dpk12']:.1f}x its baseline ({r['deaths3']} deaths in 3 months)"})
    for r in rows:
        if r["surge"]:
            items.append({"kind": "surge", "hot": True, "r": r,
                          "head": f"Surge: {r['name']} at {_mult(r['vs_med'])} its normal monthly volume"})
    for r in rows:
        if r["z"] >= 1.5 and not r["surge"]:
            items.append({"kind": "z", "hot": False, "r": r,
                          "head": f"Above trend: {r['name']} trend-adjusted z {r['z']:+.2f} (trigger 1.5)"})
    for r in rows:
        w = r["wow"]
        if w is not None and abs(w) >= max(100, 0.10 * r["latest"]):
            items.append({"kind": "wow", "hot": False, "r": r,
                          "head": f"Week-over-week: {r['name']} {w:+,} reports vs 7 days ago"})
    items = _dedupe(items)
    for it in items:
        d2e = it["r"]["days_to_earn"]
        if d2e is not None and 0 <= d2e <= 14:
            it["head"] += f" \u2014 earnings in {d2e} days"
    noteworthy = bool(items)

    # ---------- bottom line ----------
    n_crit = sum(1 for r in rows if r["signal"] == "CRITICAL" and not r["combined"])
    hot = [it for it in items if it["hot"]]
    near = sorted({(it["r"]["ticker"], it["r"]["days_to_earn"]) for it in items
                   if it["r"]["days_to_earn"] is not None and 0 <= it["r"]["days_to_earn"] <= 21},
                  key=lambda x: x[1])
    deteriorating = [r for r in rows if not r["combined"] and r["prev_r"] is not None and r["r"] - r["prev_r"] >= 15]
    improving = [r for r in rows if not r["combined"] and r["prev_r"] is not None and r["prev_r"] - r["r"] >= 15]
    bl = []
    if refreshed is False:
        bl.append("No new FDA data today; nothing below has changed since the last load.")
    elif refreshed:
        bl.append("New FDA data arrived.")
    if hot:
        bl.append("Needs a human read: " + "; ".join(it["head"].split(" \u2014 ")[0] for it in hot[:4]) + ".")
    else:
        bl.append("No upgrades, recalls, severity shifts, or surges.")
    if near:
        bl.append("Earnings inside three weeks for flagged names: "
                  + ", ".join(f"{tk} ({d}d)" for tk, d in near) + ".")
    if deteriorating:
        bl.append("Deteriorating since last load: " + ", ".join(f"{r['name']} (R {r['prev_r']}\u2192{r['r']})" for r in deteriorating[:4]) + ".")
    if improving:
        bl.append("Improving: " + ", ".join(f"{r['name']} (R {r['prev_r']}\u2192{r['r']})" for r in improving[:4]) + ".")
    bottom_line = " ".join(bl)

    n_up = sum(1 for i in items if i["kind"] == "signal" and i["hot"])
    n_rc = sum(1 for i in items if i["kind"] == "recall")
    n_sv = sum(1 for i in items if i["kind"] == "severity")
    n_sg = sum(1 for i in items if i["kind"] == "surge")
    bits = []
    if n_up: bits.append(f"{n_up} upgrade{'s' if n_up > 1 else ''}")
    if n_rc: bits.append(f"{n_rc} new recall{'s' if n_rc > 1 else ''}")
    if n_sv: bits.append(f"{n_sv} severity shift{'s' if n_sv > 1 else ''}")
    if n_sg: bits.append(f"{n_sg} surge{'s' if n_sg > 1 else ''}")
    if not bits: bits.append("no state changes")
    prefix = ("tracking started" if refreshed is None else "NEW FDA DATA" if refreshed else "no new FDA data")
    subject = (f"MAUDE Monitor {datetime.now().strftime('%b %d')}: {prefix} | "
               + ", ".join(bits) + f" | {n_crit} critical")

    # ---------- HTML ----------
    H = []
    H.append('<!DOCTYPE html><html><body style="margin:0;padding:0;background:#eef0ee;">')
    H.append('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#eef0ee;">'
             '<tr><td align="center" style="padding:20px 10px;">')
    H.append('<table role="presentation" width="700" cellpadding="0" cellspacing="0" '
             'style="max-width:700px;width:100%;background:#ffffff;border-radius:10px;'
             'font-family:Helvetica,Arial,sans-serif;color:#1d2420;font-size:14px;line-height:1.5;">')
    H.append(f'<tr><td style="background:#1d5a66;color:#ffffff;padding:18px 24px;border-radius:10px 10px 0 0;">'
             f'<div style="font-size:20px;font-weight:700;">MAUDE Monitor</div>'
             f'<div style="font-size:13px;opacity:.9;">{_esc(date_label)} &nbsp;&middot;&nbsp; '
             f'<a href="{DASHBOARD_URL}" style="color:#ffffff;text-decoration:underline;">Open the dashboard</a></div>'
             f'</td></tr>')
    if refreshed is None:
        band_bg, band_tx = "#f2f3f0", "#4a554e"
        band = (f"FDA dataset last updated {fda_lu}. Refresh tracking started with this run; from the next run "
                f"this band will say whether FDA has published new data.")
    elif refreshed:
        band_bg, band_tx = "#e3f1e8", "#1f5c38"
        band = f"New FDA data has arrived since the last run (dataset last updated {fda_lu}). Everything below reflects it."
    else:
        band_bg, band_tx = "#f2f3f0", "#4a554e"
        band = (f"No new FDA data since the last run (dataset last updated {fda_lu}). The board is unchanged; "
                f"FDA publishes MAUDE updates periodically rather than daily.")
    H.append(f'<tr><td style="background:{band_bg};color:{band_tx};padding:10px 24px;font-size:13px;">{_esc(band)}</td></tr>')

    def section(title):
        H.append(f'<tr><td style="padding:18px 24px 6px;font-size:12px;letter-spacing:.04em;'
                 f'color:#7a857d;font-weight:700;">{_esc(title)}</td></tr>')

    section("Bottom line")
    H.append(f'<tr><td style="padding:0 24px;font-size:14px;line-height:1.55;">{_esc(bottom_line)}</td></tr>')

    section("What matters today")
    if items:
        H.append('<tr><td style="padding:0 24px;">')
        for it in items[:8]:
            color = "#b3272e" if it["hot"] else "#bc5f04"
            H.append(f'<div style="border-left:3px solid {color};padding:8px 12px;margin:0 0 10px;'
                     f'background:#f2f3f0;border-radius:4px;"><div style="font-weight:700;">{_esc(it["head"])}</div>'
                     f'<div style="color:#2b332e;margin-top:3px;">{_esc(_plain(it["r"], bench))}</div>'
                     f'<div style="margin-top:4px;"><a href="{DASHBOARD_URL}#card-{it["r"]["id"]}" '
                     f'style="color:#1d5a66;font-size:12px;">Open card on dashboard</a></div></div>')
        H.append('</td></tr>')
    else:
        H.append('<tr><td style="padding:0 24px;color:#4a554e;">Nothing requires action today.</td></tr>')

    # grouped rows: company row first, then its devices
    def grouped(rows):
        out = []
        by_tk = {}
        for r in rows:
            by_tk.setdefault(r["ticker"], []).append(r)
        for tk in sorted(by_tk, key=lambda t: min(order.get(x["signal"], 9) * 1000 - x["r"] for x in by_tk[t])):
            grp = by_tk[tk]
            comb = [x for x in grp if x["combined"]]
            devs = [x for x in grp if not x["combined"]]
            if comb:
                out.append((comb[0], False))
                out.extend((x, True) for x in sorted(devs, key=lambda x: -x["r"]))
            else:
                out.extend((x, False) for x in sorted(devs, key=lambda x: -x["r"]))
        return out

    section("Signal board (grouped by company, worst first)")
    H.append('<tr><td style="padding:0 24px;">'
             '<table role="presentation" width="100%" cellpadding="5" cellspacing="0" '
             'style="border-collapse:collapse;font-size:12px;">'
             '<tr style="background:#f2f3f0;color:#4a554e;">'
             '<th align="left">Device</th><th align="left">Signal</th><th align="right">R</th>'
             '<th align="right">&Delta;R vs last load</th><th align="right">z adj</th>'
             f'<th align="right">Stock 1mo vs {_esc(bench)}</th><th align="right">&Delta; 7d</th>'
             '<th align="right">Next earnings</th></tr>')
    for r, indent in grouped(rows):
        dr = "" if r["prev_r"] is None else f"{r['r'] - r['prev_r']:+d}"
        drc = "#b3272e" if dr.startswith("+") and dr != "+0" else "#2c6e49" if dr.startswith("-") else "#7a857d"
        rel = (r.get("perf") or {}).get("1mo_rel")
        pad = "padding-left:18px;color:#4a554e;" if indent else "font-weight:600;"
        H.append(f'<tr style="border-bottom:1px solid #e3e6e1;">'
                 f'<td style="{pad}">{_esc(r["name"])}<span style="color:#7a857d;font-weight:400;"> {_esc(r["ticker"])}</span></td>'
                 f'<td>{_pill(r["signal"])}</td><td align="right"><b>{r["r"]}</b></td>'
                 f'<td align="right" style="color:{drc};">{dr or "n/a"}</td>'
                 f'<td align="right">{r["z"]:+.2f}</td>'
                 f'<td align="right">{"n/a" if rel is None else f"{rel:+.1f}%"}</td>'
                 f'<td align="right">{_fmt_delta(r["wow"])}</td>'
                 f'<td align="right">{_esc(r["earnings"] or "n/a")}</td></tr>')
    H.append('</table></td></tr>')
    wsd = meta.get("week_snapshot_date")
    H.append(f'<tr><td style="padding:6px 24px 0;font-size:11.5px;color:#7a857d;">&Delta;R compares the R-score to the '
             f'{"snapshot from " + _esc(wsd) if wsd else "previous run"} (red = deteriorating). Stock column is the share price move '
             f'over the last month relative to {_esc(bench)}: if the stock has already lagged, the market has priced some of this in. '
             f'&Delta; 7d is new reports vs a week ago and includes FDA backfill.</td></tr>')

    section("Monthly read (latest complete month vs history)")
    H.append('<tr><td style="padding:0 24px;">'
             '<table role="presentation" width="100%" cellpadding="5" cellspacing="0" '
             'style="border-collapse:collapse;font-size:12px;">'
             '<tr style="background:#f2f3f0;color:#4a554e;">'
             '<th align="left">Device</th><th align="right">Reports</th><th align="right">vs prior mo</th>'
             '<th align="right">vs yr ago</th><th align="right">vs 12-mo median</th>'
             '<th align="right">Deaths/1k (3mo vs base)</th><th align="left">Flag</th></tr>')
    for r, indent in grouped(rows):
        flag = "surge" if r["surge"] else "batch filing" if r["batch"] else "small base" if r["small_base"] else ""
        fcolor = "#b3272e" if r["surge"] else "#7a857d"
        if r["sev_base_ok"] and r["dpk3"] is not None and r["dpk12"] is not None:
            dpk = f"{r['dpk3']:.2f} vs {r['dpk12']:.2f}"
        else:
            dpk = '<span style="color:#7a857d;">base too small</span>'
        pad = "padding-left:18px;color:#4a554e;" if indent else "font-weight:600;"
        H.append(f'<tr style="border-bottom:1px solid #e3e6e1;"><td style="{pad}">{_esc(r["name"])}'
                 f'<span style="color:#7a857d;font-weight:400;"> {_esc(r["ticker"])}</span></td>'
                 f'<td align="right">{r["latest"]:,}</td><td align="right">{_pct(r["mom"])}</td>'
                 f'<td align="right">{_pct(r["yoy"])}</td><td align="right">{_mult(r["vs_med"])}</td>'
                 f'<td align="right">{dpk}</td><td style="color:{fcolor};">{flag}</td></tr>')
    H.append('</table></td></tr>')
    provm = next((r["month"] for r in rows if r["provisional"]), None)
    if provm:
        H.append(f'<tr><td style="padding:6px 24px 0;font-size:11.5px;color:#7a857d;">Latest complete month is '
                 f'{_mon(provm)} and is provisional: counts keep rising for weeks as late reports arrive, so the '
                 f'month-over-month figure is biased low until it matures. Deaths per 1,000 is shown only where the '
                 f'3-month base is at least 300 reports.</td></tr>')

    flagged = [r for r in rows if r["signal"] in ("CRITICAL", "ELEVATED") and not r["combined"]]
    if flagged:
        section("Why these are flagged")
        H.append('<tr><td style="padding:0 24px;">')
        for r in flagged[:8]:
            det = r["detail"] or {}
            hits = [c for c in det.get("components", []) if c["points"] > 0]
            why = "; ".join(f"{c['factor'].split(' (')[0].split(',')[0]} {c['value']} ({c['points']} pts)" for c in hits)
            H.append(f'<div style="margin:0 0 8px;"><b>{_esc(r["name"])}</b> is {SIG_PLAIN[r["signal"]]}. '
                     f'{_esc(why) if why else "No factor tripped a threshold."}</div>')
        H.append('</td></tr>')
        section("What the stock did after prior spikes")
        H.append('<tr><td style="padding:0 24px;">'
                 '<table role="presentation" width="100%" cellpadding="5" cellspacing="0" '
                 'style="border-collapse:collapse;font-size:12px;">'
                 f'<tr style="background:#f2f3f0;color:#4a554e;"><th align="left">Device</th><th align="right">Prior spikes</th>'
                 f'<th align="right">Stock lagged {_esc(bench)}</th><th align="right">Avg vs {_esc(bench)} ({_esc(rows[0]["horizon"])})</th>'
                 f'<th align="right">Short P&amp;L per $10K</th><th align="left">Reliability</th></tr>')
        for r in flagged[:8]:
            if r["car_n"]:
                H.append(f'<tr style="border-bottom:1px solid #e3e6e1;"><td>{_esc(r["name"])}</td>'
                         f'<td align="right">{r["car_n"]}</td><td align="right">{r["car_hit"]:.0f}%</td>'
                         f'<td align="right">{r["car_mean"]:+.1f}%</td>'
                         f'<td align="right">${r["car_pnl"]:+,.0f}</td><td>{_esc(r["car_grade"])}</td></tr>')
            else:
                H.append(f'<tr style="border-bottom:1px solid #e3e6e1;"><td>{_esc(r["name"])}</td>'
                         f'<td align="right" colspan="5" style="color:#7a857d;">not enough history</td></tr>')
        H.append('</table></td></tr>')
        H.append('<tr><td style="padding:6px 24px 0;font-size:11.5px;color:#7a857d;">A prior spike is a month the trend-adjusted '
                 'z crossed 1.5; the stock\'s next-3-month return is measured against the medtech ETF, so a short that merely '
                 'tracked the sector scores zero. Reliability: STRONG = lagged 60%+ of the time over 3+ spikes; MODERATE 45-60%; '
                 'WEAK below. Samples are small; this is context, not a forecast.</td></tr>')

    fund = meta.get("fundamentals", {})
    H.append('<tr><td style="padding:18px 24px 22px;font-size:11.5px;color:#7a857d;line-height:1.55;border-top:1px solid #e3e6e1;">'
             'Signals: CRITICAL = R-score 70+, ELEVATED 50-69, WATCH 30-49, NORMAL below 30. Every R-score factor is relative to '
             'the device\'s own history (z vs trend, slope as % of run rate, deaths and injuries per 1,000 reports vs the prior '
             '12 months, volume vs the 12-month median), so the score measures deterioration, not size. '
             'MAUDE reports are unverified submissions and do not establish causation; counts are not incidence rates. '
             f'Earnings dates are from a market-data feed and should be confirmed. Fundamentals verified: '
             f'{fund.get("verified", 0)}/{fund.get("total", 0)}. Run {_esc(meta.get("run_at", ""))}, mode {_esc(meta.get("mode", ""))}. '
             f'<a href="{DASHBOARD_URL}" style="color:#1d5a66;">Full detail on the dashboard</a>.</td></tr>')
    H.append('</table></td></tr></table></body></html>')
    html = "".join(H)

    T = [f"MAUDE Monitor {date_label}", DASHBOARD_URL, "", band, "", "BOTTOM LINE", bottom_line, "", "WHAT MATTERS TODAY"]
    T += [f"  - {it['head']}\n    {_plain(it['r'], bench)}" for it in items[:8]] or ["  Nothing requires action today."]
    T += ["", "SIGNAL BOARD"]
    for r, indent in grouped(rows):
        T.append(f"  {'  ' if indent else ''}{r['name']:<28} {r['signal']:<9} R={r['r']:<3} "
                 f"dR={'n/a' if r['prev_r'] is None else r['r'] - r['prev_r']:>4} z={r['z']:+.2f} "
                 f"mom={_pct(r['mom'])} yoy={_pct(r['yoy'])} earnings={r['earnings'] or 'n/a'}")
    text = "\n".join(T)
    return subject, html, text, noteworthy


def _send_resend(subject, html, text):
    payload = json.dumps({"from": RESEND_FROM, "to": ALERT_TO, "subject": subject,
                          "html": html, "text": text}).encode()
    req = Request("https://api.resend.com/emails", data=payload, method="POST",
                  headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                           "Content-Type": "application/json",
                           "Accept": "application/json",
                           # Resend is fronted by Cloudflare, which rejects
                           # Python's default user agent (error 1010)
                           "User-Agent": "Mozilla/5.0 (compatible; MAUDE-Monitor/4.0; +https://github.com/JtrunzPHP/maude-monitor)"})
    with urlopen(req, timeout=30) as resp:
        body = resp.read().decode()
    return body[:160]


def send_digest(all_res, meta, diffs, transitions):
    if not ALERT_TO:
        return {"sent": False, "reason": "ALERT_TO not set"}
    if not (RESEND_API_KEY or SMTP_HOST):
        return {"sent": False, "reason": "no email provider configured (RESEND_API_KEY or SMTP_*)"}
    subject, html, text, noteworthy = build_digest(all_res, meta, diffs, transitions)
    if DIGEST_MODE == "changes" and not noteworthy:
        return {"sent": False, "reason": "no changes (DIGEST_MODE=changes)"}
    if RESEND_API_KEY:
        try:
            info = _send_resend(subject, html, text)
            print(f"  Digest sent via Resend to {', '.join(ALERT_TO)}: {subject} ({info})")
            return {"sent": True, "subject": subject, "channel": "resend"}
        except Exception as e:
            detail = ""
            try:
                detail = e.read().decode()[:200]
            except Exception:
                pass
            print(f"  Digest email failed (Resend): {str(e)[:100]} {detail}")
            return {"sent": False, "reason": f"resend: {str(e)[:100]} {detail}"}
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = ALERT_FROM or SMTP_USER
        msg["To"] = ", ".join(ALERT_TO)
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as srv:
            srv.starttls()
            if SMTP_USER:
                srv.login(SMTP_USER, SMTP_PASS)
            srv.sendmail(msg["From"], ALERT_TO, msg.as_string())
        print(f"  Digest emailed to {len(ALERT_TO)} recipient(s): {subject}")
        return {"sent": True, "subject": subject}
    except Exception as e:
        print(f"  Digest email failed: {str(e)[:120]}")
        return {"sent": False, "reason": str(e)[:120]}
