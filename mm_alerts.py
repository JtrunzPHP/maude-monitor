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
from datetime import datetime
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


def write_snapshot(all_res):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {"run_at": datetime.now().isoformat(timespec="seconds"), "devices": {}}
    for did, res in all_res.items():
        recv = res.get("recv", {})
        recalls = (res.get("modules", {}).get("recalls") or {}).get("recalls", [])
        snap["devices"][did] = {"total": sum(recv.values()), "by_month": recv,
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


def diff_snapshot(all_res, prev):
    """Returns {device_id: {new_reports, prev_run, changed_months}}."""
    out = {}
    if not prev:
        return out
    pdev = prev.get("devices", {})
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
        out[did] = {"new_reports": new_total, "prev_run": prev.get("run_at", ""),
                    "changed_months": changed[-6:], "new_recalls": new_recalls}
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


def build_digest(all_res, meta, diffs, transitions):
    """Returns (subject, html, text, noteworthy) for the digest email."""
    date_label = datetime.now().strftime("%a %b %d, %Y")
    rows = []
    for did, res in all_res.items():
        st = res.get("stats")
        if not st:
            continue
        d = diffs.get(did, {})
        rows.append({"id": did, "name": res["device"]["name"], "ticker": res["device"]["ticker"],
                     "signal": res["signal"], "r": res.get("r_score") or 0,
                     "z": st["z_score_adj"], "latest": st["latest_value"],
                     "month": st["latest_month"], "provisional": st["provisional"],
                     "deaths3": st["deaths_3mo"], "inj3": st["injuries_3mo"],
                     "new": d.get("new_reports"), "new_recalls": d.get("new_recalls", []),
                     "combined": res["device"]["is_combined"],
                     "detail": res.get("r_detail")})
    order = {"CRITICAL": 0, "ELEVATED": 1, "WATCH": 2, "NORMAL": 3}
    rows.sort(key=lambda r: (order.get(r["signal"], 9), -r["r"]))

    # --- what changed ---
    changes = []
    for t in transitions:
        name = all_res.get(t["device"], {}).get("device", {}).get("name", t["device"])
        res = all_res.get(t["device"], {})
        det = res.get("r_detail") or {}
        hits = [c for c in det.get("components", []) if c["points"] > 0]
        why = "; ".join(f"{(lambda f: f[0].lower() + f[1:])(c['factor'].split(' (')[0])} "
                        f"{c['value']} ({c['points']} pts)" for c in hits[:3])
        verb = "upgraded" if t["upgraded"] else "downgraded"
        changes.append({"kind": "signal", "upgraded": t["upgraded"],
                        "text": f"{name} {verb} from {t['from']} to {t['to']}"
                                + (f". Drivers: {why}." if why else ".")})
    for r in rows:
        for rc in r["new_recalls"]:
            changes.append({"kind": "recall", "upgraded": True,
                            "text": f"New FDA recall for {r['name']}: {rc.get('classification','')} "
                                    f"initiated {rc.get('initiated','')}. {rc.get('reason','')}"})
    zcross = [r for r in rows if r["z"] >= 1.5 and not any(
        t["device"] == r["id"] for t in transitions)]
    movers = sorted([r for r in rows if r["new"]], key=lambda r: -abs(r["new"]))[:5]
    noteworthy = bool(changes) or bool(zcross)

    n_up = sum(1 for c in changes if c["kind"] == "signal" and c["upgraded"])
    n_rc = sum(1 for c in changes if c["kind"] == "recall")
    n_crit = sum(1 for r in rows if r["signal"] == "CRITICAL")
    n_elev = sum(1 for r in rows if r["signal"] == "ELEVATED")
    bits = []
    if n_up:
        bits.append(f"{n_up} upgrade{'s' if n_up > 1 else ''}")
    if n_rc:
        bits.append(f"{n_rc} new recall{'s' if n_rc > 1 else ''}")
    if zcross:
        bits.append(f"{len(zcross)} above z trigger")
    if not bits:
        bits.append("no signal changes")
    subject = f"MAUDE Monitor {datetime.now().strftime('%b %d')}: " + ", ".join(bits) \
              + f" | {n_crit} critical, {n_elev} elevated"

    # --- HTML ---
    H = []
    H.append('<!DOCTYPE html><html><body style="margin:0;padding:0;background:#eef0ee;">')
    H.append('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#eef0ee;">'
             '<tr><td align="center" style="padding:20px 10px;">')
    H.append('<table role="presentation" width="660" cellpadding="0" cellspacing="0" '
             'style="max-width:660px;width:100%;background:#ffffff;border-radius:10px;'
             'font-family:Helvetica,Arial,sans-serif;color:#1d2420;font-size:14px;line-height:1.5;">')
    H.append(f'<tr><td style="background:#1d5a66;color:#ffffff;padding:18px 24px;border-radius:10px 10px 0 0;">'
             f'<div style="font-size:20px;font-weight:700;">MAUDE Monitor</div>'
             f'<div style="font-size:13px;opacity:.9;">{_esc(date_label)} &nbsp;&middot;&nbsp; '
             f'<a href="{DASHBOARD_URL}" style="color:#ffffff;text-decoration:underline;">Open the dashboard</a></div>'
             f'</td></tr>')

    def section(title):
        H.append(f'<tr><td style="padding:18px 24px 6px;font-size:12px;letter-spacing:.04em;'
                 f'color:#7a857d;font-weight:700;">{_esc(title)}</td></tr>')

    section("What changed since the last run")
    if changes:
        H.append('<tr><td style="padding:0 24px;">')
        for c in changes:
            color = "#b3272e" if c["upgraded"] else "#2c6e49"
            H.append(f'<div style="border-left:3px solid {color};padding:6px 10px;margin:0 0 8px;'
                     f'background:#f2f3f0;border-radius:4px;">{_esc(c["text"])}</div>')
        H.append('</td></tr>')
    else:
        H.append('<tr><td style="padding:0 24px;color:#4a554e;">No signal changes or new recalls. '
                 'Report counts moved as listed below.</td></tr>')
    if zcross:
        H.append('<tr><td style="padding:8px 24px 0;color:#4a554e;">Above the z trigger (1.5) without a signal change: '
                 + ", ".join(f"{_esc(r['name'])} (z {r['z']:+.2f})" for r in zcross) + '.</td></tr>')
    if movers:
        H.append('<tr><td style="padding:8px 24px 0;color:#4a554e;">Largest report-count moves since last run: '
                 + ", ".join(f"{_esc(r['name'])} {_fmt_delta(r['new'])}" for r in movers)
                 + '. Includes FDA backfill of prior months.</td></tr>')

    section("Signal board (worst first)")
    H.append('<tr><td style="padding:0 24px;">'
             '<table role="presentation" width="100%" cellpadding="6" cellspacing="0" '
             'style="border-collapse:collapse;font-size:12.5px;">'
             '<tr style="background:#f2f3f0;color:#4a554e;">'
             '<th align="left">Device</th><th align="left">Signal</th><th align="right">R</th>'
             '<th align="right">z adj</th><th align="right">Reports</th><th align="right">&Delta; run</th>'
             '<th align="right">D / I 3mo</th></tr>')
    for r in rows:
        prov = "*" if r["provisional"] else ""
        H.append(f'<tr style="border-bottom:1px solid #e3e6e1;">'
                 f'<td>{_esc(r["name"])}<span style="color:#7a857d;"> {_esc(r["ticker"])}</span></td>'
                 f'<td>{_pill(r["signal"])}</td><td align="right"><b>{r["r"]}</b></td>'
                 f'<td align="right">{r["z"]:+.2f}</td><td align="right">{r["latest"]:,}{prov}</td>'
                 f'<td align="right">{_fmt_delta(r["new"])}</td>'
                 f'<td align="right">{r["deaths3"]} / {r["inj3"]:,}</td></tr>')
    H.append('</table></td></tr>')
    provm = next((r["month"] for r in rows if r["provisional"]), None)
    if provm:
        H.append(f'<tr><td style="padding:6px 24px 0;font-size:11.5px;color:#7a857d;">* {provm} is the latest '
                 f'complete month and is still provisional; MAUDE counts keep rising for weeks as late reports arrive.</td></tr>')

    watch = [r for r in rows if (r["z"] >= 1.0 or r["deaths3"] >= 1) and r["signal"] != "NORMAL"]
    if watch:
        section("Why these are flagged")
        H.append('<tr><td style="padding:0 24px;">')
        for r in watch[:6]:
            det = r["detail"] or {}
            hits = [c for c in det.get("components", []) if c["points"] > 0]
            why = "; ".join(f"{c['factor'].split(' (')[0]} at {c['value']} ({c['points']} pts)" for c in hits)
            H.append(f'<div style="margin:0 0 8px;"><b>{_esc(r["name"])}</b> is {SIG_PLAIN[r["signal"]]}. '
                     f'{_esc(why) if why else "No factor tripped a threshold."}</div>')
        H.append('</td></tr>')

    fund = meta.get("fundamentals", {})
    H.append('<tr><td style="padding:18px 24px 22px;font-size:11.5px;color:#7a857d;line-height:1.55;border-top:1px solid #e3e6e1;">'
             'Signals: CRITICAL = R-score 70+, ELEVATED 50-69, WATCH 30-49, NORMAL below 30. '
             'The R-score combines the trend-adjusted z (latest month vs the device\'s own trend), 6-month slope, '
             'deaths and injuries in the last 3 months, and reports per $M revenue. '
             'MAUDE reports are unverified submissions and do not establish causation; counts are not incidence rates. '
             f'Fundamentals verified: {fund.get("verified", 0)}/{fund.get("total", 0)}; rate-based metrics are flagged on the dashboard until sourced. '
             f'Run {_esc(meta.get("run_at", ""))}, mode {_esc(meta.get("mode", ""))}. '
             f'<a href="{DASHBOARD_URL}" style="color:#1d5a66;">Full detail on the dashboard</a>.</td></tr>')
    H.append('</table></td></tr></table></body></html>')
    html = "".join(H)

    # --- plain text fallback ---
    T = [f"MAUDE Monitor {date_label}", DASHBOARD_URL, "", "WHAT CHANGED"]
    T += [f"  - {c['text']}" for c in changes] or ["  No signal changes or new recalls."]
    T += ["", "SIGNAL BOARD"]
    for r in rows:
        T.append(f"  {r['name']:<28} {r['signal']:<9} R={r['r']:<3} z={r['z']:+.2f} "
                 f"reports={r['latest']:,} delta={_fmt_delta(r['new'])}")
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
