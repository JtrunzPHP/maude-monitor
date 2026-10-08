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

from mm_config import SNAPSHOT_DIR, STATE_FILE, SLACK_WEBHOOK_URL, \
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_TO, ALERT_FROM

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
        snap["devices"][did] = {"total": sum(recv.values()), "by_month": recv}
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
        out[did] = {"new_reports": new_total, "prev_run": prev.get("run_at", ""),
                    "changed_months": changed[-6:]}
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
    if SMTP_HOST and ALERT_TO:
        try:
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = ALERT_FROM or SMTP_USER
            msg["To"] = ", ".join(ALERT_TO)
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
                s.starttls()
                if SMTP_USER:
                    s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(msg["From"], ALERT_TO, msg.as_string())
            sent.append("email")
        except Exception as e:
            print(f"  Email alert failed: {str(e)[:80]}")
    print(body)
    return {"sent": bool(sent), "channels": sent, "body": body}
