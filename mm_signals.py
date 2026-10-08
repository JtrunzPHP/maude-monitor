"""MAUDE Monitor V4 — signal modules.

Changes vs V3:
- Correlation suite: Benjamini-Hochberg FDR across the full signal x lag
  grid. Headline significance is q < FDR_Q. Raw p still reported.
- PRR: computed in a SECOND pass after all devices are fetched, against a
  comparator of all other non-combined devices. V3 accumulated the
  comparator during the loop, so early devices were compared against
  almost nothing (order-dependent results), and combined rows
  double-counted their own individual devices.
- Cross-company ranking: same second-pass fix.
- Rate-based correlation signals are only included when the underlying
  fundamentals series is marked verified; otherwise they are listed as
  excluded so no conclusion rests on unverified inputs.
"""

import math

from mm_config import FDR_Q
from mm_stats import spearman, benjamini_hochberg, trailing_z


# ----------------------------------------------------------------------
# MAUDE -> stock correlation suite
# ----------------------------------------------------------------------
def correlation_suite(counts, stock, max_lag=6, revenue=None, installed_base=None,
                      revenue_verified=False, base_verified=False):
    if not counts or not stock:
        return {"status": "insufficient_data", "message": "Missing MAUDE or market data.",
                "best_rho": 0, "best_p": 1.0, "best_q": 1.0, "best_lag": 0,
                "significant": False, "direction": "none", "signal_analysis": {},
                "confidence": 0, "excluded_signals": []}
    common = sorted(set(counts) & set(stock))
    if len(common) < 14:
        return {"status": "insufficient_data",
                "message": f"{len(common)} overlapping months; need 14+.",
                "best_rho": 0, "best_p": 1.0, "best_q": 1.0, "best_lag": 0,
                "significant": False, "direction": "none", "signal_analysis": {},
                "confidence": 0, "excluded_signals": []}

    mc = [counts[m] for m in common]
    sp = [stock[m] for m in common]
    sret = [0.0] + [(sp[i] - sp[i - 1]) / sp[i - 1] * 100 if sp[i - 1] > 0 else 0.0
                    for i in range(1, len(sp))]

    def q_of(m):
        yr, mo = m.split("-")
        return f"{yr}-Q{(int(mo) - 1) // 3 + 1}"

    signals = {"raw_counts": mc,
               "count_delta": [0.0] + [mc[i] - mc[i - 1] for i in range(1, len(mc))]}
    signals["z_score"] = trailing_z(mc, window=6)
    dl = signals["count_delta"]
    signals["acceleration"] = [0.0, 0.0] + [dl[i] - dl[i - 1] for i in range(2, len(dl))]

    excluded = []
    if revenue:
        if revenue_verified:
            rr, last = [], 0.0
            for i, m in enumerate(common):
                qr = revenue.get(q_of(m))
                last = mc[i] / (qr / 3) * 1e6 if qr else last
                rr.append(last)
            signals["rate_per_rev"] = rr
        else:
            excluded.append("rate_per_rev (revenue series unverified)")
    if installed_base:
        if base_verified:
            rb, last = [], 0.0
            for i, m in enumerate(common):
                ib = installed_base.get(q_of(m))
                last = mc[i] / ib * 10000 if ib else last
                rb.append(last)
            signals["rate_per_base"] = rb
        else:
            excluded.append("rate_per_base (installed-base series unverified)")

    tests = []
    for sn, sv in signals.items():
        for lag in range(0, min(max_lag + 1, len(sv) - 6)):
            ss = sv[:len(sv) - lag] if lag > 0 else sv
            rs = sret[lag:] if lag > 0 else sret
            ml = min(len(ss), len(rs))
            if ml < 8:
                continue
            a, b = ss[:ml], rs[:ml]
            if all(v == a[0] for v in a) or all(v == b[0] for v in b):
                continue
            rho, p = spearman(a, b)
            tests.append({"signal": sn, "lag": lag, "rho": rho, "p": p})
    if not tests:
        return {"status": "insufficient_data", "message": "No valid tests.",
                "best_rho": 0, "best_p": 1.0, "best_q": 1.0, "best_lag": 0,
                "significant": False, "direction": "none", "signal_analysis": {},
                "confidence": 0, "excluded_signals": excluded}

    benjamini_hochberg(tests)

    sa = {}
    for sn in signals:
        rows = [t for t in tests if t["signal"] == sn]
        if not rows:
            continue
        best = max(rows, key=lambda t: abs(t["rho"]))
        sa[sn] = {"best_rho": best["rho"], "best_p": best["p"], "best_q": best["q"],
                  "best_lag": best["lag"], "significant": best["q"] < FDR_Q,
                  "direction": "negative" if best["rho"] < 0 else "positive",
                  "lag_detail": {f"{t['lag']}mo": {"rho": t["rho"], "p": t["p"],
                                                   "q": t["q"]} for t in rows}}

    best = max(tests, key=lambda t: abs(t["rho"]))
    osig = best["q"] < FDR_Q
    sig_count = sum(1 for s in sa.values() if s["significant"])
    neg_count = sum(1 for s in sa.values() if s["significant"] and s["direction"] == "negative")
    aar = sum(abs(s["best_rho"]) for s in sa.values()) / max(len(sa), 1)
    con = neg_count / max(sig_count, 1)
    conf = min(100, int(abs(best["rho"]) * 40 + aar * 20
                        + sig_count / max(len(sa), 1) * 20 + con * 20))

    msg = (f"Best: rho={best['rho']:+.3f} at {best['lag']}mo lag "
           f"(p={best['p']:.4f}, q={best['q']:.4f}, signal={best['signal']}, "
           f"{len(tests)} tests, FDR-corrected). ")
    if osig and best["rho"] < -0.2:
        msg += f"MAUDE {best['signal']} leads stock declines by {best['lag']}mo. "
    elif osig and best["rho"] > 0.2:
        msg += "Positive co-movement; market likely pricing in concurrently. "
    else:
        msg += "No lead-lag survives FDR correction. "
    msg += f"Confidence {conf}/100 ({sig_count}/{len(sa)} signals significant at q<{FDR_Q})."
    if excluded:
        msg += " Excluded: " + "; ".join(excluded) + "."

    return {"status": "ok", "best_rho": best["rho"], "best_p": best["p"],
            "best_q": best["q"], "best_lag": best["lag"], "best_signal": best["signal"],
            "significant": osig, "direction": "negative" if best["rho"] < 0 else "positive",
            "signal_analysis": sa, "confidence": conf, "tests_run": len(tests),
            "signals_significant": sig_count, "signals_negative": neg_count,
            "message": msg, "excluded_signals": excluded}


# ----------------------------------------------------------------------
# PRR — two-pass, non-combined comparator universe
# ----------------------------------------------------------------------
def prr_all(failure_modes_by_device, combined_ids):
    """failure_modes_by_device: {device_id: failure-mode dict}. Comparator
    for each device = sum of all OTHER non-combined devices (combined rows
    excluded from the universe entirely to avoid double counting)."""
    universe = {did: fm for did, fm in failure_modes_by_device.items()
                if fm and fm.get("status") == "ok" and fm.get("total", 0) >= 10
                and did not in combined_ids}
    results = {}
    for did, fm in failure_modes_by_device.items():
        if not fm or fm.get("status") != "ok" or fm.get("total", 0) < 10:
            results[did] = {"status": "insufficient_data",
                            "message": "Too few classified narratives"}
            continue
        cats = fm["categories"]
        total_this = fm["total"]
        gc, gt = {}, 0
        for odid, ofm in universe.items():
            if odid == did:
                continue
            for cat, count in ofm["categories"].items():
                gc[cat] = gc.get(cat, 0) + count
            gt += ofm["total"]
        if gt < 50:
            results[did] = {"status": "ok", "prr_signals": [], "sig_count": 0,
                            "level": "NORMAL", "comparator_n": gt,
                            "message": "Comparator universe too small"}
            continue
        signals = []
        for cat, a in cats.items():
            if a < 3 or cat == "other":
                continue
            b = total_this - a
            c = gc.get(cat, 0)
            d = gt - c
            if (a + b) == 0 or (c + d) == 0:
                continue
            if c == 0:
                # Haldane-Anscombe continuity correction: a comparator with
                # ZERO cases of this mode is the strongest signal, not a
                # reason to skip (V3 skipped these)
                aa, bb, cc, dd = a + 0.5, b + 0.5, 0.5, d + 0.5
            else:
                aa, bb, cc, dd = a, b, c, d
            prr = (aa / (aa + bb)) / (cc / (cc + dd))
            exp = (aa + bb) * (aa + cc) / (aa + bb + cc + dd)
            chi2 = (aa - exp) ** 2 / exp if exp > 0 else 0.0
            if prr >= 2.0 and chi2 >= 4.0:
                signals.append({"mode": cat, "count": a, "prr": round(prr, 2),
                                "chi2": round(chi2, 1), "signal": True})
            elif prr >= 1.5:
                signals.append({"mode": cat, "count": a, "prr": round(prr, 2),
                                "chi2": round(chi2, 1), "signal": False})
        signals.sort(key=lambda x: -x["prr"])
        sc = sum(1 for s in signals if s["signal"])
        lev = "ALERT" if sc >= 2 else "WATCH" if sc >= 1 else "NORMAL"
        results[did] = {"status": "ok", "prr_signals": signals, "sig_count": sc,
                        "level": lev, "comparator_n": gt,
                        "message": f"{sc} disproportionate modes vs {gt:,} "
                                   f"comparator narratives. {lev}."}
    return results


# ----------------------------------------------------------------------
# Recall cascade position
# ----------------------------------------------------------------------
def recall_cascade(stats, recalls, events):
    score = 0
    steps = []
    evts = events or []
    if stats and stats.get("z_score_adj", 0) >= 1.5:
        score += 15
        steps.append({"step": "MAUDE anomaly", "status": "ACTIVE",
                      "detail": f"Trend-adj z={stats['z_score_adj']:+.2f}"})
    elif stats and stats.get("z_score_adj", 0) >= 1.0:
        score += 8
        steps.append({"step": "MAUDE elevated", "status": "WATCH",
                      "detail": f"Trend-adj z={stats['z_score_adj']:+.2f}"})
    if stats and stats.get("slope_6mo", 0) > 30:
        score += 10
        steps.append({"step": "Rising trend", "status": "ACTIVE",
                      "detail": f"Slope {stats['slope_6mo']:+.1f}/mo"})
    if stats and stats.get("deaths_3mo", 0) >= 1:
        score += 15
        steps.append({"step": "Deaths reported", "status": "CRITICAL",
                      "detail": f"{stats['deaths_3mo']} in 3mo"})
    reg = [e for e in evts if e.get("type") == "regulatory"]
    if reg:
        score += 20
        steps.append({"step": "Warning letter", "status": "ISSUED",
                      "detail": reg[-1].get("label", "")})
    rec = [e for e in evts if e.get("type") == "recall"]
    if rec:
        score += 25
        steps.append({"step": "Recall", "status": "ISSUED",
                      "detail": rec[-1].get("label", "")})
    if recalls and recalls.get("class1_count", 0) > 0:
        score += 15
        steps.append({"step": "Class I recall", "status": "CONFIRMED",
                      "detail": f"{recalls['class1_count']} Class I"})
    score = min(100, score)
    phase = ("LATE CASCADE" if score >= 70 else "MID CASCADE" if score >= 40
             else "EARLY CASCADE" if score >= 15 else "NO CASCADE")
    return {"status": "ok", "score": score, "phase": phase, "steps": steps,
            "message": f"Cascade position: {phase} ({score}/100), "
                       f"{len(steps)} steps active."}


# ----------------------------------------------------------------------
# Cross-company relative signal — second pass
# ----------------------------------------------------------------------
def cross_company(company_stats):
    """company_stats: {ticker: {rate, slope, z, name, rate_verified}}.
    Composite uses rate only when every participating rate is from a
    verified installed-base series; otherwise ranks on z and slope and
    says so."""
    if len(company_stats) < 3:
        return {}
    use_rate = all(d.get("rate") is not None and d.get("rate_verified")
                   for d in company_stats.values())
    mr = max(abs(d["slope"]) for d in company_stats.values()) or 1.0
    mx = max((d["rate"] or 0) for d in company_stats.values()) or 1.0
    peers = []
    for tk, d in company_stats.items():
        if use_rate:
            comp = d["z"] * 0.4 + (d["slope"] / mr) * 0.3 + ((d["rate"] or 0) / mx) * 0.3
        else:
            comp = d["z"] * 0.6 + (d["slope"] / mr) * 0.4
        peers.append({"ticker": tk, "name": d["name"],
                      "rate": round(d["rate"], 2) if d.get("rate") is not None else None,
                      "rate_verified": bool(d.get("rate_verified")),
                      "slope": round(d["slope"], 1), "z": round(d["z"], 2),
                      "composite": round(comp, 3)})
    peers.sort(key=lambda x: -x["composite"])
    note = "" if use_rate else " Rate/10K excluded from composite (unverified installed base)."
    out = {}
    for i, p in enumerate(peers):
        rank = i + 1
        total = len(peers)
        sig = ("WORST (short screen)" if rank == 1 else
               "WEAK" if rank <= max(1, round(total * 0.25)) else
               "BEST" if rank == total else
               "STRONG" if rank >= round(total * 0.75) else "NEUTRAL")
        out[p["ticker"]] = {"status": "ok", "ranking": peers, "rank": rank,
                            "total": total, "signal": sig, "composite_uses_rate": use_rate,
                            "message": f"Rank {rank}/{total} ({sig}).{note}"}
    return out


def peer_relative(r_scores):
    if not r_scores:
        return {}
    sp = sorted(r_scores.items(), key=lambda x: -x[1])
    out = {}
    total = len(sp)
    for i, (tk, score) in enumerate(sp):
        rank = i + 1
        if rank == 1:
            sig = "WORST"
        elif rank == total:
            sig = "BEST"
        elif rank <= total * 0.25:
            sig = "WEAK"
        elif rank >= total * 0.75:
            sig = "STRONG"
        else:
            sig = "NEUTRAL"
        out[tk] = {"rank": rank, "total": total, "score": score, "signal": sig,
                   "peers": sp, "message": f"R-score rank {rank}/{total} ({sig})"}
    return out


# ----------------------------------------------------------------------
# Heuristic composites (explicitly labeled heuristic on the dashboard)
# ----------------------------------------------------------------------
def recall_probability(stats, fm):
    if not stats:
        return {"status": "insufficient_data", "probability": 0}
    s = 0
    z = stats["z_score_adj"]
    if z >= 3:
        s += 25
    elif z >= 2:
        s += 15
    elif z >= 1.5:
        s += 10
    if stats["deaths_3mo"] >= 3:
        s += 25
    elif stats["deaths_3mo"] >= 1:
        s += 15
    if stats["slope_6mo"] > 50:
        s += 15
    elif stats["slope_6mo"] > 20:
        s += 10
    if fm and fm.get("status") == "ok":
        c = fm.get("categories", {})
        if c.get("alarm_alert", 0) > 5:
            s += 15
        if c.get("sensor_failure", 0) > 10:
            s += 10
    s = min(100, s)
    lev = "HIGH" if s >= 70 else "MODERATE" if s >= 40 else "LOW"
    return {"status": "ok", "probability": s, "level": lev,
            "message": f"Recall probability {lev} ({s}/100). Heuristic score, "
                       f"not a calibrated probability."}


def earnings_predictor(stats, corr, fm):
    if not stats:
        return {"status": "insufficient_data", "score": 0}
    s = 50
    factors = []
    if stats["z_score_adj"] >= 2:
        s -= 15
        factors.append(("Trend-adj MAUDE z elevated", -15))
    elif stats["z_score_adj"] <= -1:
        s += 10
        factors.append(("MAUDE below trend", 10))
    if stats["slope_6mo"] > 30:
        s -= 10
        factors.append(("Rising report trend", -10))
    elif stats["slope_6mo"] < -10:
        s += 5
        factors.append(("Declining report trend", 5))
    if corr and corr.get("significant") and corr.get("direction") == "negative":
        s -= 10
        factors.append(("FDR-significant negative lead", -10))
    if stats["deaths_3mo"] >= 2:
        s -= 10
        factors.append(("Deaths last 3mo", -10))
    if stats["injuries_3mo"] >= 20:
        s -= 5
        factors.append(("Injuries last 3mo", -5))
    if fm and fm.get("status") == "ok" and fm.get("categories", {}).get("alarm_alert", 0) > 5:
        s -= 5
        factors.append(("Alarm/alert failures", -5))
    s = max(0, min(100, s))
    out = "POSITIVE" if s >= 65 else "NEUTRAL" if s >= 40 else "NEGATIVE"
    return {"status": "ok", "score": s, "outlook": out, "factors": factors,
            "message": f"Earnings setup {out} ({s}/100). Heuristic score."}
