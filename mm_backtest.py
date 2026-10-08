"""MAUDE Monitor V4 — CAR event-study backtest.

Changes vs V3:
- The exit horizon is PRE-SPECIFIED (PRIMARY_HORIZON, default 3mo). V3
  picked whichever horizon paid best after the fact, which overstates hit
  rates and P&L. Headline stats here come only from the fixed horizon.
- An exploratory per-horizon table is still produced but is labeled
  in-sample/exploratory and never feeds the headline grade.
- Entry triggers use the trend-adjusted z (walk-forward, trailing data
  only) so installed-base growth does not generate "signals".
- CAR is measured vs a medtech sector benchmark (IHI by default) with the
  broad-market benchmark (SPY) shown alongside, so device-specific moves
  are not credited to sector beta.
"""

import math

from mm_config import Z_TRIGGER, MOM_TRIGGER, SIGNAL_COOLDOWN, PRIMARY_HORIZON
from mm_stats import trend_adjusted_z

HORIZONS = [("1mo", 1), ("2mo", 2), ("3mo", 3), ("6mo", 6)]


def car_backtest(counts, stock, batch, bench_primary, bench_secondary,
                 ticker, primary_horizon=PRIMARY_HORIZON):
    if not counts or not stock:
        return {"status": "insufficient_data", "signals": [], "case_studies": [],
                "summary": {}}
    sm = sorted(set(counts) & set(stock))
    if len(sm) < 12:
        return {"status": "insufficient_data", "signals": [], "case_studies": [],
                "summary": {}}

    mc = {m: counts[m] for m in sm}
    vals = [mc[m] for m in sm]
    batch_idx = {i for i, m in enumerate(sm) if batch.get(m)}
    z_adj = trend_adjusted_z(vals, batch_idx)

    mom = [0.0]
    for i in range(1, len(sm)):
        prev = vals[i - 1]
        mom.append((vals[i] - prev) / prev * 100 if prev > 0 else 0.0)

    cases = []
    last_sig = -999
    for i, m in enumerate(sm):
        if i - last_sig < SIGNAL_COOLDOWN or i < 8:
            continue
        if i in batch_idx:
            continue
        z = z_adj[i]
        trig = None
        if z >= Z_TRIGGER:
            trig = f"Trend-adj z spike {z:+.2f}"
        elif mom[i] >= MOM_TRIGGER and z >= 1.0:
            trig = f"MoM surge +{mom[i]:.0f}% (z {z:+.2f})"
        if not trig:
            continue
        last_sig = i
        ep = stock[m]
        fwd = {}
        for hn, hm in HORIZONS:
            if i + hm >= len(sm):
                continue
            xm = sm[i + hm]
            xp = stock[xm]
            sr = (xp - ep) / ep * 100
            b1e, b1x = bench_primary.get(m, 0), bench_primary.get(xm, 0)
            br1 = (b1x - b1e) / b1e * 100 if b1e > 0 else None
            b2e, b2x = bench_secondary.get(m, 0), bench_secondary.get(xm, 0)
            br2 = (b2x - b2e) / b2e * 100 if b2e > 0 else None
            ar = sr - br1 if br1 is not None else None
            fwd[hn] = {"exit_month": xm, "exit_price": round(xp, 2),
                       "stock_ret": round(sr, 2),
                       "bench_ret": round(br1, 2) if br1 is not None else None,
                       "bench2_ret": round(br2, 2) if br2 is not None else None,
                       "abnormal_ret": round(ar, 2) if ar is not None else None,
                       "short_pnl": round(-ar / 100 * 10000, 2) if ar is not None else None}
        ph = fwd.get(primary_horizon)
        cases.append({"month": m, "ticker": ticker, "trigger": trig,
                      "entry": round(ep, 2), "reports": mc[m], "z": round(z, 2),
                      "fwd": fwd, "primary": ph,
                      "profitable": bool(ph and ph.get("short_pnl") is not None
                                         and ph["short_pnl"] > 0)})

    evaluable = [c for c in cases if c["primary"] and c["primary"].get("short_pnl") is not None]
    tot = len(evaluable)
    prof = sum(1 for c in evaluable if c["profitable"])
    tp = sum(c["primary"]["short_pnl"] for c in evaluable)
    mean_car = (sum(c["primary"]["abnormal_ret"] for c in evaluable) / tot) if tot else 0.0
    hr = prof / tot * 100 if tot else 0.0
    grade = "STRONG" if (hr >= 60 and tot >= 3) else "MODERATE" if hr >= 45 else "WEAK"
    if tot < 3:
        grade = "INSUFFICIENT" if tot == 0 else grade + " (small n)"

    expl = {}
    for hn, _ in HORIZONS:
        rows = [c["fwd"][hn] for c in cases if hn in c["fwd"]
                and c["fwd"][hn].get("short_pnl") is not None]
        if rows:
            w = sum(1 for r in rows if r["short_pnl"] > 0)
            expl[hn] = {"n": len(rows), "hit_rate": round(w / len(rows) * 100, 1),
                        "total_pnl": round(sum(r["short_pnl"] for r in rows), 2),
                        "mean_car": round(sum(r["abnormal_ret"] for r in rows) / len(rows), 2)}

    return {"status": "ok", "case_studies": cases, "primary_horizon": primary_horizon,
            "exploratory": expl,
            "summary": {"total": tot, "profitable": prof, "hit_rate": round(hr, 1),
                        "total_pnl": round(tp, 2), "mean_car": round(mean_car, 2),
                        "grade": grade,
                        "message": (f"{grade}: {hr:.0f}% hit rate over {tot} signals "
                                    f"at the pre-specified {primary_horizon} horizon; "
                                    f"mean CAR {mean_car:+.1f}%, short P&L "
                                    f"${tp:+,.0f}/$10K vs sector benchmark.")}}
