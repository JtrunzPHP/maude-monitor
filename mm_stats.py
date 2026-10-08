"""MAUDE Monitor V4 — statistics core.

Methodology changes vs V3:
- Trend-adjusted z is the primary anomaly score. Raw-count z on a device
  with a growing installed base flags growth, not deterioration; the
  trend-adjusted score measures the residual vs the device's own trailing
  OLS trend. The raw trailing z is still computed and shown.
- Rolling baselines exclude the current observation (no self-contamination)
  and exclude batch-reporting months.
- Benjamini-Hochberg FDR correction across the full signal x lag grid of
  correlation tests; "significant" now means q < FDR_Q, not raw p < 0.05
  across ~40 uncorrected tests.
"""

import math

from mm_config import Z_WINDOW, TREND_WINDOW


# ----------------------------------------------------------------------
# Spearman correlation with p-value (regularized incomplete beta for the
# t-distribution CDF; no scipy dependency)
# ----------------------------------------------------------------------
def _betacf(a, b, x):
    eps = 1e-12
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = max(1.0 - qab * x / qap, eps)
    d = 1.0 / d
    h = d
    for m in range(1, 201):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = max(1.0 + aa * d, eps)
        c = max(1.0 + aa / c, eps)
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = max(1.0 + aa * d, eps)
        c = max(1.0 + aa / c, eps)
        d = 1.0 / d
        h *= d * c
        if abs(d * c - 1.0) < eps:
            break
    return h


def _gammaln(z):
    if z <= 0:
        return 0.0
    coef = [76.18009172947146, -86.50532032941677, 24.01409824083091,
            -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5]
    y = z
    tmp = z + 5.5
    tmp -= (z + 0.5) * math.log(tmp)
    ser = 1.000000000190015
    for c in coef:
        y += 1
        ser += c / y
    return -tmp + math.log(2.5066282746310005 * ser / z)


def _ibeta(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lb = _gammaln(a) + _gammaln(b) - _gammaln(a + b)
    fr = math.exp(math.log(max(x, 1e-300)) * a + math.log(max(1.0 - x, 1e-300)) * b - lb)
    if x < (a + 1.0) / (a + b + 2.0):
        return fr * _betacf(a, b, x) / a
    return 1.0 - fr * _betacf(b, a, 1.0 - x) / b


def _rank(arr):
    indexed = sorted(range(len(arr)), key=lambda i: arr[i])
    ranks = [0.0] * len(arr)
    i = 0
    while i < len(arr):
        j = i
        while j < len(arr) - 1 and arr[indexed[j]] == arr[indexed[j + 1]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[indexed[k]] = avg
        i = j + 1
    return ranks


def spearman(x, y):
    """Returns (rho, two-sided p). n < 5 returns (0, 1)."""
    n = len(x)
    if n < 5 or n != len(y):
        return 0.0, 1.0
    rx, ry = _rank(x), _rank(y)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if dx == 0 or dy == 0:
        return 0.0, 1.0
    rho = max(-1.0, min(1.0, num / (dx * dy)))
    if abs(rho) >= 0.9999:
        return round(rho, 4), 0.0001
    df = n - 2
    t = rho * math.sqrt(df / (1.0 - rho * rho))
    p = _ibeta(df / 2.0, 0.5, df / (df + t * t))
    return round(rho, 4), round(max(0.0001, min(1.0, p)), 4)


def benjamini_hochberg(tests):
    """tests: list of dicts each containing key 'p'. Adds 'q' in place and
    returns the list. Standard step-up BH with monotonicity enforcement."""
    m = len(tests)
    if m == 0:
        return tests
    order = sorted(range(m), key=lambda i: tests[i]["p"])
    qs = [0.0] * m
    prev = 1.0
    for rank_pos in range(m - 1, -1, -1):
        i = order[rank_pos]
        q = tests[i]["p"] * m / (rank_pos + 1)
        prev = min(prev, q)
        qs[i] = round(min(1.0, prev), 4)
    for i, t in enumerate(tests):
        t["q"] = qs[i]
    return tests


# ----------------------------------------------------------------------
# Anomaly scores
# ----------------------------------------------------------------------
def trailing_z(values, exclude_idx=None, window=Z_WINDOW):
    """Rolling z per point using the trailing `window` observations BEFORE
    each point (self excluded). exclude_idx: set of indices (batch months)
    left out of baselines."""
    exclude_idx = exclude_idx or set()
    out = []
    for i in range(len(values)):
        base = [values[j] for j in range(max(0, i - window), i) if j not in exclude_idx]
        if len(base) < 6:
            out.append(0.0)
            continue
        mu = sum(base) / len(base)
        sd = math.sqrt(sum((v - mu) ** 2 for v in base) / len(base))
        sd = max(sd, math.sqrt(max(mu, 1.0)))   # Poisson floor for count data
        out.append(round((values[i] - mu) / sd, 2))
    return out


def trend_adjusted_z(values, exclude_idx=None, window=TREND_WINDOW):
    """z of the residual vs a trailing OLS trend fit on the prior `window`
    observations. Separates deterioration from installed-base growth."""
    exclude_idx = exclude_idx or set()
    out = []
    for i in range(len(values)):
        pts = [(j, values[j]) for j in range(max(0, i - window), i) if j not in exclude_idx]
        if len(pts) < 8:
            out.append(0.0)
            continue
        n = len(pts)
        xm = sum(p[0] for p in pts) / n
        ym = sum(p[1] for p in pts) / n
        den = sum((p[0] - xm) ** 2 for p in pts)
        slope = sum((p[0] - xm) * (p[1] - ym) for p in pts) / den if den > 0 else 0.0
        resid = [p[1] - (ym + slope * (p[0] - xm)) for p in pts]
        sd = math.sqrt(sum(r * r for r in resid) / n)
        pred = ym + slope * (i - xm)
        sd = max(sd, math.sqrt(max(abs(pred), 1.0)))   # Poisson floor for count data
        out.append(round((values[i] - pred) / sd, 2))
    return out


def detect_batch(recv, evnt):
    """Months where date_received volume far exceeds date_of_event volume:
    summary / retrospective batch filings, not fresh field events."""
    batch = {}
    for m in recv:
        rc = recv.get(m, 0)
        ec = evnt.get(m, 0)
        if ec > 0 and rc > 2.5 * ec:
            batch[m] = "batch"
        elif ec > 0 and rc > 1.8 * ec and rc > 100:
            batch[m] = "mild_batch"
        else:
            batch[m] = None
    return batch


# ----------------------------------------------------------------------
# Device stats bundle
# ----------------------------------------------------------------------
def build_device_stats(recv, evnt, sev, revenue, installed_base, batch,
                       provisional_month=None):
    if not recv:
        return None
    months = sorted(recv.keys())
    vals = [recv[m] for m in months]
    n = len(vals)
    if n < 3:
        return None

    batch_idx = {i for i, m in enumerate(months) if batch.get(m)}
    z_raw = trailing_z(vals, batch_idx)
    z_adj = trend_adjusted_z(vals, batch_idx)

    clean = [vals[i] for i in range(n) if i not in batch_idx] or vals
    mean_val = sum(clean) / len(clean)
    std_val = math.sqrt(sum((v - mean_val) ** 2 for v in clean) / len(clean)) or 1.0

    lm, lv = months[-1], vals[-1]

    recent = vals[-6:] if n >= 6 else vals
    nr = len(recent)
    if nr >= 3:
        xm = (nr - 1) / 2.0
        ym = sum(recent) / nr
        den = sum((i - xm) ** 2 for i in range(nr))
        slope = sum((i - xm) * (recent[i] - ym) for i in range(nr)) / den if den > 0 else 0.0
    else:
        slope = 0.0

    l3 = months[-3:]
    d3 = sum(sev.get("death", {}).get(m, 0) for m in l3)
    i3 = sum(sev.get("injury", {}).get(m, 0) for m in l3)
    m3 = sum(sev.get("malfunction", {}).get(m, 0) for m in l3)

    def q_of(m):
        yr, mo = m.split("-")
        return f"{yr}-Q{(int(mo) - 1) // 3 + 1}"

    rate_m = []
    rate_10k = []
    for i, m in enumerate(months):
        q = q_of(m)
        qr = revenue.get(q)
        rate_m.append(round(vals[i] / (qr / 3) * 1e6, 1) if qr else None)
        ib = installed_base.get(q)
        rate_10k.append(round(vals[i] / ib * 10000, 2) if ib else None)

    ma6 = []
    for i in range(n):
        w = vals[max(0, i - 5):i + 1]
        ma6.append(round(sum(w) / len(w), 1))

    return {
        "months": months, "values": vals,
        "event_values": [evnt.get(m, 0) for m in months],
        "z_raw": z_raw, "z_adj": z_adj,
        "z_score": z_raw[-1], "z_score_adj": z_adj[-1],
        "mean": mean_val, "std": std_val,
        "latest_month": lm, "latest_value": lv,
        "provisional": lm == provisional_month,
        "slope_6mo": round(slope, 2),
        "deaths_3mo": d3, "injuries_3mo": i3, "malfunctions_3mo": m3,
        "rate_per_m": rate_m[-1], "rate_per_10k": rate_10k[-1],
        "rate_m_series": rate_m, "rate_10k_series": rate_10k,
        "ma6": ma6,
        "sigma1_lo": round(max(0, mean_val - std_val), 1),
        "sigma1_hi": round(mean_val + std_val, 1),
        "sigma2_lo": round(max(0, mean_val - 2 * std_val), 1),
        "sigma2_hi": round(mean_val + 2 * std_val, 1),
        "batch_months": [m for m in months if batch.get(m)],
    }


def severity_weighted(sev, months):
    if not months or not sev:
        return {"status": "insufficient_data"}
    scores = {m: sev.get("death", {}).get(m, 0) * 100
                 + sev.get("injury", {}).get(m, 0) * 10
                 + sev.get("malfunction", {}).get(m, 0) for m in months}
    vals = [scores[m] for m in months]
    if len(vals) < 3:
        return {"status": "insufficient_data"}
    latest = vals[-1]
    mu = sum(vals) / len(vals)
    sd = math.sqrt(sum((v - mu) ** 2 for v in vals) / len(vals)) or 1.0
    z = (latest - mu) / sd
    r3 = sum(vals[-3:])
    p3 = sum(vals[-6:-3]) if len(vals) >= 6 else 0
    chg = (r3 - p3) / p3 * 100 if p3 > 0 else 0.0
    lev = "CRITICAL" if z >= 2 else "ELEVATED" if z >= 1 else "NORMAL"
    return {"status": "ok", "scores": scores, "latest": latest, "z": round(z, 2),
            "recent_3mo": r3, "change_pct": round(chg, 1), "level": lev,
            "message": f"Severity score {latest:,.0f} (z={z:+.2f}); "
                       f"3mo change {chg:+.1f}%. {lev}."}


def _tier(value, tiers):
    """tiers: list of (threshold, points, label) descending; first match wins."""
    for thr, pts, label in tiers:
        if value >= thr:
            return pts, label
    return 0, "below the lowest threshold"


def r_score_detail(stats):
    """Composite 0-100 risk score with a full point breakdown. Driven by the
    trend-adjusted z so installed-base growth alone no longer scores.
    Each factor contributes 0-20 points; five factors, capped at 100."""
    if not stats:
        return None
    comps = []

    z = abs(stats["z_score_adj"])
    pts, lab = _tier(z, [(3, 20, "|z| >= 3"), (2, 15, "|z| >= 2"),
                         (1.5, 10, "|z| >= 1.5"), (1, 5, "|z| >= 1")])
    comps.append({"factor": "Trend-adjusted z-score (latest month vs own trend)",
                  "value": f"{stats['z_score_adj']:+.2f}", "points": pts, "rule": lab,
                  "scale": "3+ = 20, 2+ = 15, 1.5+ = 10, 1+ = 5"})

    sl = stats["slope_6mo"]
    pts, lab = _tier(sl, [(100.0001, 20, "slope > 100/mo"), (50.0001, 15, "slope > 50/mo"),
                          (20.0001, 10, "slope > 20/mo"), (0.0001, 5, "slope > 0")])
    comps.append({"factor": "6-month trend slope (reports per month, per month)",
                  "value": f"{sl:+.1f}", "points": pts, "rule": lab,
                  "scale": ">100 = 20, >50 = 15, >20 = 10, >0 = 5"})

    d = stats["deaths_3mo"]
    pts, lab = _tier(d, [(5, 20, "5+ deaths"), (2, 15, "2-4 deaths"), (1, 10, "1 death")])
    comps.append({"factor": "Deaths reported, last 3 months", "value": f"{d}",
                  "points": pts, "rule": lab, "scale": "5+ = 20, 2-4 = 15, 1 = 10"})

    inj = stats["injuries_3mo"]
    pts, lab = _tier(inj, [(50, 20, "50+ injuries"), (20, 15, "20-49"), (5, 10, "5-19"),
                           (1, 5, "1-4")])
    comps.append({"factor": "Injuries reported, last 3 months", "value": f"{inj:,}",
                  "points": pts, "rule": lab, "scale": "50+ = 20, 20+ = 15, 5+ = 10, 1+ = 5"})

    rpm = stats.get("rate_per_m")
    if rpm:
        pts, lab = _tier(rpm, [(500.0001, 20, "> 500 per $M"), (200.0001, 15, "> 200 per $M"),
                               (100.0001, 10, "> 100 per $M"), (50.0001, 5, "> 50 per $M")])
        comps.append({"factor": "Reports per $M quarterly revenue (unverified inputs)",
                      "value": f"{rpm:,.1f}", "points": pts, "rule": lab,
                      "scale": ">500 = 20, >200 = 15, >100 = 10, >50 = 5"})
    else:
        comps.append({"factor": "Reports per $M quarterly revenue", "value": "n/a",
                      "points": 0, "rule": "no revenue figure for this month's quarter",
                      "scale": ">500 = 20, >200 = 15, >100 = 10, >50 = 5"})

    total = min(100, sum(c["points"] for c in comps))
    return {"score": total, "components": comps,
            "thresholds": "CRITICAL 70+, ELEVATED 50-69, WATCH 30-49, NORMAL below 30"}


def r_score(stats):
    d = r_score_detail(stats)
    return None if d is None else d["score"]


def signal_from_r(rscore):
    if rscore is None:
        return "NORMAL"
    if rscore >= 70:
        return "CRITICAL"
    if rscore >= 50:
        return "ELEVATED"
    if rscore >= 30:
        return "WATCH"
    return "NORMAL"
