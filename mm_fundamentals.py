"""MAUDE Monitor V4 — company fundamentals (revenue, installed base).

DATA INTEGRITY RULES
--------------------
Every series below carries a `verified` flag and a `source` string. The
numbers themselves were carried over from V3.4b, where they were hardcoded
without citations, so every series ships as verified=False until each one
is checked against the company's filings or investor materials. The
dashboard surfaces this: any metric derived from an unverified series
(Rate/$M, Rate/10K, the two rate correlation signals) is rendered with an
"unverified inputs" flag, and the header shows the share of fundamentals
verified.

To verify a series: confirm the quarterly figures against the 10-Q/10-K or
investor deck, replace any that are wrong, set verified=True, and put the
actual citation in `source` (e.g. "DXCM 10-K FY2025 p.48; Q1'26 8-K").
Do not set verified=True without a citation.
"""

UNVERIFIED = "Carried from V3.4b hardcoded tables; NOT verified against filings"

REVENUE_LAST_UPDATED = "2026-03-26"

# Quarterly revenue, $M (product/segment level where noted in V3)
QUARTERLY_REVENUE = {
    "DXCM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 921, "2023-Q2": 871.3, "2023-Q3": 975, "2023-Q4": 1010,
        "2024-Q1": 921, "2024-Q2": 1004, "2024-Q3": 994.2, "2024-Q4": 1115,
        "2025-Q1": 1036, "2025-Q2": 1092, "2025-Q3": 1174, "2025-Q4": 1260, "2026-Q1": 1270}},
    "PODD": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 412.5, "2023-Q2": 432.1, "2023-Q3": 476, "2023-Q4": 521.5,
        "2024-Q1": 481.5, "2024-Q2": 530.4, "2024-Q3": 543.9, "2024-Q4": 597.7,
        "2025-Q1": 555, "2025-Q2": 655, "2025-Q3": 706.3, "2025-Q4": 783.8, "2026-Q1": 810}},
    "TNDM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 171.1, "2023-Q2": 185.5, "2023-Q3": 194.1, "2023-Q4": 196.3,
        "2024-Q1": 193.5, "2024-Q2": 214.6, "2024-Q3": 249.5, "2024-Q4": 282.6,
        "2025-Q1": 226, "2025-Q2": 207.9, "2025-Q3": 290.4, "2025-Q4": 290.4, "2026-Q1": 260}},
    "ABT_LIBRE": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 1100, "2023-Q2": 1200, "2023-Q3": 1400, "2023-Q4": 1400,
        "2024-Q1": 1500, "2024-Q2": 1600, "2024-Q3": 1700, "2024-Q4": 1800,
        "2025-Q1": 1700, "2025-Q2": 1850, "2025-Q3": 2000, "2025-Q4": 2100, "2026-Q1": 2200}},
    "BBNX": {"verified": False, "source": UNVERIFIED, "series": {
        "2025-Q1": 20, "2025-Q2": 24, "2025-Q3": 24.2, "2025-Q4": 32.1, "2026-Q1": 32}},
    "MDT_DM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 570, "2023-Q2": 580, "2023-Q3": 600, "2023-Q4": 620,
        "2024-Q1": 620, "2024-Q2": 647, "2024-Q3": 691, "2024-Q4": 694,
        "2025-Q1": 728, "2025-Q2": 750, "2025-Q3": 770, "2025-Q4": 780, "2026-Q1": 800}},
    "SQEL": {"verified": False, "source": "Private; no public revenue", "series": {}},
    "PRCT": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 24.4, "2023-Q2": 34.0, "2023-Q3": 34.2, "2023-Q4": 43.6,
        "2024-Q1": 44.5, "2024-Q2": 53.0, "2024-Q3": 58.8, "2024-Q4": 68.2,
        "2025-Q1": 75, "2025-Q2": 78, "2025-Q3": 82, "2025-Q4": 85, "2026-Q1": 90}},
    "CVRX": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 8.5, "2023-Q2": 9.5, "2023-Q3": 10.0, "2023-Q4": 11.3,
        "2024-Q1": 10.8, "2024-Q2": 12.4, "2024-Q3": 13.0, "2024-Q4": 15.3,
        "2025-Q1": 12.3, "2025-Q2": 14.0, "2025-Q3": 14.4, "2025-Q4": 16.0, "2026-Q1": 15}},
}

# Installed base, thousands. V3 comments noted PRCT (systems, not patients)
# and CVRX (cumulative implants) were explicitly estimates.
INSTALLED_BASE_K = {
    "DXCM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 2000, "2023-Q2": 2100, "2023-Q3": 2200, "2023-Q4": 2350,
        "2024-Q1": 2500, "2024-Q2": 2600, "2024-Q3": 2750, "2024-Q4": 2900,
        "2025-Q1": 3000, "2025-Q2": 3100, "2025-Q3": 3250, "2025-Q4": 3400, "2026-Q1": 3550}},
    "PODD": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 380, "2023-Q2": 400, "2023-Q3": 420, "2023-Q4": 440,
        "2024-Q1": 460, "2024-Q2": 480, "2024-Q3": 510, "2024-Q4": 540,
        "2025-Q1": 560, "2025-Q2": 590, "2025-Q3": 620, "2025-Q4": 660, "2026-Q1": 700}},
    "TNDM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 380, "2023-Q2": 390, "2023-Q3": 400, "2023-Q4": 410,
        "2024-Q1": 420, "2024-Q2": 430, "2024-Q3": 445, "2024-Q4": 460,
        "2025-Q1": 470, "2025-Q2": 480, "2025-Q3": 495, "2025-Q4": 510, "2026-Q1": 520}},
    "ABT_LIBRE": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 4500, "2023-Q2": 4700, "2023-Q3": 4900, "2023-Q4": 5100,
        "2024-Q1": 5300, "2024-Q2": 5600, "2024-Q3": 5900, "2024-Q4": 6200,
        "2025-Q1": 6400, "2025-Q2": 6700, "2025-Q3": 7000, "2025-Q4": 7300, "2026-Q1": 7600}},
    "BBNX": {"verified": False, "source": UNVERIFIED, "series": {
        "2025-Q1": 2, "2025-Q2": 4, "2025-Q3": 7, "2025-Q4": 12, "2026-Q1": 18}},
    "MDT_DM": {"verified": False, "source": UNVERIFIED, "series": {
        "2023-Q1": 800, "2023-Q2": 820, "2023-Q3": 850, "2023-Q4": 880,
        "2024-Q1": 900, "2024-Q2": 930, "2024-Q3": 960, "2024-Q4": 1000,
        "2025-Q1": 1050, "2025-Q2": 1100, "2025-Q3": 1150, "2025-Q4": 1200, "2026-Q1": 1250}},
    "SQEL": {"verified": False, "source": "Private; no public installed base", "series": {}},
    "PRCT": {"verified": False, "source": UNVERIFIED + " (V3 noted these were estimates; US systems, not patients)", "series": {
        "2023-Q1": 0.192, "2023-Q2": 0.220, "2023-Q3": 0.260, "2023-Q4": 0.310,
        "2024-Q1": 0.350, "2024-Q2": 0.400, "2024-Q3": 0.450, "2024-Q4": 0.505,
        "2025-Q1": 0.560, "2025-Q2": 0.610, "2025-Q3": 0.660, "2025-Q4": 0.720, "2026-Q1": 0.780}},
    "CVRX": {"verified": False, "source": UNVERIFIED + " (V3 noted these were estimates; cumulative US implants)", "series": {
        "2023-Q1": 1.5, "2023-Q2": 1.8, "2023-Q3": 2.1, "2023-Q4": 2.4,
        "2024-Q1": 2.8, "2024-Q2": 3.2, "2024-Q3": 3.6, "2024-Q4": 4.1,
        "2025-Q1": 4.4, "2025-Q2": 4.8, "2025-Q3": 5.2, "2025-Q4": 5.7, "2026-Q1": 6.1}},
}


def revenue_series(rev_key):
    entry = QUARTERLY_REVENUE.get(rev_key, {})
    return entry.get("series", {}), bool(entry.get("verified"))


def installed_base_series(rev_key):
    entry = INSTALLED_BASE_K.get(rev_key, {})
    return entry.get("series", {}), bool(entry.get("verified"))


def integrity_summary():
    """Share of fundamental series verified, for the dashboard header."""
    rows = []
    for name, table in (("revenue", QUARTERLY_REVENUE), ("installed_base", INSTALLED_BASE_K)):
        for key, entry in table.items():
            if not entry.get("series"):
                continue
            rows.append({"table": name, "key": key, "verified": bool(entry.get("verified")),
                         "source": entry.get("source", "")})
    total = len(rows)
    ver = sum(1 for r in rows if r["verified"])
    return {"total": total, "verified": ver,
            "pct": round(ver / total * 100) if total else 0, "rows": rows}


def revenue_staleness(now=None):
    from datetime import datetime
    try:
        lu = datetime.strptime(REVENUE_LAST_UPDATED, "%Y-%m-%d")
        days = ((now or datetime.now()) - lu).days
        if days > 120:
            return {"stale": True, "days": days, "message": f"STALE ({days}d)"}
        if days > 90:
            return {"stale": False, "days": days, "message": f"Review due ({days}d)"}
        return {"stale": False, "days": days, "message": f"Current ({days}d)"}
    except Exception:
        return {"stale": True, "days": 999, "message": "Unknown"}
