"""MAUDE Monitor V4 — configuration.

Device registry, manual event annotations, and runtime settings.

V4 query model: each device defines brand-name PHRASES (matched as quoted
phrases against device.brand_name) and optional manufacturer phrases
(device.manufacturer_d_name). Quoted-phrase matching is far more precise
than the unquoted token search V3 used; expect counts to shift vs V3 —
the new counts are the more accurate ones. Run with --discover to print
the top exact brand names and product codes each query actually matches,
then tighten the phrases or pin product_codes per device if desired.
"""

import os

# ----------------------------------------------------------------------
# Runtime settings
# ----------------------------------------------------------------------
OPENFDA_API_KEY = os.environ.get("OPENFDA_API_KEY", "")
EDGAR_CONTACT = os.environ.get("EDGAR_CONTACT", "research@example.com")

START_DATE_BACKFILL = "20230101"
START_DATE_QUICK = "20250901"

API_SLEEP = 0.35            # seconds between openFDA calls
API_RETRIES = 3
API_TIMEOUT = 30

MAX_NARRATIVES = 400        # MDR narratives to pull per device for failure modes / PRR
NARRATIVE_PAGE = 100

Z_WINDOW = 12               # trailing months for rolling z baseline
TREND_WINDOW = 24           # trailing months for trend-adjusted z
Z_TRIGGER = 1.5             # backtest entry threshold (trend-adjusted z)
MOM_TRIGGER = 30.0          # alt entry: MoM % surge (with z >= 1.0)
SIGNAL_COOLDOWN = 3         # months between backtest signals
PRIMARY_HORIZON = "3mo"     # pre-specified backtest exit horizon (fixes V3 lookahead)
FDR_Q = 0.10                # Benjamini-Hochberg significance level for correlations

BENCHMARK_PRIMARY = "IHI"   # iShares U.S. Medical Devices ETF — sector-adjusted CAR
BENCHMARK_SECONDARY = "SPY"

OUTPUT_HTML = os.path.join("docs", "index.html")
SNAPSHOT_DIR = os.path.join("data", "snapshots")
STATE_FILE = os.path.join("data", "signal_state.json")
PAYLOAD_FILE = os.path.join("data", "latest_payload.json")

# Alerting (all optional; silently skipped when unset)
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587") or 587)
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
ALERT_TO = [a.strip() for a in os.environ.get("ALERT_TO", "").split(",") if a.strip()]
ALERT_FROM = os.environ.get("ALERT_FROM", SMTP_USER)

# ----------------------------------------------------------------------
# Device registry
# ----------------------------------------------------------------------
# phrases            -> OR'd quoted phrases against device.brand_name
# manufacturer       -> OR'd quoted phrases against device.manufacturer_d_name
#                       (used when brand names alone under-capture a company row)
# product_codes      -> optional list of FDA product codes; when set, a
#                       product-code count series is pulled as a cross-check
#                       and shown on the dashboard. Left empty by default:
#                       populate from --discover output, not from guesses.
# require_manufacturer -> optional; when set, the brand match is ANDed with
#                       device.manufacturer_d_name phrases. Use it when a
#                       brand word is generic (e.g. "volt") so other makers'
#                       devices with the same word do not leak in.
# is_combined        -> company-level row (drives peer/cross-company modules)
DEVICES = [
    {"id": "DXCM_G7", "name": "Dexcom G7", "phrases": ["dexcom g7"], "manufacturer": [],
     "ticker": "DXCM", "rev_key": "DXCM", "company": "Dexcom", "is_combined": False, "product_codes": []},
    {"id": "DXCM_G6", "name": "Dexcom G6", "phrases": ["dexcom g6"], "manufacturer": [],
     "ticker": "DXCM", "rev_key": "DXCM", "company": "Dexcom", "is_combined": False, "product_codes": []},
    {"id": "DXCM_ALL", "name": "Dexcom (All CGM)", "phrases": ["dexcom"], "manufacturer": ["dexcom"],
     "ticker": "DXCM", "rev_key": "DXCM", "company": "Dexcom", "is_combined": True, "product_codes": []},
    {"id": "PODD_OP5", "name": "Omnipod 5", "phrases": ["omnipod 5"], "manufacturer": [],
     "ticker": "PODD", "rev_key": "PODD", "company": "Insulet", "is_combined": False, "product_codes": []},
    {"id": "PODD_DASH", "name": "Omnipod DASH", "phrases": ["omnipod dash"], "manufacturer": [],
     "ticker": "PODD", "rev_key": "PODD", "company": "Insulet", "is_combined": False, "product_codes": []},
    {"id": "PODD_ALL", "name": "Insulet (All Omnipod)", "phrases": ["omnipod"], "manufacturer": [],
     "ticker": "PODD", "rev_key": "PODD", "company": "Insulet", "is_combined": True, "product_codes": []},
    {"id": "TNDM_TSLIM", "name": "t:slim X2", "phrases": ["t:slim"], "manufacturer": [],
     "ticker": "TNDM", "rev_key": "TNDM", "company": "Tandem", "is_combined": False, "product_codes": []},
    {"id": "TNDM_MOBI", "name": "Tandem Mobi", "phrases": ["tandem mobi"], "manufacturer": [],
     "ticker": "TNDM", "rev_key": "TNDM", "company": "Tandem", "is_combined": False, "product_codes": []},
    {"id": "TNDM_ALL", "name": "Tandem (All Pumps)", "phrases": ["t:slim", "tandem mobi"],
     "manufacturer": ["tandem diabetes"],
     "ticker": "TNDM", "rev_key": "TNDM", "company": "Tandem", "is_combined": True, "product_codes": []},
    {"id": "ABT_LIBRE", "name": "Abbott FreeStyle Libre", "phrases": ["freestyle libre"], "manufacturer": [],
     "ticker": "ABT", "rev_key": "ABT_LIBRE", "company": "Abbott", "is_combined": False, "product_codes": []},
    {"id": "ABT_ALL", "name": "Abbott (All Libre)", "phrases": ["libre"], "manufacturer": [],
     "ticker": "ABT", "rev_key": "ABT_LIBRE", "company": "Abbott", "is_combined": True, "product_codes": []},
    {"id": "BBNX_ILET", "name": "Beta Bionics iLet", "phrases": ["ilet"], "manufacturer": ["beta bionics"],
     "ticker": "BBNX", "rev_key": "BBNX", "company": "Beta Bionics", "is_combined": True, "product_codes": []},
    {"id": "MDT_780G", "name": "Medtronic 780G", "phrases": ["780g"], "manufacturer": [],
     "ticker": "MDT", "rev_key": "MDT_DM", "company": "Medtronic", "is_combined": False, "product_codes": []},
    {"id": "MDT_ALL", "name": "Medtronic (All Pumps)", "phrases": ["minimed"], "manufacturer": [],
     "ticker": "MDT", "rev_key": "MDT_DM", "company": "Medtronic", "is_combined": True, "product_codes": []},
    {"id": "SQEL_TWIIST", "name": "Sequel twiist", "phrases": ["twiist"], "manufacturer": [],
     "ticker": "SQEL", "rev_key": "SQEL", "company": "Sequel", "is_combined": True, "product_codes": []},
    {"id": "PRCT_AQUABEAM", "name": "Procept AquaBeam", "phrases": ["aquabeam", "aquablation"], "manufacturer": [],
     "ticker": "PRCT", "rev_key": "PRCT", "company": "Procept", "is_combined": True, "product_codes": []},
    {"id": "CVRX_BAROSTIM", "name": "CVRx Barostim", "phrases": ["barostim"], "manufacturer": [],
     "ticker": "CVRX", "rev_key": "CVRX", "company": "CVRx", "is_combined": True, "product_codes": []},
    # Abbott Volt PFA (pulsed field ablation). FDA approved 2025-12-22; US
    # MAUDE history starts 2026, so stock-linked modules stay thin until the
    # series reaches 14+ months.
    {"id": "ABT_VOLT", "name": "Abbott Volt PFA", "phrases": ["volt pfa", "volt"],
     "manufacturer": [], "require_manufacturer": ["abbott"],
     "ticker": "ABT", "rev_key": "ABT_VOLT", "company": "Abbott", "is_combined": False,
     "product_codes": []},
]

COMPANIES = list(dict.fromkeys(d["company"] for d in DEVICES))

PRIVATE_TICKERS = {"SQEL"}

# SEC CIK map — carried from V3.4b unchanged.
CIK_MAP = {
    "DXCM": "0001093557", "PODD": "0001145197", "TNDM": "0001438133",
    "ABT": "0000001800", "MDT": "0000064670", "BBNX": "0001828723",
    "PRCT": "0001588978", "CVRX": "0001235912",
}

CT_SPONSOR_MAP = {
    "DXCM": "Dexcom", "PODD": "Insulet", "TNDM": "Tandem Diabetes",
    "ABT": "Abbott", "MDT": "Medtronic", "BBNX": "Beta Bionics",
    "SQEL": "Sequel AG", "PRCT": "PROCEPT BioRobotics", "CVRX": "CVRx",
}

PAYER_NOTES = {
    "DXCM": "Broad commercial + Medicare CGM", "PODD": "Broad commercial + Medicare pump",
    "TNDM": "Broad commercial + Medicare pump", "ABT": "Broad commercial + Medicare CGM",
    "MDT": "Broad commercial + Medicare pump", "BBNX": "Limited (new entrant)",
    "SQEL": "Pre-market", "PRCT": "Broad commercial + Medicare (BPH / aquablation)",
    "CVRX": "DRG 276 + APC 1580 (heart failure)",
}

# ----------------------------------------------------------------------
# Manual event annotations (chart markers).
# Recall events are ALSO auto-pulled from the FDA recall API each run and
# merged with these; keep this list for warning letters, launches, and
# anything the APIs do not expose. Carried from V3.4b.
# ----------------------------------------------------------------------
PRODUCT_EVENTS = {
    "DXCM_G7": [{"date": "2024-03", "label": "FDA Warning Letter", "type": "regulatory"},
                {"date": "2025-04", "label": "G7 15-Day Cleared", "type": "launch"},
                {"date": "2025-05", "label": "Class I Recall (Receiver)", "type": "recall"}],
    "DXCM_G6": [{"date": "2024-03", "label": "FDA Warning Letter", "type": "regulatory"},
                {"date": "2025-01", "label": "G6 Receiver SW Recall", "type": "recall"},
                {"date": "2025-05", "label": "Class I Recall (Receiver)", "type": "recall"}],
    "DXCM_ALL": [{"date": "2024-03", "label": "FDA Warning Letter", "type": "regulatory"},
                 {"date": "2025-05", "label": "Class I Recall", "type": "recall"}],
    "PODD_OP5": [{"date": "2024-09", "label": "OP5 Gen2 Launch", "type": "launch"}],
    "PODD_DASH": [], 
    "PODD_ALL": [{"date": "2024-09", "label": "OP5 Gen2 Launch", "type": "launch"}],
    "TNDM_TSLIM": [{"date": "2024-06", "label": "Mobi Launch", "type": "launch"}],
    "TNDM_MOBI": [{"date": "2024-06", "label": "Mobi Cleared", "type": "launch"}],
    "TNDM_ALL": [{"date": "2024-06", "label": "Mobi Launch", "type": "launch"}],
    "ABT_LIBRE": [], "ABT_ALL": [],
    "BBNX_ILET": [{"date": "2025-02", "label": "IPO", "type": "launch"}],
    "MDT_780G": [], "MDT_ALL": [], "SQEL_TWIIST": [],
    "PRCT_AQUABEAM": [{"date": "2023-06", "label": "UHC Coverage", "type": "launch"},
                      {"date": "2024-10", "label": "Hydros System Launch", "type": "launch"}],
    "CVRX_BAROSTIM": [{"date": "2024-10", "label": "DRG 276 Reassignment", "type": "launch"},
                      {"date": "2026-01", "label": "Category I CPT Codes", "type": "launch"}],
    "ABT_VOLT": [{"date": "2025-12", "label": "FDA Approval (Volt PFA)", "type": "launch"}],
}
