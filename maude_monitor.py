#!/usr/bin/env python3
"""MAUDE Monitor V4 — CLI entry point.

Usage:
  python maude_monitor.py                     # standard run (since 2023-01)
  python maude_monitor.py --mode quick        # short lookback sanity run
  python maude_monitor.py --discover          # print what each device query
                                              # matches (brand names, product
                                              # codes) and exit

Environment:
  OPENFDA_API_KEY     strongly recommended (rate limits)
  EDGAR_CONTACT       email for the SEC User-Agent header
  SLACK_WEBHOOK_URL   optional signal-transition alerts
  SMTP_HOST/PORT/USER/PASS, ALERT_TO, ALERT_FROM   optional email alerts
"""

import argparse
import sys


def main():
    ap = argparse.ArgumentParser(description="MAUDE Monitor V4")
    ap.add_argument("--mode", choices=["standard", "quick"], default="standard")
    ap.add_argument("--discover", action="store_true",
                    help="Print top brand names / product codes per device query")
    args = ap.parse_args()

    if args.discover:
        import mm_fda as fda
        from mm_config import DEVICES, START_DATE_BACKFILL
        for dev in DEVICES:
            fda.discover(dev, START_DATE_BACKFILL)
        return 0

    from mm_pipeline import run_pipeline
    from mm_dashboard import write_dashboard

    all_res, meta, stock = run_pipeline(mode=args.mode)
    write_dashboard(all_res, meta, stock)
    return 0


if __name__ == "__main__":
    sys.exit(main())
