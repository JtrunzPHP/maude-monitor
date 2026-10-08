"""MAUDE Monitor V4 — market data.

The V3 hardcoded STOCK_MONTHLY fallback table is REMOVED: those prices
carried no source and cannot be verified, so correlation and CAR results
built on them were untrustworthy. V4 uses live sources only:

  1. yfinance (primary)
  2. Stooq free CSV endpoint (fallback; real exchange data, no key)

If both fail for a ticker, the market-linked modules for that ticker
report "no market data" rather than silently computing on fabricated
prices. The dashboard header shows the source used per run.
"""

import csv
import io
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request


def _yf_monthly(ticker, period="4y"):
    try:
        import yfinance as yf
    except ImportError:
        return None
    try:
        data = yf.download(ticker, period=period, interval="1mo",
                           progress=False, auto_adjust=True)
        if data is None or len(data) == 0:
            return None
        monthly = {}
        for idx, row in data.iterrows():
            m = idx.strftime("%Y-%m")
            cv = row.get("Close") if "Close" in data.columns else row.iloc[-1]
            if hasattr(cv, "item"):
                cv = cv.item()
            monthly[m] = round(float(cv), 2)
        return monthly or None
    except Exception as e:
        print(f"    yfinance {ticker}: {str(e)[:70]}")
        return None


def _stooq_monthly(ticker, years=4):
    """Stooq daily CSV -> month-end closes. US tickers use the .us suffix;
    ETFs like IHI/SPY resolve the same way."""
    sym = f"{ticker.lower()}.us"
    d1 = (datetime.now() - timedelta(days=365 * years)).strftime("%Y%m%d")
    d2 = datetime.now().strftime("%Y%m%d")
    url = f"https://stooq.com/q/d/l/?s={sym}&d1={d1}&d2={d2}&i=d"
    try:
        req = Request(url, headers={"User-Agent": "MAUDE-Monitor/4.0"})
        with urlopen(req, timeout=20) as resp:
            text = resp.read().decode()
        if not text or text.strip().lower().startswith("no data"):
            return None
        monthly = {}
        for row in csv.DictReader(io.StringIO(text)):
            date = row.get("Date", "")
            close = row.get("Close", "")
            if len(date) >= 7 and close:
                monthly[date[:7]] = round(float(close), 2)   # last row per month wins
        return monthly or None
    except Exception as e:
        print(f"    stooq {ticker}: {str(e)[:70]}")
        return None


def fetch_monthly_prices(tickers):
    """Returns (prices_by_ticker, source_notes)."""
    prices, notes = {}, {}
    for tk in tickers:
        series = _yf_monthly(tk)
        src = "yfinance"
        if not series:
            series = _stooq_monthly(tk)
            src = "stooq"
        if series:
            prices[tk] = series
            notes[tk] = src
            print(f"    {tk}: {len(series)} months via {src}")
        else:
            notes[tk] = "unavailable"
            print(f"    {tk}: NO MARKET DATA (yfinance and stooq both failed)")
        time.sleep(0.4)
    return prices, notes


def fetch_benchmarks(primary="IHI", secondary="SPY"):
    bench, notes = fetch_monthly_prices([primary, secondary])
    return {"primary": {"ticker": primary, "series": bench.get(primary, {})},
            "secondary": {"ticker": secondary, "series": bench.get(secondary, {})},
            "notes": notes}
