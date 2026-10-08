"""MAUDE Monitor V4 — pipeline orchestrator."""

import time
from datetime import datetime

import mm_fda as fda
import mm_market as market
import mm_external as ext
import mm_alerts as alerts
from mm_config import (DEVICES, PRIVATE_TICKERS, PRODUCT_EVENTS, API_SLEEP,
                       START_DATE_BACKFILL, START_DATE_QUICK,
                       BENCHMARK_PRIMARY, BENCHMARK_SECONDARY, OPENFDA_API_KEY)
from mm_fundamentals import (revenue_series, installed_base_series,
                             integrity_summary, revenue_staleness)
from mm_stats import (build_device_stats, detect_batch, severity_weighted,
                      r_score, signal_from_r)
from mm_signals import (correlation_suite, prr_all, recall_cascade,
                        cross_company, peer_relative, recall_probability,
                        earnings_predictor)
from mm_backtest import car_backtest


def _merge_events(manual, auto):
    """Merge manual PRODUCT_EVENTS with auto-pulled recall events, deduped
    by (month, type) with manual labels taking precedence."""
    seen = {(e["date"], e["type"]) for e in manual}
    merged = list(manual)
    for e in auto:
        if (e["date"], e["type"]) not in seen:
            merged.append(e)
            seen.add((e["date"], e["type"]))
    return sorted(merged, key=lambda e: e["date"])


def run_pipeline(mode="standard"):
    start = START_DATE_QUICK if mode == "quick" else START_DATE_BACKFILL
    run_at = datetime.now()
    print(f"MAUDE Monitor V4 — mode={mode}, start={start}, "
          f"api_key={'set' if OPENFDA_API_KEY else 'MISSING (40 req/min limit)'}")

    # ---------- market data ----------
    print("\n=== Market data ===")
    tickers = sorted({d["ticker"] for d in DEVICES if d["ticker"] not in PRIVATE_TICKERS})
    stock, stock_notes = market.fetch_monthly_prices(tickers)
    bench = market.fetch_benchmarks(BENCHMARK_PRIMARY, BENCHMARK_SECONDARY)
    bench1 = bench["primary"]["series"]
    bench2 = bench["secondary"]["series"]

    # combined rows whose ticker also has individual rows (overlapping
    # universes; excluded from the PRR comparator)
    individual_tickers = {d["ticker"] for d in DEVICES if not d["is_combined"]}
    overlap_combined = {d["id"] for d in DEVICES
                        if d["is_combined"] and d["ticker"] in individual_tickers}

    all_res = {}
    failure_modes = {}
    r_scores_company = {}
    company_xstats = {}

    for dev in DEVICES:
        did, tk = dev["id"], dev["ticker"]
        rk = dev.get("rev_key", tk)
        print(f"\n{'=' * 52}\n{dev['name']} ({tk})")

        recv_raw = fda.fetch_counts(dev, "date_received", start)
        recv, recv_meta = fda.trim_partial_month(recv_raw)
        print(f"  date_received: {len(recv)} months, total={sum(recv.values()):,}"
              + (f" (dropped partial {recv_meta['dropped_partial']})"
                 if recv_meta["dropped_partial"] else ""))
        time.sleep(API_SLEEP)

        evnt_raw = fda.fetch_counts(dev, "date_of_event", start)
        evnt = {m: v for m, v in evnt_raw.items()
                if recv_meta["provisional_month"] is None or m <= recv_meta["provisional_month"]}
        print(f"  date_of_event: {len(evnt)} months, total={sum(evnt.values()):,}")
        time.sleep(API_SLEEP)

        sev_raw = fda.fetch_severity(dev, start)
        sev = {et: {m: v for m, v in d.items()
                    if recv_meta["provisional_month"] is None
                    or m <= recv_meta["provisional_month"]}
               for et, d in sev_raw.items()}
        print(f"  severity: deaths={sum(sev['death'].values())}, "
              f"injuries={sum(sev['injury'].values())}")

        pc_counts = {}
        if dev.get("product_codes"):
            pc_counts, _ = fda.trim_partial_month(
                fda.fetch_product_code_counts(dev, "date_received", start))
            time.sleep(API_SLEEP)

        rev, rev_ok = revenue_series(rk)
        base, base_ok = installed_base_series(rk)
        batch = detect_batch(recv, evnt)
        stats = build_device_stats(recv, evnt, sev, rev, base, batch,
                                   provisional_month=recv_meta["provisional_month"])
        if stats:
            print(f"  z_raw={stats['z_score']:+.2f}  z_trend_adj={stats['z_score_adj']:+.2f}"
                  f"  latest={stats['latest_value']:,}"
                  + ("  [provisional month]" if stats["provisional"] else ""))

        # failure modes (paginated narratives) — individual devices plus
        # single-product company rows; skipped for overlapping _ALL rows
        fm = None
        if stats and did not in overlap_combined:
            print("  Pulling narratives for failure-mode classification...")
            narratives = fda.fetch_narratives(dev, start)
            fm = fda.classify_failure_modes(narratives)
            print(f"  -> {fm['total']} narratives classified")
            failure_modes[did] = fm
        time.sleep(API_SLEEP)

        recalls = fda.fetch_recalls(dev)
        time.sleep(API_SLEEP)
        events = _merge_events(PRODUCT_EVENTS.get(did, []),
                               fda.recalls_to_events(recalls))

        modules = {"failure_modes": fm, "recalls": recalls}

        if stats:
            modules["correlation"] = correlation_suite(
                recv, stock.get(tk, {}), max_lag=6, revenue=rev,
                installed_base=base, revenue_verified=rev_ok, base_verified=base_ok)
            modules["car"] = car_backtest(recv, stock.get(tk, {}), batch,
                                          bench1, bench2, tk)
            if modules["car"].get("status") == "ok":
                s2 = modules["car"]["summary"]
                print(f"  CAR: {s2['grade']} {s2['hit_rate']:.0f}% over {s2['total']} "
                      f"signals @ {modules['car']['primary_horizon']}")
            modules["severity_weighted"] = severity_weighted(sev, stats["months"])
            modules["cascade"] = recall_cascade(stats, recalls, events)
            modules["recall_prob"] = recall_probability(stats, fm)
            modules["earnings_pred"] = earnings_predictor(stats, modules["correlation"], fm)

        if dev["is_combined"]:
            modules["edgar"] = ext.edgar_summary(tk)
            modules["trials"] = ext.clinical_trials(tk)
            modules["payer"] = ext.payer_note(tk)
            if stats:
                rsc = r_score(stats)
                if rsc is not None:
                    r_scores_company[tk] = rsc
                company_xstats[tk] = {"name": dev["name"],
                                      "rate": stats.get("rate_per_10k"),
                                      "rate_verified": base_ok,
                                      "slope": stats.get("slope_6mo", 0),
                                      "z": stats.get("z_score_adj", 0)}

        rsc = r_score(stats) if stats else None
        sig = signal_from_r(rsc)
        all_res[did] = {"device": dev, "stats": stats, "r_score": rsc,
                        "signal": sig, "recv": recv, "evnt": evnt, "sev": sev,
                        "batch": batch, "events": events, "modules": modules,
                        "pc_counts": pc_counts,
                        "rev_verified": rev_ok, "base_verified": base_ok,
                        "brand_field": fda._field_cache.get(did, "device.brand_name")}
        print(f"  >>> Signal: {sig} | R={rsc}")

    # ---------- second-pass modules ----------
    print("\n=== Second-pass modules ===")
    prr_results = prr_all(failure_modes, overlap_combined)
    for did, res in all_res.items():
        res["modules"]["prr"] = prr_results.get(did)

    xc = cross_company(company_xstats)
    pr = peer_relative(r_scores_company)
    for did, res in all_res.items():
        tk = res["device"]["ticker"]
        if res["device"]["is_combined"]:
            res["modules"]["cross_company"] = xc.get(tk)
            res["modules"]["peer_relative"] = pr.get(tk)

    # ---------- diffing + alerts ----------
    prev_snap = alerts.load_previous_snapshot()
    diffs = alerts.diff_snapshot(all_res, prev_snap)
    alerts.write_snapshot(all_res)
    for did, d in diffs.items():
        all_res[did]["new_since_last_run"] = d["new_reports"]
        all_res[did]["changed_months"] = d["changed_months"]

    prev_state = alerts.load_state()
    current_signals = {did: res["signal"] for did, res in all_res.items()}
    transitions = alerts.detect_transitions(prev_state, current_signals)
    alert_result = alerts.send_alerts(transitions, diffs, all_res)
    alerts.save_state(current_signals)

    # ---------- run metadata ----------
    integ = integrity_summary()
    meta = {
        "run_at": run_at.strftime("%Y-%m-%d %H:%M"),
        "mode": mode, "start_date": start,
        "api_key_set": bool(OPENFDA_API_KEY),
        "stock_sources": stock_notes,
        "benchmark_primary": BENCHMARK_PRIMARY,
        "benchmark_secondary": BENCHMARK_SECONDARY,
        "benchmark_ok": bool(bench1),
        "fundamentals": {"verified": integ["verified"], "total": integ["total"],
                         "pct": integ["pct"]},
        "revenue_staleness": revenue_staleness(),
        "prev_run": (prev_snap or {}).get("run_at", ""),
        "transitions": transitions,
        "alerts": {"sent": alert_result.get("sent", False),
                   "channels": alert_result.get("channels", [])},
    }

    print("\n=== SUMMARY ===")
    for did, res in sorted(all_res.items(), key=lambda kv: -(kv[1]["r_score"] or 0)):
        st = res["stats"]
        if st:
            print(f"  {res['device']['name']:<30} {res['signal']:<9} "
                  f"R={res['r_score'] or 0:<3} z_adj={st['z_score_adj']:+.2f} "
                  f"new={res.get('new_since_last_run', 'n/a')}")
        else:
            print(f"  {res['device']['name']:<30} NO DATA")

    return all_res, meta, stock
