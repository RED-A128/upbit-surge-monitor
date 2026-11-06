
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Gap Integrity Audit
Clean V001

Re-evaluate retrospective +30% event outcomes using
actual UTC candle continuity.

Original OHLCV and event-study reports are read-only.
This audit does not prove independent OOS or profitability.

Usage:
    py audit_pattern_256_event_gap_integrity.py --root .
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V001"
STEPS = {"h1": 1, "h4": 4, "d1": 24}
HORIZONS = (24, 72, 168)
TARGET_MULTIPLIER = 1.30

OUTPUT_COLUMNS = [
    "timeframe", "market", "period", "signal_utc",
    "entry_utc", "horizon_hours", "original_outcome",
    "corrected_outcome", "first_gap_utc",
    "hours_to_target", "observed_candles",
    "expected_candles", "missing_candles",
    "full_window_contiguous", "original_outcome_agrees",
]

SUMMARY_COLUMNS = [
    "timeframe", "period", "horizon_hours",
    "events", "confirmed_hits", "confirmed_misses",
    "gap_censored", "end_censored",
    "confirmed_hit_rate_pct", "original_hit_rate_pct",
    "original_outcome_disagreements",
]


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def save_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def previous_evidence(root):
    folder = root / "data/reports/pattern_256_trigger_event_study"
    result_file = folder / "event_study_result.json"
    manifest_file = folder / "event_study_manifest.json"
    files_file = folder / "event_study_files.csv"
    events_file = folder / "event_study_events.csv"

    for path in (
        result_file, manifest_file, files_file, events_file
    ):
        check(path.is_file(), f"Missing input: {path}")

    result = load_json(result_file)
    manifest = load_json(manifest_file)

    check(result.get("execution") == "PASS", "Previous run failed")
    check(result.get("jobs") == 870, "Unexpected job count")
    check(result.get("failed_jobs") == 0, "Previous failures")
    check(result.get("event_records") == 252600,
          "Unexpected event record count")
    check(manifest.get("execution") == "PASS",
          "Previous manifest failed")

    hashes = manifest.get("output_sha256", {})
    expected_names = {
        "event_study_result.json",
        "event_study_files.csv",
        "event_study_events.csv",
    }
    check(set(hashes) == expected_names,
          "Previous manifest output set differs")

    for name, expected in hashes.items():
        check(digest(folder / name) == expected.lower(),
              f"Previous SHA256 mismatch: {name}")

    inventory = {}
    with files_file.open(
        "r", newline="", encoding="utf-8-sig"
    ) as f:
        for row in csv.DictReader(f):
            key = (row["timeframe"], row["market"])
            check(key not in inventory, f"Duplicate job: {key}")
            check(row["status"] == "PASS",
                  f"Previous job not PASS: {key}")
            inventory[key] = row

    check(len(inventory) == 870, "Incomplete inventory")

    return folder, inventory, digest(manifest_file)


def load_ohlcv(path, expected_market):
    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
        usecols=[
            "market", "candle_date_time_utc",
            "open", "high", "low",
        ],
        low_memory=False,
    )

    check(len(df) > 0, f"Empty OHLCV: {path}")
    check(
        df["market"].astype(str).eq(expected_market).all(),
        f"Market mismatch: {path}",
    )

    timestamps = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True,
        errors="coerce",
    )

    check(timestamps.notna().all(), f"Invalid time: {path}")
    check(timestamps.is_monotonic_increasing,
          f"Unsorted time: {path}")
    check(not timestamps.duplicated().any(),
          f"Duplicate time: {path}")

    times = timestamps.to_numpy(dtype="datetime64[ns]")
    opens = pd.to_numeric(
        df["open"], errors="coerce"
    ).to_numpy(float)
    highs = pd.to_numeric(
        df["high"], errors="coerce"
    ).to_numpy(float)
    lows = pd.to_numeric(
        df["low"], errors="coerce"
    ).to_numpy(float)

    check(
        np.isfinite(opens).all()
        and np.isfinite(highs).all()
        and np.isfinite(lows).all()
        and (opens > 0).all()
        and (highs >= lows).all(),
        f"Invalid prices: {path}",
    )

    return times, opens, highs


def audit_event(row, times, opens, highs, step_hours):
    step = np.timedelta64(step_hours, "h")
    hour = np.timedelta64(1, "h")

    entry_time = np.datetime64(
        pd.Timestamp(row["entry_utc"], tz="UTC")
        .tz_localize(None).to_datetime64(),
        "ns",
    )
    signal_time = np.datetime64(
        pd.Timestamp(row["signal_utc"], tz="UTC")
        .tz_localize(None).to_datetime64(),
        "ns",
    )

    horizon = int(row["horizon_hours"])
    check(horizon in HORIZONS, "Unexpected horizon")
    check(horizon % step_hours == 0,
          "Horizon not divisible by timeframe")

    index = int(np.searchsorted(times, entry_time))
    check(index < len(times), "Entry missing")
    check(times[index] == entry_time, "Entry timestamp mismatch")
    check(entry_time - signal_time == step,
          "Entry is not next regular candle")

    entry_price = float(row["entry_price"])
    check(
        np.isclose(
            opens[index], entry_price,
            rtol=1e-10, atol=1e-10,
        ),
        "Entry price mismatch",
    )

    expected = horizon // step_hours
    deadline = entry_time + np.timedelta64(horizon, "h")
    last_open = deadline - step

    right = int(np.searchsorted(
        times, last_open, side="right"
    ))

    observed_times = times[index:right]
    observed_highs = highs[index:right]

    check(len(observed_times) > 0, "Empty observation")

    # Identify first missing expected candle.
    # The next expected candle after a valid prefix
    # must be exactly one timeframe step later.
    prefix_length = 0
    first_gap = None

    for j, actual_time in enumerate(observed_times):
        expected_time = entry_time + j * step
        if actual_time != expected_time:
            first_gap = expected_time
            break
        prefix_length += 1

    if first_gap is None and prefix_length < expected:
        first_gap = entry_time + prefix_length * step

    complete = prefix_length == expected

    # Hits before the first gap remain observable.
    prefix_highs = observed_highs[:prefix_length]
    hit_indices = np.flatnonzero(
        prefix_highs >= entry_price * TARGET_MULTIPLIER
    )

    if len(hit_indices):
        hit_index = int(hit_indices[0])
        corrected = "HIT"
        hit_close = (
            entry_time + (hit_index + 1) * step
        )
        hours_to_target = float(
            (hit_close - entry_time) / hour
        )
    elif complete:
        corrected = "MISS"
        hours_to_target = ""
    elif first_gap is not None and first_gap <= times[-1]:
        corrected = "GAP_CENSORED"
        hours_to_target = ""
    else:
        corrected = "END_CENSORED"
        hours_to_target = ""

    original = row["outcome"]

    # Only a fully contiguous observation can confirm
    # the original HIT/MISS classification without
    # uncertainty from missing candles.
    agrees = (
        original == corrected
        if complete else ""
    )

    missing = expected - len(observed_times)
    # Missing count from expected total is only a lower
    # bound if extra irregular timestamps exist.
    check(missing >= 0, "Unexpected excess candles")

    return {
        "timeframe": row["timeframe"],
        "market": row["market"],
        "period": row["period"],
        "signal_utc": row["signal_utc"],
        "entry_utc": row["entry_utc"],
        "horizon_hours": horizon,
        "original_outcome": original,
        "corrected_outcome": corrected,
        "first_gap_utc": (
            str(first_gap) if first_gap is not None else ""
        ),
        "hours_to_target": hours_to_target,
        "observed_candles": len(observed_times),
        "expected_candles": expected,
        "missing_candles": missing,
        "full_window_contiguous": int(complete),
        "original_outcome_agrees": agrees,
    }


def run(root):
    root = root.resolve()
    folder, inventory, previous_hash = previous_evidence(root)

    events_path = folder / "event_study_events.csv"
    output = root / "data/reports/pattern_256_event_gap_integrity"
    output.mkdir(parents=True, exist_ok=True)

    events = pd.read_csv(
        events_path,
        encoding="utf-8-sig",
        dtype={
            "timeframe": str,
            "market": str,
            "period": str,
            "signal_utc": str,
            "entry_utc": str,
            "outcome": str,
        },
        low_memory=False,
    )

    check(len(events) == 252600, "Event count changed")

    expected_columns = {
        "timeframe", "market", "period", "signal_utc",
        "entry_utc", "entry_price", "horizon_hours",
        "outcome",
    }
    check(
        expected_columns.issubset(events.columns),
        "Missing event columns",
    )

    grouped = events.groupby(
        ["timeframe", "market"], sort=True
    )
    check(len(grouped) == 870, "Incomplete event coverage")

    output_rows = []
    errors = []
    completed = 0

    for (tf, market), group in grouped:
        try:
            check(tf in STEPS, f"Unknown timeframe: {tf}")
            key = (tf, market)
            check(key in inventory, f"Unknown job: {key}")

            source = (
                root / "data/ohlcv" / tf / f"{market}.csv"
            )
            check(source.is_file(), f"Missing OHLCV: {source}")

            before = digest(source)
            check(
                before == inventory[key]["ohlcv_sha256"],
                f"Source hash changed: {tf}/{market}",
            )

            times, opens, highs = load_ohlcv(
                source, market
            )

            check(
                len(times) == int(inventory[key]["signal_rows"]),
                f"Row count changed: {tf}/{market}",
            )

            # Every selected event has one record per horizon.
            check(
                len(group) == (
                    int(inventory[key]["selected_events"]) * 3
                ),
                f"Event count mismatch: {tf}/{market}",
            )

            unique_events = group[
                ["signal_utc", "entry_utc"]
            ].drop_duplicates()

            check(
                len(unique_events) == int(
                    inventory[key]["selected_events"]
                ),
                f"Duplicate event structure: {tf}/{market}",
            )

            for _, row in group.iterrows():
                output_rows.append(
                    audit_event(
                        row, times, opens, highs, STEPS[tf]
                    )
                )

            check(
                digest(source) == before,
                f"Source changed during audit: {tf}/{market}",
            )

            completed += 1

        except Exception as exc:
            message = f"{tf}/{market}: {type(exc).__name__}: {exc}"
            errors.append(message)
            print("[FAIL]", message, flush=True)

        if completed % 25 == 0 and completed > 0:
            print(
                f"[PROGRESS] completed={completed}/870 "
                f"errors={len(errors)}",
                flush=True,
            )

    # Prevent incomplete results from being accepted.
    success = (
        completed == 870
        and not errors
        and len(output_rows) == len(events)
    )

    aggregates = defaultdict(list)
    for row in output_rows:
        key = (
            row["timeframe"],
            row["period"],
            row["horizon_hours"],
        )
        aggregates[key].append(row)

    summary = []
    for (tf, period, horizon), rows in sorted(
        aggregates.items()
    ):
        hits = sum(
            r["corrected_outcome"] == "HIT" for r in rows
        )
        misses = sum(
            r["corrected_outcome"] == "MISS" for r in rows
        )
        gap = sum(
            r["corrected_outcome"] == "GAP_CENSORED"
            for r in rows
        )
        end = sum(
            r["corrected_outcome"] == "END_CENSORED"
            for r in rows
        )

        old_hits = sum(
            r["original_outcome"] == "HIT" for r in rows
        )
        old_misses = sum(
            r["original_outcome"] == "MISS" for r in rows
        )

        known = hits + misses
        old_known = old_hits + old_misses

        disagreements = sum(
            r["original_outcome_agrees"] is False
            for r in rows
        )

        summary.append({
            "timeframe": tf,
            "period": period,
            "horizon_hours": horizon,
            "events": len(rows),
            "confirmed_hits": hits,
            "confirmed_misses": misses,
            "gap_censored": gap,
            "end_censored": end,
            "confirmed_hit_rate_pct": (
                round(100 * hits / known, 4)
                if known else ""
            ),
            "original_hit_rate_pct": (
                round(100 * old_hits / old_known, 4)
                if old_known else ""
            ),
            "original_outcome_disagreements": disagreements,
        })

    result = {
        "audit": "pattern_256_event_gap_integrity",
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS" if success else "REVIEW_REQUIRED",
        "jobs": len(grouped),
        "completed_jobs": completed,
        "failed_jobs": len(errors),
        "errors": errors[:100],
        "original_event_records": len(events),
        "audited_event_records": len(output_rows),
        "confirmed_hits": sum(
            r["corrected_outcome"] == "HIT"
            for r in output_rows
        ),
        "confirmed_misses": sum(
            r["corrected_outcome"] == "MISS"
            for r in output_rows
        ),
        "gap_censored": sum(
            r["corrected_outcome"] == "GAP_CENSORED"
            for r in output_rows
        ),
        "end_censored": sum(
            r["corrected_outcome"] == "END_CENSORED"
            for r in output_rows
        ),
        "summary": summary,
        "gap_integrity_audit_completed": success,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "prediction_performance_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Historical study, not independent OOS.",
            "Gap-censored events are not confirmed misses.",
            "Intracandle target touches are not executable trades.",
            "Timeframe and market event dependence remains.",
            "Source data cannot reveal unrecorded price paths.",
            "No fees, spread, slippage or liquidity modeling.",
        ],
    }

    details_file = output / "gap_integrity_events.csv"
    summary_file = output / "gap_integrity_summary.csv"
    result_file = output / "gap_integrity_result.json"
    manifest_file = output / "gap_integrity_manifest.json"

    save_csv(details_file, OUTPUT_COLUMNS, output_rows)
    save_csv(summary_file, SUMMARY_COLUMNS, summary)
    save_json(result_file, result)

    save_json(manifest_file, {
        "audit": result["audit"],
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            details_file.name: digest(details_file),
            summary_file.name: digest(summary_file),
            result_file.name: digest(result_file),
        },
    })

    print(json.dumps({
        "execution": result["execution"],
        "completed_jobs": completed,
        "audited_events": len(output_rows),
        "confirmed_hits": result["confirmed_hits"],
        "confirmed_misses": result["confirmed_misses"],
        "gap_censored": result["gap_censored"],
        "end_censored": result["end_censored"],
        "research_decision": "HOLD",
    }, ensure_ascii=False, indent=2))

    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    try:
        return run(Path(args.root))
    except KeyboardInterrupt:
        print("[STOP] Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
