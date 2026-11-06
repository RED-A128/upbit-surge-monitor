
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Trigger +30% Event Study
Clean V001

Retrospective research only.
No automatic trading, training approval, or independent OOS claim.

Entry: next regular candle open after a 0->1 trigger.
Horizons: 24h, 72h, 168h.
Target: high >= entry * 1.30.
Cooldown: 168 calendar hours, per market/timeframe.
Missing candles: actual UTC time, never assumed continuous.
Right-censored events: excluded from success/failure denominators.
Checkpoint: source hashes + script hash + previous manifest hash.

Usage:
  py evaluate_pattern_256_trigger_event_study.py --root .
  py evaluate_pattern_256_trigger_event_study.py --root . --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

VERSION = "Clean V001"
TIMEFRAMES = {"h1": 1, "h4": 4, "d1": 24}
HORIZONS = (24, 72, 168)
COOLDOWN_HOURS = 168
TARGET_RETURN = 0.30

PERIODS = (
    ("DEVELOPMENT", "2017-01-01", "2024-01-01"),
    ("VALIDATION", "2024-01-01", "2025-07-01"),
    ("RETROSPECTIVE_TEST", "2025-07-01", "2026-10-01"),
)

EVENT_COLUMNS = (
    "timeframe", "market", "period", "signal_utc",
    "entry_utc", "entry_price", "horizon_hours",
    "outcome", "target_hit", "hours_to_target",
    "max_high_return_pct", "mae_pct",
    "observed_candles", "complete_window",
)

FILE_COLUMNS = (
    "timeframe", "market", "signal_rows",
    "trigger_run_starts", "selected_events",
    "event_records", "skipped_entry_gap",
    "skipped_end_of_data", "skipped_cooldown",
    "ohlcv_sha256", "signal_sha256",
    "status", "error",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_previous(root):
    folder = root / "data/reports/pattern_256_signal_ohlcv_alignment"
    result_path = folder / "alignment_result.json"
    manifest_path = folder / "alignment_manifest.json"
    inventory_path = folder / "alignment_files.csv"

    require(result_path.is_file(), "Missing alignment result")
    require(manifest_path.is_file(), "Missing alignment manifest")
    require(inventory_path.is_file(), "Missing alignment inventory")

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    require(result.get("execution") == "PASS", "Alignment not PASS")
    require(result.get("jobs") == 870, "Unexpected previous job count")
    require(result.get("completed_jobs") == 870, "Incomplete alignment")
    require(result.get("failed_jobs") == 0, "Previous failures")
    require(result.get("mismatches") == 0, "Previous mismatches")
    require(
        result.get("signal_ohlcv_alignment_verified") is True,
        "Alignment not verified",
    )
    require(result.get("research_decision") == "HOLD", "Unexpected decision")
    require(manifest.get("execution") == "PASS", "Manifest not PASS")

    hashes = manifest.get("output_sha256", {})
    require(
        set(hashes) == {"alignment_files.csv", "alignment_result.json"},
        "Unexpected manifest outputs",
    )
    for name, expected in hashes.items():
        path = folder / name
        require(path.is_file(), f"Missing previous output: {name}")
        require(sha256(path) == expected.lower(), f"SHA256 mismatch: {name}")

    inventory = {}
    with inventory_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["timeframe"], row["market"])
            require(key not in inventory, f"Duplicate inventory: {key}")
            require(row["status"] == "PASS", f"Failed inventory: {key}")
            inventory[key] = row

    require(len(inventory) == 870, "Incomplete alignment inventory")
    return sha256(manifest_path), inventory


def discover(root, inventory):
    jobs = []
    for tf in TIMEFRAMES:
        source_dir = root / "data/ohlcv" / tf
        signal_dir = root / "data/signals/256" / tf
        require(source_dir.is_dir(), f"Missing: {source_dir}")
        require(signal_dir.is_dir(), f"Missing: {signal_dir}")

        sources = {p.stem: p for p in source_dir.glob("KRW-*.csv")}
        signals = {p.stem: p for p in signal_dir.glob("KRW-*.csv")}
        require(set(sources) == set(signals), f"Coverage mismatch: {tf}")

        for market in sorted(sources):
            key = (tf, market)
            require(key in inventory, f"Missing previous evidence: {key}")
            jobs.append((tf, market, sources[market], signals[market]))

    require(len(jobs) == 870, "Expected 870 jobs")
    require(
        {(tf, market) for tf, market, _, _ in jobs} == set(inventory),
        "Market/timeframe inventory mismatch",
    )
    return jobs


def period_for(timestamp):
    date = pd.Timestamp(timestamp).strftime("%Y-%m-%d")
    for name, start, end in PERIODS:
        if start <= date < end:
            return name
    return "OUTSIDE_PERIODS"


def load_frame(path, market, columns):
    df = pd.read_csv(
        path, encoding="utf-8-sig",
        usecols=list(columns),
        low_memory=False,
    )
    require(len(df) > 0, f"Empty input: {path}")
    require(
        df["market"].notna().all()
        and df["market"].astype(str).eq(market).all(),
        f"Market mismatch: {path}",
    )

    times = pd.to_datetime(
        df["candle_date_time_utc"], utc=True, errors="coerce"
    )
    require(times.notna().all(), f"Invalid timestamps: {path}")
    require(times.is_monotonic_increasing, f"Unsorted timestamps: {path}")
    require(not times.duplicated().any(), f"Duplicate timestamps: {path}")

    df["_utc"] = times
    return df


def inspect_job(tf, market, source_path, signal_path, previous):
    source_hash = sha256(source_path)
    signal_hash = sha256(signal_path)

    require(
        source_hash == previous["ohlcv_sha256"],
        f"OHLCV changed since alignment audit: {tf}/{market}",
    )
    require(
        signal_hash == previous["signal_sha256"],
        f"Signal changed since alignment audit: {tf}/{market}",
    )

    source = load_frame(
        source_path, market,
        ("market", "candle_date_time_utc", "open", "high", "low"),
    )
    signals = load_frame(
        signal_path, market,
        ("market", "candle_date_time_utc", "signal_trigger"),
    )

    require(len(source) == len(signals), "Row count mismatch")
    require(len(source) == int(previous["rows"]), "Previous rows mismatch")
    require(
        source["_utc"].equals(signals["_utc"]),
        "Source/signal timestamps differ",
    )

    times = source["_utc"].to_numpy(dtype="datetime64[ns]")
    opens = pd.to_numeric(source["open"], errors="coerce").to_numpy(float)
    highs = pd.to_numeric(source["high"], errors="coerce").to_numpy(float)
    lows = pd.to_numeric(source["low"], errors="coerce").to_numpy(float)

    raw_trigger = signals["signal_trigger"].astype(str).str.strip().str.lower()
    require(
        raw_trigger.isin(("0", "1", "0.0", "1.0", "true", "false")).all(),
        "Invalid binary trigger values",
    )
    trigger = raw_trigger.isin(("1", "1.0", "true")).to_numpy()

    require(
        np.isfinite(opens).all()
        and np.isfinite(highs).all()
        and np.isfinite(lows).all()
        and (opens > 0).all(),
        "Invalid OHLC prices",
    )

    previous_trigger = np.r_[False, trigger[:-1]]
    starts = np.flatnonzero(trigger & ~previous_trigger)

    one_hour = np.timedelta64(1, "h")
    step = np.timedelta64(TIMEFRAMES[tf], "h")

    events = []
    selected = 0
    skipped_gap = 0
    skipped_end = 0
    skipped_cooldown = 0
    cooldown_until = np.datetime64("1970-01-01", "ns")

    for i in starts:
        signal_time = times[i]

        if signal_time < cooldown_until:
            skipped_cooldown += 1
            continue

        entry_index = int(i) + 1
        if entry_index >= len(times):
            skipped_end += 1
            continue

        entry_time = times[entry_index]
        if entry_time - signal_time != step:
            skipped_gap += 1
            continue

        entry = float(opens[entry_index])
        selected += 1

        # Cooldown is based on signal time and is applied
        # only after an eligible entry is found.
        cooldown_until = signal_time + np.timedelta64(
            COOLDOWN_HOURS, "h"
        )

        for horizon in HORIZONS:
            # Include candles whose open time is within the horizon.
            # A candle's high/low is only observable at its close.
            # Therefore include only candles fully closed by deadline.
            deadline = entry_time + np.timedelta64(horizon, "h")
            last_open = deadline - step

            right = int(np.searchsorted(times, last_open, side="right"))
            right = max(right, entry_index)

            window_highs = highs[entry_index:right]
            window_lows = lows[entry_index:right]
            window_times = times[entry_index:right]

            # A complete observation requires enough recorded
            # time coverage to reach the final expected candle.
            complete = (
                len(window_times) > 0
                and window_times[-1] == last_open
            )

            hit_indices = np.flatnonzero(
                window_highs >= entry * (1 + TARGET_RETURN)
            )
            hit = len(hit_indices) > 0

            if hit:
                first_hit = int(hit_indices[0])
                # Conservative: count hit at candle close.
                hit_close = window_times[first_hit] + step
                hours_to_target = float(
                    (hit_close - entry_time) / one_hour
                )
                outcome = "HIT"
            elif complete:
                hours_to_target = ""
                outcome = "MISS"
            else:
                hours_to_target = ""
                outcome = "CENSORED"

            max_high = (
                (float(np.max(window_highs)) / entry - 1) * 100
                if len(window_highs) else ""
            )
            mae = (
                (float(np.min(window_lows)) / entry - 1) * 100
                if len(window_lows) else ""
            )

            events.append({
                "timeframe": tf,
                "market": market,
                "period": period_for(signal_time),
                "signal_utc": str(signal_time),
                "entry_utc": str(entry_time),
                "entry_price": entry,
                "horizon_hours": horizon,
                "outcome": outcome,
                "target_hit": int(hit),
                "hours_to_target": hours_to_target,
                "max_high_return_pct": max_high,
                "mae_pct": mae,
                "observed_candles": len(window_times),
                "complete_window": int(complete),
            })

    require(
        sha256(source_path) == source_hash
        and sha256(signal_path) == signal_hash,
        "Source changed during event study",
    )

    return {
        "version": VERSION,
        "timeframe": tf,
        "market": market,
        "signal_rows": len(source),
        "trigger_run_starts": len(starts),
        "selected_events": selected,
        "event_records": len(events),
        "skipped_entry_gap": skipped_gap,
        "skipped_end_of_data": skipped_end,
        "skipped_cooldown": skipped_cooldown,
        "ohlcv_sha256": source_hash,
        "signal_sha256": signal_hash,
        "status": "PASS",
        "error": "",
        "events": events,
    }


def valid_checkpoint(data, tf, market, source_hash, signal_hash,
                     previous_hash, script_hash):
    if not isinstance(data, dict):
        return False

    checks = {
        "version": VERSION,
        "timeframe": tf,
        "market": market,
        "ohlcv_sha256": source_hash,
        "signal_sha256": signal_hash,
        "previous_sha256": previous_hash,
        "script_sha256": script_hash,
        "status": "PASS",
    }
    if any(data.get(k) != v for k, v in checks.items()):
        return False

    events = data.get("events")
    if not isinstance(events, list):
        return False
    if data.get("event_records") != len(events):
        return False
    if len(events) != data.get("selected_events", -1) * len(HORIZONS):
        return False
    return all(
        isinstance(e, dict)
        and all(k in e for k in EVENT_COLUMNS)
        for e in events
    )


def run(root, force):
    root = root.resolve()
    previous_hash, inventory = verify_previous(root)
    script_hash = sha256(Path(__file__).resolve())
    jobs = discover(root, inventory)

    output = root / "data/reports/pattern_256_trigger_event_study"
    checkpoints = output / "checkpoints"
    checkpoints.mkdir(parents=True, exist_ok=True)

    file_rows = []
    event_rows = []
    errors = []
    executed = 0
    resumed = 0

    for index, (tf, market, source, signal) in enumerate(jobs, 1):
        try:
            source_hash = sha256(source)
            signal_hash = sha256(signal)
            previous = inventory[(tf, market)]

            require(
                source_hash == previous["ohlcv_sha256"]
                and signal_hash == previous["signal_sha256"],
                "Input changed since alignment audit",
            )

            cp = checkpoints / f"{tf}_{market}.json"
            data = None

            if not force and cp.is_file():
                try:
                    candidate = read_json(cp)
                    if valid_checkpoint(
                        candidate, tf, market, source_hash,
                        signal_hash, previous_hash, script_hash
                    ):
                        data = candidate
                except (OSError, ValueError, TypeError):
                    pass

            if data is None:
                data = inspect_job(
                    tf, market, source, signal, previous
                )
                data["previous_sha256"] = previous_hash
                data["script_sha256"] = script_hash
                write_json(cp, data)
                executed += 1
            else:
                resumed += 1

            file_rows.append({
                k: data[k] for k in FILE_COLUMNS
            })
            event_rows.extend(data["events"])

        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append({
                "timeframe": tf, "market": market, "error": message
            })
            file_rows.append({
                "timeframe": tf, "market": market,
                "signal_rows": 0, "trigger_run_starts": 0,
                "selected_events": 0, "event_records": 0,
                "skipped_entry_gap": 0,
                "skipped_end_of_data": 0,
                "skipped_cooldown": 0,
                "ohlcv_sha256": "",
                "signal_sha256": "",
                "status": "FAIL", "error": message,
            })
            print(f"[FAIL] {tf}/{market}: {message}", flush=True)

        if index % 25 == 0 or index == len(jobs):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"executed={executed} resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    summary = []
    for tf in TIMEFRAMES:
        for period, _, _ in PERIODS:
            for horizon in HORIZONS:
                selected = [
                    e for e in event_rows
                    if e["timeframe"] == tf
                    and e["period"] == period
                    and e["horizon_hours"] == horizon
                ]
                hits = sum(e["outcome"] == "HIT" for e in selected)
                misses = sum(e["outcome"] == "MISS" for e in selected)
                censored = sum(e["outcome"] == "CENSORED" for e in selected)
                known = hits + misses

                summary.append({
                    "timeframe": tf,
                    "period": period,
                    "horizon_hours": horizon,
                    "events": len(selected),
                    "hits": hits,
                    "misses": misses,
                    "censored": censored,
                    "hit_rate_pct": (
                        round(hits / known * 100, 4)
                        if known else ""
                    ),
                })

    passed = (
        not errors
        and executed + resumed == len(jobs)
        and len(file_rows) == len(jobs)
        and len(event_rows) == sum(
            int(r["event_records"]) for r in file_rows
        )
    )

    result = {
        "audit": "pattern_256_trigger_event_study",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "execution": "PASS" if passed else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "completed_jobs": executed + resumed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(errors),
        "errors": errors[:100],
        "trigger_run_starts": sum(
            int(r["trigger_run_starts"]) for r in file_rows
        ),
        "selected_events": sum(
            int(r["selected_events"]) for r in file_rows
        ),
        "event_records": len(event_rows),
        "skipped_entry_gap": sum(
            int(r["skipped_entry_gap"]) for r in file_rows
        ),
        "skipped_end_of_data": sum(
            int(r["skipped_end_of_data"]) for r in file_rows
        ),
        "skipped_cooldown": sum(
            int(r["skipped_cooldown"]) for r in file_rows
        ),
        "summary": summary,
        "target_return": TARGET_RETURN,
        "horizons_hours": list(HORIZONS),
        "cooldown_hours": COOLDOWN_HOURS,
        "retrospective_event_study_completed": passed,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "upstream_leakage_free_proven": False,
        "actual_trade_execution_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Retrospective outcomes are not independent OOS.",
            "High-price target touches do not prove execution.",
            "No fees, spread, slippage, or order-book liquidity.",
            "A candle with a data gap is not a continuous market.",
            "Overlapping timeframes can represent the same move.",
            "MAE is the minimum low over the full horizon, "
            "not necessarily the drawdown before target hit.",
            "No feature point-in-time causality proof.",
        ],
    }

    files_path = output / "event_study_files.csv"
    events_path = output / "event_study_events.csv"
    result_path = output / "event_study_result.json"
    manifest_path = output / "event_study_manifest.json"

    write_csv(files_path, FILE_COLUMNS, file_rows)
    write_csv(events_path, EVENT_COLUMNS, event_rows)
    write_json(result_path, result)

    write_json(manifest_path, {
        "audit": "pattern_256_trigger_event_study",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            files_path.name: sha256(files_path),
            events_path.name: sha256(events_path),
            result_path.name: sha256(result_path),
        },
    })

    print(json.dumps({
        "execution": result["execution"],
        "jobs": result["jobs"],
        "completed": result["completed_jobs"],
        "failed": result["failed_jobs"],
        "selected_events": result["selected_events"],
        "event_records": result["event_records"],
        "summary": summary,
        "research_decision": "HOLD",
    }, ensure_ascii=False, indent=2))

    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    try:
        return run(Path(args.root), args.force)
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
